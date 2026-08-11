'use strict';

const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const { EventEmitter } = require('node:events');

const { WrpcClient, RpcServer, defineRouter, procedure } = require('../../index.js');
const { wrpcFastify, findUwsApp } = require('../../fastify.js');
const { ProtocolClient } = require('../websocket/protocolClient.js');
const { optional, quiet } = require('./boots.js');

const fastify = optional('fastify');
// Deliberately NOT requireUws(): pnpm resolves a second uWebSockets.js copy
// for fastify-uws, and loading two builds of the native addon into one
// process segfaults on exit. This file only ever touches the copy
// fastify-uws itself loads.
const fastifyUws = optional('fastify-uws');

const noFastify = fastify ? false : 'fastify unavailable';
const noFastifyUws = fastify && fastifyUws ? false : 'fastify-uws unavailable';

// `mark` starts a session from inside a route handler: the session lands in
// the SessionManager of whichever RpcServer served the call, so finding it
// in the decorated core proves the routes and the decorator share one.
const createRouter = (trace = []) =>
  defineRouter({
    probe: {
      echo: procedure({
        access: 'public',
        handler: async (_context, args) => {
          trace.push('procedure');
          return args;
        },
      }),
      mark: procedure({
        access: 'public',
        handler: async (context, args) => {
          context.client.startSession(undefined, { marker: args.marker });
          return { ok: true };
        },
      }),
    },
  });

// `Connection: close` on purpose: fastify-uws does not drop idle keep-alive
// sockets when it closes, and a pooled one would hold the run open for the
// whole undici keep-alive timeout.
const postPacket = (url, method, args) =>
  fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Connection: 'close' },
    body: JSON.stringify({ type: 'call', id: '1', method, args }),
  });

// `bodyLimit` is a fastify CONSTRUCTOR option, not a plugin one — it has to
// be split out here or it would be forwarded to wrpcFastify and ignored.
const boot = async (t, options = {}) => {
  const { serverFactory = null, hooks = null, bodyLimit, ...pluginOptions } = options;
  const app = fastify({
    ...(serverFactory ? { serverFactory } : {}),
    ...(bodyLimit === undefined ? {} : { bodyLimit }),
    logger: false,
  });
  // Teardown is registered before anything can throw: a leaked app keeps a
  // listening socket (a native one under uws) and would wedge the whole run
  // instead of failing the test.
  t.after(async () => {
    app.server.closeAllConnections?.();
    await app.close().catch(() => {});
  });
  if (hooks) for (const [name, handler] of Object.entries(hooks)) app.addHook(name, handler);
  await app.register(wrpcFastify, { console: quiet, ...pluginOptions });
  await app.listen({ host: '127.0.0.1', port: 0 });
  const { port } = app.server.address();
  return { app, port, origin: `http://127.0.0.1:${port}` };
};

// A fastify look-alike: enough surface for the plugin body, with the
// backend detection pointed at whatever `server` we hand it.
const fakeFastify = (server = {}) => {
  const instance = {
    server,
    log: quiet,
    routes: [],
    hooks: new Map(),
    decorations: new Map(),
    route: (definition) => void instance.routes.push(definition),
    decorate: (name, value) => void instance.decorations.set(name, value),
    addHook: (name, handler) => void instance.hooks.set(name, handler),
  };
  return instance;
};

const fakeEngine = (overrides = {}) => {
  const source = new EventEmitter();
  const engine = {
    name: 'fake',
    source,
    attached: null,
    closed: false,
    attach(attachOptions) {
      engine.attached = attachOptions;
      return source;
    },
    close() {
      engine.closed = true;
    },
    ...overrides,
  };
  return engine;
};

test('backend detection: a plain fastify runs on the node engine', { skip: noFastify }, async (t) => {
  const { app, origin, port } = await boot(t, { router: createRouter() });

  assert.ok(app.server instanceof http.Server, 'plain fastify owns a real node http server');
  assert.strictEqual(findUwsApp(app.server), null);
  // The node engine drives the handshake off the http server's own event.
  assert.strictEqual(app.server.listenerCount('upgrade'), 1);

  const res = await postPacket(`${origin}/api`, 'probe/echo', { a: 1 });
  assert.strictEqual(res.status, 200);
  assert.deepStrictEqual((await res.json()).result, { a: 1 });

  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
  t.after(() => void client.close());
  await client.load('probe');
  assert.deepStrictEqual(await client.api.probe.echo({ b: 2 }), { b: 2 });
});

test('backend detection: a fastify-uws instance runs on the uws engine', { skip: noFastifyUws }, async (t) => {
  const { app, origin, port } = await boot(t, {
    router: createRouter(),
    serverFactory: fastifyUws.serverFactory,
  });

  assert.ok(!(app.server instanceof http.Server), 'fastify-uws fakes the node server');
  const uwsApp = findUwsApp(app.server);
  assert.ok(uwsApp, 'the uws TemplatedApp is reachable on the fake server');
  assert.strictEqual(typeof uwsApp.ws, 'function');
  // Nothing was bound to a node upgrade event — uws owns the handshake.
  assert.strictEqual(app.server.listenerCount('upgrade'), 0);

  const res = await postPacket(`${origin}/api`, 'probe/echo', { a: 1 });
  assert.strictEqual(res.status, 200);
  assert.deepStrictEqual((await res.json()).result, { a: 1 });

  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
  t.after(() => void client.close());
  await client.load('probe');
  assert.deepStrictEqual(await client.api.probe.echo({ b: 2 }), { b: 2 });
});

test('findUwsApp: only a real uws app on a uws-backed server passes', () => {
  assert.strictEqual(findUwsApp(null), null);
  assert.strictEqual(findUwsApp(undefined), null);
  assert.strictEqual(findUwsApp({}), null);
  assert.strictEqual(findUwsApp('nope'), null);
  assert.strictEqual(findUwsApp(http.createServer()), null);

  // The symbol is matched by description, not identity: fastify-uws exports
  // no accessor, so a private Symbol('uws.app') is all there is to go on.
  const app = { ws: () => {}, any: () => {}, listen: () => {} };
  assert.strictEqual(findUwsApp({ [Symbol('uws.app')]: app }), app);
  // Right symbol, wrong payload — still not a TemplatedApp.
  assert.strictEqual(findUwsApp({ [Symbol('uws.app')]: {} }), null);
  assert.strictEqual(findUwsApp({ [Symbol('other.app')]: app }), null);
});

test('options.engine short-circuits detection and preClose owns teardown', async () => {
  const engine = fakeEngine();
  // A server nothing can detect: without the explicit engine this throws.
  const instance = fakeFastify({ notDetectable: true });
  await wrpcFastify(instance, { router: createRouter(), console: quiet, engine });

  assert.ok(engine.attached, 'the injected engine was attached');
  assert.strictEqual(engine.attached.server, instance.server, 'a hosted engine gets the fastify server');
  assert.ok(instance.decorations.get('wrpc') instanceof RpcServer);
  assert.deepStrictEqual(
    instance.routes.map((route) => route.url),
    ['/api', '/api/events', '/api/:unit/:method'],
    'packet route + SSE stream + REST route',
  );
  assert.ok(instance.hooks.has('preClose'));

  assert.strictEqual(engine.closed, false);
  await instance.hooks.get('preClose')();
  assert.strictEqual(engine.closed, true);
});

test('a non-Engine options.engine is refused at the boundary', async () => {
  const instance = fakeFastify(http.createServer());
  await assert.rejects(
    wrpcFastify(instance, { router: createRouter(), console: quiet, engine: {} }),
    /does not implement the Engine contract/,
  );
  assert.strictEqual(instance.routes.length, 0, 'nothing is registered when the engine is rejected');
});

test('a fastify instance with no detectable backend fails loudly', async () => {
  const instance = fakeFastify({ neitherNodeNorUws: true });
  await assert.rejects(
    wrpcFastify(instance, { router: createRouter(), console: quiet }),
    /could not detect a WebSocket backend/,
  );
});

test('the wrpc decorator is the very core the routes call through', { skip: noFastify }, async (t) => {
  const { app, origin } = await boot(t, { router: createRouter() });

  assert.ok(app.wrpc instanceof RpcServer);
  // skip-override: the decorator and the routes belong to the ROOT instance,
  // not to a child scope that vanishes with the plugin.
  assert.strictEqual(app.hasDecorator('wrpc'), true);
  assert.strictEqual(wrpcFastify[Symbol.for('skip-override')], true);

  const res = await postPacket(`${origin}/api`, 'probe/mark', { marker: 'from-the-route' });
  assert.strictEqual(res.status, 200);
  assert.deepStrictEqual((await res.json()).result, { ok: true });

  // The session the route handler started is in the decorated core's own
  // SessionManager — one core, not one per surface.
  const [cookie] = res.headers.getSetCookie();
  const token = cookie.match(/^token=([^;]+)/)[1];
  const session = await app.wrpc.sessions.restore(token);
  assert.ok(session, 'the decorated core knows the session the route created');
  assert.strictEqual(session.state.marker, 'from-the-route');
});

test('fastify hooks run before wrpc sees the call', { skip: noFastify }, async (t) => {
  // The point of routing RPC through fastify's own router: the app's hooks,
  // auth and error handling run first, on RPC calls like on any other route.
  const trace = [];
  const seen = [];
  const onRequest = async (request) => {
    trace.push('hook');
    seen.push(`${request.method} ${request.url}`);
  };
  const { origin } = await boot(t, { router: createRouter(trace), hooks: { onRequest } });

  const res = await postPacket(`${origin}/api`, 'probe/echo', { hooked: true });
  assert.strictEqual(res.status, 200);
  assert.deepStrictEqual((await res.json()).result, { hooked: true });
  assert.deepStrictEqual(seen, ['POST /api']);
  assert.deepStrictEqual(trace, ['hook', 'procedure'], 'the hook ran before the procedure');
});

test('preClose tears the core down and leaves no sockets', { skip: noFastify }, async (t) => {
  const { app, port } = await boot(t, { router: createRouter() });
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
  t.after(() => void client.close());
  await client.load('probe');
  assert.deepStrictEqual(await client.api.probe.echo({ ok: true }), { ok: true });
  assert.strictEqual(app.wrpc.clients.size, 1, 'the WebSocket client is tracked by the core');

  await app.close();
  assert.strictEqual(app.wrpc.clients.size, 0);
  assert.strictEqual(app.server.listening, false);
});

test('an in-flight HTTP call keeps its client until the response is written', { skip: noFastify }, async (t) => {
  // Regression: onAbort used to be wired to request.raw, whose 'close' fires
  // as soon as fastify finishes consuming the body — before the handler runs.
  // Every HTTP client was therefore destroyed at call start: rpc.clients was
  // empty mid-call and a handler watching client 'close' saw a false abort.
  let observed = null;
  let abortedEarly = false;
  const router = defineRouter({
    probe: {
      inspect: procedure({
        access: 'public',
        handler: async (context) => {
          context.client.on('close', () => {
            abortedEarly = true;
          });
          await new Promise((resolve) => setTimeout(resolve, 20));
          // Sampled INSIDE the handler: after the response is written the
          // client is evicted for real, so checking afterwards proves nothing.
          observed = {
            tracked: app.wrpc.clients.has(context.client),
            size: app.wrpc.clients.size,
            abortedEarly,
          };
          return { ok: true };
        },
      }),
    },
  });
  const booted = await boot(t, { router });
  const { app, origin } = booted;

  const res = await postPacket(`${origin}/api`, 'probe/inspect', {});
  assert.strictEqual(res.status, 200);
  assert.deepStrictEqual((await res.json()).result, { ok: true });
  assert.deepStrictEqual(
    observed,
    { tracked: true, size: 1, abortedEarly: false },
    'the client must outlive the handler, with no abort while the call runs',
  );

  // ...and it is evicted once the response is out
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.strictEqual(app.wrpc.clients.size, 0);
});

test("fastify's own logger is adapted, not handed to the core raw", { skip: noFastify }, async (t) => {
  // Regression: the plugin defaulted `console` to fastify.log, but that is
  // pino — it has info/warn/error and no `log`, which the core calls after
  // writing every successful callback. `fastify.log` is always truthy, so
  // `??` never fell through to globalThis.console. The throw landed inside
  // handleRpc's catch, which then answered a SECOND time: over WebSocket
  // every successful call came back as the result AND a bogus 500, because
  // (unlike the HTTP transport) a ws transport has no responded-once guard.
  const app = fastify({ logger: { level: 'silent' } });
  t.after(async () => {
    app.server.closeAllConnections?.();
    await app.close().catch(() => {});
  });
  assert.strictEqual(typeof app.log.log, 'undefined', 'pino has no console.log');

  // Booting WITHOUT an explicit `console` is the whole point of this test.
  await app.register(wrpcFastify, { router: createRouter() });
  await app.listen({ host: '127.0.0.1', port: 0 });
  const { port } = app.server.address();

  const rest = await postPacket(`http://127.0.0.1:${port}/api`, 'probe/echo', { via: 'pino' });
  assert.strictEqual(rest.status, 200);
  assert.deepStrictEqual((await rest.json()).result, { via: 'pino' });

  // Counted at the frame level: a WrpcClient would resolve on the first
  // callback and quietly ignore the second, hiding the defect.
  const peer = new ProtocolClient(`ws://127.0.0.1:${port}/api`);
  t.after(() => void peer.close());
  await new Promise((resolve) => peer.on('open', resolve));
  const frames = [];
  peer.on('message', (data) => frames.push(JSON.parse(data.toString())));
  peer.sendText(JSON.stringify({ type: 'call', id: '9', method: 'probe/echo', args: { via: 'ws' } }));
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.deepStrictEqual(frames, [{ type: 'callback', id: '9', result: { via: 'ws' } }]);
});

test('a logger with neither log nor info falls back to the global console', { skip: noFastify }, async (t) => {
  const app = fakeFastify({});
  app.log = { warn: () => {} }; // not a console, not a pino
  const engine = fakeEngine();
  await wrpcFastify(app, { router: createRouter(), engine });
  assert.ok(app.decorations.get('wrpc') instanceof RpcServer);
  t.after(() => engine.close());
});

test("body limits are fastify's, not the adapter's", { skip: noFastify }, async (t) => {
  // This plugin never touches the request stream — fastify parses the body
  // and hands over request.body — so unlike the express and uws adapters
  // there is nothing here to meter. fastify's own bodyLimit guards the RPC
  // routes and answers 413 before the handler runs.
  const oversized = (bytes) =>
    JSON.stringify({ type: 'call', id: '1', method: 'probe/echo', args: { big: 'x'.repeat(bytes) } });

  await t.test('the app-wide bodyLimit applies to the RPC routes', async (sub) => {
    const { origin } = await boot(sub, { router: createRouter(), bodyLimit: 512 });
    const res = await fetch(`${origin}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Connection: 'close' },
      body: oversized(4096),
    });
    assert.strictEqual(res.status, 413);
    assert.strictEqual((await res.json()).code, 'FST_ERR_CTP_BODY_TOO_LARGE');
  });

  await t.test('maxBodySize narrows it per route', async (sub) => {
    const { origin } = await boot(sub, { router: createRouter(), maxBodySize: 512 });
    const res = await fetch(`${origin}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Connection: 'close' },
      body: oversized(4096),
    });
    assert.strictEqual(res.status, 413);

    const ok = await postPacket(`${origin}/api`, 'probe/echo', { small: true });
    assert.strictEqual(ok.status, 200);
  });

  await t.test('unset, the app keeps its own limit — a plugin must not raise it', async (sub) => {
    // MAX_BODY_SIZE (10 MiB) is deliberately NOT applied as a default here:
    // silently loosening the host's 1 MiB limit would be a regression the
    // app never asked for.
    const { origin } = await boot(sub, { router: createRouter(), bodyLimit: 512 });
    const res = await fetch(`${origin}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Connection: 'close' },
      body: oversized(4096),
    });
    assert.strictEqual(res.status, 413);
  });
});
