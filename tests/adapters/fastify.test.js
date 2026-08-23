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
  await app.register(wrpcFastify, { logger: false, ...pluginOptions });
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
  await wrpcFastify(instance, { router: createRouter(), logger: false, engine });

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
    wrpcFastify(instance, { router: createRouter(), logger: false, engine: {} }),
    /does not implement the Engine contract/,
  );
  assert.strictEqual(instance.routes.length, 0, 'nothing is registered when the engine is rejected');
});

test('a fastify instance with no detectable backend fails loudly', async () => {
  const instance = fakeFastify({ neitherNodeNorUws: true });
  await assert.rejects(
    wrpcFastify(instance, { router: createRouter(), logger: false }),
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

test('a partial fastify logger is accepted rather than rejected', { skip: noFastify }, async (t) => {
  const app = fakeFastify({});
  app.log = { warn: () => {} }; // neither a full console nor a pino
  const engine = fakeEngine();
  await wrpcFastify(app, { router: createRouter(), engine });
  assert.ok(app.decorations.get('wrpc') instanceof RpcServer);
  t.after(() => engine.close());
});

test('a fastify pino goes in as a structured logger', { skip: noFastify }, async (t) => {
  const entries = [];
  const app = fakeFastify({});
  // The shape fastify's default logger actually has: `child`/`level` and no
  // `log`, which used to need an adapter in between.
  const pinoLike = {
    level: 'info',
    child: () => pinoLike,
    info: (entry, message) => entries.push([entry, message]),
    debug: () => {},
    warn: () => {},
    error: () => {},
  };
  app.log = pinoLike;
  const engine = fakeEngine();
  await wrpcFastify(app, { router: createRouter(), engine });
  const rpc = app.decorations.get('wrpc');
  assert.ok(rpc instanceof RpcServer);
  rpc.broadcast('ping', 1);
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

// ---------------------------------------------------------------------------
// Delegated REST routes: a procedure with an `http` mapping becomes a real
// fastify route — fastify owns routing, validation, serialization and error
// shape; wrpc supplies the Context and the bare handler.

// Ajv-shaped, permissive: the delegated HTTP path is validated by fastify's
// real ajv; injecting one here satisfies the router's "a schema must have a
// compiler" rule for the ws path without pulling ajv into devDependencies.
const permissiveAjv = { compile: () => () => true };

const restRouter = (trace = []) =>
  defineRouter(
    {
      projects: {
        create: procedure({
          access: 'public',
          http: { method: 'POST', path: '/projects/:orgId', status: 201 },
          schema: {
            params: { type: 'object', properties: { orgId: { type: 'string' } }, required: ['orgId'] },
            body: { type: 'object', properties: { name: { type: 'string', minLength: 2 } }, required: ['name'] },
            response: {
              201: { type: 'object', properties: { orgId: { type: 'string' }, name: { type: 'string' } } },
            },
            tags: ['Projects'],
          },
          preHandler: async (_context, args) => void trace.push(`proc:preHandler:${args.body.name}`),
          handler: async (_context, { params, body }) => {
            trace.push('handler');
            return { orgId: params.orgId, name: body.name, secret: 'trimmed by fjs' };
          },
        }),
        secure: procedure({
          http: { method: 'GET', path: '/projects/:id/secure' },
          handler: async () => ({ ok: true }),
        }),
        slow: procedure({
          access: 'public',
          timeout: 20,
          http: { method: 'GET', path: '/projects/:id/slow' },
          handler: () => new Promise(() => {}),
        }),
        login: procedure({
          access: 'public',
          http: { method: 'POST', path: '/login' },
          handler: async (context) => {
            context.client.startSession(undefined, { user: 'ada' });
            return { ok: true };
          },
        }),
        stamped: procedure({
          access: 'public',
          http: { method: 'GET', path: '/projects-stamped' },
          schema: {
            headers: {
              type: 'object',
              properties: { 'x-app-version': { type: 'string' } },
              required: ['x-app-version'],
            },
          },
          handler: async (context) => ({ v: context.meta.headers['x-app-version'] }),
        }),
      },
    },
    {
      validation: { ajv: permissiveAjv },
      hooks: {
        onRequest: async () => void trace.push('router:onRequest'),
        onResponse: async () => void trace.push('router:onResponse'),
        onError: async (_context, error) => void trace.push(`router:onError:${error.code ?? 'none'}`),
      },
    },
  );

test('delegated REST: fastify validates, serializes and answers wire errors', { skip: noFastify }, async (t) => {
  const trace = [];
  const app = fastify({ logger: false });
  t.after(() => app.close());
  await app.register(wrpcFastify, { router: restRouter(trace), logger: false });
  await app.ready();

  await t.test('a valid call: status from http.status, fjs trims the body, hooks ran in order', async () => {
    trace.length = 0;
    const res = await app.inject({ method: 'POST', url: '/api/projects/42', payload: { name: 'Alpha' } });
    assert.strictEqual(res.statusCode, 201);
    assert.deepStrictEqual(res.json(), { orgId: '42', name: 'Alpha' });
    assert.deepStrictEqual(trace, ['router:onRequest', 'proc:preHandler:Alpha', 'handler', 'router:onResponse']);
  });

  await t.test("fastify's schema validation answers the wire error shape with details", async () => {
    trace.length = 0;
    const res = await app.inject({ method: 'POST', url: '/api/projects/42', payload: { name: 'A' } });
    assert.strictEqual(res.statusCode, 400);
    const body = res.json();
    assert.strictEqual(body.code, 400);
    assert.match(body.message, /fewer than 2 characters/);
    assert.strictEqual(body.details.issues.length, 1);
    // wrpc's own validator never ran — fastify's did, once.
    assert.strictEqual(trace.includes('handler'), false);
    assert.strictEqual(trace.at(-1), 'router:onResponse');
  });

  await t.test('access !== public without a session answers 403 before the handler', async () => {
    trace.length = 0;
    const res = await app.inject({ method: 'GET', url: '/api/projects/1/secure' });
    assert.strictEqual(res.statusCode, 403);
    assert.strictEqual(res.json().code, 403);
    assert.strictEqual(trace.includes('handler'), false);
  });

  await t.test('invokeBare keeps the timeout: 408 through the wire error handler and onError', async () => {
    trace.length = 0;
    const res = await app.inject({ method: 'GET', url: '/api/projects/1/slow' });
    assert.strictEqual(res.statusCode, 408);
    assert.strictEqual(res.json().code, 408);
    assert.ok(trace.includes('router:onError:408'));
  });

  await t.test('schema.headers travels verbatim: fastify validates it on the delegated route', async () => {
    // The carve-out is structural — invokeBare runs no wrpc validators — so
    // the ONLY headers check on this path is fastify's own.
    const missing = await app.inject({ method: 'GET', url: '/api/projects-stamped' });
    assert.strictEqual(missing.statusCode, 400);
    assert.match(missing.json().message, /x-app-version/);
    const ok = await app.inject({
      method: 'GET',
      url: '/api/projects-stamped',
      headers: { 'x-app-version': '5.5' },
    });
    assert.strictEqual(ok.statusCode, 200);
    assert.deepStrictEqual(ok.json(), { v: '5.5' });
  });

  await t.test('startSession from a delegated handler sets the cookie on the fastify reply', async () => {
    const res = await app.inject({ method: 'POST', url: '/api/login', payload: {} });
    assert.strictEqual(res.statusCode, 200);
    assert.match(String(res.headers['set-cookie'] ?? ''), /token=/);
  });

  await t.test('the delegated route beats the conventional parametric one; packet mode intact', async () => {
    const packet = {
      type: 'call',
      id: '1',
      method: 'projects/create',
      args: { params: { orgId: 'x' }, query: {}, body: { name: 'Beta' } },
    };
    const res = await app.inject({ method: 'POST', url: '/api', payload: packet });
    assert.strictEqual(res.json().result.name, 'Beta');
  });

  await t.test('the schema fastify sees is the effective one — wrpc error statuses documented', async () => {
    const probe = fastify({ logger: false });
    t.after(() => probe.close());
    const seen = [];
    probe.addHook('onRoute', (route) => {
      if (route.method === 'POST' && route.url === '/api/projects/:orgId') seen.push(route.schema);
    });
    await probe.register(wrpcFastify, { router: restRouter(), logger: false });
    await probe.ready();
    assert.strictEqual(seen.length, 1);
    assert.deepStrictEqual(seen[0].tags, ['Projects']);
    for (const code of ['201', '400', '429', '500', '503']) assert.ok(seen[0].response[code], `response ${code}`);
  });
});

test('delegated REST: the context carries its call identity', { skip: noFastify }, async (t) => {
  const seen = [];
  const router = defineRouter(
    {
      projects: {
        find: procedure({
          access: 'public',
          http: { method: 'GET', path: '/projects/:id' },
          handler: async (context, { params }) => ({ id: params.id, method: context.method }),
        }),
      },
    },
    {
      hooks: {
        onRequest: async (context) => void seen.push([context.method, context.procedure]),
      },
    },
  );
  const app = fastify({ logger: false });
  t.after(() => app.close());
  await app.register(wrpcFastify, { router, logger: false });
  await app.ready();
  const res = await app.inject({ method: 'GET', url: '/api/projects/7' });
  assert.strictEqual(res.statusCode, 200);
  assert.deepStrictEqual(res.json(), { id: '7', method: 'projects/find' });
  assert.strictEqual(seen.length, 1);
  assert.strictEqual(seen[0][0], 'projects/find');
  assert.strictEqual(seen[0][1], router.getProcedure('projects', undefined, 'find'));
});

test('delegated REST: rest.version registers the prefixed url', { skip: noFastify }, async (t) => {
  const seen = [];
  const router = defineRouter(
    {
      'auth.v1': {
        signIn: procedure({
          access: 'public',
          http: { method: 'POST', path: '/auth/signIn' },
          handler: async (context) => ({ method: context.method }),
        }),
      },
    },
    {
      rest: { version: 'path' },
      hooks: { onRequest: async (context) => void seen.push(context.method) },
    },
  );
  const app = fastify({ logger: false });
  t.after(() => app.close());
  await app.register(wrpcFastify, { router, logger: false });
  await app.ready();
  const res = await app.inject({ method: 'POST', url: '/api/v1/auth/signIn', payload: {} });
  assert.strictEqual(res.statusCode, 200);
  assert.deepStrictEqual(
    res.json(),
    { method: 'auth.v1/signIn' },
    'the delegated context carries the versioned target',
  );
  assert.deepStrictEqual(seen, ['auth.v1/signIn']);
});

test('codec.rest and delegated REST routes refuse each other loudly', { skip: noFastify }, async (t) => {
  const restCodec = { rest: { encode: (value) => JSON.stringify(value), decode: (body) => JSON.parse(String(body)) } };
  const mapped = () =>
    defineRouter({
      projects: {
        find: procedure({ access: 'public', http: { method: 'GET', path: '/projects/:id' }, handler: async () => 1 }),
      },
    });

  await t.test('codec.rest + http mappings throws at ready', async () => {
    const app = fastify({ logger: false });
    t.after(() => app.close().catch(() => {}));
    app.register(wrpcFastify, { router: mapped(), logger: false, codec: restCodec });
    await assert.rejects(() => app.ready(), /codec\.rest and delegated REST routes .* are mutually exclusive/);
  });

  await t.test('the options.rpc path is covered by the same check', async () => {
    const app = fastify({ logger: false });
    t.after(() => app.close().catch(() => {}));
    const rpc = new RpcServer({ router: mapped(), logger: false, codec: restCodec });
    app.register(wrpcFastify, { rpc, logger: false });
    await assert.rejects(() => app.ready(), /mutually exclusive/);
  });

  await t.test('codec.rest without http mappings registers normally', async () => {
    const app = fastify({ logger: false });
    t.after(() => app.close());
    await app.register(wrpcFastify, { router: createRouter(), logger: false, codec: restCodec });
    await app.ready();
    const injected = await app.inject({
      method: 'POST',
      url: '/api',
      payload: { type: 'call', id: '1', method: 'probe/echo', args: { a: 1 } },
    });
    assert.strictEqual(injected.statusCode, 200);
  });
});

test('delegated REST: restErrors "app" leaves the error shape to the app', { skip: noFastify }, async (t) => {
  const app = fastify({ logger: false });
  t.after(() => app.close());
  await app.register(wrpcFastify, { router: restRouter(), logger: false, restErrors: 'app' });
  await app.ready();
  const res = await app.inject({ method: 'GET', url: '/api/projects/1/secure' });
  assert.strictEqual(res.statusCode, 403);
  // fastify's default error body, not wrpc's wire shape
  assert.strictEqual(res.json().error, 'Forbidden');
});

// ---------------------------------------------------------------------------
// Reverse engineering: the app's own routes become wrpc procedures,
// dispatched through fastify.inject so the route's whole pipeline runs.

const { defaultName, defaultUnit, routeSignature } = require('../../src/adapters/mirror.js');

test('mirror naming: reverse REST semantics', () => {
  const knownUnits = new Set(['projects']);
  const name = (method, url) => {
    const segments = url.split('/').filter((s) => s.length > 0);
    const named = defaultUnit(segments, knownUnits);
    return `${named.unit}/${defaultName(method, segments.slice(named.tailIndex))}`;
  };
  assert.strictEqual(name('POST', '/projects'), 'projects/create');
  assert.strictEqual(name('POST', '/workspace/projects/:orgId'), 'projects/create');
  assert.strictEqual(name('GET', '/workspace/projects'), 'projects/findAll');
  assert.strictEqual(name('GET', '/workspace/projects/:id'), 'projects/findById');
  assert.strictEqual(name('GET', '/workspace/projects/slug/:slug'), 'projects/findBySlug');
  assert.strictEqual(name('GET', '/workspace/projects/archive'), 'projects/findAllArchive');
  assert.strictEqual(name('PATCH', '/projects/:id'), 'projects/update');
  assert.strictEqual(name('PUT', '/projects/:id'), 'projects/replace');
  assert.strictEqual(name('DELETE', '/projects/:id'), 'projects/delete');
  assert.strictEqual(name('POST', '/projects/:orgId/archive/:id'), 'projects/createArchive');
  assert.strictEqual(name('DELETE', '/org-users/:id'), 'orgUsers/delete');
  assert.strictEqual(defaultUnit([':id']), null);
});

test('mirror signature: JSON Schema distilled into the closed format', () => {
  const signature = routeSignature({
    params: { type: 'object', properties: { id: { type: 'string' } }, required: ['id'] },
    body: {
      type: 'object',
      properties: { name: { type: 'string' }, tags: { type: 'array', items: { type: 'string' } } },
      required: ['name'],
    },
    response: { 200: { type: 'object', properties: { id: { type: 'string' }, count: { type: 'integer' } } } },
  });
  assert.deepStrictEqual(signature, {
    args: {
      params: { id: 'string' },
      body: { name: 'string', 'tags?': ['string'] },
    },
    returns: { 'id?': 'string', 'count?': 'number' },
  });
  // Anything inexpressible degrades to 'unknown', never a guess.
  assert.deepStrictEqual(routeSignature({ body: { oneOf: [] } }), { args: { body: 'unknown' } });
  assert.strictEqual(routeSignature(null), null);
});

test('mirror: app routes become callable wrpc procedures', { skip: noFastify }, async (t) => {
  const app = fastify({ logger: false });
  t.after(() => app.close());
  const router = defineRouter({ own: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } });
  await app.register(wrpcFastify, { router, logger: false, mirror: { access: 'public' } });

  const seenAuth = [];
  app.addHook('onRequest', async (req) => void seenAuth.push(req.headers['x-auth'] ?? null));
  app.post(
    '/workspace/projects/:orgId',
    {
      schema: {
        body: { type: 'object', properties: { name: { type: 'string', minLength: 2 } }, required: ['name'] },
        response: { 200: { type: 'object', properties: { name: { type: 'string' }, orgId: { type: 'string' } } } },
      },
    },
    async (req) => ({ name: req.body.name, orgId: req.params.orgId, secret: 'trimmed' }),
  );
  app.get('/workspace/projects/slug/:slug', async (req) => ({ slug: req.params.slug }));
  app.get('/workspace/projects', async (req) => ({ q: req.query }));
  app.get('/boom/:id', async (req, reply) =>
    reply.code(404).send({ message: 'nope', code: 404, details: { id: req.params.id } }),
  );
  app.route({
    method: 'GET',
    url: '/named',
    config: { wrpc: { unit: 'misc', name: 'custom' } },
    handler: async () => ({ ok: 1 }),
  });
  app.route({ method: 'GET', url: '/hidden', config: { wrpc: false }, handler: async () => ({}) });

  await app.listen({ host: '127.0.0.1', port: 0 });
  const port = app.server.address().port;
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false, reconnect: false });
  t.after(() => void client.close());

  await t.test('the full route pipeline runs: hooks, validation, serialization', async () => {
    await client.load('projects');
    const created = await client.api.projects.create({ params: { orgId: '7' }, body: { name: 'Beta' } });
    assert.deepStrictEqual(created, { name: 'Beta', orgId: '7' }); // fjs trimmed `secret`
    await assert.rejects(client.api.projects.create({ params: { orgId: '7' }, body: { name: 'B' } }), (error) => {
      assert.strictEqual(error.code, 400);
      return true;
    });
  });

  await t.test('reverse naming and the query leg', async () => {
    assert.deepStrictEqual(await client.api.projects.findBySlug({ params: { slug: 'alpha' } }), { slug: 'alpha' });
    assert.deepStrictEqual(await client.api.projects.findAll({ query: { x: '1' } }), { q: { x: '1' } });
  });

  await t.test('route errors flow through with status, message and details', async () => {
    await client.load('boom');
    await assert.rejects(client.api.boom.findById({ params: { id: 'zz' } }), (error) => {
      assert.strictEqual(error.code, 404);
      assert.strictEqual(error.message, 'nope');
      assert.deepStrictEqual(error.details, { id: 'zz' });
      return true;
    });
  });

  await t.test('config.wrpc: object renames, false hides, wrpc routes never mirror', async () => {
    await client.load('misc');
    assert.deepStrictEqual(await client.api.misc.custom(), { ok: 1 });
    // 'hidden' is opted out entirely — not even introspectable.
    const res = await fetch(`http://127.0.0.1:${port}/api/system/introspect`);
    const { result } = await res.json();
    assert.strictEqual('hidden' in result, false);
  });

  await t.test('the headers option maps the wrpc context into the injected request', async () => {
    // A second app whose mirror stamps a header from the context.
    const app2 = fastify({ logger: false });
    t.after(() => app2.close());
    await app2.register(wrpcFastify, {
      router: defineRouter({}),
      logger: false,
      mirror: { access: 'public', headers: (context) => ({ 'x-auth': `ctx-${typeof context.uuid}` }) },
    });
    const stamped = [];
    app2.get('/things', async (req) => {
      stamped.push(req.headers['x-auth']);
      return { ok: true };
    });
    await app2.listen({ host: '127.0.0.1', port: 0 });
    const port2 = app2.server.address().port;
    const client2 = await WrpcClient.connect(`ws://127.0.0.1:${port2}/api`, { heartbeat: false, reconnect: false });
    t.after(() => void client2.close());
    await client2.load('things');
    await client2.api.things.findAll();
    assert.deepStrictEqual(stamped, ['ctx-string']);
  });

  await t.test('mirrored signatures reach introspection', async () => {
    const res = await fetch(`http://127.0.0.1:${port}/api/system/introspect`);
    const packet = await res.json();
    assert.deepStrictEqual(packet.result.projects.create.signature.args.body, { name: 'string' });
    assert.deepStrictEqual(packet.result.projects.create.meta.mirrored, {
      method: 'POST',
      path: '/workspace/projects/:orgId',
    });
  });

  await t.test('the wrpc router the app declared is untouched', async () => {
    await client.load('own');
    assert.strictEqual(await client.api.own.ping(), 'pong');
  });
});

test('mirror: a naming collision throws at onReady with a hint', { skip: noFastify }, async (t) => {
  const app = fastify({ logger: false });
  t.after(() => app.close());
  await app.register(wrpcFastify, { router: defineRouter({}), logger: false, mirror: true });
  app.get('/things/:id', async () => ({}));
  app.get('/things/special', async () => ({}));
  app.get('/other/things/special', async () => ({}));
  await assert.rejects(app.ready(), /things\/findAllSpecial.*config\.wrpc\.name/s);
});
