'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const crypto = require('node:crypto');
const { EventEmitter } = require('node:events');

const { Server, defineRouter, procedure } = require('../../index.js');
const { ProtocolClient } = require('../websocket/protocolClient.js');

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const COOKIE_RE = /^token=([^;]+); Path=\/; HttpOnly; Secure; SameSite=Lax$/;

const createRouter = () =>
  defineRouter({
    echo: {
      args: procedure({ access: 'public', handler: async (_context, args) => args }),
    },
    auth: {
      login: procedure({
        access: 'public',
        handler: async (context, args) => {
          context.client.startSession(undefined, { user: args.user });
          return { ok: true };
        },
      }),
      whoami: procedure({
        access: 'session',
        handler: async (context) => ({ user: context.session.state.user }),
      }),
      logout: procedure({
        access: 'session',
        handler: async (context) => ({ ok: await context.client.finalizeSession() }),
      }),
    },
  });

const startServer = async (t, options = {}) => {
  const server = new Server({
    router: createRouter(),
    protocol: 'http',
    host: '127.0.0.1',
    port: 0,
    logger: false,
    ...options,
  });
  await server.listen();
  const { port } = server.httpServer.address();
  t.after(() => server.close());
  return { server, port, origin: `http://127.0.0.1:${port}` };
};

const postPacket = (url, method, args, headers = {}) =>
  fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify({ type: 'call', id: '1', method, args }),
  });

const login = async (origin, base, user) => {
  const res = await postPacket(`${origin}${base}`, 'auth/login', { user });
  assert.strictEqual(res.status, 200);
  const cookies = res.headers.getSetCookie();
  assert.strictEqual(cookies.length, 1);
  const match = cookies[0].match(COOKIE_RE);
  assert.ok(match, `Unexpected session cookie: ${cookies[0]}`);
  return match[1];
};

test('basePath default /api: packet POST endpoint', async (t) => {
  const { origin } = await startServer(t);
  const res = await postPacket(`${origin}/api`, 'echo/args', { a: 1, b: 'two' });
  assert.strictEqual(res.status, 200);
  assert.strictEqual(res.headers.get('content-type'), 'application/json');
  const packet = await res.json();
  assert.deepStrictEqual(packet, { type: 'callback', id: '1', result: { a: 1, b: 'two' } });
});

test('basePath default /api: REST GET with query args', async (t) => {
  const { origin } = await startServer(t);
  const res = await fetch(`${origin}/api/echo/args?a=1&b=two`);
  assert.strictEqual(res.status, 200);
  const packet = await res.json();
  assert.strictEqual(packet.type, 'callback');
  assert.match(packet.id, UUID_RE);
  assert.deepStrictEqual(packet.result, { a: '1', b: 'two' });
});

test('basePath default /api: path outside basePath is 404', async (t) => {
  const { origin } = await startServer(t);
  const res = await fetch(`${origin}/nope`);
  assert.strictEqual(res.status, 404);
  const packet = await res.json();
  assert.deepStrictEqual(packet, {
    type: 'callback',
    id: '',
    error: { message: 'Not Found', code: 404 },
  });
});

test('custom basePath /rpc: packet and REST move, /api becomes 404', async (t) => {
  const { origin } = await startServer(t, { basePath: '/rpc' });

  const packetRes = await postPacket(`${origin}/rpc`, 'echo/args', { x: 1 });
  assert.strictEqual(packetRes.status, 200);
  assert.deepStrictEqual((await packetRes.json()).result, { x: 1 });

  const restRes = await fetch(`${origin}/rpc/echo/args?x=2`);
  assert.strictEqual(restRes.status, 200);
  assert.deepStrictEqual((await restRes.json()).result, { x: '2' });

  const missRes = await postPacket(`${origin}/api`, 'echo/args', { x: 3 });
  assert.strictEqual(missRes.status, 404);
});

test("basePath '': packet at / and REST at /unit/method", async (t) => {
  const { origin } = await startServer(t, { basePath: '' });

  const packetRes = await postPacket(`${origin}/`, 'echo/args', { q: 1 });
  assert.strictEqual(packetRes.status, 200);
  assert.deepStrictEqual((await packetRes.json()).result, { q: 1 });

  const restRes = await fetch(`${origin}/echo/args?q=2`);
  assert.strictEqual(restRes.status, 200);
  assert.deepStrictEqual((await restRes.json()).result, { q: '2' });
});

test('packet endpoint: non-POST method is 403', async (t) => {
  const { origin } = await startServer(t);
  const res = await fetch(`${origin}/api`);
  assert.strictEqual(res.status, 403);
  const packet = await res.json();
  assert.deepStrictEqual(packet, {
    type: 'callback',
    id: '',
    error: { message: 'Forbidden', code: 403 },
  });
});

test('OPTIONS preflight: 200 with CORS headers, no body processing', async (t) => {
  const { origin } = await startServer(t);
  const res = await fetch(`${origin}/api`, { method: 'OPTIONS', body: 'not-json-at-all' });
  assert.strictEqual(res.status, 200);
  assert.strictEqual(res.headers.get('access-control-allow-origin'), '*');
  assert.strictEqual(res.headers.get('access-control-allow-methods'), 'POST, GET, OPTIONS');
  assert.strictEqual(
    res.headers.get('access-control-allow-headers'),
    'Content-Type, x-wrpc-channel, last-event-id, x-wrpc-meta',
    'the SSE transport sends two headers a preflight has to name explicitly',
  );
  assert.strictEqual(await res.text(), '');

  const anywhere = await fetch(`${origin}/definitely/not/an/api/path`, { method: 'OPTIONS' });
  assert.strictEqual(anywhere.status, 200);
  assert.strictEqual(anywhere.headers.get('access-control-allow-origin'), '*');
});

test('cors: no cors option means wildcard ACAO on responses', async (t) => {
  const { origin } = await startServer(t);
  const res = await postPacket(`${origin}/api`, 'echo/args', {}, { Origin: 'http://anything.example' });
  assert.strictEqual(res.status, 200);
  assert.strictEqual(res.headers.get('access-control-allow-origin'), '*');
  assert.strictEqual(res.headers.get('vary'), null);
});

test('cors origins list: allowed origin echoed with Vary, disallowed gets no ACAO', async (t) => {
  const { origin } = await startServer(t, { cors: { origins: ['http://app.example'] } });

  const allowed = await postPacket(`${origin}/api`, 'echo/args', {}, { Origin: 'http://app.example' });
  assert.strictEqual(allowed.status, 200);
  assert.strictEqual(allowed.headers.get('access-control-allow-origin'), 'http://app.example');
  assert.strictEqual(allowed.headers.get('vary'), 'Origin');

  // Refused outright, not merely denied the header: the page could not read
  // the answer either way, but the call itself must not run cross-site.
  const denied = await postPacket(`${origin}/api`, 'echo/args', {}, { Origin: 'http://evil.example' });
  assert.strictEqual(denied.status, 403);
  assert.strictEqual(denied.headers.get('access-control-allow-origin'), null);
  assert.strictEqual(denied.headers.get('vary'), 'Origin');
});

test('cors origins function decides per origin', async (t) => {
  const origins = (origin) => origin.endsWith('.good.example');
  const { origin } = await startServer(t, { cors: { origins } });

  const allowed = await postPacket(`${origin}/api`, 'echo/args', {}, { Origin: 'http://a.good.example' });
  assert.strictEqual(allowed.headers.get('access-control-allow-origin'), 'http://a.good.example');

  const denied = await postPacket(`${origin}/api`, 'echo/args', {}, { Origin: 'http://bad.example' });
  assert.strictEqual(denied.headers.get('access-control-allow-origin'), null);
});

test('cors credentials: ACAC true only for allowed origins', async (t) => {
  const cors = { origins: ['http://app.example'], credentials: true };
  const { origin } = await startServer(t, { cors });

  const allowed = await postPacket(`${origin}/api`, 'echo/args', {}, { Origin: 'http://app.example' });
  assert.strictEqual(allowed.headers.get('access-control-allow-credentials'), 'true');

  const denied = await postPacket(`${origin}/api`, 'echo/args', {}, { Origin: 'http://evil.example' });
  assert.strictEqual(denied.headers.get('access-control-allow-credentials'), null);
  assert.strictEqual(denied.headers.get('access-control-allow-origin'), null);
});

test('cors: custom headers and methods strings are used verbatim', async (t) => {
  const cors = { headers: 'Content-Type, X-Custom', methods: 'POST, OPTIONS' };
  const { origin } = await startServer(t, { cors });
  const res = await fetch(`${origin}/api`, { method: 'OPTIONS' });
  assert.strictEqual(res.status, 200);
  assert.strictEqual(res.headers.get('access-control-allow-headers'), 'Content-Type, X-Custom');
  assert.strictEqual(res.headers.get('access-control-allow-methods'), 'POST, OPTIONS');
  assert.strictEqual(res.headers.get('access-control-allow-origin'), '*');
});

test('REST args: query params merged with JSON body, body wins', async (t) => {
  const { origin } = await startServer(t);
  const res = await fetch(`${origin}/api/echo/args?a=1&b=query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ b: 'body', c: 3 }),
  });
  assert.strictEqual(res.status, 200);
  const packet = await res.json();
  assert.deepStrictEqual(packet.result, { a: '1', b: 'body', c: 3 });
});

test('http session e2e: login sets cookie, cookie restores, finalize invalidates', async (t) => {
  const { origin } = await startServer(t);
  const token = await login(origin, '/api', 'alice');
  assert.match(token, UUID_RE);

  const denied = await postPacket(`${origin}/api`, 'auth/whoami', {});
  assert.strictEqual(denied.status, 403);

  const whoRes = await postPacket(`${origin}/api`, 'auth/whoami', {}, { Cookie: `token=${token}` });
  assert.strictEqual(whoRes.status, 200);
  assert.deepStrictEqual((await whoRes.json()).result, { user: 'alice' });

  const logoutRes = await postPacket(`${origin}/api`, 'auth/logout', {}, { Cookie: `token=${token}` });
  assert.strictEqual(logoutRes.status, 200);
  assert.deepStrictEqual((await logoutRes.json()).result, { ok: true });

  const stale = await postPacket(`${origin}/api`, 'auth/whoami', {}, { Cookie: `token=${token}` });
  assert.strictEqual(stale.status, 403);
});

test('sessions option: custom cookie name is set and restored', async (t) => {
  const { origin } = await startServer(t, { sessions: { cookie: { name: 'sid' } } });

  const loginRes = await postPacket(`${origin}/api`, 'auth/login', { user: 'carol' });
  assert.strictEqual(loginRes.status, 200);
  const cookies = loginRes.headers.getSetCookie();
  assert.strictEqual(cookies.length, 1);
  const match = cookies[0].match(/^sid=([^;]+); Path=\/; HttpOnly; Secure; SameSite=Lax$/);
  assert.ok(match, `Unexpected session cookie: ${cookies[0]}`);
  const token = match[1];

  const whoRes = await postPacket(`${origin}/api`, 'auth/whoami', {}, { Cookie: `sid=${token}` });
  assert.strictEqual(whoRes.status, 200);
  assert.deepStrictEqual((await whoRes.json()).result, { user: 'carol' });

  const wrongName = await postPacket(`${origin}/api`, 'auth/whoami', {}, { Cookie: `token=${token}` });
  assert.strictEqual(wrongName.status, 403);
});

// WrpcSocket-shaped fake: enough surface for RpcServer.attachSocket and
// ServerWsTransport (on/send/terminate/remoteAddress).
class FakeWsSocket extends EventEmitter {
  remoteAddress = '127.0.0.1';
  #sent = [];
  #waiters = [];

  send(data) {
    const text = String(data);
    const waiter = this.#waiters.shift();
    if (waiter) waiter(text);
    else this.#sent.push(text);
    return true;
  }

  nextSent() {
    if (this.#sent.length > 0) return Promise.resolve(this.#sent.shift());
    return new Promise((resolve) => this.#waiters.push(resolve));
  }

  terminate() {
    this.emit('close');
  }
}

test('ws attach: cookie in upgrade headers restores the http-started session', async (t) => {
  const { server, origin } = await startServer(t);
  const token = await login(origin, '/api', 'bob');

  const socket = new FakeWsSocket();
  const client = server.rpc.attachSocket(socket, { headers: { cookie: `token=${token}` } });
  const restored = await client.sessionReady;
  assert.strictEqual(restored, true);
  assert.ok(client.session);
  assert.strictEqual(client.session.token, token);
  assert.strictEqual(client.session.state.user, 'bob');

  const call = { type: 'call', id: '7', method: 'auth/whoami', args: {} };
  socket.emit('message', Buffer.from(JSON.stringify(call)), false);
  const reply = JSON.parse(await socket.nextSent());
  assert.deepStrictEqual(reply, { type: 'callback', id: '7', result: { user: 'bob' } });
});

test('ws attach: no cookie means no session, session-access call gets 403', async (t) => {
  const { server } = await startServer(t);
  const socket = new FakeWsSocket();
  const client = server.rpc.attachSocket(socket, { headers: {} });
  const restored = await client.sessionReady;
  assert.strictEqual(restored, false);
  assert.strictEqual(client.session, null);

  const call = { type: 'call', id: '8', method: 'auth/whoami', args: {} };
  socket.emit('message', Buffer.from(JSON.stringify(call)), false);
  const reply = JSON.parse(await socket.nextSent());
  assert.deepStrictEqual(reply, {
    type: 'callback',
    id: '8',
    error: { message: 'Forbidden', code: 403 },
  });
});

test('ws attach: client.meta snapshots the upgrade (frozen, null-proto headers)', async (t) => {
  const { server } = await startServer(t);
  const socket = new FakeWsSocket();
  const client = server.rpc.attachSocket(socket, {
    headers: { 'x-app-version': '2.1.0', cookie: 'token=nope' },
    url: '/api?x=1',
    remoteAddress: '10.0.0.7',
  });
  assert.strictEqual(client.meta.url, '/api?x=1');
  assert.strictEqual(client.meta.remoteAddress, '10.0.0.7');
  assert.strictEqual(client.meta.headers['x-app-version'], '2.1.0');
  assert.ok(Object.isFrozen(client.meta) && Object.isFrozen(client.meta.headers) && Object.isFrozen(client.meta.data));
  // Null-prototyped: a header named like an Object.prototype member must
  // answer undefined, not a function.
  assert.strictEqual(client.meta.headers.toString, undefined);
  assert.strictEqual(Object.getPrototypeOf(client.meta.headers), null);
  // A bare attach still carries the complete (empty) shape.
  const bare = server.rpc.attachSocket(new FakeWsSocket(), {});
  assert.strictEqual(bare.meta.url, '');
  assert.strictEqual(bare.meta.remoteAddress, '127.0.0.1');
  assert.strictEqual(bare.meta.protocol, '');
  assert.deepStrictEqual(Object.keys(bare.meta.headers), []);
  assert.deepStrictEqual(Object.keys(bare.meta.data), []);
});

const wrpcH = (value) => `wrpc_h=${encodeURIComponent(JSON.stringify(value))}`;

test('ws attach: declared headers (wrpc_h) merge UNDER the observed ones', async (t) => {
  const { server } = await startServer(t);
  const declared = {
    'X-App-Version': '1.2.3', // lowercased on the way in
    'x-thing': 'declared', // observed value must win
    cookie: 'token=forged', // reserved: cannot be spoofed through the URL
    'x-wrpc-channel': 'forged', // reserved prefix
    'Sec-Fetch-Site': 'same-origin', // reserved prefix
    num: 5, // not a string: dropped
    nested: { a: 1 }, // not a string: dropped
    __proto__: { polluted: 1 }, // never carried over
  };
  const socket = new FakeWsSocket();
  const client = server.rpc.attachSocket(socket, {
    headers: { cookie: 'token=real', 'x-thing': 'observed' },
    url: `/api?${wrpcH(declared)}`,
  });
  assert.strictEqual(client.meta.headers['x-app-version'], '1.2.3');
  assert.strictEqual(client.meta.headers['x-thing'], 'observed');
  assert.strictEqual(client.meta.headers.cookie, 'token=real');
  assert.strictEqual(client.meta.headers['x-wrpc-channel'], undefined);
  assert.strictEqual(client.meta.headers['sec-fetch-site'], undefined);
  assert.strictEqual(client.meta.headers.num, undefined);
  assert.strictEqual(client.meta.headers.nested, undefined);
  assert.strictEqual({}.polluted, undefined, 'Object.prototype survived');
  assert.strictEqual(Object.hasOwn(client.meta.headers, '__proto__'), false);
});

test('ws attach: malformed or oversize wrpc_h is refused, never fatal', async (t) => {
  const { server } = await startServer(t, { metaMaxBytes: 64 });
  const attach = (url) => server.rpc.attachSocket(new FakeWsSocket(), { headers: { 'x-obs': 'kept' }, url });
  const cases = [
    `/api?${wrpcH({ pad: 'x'.repeat(200) })}`, // over the cap (encoded length)
    '/api?wrpc_h=not-json', // malformed JSON
    `/api?${wrpcH(['a', 'b'])}`, // an array
    `/api?${wrpcH(7)}`, // a bare number
    '/api?wrpc_h=%', // a broken percent sequence
    '/api?other=1', // no parameter at all
  ];
  for (const url of cases) {
    const client = attach(url);
    // The label is refused; the connection and the observed headers are not.
    assert.strictEqual(client.meta.headers['x-obs'], 'kept', url);
    assert.deepStrictEqual(Object.keys(client.meta.headers), ['x-obs'], `nothing declared survives for ${url}`);
  }
});

test('onConnect: client.sessionReady is assigned before the hooks run (and awaiting it does not deadlock)', async (t) => {
  const seen = [];
  const router = createRouter();
  router.addHook('onConnect', async (client) => {
    // The documented recipe: the hook awaits the restore. Before the fix it
    // awaited the constructor's resolved default and saw session === null.
    const restored = await client.sessionReady;
    seen.push([restored, client.session?.state?.user ?? null]);
  });
  const { server, origin } = await startServer(t, { router });
  const token = await login(origin, '/api', 'dana');

  const socket = new FakeWsSocket();
  const client = server.rpc.attachSocket(socket, { headers: { cookie: `token=${token}` } });
  await client.ready;
  // onConnect fires for the login POST's per-request client too; the ws
  // attach is the last entry, and it must see the restored session.
  assert.deepStrictEqual(seen.at(-1), [true, 'dana']);
});

test('ws attach: a call racing onConnect is dispatched only after the hooks settled', async (t) => {
  const router = defineRouter({
    probe: {
      rooms: procedure({ access: 'public', handler: async (context) => [...context.client.rooms] }),
    },
  });
  router.addHook('onConnect', async (client) => {
    await new Promise((resolve) => setTimeout(resolve, 20));
    client.join('lobby');
  });
  const { server } = await startServer(t, { router });
  const socket = new FakeWsSocket();
  server.rpc.attachSocket(socket, { headers: {} });
  // Sent in the same tick as the attach — the exact race client.ready closes.
  const call = { type: 'call', id: '9', method: 'probe/rooms', args: {} };
  socket.emit('message', Buffer.from(JSON.stringify(call)), false);
  const reply = JSON.parse(await socket.nextSent());
  assert.deepStrictEqual(reply, { type: 'callback', id: '9', result: ['lobby'] });
});

test('packet POST: dispatch waits for client.ready (session restore plus onConnect hooks)', async (t) => {
  const router = defineRouter({
    probe: {
      stamp: procedure({ access: 'public', handler: async (context) => context.client.data.stamp ?? null }),
    },
  });
  router.addHook('onConnect', async (client) => {
    await new Promise((resolve) => setTimeout(resolve, 20));
    client.data.stamp = 'hooked';
  });
  const { origin } = await startServer(t, { router });
  const res = await postPacket(`${origin}/api`, 'probe/stamp', {});
  const packet = await res.json();
  assert.deepStrictEqual(packet, { type: 'callback', id: '1', result: 'hooked' });
});

const wsHeaders = (extra = {}) => ({
  Upgrade: 'websocket',
  Connection: 'Upgrade',
  'Sec-WebSocket-Key': crypto.randomBytes(16).toString('base64'),
  'Sec-WebSocket-Version': '13',
  ...extra,
});

const statusOf = (result) => parseInt(result.statusLine.split(' ')[1], 10);

test('ws upgrade: cors origins gate the handshake', async (t) => {
  const { port } = await startServer(t, { cors: { origins: ['http://ok.example'] } });

  const denied = await ProtocolClient.attemptHandshake({
    host: '127.0.0.1',
    port,
    path: '/api',
    headers: wsHeaders({ Origin: 'http://evil.example' }),
  });
  assert.strictEqual(statusOf(denied), 403);

  const allowed = await ProtocolClient.attemptHandshake({
    host: '127.0.0.1',
    port,
    path: '/api',
    headers: wsHeaders({ Origin: 'http://ok.example' }),
  });
  assert.strictEqual(statusOf(allowed), 101);

  const anonymous = await ProtocolClient.attemptHandshake({
    host: '127.0.0.1',
    port,
    path: '/api',
    headers: wsHeaders(),
  });
  assert.strictEqual(statusOf(anonymous), 101);
});

test('security regressions: CSRF gate, request cleanup, HTTP stream rejection', async (t) => {
  await t.test('cross-site GET does not get the cookie session (CSRF)', async (sub) => {
    const { origin } = await startServer(sub);
    const token = await login(origin, '/api', 'victim');
    const cookie = `token=${token}`;

    // A cross-site top-level navigation carries the Lax cookie...
    const crossSite = await fetch(`${origin}/api/auth/whoami`, {
      headers: { cookie, 'sec-fetch-site': 'cross-site', 'sec-fetch-mode': 'navigate' },
    });
    assert.strictEqual((await crossSite.json()).error.code, 403);

    // ...while a same-origin fetch from the app still works
    const sameOrigin = await fetch(`${origin}/api/auth/whoami`, {
      headers: { cookie, 'sec-fetch-site': 'same-origin' },
    });
    assert.deepStrictEqual((await sameOrigin.json()).result, { user: 'victim' });

    // non-browser peers (no Fetch metadata) keep working
    const bare = await fetch(`${origin}/api/auth/whoami`, { headers: { cookie } });
    assert.deepStrictEqual((await bare.json()).result, { user: 'victim' });

    // an unsafe method is not ambient-authority reachable, so it keeps the session
    const post = await fetch(`${origin}/api/auth/whoami`, {
      method: 'POST',
      headers: { cookie, 'sec-fetch-site': 'cross-site' },
    });
    assert.deepStrictEqual((await post.json()).result, { user: 'victim' });
  });

  await t.test('stream packets over HTTP are rejected instead of hanging', async (sub) => {
    const { server, origin } = await startServer(sub);
    const res = await fetch(`${origin}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'stream', id: 'x', name: 'u', size: 10 }),
    });
    assert.strictEqual(res.status, 400);
    const packet = await res.json();
    assert.match(packet.error.message, /persistent connection/);
    // the client was evicted once the response was written
    assert.strictEqual(server.rpc.clients.size, 0);
  });

  await t.test('an aborted request evicts its client', async (sub) => {
    const { server, origin } = await startServer(sub, {
      router: defineRouter({
        slow: {
          wait: procedure({
            access: 'public',
            handler: () => new Promise((resolve) => setTimeout(() => resolve('late'), 3000)),
          }),
        },
      }),
    });
    const controller = new AbortController();
    const pending = fetch(`${origin}/api/slow/wait`, { signal: controller.signal }).catch(() => null);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.strictEqual(server.rpc.clients.size, 1);
    controller.abort();
    await pending;
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.strictEqual(server.rpc.clients.size, 0);
  });
});

test('maxBodySize on the built-in Server refuses an oversized body', async (t) => {
  const { origin } = await startServer(t, { maxBodySize: 256 });
  const res = await postPacket(`${origin}/api`, 'echo/args', { blob: 'x'.repeat(1024) });
  assert.strictEqual(res.status, 400);
  const body = await res.json();
  assert.match(body.error.message, /Body size limit exceeded/);
});

test('introspection option', async (t) => {
  const probe = (origin) =>
    postPacket(`${origin}/api`, 'system/introspect', ['echo']).then(async (res) => (await res.json()).error?.code);

  await t.test('false leaves the API surface unadvertised', async () => {
    const { origin } = await startServer(t, { introspection: false });
    assert.strictEqual(await probe(origin), 404);
  });

  await t.test("'session' gates it behind a session", async () => {
    const { origin } = await startServer(t, { introspection: 'session' });
    assert.strictEqual(await probe(origin), 403);
  });

  await t.test('true (the default) keeps it public', async () => {
    const { origin } = await startServer(t);
    assert.strictEqual(await probe(origin), undefined);
  });
});

test('x-wrpc-meta: a REST caller passes per-request meta; connection meta.data doubles as callMeta', async (t) => {
  const router = defineRouter({
    probe: {
      peek: procedure({
        access: 'public',
        handler: async (context) => ({ call: { ...context.callMeta }, data: { ...context.meta.data } }),
      }),
    },
  });
  const { origin } = await startServer(t, { router });
  const meta = encodeURIComponent(JSON.stringify({ idem: '9f3c', v: '1.2.3' }));
  // The conventional REST mode: one GET, the header carries the meta.
  const res = await fetch(`${origin}/api/probe/peek`, { headers: { 'x-wrpc-meta': meta } });
  const { result } = await res.json();
  assert.deepStrictEqual(result.call, { idem: '9f3c', v: '1.2.3' });
  assert.deepStrictEqual(result.data, { idem: '9f3c', v: '1.2.3' });
  // Without the header both bags read empty, not null.
  const bare = await fetch(`${origin}/api/probe/peek`);
  const { result: none } = await bare.json();
  assert.deepStrictEqual(none, { call: {}, data: {} });
});

test('ws attach: the wrpc_meta query lands on client.meta.data', async (t) => {
  const { server } = await startServer(t);
  const declared = encodeURIComponent(JSON.stringify({ v: '2.0', locale: 'de-CH' }));
  const socket = new FakeWsSocket();
  const client = server.rpc.attachSocket(socket, { headers: {}, url: `/api?wrpc_meta=${declared}` });
  assert.deepStrictEqual({ ...client.meta.data }, { v: '2.0', locale: 'de-CH' });
  assert.ok(Object.isFrozen(client.meta.data));
});

test('sessions.transport: a bearer strategy restores from Authorization and skips the CSRF rule', async (t) => {
  const bearer = {
    ambient: false,
    read: ({ headers }) => {
      const value = headers?.authorization;
      return typeof value === 'string' && value.startsWith('Bearer ') ? value.slice(7) : null;
    },
    write: () => null, // a server cannot SEND Authorization: the handler returns tokens
  };
  const router = defineRouter({
    auth: {
      login: procedure({
        access: 'public',
        handler: async (context) => {
          context.client.startSession(undefined, { user: 'bea' });
          return { token: context.session.token };
        },
      }),
      whoami: procedure({ access: 'session', handler: async (context) => ({ user: context.session.state.user }) }),
    },
  });
  const { server, origin } = await startServer(t, { router, sessions: { transport: bearer } });

  const res = await postPacket(`${origin}/api`, 'auth/login', {});
  assert.strictEqual(res.status, 200);
  // write() answered null, so no Set-Cookie is stamped — the token travels
  // in the handler's own result instead.
  assert.deepStrictEqual(res.headers.getSetCookie(), []);
  const { token } = (await res.json()).result;
  assert.ok(token);

  // A safe-method REST call with NO same-origin fetch header: under the
  // cookie default this runs sessionless (ambient authority, CSRF); a
  // bearer credential is script-attached, so it restores.
  const who = await fetch(`${origin}/api/auth/whoami`, { headers: { authorization: `Bearer ${token}` } });
  assert.strictEqual(who.status, 200);
  assert.deepStrictEqual((await who.json()).result, { user: 'bea' });

  // The same header on a ws upgrade restores the session for the socket.
  const socket = new FakeWsSocket();
  const client = server.rpc.attachSocket(socket, { headers: { authorization: `Bearer ${token}` } });
  assert.strictEqual(await client.sessionReady, true);
  assert.strictEqual(client.session.state.user, 'bea');

  // A wrong token is nobody.
  const miss = await fetch(`${origin}/api/auth/whoami`, { headers: { authorization: 'Bearer nope' } });
  assert.strictEqual(miss.status, 403);
});

test('x-wrpc-meta-*: prefixed headers are the curl-friendly spelling (strings, JSON header wins)', async (t) => {
  const router = defineRouter({
    probe: {
      peek: procedure({
        access: 'public',
        handler: async (context) => ({ call: { ...context.callMeta }, data: { ...context.meta.data } }),
      }),
    },
  });
  const { origin } = await startServer(t, { router, metaMaxBytes: 128 });

  // The S3 x-amz-meta-* idiom: one header per key, no encoding needed.
  const res = await fetch(`${origin}/api/probe/peek`, {
    headers: { 'x-wrpc-meta-idem': '9f3c', 'x-wrpc-meta-locale': 'de-CH' },
  });
  const { result } = await res.json();
  assert.deepStrictEqual(result.call, { idem: '9f3c', locale: 'de-CH' });
  assert.deepStrictEqual(result.data, { idem: '9f3c', locale: 'de-CH' });

  // On a key collision the canonical JSON header wins — it is the
  // type-faithful channel the wrpc client emits.
  const canonical = encodeURIComponent(JSON.stringify({ idem: 'json-wins', retries: 3 }));
  const both = await fetch(`${origin}/api/probe/peek`, {
    headers: { 'x-wrpc-meta': canonical, 'x-wrpc-meta-idem': 'prefixed', 'x-wrpc-meta-extra': 'kept' },
  });
  const merged = (await both.json()).result.call;
  assert.deepStrictEqual(merged, { idem: 'json-wins', retries: 3, extra: 'kept' });

  // Refusals: a bare prefix, a __proto__ key, and the size cap.
  const bad = await fetch(`${origin}/api/probe/peek`, {
    headers: { 'x-wrpc-meta-': 'nameless', 'x-wrpc-meta-__proto__': 'nope' },
  });
  assert.deepStrictEqual((await bad.json()).result.call, {});
  assert.strictEqual({}.polluted, undefined);
  const over = await fetch(`${origin}/api/probe/peek`, {
    headers: { 'x-wrpc-meta-pad': 'x'.repeat(200) },
  });
  assert.deepStrictEqual((await over.json()).result.call, {}, 'over the cap the whole label is refused');

  // A malformed canonical header does not take the prefixed ones with it.
  const mixed = await fetch(`${origin}/api/probe/peek`, {
    headers: { 'x-wrpc-meta': '%not-json', 'x-wrpc-meta-idem': 'survives' },
  });
  assert.deepStrictEqual((await mixed.json()).result.call, { idem: 'survives' });
});
