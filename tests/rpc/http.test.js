'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const crypto = require('node:crypto');
const { EventEmitter } = require('node:events');

const { Server, defineRouter, procedure } = require('../../index.js');
const { ProtocolClient } = require('../websocket/protocolClient.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

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
    console: quiet,
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
  assert.strictEqual(res.headers.get('access-control-allow-headers'), 'Content-Type');
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

  const denied = await postPacket(`${origin}/api`, 'echo/args', {}, { Origin: 'http://evil.example' });
  assert.strictEqual(denied.status, 200);
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
