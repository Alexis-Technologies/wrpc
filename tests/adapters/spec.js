'use strict';

// One behavioral specification, five boots.
//
// Every supported way of standing wrpc up — the batteries-included Server over
// either engine, the fastify plugin over either backend, express middleware on
// a listener it does not own — has to answer identically. So the behavior is
// written once, here, and tests/adapters/swap.test.js replays it against each
// entry of buildBoots(). The only sanctioned difference is who answers a path
// outside basePath (`entry.pathMiss`).
//
// Not a *.test.js — imported by the adapter tests.
//
// Cleanup discipline: every boot registers its close through `t.after` the
// moment it resolves, never as a trailing await. A standalone (uws) engine
// holds a native listen socket, and an assertion failing before an un-hooked
// close would wedge the whole run instead of failing it.

const assert = require('node:assert');
const { randomUUID } = require('node:crypto');
const { Blob } = require('node:buffer');

const { WrpcClient, defineRouter, procedure } = require('../../index.js');

const JSON_HEADERS = { 'Content-Type': 'application/json' };

// The single router definition every boot shares.
const router = defineRouter({
  test: {
    hello: procedure({
      access: 'public',
      handler: async (_context, { name }) => `Hello, ${name}`,
    }),
    // REST mode hands args in as a plain object: the assertions on argument
    // shape (query strings, body merge) all go through here.
    echo: procedure({
      access: 'public',
      handler: async (_context, args) => args,
    }),
    fail: procedure({
      access: 'public',
      handler: async () => {
        const error = new Error('Boom');
        error.code = 418;
        throw error;
      },
    }),
    whoami: procedure({
      access: 'session',
      handler: async (context) => ({ user: context.session.state.user }),
    }),
    notify: procedure({
      access: 'public',
      handler: async (context) => {
        await context.client.emit('test/ping', { ping: true });
        return { ok: true };
      },
    }),
    readUpload: procedure({
      access: 'public',
      handler: async (context, { id }) => {
        const stream = context.client.getStream(id);
        const chunks = [];
        for await (const chunk of stream) chunks.push(Buffer.from(chunk));
        return { name: stream.name, size: stream.size, data: Buffer.concat(chunks).toString('utf8') };
      },
    }),
    download: procedure({
      access: 'public',
      handler: async (context, { name }) => {
        const payload = Buffer.from('payload from the server');
        const stream = context.client.createStream(name, payload.length);
        queueMicrotask(() => {
          stream.write(payload);
          stream.end();
        });
        return { id: stream.id };
      },
    }),
  },
  auth: {
    login: procedure({
      access: 'public',
      handler: async (context, args) => {
        context.client.startSession(undefined, { user: args?.user ?? 'ada' });
        return { ok: true };
      },
    }),
    logout: procedure({
      access: 'session',
      handler: async (context) => context.client.finalizeSession(),
    }),
  },
});

const callPacket = (method, args = {}) => ({ type: 'call', id: randomUUID(), method, args });

// A packet-mode POST: the wire form every transport shares.
const rpcPost = async (url, method, args = {}, headers = {}) => {
  const packet = callPacket(method, args);
  const res = await fetch(url, {
    method: 'POST',
    headers: { ...JSON_HEADERS, ...headers },
    body: JSON.stringify(packet),
  });
  const body = await res.json();
  return { res, body, id: packet.id };
};

const rpcGet = async (url, headers = {}) => {
  const res = await fetch(url, { headers });
  const body = await res.json();
  return { res, body };
};

const setCookies = (res) => {
  if (typeof res.headers.getSetCookie === 'function') return res.headers.getSetCookie();
  const single = res.headers.get('set-cookie');
  return single ? [single] : [];
};

const readSessionToken = (res) => {
  for (const cookie of setCookies(res)) {
    const [pair] = cookie.split(';');
    const eq = pair.indexOf('=');
    if (pair.slice(0, eq).trim() === 'token') return pair.slice(eq + 1).trim();
  }
  return null;
};

// Logs in over HTTP and returns the Cookie header a browser would send back.
const login = async (base, user) => {
  const { res, body } = await rpcPost(base, 'auth/login', { user });
  assert.deepStrictEqual(body.result, { ok: true }, 'login answers');
  const token = readSessionToken(res);
  assert.ok(token, 'login sets the session cookie');
  return { token, cookie: `token=${token}` };
};

const runAdapterSpec = async (entry, t) => {
  const boot = async (options) => {
    const instance = await entry.boot({ router, ...options });
    // Registered immediately: a later assertion failure must not leak a listener.
    t.after(() => instance.close());
    return instance;
  };

  const main = await boot({});
  const origin = `http://127.0.0.1:${main.port}`;
  const base = `${origin}/api`;

  await t.test('packet endpoint: a call packet is answered with its callback', async () => {
    const { res, body, id } = await rpcPost(base, 'test/hello', { name: 'Ada' });
    assert.strictEqual(res.status, 200);
    assert.strictEqual(body.type, 'callback');
    assert.strictEqual(body.id, id);
    assert.strictEqual(body.result, 'Hello, Ada');
  });

  await t.test('packet endpoint: system/introspect is public', async () => {
    const { res, body } = await rpcPost(base, 'system/introspect', {});
    assert.strictEqual(res.status, 200);
    assert.ok(body.result.test, 'introspection exposes the test unit');
    assert.strictEqual(body.result.test.hello.access, 'public');
    assert.strictEqual(body.result.test.whoami.access, 'session');
  });

  await t.test('REST mode: GET takes its args from the query as strings', async () => {
    const { res, body } = await rpcGet(`${base}/test/echo?a=1&b=two`);
    assert.strictEqual(res.status, 200);
    assert.deepStrictEqual(body.result, { a: '1', b: 'two' });
  });

  await t.test('REST mode: POST merges query params with the body, body wins', async () => {
    const res = await fetch(`${base}/test/echo?a=1&b=query`, {
      method: 'POST',
      headers: JSON_HEADERS,
      body: JSON.stringify({ b: 'body', c: 3 }),
    });
    const body = await res.json();
    assert.strictEqual(res.status, 200);
    assert.deepStrictEqual(body.result, { a: '1', b: 'body', c: 3 });
  });

  await t.test('an unknown method is a 404 error packet', async () => {
    const { res, body, id } = await rpcPost(base, 'test/nothing', {});
    assert.strictEqual(res.status, 404);
    assert.strictEqual(body.type, 'callback');
    assert.strictEqual(body.id, id);
    assert.strictEqual(body.error.code, 404);
  });

  await t.test('a handler error propagates its own code', async () => {
    const { res, body } = await rpcPost(base, 'test/fail', {});
    assert.strictEqual(res.status, 418);
    assert.strictEqual(body.error.code, 418);
    assert.strictEqual(body.error.message, 'Boom');
  });

  await t.test('access control: a session procedure without a session is 403', async () => {
    const { res, body } = await rpcPost(base, 'test/whoami', {});
    assert.strictEqual(res.status, 403);
    assert.strictEqual(body.error.code, 403);
  });

  await t.test('sessions over HTTP: login sets a cookie, the cookie restores it, logout kills it', async () => {
    const { cookie } = await login(base, 'grace');

    const restored = await rpcPost(base, 'test/whoami', {}, { cookie });
    assert.strictEqual(restored.res.status, 200);
    assert.deepStrictEqual(restored.body.result, { user: 'grace' });

    const out = await rpcPost(base, 'auth/logout', {}, { cookie });
    assert.strictEqual(out.res.status, 200);
    assert.strictEqual(out.body.result, true);

    const revoked = await rpcPost(base, 'test/whoami', {}, { cookie });
    assert.strictEqual(revoked.res.status, 403);
    assert.strictEqual(revoked.body.error.code, 403);
  });

  await t.test('CSRF gate: a cross-site GET gets no ambient session', async () => {
    const { cookie } = await login(base, 'ada');
    const url = `${base}/test/whoami`;

    const cross = await rpcGet(url, { cookie, 'sec-fetch-site': 'cross-site', 'sec-fetch-mode': 'navigate' });
    assert.strictEqual(cross.res.status, 403, 'cross-site navigation must not dispatch a session procedure');
    assert.strictEqual(cross.body.error.code, 403);

    const same = await rpcGet(url, { cookie, 'sec-fetch-site': 'same-origin', 'sec-fetch-mode': 'cors' });
    assert.strictEqual(same.res.status, 200);
    assert.deepStrictEqual(same.body.result, { user: 'ada' });

    // Non-browser peers (curl, server-to-server) send no Fetch metadata at all.
    const bare = await rpcGet(url, { cookie });
    assert.strictEqual(bare.res.status, 200);
    assert.deepStrictEqual(bare.body.result, { user: 'ada' });
  });

  await t.test('a path outside basePath is 404', async () => {
    const res = await fetch(`${origin}/nowhere`);
    const text = await res.text();
    assert.strictEqual(res.status, 404);
    // Only the boots where wrpc owns every request answer with a wrpc packet;
    // composed as middleware/plugin the host framework's own 404 wins.
    if (entry.pathMiss !== 'wrpc') return;
    const packet = JSON.parse(text);
    assert.strictEqual(packet.type, 'callback');
    assert.strictEqual(packet.error.code, 404);
  });

  await t.test('CORS: an allowed origin is echoed with Vary, a disallowed one is not', async () => {
    const cors = { origins: ['https://allowed.example'], credentials: true };
    const server = await boot({ cors });
    const corsBase = `http://127.0.0.1:${server.port}/api`;

    const allowed = await rpcPost(corsBase, 'test/hello', { name: 'CORS' }, { origin: 'https://allowed.example' });
    assert.strictEqual(allowed.res.status, 200);
    assert.strictEqual(allowed.res.headers.get('access-control-allow-origin'), 'https://allowed.example');
    assert.strictEqual(allowed.res.headers.get('vary'), 'Origin');
    assert.strictEqual(allowed.res.headers.get('access-control-allow-credentials'), 'true');

    const denied = await rpcPost(corsBase, 'test/hello', { name: 'CORS' }, { origin: 'https://evil.example' });
    // CORS is enforced by the browser: the call still runs, the grant is withheld.
    assert.strictEqual(denied.res.status, 200);
    assert.strictEqual(denied.res.headers.get('access-control-allow-origin'), null);
    assert.strictEqual(denied.res.headers.get('vary'), 'Origin');

    const preflight = await fetch(corsBase, {
      method: 'OPTIONS',
      headers: { origin: 'https://allowed.example', 'access-control-request-method': 'POST' },
    });
    await preflight.text();
    assert.strictEqual(preflight.status, 200);
    assert.strictEqual(preflight.headers.get('access-control-allow-origin'), 'https://allowed.example');
    assert.strictEqual(preflight.headers.get('access-control-allow-methods'), 'POST, GET, OPTIONS');
  });

  await t.test('basePath: both endpoints move together', async () => {
    const server = await boot({ basePath: '/rpc' });
    const moved = `http://127.0.0.1:${server.port}/rpc`;

    const packet = await rpcPost(moved, 'test/hello', { name: 'Moved' });
    assert.strictEqual(packet.res.status, 200);
    assert.strictEqual(packet.body.result, 'Hello, Moved');

    const rest = await rpcGet(`${moved}/test/echo?a=1`);
    assert.strictEqual(rest.res.status, 200);
    assert.deepStrictEqual(rest.body.result, { a: '1' });

    const old = await fetch(`http://127.0.0.1:${server.port}/api`, {
      method: 'POST',
      headers: JSON_HEADERS,
      body: JSON.stringify(callPacket('test/hello', { name: 'Old' })),
    });
    await old.text();
    assert.strictEqual(old.status, 404, 'the default base path no longer answers');
  });

  await t.test('WebSocket: load, call, event and streams over one connection', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${main.port}/api`);
    sub.after(() => void client.close());

    // load() goes through system/introspect
    await client.load('test');
    assert.strictEqual(typeof client.api.test.hello, 'function');
    assert.strictEqual(await client.api.test.hello({ name: 'Socket' }), 'Hello, Socket');

    await assert.rejects(client.api.test.fail(), (error) => error.code === 418 && error.message === 'Boom');
    await assert.rejects(client.api.test.whoami(), (error) => error.code === 403);

    const ping = new Promise((resolve) => client.api.test.on('ping', resolve));
    assert.deepStrictEqual(await client.api.test.notify(), { ok: true });
    assert.deepStrictEqual(await ping, { ping: true });

    if (!entry.streams) return;

    const data = 'binary payload travelling up to the server';
    const blob = new Blob([data]);
    blob.name = 'upload-stream';
    const uploader = client.createBlobUploader(blob);
    const uploading = client.api.test.readUpload({ id: uploader.id });
    await uploader.upload();
    const uploaded = await uploading;
    assert.strictEqual(uploaded.name, 'upload-stream');
    assert.strictEqual(uploaded.size, blob.size);
    assert.strictEqual(uploaded.data, data);

    const { id } = await client.api.test.download({ name: 'download-stream' });
    const readable = client.getStream(id);
    const downloaded = await readable.toBlob();
    assert.strictEqual(await downloaded.text(), 'payload from the server');
  });
};

module.exports = { runAdapterSpec, router };
