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

const { WrpcClient, defineRouter, procedure, createEventStream, tracked } = require('../../index.js');

const JSON_HEADERS = { 'Content-Type': 'application/json' };

// Filled by the `chat.on.typing` event handler, drained by `chat/typed`.
const typedEvents = [];

// Open `chat/forever` subscriptions, so a spec can watch one being released.
const liveSubscriptions = new Set();

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
    // What proves no host drops the request identity on its way in.
    peekMeta: procedure({
      access: 'public',
      handler: async (context) => ({
        url: context.meta.url,
        protocol: context.meta.protocol,
        hasHeaders: Object.keys(context.meta.headers).length > 0,
        appVersion: context.meta.headers['x-app-version'] ?? null,
      }),
    }),
    whoami: procedure({
      access: 'session',
      handler: async (context) => ({ user: context.session.state.user }),
    }),
    notify: procedure({
      access: 'public',
      handler: async (context) => {
        context.client.sendEvent('test/ping', { ping: true });
        return { ok: true };
      },
    }),
    // A declaratively-mapped route: every boot must serve it as a real REST
    // endpoint — plain result body, status from http.status — whether the
    // host is the core trie (shell/express/uws) or fastify's own router
    // (the delegated route).
    mapped: procedure({
      access: 'public',
      http: { method: 'POST', path: '/things/:thingId', status: 201 },
      handler: async (_context, { params, query, body }) => ({ thingId: params.thingId, q: query, name: body?.name }),
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
  // Rooms and inbound events are core behavior, so every boot has to answer
  // for them too. `typed` drains what the event handler recorded — an event
  // never answers on the wire, so a call is the only way to observe it.
  chat: {
    join: procedure({
      access: 'public',
      handler: async (context, { room }) => {
        context.client.join(room);
        return { rooms: [...context.client.rooms] };
      },
    }),
    leave: procedure({
      access: 'public',
      handler: async (context, { room }) => ({ left: context.client.leave(room) }),
    }),
    shout: procedure({
      access: 'public',
      handler: async (context, { room, text, self = true }) => {
        const target = self ? context.server.to(room) : context.server.to(room).except(context.client);
        return { sent: target.emit('chat/message', { text }) };
      },
    }),
    members: procedure({
      access: 'public',
      handler: async (context, { room }) => ({ count: context.server.rooms.count(room) }),
    }),
    typed: procedure({
      access: 'public',
      handler: async () => {
        const seen = typedEvents.splice(0, typedEvents.length);
        return { seen };
      },
    }),
    // A subscription: opened with {type:'subscribe'}, answered with a
    // stream of {type:'data'} and one {type:'end'}. Every boot must carry it.
    ticks: procedure.subscription({
      access: 'public',
      handler: async function* (_context, { to = 3, from = 0 } = {}) {
        for (let i = from + 1; i <= to; i++) yield tracked(String(i), { n: i });
      },
    }),
    forever: procedure.subscription({
      access: 'public',
      handler: async function* (_context, _args, { signal }) {
        const stream = createEventStream({ signal });
        liveSubscriptions.add(stream);
        const timer = setInterval(() => stream.push({ tick: true }), 5);
        try {
          yield* stream;
        } finally {
          clearInterval(timer);
          liveSubscriptions.delete(stream);
        }
      },
    }),
    on: {
      typing: procedure({
        access: 'public',
        handler: async (_context, data) => void typedEvents.push(data),
      }),
    },
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

// Events travel one way, so there is no reply to await: give the fan-out a
// round trip to land before asserting on it.
const settle = () => new Promise((resolve) => setTimeout(resolve, 25));

const waitFor = async (predicate, message) => {
  for (let i = 0; i < 100; i++) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail(message);
};

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

  await t.test('a declarative REST route answers a plain result with its own status', async () => {
    const res = await fetch(`${base}/things/42?x=1`, {
      method: 'POST',
      headers: JSON_HEADERS,
      body: JSON.stringify({ name: 'Alpha' }),
    });
    assert.strictEqual(res.status, 201);
    assert.deepStrictEqual(await res.json(), { thingId: '42', q: { x: '1' }, name: 'Alpha' });
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
    // Refused server-side, not just denied the grant: the page could not
    // read the answer either way, but the call itself must not run.
    assert.strictEqual(denied.res.status, 403);
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

  await t.test('client.meta: no host drops the request url or headers', async (sub) => {
    // The packet endpoint: a per-request client still sees url + headers —
    // including a client-declared one carried as a REAL request header.
    const { res, body } = await rpcPost(base, 'test/peekMeta', {}, { 'x-app-version': '9.9' });
    assert.strictEqual(res.status, 200);
    assert.ok(body.result.url.includes('/api'), `http: meta.url survived (got '${body.result.url}')`);
    assert.strictEqual(body.result.hasHeaders, true, 'http: meta.headers observed');
    assert.strictEqual(body.result.appVersion, '9.9', 'http: a declared header rides as a real one');
    // The ws upgrade: url, headers, the negotiated subprotocol — and the
    // declared headers, carried by the wrpc_h query (lowercased on arrival).
    const client = await WrpcClient.connect(`ws://127.0.0.1:${main.port}/api`, {
      headers: { 'X-App-Version': '8.8' },
    });
    sub.after(() => void client.close());
    const meta = await client.call('test/peekMeta');
    assert.ok(meta.url.includes('/api'), `ws: the upgrade url survived (got '${meta.url}')`);
    assert.strictEqual(meta.hasHeaders, true, 'ws: the upgrade headers survived');
    assert.strictEqual(meta.protocol, 'wrpc.v1');
    assert.strictEqual(meta.appVersion, '8.8', 'ws: the declared header arrived through the connect url');
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

  await t.test('rooms and inbound events over two connections', async (sub) => {
    const url = `ws://127.0.0.1:${main.port}/api`;
    const [ada, grace] = await Promise.all([WrpcClient.connect(url), WrpcClient.connect(url)]);
    sub.after(() => {
      ada.close();
      grace.close();
    });
    await Promise.all([ada.load('chat'), grace.load('chat')]);

    const received = { ada: [], grace: [] };
    ada.api.chat.on('message', (data) => void received.ada.push(data));
    grace.api.chat.on('message', (data) => void received.grace.push(data));

    assert.deepStrictEqual(await ada.api.chat.join({ room: 'lobby' }), { rooms: ['lobby'] });
    await grace.api.chat.join({ room: 'lobby' });
    assert.deepStrictEqual(await ada.api.chat.members({ room: 'lobby' }), { count: 2 });

    // A room broadcast reaches every member, the sender included...
    assert.deepStrictEqual(await ada.api.chat.shout({ room: 'lobby', text: 'hello' }), { sent: 2 });
    await settle();
    assert.deepStrictEqual(received.ada, [{ text: 'hello' }]);
    assert.deepStrictEqual(received.grace, [{ text: 'hello' }]);

    // ...unless it is excluded.
    assert.deepStrictEqual(await ada.api.chat.shout({ room: 'lobby', text: 'psst', self: false }), { sent: 1 });
    await settle();
    assert.strictEqual(received.ada.length, 1, 'except() drops the sender');
    assert.deepStrictEqual(received.grace.at(-1), { text: 'psst' });

    // A client -> server event is fire-and-forget: nothing comes back on the
    // wire, and the handler's effect is observed through a call.
    ada.sendEvent('chat/typing', { who: 'ada' });
    await settle();
    assert.deepStrictEqual(await ada.api.chat.typed(), { seen: [{ who: 'ada' }] });

    assert.deepStrictEqual(await grace.api.chat.leave({ room: 'lobby' }), { left: true });
    assert.deepStrictEqual(await ada.api.chat.members({ room: 'lobby' }), { count: 1 });

    // A dropped connection releases its rooms without anyone calling leave.
    grace.close();
    await waitFor(
      async () => (await ada.api.chat.members({ room: 'lobby' })).count === 1,
      'a disconnect must not change a room it had already left',
    );
    ada.close();
    await waitFor(() => main.rpc.rooms.has('lobby') === false, 'the last member disconnecting must drop the room');
  });

  await t.test('subscriptions, resume and cancellation over one connection', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${main.port}/api`);
    sub.after(() => void client.close());
    await client.load('chat');

    // subscribe -> data* -> end
    const seen = [];
    const ended = new Promise((resolve) => {
      client.api.chat.ticks.subscribe({ to: 3 }, { onData: (data) => seen.push(data), onEnd: resolve });
    });
    await ended;
    assert.deepStrictEqual(seen, [{ n: 1 }, { n: 2 }, { n: 3 }]);

    // The same feed resumed from an eventId yields only what came after.
    const resumed = [];
    let handle = null;
    const done = new Promise((resolve) => {
      handle = client.api.chat.ticks.subscribe(
        { to: 3, from: 2 },
        { lastEventId: '2', onData: (data) => resumed.push(data), onEnd: resolve },
      );
    });
    await done;
    assert.deepStrictEqual(resumed, [{ n: 3 }]);
    assert.strictEqual(handle.lastEventId, '3');

    // for await, and breaking out of it, reaches the server
    const before = liveSubscriptions.size;
    for await (const value of client.api.chat.forever.iterate()) {
      assert.deepStrictEqual(value, { tick: true });
      break;
    }
    await waitFor(() => liveSubscriptions.size === before, 'breaking the loop never released the generator');

    // A dropped connection releases whatever is still running.
    const handle2 = client.api.chat.forever.subscribe();
    await waitFor(() => liveSubscriptions.size === before + 1, 'the subscription never opened');
    assert.strictEqual(handle2.closed, false);
    client.close();
    await waitFor(() => liveSubscriptions.size === before, 'a disconnect left a generator running');
  });
};

module.exports = { runAdapterSpec, router };
