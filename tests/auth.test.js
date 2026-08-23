'use strict';

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { Server, WrpcClient, defineRouter, procedure } = require('../index.js');
const {
  isTokenStore,
  memoryStore,
  webStorage,
  cookieStorage,
  bearerAuth,
  bearerTransport,
  payloadTransport,
} = require('../auth.js');

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

// ---------------------------------------------------------------------------
// The store contract, run against every implementation — the engineContract
// pattern: one spec, many implementations.

const fakeWebStorage = () => {
  const map = new Map();
  return {
    getItem: (key) => (map.has(key) ? map.get(key) : null),
    setItem: (key, value) => void map.set(key, String(value)),
    removeItem: (key) => void map.delete(key),
  };
};

const fakeDocument = () => {
  // Enough of document.cookie for the store: an accessor that appends on
  // assignment and serializes name=value pairs on read, honouring Max-Age=0.
  const jar = new Map();
  return {
    get cookie() {
      return [...jar.entries()].map(([name, value]) => `${name}=${value}`).join('; ');
    },
    set cookie(text) {
      const [pair, ...attrs] = String(text).split('; ');
      const eq = pair.indexOf('=');
      const name = pair.slice(0, eq);
      if (attrs.some((attr) => attr === 'Max-Age=0')) jar.delete(name);
      else jar.set(name, pair.slice(eq + 1));
    },
  };
};

const STORES = [
  ['memoryStore', () => memoryStore()],
  ['webStorage', () => webStorage(fakeWebStorage())],
  ['cookieStorage', () => cookieStorage(fakeDocument())],
];

test('token stores: one contract, every implementation', async (t) => {
  for (const [name, build] of STORES) {
    await t.test(name, async () => {
      const store = build();
      assert.strictEqual(isTokenStore(store), true);
      assert.ok((await store.get('tokens')) === undefined || (await store.get('tokens')) === null);
      await store.set('tokens', { access: 'a1', refresh: 'r1' });
      assert.deepStrictEqual(await store.get('tokens'), { access: 'a1', refresh: 'r1' });
      await store.set('tokens', { access: 'a2' });
      assert.deepStrictEqual(await store.get('tokens'), { access: 'a2' }, 'set replaces');
      await store.delete('tokens');
      const gone = await store.get('tokens');
      assert.ok(gone === undefined || gone === null, 'delete removes');
      await store.delete('tokens'); // idempotent
    });
  }
});

test('token stores: prefixes isolate applications', async () => {
  const backing = fakeWebStorage();
  const a = webStorage(backing, { prefix: 'a:' });
  const b = webStorage(backing, { prefix: 'b:' });
  await a.set('tokens', { access: '1' });
  assert.strictEqual(await b.get('tokens'), undefined);
});

// ---------------------------------------------------------------------------
// The server halves on their own

test('bearerTransport reads the header, then the subprotocol offer, then the parsed declared bag', () => {
  const transport = bearerTransport();
  assert.strictEqual(transport.ambient, false);
  assert.strictEqual(transport.read({ headers: { authorization: 'Bearer abc' } }), 'abc');
  assert.strictEqual(transport.read({ headers: { authorization: 'Basic abc' } }), null);
  // The ws subprotocol carrier: the client offers wrpc.bearer.<token> next
  // to the wire revision, keeping the credential out of the connect URL.
  assert.strictEqual(transport.read({ headers: { 'sec-websocket-protocol': 'wrpc.v1, wrpc.bearer.tok9' } }), 'tok9');
  // The declared bag arrives PARSED from the core (kebab keys, capped on the
  // configurable metaMaxBytes) — this module never re-derives it from the
  // URL, so it cannot drift from the core parser.
  assert.strictEqual(transport.read({ headers: {}, declared: { authorization: 'Bearer fromquery' } }), 'fromquery');
  assert.strictEqual(
    transport.read({ headers: { authorization: 'Bearer real' }, declared: { authorization: 'Bearer other' } }),
    'real',
  );
  const raw = encodeURIComponent(JSON.stringify({ Authorization: 'Bearer fromquery' }));
  assert.strictEqual(transport.read({ headers: {}, url: `/api?wrpc_h=${raw}` }), null);
  assert.strictEqual(transport.write('abc'), null);
});

test('payloadTransport reads the declared meta field (header and ws query)', () => {
  const transport = payloadTransport();
  const meta = encodeURIComponent(JSON.stringify({ token: 'tok1', v: '1' }));
  assert.strictEqual(transport.read({ headers: { 'x-wrpc-meta': meta } }), 'tok1');
  assert.strictEqual(transport.read({ headers: {}, url: `/api?wrpc_meta=${meta}` }), 'tok1');
  assert.strictEqual(transport.read({ headers: {}, url: '/api?other=1' }), null);
  assert.strictEqual(payloadTransport({ field: 'sid' }).read({ headers: { 'x-wrpc-meta': meta } }), null);
});

// ---------------------------------------------------------------------------
// bearerAuth end to end: sign in -> store -> reconnect presents the header
// -> the upgrade restores the session BEFORE the session-gated re-subscribe.

const bearerBoot = async (t) => {
  const state = { signIns: 0, refreshes: 0, subscribes: 0, expired: new Set() };
  const definition = defineRouter({
    auth: {
      signIn: procedure({
        access: 'public',
        handler: async (context) => {
          state.signIns++;
          context.client.startSession(undefined, { user: 'noa' });
          return { access: context.session.token };
        },
      }),
      refresh: procedure({
        access: 'public',
        handler: async (context, { access }) => {
          state.refreshes++;
          // A rotated pair: the old token dies, the live connection is
          // re-bound, the stored pair covers the next connection.
          state.expired.add(access);
          context.client.startSession(undefined, { user: 'noa' });
          return { access: context.session.token };
        },
      }),
    },
    secure: {
      whoami: procedure({ access: 'session', handler: async (context) => context.session.state.user }),
      feed: procedure.subscription({
        access: 'session',
        handler: async function* (_context, _args, { signal }) {
          state.subscribes++;
          await timers.setTimeout(60_000, undefined, { signal }).catch(() => {});
        },
      }),
    },
  });
  const server = new Server({
    router: definition,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 50 },
    introspection: 'session',
    sessions: { transport: bearerTransport() },
  });
  await server.listen();
  t.after(() => server.close());
  return { server, port: server.address().port, state };
};

test('bearerAuth: the full cycle over a real server with a bearer token transport', async (t) => {
  const { server, port, state } = await bearerBoot(t);
  const store = memoryStore();
  const client = await WrpcClient.connect(
    `ws://127.0.0.1:${port}/api`,
    Object.assign(
      { heartbeat: false, logger: false, reconnectTimeout: 10 },
      bearerAuth({
        store,
        signIn: (c) => c.call('auth/signIn'),
        refresh: async (c, tokens) => c.call('auth/refresh', { access: tokens?.access }),
      }),
    ),
  );
  t.after(() => void client.close());

  // First connect: nothing stored -> signIn ran, the pair is in the store.
  assert.strictEqual(state.signIns, 1);
  const stored = await store.get('tokens');
  assert.ok(stored.access);

  // The session is live (bound by startSession over the socket): a gated
  // load and subscription work.
  await client.load('secure');
  assert.strictEqual(await client.api.secure.whoami(), 'noa');
  client.api.secure.feed.subscribe({}, {});
  await waitFor(() => state.subscribes === 1, 'the subscription never opened');

  // Reconnect: the stored token rides the upgrade (wrpc_h), the bearer
  // transport restores the session BEFORE the re-subscribe — signIn must
  // NOT run again, and the session-gated feed must resume.
  for (const connection of server.wsServer.connections) connection.terminate();
  await waitFor(() => state.subscribes === 2, 'the gated subscription did not survive the reconnect');
  assert.strictEqual(state.signIns, 1, 'the stored credential made a second signIn unnecessary');
});

test('bearerAuth: a mid-session refusal refreshes once, rotates the pair and retries', async (t) => {
  const { server, port, state } = await bearerBoot(t);
  const store = memoryStore();
  const client = await WrpcClient.connect(
    `ws://127.0.0.1:${port}/api`,
    Object.assign(
      { heartbeat: false, logger: false, reconnect: false },
      bearerAuth({
        store,
        signIn: (c) => c.call('auth/signIn'),
        refresh: async (c, tokens) => c.call('auth/refresh', { access: tokens?.access }),
      }),
    ),
  );
  t.after(() => void client.close());
  await client.load('secure');
  const before = (await store.get('tokens')).access;

  // Expire the session server-side: the next gated call is refused with
  // wrpc's own 403, which bearerAuth's default `on` covers.
  for (const serverClient of server.clients) await serverClient.finalizeSession();

  // The refused call triggers ONE refresh (auth/refresh re-binds the live
  // connection via startSession) and is retried exactly once.
  assert.strictEqual(await client.api.secure.whoami(), 'noa');
  assert.strictEqual(state.refreshes, 1);
  const rotated = (await store.get('tokens')).access;
  assert.notStrictEqual(rotated, before, 'the stored pair was rotated');
  assert.strictEqual(state.signIns, 1, 'refresh healed the session without a second signIn');
});

test('bearerAuth: a THROWING refresh clears the store like a falsy return does', async () => {
  const store = memoryStore();
  store.set('tokens', { access: 'dead', refresh: 'revoked' });
  const options = bearerAuth({
    store,
    signIn: () => null,
    refresh: () => {
      throw new Error('refresh token revoked');
    },
  });
  await assert.rejects(options.refresh.handler({}, new Error('401')), /refresh token revoked/);
  // The pair just proved dead: leaving it stored would make every later
  // reconnect's authenticate short-circuit on a corpse forever.
  assert.strictEqual(store.get('tokens'), undefined);
});

// ---------------------------------------------------------------------------
// The carrier matrix: one payloadTransport server, every carrier x both
// meta spellings — the drift this matrix exists to catch is a token that
// restores on ws but silently 403s on http/sse under metaFormat 'prefixed'.

require('../sse.js'); // registers the 'sse' client transport

const payloadBoot = async (t) => {
  const definition = defineRouter({
    auth: {
      signIn: procedure({
        access: 'public',
        handler: async (context) => {
          context.client.startSession(undefined, { user: 'zoe' });
          return { token: context.session.token };
        },
      }),
    },
    secure: {
      whoami: procedure({ access: 'session', handler: async (context) => context.session.state.user }),
    },
  });
  const server = new Server({
    router: definition,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 50 },
    sessions: { transport: payloadTransport() },
  });
  await server.listen();
  t.after(() => server.close());
  const port = server.address().port;
  const first = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    heartbeat: false,
    logger: false,
    reconnect: false,
  });
  const { token } = await first.call('auth/signIn', {});
  first.close();
  return { server, port, token };
};

test('payloadTransport: the token restores on every carrier and both meta spellings', async (t) => {
  const { port, token } = await payloadBoot(t);
  const cases = [
    ['ws json', `ws://127.0.0.1:${port}/api`, {}],
    ['ws prefixed', `ws://127.0.0.1:${port}/api`, { metaFormat: 'prefixed' }],
    ['http json', `http://127.0.0.1:${port}/api`, { transport: 'http' }],
    ['http prefixed', `http://127.0.0.1:${port}/api`, { transport: 'http', metaFormat: 'prefixed' }],
    ['sse json', `http://127.0.0.1:${port}/api`, { transport: 'sse' }],
    ['sse prefixed', `http://127.0.0.1:${port}/api`, { transport: 'sse', metaFormat: 'prefixed' }],
  ];
  for (const [label, url, extra] of cases) {
    const client = await WrpcClient.connect(url, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      meta: { token },
      ...extra,
    });
    await client.load('secure');
    assert.strictEqual(await client.api.secure.whoami(), 'zoe', `carrier: ${label}`);
    client.close();
  }
});

test('payloadTransport: a hand-written x-wrpc-meta-<field> header restores too', async (t) => {
  const { port, token } = await payloadBoot(t);
  // The curl shape: no wrpc client at all, one per-key header. GET is a safe
  // method, but a non-ambient carrier restores on it by design.
  const res = await fetch(`http://127.0.0.1:${port}/api/secure/whoami`, {
    headers: { 'x-wrpc-meta-token': token },
  });
  const body = await res.json();
  assert.strictEqual(res.status, 200);
  assert.strictEqual(body.result, 'zoe');
});

test('bearerAuth over ws: the token rides the subprotocol offer, never the connect URL', async (t) => {
  const { server, port } = await bearerBoot(t);
  const store = memoryStore();
  const client = await WrpcClient.connect(
    `ws://127.0.0.1:${port}/api`,
    Object.assign(
      { heartbeat: false, logger: false, reconnect: false },
      bearerAuth({ store, signIn: (c) => c.call('auth/signIn') }),
    ),
  );
  t.after(() => void client.close());
  await client.load('secure');
  // Session restored on reconnect-shaped opens proves the carrier works;
  // here the FIRST connect signs in, so assert on a second connection that
  // presents the stored token.
  const stored = await store.get('tokens');
  assert.ok(stored.access);
  const again = await WrpcClient.connect(
    `ws://127.0.0.1:${port}/api`,
    Object.assign(
      { heartbeat: false, logger: false, reconnect: false },
      bearerAuth({ store, signIn: () => assert.fail('stored token must restore without a signIn') }),
    ),
  );
  t.after(() => void again.close());
  await again.load('secure');
  // The stored token restored the session with NO signIn — the subprotocol
  // carrier did the work.
  assert.strictEqual(await again.api.secure.whoami(), 'noa');
  // And the upgrade URL the server observed carries no credential: wrpc_h
  // (the query fallback that lands in access logs) must be absent.
  for (const peer of server.clients) {
    assert.ok(!String(peer.meta?.url ?? '').includes('wrpc_h'), 'the bearer token leaked into the connect URL');
  }
});

test('payloadTransport: raw fallbacks — prefixed header, oversize canonical, camelCase field', () => {
  const transport = payloadTransport();
  // The per-key spelling as a raw header (no parsed bag: the SSE key path).
  assert.strictEqual(transport.read({ headers: { 'x-wrpc-meta-token': 'tok2' } }), 'tok2');
  // An oversize canonical header is ignored, not parsed.
  const big = 'x'.repeat(3000);
  assert.strictEqual(transport.read({ headers: { 'x-wrpc-meta': big } }), null);
  // The parsed bag wins over everything.
  assert.strictEqual(transport.read({ headers: { 'x-wrpc-meta-token': 'raw' }, meta: { token: 'bag' } }), 'bag');
  // A camelCase field matches its kebab key in the parsed bag.
  const camel = payloadTransport({ field: 'authToken' });
  assert.strictEqual(camel.read({ headers: {}, meta: { 'auth-token': 'k1' } }), 'k1');
  assert.strictEqual(camel.read({ headers: { 'x-wrpc-meta-auth-token': 'k2' } }), 'k2');
});
