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

test('bearerTransport reads the real header first, the ws query as fallback', () => {
  const transport = bearerTransport();
  assert.strictEqual(transport.ambient, false);
  assert.strictEqual(transport.read({ headers: { authorization: 'Bearer abc' } }), 'abc');
  assert.strictEqual(transport.read({ headers: { authorization: 'Basic abc' } }), null);
  const declared = encodeURIComponent(JSON.stringify({ Authorization: 'Bearer fromquery' }));
  assert.strictEqual(transport.read({ headers: {}, url: `/api?wrpc_h=${declared}` }), 'fromquery');
  // The observed header wins over the declared one.
  assert.strictEqual(
    transport.read({ headers: { authorization: 'Bearer real' }, url: `/api?wrpc_h=${declared}` }),
    'real',
  );
  assert.strictEqual(transport.read({ headers: {}, url: '/api?wrpc_h=%7Bnot-json' }), null);
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
