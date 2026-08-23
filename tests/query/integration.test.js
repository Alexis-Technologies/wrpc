'use strict';

// The cache bridge against a REAL client and server. The unit tests drive it
// with a fake client, which cannot prove the two things that actually matter
// here: that a path resolves against the objects `load()` really builds (own
// properties on a unit Emitter), and that a subscription opened through the
// bridge really delivers server values into the cache.

const { test } = require('node:test');
const assert = require('node:assert');

const { Server, connect, defineRouter, procedure, tracked } = require('../../index.js');
const { createQueryUtils } = require('../../query.js');

// The two methods of QueryClient this needs, with real hashing semantics: keys
// are compared by value, so a write and a read of the same path meet.
const cache = () => {
  const entries = new Map();
  const hash = (key) => JSON.stringify(key);
  return {
    entries,
    setQueryData(queryKey, updater) {
      const id = hash(queryKey);
      const previous = entries.get(id)?.value;
      const value = typeof updater === 'function' ? updater(previous) : updater;
      entries.set(id, { queryKey, value });
      return value;
    },
    read(queryKey) {
      return entries.get(hash(queryKey))?.value;
    },
  };
};

const router = defineRouter({
  chat: {
    list: procedure({
      access: 'public',
      handler: async (_context, { room }) => [{ id: '1', room }],
    }),
    send: procedure({
      access: 'public',
      handler: async (_context, { text }) => ({ id: `id-${text}` }),
    }),
    slow: procedure({
      access: 'public',
      handler: async (context) =>
        new Promise((resolve, reject) => {
          context.signal?.addEventListener('abort', () => reject(new Error('aborted')), { once: true });
        }),
    }),
    onMessage: procedure.subscription({
      access: 'public',
      handler: async function* (_context, { room }) {
        yield tracked('1', { room, text: 'one' });
        yield tracked('2', { room, text: 'two' });
      },
    }),
  },
});

const boot = async (t) => {
  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 100 },
  });
  await server.listen();
  t.after(() => server.close());
  const client = await connect(`ws://127.0.0.1:${server.address().port}/`);
  t.after(() => void client.close());
  await client.load('chat');
  return { server, client };
};

test('query integration: queryFn and mutationFn call through a real client', async (t) => {
  const { client } = await boot(t);
  const wq = createQueryUtils(client);

  const list = wq.queryOptions(['chat', 'list'], { room: 'a' });
  assert.deepStrictEqual(await list.queryFn(), [{ id: '1', room: 'a' }]);
  assert.deepStrictEqual(list.queryKey, ['chat', 'list', { room: 'a' }]);

  const send = wq.mutationOptions(['chat', 'send']);
  assert.deepStrictEqual(await send.mutationFn({ text: 'hi' }), { id: 'id-hi' });
});

test('query integration: the AbortSignal TanStack passes reaches the server', async (t) => {
  const { client } = await boot(t);
  const wq = createQueryUtils(client);
  const controller = new AbortController();
  const pending = wq.queryOptions(['chat', 'slow']).queryFn({ signal: controller.signal });
  controller.abort();
  // 499 is the wire code for "the caller took it back" — proof the signal was
  // forwarded rather than dropped into the args object.
  await assert.rejects(
    () => pending,
    (error) => error.code === 499,
  );
});

test('query integration: Emitter methods on a real unit are not procedures', async (t) => {
  const { client } = await boot(t);
  const wq = createQueryUtils(client);
  // `on` really is a function on the real unit object's prototype: without the
  // own-property check this would call Emitter.prototype.on as a procedure.
  assert.strictEqual(typeof client.api.chat.on, 'function');
  await assert.rejects(() => wq.queryOptions(['chat', 'on']).queryFn(), /has no method 'on'/);
  await assert.rejects(() => wq.queryOptions(['chat', 'nope']).queryFn(), /has no method 'nope'/);
});

test('query integration: a subscription lands in the cache', async (t) => {
  const { client } = await boot(t);
  const queryClient = cache();
  const wq = createQueryUtils(client, { queryClient });

  const key = ['messages', 'a'];
  const ended = new Promise((resolve) => {
    wq.subscriptionHandler(
      ['chat', 'onMessage'],
      { room: 'a' },
      {
        queryKey: key,
        update: (previous, data) => [...(previous ?? []), data.text],
        onEnd: resolve,
      },
    );
  });
  await ended;

  assert.deepStrictEqual(queryClient.read(key), ['one', 'two']);
});

test('query integration: a subscription path is refused as a query, and vice versa', async (t) => {
  const { client } = await boot(t);
  const wq = createQueryUtils(client, { queryClient: cache() });
  await assert.rejects(() => wq.queryOptions(['chat', 'onMessage']).queryFn(), /is a subscription/);
  assert.throws(() => wq.subscriptionHandler(['chat', 'list']), /is a call/);
});
