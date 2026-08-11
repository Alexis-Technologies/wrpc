'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { Emitter } = require('../../src/utils.js');
const { createQueryUtils } = require('../../query.js');

// A stand-in for a loaded client: `api` holds unit Emitters carrying OWN
// method properties, which is exactly the shape `WrpcClient.load()` builds.
const fakeClient = () => {
  const calls = [];
  const subscribes = [];
  const chat = new Emitter();
  chat.send = (args, options) => {
    calls.push({ target: 'chat/send', args, options });
    return Promise.resolve({ id: 'made' });
  };
  chat.onMessage = {
    kind: 'subscription',
    subscribe: (args, options) => {
      const record = { args, options, closed: false };
      subscribes.push(record);
      record.unsubscribe = () => {
        record.closed = true;
        return true;
      };
      return record;
    },
    iterate: () => assert.fail('iterate is not what the cache bridge uses'),
  };
  const api = { chat };
  return { api, calls, subscribes };
};

const fakeQueryClient = () => {
  const writes = [];
  return {
    writes,
    setQueryData(queryKey, updater) {
      const previous = writes.length > 0 ? writes[writes.length - 1].value : undefined;
      const value = typeof updater === 'function' ? updater(previous) : updater;
      writes.push({ queryKey, value });
      return value;
    },
  };
};

// ---------------------------------------------------------------------------

test('query: createQueryUtils validates what it is handed', () => {
  assert.throws(() => createQueryUtils(), /must be a WrpcClient/);
  assert.throws(() => createQueryUtils({}), /must be a WrpcClient/);
  assert.throws(() => createQueryUtils({ api: null }), /must be a WrpcClient/);
  assert.throws(() => createQueryUtils(fakeClient(), { prefix: 'wrpc' }), /prefix must be an array/);
  // A queryClient is optional here, but if given it must be usable
  assert.throws(() => createQueryUtils(fakeClient(), { queryClient: {} }), /setQueryData\(\) method/);
  assert.ok(createQueryUtils(fakeClient(), { queryClient: fakeQueryClient() }));
});

test('query: queryKey is prefix-matchable', () => {
  const wq = createQueryUtils(fakeClient());
  assert.deepStrictEqual(wq.queryKey(['chat', 'list']), ['chat', 'list']);
  // The args go LAST, so invalidating ['chat'] or ['chat','list'] reaches
  // every entry underneath regardless of arguments.
  assert.deepStrictEqual(wq.queryKey(['chat', 'list'], { room: 'a' }), ['chat', 'list', { room: 'a' }]);
  assert.deepStrictEqual(wq.queryKey('chat'), ['chat']);

  const prefixed = createQueryUtils(fakeClient(), { prefix: ['wrpc', 1] });
  assert.deepStrictEqual(prefixed.queryKey(['chat', 'list'], {}), ['wrpc', 1, 'chat', 'list', {}]);
});

test('query: queryOptions forwards args and the AbortSignal', async () => {
  const client = fakeClient();
  const wq = createQueryUtils(client);
  const options = wq.queryOptions(['chat', 'send'], { text: 'hi' }, { staleTime: 5000 });

  assert.deepStrictEqual(options.queryKey, ['chat', 'send', { text: 'hi' }]);
  assert.strictEqual(options.staleTime, 5000, 'extra options pass through');
  assert.strictEqual(typeof options.queryFn, 'function');

  const controller = new AbortController();
  assert.deepStrictEqual(await options.queryFn({ signal: controller.signal }), { id: 'made' });
  assert.deepStrictEqual(client.calls[0].args, { text: 'hi' });
  // Forwarding the signal is what makes a cancelled query reach the server
  assert.strictEqual(client.calls[0].options.signal, controller.signal);

  // TanStack always passes a context, but nothing should break without one
  await options.queryFn();
  assert.strictEqual(client.calls[1].options.signal, undefined);

  // No args at all becomes the empty object the client's scaffolding expects
  await wq.queryOptions(['chat', 'send']).queryFn();
  assert.deepStrictEqual(client.calls[2].args, {});
});

test('query: mutationOptions takes its args from the variables', async () => {
  const client = fakeClient();
  const wq = createQueryUtils(client);
  const options = wq.mutationOptions(['chat', 'send'], { retry: 2 });
  assert.deepStrictEqual(options.mutationKey, ['chat', 'send']);
  assert.strictEqual(options.retry, 2);
  assert.deepStrictEqual(await options.mutationFn({ text: 'hi' }), { id: 'made' });
  assert.deepStrictEqual(client.calls[0].args, { text: 'hi' });
  await options.mutationFn();
  assert.deepStrictEqual(client.calls[1].args, {});
});

test('query: paths are resolved lazily, so options survive load() and reconnects', async () => {
  const client = { api: {} };
  const wq = createQueryUtils(client);
  // Building options for a unit that is not loaded yet must NOT throw: a
  // component declares its queries before anything is connected.
  const options = wq.queryOptions(['chat', 'send'], { text: 'hi' });
  await assert.rejects(() => options.queryFn(), /unit 'chat' is not loaded/);

  // ...and once it IS loaded the very same options object works
  const loaded = fakeClient();
  client.api.chat = loaded.api.chat;
  assert.deepStrictEqual(await options.queryFn(), { id: 'made' });
});

test('query: a bad path is refused with something actionable', async () => {
  const client = fakeClient();
  const wq = createQueryUtils(client);
  assert.throws(() => wq.queryOptions('chat'), /a path is \[unit, method\]/);
  assert.throws(() => wq.queryOptions(['chat']), /a path is \[unit, method\]/);
  assert.throws(() => wq.queryOptions(['chat', 'send', 'extra']), /a path is \[unit, method\]/);
  assert.throws(() => wq.queryOptions([1, 2]), /a path is \[unit, method\] of strings/);
  assert.throws(() => wq.mutationOptions(['chat']), /a path is \[unit, method\]/);
  assert.throws(() => wq.subscriptionHandler(['chat'], {}, { queryClient: fakeQueryClient() }), /a path is/);

  // A resolution failure REJECTS, so it lands in the query's error channel
  await assert.rejects(() => wq.queryOptions(['nope', 'send']).queryFn(), /unit 'nope' is not loaded/);
  await assert.rejects(() => wq.queryOptions(['chat', 'nope']).queryFn(), /unit 'chat' has no method 'nope'/);
});

test('query: inherited properties are not methods', async () => {
  const client = fakeClient();
  const wq = createQueryUtils(client);
  // A unit object is an Emitter, so `on` IS a real function on its prototype.
  // Resolving it would call Emitter.prototype.on as though it were a procedure.
  await assert.rejects(() => wq.queryOptions(['chat', 'on']).queryFn(), /unit 'chat' has no method 'on'/);
  await assert.rejects(() => wq.queryOptions(['chat', 'emit']).queryFn(), /has no method 'emit'/);
  await assert.rejects(() => wq.queryOptions(['chat', 'toString']).queryFn(), /has no method 'toString'/);
  // ...and `api` is a plain object, so the same holds one level up
  await assert.rejects(() => wq.queryOptions(['constructor', 'x']).queryFn(), /unit 'constructor' is not loaded/);
  await assert.rejects(() => wq.queryOptions(['__proto__', 'x']).queryFn(), /unit '__proto__' is not loaded/);
});

test('query: a call and a subscription are not interchangeable', async () => {
  const client = fakeClient();
  const wq = createQueryUtils(client, { queryClient: fakeQueryClient() });
  await assert.rejects(
    () => wq.queryOptions(['chat', 'onMessage']).queryFn(),
    /is a subscription — use subscriptionHandler/,
  );
  await assert.rejects(() => wq.mutationOptions(['chat', 'onMessage']).mutationFn(), /is a subscription/);
  assert.throws(() => wq.subscriptionHandler(['chat', 'send']), /is a call — use queryOptions\(\)/);

  client.api.chat.notAMethod = { kind: 'something-else' };
  await assert.rejects(() => wq.queryOptions(['chat', 'notAMethod']).queryFn(), /is not callable/);
  assert.throws(() => wq.subscriptionHandler(['chat', 'notAMethod']), /is not a subscription/);
});

test('query: subscriptionHandler writes every value into the cache', () => {
  const client = fakeClient();
  const queryClient = fakeQueryClient();
  const wq = createQueryUtils(client, { queryClient });
  const seen = [];

  const handle = wq.subscriptionHandler(
    ['chat', 'onMessage'],
    { room: 'a' },
    {
      lastEventId: '7',
      onData: (data) => void seen.push(data),
    },
  );

  const [opened] = client.subscribes;
  assert.deepStrictEqual(opened.args, { room: 'a' });
  assert.strictEqual(opened.options.lastEventId, '7', 'resume is passed straight through');

  opened.options.onData({ text: 'one' });
  opened.options.onData({ text: 'two' });
  assert.deepStrictEqual(
    queryClient.writes.map((write) => write.value),
    [{ text: 'one' }, { text: 'two' }],
  );
  // Default key: the path plus the args, so it matches the query for the same
  // arguments and nothing else.
  assert.deepStrictEqual(queryClient.writes[0].queryKey, ['chat', 'onMessage', { room: 'a' }]);
  assert.deepStrictEqual(seen, [{ text: 'one' }, { text: 'two' }]);

  assert.strictEqual(handle.unsubscribe(), true);
  assert.strictEqual(handle.closed, true);
});

test('query: the default update replaces, and a custom one can accumulate', () => {
  const client = fakeClient();
  const queryClient = fakeQueryClient();
  const wq = createQueryUtils(client, { queryClient });

  wq.subscriptionHandler(
    ['chat', 'onMessage'],
    { room: 'a' },
    {
      queryKey: ['messages'],
      update: (previous, data) => [...(previous ?? []), data.text],
    },
  );
  const [opened] = client.subscribes;
  opened.options.onData({ text: 'one' });
  opened.options.onData({ text: 'two' });
  assert.deepStrictEqual(queryClient.writes[1].value, ['one', 'two']);
  assert.deepStrictEqual(queryClient.writes[1].queryKey, ['messages'], 'an explicit key wins');
});

test('query: subscriptionHandler needs a queryClient, and validates it', () => {
  const client = fakeClient();
  const wq = createQueryUtils(client);
  assert.throws(() => wq.subscriptionHandler(['chat', 'onMessage']), /pass a queryClient with a setQueryData/);
  assert.throws(
    () => wq.subscriptionHandler(['chat', 'onMessage'], {}, { queryClient: { setQueryData: 'no' } }),
    /pass a queryClient with a setQueryData/,
  );
  assert.throws(
    () => wq.subscriptionHandler(['chat', 'onMessage'], {}, { queryClient: fakeQueryClient(), update: 'nope' }),
    /update must be a function/,
  );
  // A per-call queryClient overrides the default one
  const perCall = fakeQueryClient();
  const withDefault = createQueryUtils(client, { queryClient: fakeQueryClient() });
  withDefault.subscriptionHandler(['chat', 'onMessage'], {}, { queryClient: perCall });
  client.subscribes[0].options.onData({ text: 'x' });
  assert.strictEqual(perCall.writes.length, 1);
});

test('query: subscription callbacks are forwarded untouched', () => {
  const client = fakeClient();
  const wq = createQueryUtils(client, { queryClient: fakeQueryClient() });
  const onError = () => {};
  const onEnd = () => {};
  wq.subscriptionHandler(['chat', 'onMessage'], undefined, { onError, onEnd });
  const [opened] = client.subscribes;
  // No args given still opens with the empty object the scaffolding expects
  assert.deepStrictEqual(opened.args, {});
  assert.strictEqual(opened.options.onError, onError);
  assert.strictEqual(opened.options.onEnd, onEnd);
  // onData is wrapped (that is the cache bridge) but must not require a listener
  opened.options.onData({ text: 'x' });
});

test("query: a failing cache write is the subscription's problem, not the client's", () => {
  const client = fakeClient();
  const broken = {
    setQueryData() {
      throw new TypeError('Do not know how to serialize a BigInt');
    },
  };
  const wq = createQueryUtils(client, { queryClient: broken });

  // onData runs synchronously inside the client's packet dispatch: an escaping
  // throw would surface as a client-level 'error' and abandon the packet.
  const errors = [];
  const seen = [];
  wq.subscriptionHandler(
    ['chat', 'onMessage'],
    {},
    { onError: (error) => void errors.push(error), onData: () => void seen.push(1) },
  );
  client.subscribes[0].options.onData({ text: 'x' });
  assert.strictEqual(errors.length, 1);
  assert.match(errors[0].message, /BigInt/);
  assert.deepStrictEqual(seen, [], 'a value that never reached the cache was not delivered');

  // With nobody listening it still propagates — a feed that stopped working
  // must not stop quietly.
  const quiet = fakeClient();
  createQueryUtils(quiet, { queryClient: broken }).subscriptionHandler(['chat', 'onMessage'], {});
  assert.throws(() => quiet.subscribes[0].options.onData({ text: 'x' }), /BigInt/);
});

test('query: a throwing update reaches onError rather than the client', () => {
  const client = fakeClient();
  const queryClient = fakeQueryClient();
  const wq = createQueryUtils(client, { queryClient });
  const errors = [];
  wq.subscriptionHandler(
    ['chat', 'onMessage'],
    {},
    {
      update: () => {
        throw new RangeError('bad shape');
      },
      onError: (error) => void errors.push(error),
    },
  );
  client.subscribes[0].options.onData({ text: 'x' });
  assert.strictEqual(errors.length, 1);
  assert.match(errors[0].message, /bad shape/);
});

test('query: the utils object is frozen', () => {
  const wq = createQueryUtils(fakeClient());
  assert.throws(() => {
    wq.queryOptions = () => {};
  }, TypeError);
});
