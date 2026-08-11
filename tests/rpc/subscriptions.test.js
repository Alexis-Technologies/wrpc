'use strict';

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { Server, WrpcClient, defineRouter, procedure } = require('../../index.js');
const { tracked, createEventLog, createEventStream } = require('../../index.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

// A subscription that never answers would hang the whole run rather than
// fail it: node's runner has no default per-test timeout.
const within = (promise, label, ms = 2000) =>
  Promise.race([
    promise,
    timers.setTimeout(ms).then(() => {
      throw new Error(`timed out waiting for ${label}`);
    }),
  ]);

// Sends a hand-built packet on a bare socket and resolves with the raw
// answer: the scaffolded api deliberately cannot express these mistakes.
const rawPacket = async (url, packet) => {
  const socket = new WebSocket(url);
  await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));
  const answer = new Promise((resolve) => {
    socket.addEventListener('message', ({ data }) => resolve(JSON.parse(data)), { once: true });
  });
  socket.send(JSON.stringify({ args: {}, ...packet }));
  try {
    return await answer;
  } finally {
    socket.close();
  }
};

const createServer = async (router, options = {}) => {
  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    console: quiet,
    timeouts: { bind: 50 },
    retry: 20,
    ...options,
  });
  await server.listen();
  return { server, port: server.address().port };
};

// ---------------------------------------------------------------------------
// The primitives, on their own

test('tracked / createEventLog: the replay buffer', async (t) => {
  const log = createEventLog({ size: 3 });

  await t.test('ids are monotonic and push returns them', () => {
    assert.strictEqual(log.push('a'), '0');
    assert.strictEqual(log.push('b'), '1');
    assert.strictEqual(log.lastEventId, '1');
    assert.strictEqual(log.length, 2);
  });

  await t.test('since() replays everything after an id, as tracked values', () => {
    const missed = log.since('0');
    assert.deepStrictEqual(missed, [tracked('1', 'b')]);
    assert.deepStrictEqual(log.since('1'), [], 'nothing was missed');
  });

  await t.test('no lastEventId means nothing to replay', () => {
    assert.deepStrictEqual(log.since(undefined), []);
    assert.deepStrictEqual(log.since(null), []);
    assert.deepStrictEqual(log.since(''), []);
  });

  await t.test('an evicted id still resumes while nothing after it is missing', () => {
    log.push('c');
    log.push('d'); // evicts 'a' (id 0), leaving 1..3
    assert.strictEqual(log.length, 3);
    // The client saw 0; 1, 2 and 3 are all still held, so nothing is missing.
    assert.deepStrictEqual(log.since('0'), [tracked('1', 'b'), tracked('2', 'c'), tracked('3', 'd')]);
    assert.deepStrictEqual(log.since('1'), [tracked('2', 'c'), tracked('3', 'd')]);
  });

  await t.test('a real gap is an honest null, not a truncated history', () => {
    log.push('e'); // evicts id 1, leaving 2..4
    assert.strictEqual(log.since('0'), null, 'the caller must know it cannot resume');
    assert.deepStrictEqual(log.since('2'), [tracked('3', 'd'), tracked('4', 'e')]);
  });

  await t.test('a nonsense id cannot resume either', () => {
    assert.strictEqual(log.since('not-a-number'), null);
  });

  await t.test('an id newer than everything held means nothing was missed', () => {
    assert.deepStrictEqual(log.since('99'), []);
  });

  await t.test('size must be a positive integer', () => {
    assert.throws(() => createEventLog({ size: 0 }), TypeError);
    assert.throws(() => createEventLog({ size: 1.5 }), TypeError);
  });
});

test('createEventStream: push becomes pull', async (t) => {
  await t.test('values arrive in order, and end() finishes the loop', async () => {
    const stream = createEventStream();
    stream.push(1);
    stream.push(2);
    queueMicrotask(() => {
      stream.push(3);
      stream.end();
    });
    const seen = [];
    for await (const value of stream) seen.push(value);
    assert.deepStrictEqual(seen, [1, 2, 3]);
  });

  await t.test('a consumer waiting is resolved by the next push', async () => {
    const stream = createEventStream();
    const pending = stream.next();
    stream.push('late');
    assert.deepStrictEqual(await pending, { value: 'late', done: false });
  });

  await t.test('an abort signal ends it', async () => {
    const controller = new AbortController();
    const stream = createEventStream({ signal: controller.signal });
    const pending = stream.next();
    controller.abort();
    assert.deepStrictEqual(await pending, { value: undefined, done: true });
    assert.strictEqual(stream.closed, true);
  });

  await t.test('an already-aborted signal yields an empty stream', async () => {
    const controller = new AbortController();
    controller.abort();
    const stream = createEventStream({ signal: controller.signal });
    assert.deepStrictEqual(await stream.next(), { value: undefined, done: true });
  });

  await t.test('the queue is bounded and reports what it dropped', () => {
    const stream = createEventStream({ highWaterMark: 2 });
    stream.push(1);
    stream.push(2);
    stream.push(3);
    assert.strictEqual(stream.length, 2);
    assert.strictEqual(stream.dropped, 1);
  });

  await t.test('fail() throws into the consumer', async () => {
    const stream = createEventStream();
    stream.fail(new Error('upstream died'));
    await assert.rejects(stream.next(), /upstream died/);
  });

  await t.test('pushing after the end is refused', () => {
    const stream = createEventStream();
    stream.end();
    assert.strictEqual(stream.push(1), false);
  });
});

// ---------------------------------------------------------------------------
// The wire

const countdown = defineRouter({
  feed: {
    // An async generator handler IS the subscription declaration — no
    // procedure.subscription() wrapper needed, only the usual access.
    count: {
      access: 'public',
      handler: async function* (_context, { to = 3 } = {}) {
        for (let i = 1; i <= to; i++) yield { n: i };
      },
    },
    // The bare-function shorthand keeps its default access, like any method.
    bare: async function* () {
      yield { secret: true };
    },
    tracked: procedure.subscription({
      access: 'public',
      handler: async function* (_context, _args, { lastEventId }) {
        const log = createEventLog({ size: 10 });
        for (const value of ['a', 'b', 'c']) log.push(value);
        const missed = log.since(lastEventId);
        for (const value of missed ?? []) yield value;
        yield tracked(log.push('live'), 'live');
      },
    }),
    failing: procedure.subscription({
      access: 'public',
      handler: async function* () {
        yield { ok: true };
        const error = new Error('generator blew up');
        error.code = 418;
        throw error;
      },
    }),
    private: procedure.subscription({
      access: 'session',
      handler: async function* () {
        yield { secret: true };
      },
    }),
    plain: procedure({ access: 'public', handler: async () => 'not a subscription' }),
  },
});

test('subscriptions: the wire lifecycle', async (t) => {
  const { server, port } = await createServer(countdown);
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
  t.after(() => void client.close());
  await client.load('feed');

  await t.test('introspection marks a subscription, and the client scaffolds one', () => {
    assert.strictEqual(client.api.feed.count.kind, 'subscription');
    assert.strictEqual(typeof client.api.feed.count.subscribe, 'function');
    assert.strictEqual(typeof client.api.feed.count.iterate, 'function');
    assert.strictEqual(client.api.feed.bare.kind, 'subscription', 'a bare generator is one too');
    assert.strictEqual(typeof client.api.feed.plain, 'function', 'a call is still a plain function');
  });

  await t.test('subscribe delivers every value and then ends', async () => {
    const seen = [];
    const ended = new Promise((resolve) => {
      client.api.feed.count.subscribe({ to: 3 }, { onData: (data) => seen.push(data), onEnd: resolve });
    });
    await within(ended, 'the subscription to end');
    assert.deepStrictEqual(seen, [{ n: 1 }, { n: 2 }, { n: 3 }]);
  });

  await t.test('iterate consumes the same stream with for await', async () => {
    const seen = [];
    const consume = (async () => {
      for await (const value of client.api.feed.count.iterate({ to: 2 })) seen.push(value);
    })();
    await within(consume, 'the iterator to finish');
    assert.deepStrictEqual(seen, [{ n: 1 }, { n: 2 }]);
  });

  await t.test('the handle reports the last tracked eventId', async () => {
    const values = [];
    let handle = null;
    const ended = new Promise((resolve) => {
      handle = client.api.feed.tracked.subscribe({}, { onData: (d) => values.push(d), onEnd: resolve });
    });
    await within(ended, 'the tracked subscription to end');
    assert.deepStrictEqual(values, ['live']);
    assert.strictEqual(handle.lastEventId, '3');
    assert.strictEqual(handle.closed, true);
  });

  await t.test('lastEventId replays what was missed', async () => {
    const values = [];
    const ended = new Promise((resolve) => {
      client.api.feed.tracked.subscribe({}, { lastEventId: '0', onData: (d) => values.push(d), onEnd: resolve });
    });
    await within(ended, 'the resumed subscription to end');
    assert.deepStrictEqual(values, ['b', 'c', 'live'], 'everything after id 0');
  });

  await t.test('a generator that throws ends with its own code', async () => {
    const values = [];
    const failure = await within(
      new Promise((resolve) => {
        client.api.feed.failing.subscribe({}, { onData: (d) => values.push(d), onError: resolve });
      }),
      'the failure',
    );
    assert.deepStrictEqual(values, [{ ok: true }]);
    assert.strictEqual(failure.code, 418);
    assert.strictEqual(failure.message, 'generator blew up');
  });

  await t.test('access control applies', async () => {
    const failure = await within(
      new Promise((resolve) => client.api.feed.private.subscribe({}, { onError: resolve })),
      'the 403',
    );
    assert.strictEqual(failure.code, 403);
  });

  // The two kinds are not interchangeable, and mixing them up has to say so
  // rather than hang. Raw packets here: the scaffolded api cannot express
  // the mistake, which is rather the point of scaffolding it.
  const url = `ws://127.0.0.1:${port}/api`;

  await t.test('subscribing to a call is refused', async () => {
    const answer = await within(rawPacket(url, { type: 'subscribe', id: 's1', method: 'feed/plain' }), 'answer');
    assert.strictEqual(answer.error.code, 400);
    assert.match(answer.error.message, /not a subscription/);
  });

  await t.test('calling a subscription is refused', async () => {
    const answer = await within(rawPacket(url, { type: 'call', id: 's2', method: 'feed/count' }), 'answer');
    assert.strictEqual(answer.error.code, 400);
    assert.match(answer.error.message, /subscription/);
  });
});

test('subscriptions: unsubscribe runs the generator finally', async (t) => {
  let opened = 0;
  let closed = 0;
  const router = defineRouter({
    feed: {
      forever: procedure.subscription({
        access: 'public',
        handler: async function* (_context, _args, { signal }) {
          opened++;
          const stream = createEventStream({ signal });
          const timer = setInterval(() => stream.push({ tick: true }), 5);
          try {
            yield* stream;
          } finally {
            clearInterval(timer);
            closed++;
          }
        },
      }),
    },
  });
  const { server, port } = await createServer(router);
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
  t.after(() => void client.close());
  await client.load('feed');

  await t.test('an explicit unsubscribe closes it server-side', async () => {
    let ticks = 0;
    const handle = client.api.feed.forever.subscribe({}, { onData: () => void ticks++ });
    await waitFor(() => ticks >= 2, 'the subscription never produced');
    handle.unsubscribe();
    assert.strictEqual(handle.closed, true);
    await waitFor(() => closed === 1, "the generator's finally never ran");
    assert.strictEqual(server.rpc.clients.size, 1);
    const [serverClient] = server.rpc.clients;
    await waitFor(() => serverClient.subscriptions.size === 0, 'the server kept the subscription');
  });

  await t.test('breaking out of for await unsubscribes too', async () => {
    const before = closed;
    for await (const value of client.api.feed.forever.iterate()) {
      assert.deepStrictEqual(value, { tick: true });
      break;
    }
    await waitFor(() => closed === before + 1, 'breaking the loop never reached the server');
  });

  await t.test('a disconnect closes everything still running', async () => {
    const before = closed;
    const other = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
    await other.load('feed');
    let ticks = 0;
    other.api.feed.forever.subscribe({}, { onData: () => void ticks++ });
    other.api.feed.forever.subscribe({}, { onData: () => void ticks++ });
    await waitFor(() => ticks >= 2, 'the subscriptions never produced');
    other.close();
    await waitFor(() => closed >= before + 2, 'a dropped connection left generators running');
  });

  assert.ok(opened >= 4);
});

test('subscriptions: a reconnect resumes from the last eventId', async (t) => {
  // The log lives outside the server, so a restart on the same port is a new
  // process from the client's point of view but the same feed from the
  // application's — exactly the case resume exists for.
  const log = createEventLog({ size: 50 });
  const live = new Set();
  const router = defineRouter({
    feed: {
      messages: procedure.subscription({
        access: 'public',
        handler: async function* (_context, _args, { lastEventId, signal }) {
          const missed = log.since(lastEventId);
          assert.notStrictEqual(missed, null, 'the buffer was too small for this test');
          for (const value of missed) yield value;
          const stream = createEventStream({ signal });
          live.add(stream);
          try {
            yield* stream;
          } finally {
            live.delete(stream);
          }
        },
      }),
    },
  });
  const publish = (text) => {
    const id = log.push(text);
    for (const stream of live) stream.push(tracked(id, text));
  };

  const first = await createServer(router);
  const { port } = first;
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    heartbeat: false,
    reconnect: { minDelay: 10, maxDelay: 20, jitter: false },
  });
  t.after(() => void client.close());
  client.on('error', noop);
  await client.load('feed');

  const seen = [];
  const handle = client.api.feed.messages.subscribe({}, { onData: (data) => seen.push(data) });
  await waitFor(() => live.size === 1, 'the subscription never opened');
  publish('one');
  await waitFor(() => seen.length === 1, 'the live value never arrived');
  assert.strictEqual(handle.lastEventId, '0');

  // The outage: values published while nobody is connected are what the
  // resume has to fill in.
  const reconnected = new Promise((resolve) => client.once('reconnect', resolve));
  await first.server.close();
  publish('two');
  publish('three');
  const second = await createServer(router, { port });
  t.after(() => second.server.close());

  const info = await within(reconnected, 'the reconnect');
  assert.strictEqual(info.subscriptions, 1, 'the reconnect re-opened the subscription');
  await waitFor(() => seen.length === 3, `only ${seen.length} values after the resume`);
  assert.deepStrictEqual(seen, ['one', 'two', 'three'], 'nothing missed, nothing duplicated');
  assert.strictEqual(handle.lastEventId, '2');

  publish('four');
  await waitFor(() => seen.length === 4, 'the resumed subscription is not live');
  assert.strictEqual(handle.closed, false);
});

test('subscriptions: limits and duplicate ids', async (t) => {
  const router = defineRouter({
    feed: {
      forever: procedure.subscription({
        access: 'public',
        handler: async function* (_context, _args, { signal }) {
          yield* createEventStream({ signal });
        },
      }),
    },
  });
  const { server, port } = await createServer(router, { maxSubscriptions: 2 });
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
  t.after(() => void client.close());
  await client.load('feed');

  await t.test('past maxSubscriptions the answer is 429', async () => {
    client.api.feed.forever.subscribe();
    client.api.feed.forever.subscribe();
    const failure = await within(
      new Promise((resolve) => client.api.feed.forever.subscribe({}, { onError: resolve })),
      'the 429',
    );
    assert.strictEqual(failure.code, 429);
  });
});
