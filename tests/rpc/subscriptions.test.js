'use strict';

const timers = require('node:timers/promises');
const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

const { Server, WrpcClient, defineRouter, procedure } = require('../../index.js');
const { tracked, createEventLog, createEventStream } = require('../../index.js');
const { waitFor } = require('../helpers/wait.js');

const noop = () => {};

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
    logger: false,
    timeouts: { bind: 50 },
    retry: 20,
    ...options,
  });
  await server.listen();
  return { server, port: server.address().port };
};

// ---------------------------------------------------------------------------
// The primitives, on their own

describe('tracked / createEventLog: the replay buffer', () => {
  // A fixed epoch pins the ids for the assertions; the default is random
  // per instance (that is what makes cross-process resume honest).
  const log = createEventLog({ size: 3, epoch: 'e' });

  it('should mint monotonic, epoch-stamped ids, and return them from push', () => {
    assert.strictEqual(log.push('a'), 'e.0');
    assert.strictEqual(log.push('b'), 'e.1');
    assert.strictEqual(log.lastEventId, 'e.1');
    assert.strictEqual(log.epoch, 'e');
    assert.strictEqual(log.length, 2);
  });

  it('should replay everything after an id from since(), as tracked values', () => {
    const missed = log.since('e.0');
    assert.deepStrictEqual(missed, [tracked('e.1', 'b')]);
    assert.deepStrictEqual(log.since('e.1'), [], 'nothing was missed');
  });

  it('should replay nothing without a lastEventId', () => {
    assert.deepStrictEqual(log.since(undefined), []);
    assert.deepStrictEqual(log.since(null), []);
    assert.deepStrictEqual(log.since(''), []);
  });

  it('should still resume an evicted id while nothing after it is missing', () => {
    log.push('c');
    log.push('d'); // evicts 'a' (id e.0), leaving e.1..e.3
    assert.strictEqual(log.length, 3);
    // The client saw e.0; e.1..e.3 are all still held, nothing is missing.
    assert.deepStrictEqual(log.since('e.0'), [tracked('e.1', 'b'), tracked('e.2', 'c'), tracked('e.3', 'd')]);
    assert.deepStrictEqual(log.since('e.1'), [tracked('e.2', 'c'), tracked('e.3', 'd')]);
  });

  it('should answer a real gap with an honest null, not a truncated history', () => {
    log.push('e'); // evicts id e.1, leaving e.2..e.4
    assert.strictEqual(log.since('e.0'), null, 'the caller must know it cannot resume');
    assert.deepStrictEqual(log.since('e.2'), [tracked('e.3', 'd'), tracked('e.4', 'e')]);
  });

  it('should not resume a foreign epoch — another process minted it', () => {
    assert.strictEqual(log.since('other.2'), null);
    assert.strictEqual(log.since('2'), null, 'a pre-epoch numeric id is foreign too');
  });

  it('should not resume a nonsense id either', () => {
    assert.strictEqual(log.since('not-a-number'), null);
    assert.strictEqual(log.since('e.NaN'), null);
  });

  it('should treat an id newer than everything held as nothing missed', () => {
    assert.deepStrictEqual(log.since('e.99'), []);
  });

  it('should require size to be a positive integer and epoch to be dot-free', () => {
    assert.throws(() => createEventLog({ size: 0 }), TypeError);
    assert.throws(() => createEventLog({ size: 1.5 }), TypeError);
    assert.throws(() => createEventLog({ epoch: 'a.b' }), TypeError);
  });
});

describe('createEventStream: push becomes pull', () => {
  it('should deliver values in order, and finish the loop on end()', async () => {
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

  it('should resolve a waiting consumer with the next push', async () => {
    const stream = createEventStream();
    const pending = stream.next();
    stream.push('late');
    assert.deepStrictEqual(await pending, { value: 'late', done: false });
  });

  it('should end on an abort signal', async () => {
    const controller = new AbortController();
    const stream = createEventStream({ signal: controller.signal });
    const pending = stream.next();
    controller.abort();
    assert.deepStrictEqual(await pending, { value: undefined, done: true });
    assert.strictEqual(stream.closed, true);
  });

  it('should yield an empty stream for an already-aborted signal', async () => {
    const controller = new AbortController();
    controller.abort();
    const stream = createEventStream({ signal: controller.signal });
    assert.deepStrictEqual(await stream.next(), { value: undefined, done: true });
  });

  it('should bound the queue and report what it dropped', () => {
    const stream = createEventStream({ highWaterMark: 2 });
    stream.push(1);
    stream.push(2);
    stream.push(3);
    assert.strictEqual(stream.length, 2);
    assert.strictEqual(stream.dropped, 1);
  });

  it('should throw into the consumer on fail()', async () => {
    const stream = createEventStream();
    stream.fail(new Error('upstream died'));
    await assert.rejects(stream.next(), /upstream died/);
  });

  it('should refuse a push after the end', () => {
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
        const log = createEventLog({ size: 10, epoch: 'e' });
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

describe('subscriptions', () => {
  describe('the wire lifecycle', () => {
    let server = null;
    let client = null;
    let url = '';
    before(async () => {
      let port = 0;
      ({ server, port } = await createServer(countdown));
      url = `ws://127.0.0.1:${port}/api`;
      client = await WrpcClient.connect(url, { heartbeat: false });
      await client.load('feed');
    });
    after(async () => {
      await server?.close();
      void client?.close();
    });

    it('should mark a subscription in introspection, and the client scaffolds one', () => {
      assert.strictEqual(client.api.feed.count.kind, 'subscription');
      assert.strictEqual(typeof client.api.feed.count.subscribe, 'function');
      assert.strictEqual(typeof client.api.feed.count.iterate, 'function');
      assert.strictEqual(client.api.feed.bare.kind, 'subscription', 'a bare generator is one too');
      assert.strictEqual(typeof client.api.feed.plain, 'function', 'a call is still a plain function');
    });

    it('should deliver every value on subscribe and then end', async () => {
      const seen = [];
      const ended = new Promise((resolve) => {
        client.api.feed.count.subscribe({ to: 3 }, { onData: (data) => seen.push(data), onEnd: resolve });
      });
      await within(ended, 'the subscription to end');
      assert.deepStrictEqual(seen, [{ n: 1 }, { n: 2 }, { n: 3 }]);
    });

    it('should consume the same stream with for await on iterate', async () => {
      const seen = [];
      const consume = (async () => {
        for await (const value of client.api.feed.count.iterate({ to: 2 })) seen.push(value);
      })();
      await within(consume, 'the iterator to finish');
      assert.deepStrictEqual(seen, [{ n: 1 }, { n: 2 }]);
    });

    it('should still end the iterator when an onEnd throws', async () => {
      // The stream has to be ended even when a terminal listener blew up:
      // skipping it leaves a for-await parked in next() with nothing coming.
      const escalated = [];
      const onError = (error) => void escalated.push(error.message);
      client.on('error', onError);
      const seen = [];
      const consume = (async () => {
        const values = client.api.feed.count.iterate(
          { to: 2 },
          {
            onEnd: () => {
              throw new Error('terminal listener blew up');
            },
          },
        );
        for await (const value of values) seen.push(value);
      })();
      await within(consume, 'the iterator to finish despite a throwing onEnd');
      assert.deepStrictEqual(seen, [{ n: 1 }, { n: 2 }]);
      assert.deepStrictEqual(escalated, ['terminal listener blew up']);
      client.off('error', onError);
    });

    it('should report the last tracked eventId on the handle', async () => {
      const values = [];
      let handle = null;
      const ended = new Promise((resolve) => {
        handle = client.api.feed.tracked.subscribe({}, { onData: (d) => values.push(d), onEnd: resolve });
      });
      await within(ended, 'the tracked subscription to end');
      assert.deepStrictEqual(values, ['live']);
      assert.strictEqual(handle.lastEventId, 'e.3');
      assert.strictEqual(handle.closed, true);
    });

    it('should replay what was missed from lastEventId', async () => {
      const values = [];
      const ended = new Promise((resolve) => {
        client.api.feed.tracked.subscribe({}, { lastEventId: 'e.0', onData: (d) => values.push(d), onEnd: resolve });
      });
      await within(ended, 'the resumed subscription to end');
      assert.deepStrictEqual(values, ['b', 'c', 'live'], 'everything after id 0');
    });

    it('should end a generator that throws with its own code', async () => {
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

    it('should apply access control', async () => {
      const failure = await within(
        new Promise((resolve) => client.api.feed.private.subscribe({}, { onError: resolve })),
        'the 403',
      );
      assert.strictEqual(failure.code, 403);
    });

    // The two kinds are not interchangeable, and mixing them up has to say so
    // rather than hang. Raw packets here (on the url the before hook kept):
    // the scaffolded api cannot express the mistake, which is rather the
    // point of scaffolding it.

    it('should refuse subscribing to a call', async () => {
      const answer = await within(rawPacket(url, { type: 'subscribe', id: 's1', method: 'feed/plain' }), 'answer');
      assert.strictEqual(answer.error.code, 400);
      assert.match(answer.error.message, /not a subscription/);
    });

    it('should refuse calling a subscription', async () => {
      const answer = await within(rawPacket(url, { type: 'call', id: 's2', method: 'feed/count' }), 'answer');
      assert.strictEqual(answer.error.code, 400);
      assert.match(answer.error.message, /subscription/);
    });
  });

  describe('unsubscribe runs the generator finally', () => {
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
    let server = null;
    let client = null;
    let port = 0;
    before(async () => {
      ({ server, port } = await createServer(router));
      client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
      await client.load('feed');
    });
    after(async () => {
      await server?.close();
      void client?.close();
    });

    it('should close it server-side on an explicit unsubscribe', async () => {
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

    it('should unsubscribe too when breaking out of for await', async () => {
      const closedBefore = closed;
      for await (const value of client.api.feed.forever.iterate()) {
        assert.deepStrictEqual(value, { tick: true });
        break;
      }
      await waitFor(() => closed === closedBefore + 1, 'breaking the loop never reached the server');
    });

    it('should close everything still running on a disconnect', async () => {
      const closedBefore = closed;
      const other = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
      await other.load('feed');
      let ticks = 0;
      other.api.feed.forever.subscribe({}, { onData: () => void ticks++ });
      other.api.feed.forever.subscribe({}, { onData: () => void ticks++ });
      await waitFor(() => ticks >= 2, 'the subscriptions never produced');
      other.close();
      await waitFor(() => closed >= closedBefore + 2, 'a dropped connection left generators running');
    });

    // close() used to end an iterate() consumer's stream and tell a
    // subscribe() consumer nothing at all, so a listener-based caller never
    // learned its feed was dead.
    it('should end every live subscription on close(), both flavours', async () => {
      const other = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
      await other.load('feed');

      const ends = [];
      const handles = [
        other.api.feed.forever.subscribe({}, { onEnd: () => void ends.push('first') }),
        other.api.feed.forever.subscribe({}, { onEnd: () => void ends.push('second') }),
      ];
      // ...and an iterator, which was already being told
      const iterated = (async () => {
        for await (const value of other.api.feed.forever.iterate()) void value;
        return 'iterator';
      })();
      let ticks = 0;
      other.api.feed.forever.subscribe({}, { onData: () => void ticks++ });
      await waitFor(() => ticks >= 1, 'the subscriptions never produced');

      other.close();

      assert.deepStrictEqual(ends, ['first', 'second'], 'every callback subscriber is told, in order');
      assert.strictEqual(await within(iterated, 'the iterator to finish'), 'iterator');
      for (const handle of handles) {
        assert.strictEqual(handle.closed, true);
        assert.strictEqual(handle.unsubscribe(), false, 'nothing is left to unsubscribe');
      }
    });

    it('should keep onError quiet on close(), which is not an unsubscribe', async () => {
      const other = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
      await other.load('feed');
      const seen = [];
      other.api.feed.forever.subscribe(
        {},
        {
          onEnd: () => void seen.push('end'),
          onError: (error) => void seen.push(`error:${error.message}`),
        },
      );
      // An explicit unsubscribe is silent — the caller already knows
      const quietHandle = other.api.feed.forever.subscribe({}, { onEnd: () => void seen.push('unsubscribed') });
      quietHandle.unsubscribe();
      other.close();
      assert.deepStrictEqual(seen, ['end'], 'a clean close is an end, never an error');
    });

    it('should not strand the others or the teardown when a listener throws', async () => {
      const other = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
      await other.load('feed');
      const escalated = [];
      other.on('error', (error) => void escalated.push(error.message));
      const ends = [];
      other.api.feed.forever.subscribe(
        {},
        {
          onEnd: () => {
            throw new Error('listener blew up');
          },
        },
      );
      other.api.feed.forever.subscribe({}, { onEnd: () => void ends.push('after') });
      const iterated = (async () => {
        for await (const value of other.api.feed.forever.iterate()) void value;
      })();

      assert.doesNotThrow(() => other.close());
      assert.deepStrictEqual(ends, ['after'], 'one bad listener must not rob the next of its signal');
      assert.deepStrictEqual(escalated, ['listener blew up']);
      // ...and the stream still ends, or an iterate() consumer waits forever
      await within(iterated, 'the iterator to finish despite a throwing listener');
      // The teardown ran past the throw: deregistering happens before the
      // transport is closed, and the socket itself closes a tick later.
      assert.strictEqual(WrpcClient.connections.has(other), false);
      await waitFor(() => other.active === false, 'the transport stayed open');
      // Every subscription above opened its own generator (the former group's closing check).
      assert.ok(opened >= 4);
    });
  });

  it('should resume from the last eventId on a reconnect', async (t) => {
    // The log lives outside the server, so a restart on the same port is a new
    // process from the client's point of view but the same feed from the
    // application's — exactly the case resume exists for.
    const log = createEventLog({ size: 50, epoch: 'e' });
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
    assert.strictEqual(handle.lastEventId, 'e.0');

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
    assert.strictEqual(handle.lastEventId, 'e.2');

    publish('four');
    await waitFor(() => seen.length === 4, 'the resumed subscription is not live');
    assert.strictEqual(handle.closed, false);
  });

  describe('limits and duplicate ids', () => {
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
    let server = null;
    let client = null;
    before(async () => {
      let port = 0;
      ({ server, port } = await createServer(router, { maxSubscriptions: 2 }));
      client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
      await client.load('feed');
    });
    after(async () => {
      await server?.close();
      void client?.close();
    });

    it('should answer 429 past maxSubscriptions', async () => {
      client.api.feed.forever.subscribe();
      client.api.feed.forever.subscribe();
      const failure = await within(
        new Promise((resolve) => client.api.feed.forever.subscribe({}, { onError: resolve })),
        'the 429',
      );
      assert.strictEqual(failure.code, 429);
    });
  });
});
