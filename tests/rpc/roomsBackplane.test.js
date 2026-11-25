'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const timers = require('node:timers/promises');

const { RoomsBackplane, BROADCAST_CHANNEL, roomChannel } = require('../../src/rpc/rooms.js');

const noop = () => {};

const settle = async () => {
  for (let i = 0; i < 8; i++) await Promise.resolve();
};

// retain() defers the subscribe by a microtask (so a synchronously throwing
// backplane surfaces as a rejection rather than in the caller's join), which
// is why a test controlling that promise has to let the tick pass first.
const tick = () => Promise.resolve();

// A controllable stand-in for an injected broker: every subscription can be
// held open, resolved late, or made to fail, which is what the reference
// counting around a room's channel has to survive.
const createBackplane = (overrides = {}) => {
  const state = { subscribed: [], unsubscribed: [], published: [], handlers: new Map() };
  const backplane = {
    ...state,
    publish(channel, message) {
      state.published.push([channel, message]);
    },
    subscribe(channel, handler) {
      state.subscribed.push(channel);
      state.handlers.set(channel, handler);
      return () => void state.unsubscribed.push(channel);
    },
    close: noop,
    ...overrides,
  };
  backplane.state = state;
  return backplane;
};

const createBinder = (backplane, { deliver = noop, log, linger = 0 } = {}) => {
  const errors = [];
  // linger: 0 — these tests exercise the immediate release mechanics; the
  // grace window has its own test below.
  const binder = new RoomsBackplane({
    backplane,
    instance: 'node-1',
    deliver,
    linger,
    log: log ?? { log: noop, error: (error) => errors.push(error) },
  });
  return { binder, errors };
};

describe('RoomsBackplane', () => {
  describe('channel reference counting', () => {
    const backplane = createBackplane();
    const { binder } = createBinder(backplane);
    before(async () => {
      binder.start();
      await settle();
    });

    it('should hold the broadcast channel from the start', () => {
      assert.deepStrictEqual(backplane.state.subscribed, [BROADCAST_CHANNEL]);
    });

    it('should subscribe a room once, however many joins', async () => {
      binder.joinRoom('chat');
      binder.joinRoom('chat');
      await settle();
      assert.deepStrictEqual(backplane.state.subscribed, [BROADCAST_CHANNEL, roomChannel('chat')]);
    });

    it('should release the channel on the last leave', async () => {
      binder.leaveRoom('chat');
      assert.deepStrictEqual(backplane.state.unsubscribed, [], 'a held channel is not released');
      binder.leaveRoom('chat');
      assert.deepStrictEqual(backplane.state.unsubscribed, [roomChannel('chat')]);
    });

    it('should treat releasing an unknown channel as a no-op', () => {
      assert.doesNotThrow(() => binder.leaveRoom('never-joined'));
    });
  });

  describe('a subscription still in flight', () => {
    it('should unsubscribe on arrival a room emptied before subscribe lands', async () => {
      let resolveSubscribe = null;
      const unsubscribed = [];
      const backplane = createBackplane({
        subscribe: (channel) =>
          new Promise((resolve) => {
            resolveSubscribe = () => resolve(() => void unsubscribed.push(channel));
          }),
      });
      const { binder } = createBinder(backplane);

      binder.joinRoom('chat');
      await tick();
      binder.leaveRoom('chat'); // the room emptied while subscribe was pending
      assert.deepStrictEqual(unsubscribed, [], 'there is nothing to unsubscribe yet');

      resolveSubscribe();
      await settle();
      assert.deepStrictEqual(unsubscribed, [roomChannel('chat')], 'the late subscription is undone');
    });

    it('should keep the channel on a rejoin before subscribe lands', async () => {
      let resolveSubscribe = null;
      const unsubscribed = [];
      const backplane = createBackplane({
        subscribe: (channel) =>
          new Promise((resolve) => {
            resolveSubscribe = () => resolve(() => void unsubscribed.push(channel));
          }),
      });
      const { binder } = createBinder(backplane);

      binder.joinRoom('chat');
      await tick();
      binder.leaveRoom('chat');
      binder.joinRoom('chat'); // somebody joined again while subscribe was pending
      resolveSubscribe();
      await settle();
      assert.deepStrictEqual(unsubscribed, [], 'the channel is still wanted');
    });

    it('should report, keep and RETRY a failing subscribe — never terminal', async () => {
      let attempts = 0;
      const backplane = createBackplane({
        subscribe: () => {
          attempts++;
          if (attempts < 2) return Promise.reject(new Error('broker refused'));
          return Promise.resolve(() => {});
        },
      });
      const { binder, errors } = createBinder(backplane);

      binder.joinRoom('chat');
      await settle();
      assert.strictEqual(errors.length, 1);
      assert.match(errors[0].message, /broker refused/);
      // The instance is degraded, not silently deaf-forever.
      assert.strictEqual(binder.healthy, false);
      // The capped-backoff retry (500ms first step) re-subscribes on its own.
      await timers.setTimeout(600);
      await settle();
      assert.strictEqual(attempts, 2, 'the retry ran');
      assert.strictEqual(binder.healthy, true, 'recovery clears the degraded state');
    });

    it('should tolerate a backplane that returns no unsubscribe function', async () => {
      const backplane = createBackplane({ subscribe: () => undefined });
      const { binder, errors } = createBinder(backplane);
      binder.joinRoom('chat');
      await settle();
      assert.doesNotThrow(() => binder.leaveRoom('chat'));
      assert.deepStrictEqual(errors, []);
    });
  });

  describe('failures while unsubscribing are contained', () => {
    it('should report a throwing unsubscribe', async () => {
      const backplane = createBackplane({
        subscribe: () => () => {
          throw new Error('unsubscribe blew up');
        },
      });
      const { binder, errors } = createBinder(backplane);
      binder.joinRoom('chat');
      await settle();
      assert.doesNotThrow(() => binder.leaveRoom('chat'));
      assert.strictEqual(errors.length, 1);
    });

    it('should report a rejected unsubscribe', async () => {
      const backplane = createBackplane({
        subscribe: () => () => Promise.reject(new Error('unsubscribe failed')),
      });
      const { binder, errors } = createBinder(backplane);
      binder.joinRoom('chat');
      await settle();
      binder.leaveRoom('chat');
      await settle();
      assert.strictEqual(errors.length, 1);
    });
  });

  describe('publishing', () => {
    it('should send a single room to its own channel, anything else to broadcast', () => {
      const backplane = createBackplane();
      const { binder } = createBinder(backplane);
      binder.publish({ rooms: ['chat'], name: 'msg', data: 1 });
      binder.publish({ rooms: ['chat', 'lobby'], name: 'msg', data: 2 });
      binder.publish({ rooms: null, name: 'msg', data: 3 });
      const channels = backplane.state.published.map(([channel]) => channel);
      assert.deepStrictEqual(channels, [roomChannel('chat'), BROADCAST_CHANNEL, BROADCAST_CHANNEL]);
      const envelope = JSON.parse(backplane.state.published[1][1]);
      // epoch/seq are the loss-detection fields (their own test below).
      assert.deepStrictEqual(envelope, {
        v: 1,
        instance: 'node-1',
        epoch: binder.epoch,
        seq: 1,
        rooms: ['chat', 'lobby'],
        name: 'msg',
        data: 2,
      });
    });

    it('should report a payload that cannot be serialized, not throw', () => {
      const backplane = createBackplane();
      const { binder, errors } = createBinder(backplane);
      const circular = {};
      circular.self = circular;
      assert.doesNotThrow(() => binder.publish({ rooms: ['chat'], name: 'msg', data: circular }));
      assert.strictEqual(errors.length, 1);
      assert.deepStrictEqual(backplane.state.published, []);
    });

    it('should report a synchronously throwing publish', () => {
      const backplane = createBackplane({
        publish() {
          throw new Error('broker is down');
        },
      });
      const { binder, errors } = createBinder(backplane);
      assert.doesNotThrow(() => binder.publish({ rooms: ['chat'], name: 'msg', data: 1 }));
      assert.strictEqual(errors.length, 1);
    });

    it('should report a rejected publish', async () => {
      const backplane = createBackplane({ publish: () => Promise.reject(new Error('publish failed')) });
      const { binder, errors } = createBinder(backplane);
      binder.publish({ rooms: ['chat'], name: 'msg', data: 1 });
      await settle();
      assert.strictEqual(errors.length, 1);
    });
  });

  describe('receiving', () => {
    const deliveries = [];
    const backplane = createBackplane();
    const { binder, errors } = createBinder(backplane, {
      deliver: (rooms, name, data) => {
        if (name === 'explode') throw new Error('delivery blew up');
        deliveries.push([rooms, name, data]);
      },
    });
    before(async () => {
      binder.start();
      await settle();
    });
    const feed = (envelope) => backplane.state.handlers.get(BROADCAST_CHANNEL)(JSON.stringify(envelope));

    it('should deliver a foreign envelope locally', () => {
      feed({ instance: 'node-2', rooms: ['chat'], name: 'msg', data: 1 });
      assert.deepStrictEqual(deliveries, [[['chat'], 'msg', 1]]);
    });

    it('should treat a null room list as every client', () => {
      deliveries.length = 0;
      feed({ instance: 'node-2', rooms: null, name: 'msg', data: 2 });
      assert.deepStrictEqual(deliveries, [[null, 'msg', 2]]);
    });

    it("should drop this instance's own envelope", () => {
      deliveries.length = 0;
      feed({ instance: 'node-1', rooms: ['chat'], name: 'msg', data: 3 });
      assert.deepStrictEqual(deliveries, []);
    });

    it('should report a failing delivery, not throw at the broker', () => {
      assert.doesNotThrow(() => feed({ instance: 'node-2', rooms: null, name: 'explode' }));
      assert.strictEqual(errors.length, 1);
    });

    it('should accept a pre-parsed envelope too', () => {
      deliveries.length = 0;
      backplane.state.handlers.get(BROADCAST_CHANNEL)({ instance: 'node-2', rooms: ['chat'], name: 'msg', data: 4 });
      assert.deepStrictEqual(deliveries, [[['chat'], 'msg', 4]]);
    });
  });

  describe('close', () => {
    it('should release every held channel and make further work a no-op', async () => {
      const backplane = createBackplane();
      const { binder } = createBinder(backplane);
      binder.start();
      binder.joinRoom('chat');
      await settle();

      binder.close();
      assert.deepStrictEqual(backplane.state.unsubscribed.sort(), [BROADCAST_CHANNEL, roomChannel('chat')].sort());

      binder.publish({ rooms: ['chat'], name: 'msg', data: 1 });
      assert.deepStrictEqual(backplane.state.published, [], 'a closed binder publishes nothing');
      binder.joinRoom('late');
      await settle();
      assert.strictEqual(backplane.state.subscribed.includes(roomChannel('late')), false);
      assert.doesNotThrow(() => binder.close(), 'close is idempotent');
    });

    it('should undo a subscription that lands after close', async () => {
      let resolveSubscribe = null;
      const unsubscribed = [];
      const backplane = createBackplane({
        subscribe: (channel) =>
          new Promise((resolve) => {
            resolveSubscribe = () => resolve(() => void unsubscribed.push(channel));
          }),
      });
      const { binder } = createBinder(backplane);
      binder.joinRoom('chat');
      await tick();
      binder.close();
      resolveSubscribe();
      await settle();
      assert.deepStrictEqual(unsubscribed, [roomChannel('chat')]);
    });

    it('should ignore a message arriving after close', async () => {
      const deliveries = [];
      const backplane = createBackplane();
      const { binder } = createBinder(backplane, { deliver: (...args) => deliveries.push(args) });
      binder.start();
      await settle();
      const handler = backplane.state.handlers.get(BROADCAST_CHANNEL);
      binder.close();
      handler(JSON.stringify({ instance: 'node-2', rooms: null, name: 'msg', data: 1 }));
      assert.deepStrictEqual(deliveries, []);
    });
  });

  it('should absorb a reconnect bounce in the linger window', async () => {
    const subscribed = [];
    const unsubscribed = [];
    const backplane = createBackplane({
      subscribe: (channel) => {
        subscribed.push(channel);
        return Promise.resolve(() => void unsubscribed.push(channel));
      },
    });
    const { binder } = createBinder(backplane, { linger: 50 });
    binder.joinRoom('me');
    await settle();
    // The single member bounces: leave + rejoin inside the window.
    binder.leaveRoom('me');
    binder.joinRoom('me');
    await settle();
    assert.strictEqual(subscribed.filter((c) => c.includes('me')).length, 1, 'no broker round trip at all');
    assert.deepStrictEqual(unsubscribed, [], 'the channel never released');
    // A leave the window EXPIRES on does release.
    binder.leaveRoom('me');
    await timers.setTimeout(80);
    await settle();
    assert.strictEqual(unsubscribed.length, 1);
  });

  it('should bound the publish counters by maxTracked, and restart an evicted channel under a suffixed epoch', async (t) => {
    const backplane = createBackplane();
    const binder = new RoomsBackplane({
      backplane,
      instance: 'node-1',
      epoch: 'e1',
      linger: 0,
      maxTracked: 2,
      deliver: noop,
      log: { log: noop, error: noop, warn: noop },
    });
    t.after(() => binder.close());
    const publish = (room) => {
      binder.publish({ rooms: [room], name: 'ev', data: null });
      const [, message] = backplane.state.published.at(-1);
      const { epoch, seq } = JSON.parse(message);
      return `${epoch}#${seq}`;
    };
    // maxTracked 2. Two channels fill the young generation; the third
    // rotates it (young → old) and starts under the rotation's epoch.
    assert.deepStrictEqual([publish('a'), publish('a'), publish('b')], ['e1#1', 'e1#2', 'e1#1']);
    assert.strictEqual(publish('c'), 'e1.1#1'); // old = {a, b}, young = {c}
    // a and b come back from the old generation with their counts intact;
    // b's return finds young full and rotates again.
    assert.strictEqual(publish('a'), 'e1#3'); // old = {b}, young = {c, a}
    assert.strictEqual(publish('b'), 'e1#2'); // old = {c, a}, young = {b}
    assert.strictEqual(publish('d'), 'e1.2#1'); // young = {b, d}
    assert.strictEqual(publish('c'), 'e1.1#2'); // taken over, then rotated: old = {b, d}, young = {c}
    // a was dropped with the generation before: a new count, a new epoch.
    assert.strictEqual(publish('a'), 'e1.3#1'); // young = {c, a}
    assert.strictEqual(publish('e'), 'e1.4#1'); // old = {c, a}, young = {e}
    assert.strictEqual(publish('b'), 'e1.4#1'); // dropped with {b, d}: new
    // Whatever the churn, the table never exceeds two generations, and a
    // channel that fell out starts over under the current rotation.
    for (let i = 0; i < 1000; i++) publish(`room-${i}`);
    assert.match(publish('a'), /^e1\.(50\d|5[1-9]\d)#1$/);
    assert.throws(() => new RoomsBackplane({ backplane, instance: 'x', deliver: noop, maxTracked: 0 }), /maxTracked/);
    assert.throws(() => new RoomsBackplane({ backplane, instance: 'x', deliver: noop, maxTracked: 1.5 }), /maxTracked/);
  });

  describe('loss detection through epoch and seq', () => {
    const backplane = createBackplane();
    const gaps = [];
    const delivered = [];
    const binder = new RoomsBackplane({
      backplane,
      instance: 'node-1',
      epoch: 'e1',
      linger: 0,
      deliver: (rooms, name) => delivered.push(name),
      onGap: (gap) => gaps.push(gap),
      log: { log: noop, error: noop, warn: noop },
    });
    before(async () => {
      binder.start();
      binder.joinRoom('chat');
      await settle();
    });
    after(() => binder.close());
    const channel = roomChannel('chat');
    const send = (instance, epoch, seq, name = 'ev') =>
      backplane.state.handlers.get(channel)(
        JSON.stringify({ v: 1, instance, epoch, seq, rooms: ['chat'], name, data: null }),
      );

    it('should carry the epoch and a per-channel seq on every envelope published', () => {
      binder.publish({ rooms: ['chat'], name: 'a', data: 1 });
      binder.publish({ rooms: ['chat'], name: 'b', data: 2 });
      binder.publish({ rooms: null, name: 'c', data: 3 });
      const envelopes = backplane.state.published.map(([, message]) => JSON.parse(message));
      assert.deepStrictEqual(
        envelopes.map((e) => [e.epoch, e.seq]),
        [
          ['e1', 1],
          ['e1', 2],
          ['e1', 1],
        ],
        'seq counts per channel: the broadcast channel starts its own',
      );
      assert.strictEqual(binder.epoch, 'e1');
    });

    it('should report no gap for a contiguous sequence', () => {
      send('node-2', 'x', 1);
      send('node-2', 'x', 2);
      send('node-2', 'x', 3);
      assert.deepStrictEqual(gaps, []);
      assert.strictEqual(delivered.length, 3);
    });

    it('should report the envelopes missed by a jump, once', () => {
      send('node-2', 'x', 7);
      assert.deepStrictEqual(gaps, [{ channel, instance: 'node-2', missed: 3, seq: 7 }]);
      send('node-2', 'x', 8);
      assert.strictEqual(gaps.length, 1);
    });

    it('should never regress the cursor on a late or duplicate envelope', () => {
      send('node-2', 'x', 5);
      send('node-2', 'x', 8);
      send('node-2', 'x', 9);
      assert.strictEqual(gaps.length, 1);
    });

    it('should reset rather than report on a new epoch (the publisher restarted)', () => {
      send('node-2', 'y', 1);
      send('node-2', 'y', 2);
      // A rotation suffix is just another epoch to a receiver: a restart of
      // that channel's count, no gap.
      send('node-2', 'y.1', 1);
      send('node-2', 'y.1', 2);
      assert.strictEqual(gaps.length, 1);
      assert.strictEqual(gaps.length, 1);
    });

    it('should start an unknown publisher clean, and leave an old instance without the fields untracked', () => {
      send('node-3', 'z', 40);
      backplane.state.handlers.get(channel)(
        JSON.stringify({ v: 1, instance: 'node-4', rooms: ['chat'], name: 'old', data: null }),
      );
      assert.strictEqual(gaps.length, 1);
      assert.strictEqual(delivered.at(-1), 'old');
    });

    it('should contain a throwing onGap and still deliver the envelope', async () => {
      const errors = [];
      const loud = new RoomsBackplane({
        backplane,
        instance: 'node-9',
        linger: 0,
        deliver: (rooms, name) => delivered.push(name),
        onGap: () => {
          throw new Error('boom');
        },
        log: { log: noop, warn: noop, error: (entry) => errors.push(entry) },
      });
      loud.start();
      await settle(); // the subscribe is deferred a microtask; the handler is now loud's
      const broadcast = backplane.state.handlers.get(BROADCAST_CHANNEL);
      broadcast(JSON.stringify({ v: 1, instance: 'n', epoch: 'q', seq: 1, rooms: null, name: 'one', data: null }));
      broadcast(JSON.stringify({ v: 1, instance: 'n', epoch: 'q', seq: 5, rooms: null, name: 'two', data: null }));
      assert.strictEqual(errors.length, 1);
      assert.strictEqual(delivered.at(-1), 'two');
      loud.close();
    });
  });

  it('should disarm a subscribe retry on close(), and subscribe nothing after it', async () => {
    let attempts = 0;
    const backplane = createBackplane({
      subscribe() {
        attempts++;
        return Promise.reject(new Error('backplane down'));
      },
    });
    const { binder, errors } = createBinder(backplane);
    binder.start();
    await settle();
    assert.strictEqual(attempts, 1);
    assert.strictEqual(binder.healthy, false, 'a channel is pending');
    binder.close();
    const said = errors.length;
    // The retry was armed for up to half a second after the failure. It used
    // to fire after close: a subscribe on a backplane already shut, and its
    // outcome logged into a finished shutdown.
    await timers.setTimeout(700);
    assert.strictEqual(attempts, 1, 'no retry after close');
    assert.strictEqual(errors.length, said, 'and nothing more is logged');
    assert.strictEqual(binder.healthy, true, 'nothing is left pending');
  });
});

describe('RpcServer', () => {
  it('should count a backplane gap by the channel KIND, never by the room name', async (t) => {
    const { RpcServer, defineRouter, procedure } = require('../../index.js');
    const { createMetrics, point } = require('../helpers/metrics.js');
    const { recorder } = require('../helpers/recorder.js');
    const metrics = createMetrics();
    t.after(() => metrics.provider.shutdown());
    const backplane = createBackplane();
    const log = recorder();
    const rpc = new RpcServer({
      router: defineRouter({ x: { ping: procedure({ access: 'public', handler: async () => 1 }) } }),
      backplane,
      instanceId: 'node-1',
      logger: log.writer,
      telemetry: { meter: metrics.meter },
    });
    t.after(() => rpc.close());
    await settle();
    // A member on this instance retains the room's channel.
    const socket = {
      events: [],
      on: noop,
      once: noop,
      off: noop,
      send: noop,
      close: noop,
      terminate: noop,
      emit: noop,
    };
    const client = rpc.attachSocket(socket, { headers: {} });
    client.join('secret-project-x');
    await settle();
    const envelope = (channel, seq, rooms) =>
      backplane.state.handlers.get(channel)(
        JSON.stringify({ v: 1, instance: 'node-2', epoch: 'e', seq, rooms, name: 'ev', data: null }),
      );
    const channel = roomChannel('secret-project-x');
    envelope(channel, 1, ['secret-project-x']);
    envelope(channel, 4, ['secret-project-x']); // two missed
    envelope(BROADCAST_CHANNEL, 1, null);
    envelope(BROADCAST_CHANNEL, 2, null);
    envelope(BROADCAST_CHANNEL, 7, null); // four missed
    const exported = await metrics.collect();
    const gaps = exported.find((metric) => metric.descriptor.name === 'wrpc.server.backplane.gaps');
    assert.ok(gaps, 'the counter was exported');
    assert.strictEqual(
      point(exported, 'wrpc.server.backplane.gaps', (a) => a['wrpc.channel.kind'] === 'room').value,
      2,
    );
    assert.strictEqual(
      point(exported, 'wrpc.server.backplane.gaps', (a) => a['wrpc.channel.kind'] === 'broadcast').value,
      4,
    );
    for (const dataPoint of gaps.dataPoints) {
      assert.deepStrictEqual(Object.keys(dataPoint.attributes), ['wrpc.channel.kind'], 'no room name on the metric');
    }
    // The exact channel and count are the log line's.
    assert.deepStrictEqual(
      log.all('backplane.gap').map((e) => [e.channel, e.missed]),
      [
        [channel, 2],
        [BROADCAST_CHANNEL, 4],
      ],
    );
  });
});
