'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

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

const createBinder = (backplane, { deliver = noop, log } = {}) => {
  const errors = [];
  const binder = new RoomsBackplane({
    backplane,
    instance: 'node-1',
    deliver,
    log: log ?? { log: noop, error: (error) => errors.push(error) },
  });
  return { binder, errors };
};

test('RoomsBackplane: channel reference counting', async (t) => {
  const backplane = createBackplane();
  const { binder } = createBinder(backplane);
  binder.start();
  await settle();

  await t.test('the broadcast channel is held from the start', () => {
    assert.deepStrictEqual(backplane.state.subscribed, [BROADCAST_CHANNEL]);
  });

  await t.test('a room subscribes once, however many joins', async () => {
    binder.joinRoom('chat');
    binder.joinRoom('chat');
    await settle();
    assert.deepStrictEqual(backplane.state.subscribed, [BROADCAST_CHANNEL, roomChannel('chat')]);
  });

  await t.test('the last leave releases the channel', async () => {
    binder.leaveRoom('chat');
    assert.deepStrictEqual(backplane.state.unsubscribed, [], 'a held channel is not released');
    binder.leaveRoom('chat');
    assert.deepStrictEqual(backplane.state.unsubscribed, [roomChannel('chat')]);
  });

  await t.test('releasing an unknown channel is a no-op', () => {
    assert.doesNotThrow(() => binder.leaveRoom('never-joined'));
  });
});

test('RoomsBackplane: a subscription still in flight', async (t) => {
  await t.test('a room emptied before subscribe lands is unsubscribed on arrival', async () => {
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

  await t.test('a rejoin before subscribe lands keeps the channel', async () => {
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

  await t.test('a failing subscribe is reported and the channel is forgotten', async () => {
    const backplane = createBackplane({
      subscribe: () => Promise.reject(new Error('broker refused')),
    });
    const { binder, errors } = createBinder(backplane);

    binder.joinRoom('chat');
    await settle();
    assert.strictEqual(errors.length, 1);
    assert.match(errors[0].message, /broker refused/);

    // Forgotten, so a later join tries again rather than assuming success.
    binder.joinRoom('chat');
    await settle();
    assert.strictEqual(errors.length, 2);
  });

  await t.test('a backplane that returns no unsubscribe function is tolerated', async () => {
    const backplane = createBackplane({ subscribe: () => undefined });
    const { binder, errors } = createBinder(backplane);
    binder.joinRoom('chat');
    await settle();
    assert.doesNotThrow(() => binder.leaveRoom('chat'));
    assert.deepStrictEqual(errors, []);
  });
});

test('RoomsBackplane: failures while unsubscribing are contained', async (t) => {
  await t.test('a throwing unsubscribe is reported', async () => {
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

  await t.test('a rejected unsubscribe is reported', async () => {
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

test('RoomsBackplane: publishing', async (t) => {
  await t.test('a single room goes to its own channel, anything else to broadcast', () => {
    const backplane = createBackplane();
    const { binder } = createBinder(backplane);
    binder.publish({ rooms: ['chat'], name: 'msg', data: 1 });
    binder.publish({ rooms: ['chat', 'lobby'], name: 'msg', data: 2 });
    binder.publish({ rooms: null, name: 'msg', data: 3 });
    const channels = backplane.state.published.map(([channel]) => channel);
    assert.deepStrictEqual(channels, [roomChannel('chat'), BROADCAST_CHANNEL, BROADCAST_CHANNEL]);
    const envelope = JSON.parse(backplane.state.published[1][1]);
    assert.deepStrictEqual(envelope, { v: 1, instance: 'node-1', rooms: ['chat', 'lobby'], name: 'msg', data: 2 });
  });

  await t.test('a payload that cannot be serialized is reported, not thrown', () => {
    const backplane = createBackplane();
    const { binder, errors } = createBinder(backplane);
    const circular = {};
    circular.self = circular;
    assert.doesNotThrow(() => binder.publish({ rooms: ['chat'], name: 'msg', data: circular }));
    assert.strictEqual(errors.length, 1);
    assert.deepStrictEqual(backplane.state.published, []);
  });

  await t.test('a synchronously throwing publish is reported', () => {
    const backplane = createBackplane({
      publish() {
        throw new Error('broker is down');
      },
    });
    const { binder, errors } = createBinder(backplane);
    assert.doesNotThrow(() => binder.publish({ rooms: ['chat'], name: 'msg', data: 1 }));
    assert.strictEqual(errors.length, 1);
  });

  await t.test('a rejected publish is reported', async () => {
    const backplane = createBackplane({ publish: () => Promise.reject(new Error('publish failed')) });
    const { binder, errors } = createBinder(backplane);
    binder.publish({ rooms: ['chat'], name: 'msg', data: 1 });
    await settle();
    assert.strictEqual(errors.length, 1);
  });
});

test('RoomsBackplane: receiving', async (t) => {
  const deliveries = [];
  const backplane = createBackplane();
  const { binder, errors } = createBinder(backplane, {
    deliver: (rooms, name, data) => {
      if (name === 'explode') throw new Error('delivery blew up');
      deliveries.push([rooms, name, data]);
    },
  });
  binder.start();
  await settle();
  const feed = (envelope) => backplane.state.handlers.get(BROADCAST_CHANNEL)(JSON.stringify(envelope));

  await t.test('a foreign envelope is delivered locally', () => {
    feed({ instance: 'node-2', rooms: ['chat'], name: 'msg', data: 1 });
    assert.deepStrictEqual(deliveries, [[['chat'], 'msg', 1]]);
  });

  await t.test('a null room list means every client', () => {
    deliveries.length = 0;
    feed({ instance: 'node-2', rooms: null, name: 'msg', data: 2 });
    assert.deepStrictEqual(deliveries, [[null, 'msg', 2]]);
  });

  await t.test("this instance's own envelope is dropped", () => {
    deliveries.length = 0;
    feed({ instance: 'node-1', rooms: ['chat'], name: 'msg', data: 3 });
    assert.deepStrictEqual(deliveries, []);
  });

  await t.test('a failing delivery is reported, not thrown at the broker', () => {
    assert.doesNotThrow(() => feed({ instance: 'node-2', rooms: null, name: 'explode' }));
    assert.strictEqual(errors.length, 1);
  });

  await t.test('a pre-parsed envelope works too', () => {
    deliveries.length = 0;
    backplane.state.handlers.get(BROADCAST_CHANNEL)({ instance: 'node-2', rooms: ['chat'], name: 'msg', data: 4 });
    assert.deepStrictEqual(deliveries, [[['chat'], 'msg', 4]]);
  });
});

test('RoomsBackplane: close', async (t) => {
  await t.test('every held channel is released and further work is a no-op', async () => {
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

  await t.test('a subscription that lands after close is undone', async () => {
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

  await t.test('a message arriving after close is ignored', async () => {
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
