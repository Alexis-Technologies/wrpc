'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { RpcServer } = require('../../index.js');
const { defineRouter, procedure } = require('../../index.js');
const { MemoryBackplane } = require('../../scaling.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

const settle = async () => {
  for (let i = 0; i < 8; i++) await Promise.resolve();
};

const router = defineRouter({ test: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } });

// A socket-shaped stub: RpcServer.attachSocket only needs the events and a
// send(), which is exactly enough to watch what a client would receive.
class FakeSocket {
  constructor() {
    this.sent = [];
    this.listeners = new Map();
    this.remoteAddress = '127.0.0.1';
  }

  on(event, listener) {
    const list = this.listeners.get(event) ?? [];
    list.push(listener);
    this.listeners.set(event, list);
  }

  once(event, listener) {
    this.on(event, listener);
  }

  emit(event, ...args) {
    for (const listener of this.listeners.get(event) ?? []) listener(...args);
  }

  send(data) {
    this.sent.push(JSON.parse(data));
    return true;
  }

  close() {
    this.emit('close');
  }

  terminate() {
    this.emit('close');
  }

  get events() {
    return this.sent.filter((packet) => packet.type === 'event');
  }
}

const attach = (rpc) => {
  const socket = new FakeSocket();
  const client = rpc.attachSocket(socket, { headers: {} });
  return { socket, client };
};

const createPair = (backplane) => {
  const first = new RpcServer({ router, logger: false, backplane, instanceId: 'node-1' });
  const second = new RpcServer({ router, logger: false, backplane, instanceId: 'node-2' });
  return { first, second };
};

test('backplane: a room event crosses instances', async (t) => {
  const backplane = new MemoryBackplane();
  const { first, second } = createPair(backplane);
  t.after(async () => {
    await first.close();
    await second.close();
    backplane.close();
  });

  const here = attach(first);
  const there = attach(second);
  here.client.join('chat');
  there.client.join('chat');
  await settle(); // the room's channel subscription lands asynchronously

  const sent = first.to('chat').emit('message', { text: 'hi' });
  assert.strictEqual(sent, 1, 'the local count covers this instance only');
  assert.deepStrictEqual(here.socket.events, [{ type: 'event', name: 'message', data: { text: 'hi' } }]);

  await settle();
  assert.deepStrictEqual(
    there.socket.events,
    [{ type: 'event', name: 'message', data: { text: 'hi' } }],
    'the remote instance delivered it to its own member',
  );
});

test('backplane: echo suppression keeps the publisher from re-delivering', async (t) => {
  const backplane = new MemoryBackplane();
  const { first, second } = createPair(backplane);
  t.after(async () => {
    await first.close();
    await second.close();
    backplane.close();
  });

  const here = attach(first);
  here.client.join('chat');
  attach(second).client.join('chat');
  await settle();

  first.to('chat').emit('message', 1);
  await settle();
  assert.strictEqual(here.socket.events.length, 1, 'the publisher sees exactly one copy');
});

test('backplane: a client in two targeted rooms gets one copy on every instance', async (t) => {
  const backplane = new MemoryBackplane();
  const { first, second } = createPair(backplane);
  t.after(async () => {
    await first.close();
    await second.close();
    backplane.close();
  });

  const there = attach(second);
  there.client.join('chat');
  there.client.join('lobby');
  await settle();

  first.to('chat', 'lobby').emit('message', 1);
  await settle();
  assert.strictEqual(there.socket.events.length, 1, 'a multi-room emit is deduplicated remotely too');
});

test('backplane: broadcast reaches every instance', async (t) => {
  const backplane = new MemoryBackplane();
  const { first, second } = createPair(backplane);
  t.after(async () => {
    await first.close();
    await second.close();
    backplane.close();
  });

  const there = attach(second); // no rooms at all
  await settle();

  first.broadcast('announce', { up: true });
  await settle();
  assert.deepStrictEqual(there.socket.events, [{ type: 'event', name: 'announce', data: { up: true } }]);
});

test('backplane: an instance without members of the room is not reached', async (t) => {
  const backplane = new MemoryBackplane();
  const { first, second } = createPair(backplane);
  t.after(async () => {
    await first.close();
    await second.close();
    backplane.close();
  });

  const there = attach(second);
  there.client.join('other');
  await settle();

  first.to('chat').emit('message', 1);
  await settle();
  assert.deepStrictEqual(there.socket.events, []);
});

test('backplane: a disconnect releases the room and its channel', async (t) => {
  const backplane = new MemoryBackplane();
  const { first, second } = createPair(backplane);
  t.after(async () => {
    await first.close();
    await second.close();
    backplane.close();
  });

  const there = attach(second);
  there.client.join('chat');
  await settle();
  assert.strictEqual(second.rooms.count('chat'), 1);

  there.socket.emit('close');
  await settle();
  assert.strictEqual(second.rooms.has('chat'), false, 'the empty room is gone');

  // With nobody subscribed to the room channel the publish reaches nobody,
  // and — the point of the assertion — nothing throws on the way.
  assert.doesNotThrow(() => first.to('chat').emit('message', 1));
  await settle();
  assert.deepStrictEqual(there.socket.events, []);
});

test('backplane: a broken backplane never breaks local delivery', async (t) => {
  const errors = [];
  const broken = {
    publish() {
      throw new Error('backplane is down');
    },
    subscribe() {
      return () => {};
    },
    close: noop,
  };
  const rpc = new RpcServer({
    router,
    backplane: broken,
    logger: { ...quiet, error: (error) => errors.push(error) },
  });
  t.after(() => rpc.close());

  const { socket, client } = attach(rpc);
  client.join('chat');
  const sent = rpc.to('chat').emit('message', { text: 'hi' });

  assert.strictEqual(sent, 1, 'local delivery is unaffected');
  assert.deepStrictEqual(socket.events, [{ type: 'event', name: 'message', data: { text: 'hi' } }]);
  assert.strictEqual(errors.length, 1, 'the failure is reported');
});

test('backplane: malformed envelopes are ignored', async (t) => {
  const backplane = new MemoryBackplane();
  const rpc = new RpcServer({ router, logger: false, backplane, instanceId: 'node-1' });
  t.after(async () => {
    await rpc.close();
    backplane.close();
  });

  const { socket, client } = attach(rpc);
  client.join('chat');
  await settle();

  for (const message of [
    'not json',
    'null',
    '[]',
    JSON.stringify({ instance: 'node-2' }), // no name
    JSON.stringify({ instance: 'node-2', name: '', rooms: ['chat'] }),
    JSON.stringify({ instance: 'node-2', name: 'x', rooms: 'chat' }), // rooms not an array
  ]) {
    backplane.publish('room:chat', message);
    backplane.publish('broadcast', message);
  }
  await settle();
  assert.deepStrictEqual(socket.events, []);
});

test('backplane: a non-conforming backplane is rejected at construction', () => {
  assert.throws(() => new RpcServer({ router, logger: false, backplane: { publish: noop } }), TypeError);
});

test('backplane: rooms work identically without one', async (t) => {
  const rpc = new RpcServer({ router, logger: false });
  t.after(() => rpc.close());
  const a = attach(rpc);
  const b = attach(rpc);
  a.client.join('chat');
  b.client.join('chat');

  assert.strictEqual(rpc.to('chat').except(b.client).emit('message', 1), 1);
  assert.strictEqual(a.socket.events.length, 1);
  assert.strictEqual(b.socket.events.length, 0);
  assert.strictEqual(rpc.instanceId.length > 0, true, 'an instance id exists even with no backplane');
});
