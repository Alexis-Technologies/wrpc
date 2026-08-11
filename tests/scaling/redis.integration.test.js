'use strict';

// The Redis backplane against a REAL Redis, not the in-repo fake of
// redis.test.js. The fake encodes the contract the adapter depends on; this
// file checks that ioredis actually honours that contract. Manual/local only
// — not wired into CI, since it needs a live server. Set REDIS_URL to run it:
//
//   REDIS_URL=redis://127.0.0.1:6379 node --test tests/scaling/redis.integration.test.js
//
// Without REDIS_URL every test skips, so `pnpm test` stays self-contained on
// a machine with no Redis. `ioredis` is a devDependency used only here: the
// adapter itself requires nothing and duck-types whatever it is handed.

const { test } = require('node:test');
const assert = require('node:assert');

const { RpcServer, defineRouter, procedure } = require('../../index.js');
const { createRedisAdapter } = require('../../scaling.js');

const REDIS_URL = process.env.REDIS_URL;

// Two separate adapters over two separate connection pairs — what two
// processes sharing one Redis actually look like, unlike MemoryBackplane
// where both instances hold the same object.
let Redis = null;
if (REDIS_URL) {
  try {
    Redis = require('ioredis');
  } catch {
    Redis = null;
  }
}

const skip = !REDIS_URL ? 'REDIS_URL is not set' : !Redis ? 'ioredis is not installed' : false;

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

const router = defineRouter({ test: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } });

// A socket-shaped stub: attachSocket only needs the events and a send().
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

// A real round trip through Redis is not a microtask, so waiting is polling
// with a deadline rather than draining the job queue.
const waitFor = async (predicate, message, timeout = 5000) => {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail(`timed out waiting for ${message}`);
};

// Every run gets its own prefix so a shared Redis (a CI service container
// reused across jobs, a developer's local instance) cannot cross-talk.
const prefix = () => `wrpc-test:${process.pid}:${Number(process.hrtime.bigint() % 1000000n)}`;

const createNode = (t, { instanceId, prefix: keyPrefix }) => {
  const pub = new Redis(REDIS_URL, { lazyConnect: false, maxRetriesPerRequest: 1 });
  const backplane = createRedisAdapter({ pub, prefix: keyPrefix, console: quiet });
  const rpc = new RpcServer({ router, console: quiet, backplane, instanceId });
  t.after(async () => {
    await rpc.close();
    backplane.close(); // quits the subscriber it duplicated, not `pub`
    await pub.quit();
  });
  return rpc;
};

const attach = (rpc) => {
  const socket = new FakeSocket();
  const client = rpc.attachSocket(socket, { headers: {} });
  return { socket, client };
};

test('redis backplane: a room event crosses two independent adapters', { skip }, async (t) => {
  const keyPrefix = prefix();
  const first = createNode(t, { instanceId: 'node-1', prefix: keyPrefix });
  const second = createNode(t, { instanceId: 'node-2', prefix: keyPrefix });

  const here = attach(first);
  const there = attach(second);
  here.client.join('chat');
  there.client.join('chat');
  // SUBSCRIBE is a round trip: publishing before it lands drops the message.
  await new Promise((resolve) => setTimeout(resolve, 250));

  const sent = first.to('chat').emit('message', { text: 'hi' });
  assert.strictEqual(sent, 1, 'the local count covers this instance only');
  assert.deepStrictEqual(here.socket.events, [{ type: 'event', name: 'message', data: { text: 'hi' } }]);

  await waitFor(() => there.socket.events.length === 1, 'the remote instance to deliver the event');
  assert.deepStrictEqual(there.socket.events, [{ type: 'event', name: 'message', data: { text: 'hi' } }]);

  // Echo suppression: the publisher's own subscriber sees its message come
  // back off the wire and must drop it instead of delivering a second copy.
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.strictEqual(here.socket.events.length, 1, 'the publisher sees exactly one copy');
});

test('redis backplane: broadcast reaches an instance holding no rooms', { skip }, async (t) => {
  const keyPrefix = prefix();
  const first = createNode(t, { instanceId: 'node-1', prefix: keyPrefix });
  const second = createNode(t, { instanceId: 'node-2', prefix: keyPrefix });

  const there = attach(second);
  await new Promise((resolve) => setTimeout(resolve, 250));

  first.broadcast('announce', { up: true });
  await waitFor(() => there.socket.events.length === 1, 'the broadcast to cross');
  assert.deepStrictEqual(there.socket.events, [{ type: 'event', name: 'announce', data: { up: true } }]);
});

test('redis backplane: a prefix isolates two logically separate deployments', { skip }, async (t) => {
  const first = createNode(t, { instanceId: 'node-1', prefix: prefix() });
  const second = createNode(t, { instanceId: 'node-2', prefix: prefix() });

  const here = attach(first);
  const there = attach(second);
  here.client.join('chat');
  there.client.join('chat');
  await new Promise((resolve) => setTimeout(resolve, 250));

  first.to('chat').emit('message', 1);
  await waitFor(() => here.socket.events.length === 1, 'the local delivery');
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.strictEqual(there.socket.events.length, 0, 'a different prefix is a different deployment');
});
