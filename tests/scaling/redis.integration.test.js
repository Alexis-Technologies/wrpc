'use strict';

// The Redis backplane and the Redis session stores against a REAL Redis, not
// the in-repo fakes of redis.test.js and sessions.test.js. A fake encodes the
// contract an adapter depends on; this file checks that ioredis and the
// server actually honour it — an expiring SET, PEXPIRE, SET … XX are exactly
// what a fake accepts whatever it is handed. CI's `redis` job runs it against
// a service container; locally, set REDIS_URL:
//
//   REDIS_URL=redis://127.0.0.1:6379 node --test tests/scaling/redis.integration.test.js
//
// Without REDIS_URL every test skips, so `pnpm test` stays self-contained on
// a machine with no Redis. `ioredis` is a devDependency used only here: the
// adapters themselves require nothing and duck-type whatever they are handed.

const { test } = require('node:test');
const assert = require('node:assert');

const { RpcServer, defineRouter, procedure } = require('../../index.js');
const { createRedisAdapter, createRedisSessionStore } = require('../../scaling.js');
const { sealedStore, generateKey } = require('../../encryption.js');
const { decodeAttachments } = require('../../src/attachments.js');
const { WRPC_V2 } = require('../../src/wire.js');
const { waitFor: sharedWaitFor } = require('../helpers/wait.js');

const REDIS_URL = process.env.REDIS_URL;

// Two separate adapters over two separate connection pairs — what two
// processes sharing one Redis actually look like, unlike MemoryBackplane
// where both instances hold the same object.
let Redis = null;
if (REDIS_URL) {
  try {
    Redis = require('ioredis');
  } catch (error) {
    if (process.env.WRPC_INTEGRATION_STRICT) {
      throw new Error('ioredis is not installed (WRPC_INTEGRATION_STRICT is set)', { cause: error });
    }
    Redis = null;
  }
}

const skip = !REDIS_URL ? 'REDIS_URL is not set' : !Redis ? 'ioredis is not installed' : false;

const router = defineRouter({ test: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } });

// A socket-shaped stub: attachSocket only needs the events and a send() —
// and the subprotocol a 2.x client negotiated, which is what lets bytes
// travel as bytes (without it the connection speaks revision 1, as to 1.0).
class FakeSocket {
  constructor() {
    this.sent = [];
    this.listeners = new Map();
    this.remoteAddress = '127.0.0.1';
    this.protocol = WRPC_V2;
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
    // A binary frame is an event whose data holds bytes — an attachments frame.
    this.sent.push(typeof data === 'string' ? JSON.parse(data) : decodeAttachments(data));
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
const waitFor = (predicate, message) => sharedWaitFor(predicate, { message, timeout: 5000, interval: 20 });

// Every run gets its own prefix so a shared Redis (a CI service container
// reused across jobs, a developer's local instance) cannot cross-talk.
const prefix = () => `wrpc-test:${process.pid}:${Number(process.hrtime.bigint() % 1000000n)}`;

const createNode = (t, { instanceId, prefix: keyPrefix, ...options }) => {
  const pub = new Redis(REDIS_URL, { lazyConnect: false, maxRetriesPerRequest: 1 });
  const backplane = createRedisAdapter({ pub, prefix: keyPrefix, logger: false });
  const rpc = new RpcServer({ router, logger: false, backplane, instanceId, ...options });
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

// The cluster's replay protection leans on what a real broker does to two
// channels of one publisher: the counter is one per process, a receiver
// hears `cluster` and its own inbox, and the window has to take whatever
// order Redis hands the two over in.
test('redis backplane: a signed cluster converges, and a copied command does not run twice', { skip }, async (t) => {
  const keyPrefix = prefix();
  const cluster = { secret: 'integration-secret', presenceInterval: 200 };
  const first = createNode(t, { instanceId: 'node-1', prefix: keyPrefix, cluster });
  const second = createNode(t, { instanceId: 'node-2', prefix: keyPrefix, cluster });
  const tap = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
  const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
  t.after(() => Promise.all([tap.quit(), raw.quit()]));
  const inbox = `${keyPrefix}:inst:node-2`;
  const seen = [];
  tap.on('message', (_channel, message) => seen.push(message));
  await tap.subscribe(inbox);

  const there = attach(second);
  there.client.join('chat');
  await waitFor(() => first.cluster.count('chat') === 1, 'presence to replicate under the secret');
  assert.deepStrictEqual(first.cluster.instances().sort(), ['node-1', 'node-2']);

  assert.strictEqual(first.sendTo(there.client.id, 'chat/dm', { text: 'once' }), true);
  await waitFor(() => there.socket.events.length === 1, 'the addressed event to cross');
  const command = seen.find((message) => message.includes('"op":"event"'));
  assert.ok(command, 'the tap saw the signed command');
  await raw.publish(inbox, command);
  await raw.publish(`${keyPrefix}:cluster`, command);
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.strictEqual(there.socket.events.length, 1, 'the copy is refused on its own channel and on the shared one');
  // Bytes take the same addressed channel, as a binary envelope signed over
  // its frame — Redis carries the base64 text of it.
  assert.strictEqual(first.sendTo(there.client.id, 'chat/file', { blob: Uint8Array.of(1, 2, 3) }), true);
  await waitFor(() => there.socket.events.length === 2, 'the binary envelope to cross');
  assert.deepStrictEqual([...there.socket.events[1].data.blob], [1, 2, 3]);
  assert.ok(seen.some((message) => message.startsWith('wrpc-bin:')));
  // And the cluster is still whole after a few presence ticks of real traffic.
  assert.deepStrictEqual(second.cluster.instances().sort(), ['node-1', 'node-2']);
});

// ---------------------------------------------------------------------------
// Session stores

const TOKEN = '0f1e2d3c-4b5a-4978-8695-a4b3c2d1e0f9';
const STATE = { userId: 42, email: 'ada@example.com', roles: ['admin'] };

// One connection per test, and ONE teardown: whatever the test left under
// its prefix is deleted before the connection is quit (two hooks would run
// in the order they were registered — the quit first).
const connect = (t, keyPrefix) => {
  const client = new Redis(REDIS_URL, { lazyConnect: false, maxRetriesPerRequest: 1 });
  t.after(async () => {
    const left = await client.keys(`${keyPrefix}*`);
    if (left.length > 0) await client.del(...left);
    await client.quit();
  });
  return client;
};

test(
  'redis session store: a row expires, slides on touch, is not brought back by an update, and is deleted',
  { skip },
  async (t) => {
    const keyPrefix = `${prefix()}:session:`;
    const client = connect(t, keyPrefix);
    const store = createRedisSessionStore({ client, prefix: keyPrefix, ttl: 60_000, logger: false });
    const key = keyPrefix + TOKEN;

    assert.strictEqual(await store.get(TOKEN), null);
    assert.strictEqual(await store.set(TOKEN, STATE), true);
    assert.deepStrictEqual(await store.get(TOKEN), STATE);
    // SET … PX: the row carries the store's ttl, in milliseconds.
    const ttl = await client.pttl(key);
    assert.ok(ttl > 50_000 && ttl <= 60_000, `the row's ttl is ${ttl} ms`);

    // Sliding expiry: a row about to expire is pushed back to the full ttl.
    await client.pexpire(key, 1000);
    assert.ok((await client.pttl(key)) <= 1000);
    await store.touch(TOKEN);
    assert.ok((await client.pttl(key)) > 50_000, 'touch restored the ttl');

    // An update (`create: false`, SET … XX) writes a row that is there…
    assert.strictEqual(await store.set(TOKEN, { ...STATE, roles: [] }, { create: false }), true);
    assert.deepStrictEqual(await store.get(TOKEN), { ...STATE, roles: [] });
    assert.ok((await client.pttl(key)) > 50_000, 'an update keeps the row expiring');

    // …and does not bring back one that is gone — a logout on another instance.
    await store.delete(TOKEN);
    assert.strictEqual(await store.get(TOKEN), null);
    assert.strictEqual(await store.set(TOKEN, STATE, { create: false }), false);
    assert.strictEqual(await client.exists(key), 0, 'the deleted session stayed deleted');
  },
);

test('sealed session store on redis: neither the token nor the state rests in Redis', { skip }, async (t) => {
  const keyPrefix = `${prefix()}:sealed:`;
  const client = connect(t, keyPrefix);
  const inner = createRedisSessionStore({ client, prefix: keyPrefix, ttl: 60_000, logger: false });
  const store = sealedStore(inner, { keys: generateKey(), logger: false });

  await store.set(TOKEN, STATE);
  assert.deepStrictEqual(await store.get(TOKEN), STATE);

  const keys = await client.keys(`${keyPrefix}*`);
  assert.strictEqual(keys.length, 1, 'one row per token');
  assert.ok(!keys[0].includes(TOKEN), 'the bearer token is not the row key');
  const raw = await client.get(keys[0]);
  assert.ok(!raw.includes('ada@example.com') && !raw.includes(TOKEN), 'the state is not readable at rest');
  const row = JSON.parse(raw);
  assert.deepStrictEqual(Object.keys(row).sort(), ['kid', 's', 'v']);
  assert.strictEqual(row.v, 1);
  assert.ok((await client.pttl(keys[0])) > 50_000, 'the sealed row expires like any other');

  // Sliding expiry reaches the sealed row through its derived key.
  await client.pexpire(keys[0], 1000);
  await store.touch(TOKEN);
  assert.ok((await client.pttl(keys[0])) > 50_000);

  await store.delete(TOKEN);
  assert.strictEqual(await store.get(TOKEN), null);
  assert.deepStrictEqual(await client.keys(`${keyPrefix}*`), []);
});

test(
  'sealed session store on redis: a key rotation moves the row on its next read and signs nobody out',
  { skip },
  async (t) => {
    const keyPrefix = `${prefix()}:rotate:`;
    const client = connect(t, keyPrefix);
    const inner = createRedisSessionStore({ client, prefix: keyPrefix, ttl: 60_000, logger: false });
    const k1 = generateKey();
    const k2 = generateKey();

    const before = sealedStore(inner, { keys: { current: 'k1', ring: { k1 } }, logger: false });
    await before.set(TOKEN, STATE);
    const [old] = await client.keys(`${keyPrefix}*`);

    // The fleet rotates: k2 is current, k1 stays on the ring for the read.
    const after = sealedStore(inner, { keys: { current: 'k2', ring: { k1, k2 } }, logger: false });
    assert.deepStrictEqual(await after.get(TOKEN), STATE, 'the session survived the rotation');
    const moved = await client.keys(`${keyPrefix}*`);
    assert.strictEqual(moved.length, 1, 'still one row per token');
    assert.notStrictEqual(moved[0], old, 'under the new key the row has a new name — the index key rotates too');
    assert.strictEqual(JSON.parse(await client.get(moved[0])).kid, 'k2');
    assert.ok((await client.pttl(moved[0])) > 50_000, 'the migrated row expires');
    assert.strictEqual(await client.exists(old), 0, 'the stale row is gone');

    // A delete clears the slot of every kid, whichever the row sits under.
    await before.set(TOKEN, STATE); // an instance still on the old ring writes
    await after.delete(TOKEN);
    assert.deepStrictEqual(await client.keys(`${keyPrefix}*`), []);
  },
);
