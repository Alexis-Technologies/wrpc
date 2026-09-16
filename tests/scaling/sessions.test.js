'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { setImmediate: settle } = require('node:timers/promises');

const { createRedisSessionStore } = require('../../scaling.js');
const { SessionManager } = require('../../src/rpc/sessions.js');

// An in-repo fake shaped like ioredis for the commands the store uses:
// get / set(key, value, 'PX', ttl) / del / pexpire, all promise-returning.
class FakeRedis {
  constructor() {
    this.entries = new Map(); // key -> { value, ttl }
    this.commands = [];
  }

  async get(key) {
    this.commands.push(['get', key]);
    return this.entries.get(key)?.value ?? null;
  }

  async set(key, value, ...args) {
    this.commands.push(['set', key, value, ...args]);
    const ttl = args[0] === 'PX' ? args[1] : null;
    this.entries.set(key, { value, ttl });
    return 'OK';
  }

  async del(key) {
    this.commands.push(['del', key]);
    return this.entries.delete(key) ? 1 : 0;
  }

  async pexpire(key, ttl) {
    this.commands.push(['pexpire', key, ttl]);
    const entry = this.entries.get(key);
    if (!entry) return 0;
    entry.ttl = ttl;
    return 1;
  }
}

test('createRedisSessionStore: injection is validated at the boundary', () => {
  assert.throws(() => createRedisSessionStore(), /options\.client must be a Redis client/);
  assert.throws(() => createRedisSessionStore({ client: { get() {} } }), /options\.client must be a Redis client/);
  assert.throws(() => createRedisSessionStore({ client: new FakeRedis(), prefix: 3 }), /prefix must be a string/);
  assert.throws(() => createRedisSessionStore({ client: new FakeRedis(), ttl: -1 }), /ttl must be >= 0/);
});

test('createRedisSessionStore: get/set/delete/touch map onto the client with the prefix and TTL', async () => {
  const redis = new FakeRedis();
  const store = createRedisSessionStore({ client: redis, ttl: 60_000 });
  assert.strictEqual(await store.get('t1'), null);
  await store.set('t1', { userId: 7 });
  assert.deepStrictEqual(redis.commands.at(-1), ['set', 'wrpc:session:t1', '{"userId":7}', 'PX', 60_000]);
  assert.deepStrictEqual(await store.get('t1'), { userId: 7 });
  await store.touch('t1');
  assert.deepStrictEqual(redis.commands.at(-1), ['pexpire', 'wrpc:session:t1', 60_000]);
  await store.delete('t1');
  assert.strictEqual(await store.get('t1'), null);
});

test('createRedisSessionStore: a custom prefix, ttl 0 (no expiry) and a client without pexpire', async () => {
  const redis = new FakeRedis();
  delete redis.pexpire;
  redis.pexpire = undefined;
  const store = createRedisSessionStore({ client: redis, prefix: 'app:s:', ttl: 0 });
  await store.set('t2', { a: 1 });
  assert.deepStrictEqual(redis.commands.at(-1), ['set', 'app:s:t2', '{"a":1}']);
  assert.strictEqual(store.touch, undefined, 'no sliding expiry without PEXPIRE');
});

test('createRedisSessionStore: a corrupt or non-object entry reads as a missing session', async () => {
  const redis = new FakeRedis();
  const store = createRedisSessionStore({ client: redis });
  redis.entries.set('wrpc:session:bad', { value: '{not json', ttl: null });
  redis.entries.set('wrpc:session:arr', { value: '[1,2]', ttl: null });
  assert.strictEqual(await store.get('bad'), null);
  assert.strictEqual(await store.get('arr'), null);
});

test('createRedisSessionStore: plugs into SessionManager and survives an "instance switch"', async () => {
  const redis = new FakeRedis();
  // Two managers over one Redis: two server instances behind a balancer.
  const first = new SessionManager({ store: createRedisSessionStore({ client: redis }) }, false);
  const second = new SessionManager({ store: createRedisSessionStore({ client: redis }) }, false);
  const session = first.create('tok', { userId: 42 });
  session.state.role = 'admin';
  await settle();
  const restored = await second.restore('tok');
  assert.deepStrictEqual({ ...restored.state }, { userId: 42, role: 'admin' });
  await second.destroy('tok');
  assert.strictEqual(await first.restore('tok'), null);
});
