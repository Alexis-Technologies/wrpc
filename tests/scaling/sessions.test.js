'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { setImmediate: settle } = require('node:timers/promises');

const { createRedisSessionStore } = require('../../src/scaling/index.js');
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
    // SET's conditions, as Redis answers them: null when they do not hold.
    if (args.includes('XX') && !this.entries.has(key)) return null;
    if (args.includes('NX') && this.entries.has(key)) return null;
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

describe('createRedisSessionStore', () => {
  it('should validate the injection at the boundary', () => {
    assert.throws(() => createRedisSessionStore(), /options\.client must be a Redis client/);
    assert.throws(() => createRedisSessionStore({ client: { get() {} } }), /options\.client must be a Redis client/);
    assert.throws(() => createRedisSessionStore({ client: new FakeRedis(), prefix: 3 }), /prefix must be a string/);
    assert.throws(() => createRedisSessionStore({ client: new FakeRedis(), ttl: -1 }), /ttl must be >= 0/);
  });

  it('should map get/set/delete/touch onto the client with the prefix and TTL', async () => {
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

  it('should take a custom prefix, ttl 0 (no expiry) and a client without pexpire', async () => {
    const redis = new FakeRedis();
    delete redis.pexpire;
    redis.pexpire = undefined;
    const store = createRedisSessionStore({ client: redis, prefix: 'app:s:', ttl: 0 });
    await store.set('t2', { a: 1 });
    assert.deepStrictEqual(redis.commands.at(-1), ['set', 'app:s:t2', '{"a":1}']);
    assert.strictEqual(store.touch, undefined, 'no sliding expiry without PEXPIRE');
  });

  it('should read a corrupt or non-object entry as a missing session', async () => {
    const redis = new FakeRedis();
    const store = createRedisSessionStore({ client: redis });
    redis.entries.set('wrpc:session:bad', { value: '{not json', ttl: null });
    redis.entries.set('wrpc:session:arr', { value: '[1,2]', ttl: null });
    assert.strictEqual(await store.get('bad'), null);
    assert.strictEqual(await store.get('arr'), null);
  });

  it('should plug into SessionManager and survive an "instance switch"', async () => {
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

  it('should not resurrect the session on a write after a logout on another instance (SET … XX)', async () => {
    const redis = new FakeRedis();
    const first = new SessionManager({ store: createRedisSessionStore({ client: redis }) }, false);
    const second = new SessionManager({ store: createRedisSessionStore({ client: redis }) }, false);
    first.create('tok', { userId: 42 });
    const restored = await second.restore('tok');
    await first.destroy('tok');
    restored.state.role = 'admin';
    await settle();
    assert.strictEqual(await first.restore('tok'), null, 'not resurrected');
    assert.strictEqual(restored.ended, true);
    // The update went out with XX; the creating write without it.
    const sets = redis.commands.filter(([command]) => command === 'set');
    assert.ok(!sets[0].includes('XX'), 'the creating write');
    assert.ok(sets.at(-1).includes('XX'), 'the update');
    const store = createRedisSessionStore({ client: redis });
    assert.strictEqual(await store.set('gone', { a: 1 }, { create: false }), false);
    assert.strictEqual(await store.set('new', { a: 1 }), true);
    assert.strictEqual(await store.set('new', { a: 2 }, { create: false }), true);
  });
});
