'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { FakeRedis } = require('./fake-redis.js');

const { createRedisAdapter, isBackplane } = require('../../src/scaling/index.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

describe('createRedisAdapter', () => {
  describe('injection, validated structurally', () => {
    it('should require a publisher', () => {
      assert.throws(() => createRedisAdapter({}), TypeError);
      assert.throws(() => createRedisAdapter({ pub: {} }), TypeError);
    });

    it('should require a publisher that supports duplicate() without a subscriber', () => {
      const pub = { publish: noop };
      assert.throws(() => createRedisAdapter({ pub }), /duplicate/);
    });

    it('should require subscribe() and on() of an explicit subscriber', () => {
      const pub = { publish: noop };
      assert.throws(() => createRedisAdapter({ pub, sub: { subscribe: noop } }), TypeError);
    });

    it('should yield a backplane from a well-shaped pair', () => {
      const adapter = createRedisAdapter({ pub: new FakeRedis() });
      assert.strictEqual(isBackplane(adapter), true);
      assert.strictEqual(adapter.name, 'redis');
      adapter.close();
    });
  });

  describe('publish and subscribe over the fake', () => {
    let pub = null;
    let adapter = null;
    before(() => {
      pub = new FakeRedis();
      adapter = createRedisAdapter({ pub });
    });
    after(() => adapter.close());

    it('should namespace channels with the prefix', async () => {
      const seen = [];
      adapter.subscribe('room:chat', (message) => seen.push(message));
      adapter.publish('room:chat', '{"n":1}');
      await null;
      assert.deepStrictEqual(pub.published, [['wrpc:room:chat', '{"n":1}']]);
      assert.deepStrictEqual(seen, ['{"n":1}']);
    });

    it('should subscribe upstream only once for a second handler on one channel', async () => {
      const subscriber = adapter;
      const first = [];
      const second = [];
      subscriber.subscribe('fanout', (m) => first.push(m));
      subscriber.subscribe('fanout', (m) => second.push(m));
      adapter.publish('fanout', 'x');
      await null;
      assert.deepStrictEqual(first, ['x']);
      assert.deepStrictEqual(second, ['x']);
    });

    it('should ignore messages on unheld channels', async () => {
      const seen = [];
      adapter.subscribe('held', (m) => seen.push(m));
      pub.bus.emit('publish', 'wrpc:not-held', 'x');
      await null;
      assert.deepStrictEqual(seen, []);
    });
  });

  it('should take a custom prefix and an explicit subscriber', async () => {
    const pub = new FakeRedis();
    const sub = pub.duplicate();
    const adapter = createRedisAdapter({ pub, sub, prefix: 'app' });
    const seen = [];
    adapter.subscribe('room:chat', (message) => seen.push(message));
    assert.ok(sub.channels.has('app:room:chat'), 'the explicit subscriber is the one subscribed');
    adapter.publish('room:chat', 'x');
    await null;
    assert.deepStrictEqual(seen, ['x']);
    adapter.close();
  });

  it('should release the upstream channel on the last unsubscribe', async () => {
    const pub = new FakeRedis();
    const sub = pub.duplicate();
    const adapter = createRedisAdapter({ pub, sub });
    const seen = [];
    // The SUBSCRIBE's own answer: an unsubscribe once Redis acknowledged it.
    const off = await adapter.subscribe('room:chat', (m) => seen.push(m));
    const other = await adapter.subscribe('room:chat', noop);

    off();
    assert.ok(sub.channels.has('wrpc:room:chat'), 'a channel with handlers left stays subscribed');
    adapter.publish('room:chat', 'still-held');
    await null;
    assert.deepStrictEqual(seen, [], 'the removed handler stops hearing');

    other();
    assert.strictEqual(sub.channels.size, 0, 'the last handler releases the channel');
    assert.doesNotThrow(other, 'unsubscribing twice is safe');
  });

  describe('failures, isolated and never thrown at the caller', () => {
    it('should report a publish rejection, not propagate it', async () => {
      const errors = [];
      const pub = new FakeRedis();
      pub.failPublish = true;
      const adapter = createRedisAdapter({ pub, logger: { ...quiet, error: (e) => errors.push(e) } });
      assert.doesNotThrow(() => adapter.publish('room:chat', 'x'));
      await null;
      await null;
      assert.strictEqual(errors.length, 1);
      adapter.close();
    });

    it('should not let a throwing handler stop the others', async () => {
      const errors = [];
      const pub = new FakeRedis();
      const adapter = createRedisAdapter({ pub, logger: { ...quiet, error: (e) => errors.push(e) } });
      const seen = [];
      adapter.subscribe('chan', () => {
        throw new Error('handler blew up');
      });
      adapter.subscribe('chan', (m) => seen.push(m));
      adapter.publish('chan', 'x');
      await null;
      assert.deepStrictEqual(seen, ['x']);
      assert.strictEqual(errors.length, 1);
      adapter.close();
    });
  });

  describe('the thinner ends of the client contract', () => {
    it('should still detach a subscriber with removeListener but no off', () => {
      const pub = new FakeRedis();
      const sub = pub.duplicate();
      sub.off = undefined; // node's EventEmitter has both; a wrapper may not
      const adapter = createRedisAdapter({ pub, sub });
      adapter.subscribe('room:chat', noop);
      adapter.close();
      assert.strictEqual(sub.listenerCount('message'), 0);
    });

    it('should drop the handler of a subscriber without unsubscribe() anyway', async () => {
      const pub = new FakeRedis();
      const sub = pub.duplicate();
      sub.unsubscribe = undefined;
      const adapter = createRedisAdapter({ pub, sub });
      const seen = [];
      const off = await adapter.subscribe('room:chat', (m) => seen.push(m));
      off();
      adapter.publish('room:chat', 'x');
      await null;
      assert.deepStrictEqual(seen, [], 'the handler is gone even if the channel stays open upstream');
      assert.doesNotThrow(() => adapter.close());
    });

    it('should report a synchronously throwing client, not propagate it', async () => {
      const errors = [];
      const logger = { ...quiet, error: (error) => errors.push(error) };
      const sub = {
        subscribe() {
          throw new Error('subscribe blew up');
        },
        unsubscribe() {
          throw new Error('unsubscribe blew up');
        },
        on: noop,
      };
      const pub = {
        publish() {
          throw new Error('publish blew up');
        },
      };
      const adapter = createRedisAdapter({ pub, sub, logger });
      // Never thrown at the caller — and never hidden from it: the answer is
      // the SUBSCRIBE's, so a subscribe that failed is a rejection.
      let subscribed = null;
      assert.doesNotThrow(() => {
        subscribed = adapter.subscribe('room:chat', noop);
      });
      await assert.rejects(subscribed, /subscribe blew up/);
      assert.strictEqual(errors.length, 1, 'the failed subscribe is reported');
      adapter.publish('room:chat', 'x');
      assert.strictEqual(errors.length, 2, 'the failed publish is reported');
      assert.doesNotThrow(() => adapter.close());
    });

    it('should report an unsubscribe that throws, not propagate it', async () => {
      const errors = [];
      const logger = { ...quiet, error: (error) => errors.push(error) };
      const sub = new FakeRedis();
      sub.unsubscribe = () => {
        throw new Error('unsubscribe blew up');
      };
      const adapter = createRedisAdapter({ pub: new FakeRedis(), sub, logger });
      const off = await adapter.subscribe('room:chat', noop);
      assert.doesNotThrow(off);
      assert.strictEqual(errors.length, 1, 'the failed unsubscribe is reported');
    });

    it('should reject a non-function handler', () => {
      const adapter = createRedisAdapter({ pub: new FakeRedis() });
      assert.throws(() => adapter.subscribe('chan', null), TypeError);
      adapter.close();
    });

    it('should yield an inert unsubscribe when subscribing after close', () => {
      const adapter = createRedisAdapter({ pub: new FakeRedis() });
      adapter.close();
      const off = adapter.subscribe('chan', noop);
      assert.strictEqual(typeof off, 'function');
      assert.doesNotThrow(off);
    });
  });

  describe('close, releasing the subscriber it opened itself', () => {
    it('should quit a duplicated subscriber — nobody else can', async () => {
      const pub = new FakeRedis();
      const adapter = createRedisAdapter({ pub });
      adapter.subscribe('room:chat', noop);
      adapter.close();
      // The duplicate lives only inside the adapter's closure, so leaving it
      // connected would strand a Redis connection with no handle to it.
      const sub = pub.bus.duplicates.at(-1);
      assert.strictEqual(sub.quitCalls, 1);
      assert.strictEqual(sub.channels.size, 0);
    });

    it('should disconnect a client with only disconnect()', () => {
      const pub = new FakeRedis();
      const created = [];
      pub.duplicate = () => {
        const sub = new FakeRedis(pub.bus);
        sub.quit = undefined;
        sub.disconnect = () => void created.push('disconnected');
        return sub;
      };
      createRedisAdapter({ pub }).close();
      assert.deepStrictEqual(created, ['disconnected']);
    });

    it('should simply release a client with neither', () => {
      const pub = new FakeRedis();
      pub.duplicate = () => {
        const sub = new FakeRedis(pub.bus);
        sub.quit = undefined;
        return sub;
      };
      assert.doesNotThrow(() => createRedisAdapter({ pub }).close());
    });

    it('should report a throwing quit, not propagate it', () => {
      const errors = [];
      const pub = new FakeRedis();
      pub.duplicate = () => {
        const sub = new FakeRedis(pub.bus);
        sub.quit = () => {
          throw new Error('quit blew up');
        };
        return sub;
      };
      const adapter = createRedisAdapter({ pub, logger: { ...quiet, error: (e) => errors.push(e) } });
      assert.doesNotThrow(() => adapter.close());
      assert.strictEqual(errors.length, 1);
    });
  });

  it('should release subscriptions on close, but not the injected clients', async () => {
    const pub = new FakeRedis();
    const sub = pub.duplicate();
    const adapter = createRedisAdapter({ pub, sub });
    const seen = [];
    adapter.subscribe('room:chat', (m) => seen.push(m));
    assert.strictEqual(sub.channels.size, 1);

    adapter.close();
    assert.strictEqual(sub.channels.size, 0, 'subscriptions are released');
    assert.strictEqual(sub.listenerCount('message'), 0, 'the message listener is detached');

    adapter.publish('room:chat', 'x');
    await null;
    assert.deepStrictEqual(seen, [], 'a closed adapter publishes nothing');
    assert.deepStrictEqual(pub.published, [], 'the injected clients are still usable');
    assert.strictEqual(typeof adapter.subscribe('room:chat', noop), 'function');
    adapter.close(); // idempotent
  });

  it('should answer subscribe() with the SUBSCRIBE Redis refuses, and leave no handler behind', async () => {
    const errors = [];
    const logger = { ...quiet, error: (error) => errors.push(error) };
    const pub = new FakeRedis();
    const sub = pub.duplicate();
    const real = sub.subscribe.bind(sub);
    let refuse = true;
    sub.subscribe = async (name) => {
      if (refuse) throw new Error('NOPERM this user has no permissions to access one of the channels');
      return real(name);
    };
    const adapter = createRedisAdapter({ pub, sub, logger });
    const seen = [];
    const handler = (message) => seen.push(message);
    // It used to answer an unsubscribe function at once, whatever Redis said:
    // the retry of everything above the adapter never ran.
    const first = adapter.subscribe('room:chat', handler);
    // A second handler that joins while the SUBSCRIBE is in flight fails with it.
    const joined = adapter.subscribe('room:chat', noop);
    await assert.rejects(first, /NOPERM/);
    await assert.rejects(joined, /NOPERM/);
    assert.strictEqual(errors.length, 1, 'said once');
    // The retry succeeds: one handler, one delivery — not the failed attempt's too.
    refuse = false;
    const off = await adapter.subscribe('room:chat', handler);
    adapter.publish('room:chat', 'once');
    await null;
    await null;
    assert.deepStrictEqual(seen, ['once']);
    off();
    adapter.close();
  });
});

describe('RpcServer over the redis adapter', () => {
  it('should retry a refused SUBSCRIBE, and not be healthy meanwhile', async (t) => {
    const { RpcServer } = require('../../src/rpc/core.js');
    const { defineRouter } = require('../../src/rpc/router.js');
    const { waitFor } = require('../helpers/wait.js');
    const pub = new FakeRedis();
    const sub = pub.duplicate();
    const real = sub.subscribe.bind(sub);
    let refuse = true;
    let attempts = 0;
    sub.subscribe = async (name) => {
      attempts++;
      if (refuse) throw new Error('NOPERM');
      return real(name);
    };
    const backplane = createRedisAdapter({ pub, sub, logger: quiet });
    const rpc = new RpcServer({ router: defineRouter({}), logger: false, backplane });
    t.after(() => rpc.close());
    await waitFor(() => attempts >= 2, 'the first subscribes were attempted');
    // Deaf on its channels, and it says so: it used to answer `true` here.
    assert.strictEqual(rpc.healthy, false);
    refuse = false;
    await waitFor(() => rpc.healthy === true, { message: 'the retry subscribed', timeout: 5000 });
  });
});
