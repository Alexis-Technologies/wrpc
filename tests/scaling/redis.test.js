'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { EventEmitter } = require('node:events');

const { createRedisAdapter, isBackplane } = require('../../scaling.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

// An in-repo fake shaped like ioredis: publish/subscribe/unsubscribe return
// promises, every channel of a connection arrives through one 'message'
// event, and duplicate() hands out a second connection. Running the adapter
// against a real Redis is a separate, optional CI job — the contract this
// fake encodes is what the adapter actually depends on.
class FakeRedis extends EventEmitter {
  constructor(bus = new EventEmitter()) {
    super();
    bus.setMaxListeners(0);
    bus.duplicates = bus.duplicates ?? [];
    this.bus = bus;
    this.channels = new Set();
    this.published = [];
    this.failPublish = false;
    this.#listen();
  }

  #listen() {
    this.bus.on('publish', (channel, message) => {
      if (!this.channels.has(channel)) return;
      this.emit('message', channel, message);
    });
  }

  duplicate() {
    const sub = new FakeRedis(this.bus);
    this.bus.duplicates.push(sub);
    return sub;
  }

  async publish(channel, message) {
    if (this.failPublish) throw new Error('redis is down');
    this.published.push([channel, message]);
    this.bus.emit('publish', channel, message);
    return 1;
  }

  async subscribe(channel) {
    this.channels.add(channel);
    return this.channels.size;
  }

  async unsubscribe(channel) {
    this.channels.delete(channel);
    return this.channels.size;
  }

  async quit() {
    this.quitCalls = (this.quitCalls ?? 0) + 1;
    return 'OK';
  }
}

test('createRedisAdapter: injection is validated structurally', async (t) => {
  await t.test('a publisher is required', () => {
    assert.throws(() => createRedisAdapter({}), TypeError);
    assert.throws(() => createRedisAdapter({ pub: {} }), TypeError);
  });

  await t.test('without a subscriber the publisher must support duplicate()', () => {
    const pub = { publish: noop };
    assert.throws(() => createRedisAdapter({ pub }), /duplicate/);
  });

  await t.test('an explicit subscriber needs subscribe() and on()', () => {
    const pub = { publish: noop };
    assert.throws(() => createRedisAdapter({ pub, sub: { subscribe: noop } }), TypeError);
  });

  await t.test('a well-shaped pair yields a backplane', () => {
    const adapter = createRedisAdapter({ pub: new FakeRedis() });
    assert.strictEqual(isBackplane(adapter), true);
    assert.strictEqual(adapter.name, 'redis');
    adapter.close();
  });
});

test('createRedisAdapter: publish and subscribe over the fake', async (t) => {
  const pub = new FakeRedis();
  const adapter = createRedisAdapter({ pub });
  t.after(() => adapter.close());

  await t.test('channels are namespaced with the prefix', async () => {
    const seen = [];
    adapter.subscribe('room:chat', (message) => seen.push(message));
    adapter.publish('room:chat', '{"n":1}');
    await null;
    assert.deepStrictEqual(pub.published, [['wrpc:room:chat', '{"n":1}']]);
    assert.deepStrictEqual(seen, ['{"n":1}']);
  });

  await t.test('a second handler on one channel subscribes upstream only once', async () => {
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

  await t.test('messages on unheld channels are ignored', async () => {
    const seen = [];
    adapter.subscribe('held', (m) => seen.push(m));
    pub.bus.emit('publish', 'wrpc:not-held', 'x');
    await null;
    assert.deepStrictEqual(seen, []);
  });
});

test('createRedisAdapter: a custom prefix and an explicit subscriber', async () => {
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

test('createRedisAdapter: unsubscribe releases the upstream channel last', async () => {
  const pub = new FakeRedis();
  const sub = pub.duplicate();
  const adapter = createRedisAdapter({ pub, sub });
  const seen = [];
  const off = adapter.subscribe('room:chat', (m) => seen.push(m));
  const other = adapter.subscribe('room:chat', noop);

  off();
  assert.ok(sub.channels.has('wrpc:room:chat'), 'a channel with handlers left stays subscribed');
  adapter.publish('room:chat', 'still-held');
  await null;
  assert.deepStrictEqual(seen, [], 'the removed handler stops hearing');

  other();
  assert.strictEqual(sub.channels.size, 0, 'the last handler releases the channel');
  assert.doesNotThrow(other, 'unsubscribing twice is safe');
});

test('createRedisAdapter: failures are isolated, never thrown at the caller', async (t) => {
  await t.test('a publish rejection is reported, not propagated', async () => {
    const errors = [];
    const pub = new FakeRedis();
    pub.failPublish = true;
    const adapter = createRedisAdapter({ pub, console: { ...quiet, error: (e) => errors.push(e) } });
    assert.doesNotThrow(() => adapter.publish('room:chat', 'x'));
    await null;
    await null;
    assert.strictEqual(errors.length, 1);
    adapter.close();
  });

  await t.test('a throwing handler does not stop the others', async () => {
    const errors = [];
    const pub = new FakeRedis();
    const adapter = createRedisAdapter({ pub, console: { ...quiet, error: (e) => errors.push(e) } });
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

test('createRedisAdapter: tolerates the thinner ends of the client contract', async (t) => {
  await t.test('a subscriber with removeListener but no off still detaches', () => {
    const pub = new FakeRedis();
    const sub = pub.duplicate();
    sub.off = undefined; // node's EventEmitter has both; a wrapper may not
    const adapter = createRedisAdapter({ pub, sub });
    adapter.subscribe('room:chat', noop);
    adapter.close();
    assert.strictEqual(sub.listenerCount('message'), 0);
  });

  await t.test('a subscriber without unsubscribe() drops its handler anyway', async () => {
    const pub = new FakeRedis();
    const sub = pub.duplicate();
    sub.unsubscribe = undefined;
    const adapter = createRedisAdapter({ pub, sub });
    const seen = [];
    const off = adapter.subscribe('room:chat', (m) => seen.push(m));
    off();
    adapter.publish('room:chat', 'x');
    await null;
    assert.deepStrictEqual(seen, [], 'the handler is gone even if the channel stays open upstream');
    assert.doesNotThrow(() => adapter.close());
  });

  await t.test('a synchronously throwing client is reported, not propagated', async () => {
    const errors = [];
    const console = { ...quiet, error: (error) => errors.push(error) };
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
    const adapter = createRedisAdapter({ pub, sub, console });
    const off = adapter.subscribe('room:chat', noop);
    assert.strictEqual(errors.length, 1, 'the failed subscribe is reported');
    adapter.publish('room:chat', 'x');
    assert.strictEqual(errors.length, 2, 'the failed publish is reported');
    off();
    assert.strictEqual(errors.length, 3, 'the failed unsubscribe is reported');
    assert.doesNotThrow(() => adapter.close());
  });

  await t.test('a non-function handler is rejected', () => {
    const adapter = createRedisAdapter({ pub: new FakeRedis() });
    assert.throws(() => adapter.subscribe('chan', null), TypeError);
    adapter.close();
  });

  await t.test('subscribing after close yields an inert unsubscribe', () => {
    const adapter = createRedisAdapter({ pub: new FakeRedis() });
    adapter.close();
    const off = adapter.subscribe('chan', noop);
    assert.strictEqual(typeof off, 'function');
    assert.doesNotThrow(off);
  });
});

test('createRedisAdapter: close releases the subscriber it opened itself', async (t) => {
  await t.test('a duplicated subscriber is quit — nobody else can', async () => {
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

  await t.test('a client with only disconnect() is disconnected', () => {
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

  await t.test('a client with neither is simply released', () => {
    const pub = new FakeRedis();
    pub.duplicate = () => {
      const sub = new FakeRedis(pub.bus);
      sub.quit = undefined;
      return sub;
    };
    assert.doesNotThrow(() => createRedisAdapter({ pub }).close());
  });

  await t.test('a throwing quit is reported, not propagated', () => {
    const errors = [];
    const pub = new FakeRedis();
    pub.duplicate = () => {
      const sub = new FakeRedis(pub.bus);
      sub.quit = () => {
        throw new Error('quit blew up');
      };
      return sub;
    };
    const adapter = createRedisAdapter({ pub, console: { ...quiet, error: (e) => errors.push(e) } });
    assert.doesNotThrow(() => adapter.close());
    assert.strictEqual(errors.length, 1);
  });
});

test('createRedisAdapter: close releases subscriptions but not the injected clients', async () => {
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
