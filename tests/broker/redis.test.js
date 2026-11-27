'use strict';

// The Redis broker over the in-process fake (tests/broker/fakeRedisServer.js):
// the same four contract suites a real server runs in
// tests/broker/redis.integration.test.js.

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const timers = require('node:timers/promises');

const { createRedisBroker } = require('../../src/broker/redis/index.js');
const { isBroker } = require('../../src/broker/index.js');
const { createFakeRedis } = require('./fakeRedisServer.js');
const { runBackplaneContract } = require('./backplaneContract.js');
const { runLogContract } = require('./logContract.js');
const { runFeedSpec } = require('./feedSpec.js');
const { runQueueContract } = require('./queueContract.js');
const { runDirectContract } = require('./directContract.js');
const { quiet, unique, waitFor, collect } = require('./support.js');

const open = (extra = {}) => {
  const client = createFakeRedis();
  const broker = createRedisBroker({ client, logger: quiet, blockMs: 20, claimIdleMs: 50, inboxTtl: 2000, ...extra });
  return { client, broker, close: () => broker.close() };
};

describe('redis broker (fake)', () => {
  runBackplaneContract('backplane contract', {
    open: async () => {
      const client = createFakeRedis();
      const a = createRedisBroker({ client, logger: quiet, blockMs: 20 });
      const b = createRedisBroker({ client: client.duplicate(), logger: quiet, blockMs: 20 });
      return [a.backplane, b.backplane, a, b];
    },
    close: async (a, b) => {
      a.close();
      b.close();
    },
    settle: 5,
  });

  runLogContract('log contract', {
    open: async () => {
      const { client, broker, close } = open();
      const peerBroker = createRedisBroker({ client: client.duplicate(), logger: quiet, blockMs: 20 });
      return {
        log: broker.log,
        peer: peerBroker.log,
        close: async () => {
          await peerBroker.close();
          await close();
        },
        trim: async (topic, keep) => {
          const key = `wrpc:log:${topic}`;
          const stream = client.server.stream(key, false);
          if (stream) stream.entries.splice(0, Math.max(0, stream.entries.length - keep));
        },
        foreignId: () => '1-0',
        beyondTip: (_topic, id) => `${Number(id.split('-')[0]) + 10_000}-0`,
      };
    },
    timeout: 3000,
  });

  runFeedSpec('the feed spec — brokerFeed over Redis Streams', {
    open: () => {
      const { client, broker, close } = open();
      return {
        broker,
        close,
        trim: async (topic, keep) => {
          const stream = client.server.stream(`wrpc:log:${topic}`, false);
          if (stream) stream.entries.splice(0, Math.max(0, stream.entries.length - keep));
        },
        foreignId: () => '1-0',
      };
    },
  });

  // The client of the latest open(), for breakConsumer.
  let queueClient = null;
  runQueueContract('queue contract', {
    open: async () => {
      const opened = open();
      queueClient = opened.client;
      const { broker, close } = opened;
      const peerBroker = createRedisBroker({
        client: queueClient.duplicate(),
        logger: quiet,
        blockMs: 20,
        claimIdleMs: 50,
      });
      return {
        queue: broker.queue,
        peer: peerBroker.queue,
        close: async () => {
          await peerBroker.close();
          await close();
        },
      };
    },
    // The server refuses reads for a moment: every XREADGROUP fails, then
    // works again — what a failover looks like from a client.
    breakConsumer: () => {
      queueClient.server.fail = (command) => (command === 'xreadgroup' ? new Error('LOADING Redis is loading') : null);
      setTimeout(() => {
        queueClient.server.fail = null;
      }, 80);
    },
    timeout: 3000,
    redelivery: 500,
  });

  it('should keep a direct listener through a failing BLPOP and a handler that throws — each a line', async (t) => {
    const { recorder } = require('../helpers/recorder.js');
    const log = recorder();
    const client = createFakeRedis();
    const broker = createRedisBroker({ client, logger: log.writer, blockMs: 20 });
    t.after(() => broker.close());
    // A service address (a group) is served through a list and BLPOP; an
    // inbox is pub/sub. The pop fails from the first call: reported, retried
    // after a backoff (a BLPOP already blocking would fail a second later).
    const address = 'svc-blpop';
    const seen = [];
    client.server.fail = (command) => (command === 'blpop' ? new Error('READONLY') : null);
    const handler = (message) => {
      seen.push(message.body);
      if (message.body === 'boom') throw new Error('handler boom');
    };
    const stop = await broker.direct.listen(address, handler, { group: address });
    await waitFor(() => log.all('broker.redis.blpop').length >= 1);
    // The listener says so itself: nothing is being taken off the list.
    assert.strictEqual(stop.healthy, false);
    client.server.fail = null;
    await broker.direct.send(address, 'boom');
    await broker.direct.send(address, 'fine');
    await waitFor(() => seen.length === 2);
    assert.deepStrictEqual(seen, ['boom', 'fine']);
    assert.strictEqual(stop.healthy, true, 'a read that works again is a healthy listener again');
    const thrown = log.all('broker.redis.listener');
    assert.strictEqual(thrown.length, 1);
    assert.strictEqual(thrown[0].err.message, 'handler boom');
    assert.strictEqual(log.find('broker.redis.blpop').err.message, 'READONLY');
  });

  it('should report a direct listener as healthy as its lease and its connection', async (t) => {
    const { recorder } = require('../helpers/recorder.js');
    const log = recorder();
    const client = createFakeRedis();
    const spawned = [];
    const connect = () => {
      const connection = client.duplicate();
      spawned.push(connection);
      return connection;
    };
    t.mock.timers.enable({ apis: ['setInterval'] });
    const broker = createRedisBroker({ client, connect, logger: log.writer, blockMs: 20, inboxTtl: 3000 });
    t.after(() => broker.close());
    const service = await broker.direct.listen('svc-health', () => {}, { group: 'svc-health' });
    const inbox = await broker.direct.listen(broker.direct.inbox(), () => {});
    assert.deepStrictEqual([service.healthy, inbox.healthy], [true, true]);
    assert.ok(Object.keys(service).includes('healthy'), 'enumerable: a spread or a log line carries it');
    // The presence lease cannot be renewed: senders stop seeing this member,
    // so it is a listener nobody routes to.
    client.server.fail = (command) => (command === 'zadd' ? new Error('READONLY') : null);
    t.mock.timers.tick(1000);
    await waitFor(() => log.all('broker.redis.presence').length === 1);
    assert.strictEqual(service.healthy, false);
    assert.strictEqual(inbox.healthy, true, 'an inbox has no lease');
    client.server.fail = null;
    t.mock.timers.tick(1000);
    await waitFor(() => service.healthy === true);
    // The connection itself, in the two shapes a client reports it.
    const [blocking, subscriber] = spawned;
    blocking.status = 'reconnecting';
    assert.strictEqual(service.healthy, false);
    blocking.status = 'ready';
    assert.strictEqual(service.healthy, true);
    subscriber.isReady = false;
    assert.strictEqual(inbox.healthy, false);
    subscriber.isReady = true;
    assert.strictEqual(inbox.healthy, true);
    await service();
    assert.strictEqual(service.healthy, false, 'stopped');
    await broker.close();
    assert.strictEqual(inbox.healthy, false, 'closed');
  });

  runDirectContract('direct contract', {
    open: async () => {
      const { client, broker, close } = open();
      const peerBroker = createRedisBroker({ client: client.duplicate(), logger: quiet, blockMs: 20, inboxTtl: 2000 });
      return {
        direct: broker.direct,
        peer: peerBroker.direct,
        close: async () => {
          await peerBroker.close();
          await close();
        },
      };
    },
    timeout: 3000,
    settle: 10,
  });

  it('should read live topics by loops of a bounded number of topics each', async (t) => {
    const { client, broker, close } = open();
    t.after(close);
    // What the tail's blocking read is given: every topic is two arguments of
    // one command, and it used to be ALL the topics of the instance — past
    // some sixty thousand the call itself threw, once a second, for good.
    const calls = [];
    const spawned = [];
    const duplicate = client.duplicate.bind(client);
    client.duplicate = (...args) => {
      const connection = duplicate(...args);
      const xread = connection.xread.bind(connection);
      connection.xread = (...argv) => {
        calls.push(argv.length);
        return xread(...argv);
      };
      spawned.push(connection);
      return connection;
    };
    const controller = new AbortController();
    t.after(() => controller.abort());
    const TOPICS = 8192 + 40;
    const iterators = [];
    for (let i = 0; i < TOPICS; i++) {
      const iterator = broker.log.read(`t${i}`, { after: null, signal: controller.signal })[Symbol.asyncIterator]();
      iterators.push(iterator);
      void iterator.next().catch(() => {});
    }
    await waitFor(() => calls.some((length) => length === 5 + 40 * 2), {
      message: 'a second loop took the overflow',
      timeout: 10_000,
    });
    assert.ok(Math.max(...calls) <= 5 + 8192 * 2, `one read was given ${Math.max(...calls)} arguments`);
    // And an entry still reaches the reader of a topic in the second loop.
    const last = TOPICS - 1;
    const fresh = broker.log.read(`t${last}`, { after: null, signal: controller.signal })[Symbol.asyncIterator]();
    const next = fresh.next();
    await timers.setTimeout(60);
    await broker.log.append(`t${last}`, 'hello');
    assert.strictEqual((await next).value.value, 'hello');
  });
});

describe('redis broker', () => {
  it('should validate the injection structurally', () => {
    assert.throws(() => createRedisBroker({}), /ioredis-shaped client/);
    assert.throws(() => createRedisBroker({ client: { xadd() {}, publish() {} } }), /ioredis-shaped client/);
    const client = createFakeRedis();
    assert.throws(() => createRedisBroker({ client, connect: 'new' }), /connect must be a function/);
    const noDuplicate = { xadd: () => {}, publish: () => {}, xreadgroup: () => {} };
    assert.throws(() => createRedisBroker({ client: noDuplicate }), /duplicate\(\), or pass options.connect/);
    const broker = createRedisBroker({ client, logger: quiet });
    assert.strictEqual(isBroker(broker), true);
    assert.strictEqual(broker.name, 'redis');
    assert.match(broker.direct.inbox(), /^wrpc\.inbox\./);
    assert.throws(() => broker.log.read('t', { from: 'middle' }), /from must be/);
    void broker.close();
  });

  it('should reject rather than throw on async validation', async () => {
    const broker = createRedisBroker({ client: createFakeRedis(), logger: quiet });
    await assert.rejects(
      broker.queue.consume('q', () => {}, { prefetch: 0 }),
      /prefetch/,
    );
    await assert.rejects(broker.queue.consume('q', 'nope'), /onDelivery must be a function/);
    await assert.rejects(broker.direct.listen('a', 'nope'), /onMessage must be a function/);
    await assert.rejects(
      broker.direct.listen('', () => {}),
      /address must be a non-empty string/,
    );
    await broker.close();
  });

  it('should open every extra connection through an injected connect()', async (t) => {
    const client = createFakeRedis();
    const opened = [];
    const broker = createRedisBroker({
      client,
      logger: quiet,
      blockMs: 20,
      connect: () => {
        const connection = client.duplicate();
        opened.push(connection);
        return connection;
      },
    });
    t.after(() => broker.close());
    const read = broker.log.read('t', { from: 'latest' });
    await read.ready;
    const pending = collect(read, 1, { timeout: 3000 });
    await broker.log.append('t', 'x');
    await pending;
    assert.ok(opened.length >= 1, 'the tail duplicated a connection');
    await broker.close();
    await waitFor(() => opened.every((connection) => connection.ended), { timeout: 2000 });
    // The injected client is never quit — its lifetime belongs to the caller.
    assert.strictEqual(client.ended, false);
  });

  // The read of the first retained id caught every error as "no such key":
  // Redis down read as an empty stream, and a resume past a trimmed history
  // was let through instead of refused.
  it('should fail a resume when Redis fails, not read the failure as an empty stream', async (t) => {
    const { client, broker, close } = open();
    t.after(close);
    const topic = unique('down');
    const first = await broker.log.append(topic, 'a');
    await broker.log.append(topic, 'b');
    const refused = new Error('ECONNREFUSED');
    client.server.fail = (command) => (command === 'xrange' || command === 'xinfo' ? refused : null);
    const read = broker.log.read(topic, { after: first });
    await assert.rejects(read.ready, (error) => error === refused);
    client.server.fail = null;
  });

  it('should mint opaque stream ids, strictly ordered across the ms boundary', async (t) => {
    const { broker, close } = open();
    t.after(close);
    const { compareIds } = require('../../src/broker/redis/index.js');
    assert.strictEqual(compareIds('10-0', '9-0') > 0, true, 'numeric, not lexicographic');
    assert.strictEqual(compareIds('5-2', '5-10') < 0, true);
    assert.strictEqual(compareIds('5-2', '5-2'), 0);
    assert.strictEqual(broker.log.parseId('1726000000000-0'), '1726000000000-0');
    assert.strictEqual(broker.log.parseId('1726000000000'), null);
    assert.strictEqual(broker.log.parseId('x-0'), null);
  });

  it('should trim a topic by MAXLEN as it is written', async (t) => {
    const { broker, close } = open({ maxLen: 3 });
    t.after(close);
    const topic = unique('capped');
    for (let i = 0; i < 6; i++) await broker.log.append(topic, String(i));
    const entries = await collect(broker.log.read(topic, { from: 'earliest' }), 3, { timeout: 2000 });
    assert.deepStrictEqual(
      entries.map((entry) => entry.value),
      ['3', '4', '5'],
    );
  });

  it('should keep a delayed retry in a sorted set until it is due', async (t) => {
    const { client, broker, close } = open();
    t.after(close);
    const name = unique('delayed');
    const seen = [];
    const consumer = await broker.queue.consume(name, (delivery) => {
      seen.push({ attempt: delivery.attempt, at: Date.now() });
      return delivery.attempt === 1 ? delivery.retry({ delay: 120 }) : delivery.ack();
    });
    t.after(() => consumer.stop());
    await broker.queue.produce(name, 'work');
    await waitFor(() => seen.length === 1, { timeout: 2000 });
    // The stream and its delayed set share one hash tag: a cluster keeps
    // them in one slot, so the promotion script may touch both.
    assert.strictEqual(client.server.zsets.get(`wrpc:q:{${name}}:delayed`)?.size, 1);
    await waitFor(() => seen.length === 2, { timeout: 3000 });
    assert.ok(seen[1].at - seen[0].at >= 100, `redelivered after ${seen[1].at - seen[0].at} ms`);
    assert.strictEqual(client.server.zsets.get(`wrpc:q:{${name}}:delayed`).size, 0);
  });

  it('should keep two delayed retries of one body as two entries; promotion is atomic, or two steps that put back', async (t) => {
    const { client, broker, close } = open();
    t.after(close);
    const name = unique('twins');
    const seen = [];
    const consumer = await broker.queue.consume(name, (delivery) => {
      seen.push([delivery.id, delivery.attempt]);
      return delivery.attempt === 1 ? delivery.retry({ delay: 60 }) : delivery.ack();
    });
    t.after(() => consumer.stop());
    // Same body, same headers: one sorted-set member before, so the second
    // overwrote the first and a message was gone.
    await broker.queue.produce(name, 'same');
    await broker.queue.produce(name, 'same');
    await waitFor(() => seen.length === 2, { timeout: 2000 });
    assert.strictEqual(client.server.zsets.get(`wrpc:q:{${name}}:delayed`).size, 2);
    await waitFor(() => seen.length === 4, { timeout: 3000 });
    assert.deepStrictEqual(
      seen.map(([, attempt]) => attempt),
      [1, 1, 2, 2],
    );
    assert.strictEqual(client.server.zsets.get(`wrpc:q:{${name}}:delayed`).size, 0);
  });

  it('should promote in two steps on a client without eval, and put the entry back on a refused XADD', async (t) => {
    const inner = createFakeRedis();
    // ioredis has eval; an injected client may not, and a proxy may refuse
    // scripts. The same fake, every method bound, minus that one.
    const client = {};
    for (
      let proto = Object.getPrototypeOf(inner);
      proto && proto !== Object.prototype;
      proto = Object.getPrototypeOf(proto)
    ) {
      for (const name of Object.getOwnPropertyNames(proto)) {
        if (name === 'constructor' || name === 'eval' || name in client) continue;
        if (typeof inner[name] === 'function') client[name] = inner[name].bind(inner);
      }
    }
    const errors = [];
    const logger = {
      ...quiet,
      error: (entry) => errors.push(entry),
      child() {
        return this;
      },
    };
    const broker = createRedisBroker({ client, logger, blockMs: 20, claimIdleMs: 50 });
    t.after(() => broker.close());
    const name = unique('twostep');
    const seen = [];
    const consumer = await broker.queue.consume(name, (delivery) => {
      seen.push(delivery.attempt);
      return delivery.attempt === 1 ? delivery.retry({ delay: 30 }) : delivery.ack();
    });
    t.after(() => consumer.stop());
    await broker.queue.produce(name, 'work');
    await waitFor(() => seen.length === 1, { timeout: 2000 });
    // The stream refuses the copy for a while: the entry goes back on the set
    // at its original time, and a later sweep promotes it.
    inner.server.fail = (command, [key]) =>
      command === 'xadd' && key === `wrpc:q:{${name}}` ? new Error('OOM') : null;
    await waitFor(() => errors.some((entry) => entry.event === 'broker.redis.sweep'), { timeout: 3000 });
    assert.strictEqual(inner.server.zsets.get(`wrpc:q:{${name}}:delayed`).size, 1, 'put back on the set');
    inner.server.fail = null;
    await waitFor(() => seen.length === 2, { timeout: 3000 });
    assert.deepStrictEqual(seen, [1, 2]);
    assert.strictEqual(inner.server.zsets.get(`wrpc:q:{${name}}:delayed`).size, 0);
  });

  it('should let another consumer claim what a stopped consumer held', async (t) => {
    const { broker, close } = open();
    t.after(close);
    const name = unique('claim');
    const first = await broker.queue.consume(name, () => {}, { prefetch: 1 });
    await broker.queue.produce(name, 'unfinished');
    await timers.setTimeout(60);
    await first.stop();
    const taken = [];
    const second = await broker.queue.consume(name, (delivery) => {
      taken.push(delivery.body);
      return delivery.ack();
    });
    t.after(() => second.stop());
    await waitFor(() => taken.length === 1, { timeout: 3000 });
    assert.deepStrictEqual(taken, ['unfinished']);
  });

  it('should report a command failure, not throw it, and flip healthy', async (t) => {
    const client = createFakeRedis();
    const errors = [];
    const logger = {
      ...quiet,
      error: (entry) => errors.push(entry),
      child() {
        return this;
      },
    };
    const broker = createRedisBroker({ client, logger, blockMs: 20, claimIdleMs: 50 });
    t.after(() => broker.close());
    const name = unique('flaky');
    const consumer = await broker.queue.consume(name, (delivery) => delivery.ack());
    t.after(() => consumer.stop());
    client.server.fail = (command) => (command === 'xreadgroup' ? new Error('READONLY') : null);
    await waitFor(() => errors.some((entry) => entry.event === 'broker.redis.read'), { timeout: 3000 });
    assert.strictEqual(consumer.healthy, false);
    client.server.fail = null;
    await waitFor(() => consumer.healthy, { timeout: 3000 });
  });

  it('should answer a send to nobody with a fast 503; a group queues, a plain inbox does not', async (t) => {
    const { client, broker, close } = open();
    t.after(close);
    const address = unique('svc');
    await assert.rejects(broker.direct.send(address, 'x'), (error) => error.code === 503);
    // A group listener leaves a presence key, so a send queues for it.
    const received = [];
    const stop = await broker.direct.listen(address, (message) => received.push(message), { group: 'svc' });
    await broker.direct.send(address, 'queued');
    await waitFor(() => received.length === 1, { timeout: 2000 });
    await stop();
    // With the group gone its last member is gone: back to 503.
    await assert.rejects(broker.direct.send(address, 'x'), (error) => error.code === 503);
    assert.strictEqual(client.server.zsets.get(`wrpc:inbox:${address}:members`).size, 0);
  });

  it('should keep a group present while ANY member is — one stopping does not take it offline', async (t) => {
    const { client, broker, close } = open();
    t.after(close);
    const address = unique('svc');
    const first = [];
    const second = [];
    const stopFirst = await broker.direct.listen(address, (message) => first.push(message.body), { group: 'svc' });
    const stopSecond = await broker.direct.listen(address, (message) => second.push(message.body), { group: 'svc' });
    t.after(() => stopSecond());
    assert.strictEqual(client.server.zsets.get(`wrpc:inbox:${address}:members`).size, 2);
    // The first member leaves: the presence used to be one key for the whole
    // group, deleted by whichever member stopped first — a 503 for senders
    // while the second was still taking work.
    await stopFirst();
    assert.strictEqual(client.server.zsets.get(`wrpc:inbox:${address}:members`).size, 1);
    await broker.direct.send(address, 'still served');
    await waitFor(() => second.length === 1, { timeout: 2000 });
    assert.deepStrictEqual(first, []);
  });

  it('should stop the heartbeats and the sweep timers on close()', async (t) => {
    const client = createFakeRedis();
    const broker = createRedisBroker({ client, logger: quiet, blockMs: 20, claimIdleMs: 50 });
    const address = unique('svc');
    await broker.direct.listen(address, () => {}, { group: 'svc' });
    await broker.queue.consume(unique('jobs'), (delivery) => delivery.ack());
    await timers.setTimeout(120);
    await broker.close();
    // The member's lease is released, and no sweep runs afterwards: a
    // consumer's XAUTOCLAIM/ZRANGEBYSCORE used to keep firing against the
    // injected client after close().
    assert.strictEqual(client.server.zsets.get(`wrpc:inbox:${address}:members`).size, 0);
    const commands = [];
    client.server.fail = (command) => {
      commands.push(command);
      return null;
    };
    await timers.setTimeout(200);
    assert.deepStrictEqual(commands, [], `commands after close(): ${commands.join(', ')}`);
    t.after(() => {
      client.server.fail = null;
    });
  });

  it('should refuse further work once closed', async (t) => {
    const { broker } = open();
    await broker.close();
    await broker.close();
    await assert.rejects(broker.log.append('t', 'x'), (error) => error.code === 503);
    await assert.rejects(broker.queue.produce('q', 'x'), (error) => error.code === 503);
    await assert.rejects(
      broker.queue.consume('q', () => {}),
      (error) => error.code === 503,
    );
    await assert.rejects(
      broker.direct.listen('a', () => {}),
      (error) => error.code === 503,
    );
    await assert.rejects(broker.direct.send('a', 'x'), (error) => error.code === 503);
  });

  // A read that reached the log tail just before close() was still waiting
  // for the stream's tip when close() quit every connection the broker owned;
  // the loop it joined then opened a connection of its own that nobody quit,
  // and a process holding one never exits (CI's redis job ran into its
  // ten-minute limit). A consumer had the same gap across its XGROUP CREATE.
  it('should open no connection after close() for a read or a consumer still opening when it runs', async () => {
    const client = createFakeRedis();
    const broker = createRedisBroker({ client, logger: quiet, blockMs: 20 });
    const opened = [];
    const duplicate = client.duplicate.bind(client);
    client.duplicate = (...args) => {
      const connection = duplicate(...args);
      opened.push(connection);
      return connection;
    };
    // The two round trips held open: the tip a tail reads first, the group a
    // consumer creates.
    let release = null;
    const held = new Promise((resolve) => {
      release = resolve;
    });
    const waiting = [];
    for (const command of ['xrevrange', 'xgroup']) {
      const original = client[command].bind(client);
      client[command] = async (...args) => {
        waiting.push(command);
        await held;
        return original(...args);
      };
    }
    const read = broker.log.read(unique('feed'), { from: 'latest' });
    read.ready.catch(() => {});
    const consuming = broker.queue.consume(unique('jobs'), (delivery) => delivery.ack());
    await waitFor(() => waiting.length === 2, { message: 'both round trips are in flight' });

    await broker.close();
    release();
    await assert.rejects(consuming, (error) => error.code === 503);
    await timers.setTimeout(50);
    assert.deepStrictEqual(
      opened.filter((connection) => !connection.ended).length,
      0,
      'every connection the broker opened is quit',
    );
  });

  it('should refuse numeric options at construction, deadLetter at consume', async () => {
    const client = createFakeRedis();
    // A string from the environment used to compare as a number and do the
    // wrong thing silently; blockMs: 0 was XREAD BLOCK 0 — forever.
    const rejected = [
      ['blockMs', [0, -1, 1.5, '1000']],
      ['claimIdleMs', [0, '60000']],
      ['maxLen', [-1, 1.5, '100']],
      ['inboxTtl', [0, '60000']],
    ];
    for (const [option, values] of rejected) {
      for (const value of values) {
        assert.throws(
          () => createRedisBroker({ client, logger: quiet, [option]: value }),
          new RegExp(`options\\.${option}`),
        );
      }
    }
    const broker = createRedisBroker({ client, logger: quiet, maxLen: 0 });
    await assert.rejects(
      broker.queue.consume('q', () => {}, { deadLetter: '' }),
      /deadLetter must be a queue name or null/,
    );
    await broker.close();
  });
});
