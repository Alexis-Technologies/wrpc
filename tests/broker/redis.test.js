'use strict';

// The Redis broker over the in-process fake (tests/broker/fakeRedisServer.js):
// the same four contract suites a real server runs in
// tests/broker/redis.integration.test.js.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { createRedisBroker } = require('../../broker/redis.js');
const { isBroker } = require('../../broker.js');
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

test('redis broker (fake): backplane contract', async (t) => {
  await runBackplaneContract(t, 'redis', {
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
});

test('redis broker (fake): log contract', async (t) => {
  await runLogContract(t, 'redis', {
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
});

test('redis broker (fake): the feed spec — brokerFeed over Redis Streams', async (t) => {
  await runFeedSpec(t, 'redis', {
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
});

test('redis broker (fake): queue contract', async (t) => {
  await runQueueContract(t, 'redis', {
    open: async () => {
      const { client, broker, close } = open();
      const peerBroker = createRedisBroker({
        client: client.duplicate(),
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
    timeout: 3000,
    redelivery: 500,
  });
});

test('redis broker (fake): direct contract', async (t) => {
  await runDirectContract(t, 'redis', {
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
});

test('redis broker: injection is validated structurally', () => {
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

test('redis broker: async validation rejects rather than throws', async () => {
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

test('redis broker: an injected connect() opens every extra connection', async (t) => {
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

test('redis broker: stream ids are opaque and strictly ordered across the ms boundary', async (t) => {
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

test('redis broker: MAXLEN trims a topic as it is written', async (t) => {
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

test('redis broker: a delayed retry rides a sorted set until it is due', async (t) => {
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

test('redis broker: two delayed retries of one body are two entries; promotion is atomic, or two steps that put back', async (t) => {
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

test('redis broker: a client without eval promotes in two steps, and a refused XADD puts the entry back', async (t) => {
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
  inner.server.fail = (command, [key]) => (command === 'xadd' && key === `wrpc:q:{${name}}` ? new Error('OOM') : null);
  await waitFor(() => errors.some((entry) => entry.event === 'broker.redis.sweep'), { timeout: 3000 });
  assert.strictEqual(inner.server.zsets.get(`wrpc:q:{${name}}:delayed`).size, 1, 'put back on the set');
  inner.server.fail = null;
  await waitFor(() => seen.length === 2, { timeout: 3000 });
  assert.deepStrictEqual(seen, [1, 2]);
  assert.strictEqual(inner.server.zsets.get(`wrpc:q:{${name}}:delayed`).size, 0);
});

test('redis broker: what a stopped consumer held is claimed by another', async (t) => {
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

test('redis broker: a command failure is reported, not thrown, and healthy flips', async (t) => {
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

test('redis broker: sending to nobody is a fast 503; a group queues, a plain inbox does not', async (t) => {
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

test('redis broker: a group is present while ANY member is — one stopping does not take it offline', async (t) => {
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

test('redis broker: close() stops the heartbeats and the sweep timers', async (t) => {
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

test('redis broker: closing refuses further work', async (t) => {
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

test('redis broker: numeric options are refused at construction, deadLetter at consume', async () => {
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
