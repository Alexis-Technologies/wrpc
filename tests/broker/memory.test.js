'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { MemoryBroker, createMemoryBroker, isBroker } = require('../../broker.js');
const { runBackplaneContract } = require('./backplaneContract.js');
const { runLogContract } = require('./logContract.js');
const { runQueueContract } = require('./queueContract.js');
const { runDirectContract } = require('./directContract.js');
const { quiet, collect, waitFor } = require('./support.js');

// One broker per open(): the in-process world IS the broker, so "two
// instances on one broker" are two views of the same object.
const memoryHarness = (capability, extra = () => ({})) => ({
  open: async () => {
    const broker = new MemoryBroker({ logger: quiet });
    return {
      [capability]: broker[capability],
      peer: broker[capability],
      broker,
      close: () => broker.close(),
      ...extra(broker),
    };
  },
});

test('MemoryBroker: backplane contract', async (t) => {
  await runBackplaneContract(t, 'memory', {
    open: async () => {
      const broker = new MemoryBroker({ logger: quiet });
      return [broker.backplane, broker.backplane];
    },
    close: async (a) => a.close(),
  });
});

test('MemoryBroker: log contract', async (t) => {
  await runLogContract(
    t,
    'memory',
    memoryHarness('log', (broker) => ({
      trim: (topic, keep) => broker.trim(topic, keep),
      foreignId: () => 'another-epoch.1',
      beyondTip: (_topic, id) => `${broker.epoch}.${Number(id.split('.')[1]) + 100}`,
    })),
  );
});

test('MemoryBroker: queue contract', async (t) => {
  await runQueueContract(t, 'memory', memoryHarness('queue'));
});

test('MemoryBroker: direct contract', async (t) => {
  await runDirectContract(t, 'memory', memoryHarness('direct'));
});

test('MemoryBroker: construction and shape', async (t) => {
  await t.test('is a broker with all four capabilities, named memory', () => {
    const broker = createMemoryBroker({ logger: quiet });
    assert.strictEqual(isBroker(broker), true);
    assert.strictEqual(broker.name, 'memory');
    for (const capability of ['log', 'queue', 'direct']) assert.strictEqual(broker[capability].name, 'memory');
    assert.strictEqual(broker.log, broker.log);
    broker.close();
    broker.close();
  });

  await t.test('options are validated', () => {
    assert.throws(() => new MemoryBroker({ epoch: 'has.dot' }), /epoch/);
    assert.throws(() => new MemoryBroker({ retention: { maxEntries: 0 } }), /maxEntries/);
    const broker = new MemoryBroker({ logger: quiet, epoch: 'e1' });
    assert.strictEqual(broker.epoch, 'e1');
    assert.throws(() => broker.log.append('', 'x'), /topic/);
    assert.throws(() => broker.log.read('t', { from: 'middle' }), /from/);
    assert.throws(() => broker.queue.consume('q', null), /onDelivery/);
    assert.throws(() => broker.queue.consume('q', () => {}, { prefetch: 0 }), /prefetch/);
    assert.throws(() => broker.queue.consume('q', () => {}, { deadLetter: 5 }), /deadLetter/);
    assert.throws(() => broker.direct.listen('a', 'nope'), /onMessage/);
    broker.close();
  });
});

test('MemoryBroker: retention and closing', async (t) => {
  await t.test('retention caps a topic and a slow reader past it fails with 410', async () => {
    const broker = new MemoryBroker({ logger: quiet, retention: { maxEntries: 3 } });
    t.after(() => broker.close());
    await broker.log.append('t', '1');
    const read = broker.log.read('t', { from: 'earliest' });
    const iterator = read[Symbol.asyncIterator]();
    // The reader is positioned at the first entry but has not pulled yet.
    for (let i = 2; i <= 6; i++) await broker.log.append('t', String(i));
    await assert.rejects(iterator.next(), (error) => error.code === 410);
    const kept = await collect(broker.log.read('t', { from: 'earliest' }), 3);
    assert.deepStrictEqual(
      kept.map((entry) => entry.value),
      ['4', '5', '6'],
    );
  });

  await t.test('a long-lived topic compacts its trimmed prefix', async () => {
    const broker = new MemoryBroker({ logger: quiet, retention: { maxEntries: 10 } });
    t.after(() => broker.close());
    for (let i = 0; i < 5000; i++) await broker.log.append('big', String(i));
    const tail = await collect(broker.log.read('big', { from: 'earliest' }), 10);
    assert.strictEqual(tail[0].value, '4990');
    assert.strictEqual(tail[9].value, '4999');
    broker.trim('big', 2);
    broker.trim('missing', 1);
    broker.trim('big', 5);
    const two = await collect(broker.log.read('big', { from: 'earliest' }), 2);
    assert.deepStrictEqual(
      two.map((entry) => entry.value),
      ['4998', '4999'],
    );
  });

  await t.test('closing ends reads, refuses new work and silences consumers', async () => {
    const broker = new MemoryBroker({ logger: quiet });
    const read = broker.log.read('t', { from: 'latest' });
    const iterator = read[Symbol.asyncIterator]();
    const pending = iterator.next();
    const seen = [];
    const consumer = await broker.queue.consume('q', (delivery) => seen.push(delivery), { prefetch: 1 });
    await broker.queue.produce('q', 'held');
    await broker.queue.produce('q', 'waiting');
    await waitFor(() => seen.length === 1);
    await seen[0].retry({ delay: 60_000 }); // a pending retry timer
    broker.close();
    assert.deepStrictEqual(await pending, { value: undefined, done: true });
    assert.strictEqual(consumer.healthy, false);
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

  await t.test('a consumer stopped through its signal lets go of its work', async () => {
    const broker = new MemoryBroker({ logger: quiet });
    t.after(() => broker.close());
    const controller = new AbortController();
    const first = [];
    const consumer = await broker.queue.consume('q', (delivery) => first.push(delivery), {
      signal: controller.signal,
    });
    await broker.queue.produce('q', 'x');
    await waitFor(() => first.length === 1);
    controller.abort();
    assert.strictEqual(consumer.healthy, false);
    await consumer.stop();
    const second = [];
    await broker.queue.consume('q', (delivery) => second.push(delivery) && delivery.ack());
    await waitFor(() => second.length === 1);
    assert.strictEqual(second[0].redelivered, true);
  });

  await t.test('dead-lettering without a dead-letter queue drops the message', async () => {
    const broker = new MemoryBroker({ logger: quiet });
    t.after(() => broker.close());
    const seen = [];
    await broker.queue.consume('q', (delivery) => {
      seen.push(delivery.body);
      return delivery.deadLetter();
    });
    await broker.queue.produce('q', 'gone');
    await waitFor(() => seen.length === 1);
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.deepStrictEqual(seen, ['gone']);
  });

  await t.test('a listener that throws or rejects is contained', async () => {
    const errors = [];
    const broker = new MemoryBroker({ logger: { ...quiet, error: (entry) => errors.push(entry) } });
    t.after(() => broker.close());
    await broker.direct.listen('a', () => {
      throw new Error('sync');
    });
    await broker.direct.listen('a', async () => {
      throw new Error('async');
    });
    await broker.direct.send('a', 'x');
    await waitFor(() => errors.length === 2);
  });

  await t.test('a message queued for a closed broker is not delivered', async () => {
    const broker = new MemoryBroker({ logger: quiet });
    const seen = [];
    await broker.direct.listen('a', (message) => seen.push(message));
    // Not awaited: the delivery microtask is queued, the close lands first.
    void broker.direct.send('a', 'x');
    broker.close();
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.deepStrictEqual(seen, []);
  });
});
