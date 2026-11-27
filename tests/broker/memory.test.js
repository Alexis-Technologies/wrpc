'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const timers = require('node:timers/promises');

const { MemoryBroker, createMemoryBroker, isBroker } = require('../../src/broker/index.js');
const { runBackplaneContract } = require('./backplane-contract.js');
const { runLogContract } = require('./log-contract.js');
const { runQueueContract } = require('./queue-contract.js');
const { runDirectContract } = require('./direct-contract.js');
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

describe('MemoryBroker', () => {
  runBackplaneContract('backplane contract', {
    open: async () => {
      const broker = new MemoryBroker({ logger: quiet });
      return [broker.backplane, broker.backplane];
    },
    close: async (a) => a.close(),
  });

  runLogContract(
    'log contract',
    memoryHarness('log', (broker) => ({
      trim: (topic, keep) => broker.trim(topic, keep),
      foreignId: () => 'another-epoch.1',
      beyondTip: (_topic, id) => `${broker.epoch}.${Number(id.split('.')[1]) + 100}`,
    })),
  );

  runQueueContract('queue contract', memoryHarness('queue'));

  runDirectContract('direct contract', memoryHarness('direct'));

  describe('construction and shape', () => {
    it('should be a broker with all four capabilities, named memory', () => {
      const broker = createMemoryBroker({ logger: quiet });
      assert.strictEqual(isBroker(broker), true);
      assert.strictEqual(broker.name, 'memory');
      for (const capability of ['log', 'queue', 'direct']) assert.strictEqual(broker[capability].name, 'memory');
      assert.strictEqual(broker.log, broker.log);
      broker.close();
      broker.close();
    });

    it('should validate its options', () => {
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

  describe('retention and closing', () => {
    it('should cap a topic by retention, and a slow reader past it fails with 410', async (t) => {
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

    it('should compact the trimmed prefix of a long-lived topic', async (t) => {
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

    it('should end reads, refuse new work and silence consumers on close', async () => {
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

    it('should make a consumer stopped through its signal let go of its work', async (t) => {
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

    it('should drop a message dead-lettered without a dead-letter queue', async (t) => {
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

    it('should contain a listener that throws or rejects', async (t) => {
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

    it('should not deliver a message queued for a closed broker', async () => {
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

  it('should retry a handler that always throws with backoff, not spin it at the head', async (t) => {
    const broker = new MemoryBroker({ logger: quiet });
    t.after(() => broker.close());
    const attempts = [];
    await broker.queue.consume('q', (delivery) => {
      attempts.push(delivery.attempt);
      throw new Error('always');
    });
    await broker.queue.produce('q', 'x');
    // It used to be released at the head and re-dispatched on a microtask:
    // this timer never fired, and neither did any other in the process.
    await timers.setTimeout(30);
    assert.deepStrictEqual(attempts, [1], 'one attempt within the first backoff (50 ms)');
    await waitFor(() => attempts.length === 2, { timeout: 500 });
    assert.deepStrictEqual(attempts, [1, 2], 'a retry, attempt + 1 — not a release');
  });

  it('should compact the dead prefix on trim(), and readers stay consistent across it', async (t) => {
    const broker = new MemoryBroker({ logger: quiet, retention: { maxEntries: 10_000 } });
    t.after(() => broker.close());
    for (let i = 0; i < 3000; i++) await broker.log.append('t', String(i));
    const read = broker.log.read('t', { from: 'earliest' });
    const iterator = read[Symbol.asyncIterator]();
    assert.strictEqual((await iterator.next()).value.value, '0');
    // The head moves past half the array, so the prefix is sliced away — the
    // same compaction the cap does; trim() used to leave it in place.
    broker.trim('t', 1);
    await assert.rejects(iterator.next(), (error) => error.code === 410, 'a reader behind the trim fails 410');
    await broker.log.append('t', '3000');
    const kept = await collect(broker.log.read('t', { from: 'earliest' }), 2);
    assert.deepStrictEqual(
      kept.map((entry) => entry.value),
      ['2999', '3000'],
    );
  });

  // ---------------------------------------------------------------------------
  // The ready messages of a queue. How many wait is up to whoever produces;
  // what taking one costs, and the order they are taken in, must not depend on
  // it. The first test below failed over the array the queue used to be.

  // Nanoseconds per message to consume a backlog of `count` that was produced
  // with nobody listening.
  const drainBacklog = async (count) => {
    const broker = new MemoryBroker({ logger: quiet });
    for (let i = 0; i < count; i++) await broker.queue.produce('q', 'x');
    let left = count;
    const started = process.hrtime.bigint();
    await new Promise((resolve, reject) => {
      const onDelivery = (delivery) => {
        if (--left === 0) resolve();
        return delivery.ack();
      };
      broker.queue.consume('q', onDelivery, { prefetch: 64 }).catch(reject);
    });
    const elapsed = Number(process.hrtime.bigint() - started);
    broker.close();
    return elapsed / count;
  };

  // `ready.shift()` moved the rest of the array on every delivery: 0.6 µs a
  // message with 8,000 waiting, 73 µs with 160,000 — twelve seconds for what
  // takes a twentieth of one. A ratio, not a time: both sizes run on the same
  // machine under the same load, and the quadratic drain was at least forty
  // times slower per message at the larger one, where the ring is no slower.
  it('should consume a backlog in linear time', async () => {
    await drainBacklog(2000);
    const short = await drainBacklog(8000);
    const long = await drainBacklog(160_000);
    assert.ok(
      long < short * 8,
      `${long.toFixed(0)} ns a message with 160,000 waiting against ${short.toFixed(0)} with 8,000`,
    );
  });

  // The queue as a plain array is still the plainest statement of its order:
  // taken from the head, released to the head, retried and produced to the
  // tail. One consumer with a prefetch of one makes the next delivery a
  // function of the settlements alone, so each is checked against that array
  // as it arrives — through the backlog growing under the consumer, wrapping
  // around the ring it is kept in, and shrinking to nothing.
  it('should deliver a released message next and a retried one last, at any backlog', async (t) => {
    const broker = new MemoryBroker({ logger: quiet });
    t.after(() => broker.close());
    const model = [];
    let next = 0;
    const produce = () => {
      model.push(next);
      return broker.queue.produce('q', String(next++));
    };
    const initial = 3000;
    for (let i = 0; i < initial; i++) await produce();
    const wrong = [];
    const counts = { delivered: 0, released: 0, retried: 0 };
    await new Promise((resolve, reject) => {
      const onDelivery = (delivery) => {
        const n = Number(delivery.body);
        const expected = model.shift();
        counts.delivered++;
        if (n !== expected && wrong.length < 5) wrong.push(`delivery ${counts.delivered}: ${n}, not ${expected}`);
        if (delivery.redelivered) {
          if (model.length === 0) resolve();
          return delivery.ack();
        }
        // Two more behind every third of the first three thousand: the tail
        // keeps moving while the head does.
        if (n < initial && n % 3 === 0) {
          void produce();
          void produce();
        }
        if (n % 5 === 0) {
          counts.released++;
          model.unshift(n);
          return delivery.release();
        }
        if (n % 7 === 0) {
          counts.retried++;
          model.push(n);
          return delivery.retry();
        }
        if (model.length === 0) resolve();
        return delivery.ack();
      };
      broker.queue.consume('q', onDelivery, { prefetch: 1 }).catch(reject);
    });
    assert.deepStrictEqual(wrong, []);
    assert.strictEqual(next, 5000);
    // Every fifth of the five thousand came twice, and so did every seventh
    // that is not also a fifth.
    assert.deepStrictEqual(counts, { delivered: 5000 + 1000 + 572, released: 1000, retried: 572 });
  });

  it('should put what a stopped consumer held back at the front, in its order, ahead of the backlog', async (t) => {
    const broker = new MemoryBroker({ logger: quiet });
    t.after(() => broker.close());
    for (let n = 0; n < 16; n++) await broker.queue.produce('q', String(n));
    const held = [];
    const first = await broker.queue.consume('q', (delivery) => void held.push(delivery), { prefetch: 8 });
    await waitFor(() => held.length === 8);
    // Sixteen ready again, the eight new ones behind the eight that were left:
    // what comes back has no room in front of them until the queue makes it.
    for (let n = 16; n < 24; n++) await broker.queue.produce('q', String(n));
    await first.stop();
    const seen = [];
    await broker.queue.consume(
      'q',
      (delivery) => {
        seen.push([Number(delivery.body), delivery.redelivered]);
        return delivery.ack();
      },
      { prefetch: 1 },
    );
    await waitFor(() => seen.length === 24);
    assert.deepStrictEqual(
      seen,
      Array.from({ length: 24 }, (_, n) => [n, n < 8]),
    );
    // A late settlement from the consumer that stopped changes nothing.
    await held[0].release();
    await timers.setTimeout(20);
    assert.strictEqual(seen.length, 24);
  });
});
