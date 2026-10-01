'use strict';

// The queue contract (src/broker/port.js), executable.
//
// harness: {
//   open(): Promise<{ queue, peer?, close() }>
//   prepare?(queue): Promise
//   timeout?: number               ms a delivery may take
//   redelivery?: number            ms before a stopped consumer's unsettled
//                                  work reappears (JetStream: ack_wait)
//   settle?: number                ms a real broker needs to route — and so
//                                  how long a "nothing more arrives" check
//                                  watches (at least 50; a fake needs none)
//   ackWindow?: number             ms an "acked message does not come back"
//                                  check watches. A real broker's harness
//                                  sets it to redelivery + settle: a lost
//                                  ack only shows once the broker
//                                  redelivers, and 50 ms never saw one.
// }

const assert = require('node:assert');
const timers = require('node:timers/promises');

const { isBrokerQueue } = require('../../src/broker/port.js');
const { unique, waitFor } = require('./support.js');

const runQueueContract = async (t, name, harness) => {
  const timeout = harness.timeout ?? 2000;
  const redelivery = harness.redelivery ?? timeout;
  // The quiet windows: how long a check that asserts ABSENCE watches. On a
  // fake a delivery is a microtask away, so 50 ms is generous; against a
  // real broker they scale with what the harness says a round trip takes —
  // a fixed 50 ms there asserted nothing.
  const quietWindow = Math.max(50, harness.settle ?? 0);
  const settledWindow = Math.max(100, harness.settle ?? 0);
  const ackWindow = harness.ackWindow ?? quietWindow;

  const open = async (sub) => {
    const env = await harness.open();
    sub.after(() => env.close());
    return env;
  };
  const queueFor = async (prefix) => {
    const name = unique(prefix);
    if (harness.prepare) await harness.prepare(name);
    return name;
  };
  const consume = async (sub, queue, name, handler, options = {}) => {
    const consumer = await queue.consume(name, handler, options);
    sub.after(() => consumer.stop());
    return consumer;
  };

  await t.test(`${name}: satisfies the structural contract`, async (sub) => {
    const { queue } = await open(sub);
    assert.strictEqual(isBrokerQueue(queue), true);
  });

  await t.test(`${name}: a message produced before any consumer is retained and delivered once`, async (sub) => {
    const { queue } = await open(sub);
    const name = await queueFor('q-retained');
    const body = JSON.stringify({ order: 1, text: 'привіт' });
    await queue.produce(name, body, { headers: { tp: '00-abc', 'x-tenant': 't1' } });
    const seen = [];
    const consumer = await consume(sub, queue, name, (delivery) => {
      seen.push(delivery);
      return delivery.ack();
    });
    await waitFor(() => seen.length === 1, { timeout });
    const [delivery] = seen;
    assert.strictEqual(delivery.body, body);
    assert.strictEqual(delivery.headers.tp, '00-abc');
    assert.strictEqual(delivery.headers['x-tenant'], 't1');
    assert.strictEqual(delivery.attempt, 1);
    assert.strictEqual(delivery.redelivered, false);
    assert.strictEqual(typeof delivery.id, 'string');
    assert.strictEqual(consumer.healthy, true);
    await timers.setTimeout(ackWindow);
    assert.strictEqual(seen.length, 1, 'an acked message came back');
  });

  await t.test(`${name}: competing consumers share the work, each message exactly once`, async (sub) => {
    const env = await open(sub);
    const name = await queueFor('q-compete');
    const counts = new Map();
    const handler = (delivery) => {
      counts.set(delivery.body, (counts.get(delivery.body) ?? 0) + 1);
      return delivery.ack();
    };
    await consume(sub, env.queue, name, handler, { prefetch: 4 });
    await consume(sub, env.peer ?? env.queue, name, handler, { prefetch: 4 });
    for (let i = 0; i < 40; i++) await env.queue.produce(name, `m${i}`);
    await waitFor(() => counts.size === 40, { timeout, message: `received ${counts.size} of 40` });
    await timers.setTimeout(quietWindow);
    for (const [body, count] of counts) assert.strictEqual(count, 1, `${body} delivered ${count} times`);
  });

  await t.test(`${name}: prefetch bounds in-flight deliveries, and they run concurrently`, async (sub) => {
    const { queue } = await open(sub);
    const name = await queueFor('q-prefetch');
    let inFlight = 0;
    let peak = 0;
    let done = 0;
    await consume(
      sub,
      queue,
      name,
      async (delivery) => {
        inFlight++;
        peak = Math.max(peak, inFlight);
        // Held until a second delivery is in flight: a concurrent consumer
        // brings one within the window, a sequential one holds this for
        // the whole of it. A fixed 30 ms sleep stood in for this and could
        // not tell the two apart on a consumer whose workers spin up after
        // a slow join and fetch one message at a time until they have
        // measured the handler (the confluent client, 3 s into a join
        // under the broker's default rebalance delay).
        await waitFor(() => inFlight >= 2 || done >= 8, { timeout: 500 }).catch(() => {});
        inFlight--;
        done++;
        await delivery.ack();
      },
      { prefetch: 4 },
    );
    for (let i = 0; i < 12; i++) await queue.produce(name, `m${i}`);
    await waitFor(() => done === 12, { timeout: timeout + 1000 });
    assert.ok(peak <= 4, `peak in-flight ${peak} exceeded prefetch 4`);
    assert.ok(peak >= 2, `deliveries ran sequentially (peak ${peak})`);
  });

  await t.test(`${name}: retry redelivers after the delay with the attempt counted`, async (sub) => {
    const { queue } = await open(sub);
    const name = await queueFor('q-retry');
    const seen = [];
    await consume(sub, queue, name, (delivery) => {
      seen.push({ attempt: delivery.attempt, redelivered: delivery.redelivered, at: Date.now() });
      if (delivery.attempt < 3) return delivery.retry({ delay: 100 });
      return delivery.ack();
    });
    await queue.produce(name, 'flaky');
    await waitFor(() => seen.length === 3, { timeout: timeout + 1000 });
    assert.deepStrictEqual(
      seen.map((entry) => [entry.attempt, entry.redelivered]),
      [
        [1, false],
        [2, true],
        [3, true],
      ],
    );
    assert.ok(seen[1].at - seen[0].at >= 80, `retried after ${seen[1].at - seen[0].at} ms`);
  });

  await t.test(`${name}: release hands the message back without counting an attempt`, async (sub) => {
    const { queue } = await open(sub);
    const name = await queueFor('q-release');
    const seen = [];
    await consume(sub, queue, name, (delivery) => {
      seen.push([delivery.attempt, delivery.redelivered]);
      return seen.length === 1 ? delivery.release() : delivery.ack();
    });
    await queue.produce(name, 'handed back');
    await waitFor(() => seen.length === 2, { timeout });
    assert.deepStrictEqual(seen, [
      [1, false],
      [1, true],
    ]);
  });

  await t.test(`${name}: deadLetter moves the message to the dead-letter queue with a reason`, async (sub) => {
    const { queue } = await open(sub);
    const name = await queueFor('q-dead');
    const dlq = await queueFor('q-dead-dlq');
    const seen = [];
    await consume(
      sub,
      queue,
      name,
      (delivery) => {
        seen.push(delivery.body);
        return delivery.deadLetter('poison');
      },
      { deadLetter: dlq },
    );
    const dead = [];
    await consume(sub, queue, dlq, (delivery) => {
      dead.push(delivery);
      return delivery.ack();
    });
    await queue.produce(name, 'poison pill', { headers: { 'x-tenant': 't1' } });
    await waitFor(() => dead.length === 1, { timeout });
    assert.strictEqual(dead[0].body, 'poison pill');
    assert.strictEqual(dead[0].headers['x-wrpc-dead-reason'], 'poison');
    assert.strictEqual(dead[0].headers['x-wrpc-attempt'], '1');
    assert.strictEqual(dead[0].headers['x-tenant'], 't1');
    await timers.setTimeout(quietWindow);
    assert.deepStrictEqual(seen, ['poison pill'], 'a dead-lettered message came back');
  });

  await t.test(`${name}: a dead-letter reason spanning lines is folded to one header line`, async (sub) => {
    const { queue } = await open(sub);
    const name = await queueFor('q-dead-lines');
    const dlq = await queueFor('q-dead-lines-dlq');
    // A validator's message lists its failures one per line; a broker header
    // is a line (NATS refuses CR/LF outright). The adapter folds it, so the
    // settlement lands and the message does not come back forever.
    await consume(sub, queue, name, (delivery) => delivery.deadLetter('500 first\nsecond\r\nthird'), {
      deadLetter: dlq,
    });
    const dead = [];
    await consume(sub, queue, dlq, (delivery) => {
      dead.push(delivery);
      return delivery.ack();
    });
    await queue.produce(name, 'multi');
    await waitFor(() => dead.length === 1, { timeout });
    assert.strictEqual(dead[0].headers['x-wrpc-dead-reason'], '500 first second third');
  });

  await t.test(`${name}: the first settlement wins`, async (sub) => {
    const { queue } = await open(sub);
    const name = await queueFor('q-settle');
    const seen = [];
    await consume(sub, queue, name, async (delivery) => {
      seen.push(delivery.attempt);
      await delivery.ack();
      await delivery.retry({ delay: 0 });
      await delivery.release();
    });
    await queue.produce(name, 'once');
    await waitFor(() => seen.length === 1, { timeout });
    await timers.setTimeout(settledWindow);
    assert.deepStrictEqual(seen, [1]);
  });

  await t.test(`${name}: stop() hands unsettled work to the next consumer`, async (sub) => {
    const { queue } = await open(sub);
    const name = await queueFor('q-stop');
    let taken = null;
    const first = await queue.consume(name, (delivery) => {
      taken = delivery; // never settled
    });
    await queue.produce(name, 'unfinished');
    await waitFor(() => taken !== null, { timeout });
    await first.stop();
    assert.strictEqual(first.healthy, false);
    const again = [];
    await consume(sub, queue, name, (delivery) => {
      again.push(delivery);
      return delivery.ack();
    });
    await waitFor(() => again.length === 1, { timeout: redelivery + timeout });
    assert.strictEqual(again[0].body, 'unfinished');
    // `redelivered` is best-effort: a broker with a delivery counter (or an
    // adapter that re-published) knows, and Kafka — where a rebalance simply
    // rewinds a committed offset — cannot.
    assert.strictEqual(typeof again[0].redelivered, 'boolean');
    // A settlement from the stopped consumer is a harmless no-op.
    await taken.ack();
  });

  await t.test(`${name}: pause() stops new deliveries but keeps held ones settleable`, async (sub) => {
    const { queue } = await open(sub);
    const name = await queueFor('q-pause');
    const seen = [];
    const consumer = await consume(sub, queue, name, (delivery) => void seen.push(delivery), { prefetch: 4 });
    assert.strictEqual(typeof consumer.pause, 'function');
    await queue.produce(name, 'held');
    await waitFor(() => seen.length === 1, { timeout });
    await consumer.pause();
    await queue.produce(name, 'waits');
    await timers.setTimeout(settledWindow);
    assert.strictEqual(seen.length, 1, 'a paused consumer took new work');
    // The held delivery is still ours to settle, and is not redelivered.
    await seen[0].ack();
    await consumer.resume();
    await waitFor(() => seen.length === 2, { timeout });
    assert.strictEqual(seen[1].body, 'waits');
    await seen[1].ack();
    await timers.setTimeout(ackWindow);
    assert.strictEqual(seen.length, 2, 'a message acked while paused came back');
  });

  await t.test(`${name}: a handler that throws does not lose the message`, async (sub) => {
    const { queue } = await open(sub);
    const name = await queueFor('q-throw');
    let calls = 0;
    const seen = [];
    await consume(sub, queue, name, async (delivery) => {
      calls++;
      seen.push([delivery.attempt, delivery.redelivered]);
      if (calls === 1) throw new Error('handler bug');
      await delivery.ack();
    });
    await queue.produce(name, 'survivor');
    await waitFor(() => calls === 2, { timeout: redelivery + timeout });
    // A retry, not a release: attempt + 1, so a handler that always throws
    // exhausts its attempts instead of spinning the queue at its head.
    assert.deepStrictEqual(seen, [
      [1, false],
      [2, true],
    ]);
  });

  // Only where the harness can make the broker refuse a publish: a
  // settlement that has to write a copy (a retry, a dead letter) and is
  // refused once still lands — the adapter tries the settlement again.
  if (harness.failNextPublish) {
    await t.test(
      `${name}: a settlement the broker refuses once is tried again, the message is not lost`,
      async (sub) => {
        const { queue } = await open(sub);
        const name = await queueFor('q-refused');
        const seen = [];
        await consume(sub, queue, name, async (delivery) => {
          seen.push([delivery.attempt, delivery.redelivered]);
          if (delivery.attempt === 1) {
            harness.failNextPublish(1);
            return delivery.retry({ delay: 0 });
          }
          await delivery.ack();
        });
        await queue.produce(name, 'once');
        await waitFor(() => seen.length === 2, { timeout: redelivery + timeout });
        assert.deepStrictEqual(seen, [
          [1, false],
          [2, true],
        ]);
      },
    );
  }

  // Only where the harness can break a live consumer from the outside (a
  // channel the server closes, a consumer that crashes): `healthy` follows.
  if (harness.breakConsumer) {
    await t.test(`${name}: healthy flips to false when the consumer breaks, and back when it recovers`, async (sub) => {
      const { queue } = await open(sub);
      const name = await queueFor('q-health');
      const seen = [];
      const consumer = await consume(sub, queue, name, (delivery) => {
        seen.push(delivery.body);
        return delivery.ack();
      });
      assert.strictEqual(consumer.healthy, true);
      await harness.breakConsumer(name);
      await waitFor(() => consumer.healthy === false, { timeout, message: 'healthy never flipped' });
      await waitFor(() => consumer.healthy === true, { timeout, message: 'the consumer never recovered' });
      await queue.produce(name, 'after');
      await waitFor(() => seen.includes('after'), { timeout });
    });
  }

  await t.test(`${name}: queues are isolated`, async (sub) => {
    const { queue } = await open(sub);
    const left = await queueFor('q-left');
    const right = await queueFor('q-right');
    const seen = [];
    await consume(sub, queue, left, (delivery) => {
      seen.push(delivery.body);
      return delivery.ack();
    });
    await queue.produce(right, 'wrong');
    await queue.produce(left, 'right');
    await waitFor(() => seen.length === 1, { timeout });
    await timers.setTimeout(quietWindow);
    assert.deepStrictEqual(seen, ['right']);
  });
};

module.exports = { runQueueContract };
