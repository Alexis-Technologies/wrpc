'use strict';

// The AMQP broker over the in-process fake (tests/broker/fakeAmqp.js): the
// same contract suites a real RabbitMQ runs in
// tests/broker/amqp.integration.test.js.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { createAmqpBroker } = require('../../broker/amqp.js');
const { isBroker } = require('../../broker.js');
const { createFakeAmqp } = require('./fakeAmqp.js');
const { runBackplaneContract } = require('./backplaneContract.js');
const { runLogContract } = require('./logContract.js');
const { runQueueContract } = require('./queueContract.js');
const { runDirectContract } = require('./directContract.js');
const { quiet, unique, waitFor, collect } = require('./support.js');

const open = (connection, extra = {}) =>
  createAmqpBroker({ connection, logger: quiet, queueType: 'classic', ...extra });

test('amqp broker (fake): backplane contract', async (t) => {
  await runBackplaneContract(t, 'amqp', {
    open: async () => {
      const connection = createFakeAmqp();
      return [open(connection).backplane, open(connection).backplane];
    },
    close: async () => {},
    settle: 10,
    timeout: 3000,
  });
});

test('amqp broker (fake): log contract', async (t) => {
  await runLogContract(t, 'amqp', {
    open: async () => {
      const connection = createFakeAmqp();
      const broker = open(connection);
      const peer = open(connection);
      return {
        log: broker.log,
        peer: peer.log,
        close: async () => {
          await broker.close();
          await peer.close();
        },
        trim: async (topic, keep) => {
          const queue = connection.server.queues.get(`wrpc.log.${topic}`);
          if (queue) queue.messages.splice(0, Math.max(0, queue.messages.length - keep));
        },
      };
    },
    timeout: 4000,
  });
});

test('amqp broker (fake): queue contract', async (t) => {
  await runQueueContract(t, 'amqp', {
    open: async () => {
      const connection = createFakeAmqp();
      const broker = open(connection);
      const peer = open(connection);
      return {
        queue: broker.queue,
        peer: peer.queue,
        close: async () => {
          await broker.close();
          await peer.close();
        },
      };
    },
    timeout: 4000,
    redelivery: 1000,
  });
});

test('amqp broker (fake): direct contract', async (t) => {
  await runDirectContract(t, 'amqp', {
    open: async () => {
      const connection = createFakeAmqp();
      const broker = open(connection);
      const peer = open(connection);
      return {
        direct: broker.direct,
        peer: peer.direct,
        close: async () => {
          await broker.close();
          await peer.close();
        },
      };
    },
    timeout: 4000,
    settle: 10,
  });
});

test('amqp broker: injection is validated structurally', async () => {
  assert.throws(() => createAmqpBroker({}), /amqplib connection/);
  assert.throws(() => createAmqpBroker({ connection: { createChannel() {} } }), /amqplib connection/);
  const broker = open(createFakeAmqp());
  assert.strictEqual(isBroker(broker), true);
  assert.strictEqual(broker.name, 'amqp');
  assert.match(broker.direct.inbox(), /^wrpc\.inbox\./);
  assert.throws(() => broker.backplane.subscribe('x', 'nope'), /handler must be a function/);
  assert.throws(() => broker.log.read('t', { from: 'middle' }), /from must be/);
  await assert.rejects(
    broker.queue.consume('q', () => {}, { prefetch: 0 }),
    /prefetch/,
  );
  await assert.rejects(broker.queue.consume('q', 'nope'), /onDelivery must be a function/);
  await assert.rejects(
    broker.direct.listen('', () => {}),
    /address must be a non-empty string/,
  );
  await broker.close();
});

test('amqp broker: a retry rides the TTL queue and is dead-lettered back', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  const name = unique('delayed');
  const seen = [];
  const consumer = await broker.queue.consume(name, (delivery) => {
    seen.push({ attempt: delivery.attempt, at: Date.now() });
    return delivery.attempt === 1 ? delivery.retry({ delay: 120 }) : delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'work');
  await waitFor(() => seen.length === 1, { timeout: 3000 });
  // It is parked on the retry queue, not the main one.
  await timers.setTimeout(30);
  assert.strictEqual(connection.server.queues.get(`wrpc.q.${name}.retry`).messages.length, 1);
  await waitFor(() => seen.length === 2, { timeout: 3000 });
  assert.ok(seen[1].at - seen[0].at >= 100);
  assert.strictEqual(connection.server.queues.get(`wrpc.q.${name}.retry`).messages.length, 0);
});

test('amqp broker: release is a requeue, which RabbitMQ 4 does not count', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  const name = unique('released');
  const seen = [];
  const consumer = await broker.queue.consume(name, (delivery) => {
    seen.push([delivery.attempt, delivery.redelivered]);
    return seen.length === 1 ? delivery.release() : delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'work');
  await waitFor(() => seen.length === 2, { timeout: 3000 });
  assert.deepStrictEqual(seen, [
    [1, false],
    [1, true],
  ]);
});

test('amqp broker: pause cancels the consumer but keeps held messages ackable', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  const name = unique('paused');
  const seen = [];
  const consumer = await broker.queue.consume(name, (delivery) => void seen.push(delivery), { prefetch: 4 });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'held');
  await waitFor(() => seen.length === 1, { timeout: 3000 });
  await consumer.pause();
  await broker.queue.produce(name, 'waits');
  await timers.setTimeout(80);
  assert.strictEqual(seen.length, 1);
  await seen[0].ack();
  await consumer.resume();
  await waitFor(() => seen.length === 2, { timeout: 3000 });
  assert.strictEqual(seen[1].body, 'waits');
  await seen[1].ack();
});

test('amqp broker: an unroutable send is refused with 503', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  await assert.rejects(broker.direct.send(unique('nobody'), 'x'), (error) => error.code === 503);
  // With a listener the same send goes through.
  const address = unique('svc');
  const seen = [];
  const stop = await broker.direct.listen(address, (message) => seen.push(message), { group: 'svc' });
  await broker.direct.send(address, 'hello', { timeout: 500 });
  await waitFor(() => seen.length === 1, { timeout: 3000 });
  await stop();
});

test('amqp broker: a feed resumes from a yielded offset, and a trimmed one answers 410', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  const topic = unique('feed');
  for (const value of ['1', '2', '3', '4']) await broker.log.append(topic, value);
  const first = await collect(broker.log.read(topic, { from: 'earliest' }), 2, { timeout: 3000 });
  const rest = await collect(broker.log.read(topic, { after: first[1].id }), 2, { timeout: 3000 });
  assert.deepStrictEqual(
    rest.map((entry) => entry.value),
    ['3', '4'],
  );
  // The retention overtakes the reader's position.
  const queue = connection.server.queues.get(`wrpc.log.${topic}`);
  queue.messages.splice(0, 3);
  const iterator = broker.log.read(topic, { after: first[0].id })[Symbol.asyncIterator]();
  await assert.rejects(iterator.next(), (error) => error.code === 410);
});

test('amqp broker: closing releases every channel and refuses new work', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  await broker.log.append(unique('t'), 'x');
  await broker.close();
  await broker.close();
  assert.ok(connection.channels.every((channel) => channel.closed));
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
  void t;
});
