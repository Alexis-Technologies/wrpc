'use strict';

// The Kafka broker over the in-process fake (tests/broker/fakeKafka.js), in
// BOTH KafkaJS shapes — kafkajs and the confluent facade — so the
// normalization in src/broker/kafka/shape.js is exercised both ways. The
// same suites run against a real broker in tests/broker/kafka.integration.test.js.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { createKafkaBroker, encodeVector, decodeVector } = require('../../broker/kafka.js');
const { isBroker, isBrokerDirect } = require('../../broker.js');
const { createFakeKafka } = require('./fakeKafka.js');
const { runBackplaneContract } = require('./backplaneContract.js');
const { runLogContract } = require('./logContract.js');
const { runQueueContract } = require('./queueContract.js');
const { quiet, unique, waitFor, collect } = require('./support.js');

const FLAVORS = ['kafkajs', 'confluent'];

// Brokers a harness opened, closed when its suite is done.
const opened = [];

const open = (flavor, extra = {}) => {
  const kafka = createFakeKafka({ flavor });
  const broker = createKafkaBroker({ kafka, logger: quiet, partitions: 2, ...extra });
  return { kafka, broker };
};

for (const flavor of FLAVORS) {
  test(`kafka broker (fake, ${flavor}): backplane contract`, async (t) => {
    await runBackplaneContract(t, `kafka/${flavor}`, {
      open: async () => {
        const kafka = createFakeKafka({ flavor });
        const a = createKafkaBroker({ kafka, logger: quiet, partitions: 1 });
        const b = createKafkaBroker({ kafka, logger: quiet, partitions: 1 });
        opened.push(a, b);
        return [a.backplane, b.backplane];
      },
      // A consumer left running would keep the fake's timers — and the test
      // process — alive.
      close: async () => {
        for (const broker of opened.splice(0)) await broker.close();
      },
      settle: 50,
      timeout: 4000,
    });
  });

  test(`kafka broker (fake, ${flavor}): log contract`, async (t) => {
    await runLogContract(t, `kafka/${flavor}`, {
      open: async () => {
        const { kafka, broker } = open(flavor);
        const peer = createKafkaBroker({ kafka, logger: quiet, partitions: 2 });
        return {
          log: broker.log,
          peer: peer.log,
          close: async () => {
            await broker.close();
            await peer.close();
          },
          beyondTip: (_topic, id) => {
            const cursor = decodeVector(id);
            for (const partition of Object.keys(cursor)) cursor[partition] += 1000;
            return encodeVector(cursor);
          },
        };
      },
      timeout: 5000,
    });
  });

  test(`kafka broker (fake, ${flavor}): queue contract`, async (t) => {
    await runQueueContract(t, `kafka/${flavor}`, {
      open: async () => {
        const { kafka, broker } = open(flavor);
        const peer = createKafkaBroker({ kafka, logger: quiet, partitions: 2 });
        return {
          queue: broker.queue,
          peer: peer.queue,
          close: async () => {
            await broker.close();
            await peer.close();
          },
        };
      },
      timeout: 6000,
      redelivery: 1500,
    });
  });
}

test('kafka broker: no direct capability — and it says so', async () => {
  const { broker } = open('kafkajs');
  assert.strictEqual(isBroker(broker), true);
  assert.strictEqual(broker.direct, undefined);
  assert.strictEqual(isBrokerDirect(broker.direct), false);
  const { attachBrokerRpc } = require('../../broker.js');
  await assert.rejects(attachBrokerRpc({ rpc: null }, broker, { service: 'x' }), /Server or an RpcServer/);
  await broker.close();
});

test('kafka broker: the flavor is detected, and can be forced', async () => {
  const kafkajs = createFakeKafka({ flavor: 'kafkajs' });
  const confluent = createFakeKafka({ flavor: 'confluent' });
  // kafkajs' client has logger(); the confluent facade does not.
  for (const broker of [
    createKafkaBroker({ kafka: kafkajs, logger: quiet }),
    createKafkaBroker({ kafka: confluent, logger: quiet }),
    createKafkaBroker({ kafka: confluent, flavor: 'confluent', logger: quiet }),
  ]) {
    assert.strictEqual(broker.name, 'kafka');
    await broker.close();
  }
  assert.throws(() => createKafkaBroker({ kafka: kafkajs, flavor: 'librdkafka' }), /flavor must be/);
  assert.throws(() => createKafkaBroker({}), /KafkaJS-shaped client/);
  assert.throws(() => createKafkaBroker({ kafka: { producer() {} } }), /KafkaJS-shaped client/);
  assert.throws(() => createKafkaBroker({ kafka: kafkajs, partitions: 0 }), /partitions/);
});

test('kafka broker: the resume token is a vector of partition offsets', () => {
  const { broker } = open('kafkajs');
  assert.strictEqual(encodeVector({ 2: 5, 0: 1 }), 'k1:0=1,2=5');
  assert.deepStrictEqual(decodeVector('k1:0=1,2=5'), { 0: 1, 2: 5 });
  assert.strictEqual(decodeVector('k1:'), null);
  assert.strictEqual(decodeVector('0=1'), null);
  assert.strictEqual(decodeVector(42), null);
  assert.strictEqual(broker.log.parseId('k1:0=1'), 'k1:0=1');
  assert.strictEqual(broker.log.parseId('k1:x=1'), null);
  assert.strictEqual(broker.log.parseId(''), null);
  assert.throws(() => broker.log.read('t', { from: 'middle' }), /from must be/);
  return broker.close();
});

test('kafka broker: a feed resumes exactly, across partitions', async (t) => {
  // A feed topic is single-partition by default (order); this one opts into
  // two, which is where the vector id earns its keep.
  const { broker } = open('kafkajs', { logPartitions: 2 });
  t.after(() => broker.close());
  const topic = unique('feed');
  for (const value of ['1', '2', '3', '4']) await broker.log.append(topic, value);
  const first = await collect(broker.log.read(topic, { from: 'earliest' }), 2, { timeout: 5000 });
  const rest = await collect(broker.log.read(topic, { after: first[1].id }), 2, { timeout: 5000 });
  // Order is per partition, so the set is what matters across two of them.
  assert.deepStrictEqual([...first, ...rest].map((entry) => entry.value).sort(), ['1', '2', '3', '4']);
  // Every id is a full vector, so a resume covers every partition.
  assert.match(rest[1].id, /^k1:0=\d+,1=\d+$/);
});

test('kafka broker: the backplane loses what was published before the group joined', async (t) => {
  const { kafka, broker } = open('kafkajs', { partitions: 1 });
  t.after(() => broker.close());
  const other = createKafkaBroker({ kafka, logger: quiet, partitions: 1 });
  t.after(() => other.close());
  // Published while this instance has no consumer at all.
  other.backplane.publish('room', 'before');
  await timers.setTimeout(30);
  const seen = [];
  await broker.backplane.subscribe('room', (message) => seen.push(message));
  other.backplane.publish('room', 'after');
  await waitFor(() => seen.length === 1, { timeout: 4000 });
  await timers.setTimeout(50);
  assert.deepStrictEqual(seen, ['after'], 'a fresh group reads from `latest`');
});

test('kafka broker: closing disconnects every consumer and deletes the groups it made', async () => {
  const { kafka, broker } = open('kafkajs');
  const seen = [];
  await broker.backplane.subscribe('room', (message) => seen.push(message));
  const consumer = await broker.queue.consume(unique('q'), (delivery) => delivery.ack());
  assert.strictEqual(consumer.healthy, true);
  assert.ok(kafka.server.groups.size >= 2);
  await broker.close();
  await broker.close();
  assert.strictEqual(consumer.healthy, false);
  assert.strictEqual(kafka.server.groups.size, 0, 'the groups it created are gone');
  await assert.rejects(broker.log.append('t', 'x'), (error) => error.code === 503);
  await assert.rejects(broker.queue.produce('q', 'x'), (error) => error.code === 503);
  await assert.rejects(
    broker.queue.consume('q', () => {}),
    (error) => error.code === 503,
  );
});

test('kafka broker: pause and resume ride the consumer, not the group', async (t) => {
  const { broker } = open('kafkajs');
  t.after(() => broker.close());
  const name = unique('paused');
  const seen = [];
  const consumer = await broker.queue.consume(name, (delivery) => {
    seen.push(delivery.body);
    return delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'first');
  await waitFor(() => seen.length === 1, { timeout: 5000 });
  await consumer.pause();
  await broker.queue.produce(name, 'second');
  await timers.setTimeout(150);
  assert.deepStrictEqual(seen, ['first']);
  await consumer.resume();
  await waitFor(() => seen.length === 2, { timeout: 5000 });
});
