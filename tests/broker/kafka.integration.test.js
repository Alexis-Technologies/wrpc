'use strict';

// The Kafka broker against a REAL broker, on whichever KafkaJS-shaped
// clients are installed (both, by default). Skipped without KAFKA_BROKERS:
//
//   docker compose up -d kafka
//   KAFKA_BROKERS=127.0.0.1:9092 node --test tests/broker/kafka.integration.test.js

const { test } = require('node:test');
const assert = require('node:assert');

const { createKafkaBroker, decodeVector, encodeVector } = require('../../broker/kafka.js');
const { runBackplaneContract } = require('./backplaneContract.js');
const { runLogContract } = require('./logContract.js');
const { runQueueContract } = require('./queueContract.js');
const { quiet, unique, collect } = require('./support.js');

const brokers = (process.env.KAFKA_BROKERS ?? '').split(',').filter(Boolean);

const clients = [];
if (brokers.length > 0) {
  for (const [flavor, load] of [
    ['kafkajs', () => require('kafkajs')],
    ['confluent', () => require('@confluentinc/kafka-javascript').KafkaJS],
  ]) {
    try {
      const lib = load();
      clients.push({ flavor, lib });
    } catch {
      // Not installed (or, for the native one, not built here).
    }
  }
}

const options =
  brokers.length === 0
    ? { skip: 'set KAFKA_BROKERS to run the Kafka integration suite' }
    : clients.length === 0
      ? { skip: 'no KafkaJS-shaped client is installed' }
      : { skip: false };

const made = [];
const create = ({ flavor, lib }, extra = {}) => {
  const kafka =
    flavor === 'confluent'
      ? new lib.Kafka({ kafkaJS: { brokers, logLevel: lib.logLevel.NOTHING } })
      : new lib.Kafka({ brokers, logLevel: lib.logLevel.NOTHING, retry: { retries: 2 } });
  const broker = createKafkaBroker({ kafka, logger: quiet, partitions: 2, ...extra });
  made.push(broker);
  return broker;
};

for (const client of clients.length > 0 ? clients : [{ flavor: 'none', lib: null }]) {
  const prefix = () => `wrpc${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;

  test(`kafka (real, ${client.flavor}): backplane contract`, options, async (t) => {
    const namespace = prefix();
    await runBackplaneContract(t, `kafka/${client.flavor}`, {
      open: async () => [
        create(client, { prefix: namespace, backplane: { partitions: 1 } }).backplane,
        create(client, { prefix: namespace, backplane: { partitions: 1 } }).backplane,
      ],
      close: async () => {},
      // A fresh consumer group is deaf until it has joined.
      settle: 1500,
      timeout: 20_000,
    });
  });

  test(`kafka (real, ${client.flavor}): log contract`, options, async (t) => {
    const namespace = prefix();
    await runLogContract(t, `kafka/${client.flavor}`, {
      open: async () => {
        const broker = create(client, { prefix: namespace });
        const peer = create(client, { prefix: namespace });
        return {
          log: broker.log,
          peer: peer.log,
          close: async () => {},
          beyondTip: (_topic, id) => {
            const cursor = decodeVector(id);
            for (const partition of Object.keys(cursor)) cursor[partition] += 1000;
            return encodeVector(cursor);
          },
        };
      },
      timeout: 30_000,
    });
  });

  test(`kafka (real, ${client.flavor}): queue contract`, options, async (t) => {
    const namespace = prefix();
    await runQueueContract(t, `kafka/${client.flavor}`, {
      open: async () => {
        const broker = create(client, { prefix: namespace });
        const peer = create(client, { prefix: namespace });
        return { queue: broker.queue, peer: peer.queue, close: async () => {} };
      },
      timeout: 30_000,
      redelivery: 10_000,
      settle: 1000,
    });
  });
}

test('kafka (real): a durable feed resumes on another instance', options, async (t) => {
  const client = clients[0];
  const namespace = `wrpc${Date.now().toString(36)}`;
  const producer = create(client, { prefix: namespace });
  const reader = create(client, { prefix: namespace });
  const topic = unique('feed');
  const first = await producer.log.append(topic, 'one');
  await producer.log.append(topic, 'two');
  const entries = await collect(reader.log.read(topic, { after: first }), 1, { timeout: 30_000 });
  assert.deepStrictEqual(
    entries.map((entry) => entry.value),
    ['two'],
  );
  void t;
});

test.after(async () => {
  for (const broker of made) await broker.close().catch(() => {});
});
