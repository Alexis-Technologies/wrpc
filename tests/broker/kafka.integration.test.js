'use strict';

// The Kafka broker against a REAL broker, on whichever KafkaJS-shaped
// clients are installed (both, by default). Skipped without KAFKA_BROKERS:
//
//   docker compose up -d kafka
//   KAFKA_BROKERS=127.0.0.1:9092 node --test tests/broker/kafka.integration.test.js

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const timers = require('node:timers/promises');

const { createKafkaBroker, decodeVector, encodeVector } = require('../../src/broker/kafka/index.js');
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

const skipReason = () => {
  if (brokers.length === 0) return 'set KAFKA_BROKERS to run the Kafka integration suite';
  if (clients.length === 0) return 'no KafkaJS-shaped client is installed';
  return false;
};
const options = { skip: skipReason() };
if (options.skip && brokers.length > 0 && process.env.WRPC_INTEGRATION_STRICT) {
  throw new Error(`${options.skip} (WRPC_INTEGRATION_STRICT is set)`);
}

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

// A broker that has only just come up answers on its port before it can
// serve a consumer group: the first group join of its life creates
// `__consumer_offsets`, and until that is done a join is slow or refused —
// `docker compose up --wait` returns well before it. So one throwaway round
// trip through the backplane (a group join, a produce, a fetch) runs before
// any contract case, and is tried a second time after a pause: the suites
// then measure the adapter, not a cold broker. A broker that is really down
// still fails here — once, by name, instead of in every case below.
const WARM_TIMEOUT = 30_000;
const WARM_PAUSE = 3000;
const warmUp = async () => {
  for (let attempt = 0; ; attempt++) {
    const namespace = `wrpcwarm${Date.now().toString(36)}${attempt}`;
    const broker = create(clients[0], { prefix: namespace, backplane: { partitions: 1 } });
    try {
      const heard = [];
      await broker.backplane.subscribe('warm', (message) => heard.push(message));
      const deadline = Date.now() + WARM_TIMEOUT;
      while (heard.length === 0 && Date.now() < deadline) {
        broker.backplane.publish('warm', 'ping');
        await timers.setTimeout(500);
      }
      if (heard.length > 0) return;
      throw new Error(`Kafka at ${brokers.join(',')} delivered nothing within ${WARM_TIMEOUT} ms of a group join`);
    } catch (error) {
      if (attempt >= 1) throw error;
      await timers.setTimeout(WARM_PAUSE);
    } finally {
      await broker.close().catch(() => {});
    }
  }
};
if (!options.skip) before(warmUp, { timeout: 2 * WARM_TIMEOUT + 3 * WARM_PAUSE });

describe('kafka (real)', () => {
  for (const client of clients.length > 0 ? clients : [{ flavor: 'none', lib: null }]) {
    // One namespace per contract suite, shared by its cases.
    const prefix = () => `wrpc${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;

    const backplaneNamespace = prefix();
    runBackplaneContract(
      `backplane contract (${client.flavor})`,
      {
        open: async () => [
          create(client, { prefix: backplaneNamespace, backplane: { partitions: 1 } }).backplane,
          create(client, { prefix: backplaneNamespace, backplane: { partitions: 1 } }).backplane,
        ],
        close: async () => {},
        // A fresh consumer group is deaf until it has joined.
        settle: 1500,
        timeout: 20_000,
      },
      options,
    );

    const logNamespace = prefix();
    runLogContract(
      `log contract (${client.flavor})`,
      {
        open: async () => {
          const broker = create(client, { prefix: logNamespace });
          const peer = create(client, { prefix: logNamespace });
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
      },
      options,
    );

    const queueNamespace = prefix();
    runQueueContract(
      `queue contract (${client.flavor})`,
      {
        open: async () => {
          const broker = create(client, { prefix: queueNamespace });
          const peer = create(client, { prefix: queueNamespace });
          return { queue: broker.queue, peer: peer.queue, close: async () => {} };
        },
        timeout: 30_000,
        redelivery: 10_000,
        settle: 1000,
        ackWindow: 11_000,
      },
      options,
    );
  }

  it('should resume a durable feed on another instance', options, async (t) => {
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
});

after(async () => {
  for (const broker of made) await broker.close().catch(() => {});
});
