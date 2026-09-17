'use strict';

// The AMQP broker against a REAL RabbitMQ: the same contract suites the
// fake runs in tests/broker/amqp.test.js. Skipped without AMQP_URL:
//
//   docker compose up -d rabbitmq
//   AMQP_URL=amqp://127.0.0.1:5672 node --test tests/broker/amqp.integration.test.js

const { test } = require('node:test');
const assert = require('node:assert');

const { createAmqpBroker } = require('../../broker/amqp.js');
const { runBackplaneContract } = require('./backplaneContract.js');
const { runLogContract } = require('./logContract.js');
const { runQueueContract } = require('./queueContract.js');
const { runDirectContract } = require('./directContract.js');
const { quiet, unique, collect } = require('./support.js');

const url = process.env.AMQP_URL;
const options = { skip: url ? false : 'set AMQP_URL to run the RabbitMQ integration suite' };

let amqp = null;
if (url) {
  try {
    amqp = require('amqplib');
  } catch {
    options.skip = 'amqplib is not installed';
  }
}

const prefix = `wrpc${Date.now().toString(36)}`;
const connections = [];
const brokers = [];

const open = async (extra = {}) => {
  const connection = await amqp.connect(url);
  connection.on('error', () => {});
  connections.push(connection);
  const broker = createAmqpBroker({ connection, prefix, logger: quiet, inboxTtl: 10_000, ...extra });
  brokers.push(broker);
  return broker;
};

test('amqp (real): backplane contract', options, async (t) => {
  await runBackplaneContract(t, 'amqp', {
    open: async () => [(await open()).backplane, (await open()).backplane],
    close: async () => {},
    settle: 150,
    timeout: 6000,
  });
});

test('amqp (real): log contract', options, async (t) => {
  await runLogContract(t, 'amqp', {
    open: async () => {
      const broker = await open();
      const peer = await open();
      return { log: broker.log, peer: peer.log, close: async () => {} };
    },
    timeout: 12_000,
  });
});

test('amqp (real): queue contract', options, async (t) => {
  await runQueueContract(t, 'amqp', {
    open: async () => {
      const broker = await open();
      const peer = await open();
      return { queue: broker.queue, peer: peer.queue, close: async () => {} };
    },
    timeout: 12_000,
    redelivery: 3000,
    settle: 200,
  });
});

test('amqp (real): direct contract', options, async (t) => {
  await runDirectContract(t, 'amqp', {
    open: async () => {
      const broker = await open();
      const peer = await open();
      return { direct: broker.direct, peer: peer.direct, close: async () => {} };
    },
    timeout: 10_000,
    settle: 150,
  });
});

test('amqp (real): a durable feed resumes on another connection', options, async (t) => {
  const producer = await open();
  const reader = await open();
  const topic = unique('feed');
  await producer.log.append(topic, 'one');
  const seen = await collect(producer.log.read(topic, { from: 'earliest' }), 1, { timeout: 10_000 });
  await producer.log.append(topic, 'two');
  const resumed = await collect(reader.log.read(topic, { after: seen[0].id }), 1, { timeout: 10_000 });
  assert.deepStrictEqual(
    resumed.map((entry) => entry.value),
    ['two'],
  );
  void t;
});

test.after(async () => {
  for (const broker of brokers) await broker.close().catch(() => {});
  for (const connection of connections) await connection.close().catch(() => {});
});
