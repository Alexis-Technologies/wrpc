'use strict';

// The AMQP broker against a REAL RabbitMQ: the same contract suites the
// fake runs in tests/broker/amqp.test.js. Skipped without AMQP_URL:
//
//   docker compose up -d rabbitmq
//   AMQP_URL=amqp://127.0.0.1:5672 node --test tests/broker/amqp.integration.test.js

const { describe, it, after } = require('node:test');
const assert = require('node:assert/strict');

const { createAmqpBroker } = require('../../src/broker/amqp/index.js');
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
  } catch (error) {
    // Not installed here: a skip — unless CI says the package is expected.
    if (process.env.WRPC_INTEGRATION_STRICT) {
      throw new Error('amqplib is not installed (WRPC_INTEGRATION_STRICT is set)', { cause: error });
    }
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

describe('amqp (real)', () => {
  runBackplaneContract(
    'backplane contract',
    {
      open: async () => [(await open()).backplane, (await open()).backplane],
      close: async () => {},
      settle: 150,
      timeout: 6000,
    },
    options,
  );

  runLogContract(
    'log contract',
    {
      open: async () => {
        const broker = await open();
        const peer = await open();
        return { log: broker.log, peer: peer.log, close: async () => {} };
      },
      timeout: 12_000,
    },
    options,
  );

  runQueueContract(
    'queue contract',
    {
      open: async () => {
        const broker = await open();
        const peer = await open();
        return { queue: broker.queue, peer: peer.queue, close: async () => {} };
      },
      timeout: 12_000,
      redelivery: 3000,
      settle: 200,
      ackWindow: 3200,
    },
    options,
  );

  runDirectContract(
    'direct contract',
    {
      open: async () => {
        const broker = await open();
        const peer = await open();
        return { direct: broker.direct, peer: peer.direct, close: async () => {} };
      },
      timeout: 10_000,
      settle: 150,
    },
    options,
  );

  it('should resume a durable feed on another connection', options, async (t) => {
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
});

after(async () => {
  for (const broker of brokers) await broker.close().catch(() => {});
  for (const connection of connections) await connection.close().catch(() => {});
});
