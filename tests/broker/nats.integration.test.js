'use strict';

// The NATS broker against a REAL server (with JetStream): the same contract
// suites the fake runs in tests/broker/nats.test.js. Skipped without
// NATS_URL; run it against the compose service:
//
//   docker compose up -d nats
//   NATS_URL=nats://127.0.0.1:4222 node --test tests/broker/nats.integration.test.js

const { test } = require('node:test');
const assert = require('node:assert');

const { createNatsBroker } = require('../../broker/nats.js');
const { runBackplaneContract } = require('./backplaneContract.js');
const { runLogContract } = require('./logContract.js');
const { runQueueContract } = require('./queueContract.js');
const { runDirectContract } = require('./directContract.js');
const { quiet, unique, collect } = require('./support.js');

const url = process.env.NATS_URL;
const options = { skip: url ? false : 'set NATS_URL to run the NATS integration suite' };

let transport = null;
let js = null;
if (url) {
  try {
    transport = require('@nats-io/transport-node');
    js = require('@nats-io/jetstream');
  } catch {
    options.skip = 'the NATS client packages are not installed';
  }
}

const connections = [];
const brokers = [];
const prefix = `wrpc${Date.now().toString(36)}`;

const open = async (extra = {}) => {
  const nc = await transport.connect({ servers: url });
  connections.push(nc);
  const broker = createNatsBroker({
    nc,
    headers: transport.headers,
    createInbox: transport.createInbox,
    jetstream: js.jetstream,
    jetstreamManager: js.jetstreamManager,
    prefix,
    logger: quiet,
    ackWait: 1500,
    stream: { log: { storage: 'memory' }, queue: { storage: 'memory', retention: 'workqueue' } },
    ...extra,
  });
  brokers.push(broker);
  return broker;
};

test('nats (real): backplane contract', options, async (t) => {
  await runBackplaneContract(t, 'nats', {
    open: async () => [(await open()).backplane, (await open()).backplane],
    close: async () => {},
    settle: 50,
    timeout: 5000,
  });
});

test('nats (real): log contract', options, async (t) => {
  await runLogContract(t, 'nats', {
    open: async () => {
      const broker = await open();
      const peer = await open();
      const manager = await js.jetstreamManager(connections[connections.length - 1]);
      return {
        log: broker.log,
        peer: peer.log,
        close: async () => {},
        trim: async (topic, keep) => {
          const name = `${prefix}_log_${topic.replace(/[^A-Za-z0-9_-]/g, '_')}`;
          const info = await manager.streams.info(name);
          await manager.streams.purge(name, { seq: Number(info.state.last_seq) - keep + 1 });
        },
        beyondTip: (_topic, id) => String(Number(id) + 1000),
      };
    },
    timeout: 10_000,
  });
});

test('nats (real): queue contract', options, async (t) => {
  await runQueueContract(t, 'nats', {
    open: async () => {
      const broker = await open();
      const peer = await open();
      return { queue: broker.queue, peer: peer.queue, close: async () => {} };
    },
    timeout: 10_000,
    redelivery: 3000,
    settle: 200,
  });
});

test('nats (real): direct contract', options, async (t) => {
  await runDirectContract(t, 'nats', {
    open: async () => {
      const broker = await open();
      const peer = await open();
      return { direct: broker.direct, peer: peer.direct, close: async () => {} };
    },
    timeout: 8000,
    settle: 50,
  });
});

test('nats (real): a durable feed resumes on another instance', options, async (t) => {
  const producer = await open();
  const reader = await open();
  const topic = unique('feed');
  const first = await producer.log.append(topic, 'one', { headers: { tp: '00-trace' } });
  await producer.log.append(topic, 'two');
  const entries = await collect(reader.log.read(topic, { after: first }), 1, { timeout: 8000 });
  assert.deepStrictEqual(
    entries.map((entry) => entry.value),
    ['two'],
  );
  void t;
});

test.after(async () => {
  for (const broker of brokers) await broker.close().catch(() => {});
  for (const nc of connections) await nc.drain().catch(() => {});
});
