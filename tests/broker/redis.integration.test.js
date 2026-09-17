'use strict';

// The Redis broker against a REAL server: the same four contract suites the
// fake runs in tests/broker/redis.test.js. Skipped without REDIS_URL; run
// it against the compose service:
//
//   pnpm redis:up
//   REDIS_URL=redis://127.0.0.1:6379 node --test tests/broker/redis.integration.test.js
//
// CI runs it in the `redis` job. Not part of the coverage numbers — the
// fake is what `pnpm test` exercises.

const { test } = require('node:test');
const assert = require('node:assert');

const { runBackplaneContract } = require('./backplaneContract.js');
const { runLogContract } = require('./logContract.js');
const { runQueueContract } = require('./queueContract.js');
const { runDirectContract } = require('./directContract.js');
const { quiet, unique, waitFor, collect } = require('./support.js');

const url = process.env.REDIS_URL;
const options = { skip: url ? false : 'set REDIS_URL to run the Redis integration suite' };

let Redis = null;
if (url) {
  try {
    Redis = require('ioredis');
  } catch {
    options.skip = 'ioredis is not installed';
  }
}

const { createRedisBroker } = require('../../broker/redis.js');

const clients = [];
const client = () => {
  const created = new Redis(url, { maxRetriesPerRequest: 2, lazyConnect: false });
  created.on('error', () => {});
  clients.push(created);
  return created;
};

const brokers = [];
const open = (extra = {}) => {
  const broker = createRedisBroker({
    client: client(),
    logger: quiet,
    blockMs: 200,
    claimIdleMs: 300,
    inboxTtl: 5000,
    ...extra,
  });
  brokers.push(broker);
  return broker;
};

test('redis (real): backplane contract', options, async (t) => {
  await runBackplaneContract(t, 'redis', {
    open: async () => {
      const [a, b] = [open(), open()];
      return [a.backplane, b.backplane];
    },
    close: async () => {},
    settle: 100,
    timeout: 5000,
  });
});

test('redis (real): log contract', options, async (t) => {
  await runLogContract(t, 'redis', {
    open: async () => {
      const broker = open();
      const peer = open();
      const admin = client();
      return {
        log: broker.log,
        peer: peer.log,
        close: async () => {
          await broker.close();
          await peer.close();
        },
        trim: async (topic, keep) => {
          await admin.xtrim(`wrpc:log:${topic}`, 'MAXLEN', String(keep));
        },
        foreignId: () => '1-0',
        beyondTip: (_topic, id) => `${Number(id.split('-')[0]) + 60_000}-0`,
      };
    },
    timeout: 8000,
  });
});

test('redis (real): queue contract', options, async (t) => {
  await runQueueContract(t, 'redis', {
    open: async () => {
      const broker = open();
      const peer = open();
      return {
        queue: broker.queue,
        peer: peer.queue,
        close: async () => {
          await broker.close();
          await peer.close();
        },
      };
    },
    timeout: 8000,
    redelivery: 2000,
    settle: 300,
  });
});

test('redis (real): direct contract', options, async (t) => {
  await runDirectContract(t, 'redis', {
    open: async () => {
      const broker = open();
      const peer = open();
      return {
        direct: broker.direct,
        peer: peer.direct,
        close: async () => {
          await broker.close();
          await peer.close();
        },
      };
    },
    timeout: 8000,
    settle: 150,
  });
});

test('redis (real): a feed survives a reader that reconnects to another instance', options, async (t) => {
  const producer = open();
  const first = open();
  const second = open();
  t.after(async () => {
    await producer.close();
    await first.close();
    await second.close();
  });
  const topic = unique('feed');
  const read = first.log.read(topic, { from: 'latest' });
  await read.ready;
  const pending = collect(read, 2, { timeout: 8000 });
  await producer.log.append(topic, 'one');
  await producer.log.append(topic, 'two');
  const seen = await pending;
  await producer.log.append(topic, 'three');
  const resumed = await collect(second.log.read(topic, { after: seen[1].id }), 1, { timeout: 8000 });
  assert.deepStrictEqual(
    resumed.map((entry) => entry.value),
    ['three'],
  );
});

test.after(async () => {
  for (const broker of brokers) await broker.close().catch(() => {});
  for (const connection of clients) {
    try {
      await connection.quit();
    } catch {
      connection.disconnect();
    }
  }
});

test('redis (real): the injected client is never quit by close()', options, async () => {
  const injected = client();
  const broker = createRedisBroker({ client: injected, logger: quiet, blockMs: 100 });
  await broker.log.append(unique('t'), 'x');
  await broker.close();
  assert.strictEqual(await injected.ping(), 'PONG');
  await waitFor(() => true);
});
