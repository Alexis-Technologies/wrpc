'use strict';

// The backplane contract (tests/broker/backplaneContract.js) over the two
// backplanes this subpath ships: the in-process one, and the Redis adapter
// over its in-repo ioredis-shaped fake. The same suite runs against a real
// Redis in tests/broker/redis.integration.test.js.

const { test } = require('node:test');

const { MemoryBackplane, createRedisAdapter } = require('../../scaling.js');
const { runBackplaneContract } = require('../broker/backplaneContract.js');
const { quiet } = require('../broker/support.js');
const { FakeRedis } = require('./fakeRedis.js');

test('MemoryBackplane: backplane contract', async (t) => {
  await runBackplaneContract(t, 'memory backplane', {
    open: async () => {
      const backplane = new MemoryBackplane({ logger: quiet });
      return [backplane, backplane];
    },
    close: async (a) => a.close(),
  });
});

test('createRedisAdapter (fake): backplane contract', async (t) => {
  await runBackplaneContract(t, 'redis adapter over the fake', {
    open: async () => {
      const pub = new FakeRedis();
      return [createRedisAdapter({ pub, logger: quiet }), createRedisAdapter({ pub: pub.duplicate(), logger: quiet })];
    },
    close: async (a, b) => {
      a.close();
      b.close();
    },
  });
});
