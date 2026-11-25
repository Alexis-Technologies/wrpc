'use strict';

// The backplane contract (tests/broker/backplaneContract.js) over the two
// backplanes this subpath ships: the in-process one, and the Redis adapter
// over its in-repo ioredis-shaped fake. The same suite runs against a real
// Redis in tests/broker/redis.integration.test.js.

const { describe } = require('node:test');

const { MemoryBackplane, createRedisAdapter } = require('../../src/scaling/index.js');
const { runBackplaneContract } = require('../broker/backplaneContract.js');
const { quiet } = require('../broker/support.js');
const { FakeRedis } = require('./fakeRedis.js');

describe('MemoryBackplane', () => {
  runBackplaneContract('backplane contract', {
    open: async () => {
      const backplane = new MemoryBackplane({ logger: quiet });
      return [backplane, backplane];
    },
    close: async (a) => a.close(),
  });
});

describe('createRedisAdapter (fake)', () => {
  runBackplaneContract('backplane contract', {
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
