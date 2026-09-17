import { expectAssignable, expectError, expectType } from 'tsd';
import * as redis from '../../broker/redis.js';
import type { Broker } from '../../broker.js';
import type { RedisBrokerClient, RedisBrokerOptions } from '../../broker/redis.js';

// The client is injected and duck-typed: ioredis satisfies it as it is.
declare const ioredis: RedisBrokerClient;
const broker = redis.createRedisBroker({ client: ioredis, prefix: 'app', maxLen: 10_000, logger: false });
expectAssignable<Broker>(broker);
expectType<number>(redis.compareIds('1-0', '2-0'));
expectAssignable<RedisBrokerOptions>({ client: ioredis, connect: () => ioredis, claimIdleMs: 30_000 });
expectError(redis.createRedisBroker({}));
expectError(redis.createRedisBroker({ client: ioredis, maxLen: 'all' }));
