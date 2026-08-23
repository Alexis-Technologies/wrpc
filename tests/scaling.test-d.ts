import { expectAssignable, expectError, expectType } from 'tsd';
import * as scaling from '../scaling.js';
import type { Backplane, BackplaneHandler, MemoryBackplane, RedisAdapterOptions, Unsubscribe } from '../scaling.js';
import type { RpcServerOptions, ServerOptions, Router } from '../index.js';

expectType<typeof MemoryBackplane>(scaling.MemoryBackplane);

// The memory backplane satisfies the structural contract
const memory = new scaling.MemoryBackplane();
expectAssignable<Backplane>(memory);
expectType<number>(memory.size);
expectType<string>(memory.name);
expectType<Unsubscribe>(
  memory.subscribe('room:chat', (message) => {
    expectType<string>(message);
  }),
);
expectType<void>(memory.publish('room:chat', '{}'));
expectType<void>(memory.close());
expectAssignable<MemoryBackplane>(scaling.createMemoryBackplane({ prefix: 'app' }));

// Anything with the right shape plugs in — that is the whole point
expectAssignable<Backplane>({
  publish: (channel: string, message: string) => void [channel, message],
  subscribe: (_channel: string, _handler: BackplaneHandler) => () => {},
  close: () => {},
});
expectError<Backplane>({ publish: () => {}, subscribe: () => () => {} }); // close missing

// Redis clients are injected, never imported
declare const ioredis: {
  publish(channel: string, message: string): Promise<number>;
  duplicate(): {
    subscribe(channel: string): Promise<unknown>;
    unsubscribe(channel: string): Promise<unknown>;
    on(event: 'message', listener: (channel: string, message: string) => void): unknown;
  };
};
expectAssignable<RedisAdapterOptions>({ pub: ioredis });
expectAssignable<RedisAdapterOptions>({ pub: ioredis, sub: ioredis.duplicate(), prefix: 'app' });
expectAssignable<Backplane>(scaling.createRedisAdapter({ pub: ioredis }));
expectError(scaling.createRedisAdapter({}));

expectType<boolean>(scaling.isBackplane(memory));
expectType<string>(scaling.DEFAULT_PREFIX);

// A backplane is accepted wherever a server is configured
declare const router: Router;
expectAssignable<RpcServerOptions>({ router, backplane: memory, instanceId: 'node-1' });
expectAssignable<ServerOptions>({ router, backplane: memory });
expectAssignable<ServerOptions>({ router, backplane: null });
