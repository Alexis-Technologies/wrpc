/**
 * `@alexify/wrpc/broker/redis` — the Redis broker: all four capabilities
 * over an INJECTED ioredis-shaped client. Valkey, KeyDB and Dragonfly speak
 * the same commands and need no adapter of their own.
 *
 * @experimental Part of the `@alexify/wrpc/broker*` family.
 */

import type { Broker } from '../broker.js';
import type { WrpcLogger } from '../index.js';

/** The command surface the adapter uses; ioredis satisfies it as it is. */
export interface RedisBrokerClient {
  xadd(key: string, ...args: Array<string>): Promise<string>;
  xrange(key: string, start: string, end: string, ...args: Array<string | number>): Promise<Array<any>>;
  xrevrange(key: string, end: string, start: string, ...args: Array<string | number>): Promise<Array<any>>;
  xread(...args: Array<string | number>): Promise<Array<any> | null>;
  xreadgroup(...args: Array<string | number>): Promise<Array<any> | null>;
  xgroup(...args: Array<string | number>): Promise<unknown>;
  xack(key: string, group: string, id: string): Promise<number>;
  xdel(key: string, id: string): Promise<number>;
  xautoclaim(...args: Array<string | number>): Promise<Array<any>>;
  xinfo(...args: Array<string>): Promise<any>;
  zadd(key: string, score: string | number, member: string): Promise<unknown>;
  zrangebyscore(key: string, min: string, max: string, ...args: Array<string | number>): Promise<Array<string>>;
  zrem(key: string, member: string): Promise<number>;
  publish(channel: string, message: string): Promise<number> | number;
  subscribe(channel: string): unknown;
  unsubscribe?(channel: string): unknown;
  on(event: string, listener: (...args: Array<any>) => void): unknown;
  set(key: string, value: string, ...args: Array<string | number>): Promise<unknown>;
  exists(key: string): Promise<number>;
  del(key: string): Promise<number>;
  rpush(key: string, value: string): Promise<number>;
  blpop(key: string, seconds: number): Promise<[string, string] | null>;
  pexpire(key: string, ms: string | number): Promise<unknown>;
  duplicate?(): RedisBrokerClient;
  quit?(): Promise<unknown>;
  disconnect?(): void;
}

export interface RedisBrokerOptions {
  /** The client every command runs on; never quit by close(). */
  client: RedisBrokerClient;
  /** A second connection for subscriptions; `client.duplicate()` otherwise. */
  subscriber?: RedisBrokerClient;
  /** Opens the extra connections blocking reads need; `client.duplicate()` otherwise. */
  connect?: () => RedisBrokerClient;
  /** Key and channel namespace; default 'wrpc'. */
  prefix?: string;
  logger?: WrpcLogger | boolean;
  /** How long a blocking read parks before looping. Default 1000 ms. */
  blockMs?: number;
  /** Idle time after which a stopped consumer's pending messages are claimed. Default 60 000 ms. */
  claimIdleMs?: number;
  /** `XADD MAXLEN ~ n` on every log append; 0 (default) never trims. */
  maxLen?: number;
  /** TTL of a group's delivery list and its presence key. Default 60 000 ms. */
  inboxTtl?: number;
}

export declare function createRedisBroker(options: RedisBrokerOptions): Broker;

/** Compares two stream ids (`<ms>-<seq>`) numerically. */
export declare function compareIds(a: string, b: string): number;
