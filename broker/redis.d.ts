/**
 * `@alexify/wrpc/broker/redis` — the Redis broker: all four capabilities
 * over an INJECTED ioredis-shaped client. Valkey, KeyDB and Dragonfly speak
 * the same commands and need no adapter of their own.
 *
 * @experimental Part of the `@alexify/wrpc/broker*` family.
 */

import type { Broker } from '../broker.js';
import type { WrpcLogger } from '../index.js';

/**
 * The command surface the adapter uses; ioredis satisfies it as it is.
 *
 * The stream, sorted-set and `set` commands are typed as `(...args) => Promise<any>`
 * on purpose: ioredis declares each of them as a stack of overloads (one per
 * argument spelling), and an overloaded method is not assignable to a single
 * signature — the exact-argument spellings this interface used to carry made
 * `createRedisBroker({ client: new Redis() })` a type error while it was the
 * documented use. A tsd test holds the real `Redis` type against this one.
 */
export interface RedisBrokerClient {
  xadd(...args: Array<any>): Promise<any>;
  xrange(...args: Array<any>): Promise<any>;
  xrevrange(...args: Array<any>): Promise<any>;
  xread(...args: Array<any>): Promise<any>;
  xreadgroup(...args: Array<any>): Promise<any>;
  xgroup(...args: Array<any>): Promise<any>;
  xack(...args: Array<any>): Promise<any>;
  xdel(...args: Array<any>): Promise<any>;
  xautoclaim(...args: Array<any>): Promise<any>;
  xinfo(...args: Array<any>): Promise<any>;
  zadd(...args: Array<any>): Promise<any>;
  zrangebyscore(...args: Array<any>): Promise<any>;
  zrem(...args: Array<any>): Promise<any>;
  /** Prunes a service group's expired members; without it they expire with the key. */
  zremrangebyscore?(...args: Array<any>): Promise<any>;
  publish(...args: Array<any>): Promise<any> | number;
  subscribe(...args: Array<any>): unknown;
  unsubscribe?(...args: Array<any>): unknown;
  on(event: string, listener: (...args: Array<any>) => void): unknown;
  set(...args: Array<any>): Promise<any>;
  exists(...args: Array<any>): Promise<any>;
  del(...args: Array<any>): Promise<any>;
  rpush(...args: Array<any>): Promise<any>;
  blpop(...args: Array<any>): Promise<any>;
  pexpire(...args: Array<any>): Promise<any>;
  /**
   * Runs a delayed retry's promotion (ZREM, then XADD) as one step. ioredis
   * has it; a client without it — or a proxy that refuses scripts — takes
   * the adapter's two-step path, which puts the entry back when the second
   * step fails.
   */
  eval?(...args: Array<any>): Promise<any>;
  duplicate?(): RedisBrokerClient;
  quit?(): Promise<unknown>;
  disconnect?(): void;
}

export interface RedisBrokerOptions {
  /**
   * Mints every id this adapter puts on the wire — consumer names and direct inboxes. Used
   * VERBATIM — wrpc never truncates it, so a generator answering characters
   * the broker refuses in a consumer name or key fails at the driver, not here.
   * Strict: a non-function, or a function that does not answer a non-empty
   * string of at most 255 characters, is a TypeError at construction.
   */
  generateId?: () => string;
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
