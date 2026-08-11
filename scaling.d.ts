/**
 * `@alexify/wrpc/scaling` — the rooms backplane.
 *
 * A backplane carries room events between wrpc instances, so
 * `server.to('chat').emit(...)` reaches everyone in the room across every
 * process rather than only the ones this process holds. It is optional:
 * without one, rooms work exactly the same inside a single instance.
 *
 * Delivery is **at-most-once**. A message published while an instance is
 * between subscriptions, or dropped by the broker, is gone — rooms are a
 * fan-out mechanism, not a queue.
 */

/** Removes one subscription; safe to call more than once. */
export type Unsubscribe = () => void | Promise<unknown>;

export type BackplaneHandler = (message: string) => void;

/**
 * The structural contract — anything with this shape plugs into
 * `new Server({ backplane })` / `new RpcServer({ backplane })`.
 *
 * `message` is always a string (the rooms layer serializes its own
 * envelope), so an adapter never has to know the payload shape.
 */
export interface Backplane {
  name?: string;
  publish(channel: string, message: string): void | Promise<unknown>;
  subscribe(
    channel: string,
    handler: BackplaneHandler,
  ): Unsubscribe | Promise<Unsubscribe>;
  close(): void | Promise<unknown>;
}

export interface MemoryBackplaneOptions {
  /** Channel namespace; default 'wrpc'. */
  prefix?: string;
  console?: Console;
}

/**
 * In-process backplane: the reference implementation, and what makes
 * multi-instance fan-out testable without a broker — two RpcServer
 * instances sharing one MemoryBackplane behave like two processes sharing
 * a Redis. Delivery is deferred to a microtask, like a real broker's.
 */
export declare class MemoryBackplane implements Backplane {
  constructor(options?: MemoryBackplaneOptions);
  readonly name: string;
  /** Number of channels with at least one subscriber. */
  readonly size: number;
  publish(channel: string, message: string): void;
  subscribe(channel: string, handler: BackplaneHandler): Unsubscribe;
  close(): void;
}

export declare function createMemoryBackplane(
  options?: MemoryBackplaneOptions,
): MemoryBackplane;

/**
 * The subset of an ioredis client this adapter uses. Injected, never
 * required — any client with this shape works.
 */
export interface RedisPublisher {
  publish(channel: string, message: string): unknown;
  duplicate?(): RedisSubscriber;
}

export interface RedisSubscriber {
  subscribe(channel: string): unknown;
  unsubscribe?(channel: string): unknown;
  on(event: 'message', listener: (channel: string, message: string) => void): unknown;
  off?(event: 'message', listener: (...args: Array<any>) => void): unknown;
  removeListener?(event: 'message', listener: (...args: Array<any>) => void): unknown;
  /** Used only on a subscriber the adapter duplicated for itself. */
  quit?(): unknown;
  disconnect?(): unknown;
}

export interface RedisAdapterOptions {
  pub: RedisPublisher;
  /** Defaults to `pub.duplicate()`; a subscribed client cannot publish. */
  sub?: RedisSubscriber;
  /** Channel namespace; default 'wrpc'. */
  prefix?: string;
  console?: Console;
}

/**
 * Redis backplane over injected ioredis-shaped clients.
 *
 * `close()` releases this adapter's subscriptions and its listener. An
 * INJECTED client is never quit — its lifetime belongs to the caller — but a
 * subscriber the adapter created for itself through `pub.duplicate()` is,
 * since no one else holds a reference to close it.
 */
export declare function createRedisAdapter(
  options: RedisAdapterOptions,
): Backplane;

/** Structural check used by RpcServer to reject a malformed backplane. */
export declare function isBackplane(value: unknown): value is Backplane;

export declare const DEFAULT_PREFIX: string;
