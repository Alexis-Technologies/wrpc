/**
 * `@alexify/wrpc/broker` — broker-agnostic messaging for wrpc.
 *
 * A broker is described by CAPABILITIES; each adapter implements the subset
 * natural to it. Every concrete broker lives behind its own subpath
 * (`@alexify/wrpc/broker/redis`, `/nats`, `/amqp`, `/kafka`) and takes its
 * client by injection — nothing here depends on a broker package.
 *
 * @experimental The API may change in a minor release until every adapter
 * has shipped.
 */

import type {
  WrpcLogger,
  Context,
  SubscriptionOptions,
  Tracked,
  ConsumePolicy,
  Server,
  RpcServer,
  Validator,
} from './index.js';

export type { ConsumePolicy } from './index.js';
import type { Backplane } from './scaling.js';

export type { Backplane } from './scaling.js';

export type MessageHeaders = Record<string, string>;

/** A read position was refused: 400 malformed or beyond the tip, 410 gone. */
export interface BrokerError extends Error {
  code: number;
  expose?: boolean;
}

// ---------------------------------------------------------------------------
// log

export interface LogEntry {
  /** The resume token after this entry — what a feed yields as its event id. */
  id: string;
  value: string;
  headers: MessageHeaders;
}

export interface LogReadOptions {
  /** Entries strictly after this id; 410 when trimmed or foreign, 400 when malformed or past the tip. */
  after?: string | null;
  /** Without `after`: only new entries (default) or everything retained. */
  from?: 'latest' | 'earliest';
  signal?: AbortSignal | null;
}

export interface LogRead extends AsyncIterable<LogEntry> {
  /** Resolves once the read position is fixed. */
  readonly ready: Promise<void>;
}

/** An ordered, replayable journal — what a durable feed reads from. */
export interface BrokerLog {
  name?: string;
  append(topic: string, value: string | Uint8Array, options?: { headers?: MessageHeaders; key?: string }): Promise<string>;
  read(topic: string, options?: LogReadOptions): LogRead;
  /** Syntax only: the id when well-formed, null otherwise. */
  parseId(text: unknown): string | null;
}

// ---------------------------------------------------------------------------
// queue

export interface Delivery {
  readonly id: string;
  readonly body: string;
  readonly headers: MessageHeaders;
  /** 1-based, counted by the adapter. */
  readonly attempt: number;
  readonly redelivered: boolean;
  ack(): Promise<void>;
  /** Redelivered no sooner than `delay` ms, attempt + 1. */
  retry(options?: { delay?: number }): Promise<void>;
  /** Back to the queue, attempt unchanged. */
  release(): Promise<void>;
  /** To the consumer's dead-letter queue, or dropped when it has none. */
  deadLetter(reason?: string): Promise<void>;
}

export interface ConsumeOptions {
  /** The broker-side consumer group; defaults to the queue name. */
  group?: string;
  /** Unsettled deliveries at once, dispatched concurrently. Default 16. */
  prefetch?: number;
  deadLetter?: string | null;
  signal?: AbortSignal | null;
}

export interface QueueConsumer {
  /** No new deliveries; the unsettled ones stay settleable and are not handed back. */
  pause(): Promise<void>;
  resume(): Promise<void>;
  /** No new deliveries; the unsettled ones are redelivered later. */
  stop(): Promise<void>;
  readonly healthy: boolean;
}

/** At-least-once work distribution between competing consumers. */
export interface BrokerQueue {
  name?: string;
  produce(queue: string, body: string | Uint8Array, options?: { headers?: MessageHeaders; key?: string }): Promise<void>;
  consume(
    queue: string,
    onDelivery: (delivery: Delivery) => unknown,
    options?: ConsumeOptions,
  ): Promise<QueueConsumer>;
}

// ---------------------------------------------------------------------------
// direct

export interface DirectMessage {
  body: string | Uint8Array;
  headers: MessageHeaders;
  correlationId: string | null;
  replyTo: string | null;
}

export interface DirectSendOptions {
  headers?: MessageHeaders;
  correlationId?: string;
  replyTo?: string;
  /** A hint: the broker may discard a message nobody took within it. */
  timeout?: number;
}

/** Addressable inboxes — the substrate of RPC over a broker. At-most-once. */
export interface BrokerDirect {
  name?: string;
  inbox(): string;
  listen(
    address: string,
    onMessage: (message: DirectMessage) => unknown,
    options?: { group?: string | null },
  ): Promise<() => Promise<void>>;
  send(address: string, body: string | Uint8Array, options?: DirectSendOptions): Promise<void>;
}

// ---------------------------------------------------------------------------

export interface Broker {
  readonly name: string;
  readonly backplane?: Backplane;
  readonly log?: BrokerLog;
  readonly queue?: BrokerQueue;
  readonly direct?: BrokerDirect;
  close(): void | Promise<void>;
}

export declare function isBroker(value: unknown): value is Broker;
export declare function isBrokerLog(value: unknown): value is BrokerLog;
export declare function isBrokerQueue(value: unknown): value is BrokerQueue;
export declare function isBrokerDirect(value: unknown): value is BrokerDirect;
export declare function isBackplane(value: unknown): value is Backplane;

export interface MemoryBrokerOptions {
  /**
   * Mints every id this adapter puts on the wire — in practice just the `epoch`, since message ids and inboxes are counters off it. Used
   * VERBATIM — wrpc never truncates it, so a generator answering characters
   * the broker refuses in a name fails at the driver, not here.
   * Strict: a non-function, or a function that does not answer a non-empty
   * string of at most 255 characters, is a TypeError at construction.
   */
  generateId?: () => string;
  /** Backplane channel namespace; default 'wrpc'. */
  prefix?: string;
  /** Defaults to the global console; `false` silences the broker. */
  logger?: WrpcLogger | boolean;
  /** Stamped into every log id; random per instance by default. */
  epoch?: string;
  retention?: { maxEntries?: number };
}

/**
 * The in-process broker: the reference implementation of every capability.
 * Two RpcServers sharing one behave like two processes sharing a real broker.
 */
export declare class MemoryBroker implements Broker {
  constructor(options?: MemoryBrokerOptions);
  readonly name: 'memory';
  readonly epoch: string;
  readonly backplane: Backplane;
  readonly log: BrokerLog;
  readonly queue: BrokerQueue;
  readonly direct: BrokerDirect;
  /** Drops all but the newest `keep` entries of a topic. */
  trim(topic: string, keep: number): void;
  close(): void;
}

export declare function createMemoryBroker(options?: MemoryBrokerOptions): MemoryBroker;

// ---------------------------------------------------------------------------
// Durable feeds

export interface GapInfo {
  /** What the client holds: the id it sent, or the last token the feed handed it. */
  lastEventId: string | null | undefined;
  /** 400 — malformed, forged or past the tip; 410 — history gone or from another log. */
  code: 400 | 410;
}

export interface BrokerFeedOptions<Value = unknown, Mapped = Value> {
  /** A fresh subscription (no lastEventId) reads new entries (default) or everything retained. */
  from?: 'latest' | 'earliest';
  /** How an entry's string value becomes the yielded value. Default 'json'. */
  decode?: 'json' | 'text' | ((text: string) => Value);
  /** Reshape a value; answer undefined to skip the entry. */
  map?: (value: Value, entry: LogEntry, context: Context) => Mapped | undefined | Promise<Mapped | undefined>;
  /**
   * An unusable lastEventId, or a reader the retention overtook. Answer a
   * snapshot (a value, an iterable, an async iterable, or nothing); the feed
   * then continues with everything appended from the moment of the gap.
   * Without it the subscription ends with the coded error.
   */
  onGap?: (
    context: Context,
    args: any,
    info: GapInfo,
  ) => unknown | Iterable<unknown> | AsyncIterable<unknown> | Promise<unknown>;
  /** HMAC-sign the yielded ids, refusing any the feed never issued. */
  secret?: string;
  /** Longer peer-supplied ids are refused with 400. Default 512. */
  maxIdLength?: number;
}

/**
 * A subscription handler reading a broker log: every value is `tracked()`
 * with the log's resume token, so a client re-subscribing with
 * `lastEventId` resumes there — on any instance.
 *
 *   feed: procedure.subscription({ access: 'session', handler: brokerFeed(broker, 'orders') })
 */
export declare function brokerFeed<Value = unknown, Mapped = Value>(
  broker: Broker | BrokerLog,
  topic: string | ((context: Context, args: any) => string | Promise<string>),
  options?: BrokerFeedOptions<Value, Mapped>,
): (context: Context, args: any, subscription: SubscriptionOptions) => AsyncGenerator<Tracked<Mapped> | unknown>;

// ---------------------------------------------------------------------------
// Consumers and publishers

/** A binding-table entry: an override for a declared consumer, or a queue bound to an ordinary procedure. */
export interface ConsumerBinding extends ConsumePolicy {
  /** 'unit.vN/method' — only for a key that is not a declared consumer. */
  target?: string;
}

export interface DeadLetterInfo {
  queue: string;
  method: string;
  /** The wire code the call failed with (400 for an undecodable body). */
  code: number;
  error: Error | null;
  delivery: Delivery;
}

export interface AttachConsumersOptions {
  /** Bind every declared `consumes` procedure, not only the table's. Default true. */
  auto?: boolean;
  /** Called before a message is dead-lettered — for alerting. */
  onDeadLetter?: (info: DeadLetterInfo) => unknown;
  logger?: WrpcLogger | boolean | null;
  /** Clients kept per `identity.trust: 'token'` binding (LRU). Default 128. */
  tokenClients?: number;
}

export interface ConsumerBindingInfo {
  key: string;
  queue: string;
  group: string;
  method: string;
  healthy: boolean;
}

export interface ConsumersHandle {
  readonly bindings: Array<ConsumerBindingInfo>;
  readonly healthy: boolean;
  /** Also triggered by the server's `'draining'`. */
  pause(): Promise<void>;
  resume(): Promise<void>;
  /** Also triggered by the server's close(). */
  stop(): Promise<void>;
}

/**
 * Delivers broker queues into procedures, at least once, through the same
 * pipeline a call takes. Keys: 'unit.vN/source' overrides a declared
 * consumer; any other key is a queue bound to `target`.
 */
export declare function attachConsumers(
  server: Server | RpcServer,
  broker: Broker | BrokerQueue,
  table?: Record<string, ConsumerBinding>,
  options?: AttachConsumersOptions,
): Promise<ConsumersHandle>;

export interface PublishedEvent {
  /** Default '<unitKey>.<event>'. */
  topic?: string;
  /** Default 'log' when the broker has one, 'queue' otherwise. */
  to?: 'log' | 'queue';
  /** A partition/ordering key, or how to derive one from the payload. */
  key?: string | ((data: any) => string);
  validate?: Validator;
}

export interface Publisher {
  /** Resolves with the log id for a log target, undefined for a queue. */
  publish(name: string, data: unknown, options?: { headers?: MessageHeaders; key?: string }): Promise<string | undefined>;
  readonly events: Array<string>;
}

/** Publishes declared `emits` events; each name must be declared unless `strict: false`. */
export declare function createPublisher(
  server: Server | RpcServer,
  broker: Broker | { name?: string; close(): unknown; log?: BrokerLog; queue?: BrokerQueue },
  table: Record<string, PublishedEvent>,
  options?: { strict?: boolean },
): Publisher;

// ---------------------------------------------------------------------------
// RPC over a broker

export interface BrokerRpcOptions {
  /** Served at `wrpc.<service>` unless `address` names the address outright. */
  service?: string;
  address?: string;
  /** A session silent this long is ended; keep above the client heartbeat. Default 90 000. */
  idleTimeout?: number;
  /** Unconfirmed frames per session before write() reports backpressure. Default 1024. */
  highWaterMark?: number;
  /** false serves stateless requests only. Default true. */
  sessions?: boolean;
  logger?: WrpcLogger | boolean | null;
  /**
   * Per-message compression on the binding (src/compression), off by
   * default. A session's frames both ways once the client named the same
   * codec on `hello` (answered on `welcome`); a stateless answer when the
   * request named it — the request itself always travels plain. Node↔Node,
   * so the codec must answer synchronously (the platform one does).
   */
  compression?: boolean | import('./client.js').CompressionOptions;
  /** The largest inflated frame accepted (default 16 MiB); past it the session ends. */
  maxMessage?: number;
}

export interface BrokerRpcHandle {
  /** The shared service address every instance consumes as a group. */
  readonly address: string;
  /** This instance's own inbox, where its sessions' frames arrive. */
  readonly inbox: string;
  readonly sessions: number;
  readonly healthy: boolean;
  /** Also triggered by the server's close(); draining stops taking new work. */
  stop(): Promise<void>;
}

/**
 * Serves wrpc over a broker's `direct` capability: stateless requests
 * (answered like packet-mode HTTP by any instance) and full-protocol
 * sessions (one instance each). Clients connect with
 * `connect('broker://<service>', { transport: 'broker', broker, mode })`.
 */
export declare function attachBrokerRpc(
  server: Server | RpcServer,
  broker: Broker | BrokerDirect,
  options: BrokerRpcOptions,
): Promise<BrokerRpcHandle>;

/** The client transport registered as `WrpcClient.transport.broker`. */
export declare class ClientBrokerTransport {
  constructor(url: string);
  readonly url: string;
  active: boolean;
  persistent: boolean;
  heartbeat: boolean;
  mode: 'stateless' | 'session';
  /**
   * Mints the session id and per-request correlation ids. Assigned by the
   * owning `WrpcClient` from its own `generateId` option, the way `codec`
   * and `log` are; set it yourself only when driving this transport
   * standalone. The session id is the key the server holds this
   * connection's frame state under.
   */
  generateId: () => string;
  open(options?: {
    broker: Broker | BrokerDirect;
    mode?: 'stateless' | 'session';
    address?: string;
    requestTimeout?: number;
    headers?: Record<string, string>;
    meta?: Record<string, unknown>;
    /** Per-message compression, off by default; on a session only once the server agreed on `welcome`. */
    compression?: boolean | import('./client.js').CompressionOptions;
    /** The largest inflated frame accepted (default 16 MiB). */
    maxMessage?: number;
  }): Promise<void>;
  /** The compression codec id in effect on the session — both ends named it — or null. */
  readonly compression: string | null;
  write(data: string | Uint8Array): boolean | void;
  close(): void;
  terminate(): void;
  on(event: string, listener: (...args: Array<any>) => void): unknown;
}

// ---------------------------------------------------------------------------
// Adapter building blocks

export interface TailEntry {
  value: string;
  headers: MessageHeaders;
  [position: string]: unknown;
}

export interface TopicTailsOptions<Cursor, Entry extends TailEntry> {
  /** Starts one tail; resolves with the tip cursor once positioned. */
  live(topic: string, options: { signal: AbortSignal; onEntry(entry: Entry): void }): Promise<Cursor | null>;
  /** Entries strictly after `after` (null: the oldest retained), at most `limit`. */
  range(topic: string, options: { after: Cursor | null; limit: number }): Promise<Array<Entry>>;
  covered(cursor: Cursor, entry: Entry): boolean;
  advance(cursor: Cursor | null, entry: Entry): Cursor;
  highWaterMark?: number;
  page?: number;
}

/** One live reader per topic, shared by every local read; see src/broker/tail.js. */
export declare class TopicTails<Cursor = string, Entry extends TailEntry = TailEntry> {
  constructor(options: TopicTailsOptions<Cursor, Entry>);
  readonly size: number;
  read(
    topic: string,
    options?: { after?: Cursor | null; from?: 'latest' | 'earliest'; signal?: AbortSignal | null },
  ): AsyncIterable<{ id: Cursor; value: string; headers: MessageHeaders }> & { readonly ready: Promise<void> };
  close(): void;
}

export interface EncodeTokenOptions {
  /** Tests ONE character; everything else is escaped. Default /[A-Za-z0-9_-]/. */
  safe?: RegExp;
  /** Default '~'. */
  escape?: string;
  /** Longer tokens are shortened with a SHA-256 digest. Default 200. */
  maxLength?: number;
}

/** Maps an arbitrary name into one broker-safe token, injectively. */
export declare function encodeToken(name: string, options?: EncodeTokenOptions): string;
export declare function toText(body: unknown): string;
export declare function toBytes(body: unknown): Uint8Array;
export declare function toHeaders(value: unknown): MessageHeaders;

export interface RetryPolicy {
  attempts: number;
  backoff: { base: number; max: number; factor: number; jitter: boolean };
  retryOn: ReadonlyArray<number>;
}

export declare const DEFAULT_RETRY: Readonly<RetryPolicy>;
