import {
  IncomingMessage,
  Server as HttpServer,
  ServerResponse,
} from 'node:http';
import { Writable } from 'node:stream';
import type { Connection } from './ws.js';
import type { Engine, EngineConnectionSource, WrpcSocket, EngineAttachOptions } from './engine.js';
import type { Backplane } from './scaling.js';

export declare class Emitter {
  constructor(options?: { maxListeners?: number });
  emit(eventName: PropertyKey, value?: unknown): Promise<void>;
  on(eventName: PropertyKey, listener: (value: any) => void): void;
  once(eventName: PropertyKey, listener: (value: any) => void): void;
  off(eventName: PropertyKey, listener?: (value: any) => void): void;
  clear(eventName?: PropertyKey): void;
  listeners(eventName: PropertyKey): Array<(value: any) => void>;
  listenerCount(eventName: PropertyKey): number;
  eventNames(): Array<PropertyKey>;
}

export class WrpcError extends Error {
  code: number;
  constructor(options: { message: string; code: number });
}

export interface ReadableOptions {
  highWaterMark?: number;
}

export class WrpcReadable extends Emitter {
  id: string;
  name: string;
  size: number;
  queue: Array<ArrayBufferView>;
  streaming: boolean;
  status: string;
  bytesRead: number;
  highWaterMark: number;
  constructor(
    id: string,
    name: string,
    size: number,
    options?: ReadableOptions,
  );
  push(data: ArrayBufferView): Promise<ArrayBufferView>;
  finalize(writable: Writable): Promise<void>;
  pipe(writable: Writable): Writable;
  toBlob(type?: string): Promise<Blob>;
  close(): Promise<void>;
  terminate(): Promise<void>;
  stop(force?: boolean): Promise<void>;
  read(): Promise<ArrayBufferView | null>;
  pull(): ArrayBufferView | undefined;
  checkStreamLimits(): void;
  waitEvent(event: PropertyKey): Promise<unknown>;
  [Symbol.asyncIterator](): AsyncIterableIterator<ArrayBufferView>;
}

export interface Transport {
  send(obj: object): void;
  write(data: string | ArrayBufferView): void;
}

export class WrpcWritable extends Emitter {
  id: string;
  name: string;
  size: number;
  transport: Transport;
  /** True once the transport closed: write() reports false, no 'drain' follows. */
  readonly closed: boolean;
  constructor(id: string, name: string, size: number, transport: Transport);
  init(): void;
  write(data: ArrayBufferView): boolean;
  end(): void;
  terminate(): void;
}

export interface BlobUploader {
  id: string;
  upload(): Promise<void>;
}

declare class ClientTransport extends Emitter {
  url: string;
  active: boolean;
  constructor(url: string);
  open(options?: WrpcClientOptions): Promise<void>;
  close(): void;
  send(obj: object): void;
  write(data: string | ArrayBufferView): void;
  online(): void;
  offline(): void;
}
export type { ClientTransport };

export class WrpcClient extends Emitter {
  static connections: Set<WrpcClient>;
  static isOnline: boolean;
  static online(): void;
  static offline(): void;
  static initialize(): void;
  static connect(
    url: string,
    options?: WrpcClientOptions,
  ): Promise<WrpcClient>;
  static transport: {
    ws: new (url: string) => ClientTransport;
    http: new (url: string) => ClientTransport;
    event: {
      getInstance(url: string): ClientTransport;
    };
  };

  url: string;
  /**
   * Loaded units. A method is a function for a call and a
   * {@link SubscriptionMethod} for a subscription — a subscription answers
   * with a stream, so it is not callable.
   */
  api: Record<string, Emitter & Record<string, any>>;
  readonly active: boolean;

  constructor(
    url: string,
    transport: ClientTransport,
    options?: WrpcClientOptions,
  );
  /** Reconnect attempts made since the last successful open. */
  readonly attempt: number;

  open(): Promise<void>;
  close(): void;
  load(...units: Array<string>): Promise<void>;
  getStream(id: string): WrpcReadable | WrpcWritable;
  createStream(name: string, size: number): WrpcWritable;
  createBlobUploader(blob: Blob): BlobUploader;
  send(obj: object): void;
  /** Fire-and-forget event to the server; `name` is 'unit/event'. */
  sendEvent(name: string, data?: unknown): void;
  /** Sends whatever calls are waiting to be batched. Safe to call anytime. */
  flush(): void;
  write(data: string | ArrayBufferView): void;
}

/**
 * Truncated exponential backoff with full jitter:
 * `delay = random(0, min(maxDelay, minDelay * factor ** attempt))`.
 */
export interface ReconnectOptions {
  /** First retry window in ms; default 2000. */
  minDelay?: number;
  /** Cap in ms; default 30000. */
  maxDelay?: number;
  /** Window growth per attempt; default 2. */
  factor?: number;
  /** Spread the delay over the whole window; default true. */
  jitter?: boolean;
  /** Attempts before giving up and emitting 'reconnect-failed'. */
  retries?: number;
}

/**
 * App-level heartbeat: `{ type: 'ping' }` out, `{ type: 'pong' }` back.
 * A browser WebSocket exposes no protocol ping, so this is the only way to
 * notice a connection that died without a close frame. WebSocket transport
 * only; `false` disables it.
 */
export interface HeartbeatOptions {
  /** Milliseconds between pings; default 30000. */
  interval?: number;
  /** Milliseconds to wait for the pong before reconnecting; default 10000. */
  timeout?: number;
}

/**
 * Call batching: several calls issued in the same tick travel as ONE frame
 * (a JSON array). Only `call` packets batch — a ping, a cancel or an
 * unsubscribe is a control packet whose whole point is to arrive now.
 */
export interface BatchOptions {
  /** 'microtask' (default) or a delay in ms. */
  flush?: 'microtask' | number;
  /** Packets per frame before an early flush; default 16. */
  maxSize?: number;
  /** Bytes per frame before an early flush; default 65536. */
  maxBytes?: number;
}

export interface CallOptions {
  /** Aborting sends `{type:'cancel'}` and rejects with code 499. */
  signal?: AbortSignal;
}

/** A live subscription, as seen by the caller that opened it. */
export interface Subscription {
  readonly id: string;
  /** The last tracked eventId seen; what a reconnect resumes from. */
  readonly lastEventId: string | undefined;
  readonly closed: boolean;
  unsubscribe(): boolean;
}

export interface SubscribeOptions {
  /** Where to resume from; the server decides what that means. */
  lastEventId?: string;
  onData?(data: any): void;
  onError?(error: WrpcError): void;
  onEnd?(): void;
}

export interface IterateOptions extends SubscribeOptions {
  /** Aborting unsubscribes and ends the iterator. */
  signal?: AbortSignal;
  highWaterMark?: number;
}

/** What `client.api[unit][method]` is when the method is a subscription. */
export interface SubscriptionMethod {
  readonly kind: 'subscription';
  subscribe(args?: object, options?: SubscribeOptions): Subscription;
  iterate(args?: object, options?: IterateOptions): AsyncIterableIterator<any> & { subscription: Subscription };
}

export interface WrpcClientOptions {
  callTimeout?: number;
  /** Coalesce calls into batch frames; `true` takes the defaults. */
  batch?: BatchOptions | boolean;
  /**
   * Which registered transport to use. Defaults to the URL scheme; 'sse'
   * exists once '@alexify/wrpc/sse' has been required.
   */
  transport?: 'ws' | 'http' | 'sse' | string;
  reconnect?: ReconnectOptions | false;
  /** Shorthand for `reconnect.minDelay`. */
  reconnectTimeout?: number;
  heartbeat?: HeartbeatOptions | false;
  worker?: ServiceWorker;
  /** Jitter source; injectable so tests can pin the backoff schedule. */
  random?: () => number;
  proxy?: (data: string, packet: object | null) => void;
}

export class WrpcClientProxy extends Emitter {
  constructor(options?: WrpcClientOptions);
  open(): Promise<void>;
  close(): void;
}

// ---------------------------------------------------------------------------
// Router / procedures

export type EventName = PropertyKey;

export interface State {
  [key: string]: unknown;
}

/**
 * A validator is a plain `(value) => value | throws` function (returning
 * undefined keeps the original value) or a Standard Schema object.
 */
export type Validator<T = unknown> =
  | ((value: T) => T | undefined | Promise<T | undefined>)
  | { '~standard': { validate(value: unknown): unknown } };

/**
 * One handler signature for both kinds, so a bare function still gets its
 * parameters contextually typed. A call ignores the third argument; a
 * subscription (an async generator) reads `lastEventId` and `signal` from it.
 */
export type ProcedureHandler = (
  context: Context,
  args: any,
  subscription: SubscriptionOptions,
) => unknown | Promise<unknown> | AsyncIterable<unknown>;

/** A handler that answers with a stream of values. */
export type SubscriptionHandler = ProcedureHandler;

export interface QueueOptions {
  concurrency: number;
  size?: number;
  timeout?: number;
}

// ---------------------------------------------------------------------------
// Subscriptions

/** A value labelled with the id a resuming client will send back. */
export interface Tracked<T = unknown> {
  id: string;
  data: T;
}

/** Labels one yielded value so a reconnect can resume after it. */
export declare function tracked<T>(eventId: string | number, data: T): Tracked<T>;

export declare function isTracked(value: unknown): boolean;

/**
 * A bounded replay buffer. `since()` answers with what a client missed —
 * or `null` when the id has fallen out of the buffer, so a caller can
 * choose between a snapshot and an error instead of silently skipping a gap.
 */
export declare class EventLog<T = unknown> {
  constructor(options?: { size?: number; start?: number });
  readonly size: number;
  readonly length: number;
  readonly lastEventId: string | null;
  push(data: T): string;
  since(lastEventId?: string | null): Array<Tracked<T>> | null;
  clear(): void;
}

export declare function createEventLog<T = unknown>(options?: { size?: number; start?: number }): EventLog<T>;

/**
 * Push -> pull adapter: the bridge between "something calls me with a
 * value" and "someone is `for await`-ing values". The queue is bounded —
 * a producer that outruns the consumer drops the OLDEST value and says so
 * through `dropped`.
 */
export declare class EventStream<T = unknown> implements AsyncIterableIterator<T> {
  constructor(options?: { signal?: AbortSignal; highWaterMark?: number });
  readonly length: number;
  readonly closed: boolean;
  dropped: number;
  push(value: T): boolean;
  end(): void;
  fail(error: Error): void;
  next(): Promise<IteratorResult<T>>;
  return(): Promise<IteratorResult<T>>;
  [Symbol.asyncIterator](): AsyncIterableIterator<T>;
}

export declare function createEventStream<T = unknown>(options?: {
  signal?: AbortSignal;
  highWaterMark?: number;
}): EventStream<T>;

/** The third argument every subscription handler receives. */
export interface SubscriptionOptions {
  /** What the client says it last saw; undefined on a fresh subscribe. */
  lastEventId?: string;
  /** Aborted on unsubscribe or disconnect. Honour it, or the feed leaks. */
  signal: AbortSignal;
}


export interface ProcedureOptions {
  handler: ProcedureHandler;
  access?: 'public' | 'session' | string;
  input?: Validator;
  output?: Validator;
  /** Milliseconds; the call fails with code 408 when exceeded. */
  timeout?: number;
  /** Concurrency limit; overflow/starvation fails with code 503. */
  queue?: QueueOptions;
  meta?: Record<string, unknown>;
  /** Flat descriptor consumed by the type-codegen CLI. */
  signature?: Record<string, unknown>;
  /** Inferred from an async generator handler; rarely written by hand. */
  kind?: 'call' | 'subscription';
}

/** Same as ProcedureOptions minus the two a stream cannot mean. */
export type SubscriptionProcedureOptions = Omit<ProcedureOptions, 'queue' | 'timeout'>;

export declare class Procedure {
  handler: ProcedureHandler;
  access: string;
  input: Validator | null;
  output: Validator | null;
  timeout: number;
  meta: Record<string, unknown>;
  signature: Record<string, unknown> | null;
  kind: 'call' | 'subscription';
  readonly subscription: boolean;
  constructor(options: ProcedureOptions);
  invoke(context: Context, args: unknown): Promise<unknown>;
  /** The value stream behind `{type:'subscribe'}`. */
  subscribe(context: Context, args: unknown, options?: Partial<SubscriptionOptions>): AsyncIterableIterator<unknown>;
}

export interface ProcedureFactory {
  (options: ProcedureOptions | ProcedureHandler): Procedure;
  /**
   * Explicit spelling for a subscription — redundant when the handler is an
   * async generator (which is detected), required when it is a plain
   * function returning an async iterable.
   */
  subscription(options: SubscriptionProcedureOptions | SubscriptionHandler): Procedure;
}

export declare const procedure: ProcedureFactory;

export type MethodDefinition = Procedure | ProcedureHandler | ProcedureOptions;

/**
 * Inbound (client -> server) event handlers. An event is a call that never
 * answers, so handlers are procedures too: access, input validation and
 * queueing all work the same way.
 */
export type EventsDefinition = Record<string, MethodDefinition>;

/**
 * A unit's methods, plus the reserved `on` key holding its event handlers.
 * `on` is therefore NOT usable as a method name.
 */
export interface UnitDefinition {
  on?: EventsDefinition;
  [method: string]: MethodDefinition | EventsDefinition | undefined;
}

/**
 * Unit keys are 'unit' or 'unit.version'; method values are procedures,
 * bare handler functions, or procedure option objects.
 */
export type RouterDefinition = Record<string, UnitDefinition>;

export interface MethodInfo {
  access: string;
  /** Present only on subscriptions; a client scaffolds a call otherwise. */
  kind?: 'subscription';
  meta?: Record<string, unknown>;
  signature?: Record<string, unknown>;
}

export declare class Router {
  constructor(definition?: RouterDefinition);
  getProcedure(
    unit: string,
    version: string | undefined,
    method: string,
  ): Procedure | null;
  /** Handler for an inbound `{ type: 'event' }` packet, if the unit declares one. */
  getEventHandler(
    unit: string,
    version: string | undefined,
    name: string,
  ): Procedure | null;
  introspect(units?: Array<string> | null): Record<string, Record<string, MethodInfo>>;
  /** Returns a NEW router; on collision the other router's procedure wins. */
  merge(other: Router): Router;
}

export declare function defineRouter(definition: RouterDefinition): Router;

// ---------------------------------------------------------------------------
// Sessions

/** Structural store contract — anything with this shape plugs in. */
export interface SessionStore {
  get(token: string): Promise<State | null>;
  set(token: string, data: State): Promise<void>;
  delete(token: string): Promise<void>;
}

export interface MemorySessionStoreOptions {
  /** LRU cap; 0 disables it. Default 10000. */
  maxSessions?: number;
  /** Entry lifetime in ms; 0 disables expiry. Default 24h. */
  ttl?: number;
  now?: () => number;
}

/**
 * Bounded in-memory store (LRU + TTL). Sessions outlive their connection,
 * so production deployments should inject a real store instead.
 */
export declare class MemorySessionStore implements SessionStore {
  constructor(options?: MemorySessionStoreOptions);
  readonly size: number;
  get(token: string): Promise<State | null>;
  set(token: string, data: State): Promise<void>;
  delete(token: string): Promise<void>;
}

export declare class Session {
  token: string;
  state: State;
  constructor(token: string, data: State, save?: (data: State) => void);
}

export interface CookieOptions {
  name?: string;
  path?: string;
  httpOnly?: boolean;
  secure?: boolean;
  sameSite?: 'Strict' | 'Lax' | 'None';
  maxAge?: number | null;
}

export interface SessionsOptions {
  store?: SessionStore;
  cookie?: CookieOptions;
  generateToken?: () => string;
}

declare class SessionManager {
  store: SessionStore;
  generateToken: () => string;
  cookie: CookieOptions & { name: string; path: string };
  create(token?: string, data?: State): Session;
  restore(token: string): Promise<Session | null>;
  destroy(token: string): Promise<void>;
  cookieHeader(token: string): string;
  cookieDeleteHeader(): string;
  readToken(cookies: Record<string, string>): string | null;
}
export type { SessionManager };

export function createProxy<T extends object>(
  data: T,
  save?: (data: T) => void,
): T;

// ---------------------------------------------------------------------------
// Rooms

/**
 * Named groups of clients, on top of the ordinary `{ type: 'event' }`
 * packets. The registry owns both directions — which clients a room holds
 * and which rooms a client joined — so a disconnect only has to call
 * `leaveAll`.
 */
export declare class RoomRegistry {
  constructor(options?: {
    /** Fires when a room gains its first member (backplane subscribe). */
    onSubscribe?: (room: string) => void;
    /** Fires when a room loses its last member (backplane unsubscribe). */
    onUnsubscribe?: (room: string) => void;
  });
  /** Number of non-empty rooms. */
  readonly size: number;
  list(): Array<string>;
  members(room: string): Set<Client>;
  count(room: string): number;
  has(room: string): boolean;
  roomsOf(client: Client): Set<string>;
  /** True when the client was not already a member. */
  join(client: Client, room: string): boolean;
  leave(client: Client, room: string): boolean;
  leaveAll(client: Client): void;
  clear(): void;
}

/**
 * An immutable, chainable delivery target: every modifier returns a NEW
 * Broadcast, so a stored `server.to('chat')` cannot be mutated by a later
 * `.except()` elsewhere.
 */
export declare class Broadcast {
  /**
   * Union, not intersection: `to('a').to('b')` reaches either room.
   * `to()` with no rooms narrows to NOBODY — a computed room list that came
   * back empty must not fall back to every connected client.
   */
  to(...rooms: Array<string>): Broadcast;
  except(...clients: Array<Client>): Broadcast;
  /** Suppresses the backplane publish; the event stays on this instance. */
  local(): Broadcast;
  /** The targeted rooms, or null when the target is every client. */
  readonly rooms: Array<string> | null;
  /**
   * Sends `{ type: 'event', name, data }` and returns how many clients
   * received it LOCALLY — remote instances are reached through the
   * backplane, whose delivery this number says nothing about.
   */
  emit(name: string, data?: unknown): number;
}

// ---------------------------------------------------------------------------
// Server core

export interface CorsOptions {
  origins?: Array<string> | ((origin: string) => boolean);
  credentials?: boolean;
  headers?: string;
  methods?: string;
}

export interface ErrorOptions {
  id?: string;
  error?: Error;
}

export declare class Context {
  client: Client;
  uuid: string;
  state: Record<string, unknown>;
  /**
   * Aborted when the caller cancels, unsubscribes, or disconnects. A handler
   * that awaits anything long-lived should pass it along; one that ignores
   * it runs to completion and has its result dropped.
   */
  readonly signal: AbortSignal | null;
  readonly session: Session | null;
  /**
   * The server this call arrived on — how a handler reaches rooms
   * (`context.server.to(room).emit(...)`) without closing over a server
   * that could not exist before the router it was built from.
   */
  readonly server: RpcServer | null;
  constructor(client: Client);
}

export class Client extends Emitter {
  source: string;
  session: Session | null;
  /** True for transports that stay open (WebSocket, worker port). */
  readonly persistent: boolean;
  /** The RpcServer this client belongs to; null for a standalone Client. */
  readonly server: RpcServer | null;
  /** Settles once the cookie-based session restore (if any) finished. */
  sessionReady: Promise<unknown>;
  streams: Map<string, WrpcReadable | WrpcWritable>;
  /** In-flight calls, by id — what `{type:'cancel'}` reaches. */
  calls: Map<string, AbortController>;
  /** Live subscriptions, by id — what `{type:'unsubscribe'}` reaches. */
  subscriptions: Map<string, AbortController>;
  maxSubscriptions: number;
  /** False on a text-only transport (SSE), where binary streams cannot go. */
  readonly binary: boolean;
  /** Resolves when the transport drained, or when it closed. */
  drain(): Promise<void>;
  error(code: number, options?: ErrorOptions): void;
  /** Returns false when the transport is above its high-water mark. */
  send(obj: object, options?: { code?: number; method?: string }): boolean;
  createContext(signal?: AbortSignal | null): Context;
  emit(name: EventName, data?: unknown): Promise<void>;
  sendEvent(name: string, data?: unknown): void;
  /** Diagnostics for inbound packets with no id to answer on. */
  warn(message: string): void;
  /** Joins a room; false when already a member. */
  join(room: string): boolean;
  leave(room: string): boolean;
  in(room: string): boolean;
  /** The rooms this client is in — a copy, safe to iterate while leaving. */
  readonly rooms: Set<string>;
  getStream(id: string): WrpcReadable | WrpcWritable;
  createStream(name: string, size: number): WrpcWritable;
  initializeSession(token?: string, data?: State): boolean;
  finalizeSession(): Promise<boolean>;
  startSession(token?: string, data?: State): boolean;
  restoreSession(token: string): Promise<boolean>;
  close(): void;
  destroy(): void;
}

/** Abstract HTTP call description consumed by RpcServer.handleHttpCall. */
export interface HttpCall {
  method: string;
  url: string;
  headers: Record<string, string | undefined>;
  body?: string | Buffer | null;
  remoteAddress?: string;
  respond(response: {
    status: number;
    headers: Record<string, string | number | Array<string>>;
    body?: Buffer;
  }): void;
  /**
   * Registers a listener for a request that ends without a response, so
   * the core can evict its client. Adapters should wire it to their
   * request/response close signal.
   */
  onAbort?(listener: () => void): void;
  /**
   * Keeps the response open and writes into it, instead of answering with a
   * body. This is what SSE rides on; a host that cannot stream simply omits
   * it and the events endpoint answers 501.
   */
  stream?(response: {
    status: number;
    headers: Record<string, string | number | Array<string>>;
  }): import('./sse.js').SseWriter | null;
}

export interface RpcServerOptions {
  router: Router;
  sessions?: SessionsOptions;
  cors?: CorsOptions | null;
  /** Default '/api'; '' serves from the root. */
  basePath?: string;
  console?: Console;
  /**
   * Carries room events between instances. Optional: without one, rooms
   * work identically inside a single instance.
   */
  backplane?: Backplane | null;
  /** Identifies this instance on the backplane; a uuid by default. */
  instanceId?: string;
  /** Packets accepted in one batch frame; default 128. */
  maxBatch?: number;
  /** Concurrent subscriptions per client; default 256. */
  maxSubscriptions?: number;
  /** SSE channel options, or `false` to remove the events endpoint. */
  sse?: import('./sse.js').SseOptions | false;
}

export declare class RpcServer extends Emitter {
  readonly router: Router;
  readonly sessions: SessionManager;
  readonly rooms: RoomRegistry;
  readonly instanceId: string;
  readonly basePath: string;
  /** Where the SSE stream lives: `${basePath}/events`. */
  readonly eventsPath: string;
  /** The SSE channel registry, or null when `sse: false`. */
  readonly sse: import('./sse.js').SseChannels | null;
  readonly clients: Set<Client>;
  constructor(options: RpcServerOptions);
  /** Everyone in any of `rooms`, each client once; with no rooms, nobody. */
  to(...rooms: Array<string>): Broadcast;
  /** Everyone connected, minus `clients`. */
  except(...clients: Array<Client>): Broadcast;
  /** Everyone connected; returns the number of LOCAL recipients. */
  broadcast(name: string, data?: unknown): number;
  attachSocket(
    socket: WrpcSocket | Connection,
    meta?: { headers?: Record<string, string | undefined>; remoteAddress?: string },
  ): Client;
  attachPort(port: MessagePort): Client;
  handleHttpCall(call: HttpCall): Promise<void>;
  matchPath(pathname: string): { mode: 'packet' | 'rest'; rest?: string } | null;
  close(): Promise<void>;
}

export interface ServerOptions extends RpcServerOptions {
  host?: string;
  port?: number;
  protocol?: string;
  nagle?: boolean;
  key?: string;
  cert?: string;
  SNICallback?: Function;
  timeouts?: { bind?: number };
  retry?: number;
  engine?: Engine;
  /** Options forwarded to the engine's attach() (path, protocols, ...). */
  ws?: Omit<EngineAttachOptions, 'server'>;
}

export class Server extends Emitter {
  /** Null with a standalone engine, which owns the network stack itself. */
  httpServer: HttpServer | null;
  wsServer: EngineConnectionSource | null;
  rpc: RpcServer;
  constructor(options: ServerOptions);
  /** The bound address, whichever side owns the listener. */
  address(): { address: string; family: string; port: number } | string | null;
  /** Rooms, forwarded to the core. */
  readonly rooms: RoomRegistry;
  readonly clients: Set<Client>;
  to(...rooms: Array<string>): Broadcast;
  except(...clients: Array<Client>): Broadcast;
  broadcast(name: string, data?: unknown): number;
  listen(): Promise<Server>;
  close(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Transports

export interface TransportOptions {
  headers?: Record<string, string>;
}

export class ServerTransport extends Emitter {
  static transport: {
    http: typeof ServerHttpTransport;
    ws: typeof ServerWsTransport;
    event: typeof ServerEventTransport;
  };
  source: string;
  constructor(source: string);
  error(code?: number, options?: ErrorOptions): boolean;
  /** Returns the transport's backpressure signal (false = above the mark). */
  send(obj: object, code?: number): boolean;
}

declare class ServerHttpTransport extends ServerTransport {
  call: HttpCall;
  headers: Record<string, string>;
  readonly responded: boolean;
  constructor(call: HttpCall, options?: TransportOptions);
  write(data: string | Buffer, httpCode?: number): boolean;
  /** True when this transport collects a batch frame's answers. */
  readonly batched: boolean;
  getCookies(): Record<string, string>;
  sendSessionCookie(cookieHeader: string): void;
  close(): void;
}
export type { ServerHttpTransport };

declare class ServerWsTransport extends ServerTransport {
  connection: WrpcSocket | Connection;
  constructor(
    connection: WrpcSocket | Connection,
    meta?: { remoteAddress?: string },
  );
  write(data: string | Buffer): boolean;
  close(): void;
}
export type { ServerWsTransport };

declare class ServerEventTransport extends ServerTransport {
  port: MessagePort;
  constructor(port: MessagePort);
  write(data: string | Buffer): void;
  close(): void;
}
export type { ServerEventTransport };

/** Per-request response headers: security defaults + CORS for `origin`. */
export function buildHeaders(
  cors?: CorsOptions | null,
  origin?: string,
): Record<string, string>;

export interface CallPacket {
  type: 'call';
  id: string;
  method: string;
  args: object;
}

export interface StreamPacket {
  type: 'stream';
  id: string;
  name?: string;
  size?: number;
  status?: 'end' | 'terminate';
}

/** Fire-and-forget, both directions; `name` is 'unit/event'. */
export interface EventPacket {
  type: 'event';
  name: string;
  data?: unknown;
}

export interface SubscribePacket {
  type: 'subscribe';
  id: string;
  method: string;
  args?: object;
  lastEventId?: string;
}

/** One value of a subscription; `eventId` only for tracked values. */
export interface DataPacket {
  type: 'data';
  id: string;
  eventId?: string;
  data?: unknown;
}

/** A subscription's terminal packet — refusals included. */
export interface EndPacket {
  type: 'end';
  id: string;
  error?: { message: string; code: number };
}

export interface UnsubscribePacket {
  type: 'unsubscribe';
  id: string;
}

export interface CancelPacket {
  type: 'cancel';
  id: string;
}

/** A JSON array of packets: several requests, or several answers, in one frame. */
export type BatchFrame = Array<
  CallPacket | SubscribePacket | UnsubscribePacket | CancelPacket | StreamPacket | EventPacket
>;

/** App-level heartbeat; see HeartbeatOptions. */
export interface HeartbeatPacket {
  type: 'ping' | 'pong';
}

export function chunkEncode(id: string, payload: Uint8Array): Uint8Array;
export function chunkDecode(chunk: Uint8Array): {
  id: string;
  payload: Uint8Array;
};
