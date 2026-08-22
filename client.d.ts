// The browser-safe half of the hand-maintained type surface: everything a
// client bundle can reach, and NOTHING that needs node types — a browser TS
// project without @types/node compiles against this file. index.d.ts
// re-exports all of it and adds the server surface on top; browser.d.ts
// (the `browser` types condition) re-exports ONLY this.

/**
 * Structural view of a node Writable — what finalize()/pipe() need. Typed
 * structurally so this file stays free of node imports: a browser TS
 * project without @types/node must compile against it.
 */
export interface WritableLike {
  write(chunk: unknown, callback?: (error?: Error | null) => void): boolean;
  end(): void;
  on(event: string, listener: (...args: Array<any>) => void): unknown;
  once(event: string, listener: (...args: Array<any>) => void): unknown;
}

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

/** Structural check for an injected wire codec (shared with the server). */
export declare function isCodec(value: unknown): boolean;

export class WrpcError extends Error {
  code: number;
  /** Structured issue lists the server attached; an optional wire field. */
  details?: unknown;
  constructor(options: { message: string; code: number; details?: unknown });
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
  finalize(writable: WritableLike): Promise<void>;
  pipe<T extends WritableLike>(writable: T): T;
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

/**
 * One method's introspection descriptor — the raw `system/introspect` value
 * shape `use()` consumes. A structural mirror of the server-side MethodInfo
 * (index.d.ts); deliberately not imported across the two hand-maintained
 * declaration files.
 */
export interface WrpcMethodInfo {
  access: string;
  /** Present only on subscriptions; a client scaffolds a call otherwise. */
  kind?: 'subscription';
  meta?: Record<string, unknown>;
  signature?: Record<string, unknown>;
  /** The declarative REST mapping, when the procedure carries one. */
  http?: { method: string; path: string; status?: number };
  /** Input schema parts (params/querystring/body) for client pre-validation. */
  schema?: { params?: object; querystring?: object; body?: object };
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

export class WrpcClient<Api = UntypedApi> extends Emitter {
  static connections: Set<WrpcClient>;
  static isOnline: boolean;
  static online(): void;
  static offline(): void;
  static initialize(): void;
  /**
   * With a contract — `WrpcClient.connect<Api>(url)` — the whole `api` is
   * typed. Without one it behaves exactly as it always did. See
   * {@link connect}, which is the same call under a name that reads better
   * with a type argument.
   */
  static connect<Api = UntypedApi>(
    url: string,
    options?: WrpcClientOptions,
  ): Promise<WrpcClient<Api>>;
  static transport: {
    ws: new (url: string) => ClientTransport;
    http: new (url: string) => ClientTransport;
    event: {
      getInstance(url: string): ClientTransport;
    };
    /** Late registration — how the sse subpath (and tests) add transports. */
    [name: string]: unknown;
  };

  url: string;
  /**
   * The loaded units. A method is a function for a call and a
   * {@link SubscriptionMethod} for a subscription — a subscription answers
   * with a stream, so it is not callable.
   *
   * With a contract type this is {@link TypedApi}; note that it types what
   * the contract DECLARES, not what has been `load()`ed yet — a unit read
   * before its `load()` is `undefined` at runtime.
   */
  api: TypedApi<Api>;
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
  load(...units: Array<Extract<keyof Api, string>>): Promise<void>;
  /**
   * One call by wire target ('unit/name', 'unit.vN/name'), no scaffolding —
   * the escape hatch for a method whose unit has not been load()ed, which is
   * every method inside an `authenticate` hook on a first connect. No
   * client-side pre-validation, no REST leg. Deliberately untyped: `api` is
   * the typed surface.
   */
  call(method: string, args?: unknown, options?: CallOptions): Promise<unknown>;
  /**
   * Static introspection: scaffolds units from an introspection artifact
   * (the raw `system/introspect` shape, e.g. generated by
   * `wrpc types --schema`) with no wire traffic. Synchronous and usable
   * before open(). A unit already load()ed is skipped (dynamic wins);
   * static units are never re-introspected on reconnect.
   */
  use(introspection: Record<string, Record<string, WrpcMethodInfo>>): this;
  getStream(id: string): WrpcReadable | WrpcWritable;
  createStream(name: string, size: number): WrpcWritable;
  createBlobUploader(blob: Blob): BlobUploader;
  send(obj: object): void;
  /** Fire-and-forget event to the server; `name` is 'unit/event'. */
  sendEvent(name: string, data?: unknown): void;
  /**
   * Registers the answer this client gives when the server asks `name`
   * ('unit/event') — the receiving half of the server's `client.ask()` and
   * `Broadcast.ask()`. One responder per name: a duplicate registration
   * throws. Client-level rather than per-unit on purpose — unit objects
   * carry server-named methods, where a method called 'respond' would
   * collide.
   */
  respond(name: string, handler: (data: unknown) => unknown): void;
  unrespond(name: string): boolean;
  /** Sends whatever calls are waiting to be batched. Safe to call anytime. */
  flush(): void;
  write(data: string | ArrayBufferView): void;
}

/**
 * Truncated exponential backoff with full jitter:
 * `delay = random(0, min(maxDelay, minDelay * factor ** attempt))`.
 */
/**
 * Runs once for all concurrent refusals; receives the FIRST refusal's error.
 * A throw makes every waiting call surface its own original refusal.
 */
export type RefreshHook = (client: WrpcClient<any>, error: unknown) => unknown;

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
  /**
   * Per-call metadata, carried as the packet's optional `meta` field and
   * surfaced server-side as `context.callMeta`. Deliberately outside schema
   * validation. On a mapped REST leg there is no packet, so it merges over
   * the connection bag and travels as request headers instead — per-call
   * winning a key collision.
   *
   * Handed to the wire VERBATIM: unlike `withMeta`, this escape hatch does
   * not kebab-normalize keys. It is the raw seam the auth hooks write
   * against; prefer `withMeta` unless you need that.
   */
  meta?: Record<string, unknown>;
}

/** A live subscription, as seen by the caller that opened it. */
export interface Subscription {
  readonly id: string;
  /** The last tracked eventId seen; what a reconnect resumes from. */
  readonly lastEventId: string | undefined;
  readonly closed: boolean;
  unsubscribe(): boolean;
}

export interface SubscribeOptions<Data = any> {
  /** Where to resume from; the server decides what that means. */
  lastEventId?: string;
  onData?(data: Data): void;
  /** The subscription died: the handler threw, or the server refused it. */
  onError?(error: WrpcError): void;
  /**
   * The subscription ended cleanly — the handler finished, or
   * {@link WrpcClient.close} took the whole client down under it.
   *
   * Exactly one of `onEnd`/`onError` fires, and only for an ending the caller
   * did not ask for: calling `unsubscribe()` yourself is silent, because you
   * already know. A reconnect is not an ending either — the client re-opens
   * its subscriptions from the last eventId it saw.
   */
  onEnd?(): void;
}

export interface IterateOptions<Data = any> extends SubscribeOptions<Data> {
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

// ---------------------------------------------------------------------------
// Contract-first typed client
//
// The contract is written ONCE, as an ordinary TypeScript interface, and
// threaded through `connect<Api>()`. Nothing is generated and nothing is
// checked at runtime: these types describe the api the server already
// introspects, so they buy autocompletion and a compile error on a typo —
// not a guarantee that the server agrees. (`wrpc types`, the codegen CLI,
// writes exactly this shape of interface FROM a running server.)
//
//   interface Api {
//     chat: {
//       send(args: { text: string }): Promise<{ id: string }>;
//       onMessage: SubscriptionContract<{ room: string }, { text: string }>;
//     };
//     'auth.v1': { signIn(args: { login: string }): Promise<{ token: string }> };
//   }
//
//   const client = await connect<Api>('wss://host');
//   await client.load('chat');
//   const { id } = await client.api.chat.send({ text: 'hi' });

/**
 * Declares a contract member as a subscription rather than a call: it answers
 * with a stream, so on the client it becomes a {@link TypedSubscriptionMethod}
 * (`subscribe`/`iterate`) instead of something callable.
 *
 * Declaration-only — there is no runtime value to construct, and the phantom
 * key exists so a plain object type can never be mistaken for one. (The
 * `~`-prefixed key is the same idiom Standard Schema uses for `~standard`.)
 * Not to be confused with {@link Subscription}, which is the live handle
 * `subscribe()` hands back.
 */
export interface SubscriptionContract<Args = void, Data = unknown> {
  readonly '~wrpc.subscription': (args: Args) => Data;
}

/**
 * No contract given: `api` keeps the loose, runtime-shaped record it has
 * always had, so nothing about an untyped client changes.
 */
export type UntypedApi = Record<string, Record<string, any>>;

/**
 * `any` has to be caught before any conditional type sees it: `any extends X`
 * matches BOTH branches, which would turn an untyped unit into a union of
 * "callable" and "subscription" and make it neither.
 */
export type IsAny<T> = 0 extends 1 & T ? true : false;

/** `[args]` when args are required, `[args?]` when the contract says `void`. */
export type ContractArgs<Args, Rest extends Array<unknown>> = [Args] extends [void]
  ? [args?: Args, ...rest: Rest]
  : [args: Args, ...rest: Rest];

/**
 * What an unusable contract member maps to. A wrpc procedure receives exactly
 * ONE args object, so a member declared with two parameters (or a rest
 * parameter, or as something that is not a function at all) cannot be called:
 * slot 1 on the client is {@link CallOptions}, not a second argument. The text
 * is what the compiler quotes back at the call site.
 */
export type InvalidContractMember = ['wrpc: a contract member is `(args) => Promise<T>` or a SubscriptionContract'];

/**
 * A contract member's parameters, as the client takes them.
 *
 * A zero-argument member KEEPS its args slot: `#scaffold` builds
 * `(args = {}, options = {})`, so slot 0 is always the wire args. Collapsing
 * the tuple would make `ping({ signal })` compile and then ship `{"signal":{}}`
 * to the server as the procedure's arguments, silently dropping the
 * cancellation the caller asked for — `ping(undefined, { signal })` is the
 * spelling that works.
 */
export type TypedParams<Params extends Array<unknown>> = Params extends readonly [] ? [args?: undefined]
  : Params extends readonly [unknown?] ? Params
  : [args: InvalidContractMember];

/** What a subscription member of a contract becomes on the client. */
export interface TypedSubscriptionMethod<Args, Data> {
  readonly kind: 'subscription';
  subscribe(...params: ContractArgs<Args, [options?: SubscribeOptions<Data>]>): Subscription;
  iterate(
    ...params: ContractArgs<Args, [options?: IterateOptions<Data>]>
  ): AsyncIterableIterator<Data> & { subscription: Subscription };
}

/**
 * One contract member, translated. A call keeps its declared parameters and
 * gains the trailing {@link CallOptions} the client accepts, which is what
 * carries `{ signal }`; its result is awaited, since the wire always answers
 * with a promise whether the handler did or not.
 */
export type TypedMethod<T> = IsAny<T> extends true ? any
  : T extends SubscriptionContract<infer Args, infer Data> ? TypedSubscriptionMethod<Args, Data>
  : T extends (...args: infer Params) => infer Result
    ? TypedCallMethod<TypedParams<Params>, Awaited<Result>>
  : InvalidContractMember;

/**
 * A callable contract member: the call itself, plus `withMeta` — the bound
 * variant that stamps every call it makes with per-call metadata. Keys are
 * kebab-normalized once at bind time, so a bound method costs no more per
 * invocation than an unbound one.
 */
export type TypedCallMethod<Params extends Array<unknown>, Result> = {
  (...args: [...Params, options?: CallOptions]): Promise<Result>;
  withMeta(meta: Record<string, unknown>): (...args: [...Params, options?: CallOptions]) => Promise<Result>;
};

/**
 * One contract unit, translated.
 *
 * `on` is dropped: a unit IS an {@link Emitter} at runtime, so `api.chat.on`
 * has to stay the listener registration. Mapping a contract key called `on`
 * would shadow it with an overload — and an *optional* one reduces the whole
 * intersection to `never`, which turns every member access on that unit into
 * an error pointing nowhere.
 */
export type TypedUnit<Unit> = IsAny<Unit> extends true ? Record<string, any>
  : { [Method in keyof Unit as Method extends 'on' ? never : Method]: TypedMethod<Unit[Method]> };

/**
 * The whole contract, translated. Each unit is also an {@link Emitter} — that
 * is where server → client events for the unit arrive.
 */
export type TypedApi<Api> = { [Unit in keyof Api]: Emitter & TypedUnit<Api[Unit]> };

/**
 * The first parameter of a parameter tuple. Projected from the tuple rather
 * than inferred from `(args: infer A) => any`, because a zero-parameter
 * function IS assignable to a one-parameter target — so that inference
 * succeeds with `unknown` where the honest answer is `void`.
 */
export type FirstArg<Params extends Array<unknown>> = Params extends readonly [] ? void
  : Params extends readonly [infer Arg] ? Arg
  : Params extends readonly [(infer Arg)?] ? Arg | undefined
  : Params extends readonly [infer Arg, ...Array<any>] ? Arg
  : void;

/**
 * The argument type of a contract member, call or subscription — declared
 * ({@link SubscriptionContract}) or already mapped
 * ({@link TypedSubscriptionMethod}).
 */
export type InferArgs<T> = T extends SubscriptionContract<infer Args, any> ? Args
  : T extends TypedSubscriptionMethod<infer Args, any> ? Args
  : T extends (...args: infer Params) => any ? FirstArg<Params>
  : void;

/** What a contract member answers with: a call's result, a subscription's value. */
export type InferResult<T> = T extends SubscriptionContract<any, infer Data> ? Data
  : T extends TypedSubscriptionMethod<any, infer Data> ? Data
  : T extends (...args: Array<any>) => infer Result ? Awaited<Result>
  : never;

/**
 * Opens a connection. The same call as {@link WrpcClient.connect}, spelled as
 * a function because that is where the contract type argument reads naturally:
 * `connect<Api>('wss://host')`. Without one, the client is untyped exactly as
 * before.
 */
export declare function connect<Api = UntypedApi>(
  url: string,
  options?: WrpcClientOptions,
): Promise<WrpcClient<Api>>;

export interface WrpcClientOptions {
  callTimeout?: number;
  /** Coalesce calls into batch frames; `true` takes the defaults. */
  batch?: BatchOptions | boolean;
  /**
   * Which registered transport to use. Defaults to the URL scheme; 'sse'
   * exists once '@alexify/wrpc/sse' has been required.
   */
  /**
   * A single registered transport name, or an ORDERED fallback list: each
   * candidate gets its own reconnect budget; when one exhausts, the next
   * takes over ('transport-fallback' fires) and only the last exhausting
   * emits 'reconnect-failed'. No default order — the list is yours.
   */
  transport?: 'ws' | 'http' | 'sse' | string | Array<string>;
  reconnect?: ReconnectOptions | false;
  /**
   * Presents this connection's credential. Awaited inside `open()` on the
   * first connect — so `await connect(url, { authenticate })` returns an
   * already-authenticated client — and again on every reconnect BEFORE the
   * subscriptions are re-opened and the units re-loaded, which is the window
   * an `'open'` listener cannot reach. A throw fails the connection: the
   * transport is terminated, `'authenticate-failed'` fires, and the normal
   * backoff takes over (a first-connect throw rejects `connect()` instead).
   * Call `client.close()` before throwing to stop instead of retrying.
   * Inside the hook, address methods with `client.call('unit/name', args)` —
   * `api` is built by `load()`, which runs after auth.
   */
  authenticate?: (client: WrpcClient<any>, info: { reconnected: boolean; attempts: number }) => unknown;
  /**
   * Single-flight credential refresh for a LIVE connection (`authenticate`
   * heals a NEW one). When a call is refused with a listed code (default
   * `[401]`), the handler runs once for ALL concurrent refusals and each
   * refused call is re-issued exactly once, with a fresh packet id, on both
   * the packet and the REST leg. If the refresh itself fails — or the retry
   * is refused again — the ORIGINAL error surfaces. Never fires for calls
   * made inside the `authenticate` hook. Note: wrpc's own "no session"
   * refusal is 403; list it via `on` to cover it.
   */
  refresh?: RefreshHook | { on?: Array<number>; handler: RefreshHook };
  /**
   * Connection-phase headers, re-evaluated on every open (function form
   * included) so a reconnect presents fresh values. They ride as REAL
   * request headers on http/sse, as one `wrpc_h` query parameter on the
   * browser ws connect URL (the WHATWG WebSocket constructor takes no
   * headers — note the URL lands in proxy access logs), and in the
   * `wrpc:connect` message on the worker transport. The server surfaces
   * them on `client.meta.headers` / `context.meta.headers`, observed
   * headers winning, reserved names (cookie, host, origin, sec-*,
   * content-*, proxy-*, x-wrpc-*) dropped from the query path. Labels,
   * never credentials on the ws leg.
   *
   * Keys are normalized to kebab-case (`xAppVersion` -> `x-app-version`), so
   * one spelling addresses a value whatever carrier brought it — and that is
   * the spelling to write in `schema.headers`. Values are stringified, so a
   * number arrives as a string on every transport rather than being coerced
   * on http and dropped on ws.
   */
  headers?: Record<string, unknown> | (() => Record<string, unknown> | Promise<Record<string, unknown>>);
  /**
   * Connection-phase metadata — headers' unvalidated sibling. Re-evaluated
   * on every open; rides the `x-wrpc-meta` request header on http/sse, the
   * `wrpc_meta` connect-URL parameter on ws, and the `wrpc:connect` message
   * on the worker transport. Lands on `client.meta.data` server-side. Never
   * runs through schema validation — see the per-call twin in CallOptions.
   *
   * Keys are kebab-normalized like `headers`; values keep their JSON types
   * unless `metaFormat` is `'prefixed'`.
   */
  meta?: Record<string, unknown> | (() => Record<string, unknown> | Promise<Record<string, unknown>>);
  /**
   * How the connection-phase `meta` bag is spelled on the wire.
   *
   * `'json'` (default) sends one `x-wrpc-meta` header of percent-encoded
   * JSON: type-faithful, and one stable name on the CORS allowlist.
   *
   * `'prefixed'` sends one `x-wrpc-meta-<key>` header per entry — the S3
   * `x-amz-meta-*` idiom, so a gateway can route on, inject or strip an
   * individual key without JSON surgery. Values become strings (objects are
   * JSON-stringified, null/undefined drop the key), and cross-origin
   * callers must name each key in `cors.metaHeaders`.
   *
   * The choice changes the wire only on http/sse; ws and the worker
   * transport have no headers and keep the one query parameter. What it
   * changes everywhere is the VALUES: `'prefixed'` flattens on every
   * transport, so the bag the server observes never depends on the carrier.
   */
  metaFormat?: 'json' | 'prefixed';
  /**
   * Pluggable query-string serializer (qs and friends) for mapped REST
   * requests over the http transport — mirror of the server's option.
   */
  querystring?: { stringify(query: object): string };
  /**
   * Client-side pre-validation: an injected ajv-shaped compiler applied to
   * the introspected input schema parts, so a doomed call rejects locally
   * (WrpcError 400 + details) without the round trip.
   */
  validation?: { ajv: { compile(schema: object): (value: unknown) => boolean } };
  /**
   * The client half of the server's wire codec — same structural shape.
   * The packet half (single-line text) frames ws/http/sse packet paths;
   * the optional `rest` section frames REST-leg BODIES (values, not
   * packets — binary allowed). Either half alone is valid.
   */
  codec?:
    | {
        encode(packet: unknown): string;
        decode(text: string): unknown;
        contentType?: string;
        rest?: { encode(value: unknown): string | Uint8Array; decode(body: Uint8Array | string): unknown; contentType?: string };
      }
    | { rest: { encode(value: unknown): string | Uint8Array; decode(body: Uint8Array | string): unknown; contentType?: string } };
  /** Shorthand for `reconnect.minDelay`. */
  reconnectTimeout?: number;
  heartbeat?: HeartbeatOptions | false;
  worker?: ServiceWorker;
  /**
   * Off by default, unlike the server: a client that printed on every
   * reconnect would be noise in a browser console nobody asked for. A logger
   * observes errors in addition to the `'error'` event, it does not replace
   * it.
   */
  logger?: WrpcLogger | boolean;
  /**
   * Off unless a tracer, a meter, or the OTel api module is supplied. With a
   * propagator, outgoing packets carry `tp`/`ts` so the server's span becomes
   * a child of this client's.
   */
  telemetry?: WrpcTelemetryOptions | null;
  /** Jitter source; injectable so tests can pin the backoff schedule. */
  random?: () => number;
  /**
   * Packet, subscription and stream ids; uuid v4 unless the app brings its
   * own (cuid/ulid/a test counter). Correlation ids, not secrets. A stream
   * id must stay within 255 UTF-8 bytes (the chunk header stores its
   * length in one byte).
   */
  generateId?: () => string;
  /**
   * WebSocket subprotocols to offer. Defaults to ['wrpc.v1'], which the
   * server echoes back as the wire revision; an empty array offers nothing
   * (the pre-versioning handshake). See protocol.md#versioning.
   */
  protocols?: Array<string>;
  proxy?: (data: string, packet: object | null) => void;
}

export class WrpcClientProxy extends Emitter {
  constructor(options?: WrpcClientOptions);
  open(): Promise<void>;
  close(): void;
}

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

/**
 * A logger to inject. Every member is optional so that both shapes are
 * structurally assignable without importing anybody's types:
 *
 * - a **structured** logger (pino, bunyan, winston) — identified by `child`
 *   or `level`, and called as `(entry, message)`
 * - a **Console** — called as `(message)`, with the entry dropped
 *
 * `true` logs to the global console; `false` disables logging outright. An
 * object that matches neither shape disables it too: an observability option
 * never throws.
 */
export interface WrpcLogger {
  child?(bindings: Record<string, unknown>): WrpcLogger;
  level?: unknown;
  log?(...args: any[]): void;
  info?(...args: any[]): void;
  debug?(...args: any[]): void;
  warn?(...args: any[]): void;
  error?(...args: any[]): void;
}

/**
 * The normalized writer wrpc hands BACK (context.log, client.log): every
 * level exists, `child` always answers, and `enabled` is false when logging
 * is off. What a user injects is {@link WrpcLogger}; this is what they get.
 */
export interface WrpcLogWriter {
  readonly enabled: boolean;
  child(bindings: Record<string, unknown>): WrpcLogWriter;
  log(entry: Record<string, unknown>, message?: string): void;
  info(entry: Record<string, unknown>, message?: string): void;
  debug(entry: Record<string, unknown>, message?: string): void;
  warn(entry: Record<string, unknown>, message?: string): void;
  error(entry: Record<string, unknown>, message?: string): void;
}

// ---------------------------------------------------------------------------
// OpenTelemetry
//
// Structural views of the OTel objects, so a real SDK is assignable without
// wrpc importing (or depending on) @opentelemetry/api. Only `Span.end` is
// required — everything a span might not implement is optional, and wrpc
// calls it optionally.

export interface WrpcSpan {
  setAttribute?(key: string, value: unknown): unknown;
  addEvent?(name: string, attributes?: Record<string, unknown>): unknown;
  recordException?(error: unknown): void;
  setStatus?(status: { code: number; message?: string }): unknown;
  end(): void;
}

export interface WrpcTracer {
  startSpan?(name: string, options?: unknown): WrpcSpan;
  startActiveSpan?<T>(name: string, options: unknown, fn: (span: WrpcSpan) => T): T;
}

export interface WrpcCounter {
  add(value: number, attributes?: Record<string, unknown>): void;
}

export interface WrpcHistogram {
  record(value: number, attributes?: Record<string, unknown>): void;
}

export interface WrpcMeter {
  createCounter(name: string, options?: unknown): WrpcCounter;
  createHistogram(name: string, options?: unknown): WrpcHistogram;
  createUpDownCounter?(name: string, options?: unknown): WrpcCounter;
}

export interface WrpcPropagation {
  inject(context: unknown, carrier: object, setter: unknown): void;
  extract(context: unknown, carrier: object, getter: unknown): unknown;
}

export interface WrpcContextApi {
  active(): unknown;
}

export interface WrpcTelemetryApi {
  trace?: { getTracer(name: string, version?: string): WrpcTracer };
  metrics?: { getMeter(name: string, version?: string): WrpcMeter };
  propagation?: WrpcPropagation;
  context?: WrpcContextApi;
}

/**
 * Two injection modes. `{ api }` — the `@opentelemetry/api` module, from
 * which wrpc derives its own tracer and meter so spans carry the
 * `@alexify/wrpc` instrumentation scope. `{ tracer, meter }` — pre-built
 * instances; either alone is a supported configuration.
 *
 * With neither, telemetry is off and every recording path is a no-op.
 *
 * @experimental The telemetry writer shapes and metric set may change in a
 * minor release; see CONTRIBUTING.md "Stability and deprecation".
 */
export interface WrpcTelemetryOptions {
  api?: WrpcTelemetryApi;
  tracer?: WrpcTracer;
  meter?: WrpcMeter;
  /**
   * Default true. When false, the peer address is left off spans. A session
   * token is never recorded at any setting — that is a credential, not an
   * identity, and the two do not share a switch.
   */
  includeIdentity?: boolean;
  /**
   * Trace context is written to, and read from, the packet's `tp`/`ts`
   * fields through these. `api` supplies both; pass them explicitly when
   * injecting a bare `tracer`/`meter`. Without a propagator wrpc emits local
   * spans only — it will not hand-roll the W3C format.
   */
  propagation?: WrpcPropagation;
  context?: WrpcContextApi;
  /**
   * Default true, as in gRPC and HTTP instrumentation: an inbound
   * `traceparent` becomes the server span's parent. Set false when peers are
   * untrusted — a hostile client can otherwise forge trace ids.
   */
  trustRemoteContext?: boolean;
}

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

/**
 * Fire-and-forget, both directions; `name` is 'unit/event'. With an `id`
 * it is an ask (server → client): the receiver MUST answer with a
 * `callback` carrying the same id.
 */
export interface EventPacket {
  type: 'event';
  name: string;
  data?: unknown;
  id?: string;
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
  error?: { message: string; code: number; details?: unknown };
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
