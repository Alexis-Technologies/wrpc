// The engine-agnostic server core — routers and procedures, subscriptions,
// sessions, rooms, the per-connection Client and per-call Context — as
// types. Everything here is node-free: it is what index.d.ts (the Node
// surface) and webrtc.browser.d.ts (a browser peer serving a router over a
// data channel) share, exactly as client.d.ts is shared by the two halves of
// the client. There is no rpc.js at runtime: the names below ship through
// the main barrel and, for a browser peer, through @alexify/wrpc/webrtc.
import { Emitter, WrpcError, WrpcReadable, WrpcWritable, WrpcLogWriter, WrpcLogger, WrpcCodec } from './client.js';

// ---------------------------------------------------------------------------
// Router / procedures

export type EventName = PropertyKey;

/** Per-message send options on the server side: `Client.sendEvent`, `Client.sendRaw` and the broadcast `emit`. */
export interface ServerSendOptions {
  /** A datagram where the transport has them (WebTransport): lossy, unordered; reliable elsewhere. */
  unreliable?: boolean;
  /** Send uncompressed even when permessage-deflate was negotiated and the size clears the threshold. */
  compress?: boolean;
}

export type BroadcastEmitOptions = ServerSendOptions;

/**
 * One message shared by every recipient of a broadcast: the serialized
 * packet plus an engine-owned cache slot. An engine claims `frames` when it
 * is null and reuses it when it already holds that engine's own cache; a
 * slot claimed by a different engine (a mixed room) means "use `text`".
 * Application code never builds one — `Broadcast.emit` does.
 */
export interface SharedMessage {
  text: string;
  frames: unknown | null;
  compress: boolean;
}

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

/**
 * One node of a {@link Signature}: a type name (`'string'`, `'number[]'`,
 * `'string|null'`), a field map whose keys may end in `?`, or a one-element
 * array meaning "an array of that". Deliberately closed — it crosses the wire
 * and ends up in a file someone compiles, so `wrpc types` renders anything it
 * does not recognise as `unknown`. See docs/reference/protocol.md.
 */
export type SignatureShape = string | [SignatureShape] | { [field: string]: SignatureShape };

/**
 * What a procedure looks like, for codegen. Not validation — `input`/`output`
 * are what enforce anything.
 */
export interface Signature {
  args?: SignatureShape;
  /** A call's result. Ignored on a subscription, which yields `data`. */
  returns?: SignatureShape;
  /** A subscription's value. Ignored on a call, which answers `returns`. */
  data?: SignatureShape;
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

export declare function isTracked(value: unknown): value is Tracked<unknown>;

/**
 * A bounded replay buffer. `since()` answers with what a client missed —
 * or `null` when the id has fallen out of the buffer, so a caller can
 * choose between a snapshot and an error instead of silently skipping a gap.
 */
export declare class EventLog<T = unknown> {
  /** Which log incarnation mints this log's ids (the `<epoch>.` prefix). */
  readonly epoch: string;
  constructor(options?: { size?: number; start?: number });
  readonly size: number;
  readonly length: number;
  readonly lastEventId: string | null;
  push(data: T): string;
  since(lastEventId?: string | null): Array<Tracked<T>> | null;
  clear(): void;
}

/**
 * Ids are `<epoch>.<n>`: random epoch per instance by default, so a
 * lastEventId from another process (or a restart) is a foreign epoch and
 * since() answers null — an honest "cannot resume" — instead of a numeric
 * coincidence. A persisted/shared log passes its own stable `epoch`.
 */
export declare function createEventLog<T = unknown>(options?: {
  size?: number;
  start?: number;
  epoch?: string;
}): EventLog<T>;

export interface SubscriptionOptions {
  /** What the client says it last saw; undefined on a fresh subscribe. */
  lastEventId?: string;
  /** Aborted on unsubscribe or disconnect. Honour it, or the feed leaks. */
  signal: AbortSignal;
}


/**
 * A lifecycle hook: named phases, fastify-style, with no `next`. A hook
 * runs and either returns (the pipeline continues) or throws an error whose
 * numeric `code` becomes the wire code. "After" is a later phase, not code
 * after a next() call. The payload depends on the phase: the packet for
 * onRequest/onSend/onResponse/onSubscribe, the args for
 * preValidation/preHandler, the result for preSerialization (returning a
 * value replaces it), the error for onError/onTimeout, the terminal packet
 * for onUnsubscribe.
 */
export type Hook = (context: Context, payload: unknown) => unknown | Promise<unknown>;
/**
 * Connection lifecycle hook; observational and contained. `onConnect`
 * receives null; `onDisconnect` receives `{ rooms }` — a snapshot of the
 * client's rooms taken before destroy() emptied the registry.
 */
export type ConnectionHook = (client: Client, payload: { rooms: Set<string> } | null) => unknown | Promise<unknown>;

/** The phases a router (or a unit's reserved `hooks` key) may register. */
export interface RouterHooks {
  onRequest?: Hook | Array<Hook>;
  preValidation?: Hook | Array<Hook>;
  preHandler?: Hook | Array<Hook>;
  preSerialization?: Hook | Array<Hook>;
  onSend?: Hook | Array<Hook>;
  onResponse?: Hook | Array<Hook>;
  onError?: Hook | Array<Hook>;
  onTimeout?: Hook | Array<Hook>;
  onSubscribe?: Hook | Array<Hook>;
  onUnsubscribe?: Hook | Array<Hook>;
  /** Router-level only. */
  onConnect?: ConnectionHook | Array<ConnectionHook>;
  /** Router-level only. */
  onDisconnect?: ConnectionHook | Array<ConnectionHook>;
}

/** The subset a unit's reserved `hooks` key accepts (no connection phases). */
export type UnitHooks = Omit<RouterHooks, 'onConnect' | 'onDisconnect'>;

/** A declarative REST mapping: this procedure IS `method path` under basePath. */
/** How a shared cache may treat a route's successful responses. GET/HEAD only. */
export interface HttpCache {
  /** Seconds. */
  maxAge: number;
  /** `public` in Cache-Control (a CDN may store it); default false → `private`. */
  public?: boolean;
  /** Seconds; adds `stale-while-revalidate`. */
  staleWhileRevalidate?: number;
  /** A weak ETag over the body and 304 on a matching If-None-Match; default true. */
  etag?: boolean;
}

export interface HttpRoute {
  method: 'GET' | 'HEAD' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  /** Relative to the server's basePath; segments are static or `:name`. */
  path: string;
  /** Success status; 204 discards the result body by contract. */
  status?: number;
  /** Static response headers on every answer of this route; transport-owned names are refused. */
  headers?: Record<string, string>;
  /**
   * The cache policy of a successful response. Emitted as declared only
   * for a public procedure on a request that restored no session and set
   * no cookie; anything session-bearing answers `private, no-store`.
   */
  cache?: HttpCache;
}

/**
 * The HTTP response seam a REST handler reaches as `context.http`: the
 * request line and headers, `setHeader()` onto this very response (refused
 * once it is sent, and for transport-owned names) and `status()` for its
 * success status. Null on every non-REST transport and on packet-mode HTTP.
 */
export interface HttpReply {
  readonly method: string;
  readonly url: string;
  readonly headers: Record<string, string | Array<string> | undefined>;
  setHeader(name: string, value: string | number): void;
  status(code: number): void;
}

/**
 * The fastify.route.schema shape: the parts wrpc understands plus any
 * passthrough keys (tags, summary, security, ...) forwarded to hosts
 * verbatim. `query` and `querystring` are interchangeable spellings.
 */
export interface ProcedureSchema {
  params?: object;
  querystring?: object;
  /** Alias of `querystring`; setting both to different objects throws. */
  query?: object;
  body?: object;
  headers?: object;
  /** Keyed by status code; `false` removes a default wrpc error entry. */
  response?: Record<string | number, object | false>;
  [passthrough: string]: unknown;
}

export interface ProcedureOptions {
  handler: ProcedureHandler;
  access?: 'public' | 'session';
  input?: Validator;
  output?: Validator;
  /** Maps this procedure onto a real REST endpoint. Calls only. */
  http?: HttpRoute;
  /** Declarative validation/serialization/docs; excludes input/output. */
  schema?: ProcedureSchema;
  /** Milliseconds; the call fails with code 408 when exceeded. */
  timeout?: number;
  /** Concurrency limit; overflow/starvation fails with code 503. */
  queue?: QueueOptions;
  meta?: Record<string, unknown>;
  /** Descriptor consumed by `wrpc types`; see {@link Signature}. */
  signature?: Signature;
  /** Inferred from an async generator handler; rarely written by hand. */
  kind?: 'call' | 'subscription';
  /** This procedure's own slice of the pipeline. */
  preValidation?: Hook | Array<Hook>;
  preHandler?: Hook | Array<Hook>;
  preSerialization?: Hook | Array<Hook>;
  onError?: Hook | Array<Hook>;
  /**
   * The delivery policy of a queue consumer. Valid only on a procedure
   * inside a unit's `consumes` block; see `@alexify/wrpc/broker`.
   * @experimental
   */
  consume?: ConsumePolicy;
}

/**
 * How a broker binding delivers a queue's messages into a consumer
 * procedure. Every field may be overridden at attach time
 * (`attachConsumers(server, broker, { 'unit.v1/source': { ... } })`).
 * @experimental
 */
export interface ConsumePolicy {
  /** The broker-side queue/topic; defaults to the `consumes` key. */
  queue?: string;
  /** The consumer group where the broker needs one; defaults to the queue. */
  group?: string;
  /** Unsettled messages held at once; at most the server's maxCalls. Default 16. */
  prefetch?: number;
  /** false disables retries (dead-letter on the first failure). */
  retry?:
    | false
    | {
        attempts?: number;
        backoff?: { base?: number; max?: number; factor?: number; jitter?: boolean };
        retryOn?: Array<number>;
      };
  /** Where exhausted or refused messages go; default `<queue>.dlq`, false drops them. */
  deadLetter?: string | false;
  /**
   * Who the consumer calls as. 'none' (default): no session — a procedure
   * with `access: 'session'` refuses to bind. 'service': one pseudo-session
   * for the binding. 'token': the session a message header's bearer token
   * restores.
   */
  identity?: {
    trust: 'none' | 'service' | 'token';
    session?: { token?: string; state?: Record<string, unknown> };
    header?: string;
  };
  /** Message headers copied into `context.callMeta` (lower-case names). */
  meta?: Array<string>;
  /** How a message body becomes the procedure's args; default JSON.parse. */
  args?: (body: string, headers: Record<string, string>) => unknown;
}

/** Same as ProcedureOptions minus the three a stream cannot mean. */
export type SubscriptionProcedureOptions = Omit<ProcedureOptions, 'queue' | 'timeout' | 'http'>;

export declare class Procedure {
  handler: ProcedureHandler;
  access: string;
  input: Validator | null;
  output: Validator | null;
  timeout: number;
  meta: Record<string, unknown>;
  signature: Signature | null;
  http: HttpRoute | null;
  /** Normalized: `query` folded into `querystring`. */
  schema: ProcedureSchema | null;
  kind: 'call' | 'subscription';
  readonly subscription: boolean;
  /** The consumer policy, frozen; null outside a `consumes` block. */
  readonly consume: Readonly<ConsumePolicy> | null;
  constructor(options: ProcedureOptions);
  invoke(context: Context, args: unknown, hooks?: Readonly<Record<string, ReadonlyArray<Hook>>>): Promise<unknown>;
  /**
   * The host-delegated entry: queue/timeout semantics without hooks or
   * validators — the host (a fastify route) already ran its own.
   */
  invokeBare(context: Context, args: unknown): Promise<unknown>;
  /** The value stream behind `{type:'subscribe'}`. */
  subscribe(
    context: Context,
    args: unknown,
    options?: Partial<SubscriptionOptions>,
    hooks?: Readonly<Record<string, ReadonlyArray<Hook>>>,
  ): AsyncIterableIterator<unknown>;
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
 * A unit's methods, plus two reserved keys: `on` holds its inbound event
 * handlers and `hooks` its slice of the lifecycle pipeline. Neither is
 * usable as a method name.
 */
export interface UnitDefinition {
  on?: EventsDefinition;
  hooks?: UnitHooks;
  /**
   * Queue consumers: procedures a broker binding invokes per delivered
   * message, keyed by source queue. Unreachable by a call packet, and not
   * part of introspection. @experimental
   */
  consumes?: EventsDefinition;
  [method: string]: MethodDefinition | EventsDefinition | UnitHooks | undefined;
}

/**
 * Unit keys are 'unit' or 'unit.vN' ('auth.v1'); method values are procedures,
 * bare handler functions, or procedure option objects.
 */
export type RouterDefinition = Record<string, UnitDefinition>;

export interface MethodInfo {
  access: string;
  /** Present only on subscriptions; a client scaffolds a call otherwise. */
  kind?: 'subscription';
  meta?: Record<string, unknown>;
  signature?: Signature;
  /** The declarative REST mapping, when the procedure carries one. */
  http?: HttpRoute;
  /** Input schema parts (params/querystring/body) for client pre-validation. */
  schema?: { params?: object; querystring?: object; body?: object };
}

/**
 * Injected JSON Schema compilers, structural: `ajv` is anything with
 * compile(schema) -> validateFn (ajv-shaped: boolean answer, `.errors` on
 * failure), `serializer` anything with compile(schema) -> (value) -> string
 * (fast-json-stringify-shaped). wrpc imports neither.
 */
export interface ValidationOptions {
  ajv?: { compile(schema: object): (value: unknown) => boolean };
  serializer?: { compile(schema: object): (value: unknown) => string };
}

/** What the injected compilers produced for one procedure. */
export interface CompiledArtifacts {
  input?: Validator;
  output?: Validator;
  serialize?: (value: unknown) => string;
}

/**
 * Router-level REST options. `version: 'path'` maps a versioned unit's
 * declared routes under a `/vN` prefix (`auth.v1` + `/auth/signIn` →
 * `/v1/auth/signIn`); a function receives the version token (`'v1'`) and the
 * declared path and returns the effective path. The default version stays
 * unprefixed, and `proc.http` (the declaration) is never mutated.
 */
export interface RestOptions {
  version?: 'path' | ((version: string, path: string) => string);
}

export declare class Router {
  constructor(
    definition?: RouterDefinition,
    options?: { hooks?: RouterHooks; validation?: ValidationOptions; rest?: RestOptions },
  );
  /**
   * Adds a unit after construction (how the fastify mirror lands units
   * discovered at onReady). Refuses an already-registered unit key.
   */
  addUnit(unitKey: string, definition: UnitDefinition): this;
  /** Adds a router-level hook after construction. Returns the router. */
  addHook(name: keyof RouterHooks, fn: Hook | ConnectionHook): this;
  /** The flattened pipeline for one procedure (router + unit + procedure). */
  hooksFor(proc: Procedure): Readonly<Record<string, ReadonlyArray<Hook>>>;
  /** The compiled { input?, output?, serialize? } for one procedure. */
  compiledFor(proc: Procedure): CompiledArtifacts | null;
  /** True when any procedure compiled a response serializer. */
  readonly hasSerializers: boolean;
  /** Router-level connection lifecycle hooks, consumed by RpcServer. */
  readonly connectionHooks: { onConnect: ReadonlyArray<ConnectionHook>; onDisconnect: ReadonlyArray<ConnectionHook> };
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
  /** A unit's queue-consumer procedure, if it declares one. @experimental */
  getConsumer(unit: string, version: string | undefined, name: string): Procedure | null;
  /** Every declared queue consumer. @experimental */
  consumers(): Array<{ unitKey: string; unit: string; version: string; name: string; procedure: Procedure }>;
  introspect(units?: Array<string> | null, options?: { schemas?: boolean }): Record<string, Record<string, MethodInfo>>;
  /** True when at least one procedure declares an `http` mapping. */
  readonly hasRestRoutes: boolean;
  /**
   * Matches a verb + decoded path segments against the REST table: null
   * (unknown path), `{ allowed }` (known path, wrong verb — a 405), or the
   * full route.
   */
  matchRest(
    method: string,
    segments: Array<string>,
  ):
    | { proc: Procedure; unitKey: string; methodName: string; params: Record<string, string>; http: HttpRoute }
    | { allowed: Array<string> }
    | null;
  /** Every declared REST route — what a host adapter registers natively. */
  restRoutes(): Array<{ unitKey: string; methodName: string; proc: Procedure; http: HttpRoute }>;
  /** Returns a NEW router; on collision the other router's procedure wins. */
  merge(other: Router): Router;
}

/**
 * The fastify-shaped schema a host receives: the user's declaration with
 * wrpc's own lifecycle error statuses documented underneath (overridable,
 * removable with `false`).
 */
export declare function effectiveSchema(proc: Procedure): ProcedureSchema;

export declare function defineRouter(
  definition: RouterDefinition,
  options?: { hooks?: RouterHooks; validation?: ValidationOptions; rest?: RestOptions },
): Router;

// ---------------------------------------------------------------------------
// Sessions

/** Structural store contract — anything with this shape plugs in. */
export interface SessionStore {
  /**
   * Optional sliding expiry: called on restore, because restoring IS active
   * use. A store without it keeps absolute TTLs — a valid policy too.
   */
  touch?(token: string): Promise<void> | void;
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
  touch(token: string): Promise<void>;
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
  /** True once finalized: a save still queued for it does not land. */
  readonly ended: boolean;
  /** Marks the session finalized (Client.finalizeSession calls it). */
  end(): void;
}

export interface CookieOptions {
  name?: string;
  path?: string;
  httpOnly?: boolean;
  secure?: boolean;
  sameSite?: 'Strict' | 'Lax' | 'None';
  maxAge?: number | null;
}

/**
 * Where the session token lives on the wire — injected like a codec or a
 * logger, checked structurally (`isTokenTransport`). The cookie default
 * keeps the behaviour wrpc always had; a bearer or payload strategy is one
 * object.
 */
export interface TokenTransport {
  /** The token this request presents, or null. */
  read(request: { headers?: Record<string, string | Array<string> | undefined>; url?: string }): string | null;
  /**
   * A Set-Cookie-style response header value to stamp (HTTP transports
   * only), or null when the carrier cannot stamp one — a bearer strategy
   * returns null and the signIn handler hands tokens back in its result.
   */
  write(token: string): string | null;
  /** Optional: the deleting stamp. */
  clear?(): string | null;
  /**
   * True when the BROWSER attaches the credential without script (a
   * cookie) — which is what the safe-method CSRF rule exists for. A
   * non-ambient carrier is exempt from that rule.
   */
  ambient?: boolean;
}

export declare function isTokenTransport(value: unknown): value is TokenTransport;

export interface SessionsOptions {
  store?: SessionStore;
  cookie?: CookieOptions;
  generateToken?: () => string;
  /** The injected token carrier; the cookie default is byte-identical. */
  transport?: TokenTransport;
}

declare class SessionManager {
  store: SessionStore;
  generateToken: () => string;
  cookie: CookieOptions & { name: string; path: string };
  /** The active token carrier (the cookie default unless injected). */
  transport: TokenTransport;
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
    /** Fires on EVERY successful join — the cluster's presence deltas. */
    onJoin?: (room: string, client: Client) => void;
    /** Fires on EVERY successful leave. */
    onLeave?: (room: string, client: Client) => void;
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
  /**
   * `unreliable: true`: a datagram to every recipient whose transport has
   * them, reliable to the rest; the flag crosses the backplane.
   * `compress: false`: sent uncompressed to every recipient that
   * negotiated permessage-deflate. The packet is serialized — and, on the
   * built-in engine, framed and deflated — ONCE for the whole fan-out.
   */
  emit(name: string, data?: unknown, options?: BroadcastEmitOptions | null): number;
  /**
   * Emits to every matching client — every instance's members included,
   * unless `local()` — and waits for each one's answer (registered
   * client-side with `client.respond(name, fn)`). Never rejects:
   * per-client failures are collected in `errors`.
   */
  ask(name: string, data?: unknown, options?: AskOptions): Promise<AskResult>;
}

export interface AskOptions {
  /** Per-client answer timeout in ms; default 7000. */
  timeout?: number;
}

export interface AskResult {
  /** Values the responders returned, in settlement order. */
  answers: Array<unknown>;
  /** Per-client failures: 501 no responder, 408 timeout, 503 disconnect. */
  errors: Array<{ message: string; code: number; details?: unknown }>;
  /** How many clients were asked, cluster-wide. */
  expected: number;
  /** True when a remote instance never answered inside the timeout. */
  incomplete: boolean;
}

/**
 * What a handler reaches through `context.server` — the part of a host
 * that addresses connected clients. `RpcServer` is one (it adds sessions,
 * the cluster, `sendTo`; narrow with `instanceof RpcServer` to reach them);
 * a WebRTC `PeerHost` is another, which is why this is a contract and not
 * the server class.
 */
export interface ClientHost {
  readonly router: Router;
  readonly rooms: RoomRegistry;
  readonly instanceId: string;
  /** The connected client with this id; undefined when not on this host. */
  getClient(id: string): Client | undefined;
  /** Chainable, immutable targeting: `to('a').except(c).emit(name, data)`. */
  to(...rooms: Array<string>): Broadcast;
  except(...clients: Array<Client>): Broadcast;
  /** Every connected client; returns how many received it locally. */
  broadcast(name: string, data?: unknown): number;
}

export interface ErrorOptions {
  id?: string;
  error?: Error;
}

/**
 * What the peer presented when the connection was made. Frozen. `headers`
 * and `data` are PEER-CONTROLLED — labels for logs, metrics and feature
 * gates, never an authorization input: authorization is the session's job.
 */
export interface ClientMeta {
  /** Client-declared metadata (the client's `meta` option); `{}` when none. */
  readonly data: Readonly<Record<string, unknown>>;
  /** Request/upgrade headers; `{}` on a worker port. */
  readonly headers: Readonly<Record<string, string | Array<string> | undefined>>;
  /** The request/upgrade URL with its query string; `''` on a worker port. */
  readonly url: string;
  readonly remoteAddress: string;
  /** The negotiated WebSocket subprotocol; `''` off ws. */
  readonly protocol: string;
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
  /** The REST response seam; null except on a REST call (see HttpReply). */
  readonly http: HttpReply | null;
  /**
   * The server this call arrived on — how a handler reaches rooms
   * (`context.server.to(room).emit(...)`) without closing over a server
   * that could not exist before the router it was built from.
   */
  readonly server: ClientHost | null;
  /**
   * A child of the connection's logger bound to this call's uuid — the
   * documented way to log from a handler: `context.log.info(...)`.
   */
  readonly log: WrpcLogWriter;
  /**
   * The wire target of this invocation — `'unit/name'` or `'unit.vN/name'`
   * for calls and subscriptions, the event name verbatim for inbound
   * events; null on a context built without a target.
   */
  readonly method: string | null;
  /** The procedure (or event handler) resolved for this invocation. */
  readonly procedure: Procedure | null;
  /** The connection's presented metadata — `client.meta`, mirrored. */
  readonly meta: ClientMeta;
  /**
   * The caller's per-invocation metadata (the packet's optional `meta`
   * field, sanitized: plain object, size-capped, `__proto__` dropped,
   * frozen). A frozen EMPTY object when the packet carried none — never
   * null, so `context.callMeta.idem` needs no `?.`. Deliberately outside
   * schema validation; a label, never an authorization input.
   */
  readonly callMeta: Readonly<Record<string, unknown>>;
  constructor(
    client: Client,
    signal?: AbortSignal | null,
    target?: { method?: string; procedure?: Procedure; callMeta?: Record<string, unknown> | null } | null,
  );
}

export class Client extends Emitter {
  /**
   * Instance-prefixed (`<instanceId>.<generateId()>`), so the id itself
   * addresses the instance holding the connection — what lets a cluster
   * command for one client travel as one message to one node.
   */
  readonly id: string;
  /**
   * The application's bag, carried by cluster descriptors (fetchClients).
   * wrpc itself never reads it — socket.io's `socket.data`.
   */
  data: Record<string, unknown>;
  source: string;
  session: Session | null;
  /** True for transports that stay open (WebSocket, worker port). */
  readonly persistent: boolean;
  /** The host this client belongs to (an RpcServer, a PeerHost); null for a standalone Client. */
  readonly server: ClientHost | null;
  /**
   * What the peer presented when the connection was made (headers, url,
   * negotiated subprotocol, client-declared metadata). Frozen; the
   * peer-controlled parts are labels, never authorization inputs.
   */
  readonly meta: ClientMeta;
  /** Settles once the cookie-based session restore (if any) finished. */
  sessionReady: Promise<unknown>;
  /**
   * What dispatch gates on: the session restore PLUS the settled onConnect
   * hooks. Two promises on purpose — an onConnect hook may `await
   * client.sessionReady`, so folding the hooks into that same promise would
   * make such a hook wait for itself. Never rejects.
   */
  ready: Promise<unknown>;
  /** True once `ready` has resolved — what lets the dispatcher skip the await per call. */
  readonly isReady: boolean;
  /** The REST response seam (Context.http); null except on a REST call. */
  http: HttpReply | null;
  streams: Map<string, WrpcReadable | WrpcWritable>;
  /** In-flight calls, by id — what `{type:'cancel'}` reaches. */
  calls: Map<string, AbortController>;
  /** Live subscriptions, by id — what `{type:'unsubscribe'}` reaches. */
  subscriptions: Map<string, AbortController>;
  maxSubscriptions: number;
  maxCalls: number;
  /** Context uuids and server-side stream ids; injectable via RpcServerOptions. */
  generateId: () => string;
  /** The connection-scoped log writer (peer binding included). */
  readonly log: WrpcLogWriter;
  /** The server's telemetry writer; disabled-shaped when unconfigured. */
  readonly otel: object;
  /** 'ws' | 'http' | 'sse' | 'event' — a metric attribute and a log field. */
  readonly transportKind: string;
  /** The per-message compression the peer negotiated for its own frames (a Node ws client's `compression`), or null. */
  readonly compression: { readonly id: string; readonly threshold: number } | null;
  /** False on a text-only transport (SSE), where binary streams cannot go. */
  readonly binary: boolean;
  /** Resolves when the transport drained, or when it closed. */
  drain(): Promise<void>;
  error(code: number, options?: ErrorOptions): void;
  /**
   * Returns false when the transport is above its high-water mark.
   * `compress: false` sends this packet uncompressed on a transport that
   * negotiated permessage-deflate (ignored elsewhere).
   */
  send(obj: object, options?: { code?: number; method?: string; compress?: boolean }): boolean;
  /**
   * Writes an ALREADY-serialized packet. With `unreliable: true`, as a
   * datagram where the transport can; reliably otherwise. `compress: false`
   * as on sendEvent.
   */
  sendRaw(text: string, options?: ServerSendOptions | null): boolean;
  /**
   * The fan-out seam: one `SharedMessage` for every recipient of a
   * broadcast. The text is serialized once; a transport with a
   * prepared-frame path (the built-in engine) encodes and deflates it once
   * into `frames` for every recipient after the first. Transports without
   * it write the text. Returns the backpressure signal.
   */
  sendShared(message: SharedMessage, options?: ServerSendOptions | null): boolean;
  createContext(signal?: AbortSignal | null): Context;
  /** The LOCAL Emitter emit — nothing reaches the wire; that is sendEvent. */
  emit(name: EventName, data?: unknown): Promise<void>;
  /** Sends a `{type:'event'}` packet to this peer; `name` is 'unit/event'. */
  /**
   * `unreliable: true` sends the event as a datagram where the transport
   * has them (WebTransport) — lossy and unordered — and reliably everywhere
   * else; the application code is the same either way.
   */
  sendEvent(name: string, data?: unknown, options?: ServerSendOptions | null): void;
  /**
   * A call in the other direction: sends `{type:'event', name, data, id}`
   * and resolves with what the peer's responder returns (registered
   * client-side with `client.respond(name, fn)`). Rejects with 408 on
   * timeout, 503 when the connection drops, 501 when the peer has no
   * responder.
   */
  ask(name: string, data?: unknown, options?: AskOptions): Promise<unknown>;
  /**
   * The bookkeeping half of ask(), for a caller that writes the packet
   * itself (Broadcast.ask's encode-once fan-out).
   */
  expectAnswer(id: string, timeout?: number): Promise<unknown>;
  /** Routes an inbound `callback` to its pending ask; false when none. */
  settleAnswer(packet: { id: string; result?: unknown; error?: { message: string; code: number; details?: unknown } }): boolean;
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
