import {
  IncomingMessage,
  Server as HttpServer,
  ServerResponse,
} from 'node:http';
import type { Connection } from './ws.js';
import type { Engine, EngineConnectionSource, WrpcSocket, EngineAttachOptions } from './engine.js';
import type { Backplane } from './scaling.js';
import {
  Emitter,
  WrpcError,
  WrpcReadable,
  WrpcWritable,
  WrpcLogger,
  WrpcLogWriter,
  WrpcTelemetryOptions,
  WrpcCodec,
} from './client.js';

// The browser-safe half of the surface lives in client.d.ts (which is what
// the `browser` types condition serves); this file is that plus the server.
export * from './client.js';

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
export interface HttpRoute {
  method: 'GET' | 'HEAD' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  /** Relative to the server's basePath; segments are static or `:name`. */
  path: string;
  /** Success status; 204 discards the result body by contract. */
  status?: number;
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
  emit(name: string, data?: unknown): number;
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

// ---------------------------------------------------------------------------
// Cluster

/** Narrows a cluster operation: one room, or every persistent client. */
export interface ClusterSelector {
  room?: string;
}

/** What fetchClients answers, one per matching client, cluster-wide. */
export interface ClientDescriptor {
  id: string;
  /** The instance holding the connection. */
  instance: string;
  rooms: Array<string>;
  /** The application's `client.data` bag. */
  data: Record<string, unknown>;
  /** 'ws' | 'sse' | 'event'. */
  transport: string;
  /** Whether the client currently carries a session. */
  session: boolean;
}

export interface ClusterOptions {
  /**
   * How often the corrective presence DIGEST is published; default 5000.
   * Raising it also delays eviction (presenceTimeout defaults to 3× it).
   */
  presenceInterval?: number;
  /** Silence after which a node is evicted; default 3× presenceInterval. */
  presenceTimeout?: number;
  /** Backstop for cluster requests (fetchClients, ask); default 2000. */
  requestTimeout?: number;
  /**
   * Which rooms replicate through presence: an array, predicate or RegExp.
   * Excluding high-cardinality families (the per-user `user:<id>` pattern)
   * keeps their deltas and digests off the wire entirely. Default: all.
   */
  rooms?: Array<string> | ((room: string) => boolean) | RegExp;
  /**
   * Per-node ceiling on one fetchClients reply; a node over it answers its
   * first `maxFetch` descriptors and the result carries `truncated: true`.
   * `0` disables. Default 1000.
   */
  maxFetch?: number;
  /**
   * Opt-in HMAC-SHA256 envelope authentication: with the same secret on
   * every node, an unsigned or mis-signed cluster message is dropped and
   * logged — "can publish on the broker" stops being "can command every
   * node". Room events travel unsigned; ACL the broker for those.
   */
  secret?: string;
}

export interface RoomsOptions {
  /**
   * How long an emptied room's backplane channel stays subscribed, in ms —
   * the grace window that absorbs reconnect churn for single-member rooms
   * and keeps the between-subscriptions loss window shut for the common
   * bounce. Default 5000; `0` unsubscribes immediately.
   */
  linger?: number;
}

export interface ClusterAskResult {
  /** One answer per node that had a responder. */
  answers: Array<unknown>;
  /** Per-node failures (no responder, responder threw). */
  errors: Array<string>;
  /** True when a node never answered inside the timeout. */
  incomplete: boolean;
}

/**
 * Presence, introspection and node-to-node messaging across every instance
 * sharing a backplane. Always present on an RpcServer: without a backplane
 * every operation degrades to its local half, so application code never
 * branches on the deployment.
 *
 * Presence is replicated — deltas plus corrective snapshots — so `count()`
 * and `presence()` are local reads with no network round-trip.
 */
export declare class Cluster extends Emitter {
  readonly instanceId: string;
  /** The boot marker distinguishing a restart from a live node. */
  readonly epoch: string;
  /** False without a backplane: every operation is local-only. */
  readonly connected: boolean;
  /**
   * False while a channel subscribe is failing and being retried: the node
   * can publish but cannot hear. 'degraded'/'recovered' fire on the
   * transitions — wire them to a readiness probe.
   */
  readonly healthy: boolean;
  /** Cluster-wide membership of `room`: a local sum, no network. */
  count(room: string): number;
  /** Per-instance breakdown of `room`; zero-count instances are omitted. */
  presence(room: string): { total: number; instances: Record<string, number> };
  /** Ids of the live instances, this one first. */
  instances(): Array<string>;
  /**
   * Descriptors of matching clients across the cluster. Resolves early the
   * moment every live node answered; on timeout the partial array carries a
   * non-enumerable `incomplete: true`.
   */
  fetchClients(
    sel?: ClusterSelector,
    options?: { timeout?: number },
  ): Promise<Array<ClientDescriptor> & { incomplete?: boolean; truncated?: boolean }>;
  /**
   * `target` is a client id (addressed: ONE instance hears it) or a
   * selector (`{ room }` / `{}`: applied on every instance). Commands are
   * fire-and-forget with the backplane's at-most-once delivery.
   */
  join(target: string | ClusterSelector, ...rooms: Array<string>): void;
  leave(target: string | ClusterSelector, ...rooms: Array<string>): void;
  disconnect(target: string | ClusterSelector): void;
  /** Fire-and-forget to every OTHER node's `cluster.on(name, ...)`. */
  sendEvent(name: string, data?: unknown): void;
  /** The LOCAL Emitter emit — remote nodes are reached by sendEvent. */
  emit(name: EventName, data?: unknown): Promise<void>;
  /**
   * Asks every other node and collects their answers — each node answers
   * through its `cluster.respond(name, fn)` responder, or contributes an
   * error when it has none.
   */
  ask(name: string, data?: unknown, options?: { timeout?: number }): Promise<ClusterAskResult>;
  /** One responder per name; a duplicate registration throws. */
  respond(name: string, handler: (data: unknown, from: string) => unknown): void;
  unrespond(name: string): boolean;
}

// ---------------------------------------------------------------------------
// Server core

export interface CorsOptions {
  origins?: Array<string> | ((origin: string) => boolean);
  credentials?: boolean;
  /**
   * `Access-Control-Allow-Headers`. Replaces the default,
   * `'Content-Type, x-wrpc-channel, last-event-id, x-wrpc-meta'` — none of
   * the last three is CORS-safelisted, so dropping `x-wrpc-channel` or
   * `last-event-id` disables cross-origin SSE, and dropping `x-wrpc-meta`
   * disables cross-origin connection metadata.
   *
   * An array is joined with `', '` — the same value, spelled as a list.
   */
  headers?: string | Array<string>;
  /**
   * Meta keys a cross-origin client may send as per-key
   * `x-wrpc-meta-<key>` headers (the client's `metaFormat: 'prefixed'`).
   * CORS has no wildcard for header names, so each key must be named;
   * `['userId']` grants `x-wrpc-meta-user-id`, kebab-normalized to match
   * what the client actually sends. Appended to `headers`, never replacing
   * it. Unnecessary with the default `metaFormat: 'json'`, which sends the
   * one already-allowed `x-wrpc-meta` header whatever the keys are.
   */
  metaHeaders?: Array<string>;
  /** `Access-Control-Allow-Methods`; default `'POST, GET, OPTIONS'`. */
  methods?: string;
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
  /**
   * The server this call arrived on — how a handler reaches rooms
   * (`context.server.to(room).emit(...)`) without closing over a server
   * that could not exist before the router it was built from.
   */
  readonly server: RpcServer | null;
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
  /** The RpcServer this client belongs to; null for a standalone Client. */
  readonly server: RpcServer | null;
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
  /** False on a text-only transport (SSE), where binary streams cannot go. */
  readonly binary: boolean;
  /** Resolves when the transport drained, or when it closed. */
  drain(): Promise<void>;
  error(code: number, options?: ErrorOptions): void;
  /** Returns false when the transport is above its high-water mark. */
  send(obj: object, options?: { code?: number; method?: string }): boolean;
  /**
   * Writes an ALREADY-serialized packet — the fan-out seam: a broadcast
   * stringifies once and hands every recipient the same text.
   */
  sendRaw(text: string): boolean;
  createContext(signal?: AbortSignal | null): Context;
  /** The LOCAL Emitter emit — nothing reaches the wire; that is sendEvent. */
  emit(name: EventName, data?: unknown): Promise<void>;
  /** Sends a `{type:'event'}` packet to this peer; `name` is 'unit/event'. */
  sendEvent(name: string, data?: unknown): void;
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

// ---------------------------------------------------------------------------
// Logging

export interface RpcServerOptions {
  router: Router;
  sessions?: SessionsOptions;
  cors?: CorsOptions | null;
  /** Default '/api'; '' serves from the root. */
  basePath?: string;
  /** Defaults to the global console; `false` silences the server. */
  logger?: WrpcLogger | boolean;
  /** Off unless a tracer, a meter, or the OTel api module is supplied. */
  telemetry?: WrpcTelemetryOptions | null;
  /**
   * Carries room events between instances. Optional: without one, rooms
   * work identically inside a single instance.
   */
  backplane?: Backplane | null;
  /**
   * Identifies this instance on the backplane; a uuid by default. Must not
   * contain '.' — it prefixes every client id (`<instanceId>.<id>`).
   */
  instanceId?: string;
  /**
   * Context uuids, server-side stream ids and synthetic REST packet ids;
   * uuid v4 unless the app brings its own. Correlation ids, not secrets.
   */
  generateId?: () => string;
  /**
   * system/introspect exposure: `true` (default) mounts it public,
   * `'session'` gates it behind a session, `false` leaves the API surface
   * unadvertised. A router defining its own introspect always wins.
   */
  introspection?:
    | boolean
    | 'session'
    | { access?: boolean | 'session'; schemas?: boolean };
  /** Packets accepted in one batch frame; default 128. */
  maxBatch?: number;
  /** Concurrent subscriptions per client; default 256. */
  maxSubscriptions?: number;
  /** In-flight calls per client; past it a call answers 429. Default 1000. */
  maxCalls?: number;
  /** SSE channel options, or `false` to remove the events endpoint. */
  sse?: import('./sse.js').SseOptions | false;
  /**
   * Presence/request tuning for the cluster layer, or `false` to opt out:
   * presence, commands and asks then degrade to their local halves while
   * the rooms backplane keeps working.
   */
  cluster?: ClusterOptions | false;
  /** Rooms-backplane tuning (see the scaling guide). */
  rooms?: RoomsOptions;
  /**
   * Pluggable query-string codec (qs and friends) for REST-mode requests.
   * The injected parser takes over prototype-pollution responsibility.
   */
  querystring?: { parse(text: string): Record<string, unknown>; stringify?(query: object): string };
  /**
   * Pluggable wire codec for wrpc packets (ws, packet-mode HTTP, SSE data,
   * worker ports). Text-only: encode must produce single-line text. REST
   * mode, SSE control frames and binary chunks stay outside it. Mutually
   * exclusive with compiled response serializers.
   */
  codec?: WrpcCodec;
  /**
   * Cap on peer-declared metadata, measured on the ENCODED input: the ws
   * `wrpc_h` connect-URL parameter and the per-packet `meta` field. Over
   * the cap the label is refused (a warn is logged), never the connection.
   * Default 2048.
   */
  metaMaxBytes?: number;
}

// The codec types (WrpcCodec, WrpcPacketCodec, WrpcRestCodec) and the
// isCodec predicate live in client.d.ts — the client accepts the same
// shape — and arrive here through the `export *`. One declaration: the
// browser and node entries used to present DIFFERENT signatures for the
// same runtime function.

export declare class RpcServer extends Emitter {
  readonly router: Router;
  readonly sessions: SessionManager;
  readonly rooms: RoomRegistry;
  readonly instanceId: string;
  readonly basePath: string;
  /** The injected codec option, verbatim; null without one. */
  readonly codec: WrpcCodec | null;
  /** Where the SSE stream lives: `${basePath}/events`. */
  readonly eventsPath: string;
  /** The SSE channel registry, or null when `sse: false`. */
  readonly sse: import('./sse.js').SseChannels | null;
  readonly clients: Set<Client>;
  /**
   * A host-delegated REST route runs its procedure outside handleHttpCall:
   * the host owns routing/validation/serialization, wrpc the session and
   * client lifecycle. Call `release()` when the response closes.
   */
  delegatedContext(
    request?: { method?: string; headers?: Record<string, string | undefined>; remoteAddress?: string; url?: string },
    target?: { method?: string; procedure?: Procedure } | null,
  ): Promise<{
    client: Client;
    context: Context;
    transport: ServerTransport;
    release: () => void;
  }>;
  /** Cluster-wide presence, introspection and node-to-node messaging. */
  readonly cluster: Cluster;
  constructor(options: RpcServerOptions);
  /** The local client with this id; undefined when not on this instance. */
  getClient(id: string): Client | undefined;
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
  /** True while drain() runs: new calls are refused with 503. */
  readonly draining: boolean;
  /**
   * False while a backplane channel subscribe is failing and being retried
   * (rooms or cluster): the node can publish but cannot HEAR — drain it
   * from rotation. Always true without a backplane.
   */
  readonly healthy: boolean;
  /**
   * The server telemetry writer, for hosts that run procedures outside the
   * dispatcher (the fastify adapter's delegated routes bracket invokeBare
   * with it). @experimental — the writer's shape may change in a minor.
   */
  readonly otel: unknown;
  /**
   * The graceful half of a shutdown: refuse new calls (503) and wait up to
   * `timeout` ms for in-flight ones to settle. Subscriptions are not waited
   * for — a live feed has no natural end. Resolves early when idle.
   */
  drain(timeout?: number): Promise<void>;
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
  /** Request-body cap in bytes for the built-in HTTP path. Default 10 MiB. */
  maxBodySize?: number;
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
  /** Cluster-wide presence, introspection and node-to-node messaging. */
  readonly cluster: Cluster;
  /** The local client with this id; undefined when not on this instance. */
  getClient(id: string): Client | undefined;
  listen(): Promise<Server>;
  /**
   * With `drain` (ms): stop intake, let in-flight calls settle up to the
   * window, send every peer 1001 "going away", then tear down what remains.
   */
  close(options?: { drain?: number }): Promise<void>;
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

