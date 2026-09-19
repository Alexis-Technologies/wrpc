import {
  IncomingMessage,
  Server as HttpServer,
  ServerResponse,
} from 'node:http';
import type { Connection } from './ws.js';
import type { Engine, EngineConnectionSource, WrpcSocket, EngineAttachOptions } from './engine.js';
import type { Backplane } from './scaling.js';
import type {
  Broadcast,
  Client,
  ClientMeta,
  Context,
  ErrorOptions,
  EventName,
  Procedure,
  RoomRegistry,
  Router,
  SessionManager,
  SessionsOptions,
  procedure,
} from './rpc.js';
import {
  Emitter,
  WrpcError,
  WrpcReadable,
  WrpcWritable,
  WrpcLogger,
  WrpcLogWriter,
  WrpcTelemetryOptions,
  WrpcCodec,
  CompressionOptions,
  Compressor,
} from './client.js';

// The browser-safe half of the surface lives in client.d.ts (which is what
// the `browser` types condition serves); the engine-agnostic server core
// (routers, sessions, rooms, Client, Context) in rpc.d.ts, shared with the
// WebRTC peer types; this file is both plus the Node server.
export * from './client.js';
export * from './rpc.js';

/**
 * The dictionary codec (raw deflate through node:zlib with a preset
 * dictionary), for `compression: { codec }` on any Node↔Node carrier. Its
 * `id` carries the dictionary's hash: two ends compress against the same
 * bytes or, when their routers differ, not at all. `threshold` defaults to
 * 64 B — with the history preloaded, small messages are what it is for.
 * A browser needs the pure-JS codec of `@alexify/wrpc/deflate` instead.
 */
export declare function dictionaryCompressor(
  dictionary: Uint8Array | string,
  options?: { level?: number; threshold?: number },
): Compressor & { readonly dictionary: Uint8Array };

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
  /**
   * Compresses the cluster envelopes this node publishes, after signing —
   * the same marker, rollout rule and synchronous-codec requirement as
   * `rooms.compression`; an unreadable envelope logs `cluster.encoded`.
   */
  compression?: boolean | CompressionOptions;
  /** The largest inflated envelope accepted (default 16 MiB). */
  maxMessage?: number;
}

export interface RoomsOptions {
  /**
   * The producer-restart marker every backplane envelope carries, so a
   * receiver can tell "this instance restarted, its sequence began again"
   * from a real gap. Random per boot by default, which is what you want
   * unless a deployment pins it deliberately across restarts.
   */
  epoch?: string;
  /**
   * How long an emptied room's backplane channel stays subscribed, in ms —
   * the grace window that absorbs reconnect churn for single-member rooms
   * and keeps the between-subscriptions loss window shut for the common
   * bounce. Default 5000; `0` unsubscribes immediately.
   */
  linger?: number;
  /**
   * Compresses every room envelope this instance publishes past the
   * threshold (src/compression), off by default. A string carrier, so a
   * compressed envelope rides as base64 under a `wrpc-enc:<id>:` marker —
   * and there is no negotiation: an instance without the option drops such
   * an envelope and logs `backplane.encoded`. Roll it out in two steps
   * (deploy the version, then turn it on). The codec must be synchronous.
   */
  compression?: boolean | CompressionOptions;
  /** The largest inflated envelope accepted (default 16 MiB). */
  maxMessage?: number;
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
  /**
   * One event to ONE client by id — an addressed command, so only the
   * instance its id names hears it. `room` narrows delivery to a client
   * still in that room. Fire-and-forget, at-most-once.
   */
  send(clientId: string, name: string, data?: unknown, options?: { room?: string }): void;
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
   * Identifies this instance on the backplane; minted by `generateId` when
   * omitted. Must not contain '.' — it prefixes every client id
   * (`<instanceId>.<id>`), and a `generateId` that returns one is refused
   * by the same rule.
   */
  instanceId?: string;
  /**
   * Every id this server mints: the `instanceId` above, client ids, context
   * uuids, server-side stream ids, synthetic REST packet ids, SSE channel
   * ids and the cluster's boot epoch. uuid v4 unless the app brings its own
   * (cuid/ulid/a test counter). Correlation ids, not secrets — a session
   * token has its own generator, `sessions.generateToken`.
   *
   * Validated once at construction: it must be a function answering a
   * non-empty string of at most 255 characters (the binary chunk header's
   * own limit). The check consumes one id, which becomes the `instanceId`
   * rather than being discarded.
   *
   * @deprecated-behaviour A non-function is reported through the logger and
   * replaced with the default. 2.0 makes it a TypeError, as it already is
   * on the options added since (SSE channels, the broker adapters).
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
  /** The HTTP side's own options: `compression`, off by default. */
  http?: { compression?: boolean | HttpCompressionOptions };
  /**
   * Accept per-message compressed frames from a Node WebSocket client that
   * negotiated them (its own `compression` option, agreed over the first
   * ping/pong) — the direction permessage-deflate cannot cover, since
   * Node's built-in WebSocket only inflates. Off by default; a browser
   * client is untouched. The codec must answer synchronously.
   */
  compression?: boolean | CompressionOptions;
  /** The largest inflated client frame accepted on a socket (default 16 MiB). */
  maxMessage?: number;
  /**
   * Binary attachments: raw bytes (typed arrays, ArrayBuffers) anywhere in
   * a packet's args, result, data or error details travel as bytes in one
   * binary frame, and arrive as Uint8Arrays — instead of the plain objects
   * JSON makes of them. On by default; `false` sends every packet as JSON
   * as revision 1 did (set it on both ends). Off by itself under a packet
   * `codec`, which owns the wire. SSE refuses them explicitly (501/415).
   */
  attachments?: boolean;
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
  /**
   * One event to one client by id, here or on the instance its id names
   * (via the cluster). `room` narrows delivery to a client still in that
   * room. True when delivered locally or handed to the backplane; false
   * when known undeliverable.
   */
  sendTo(clientId: string, name: string, data?: unknown, options?: { room?: string }): boolean;
  /** Everyone in any of `rooms`, each client once; with no rooms, nobody. */
  to(...rooms: Array<string>): Broadcast;
  /** Everyone connected, minus `clients`. */
  except(...clients: Array<Client>): Broadcast;
  /** Everyone connected; returns the number of LOCAL recipients. */
  broadcast(name: string, data?: unknown): number;
  /**
   * `meta` is the handshake as the host saw it: `url` carries the declared
   * `wrpc_h`/`wrpc_meta` query, `kind` names the wire for logs and metrics
   * when the socket is not a WebSocket (`'wt'` from `@alexify/wrpc/wt`).
   */
  attachSocket(
    socket: WrpcSocket | Connection,
    meta?: { headers?: Record<string, string | undefined>; url?: string; remoteAddress?: string; kind?: string },
  ): Client;
  attachPort(port: MessagePort): Client;
  /**
   * Any persistent transport announcing its inbound traffic as 'packet'
   * (text) and 'chunk' (bytes) events — the seam a wire the core never heard
   * of plugs into (`attachChannel` in `@alexify/wrpc/webrtc` does). `meta` is
   * what the application observed about the connection, if anything.
   *
   * Identity, one of: `session` — a pseudo-session the host vouches for,
   * in place before the onConnect hooks run; `request` — what the peer
   * presented, restored through the configured token carrier (a bearer
   * token in a broker message's headers restores a real session).
   */
  attach(transport: InboundTransport, options?: AttachOptions): Client;
  handleHttpCall(call: HttpCall): Promise<void>;
  matchPath(pathname: string): { mode: 'packet' | 'rest'; rest?: string } | null;
  /** The per-connection caps every attached client gets. */
  readonly limits: Readonly<{ maxBatch: number; maxSubscriptions: number; maxCalls: number }>;
  /**
   * True while drain() runs: new calls are refused with 503. Draining is
   * announced once as a `'draining'` event, so a binding that pulls work
   * on its own (a broker consumer) stops fetching.
   */
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
   * The normalized log writer — not the `logger` that was passed in. For
   * hosts and attachers that need somewhere to report a failure which never
   * reaches a `Client`: a framework adapter, a WebTransport session.
   * Re-wrapping a writer is free, so passing this straight into another
   * component's `logger` option is the intended use.
   */
  readonly log: WrpcLogWriter;
  /**
   * The graceful half of a shutdown: refuse new calls (503) and wait up to
   * `timeout` ms for in-flight ones to settle. Subscriptions are not waited
   * for — a live feed has no natural end. Resolves early when idle.
   */
  drain(timeout?: number): Promise<void>;
  /** Emits `'close'` first, then tears every client, channel and binding down. */
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
  /** One event to one client by id, here or on another instance — forwarded to the core. */
  sendTo(clientId: string, name: string, data?: unknown, options?: { room?: string }): boolean;
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
  /** The normalized `http.compression` option; the core passes its own. */
  compression?: Readonly<{
    threshold: number;
    filter: ((call: HttpCall) => boolean) | null;
    level?: number;
    memLevel?: number;
    async: { threshold: number } | null;
  }> | null;
}

/**
 * gzip for packet-mode and REST answers — `http: { compression }`, off
 * by default. A response is encoded when the request's `Accept-Encoding`
 * admits gzip, the body is at or over `threshold`, nothing upstream set a
 * `Content-Encoding`, and `filter` (when given) says yes; it then carries
 * `Content-Encoding: gzip` and `Vary: Accept-Encoding`. Nothing changes on
 * the client: `fetch` inflates by itself.
 */
export interface HttpCompressionOptions {
  /** Bytes; smaller bodies go plain. Default 1024. */
  threshold?: number;
  /** Per request: compress this answer at all? Runs after the cheaper checks. */
  filter?: (call: HttpCall) => boolean;
  /** zlib level, -1..9. */
  level?: number;
  /** zlib memLevel, 1..9. */
  memLevel?: number;
  /**
   * Bodies at or over `threshold` bytes gzip on zlib's threadpool and the
   * response is written from the callback — the same shape as
   * `perMessageDeflate.async`. Default threshold 256 KiB; `{}` or `true`
   * takes it. Off by default.
   */
  async?: boolean | { threshold?: number };
}

export type AttachOptions = {
  meta?: ClientMeta | null;
  /**
   * false: a request/response carrier (a broker consumer binding) — calls
   * only, and not counted among connected clients. Default true.
   */
  persistent?: boolean;
} & (
  | { session?: null; request?: null }
  | { session: { token?: string; state?: Record<string, unknown>; [key: string]: unknown }; request?: null }
  | {
      session?: null;
      request: { headers?: Record<string, string | undefined>; url?: string; remoteAddress?: string };
    }
);

/** What RpcServer.attach() accepts: persistent, and announcing 'packet'/'chunk'. */
export interface InboundTransport extends Emitter {
  kind?: string;
  source?: string;
  /** Truthy for a persistent transport; ignored (and cleared) with `persistent: false`. */
  connection?: unknown;
  write(data: string | Uint8Array): boolean;
  close(): void;
}

export class ServerTransport extends Emitter {
  static transport: {
    http: typeof ServerHttpTransport;
    ws: typeof ServerWsTransport;
    event: typeof ServerEventTransport;
  };
  source: string;
  /** 'http' | 'ws' | 'event' | 'sse' | 'webrtc' | 'wt' — what `Client.transportKind` reports. */
  kind: string;
  /** Set on transports that stay open; `Client.persistent` is its truthiness. */
  connection?: unknown;
  /** False on a text-only transport, where binary streams are refused. */
  binary?: boolean;
  constructor(source: string);
  error(code?: number, options?: ErrorOptions): boolean;
  /** Returns the transport's backpressure signal (false = above the mark). */
  send(obj: object, code?: number): boolean;
  /** The raw write every send() ends in; returns the same backpressure signal. */
  write(data: string | Uint8Array): boolean;
  /** @experimental A packet as one datagram where the connection has them (WebTransport); false otherwise. */
  writeUnreliable?(text: string): boolean;
  close(): void;
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
  /** Set to the transport itself: a port stays open, so the Client is persistent. */
  connection: ServerEventTransport;
  constructor(port: MessagePort);
  write(data: string | Buffer): boolean;
  close(): void;
}
export type { ServerEventTransport };

/** Per-request response headers: security defaults + CORS for `origin`. */
export function buildHeaders(
  cors?: CorsOptions | null,
  origin?: string,
): Record<string, string>;

