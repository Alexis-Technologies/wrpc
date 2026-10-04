import type { Client, RpcServer, Server, ServerTransport } from './index.js';

/**
 * `@alexify/wrpc/wt` — the server half of WebTransport (HTTP/3).
 *
 * @experimental The whole subpath may change in a minor (see
 * docs/reference/stability.md).
 *
 * The client transport is in the base entry: `connect(url, { transport:
 * ['wt', 'ws'], wt: { serverCertificateHashes } })` needs no import. What a
 * Node server needs is here — and Node has no WebTransport of its own, so
 * the implementation is the application's to inject: the sessions a host
 * hands out (`@fails-components/webtransport`, `quico`, or anything with
 * the W3C session shape) are attached to an ordinary `Server`/`RpcServer`,
 * where they share rooms, cluster and sessions with WebSocket clients.
 *
 * ```js
 * const { Http3Server } = await import('@fails-components/webtransport');
 * const { acceptSessions } = require('@alexify/wrpc/wt');
 *
 * const h3 = new Http3Server({ port: 4433, host: '0.0.0.0', secret, cert, privKey });
 * h3.startServer();
 * acceptSessions(server, h3.sessionStream('/api'));
 * ```
 *
 * A WebTransport CONNECT sends no cookies, so `sessions.transport` must be
 * a bearer or payload token transport (`@alexify/wrpc/auth`), and declared
 * headers/meta ride the connect URL as on ws.
 */

/** A `{ readable, writable }` pair of WHATWG streams — one WebTransport stream. */
export interface WtStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
}

export interface WtDatagrams {
  readonly readable: ReadableStream<Uint8Array>;
  /** The legacy datagram stream; `createWritable()` is preferred where present. */
  readonly writable?: WritableStream<Uint8Array>;
  createWritable?(options?: object): WritableStream<Uint8Array>;
  readonly maxDatagramSize?: number;
}

export interface WtCloseInfo {
  closeCode?: number;
  reason?: string;
}

/**
 * The structural session contract — the W3C WebTransport shape, so a
 * session from any implementation qualifies as-is; quico's http-shaped one
 * goes through fromQuico().
 */
export interface WtSession {
  readonly ready?: Promise<void>;
  readonly closed: Promise<WtCloseInfo>;
  readonly incomingBidirectionalStreams: ReadableStream<WtStream>;
  readonly incomingUnidirectionalStreams?: ReadableStream<ReadableStream<Uint8Array>>;
  createBidirectionalStream(): Promise<WtStream>;
  createUnidirectionalStream?(): Promise<WritableStream<Uint8Array>>;
  readonly datagrams?: WtDatagrams | null;
  close(info?: WtCloseInfo): void;
}

export declare function isWtSession(value: unknown): value is WtSession;
export declare function isWtStream(value: unknown): value is WtStream;
export declare function isWtDatagrams(value: unknown): value is WtDatagrams;

/** The CONNECT request as the host implementation saw it. */
export interface SessionMeta {
  headers?: Record<string, string>;
  /** Path and query — where a browser client declares `wrpc_h`/`wrpc_meta`. */
  url?: string;
  remoteAddress?: string;
}

export interface WtSocketOptions {
  remoteAddress?: string;
  /** Outbound bytes queued on the control stream before send() answers false (default 1 MiB). */
  highWaterMark?: number;
  /** Queue level under which 'drain' fires (default 256 KiB). */
  lowWaterMark?: number;
  /**
   * Outbound bytes queued before the session is terminated for a peer that
   * never drains (default 64 MiB; 0 = off) — the cap behind the high-water
   * mark, as the WebSocket engine's. Checked before a frame is queued: one
   * frame past it on an empty queue is sent.
   */
  maxBackpressure?: number;
  /** The largest inbound message accepted (default 16 MiB); past it the peer is hung up with 1002. */
  maxMessage?: number;
  /**
   * Terminates the session when nothing arrives on the control stream for
   * this long (ms; 0 = off, the default). A wrpc client pings on its
   * heartbeat, so a live one is never idle; a host that reports no session
   * end (quico 0.4) needs it to shed a peer that vanished.
   */
  idleTimeout?: number;
  /**
   * How long a graceful `close()` waits for the control stream to take what
   * `send()` already accepted before it closes the session (ms, default
   * 1000) — closing a session resets its streams and drops what they still
   * hold. `terminate()` never waits.
   */
  closeTimeout?: number;
  /**
   * Inbound unidirectional streams held for their open packet at once —
   * read no further than their first read meanwhile — before a further
   * one is cancelled unread (default 32). A peer that opens streams for
   * ids it never names used to have every byte of them buffered, before
   * any authentication, past `maxMessage`.
   */
  maxHeldStreams?: number;
  /** How long a held stream waits for its open packet before it is cancelled (ms, default 10 s). */
  holdTimeout?: number;
  /**
   * Per-message compression on the control stream (src/compression), off
   * by default: `true` for the platform codec (raw deflate through
   * node:zlib), `{ codec, threshold }` to inject one or move the size
   * under which a message goes plain (1 KiB). Announced in the
   * capabilities message; applied only once the client named the same
   * codec, so a client without it is served plain.
   */
  compression?: boolean | import('./client.js').CompressionOptions;
  /**
   * Hears a codec that failed on the way out — the frame left plain.
   * `attachSession` hands it to the server (`compression.failed`, counted);
   * a socket built by hand says nothing unless this is given.
   */
  onCodecError?: (codec: string, error: unknown) => void;
}

export interface AttachSessionOptions extends SessionMeta, Omit<WtSocketOptions, 'onCodecError'> {
  /** Refuses the session (closed 403) when it answers false. */
  verify?: (info: SessionMeta & { session: WtSession }) => boolean | Promise<boolean>;
  /**
   * How long the whole accept path — `verify`, the session's `ready`, the
   * first stream — may take (default 10 s; closed 408 past it).
   */
  acceptTimeout?: number;
  /** The structured logger the session reports through — a child bound to the peer; the server's by default, acceptSessions hands its own down. */
  logger?: unknown;
  /** Aborting it ends an accept still in its handshake (closed 1001); what acceptSessions' stop() does. */
  signal?: AbortSignal;
  /** What `Client.transportKind` reports (default 'wt'). */
  kind?: string;
}

/**
 * A WebTransport session as a `WrpcSocket` — the engine-port socket contract
 * over the client's control stream, what `RpcServer.attachSocket` takes.
 * `close(code, reason)` maps to `session.close({ closeCode: code, reason })`.
 */
export declare class WtSocket {
  constructor(session: WtSession, stream: WtStream, options?: WtSocketOptions);
  /** The codecs in effect — null until the two lists share one. */
  readonly compression: import('./client.js').NegotiatedCompression | null;
  readonly session: WtSession;
  readonly stream: WtStream;
  readonly bufferedAmount: number;
  readonly isPaused: boolean;
  readonly remoteAddress: string;
  readonly protocol: string;
  /** `options.compress === false` sends this one message plain whatever was negotiated. */
  send(data: string | Uint8Array | ArrayBuffer, options?: { compress?: boolean } | null): boolean;
  /**
   * Told about an outbound stream packet before it is serialized: true when
   * the packet must not go on the control stream — its stream's own FIN or
   * reset carries it.
   */
  streamControl(packet: { type: string; id?: string; status?: string }): boolean;
  /**
   * A packet as one datagram; false when the session has none, the packet
   * does not fit, or the socket is closed. True — and dropped — when the
   * session has not taken the datagrams before it: never queued.
   */
  sendUnreliable(data: string): boolean;
  /** Datagrams dropped that way so far. */
  readonly droppedDatagrams: number;
  /** The largest datagram the session carries; 0 when it carries none. */
  readonly maxDatagramSize: number;
  pause(): void;
  resume(): void;
  close(code?: number, reason?: string): void;
  terminate(): void;
  on(event: 'message', listener: (data: string | Uint8Array, isBinary: boolean) => void): void;
  on(event: 'close', listener: (code: number, reason: string) => void): void;
  on(event: 'drain' | 'error', listener: (value?: unknown) => void): void;
  once(event: string, listener: (...args: Array<any>) => void): void;
  off(event: string, listener?: (...args: Array<any>) => void): void;
  listenerCount(event: string): number;
}

/**
 * The server-side transport a WebTransport client gets — the WebSocket one
 * over a WtSocket, seeing stream packets before serialization so a binary
 * stream's end travels as its own WebTransport stream's FIN. Registered as
 * `ServerTransport.transport.wt`; `attachSocket(socket, { kind: 'wt' })`
 * picks it.
 */
export declare class ServerWtTransport extends ServerTransport {
  kind: 'wt';
  connection: WtSocket;
  constructor(connection: WtSocket, meta?: { remoteAddress?: string; kind?: string });
}

/**
 * Hands one session to a server as a client: waits for its control stream,
 * wraps it as a WtSocket and attaches it the way a WebSocket is — sessions,
 * declared headers and flow control apply. Null when refused (`verify`, or
 * no control stream within `acceptTimeout`).
 */
export declare function attachSession(
  server: Server | RpcServer,
  session: WtSession,
  options?: AttachSessionOptions,
): Promise<Client | null>;

export interface AcceptSessionsOptions extends Omit<AttachSessionOptions, keyof SessionMeta | 'signal'> {
  /** Reads the CONNECT request off each session (fromFails by default). */
  meta?: (session: WtSession) => SessionMeta | Promise<SessionMeta>;
  /** Sessions are attached concurrently: this may be called out of arrival order. */
  onClient?: (client: Client, session: WtSession) => void;
  /**
   * Hears a session that failed to attach, or — with `session` null — the
   * source itself failing, which ends the loop. Defaults to the server's
   * log (`wt.attach` / `wt.source`); one that throws is logged
   * (`wt.onError`), never a rejection of `done`.
   */
  onError?: (error: Error, session: WtSession | null) => void;
  /** Sessions held in their handshake at once (default 256); the next one is refused 503, logged `wt.accept.saturated` once per episode. */
  maxPending?: number;
}

export interface SessionAcceptor {
  /** Ends the loop, cancels the stream, and closes what is still handshaking (1001). */
  stop(): Promise<void>;
  /** Settles once the loop has ended and every pending attach with it. */
  readonly done: Promise<void>;
}

/**
 * Attaches every session a host hands out — a `sessionStream(path)` from
 * `@fails-components/webtransport`, or any (async) iterable of sessions.
 */
export declare function acceptSessions(
  server: Server | RpcServer,
  sessions: ReadableStream<WtSession> | AsyncIterable<WtSession> | Iterable<WtSession>,
  options?: AcceptSessionsOptions,
): SessionAcceptor;

/** The CONNECT request off a `@fails-components/webtransport` session (`header`, `peerAddress`). */
export declare function fromFails(session: unknown): Required<SessionMeta>;

/**
 * The request callback a `@fails-components/webtransport` server needs
 * (`h3.setRequestCallback(failsRequestCallback)`): routes the session by
 * pathname — `sessionStream(path)` matches the request path literally, and a
 * wrpc client declares its headers in the query — keeping `:path` intact.
 */
export declare function failsRequestCallback(args: { header: Record<string, string> }): Promise<{ path: string } & Record<string, unknown>>;

/** What quico's request handler receives for a WebTransport CONNECT. */
export interface QuicoRequest {
  headers: Record<string, string>;
  url?: string | null;
  on(event: string, listener: (...args: Array<any>) => void): unknown;
}
export interface QuicoResponse {
  writeHead(status: number, headers?: Record<string, string>): unknown;
  end(chunk?: unknown): unknown;
  sendDatagram(data: Uint8Array): unknown;
  createBidirectionalStream(): unknown;
  createUnidirectionalStream?(): unknown;
}

/**
 * quico's http-shaped WebTransport request as a session plus its CONNECT
 * request: `const { session, ...meta } = fromQuico(req, res);
 * attachSession(server, session, meta)`. Accepts the request (`writeHead(200)`)
 * unless `accept: false`.
 */
export declare function fromQuico(
  req: QuicoRequest,
  res: QuicoResponse,
  options?: { accept?: boolean; maxDatagramSize?: number },
): { session: WtSession } & Required<SessionMeta>;

// ---------------------------------------------------------------------------
// Stream framing (docs/reference/wire-format.md)

export declare const HEADER_BYTES: 5;
export declare const KIND_TEXT: 0;
export declare const KIND_BINARY: 1;
/** The capabilities message each end sends first on the control stream. */
export declare const KIND_CAPS: 2;
/** A packet (3) or a chunk (4) compressed with the sender's negotiated codec — accepted only once the two lists share one. */
export declare const KIND_TEXT_COMPRESSED: 3;
export declare const KIND_BINARY_COMPRESSED: 4;
export declare const DEFAULT_MAX_MESSAGE: number;
export declare const INLINE_TEXT: number;
export declare const DEFAULT_ACCEPT_TIMEOUT: number;
/** The default `maxPending` of `acceptSessions` (256): sessions attaching at once before the next is refused 503. */
export declare const DEFAULT_MAX_PENDING: number;
export declare const DEFAULT_HIGH_WATER_MARK: number;
export declare const DEFAULT_LOW_WATER_MARK: number;

export declare class FramingError extends Error {
  name: 'FramingError';
  code: 'kind' | 'too-large' | 'utf8';
}

export declare function toBytes(input: ArrayBuffer | ArrayBufferView): Uint8Array;
/** One message: a fresh frame, header included. */
export declare function frame(kind: 0 | 1 | 2 | 3 | 4, bytes: Uint8Array): Uint8Array;
/** A capabilities message: UTF-8 JSON under a KIND 2 header. */
export declare function frameCaps(text: string): Uint8Array;
/** A packet's bytes as text — the parser's own decode, for a packet inflated after parsing; throws FramingError on invalid UTF-8. */
export declare function decodeText(bytes: Uint8Array): string;
/** The object a capabilities message carries, or null when it is not one. */
export declare function parseCaps(text: string): Record<string, unknown> | null;
/** A packet: UTF-8 under a KIND 0 header. */
export declare function frameText(text: string): Uint8Array;

export declare const DATAGRAM_HEADER_BYTES: 1;
/** A packet as one datagram: `[KIND_TEXT][UTF-8]`. */
export declare function datagramText(text: string): Uint8Array;
/** The packet a datagram carries, or null when it carries anything else. */
export declare function parseDatagram(input: ArrayBuffer | ArrayBufferView): string | null;
/** A writer on the session's datagrams — `createWritable()` where present, the legacy `writable` otherwise; null without datagrams. */
export declare function datagramWriter(datagrams: WtDatagrams | null | undefined): WritableStreamDefaultWriter<Uint8Array> | null;

/**
 * The receiving half: feed it what the stream's reader yields, in order; it
 * calls `onMessage` once per completed message. Throws FramingError on a
 * malformed header and resets.
 */
export declare class StreamParser {
  constructor(options: { maxMessage?: number; onMessage: (kind: 0 | 1 | 2 | 3 | 4, data: string | Uint8Array) => void });
  /** Whether the compressed kinds (3, 4) are accepted; a protocol error until the transport sets it. */
  compressed: boolean;
  readonly pending: number;
  push(input: ArrayBuffer | ArrayBufferView): void;
  reset(): void;
}
