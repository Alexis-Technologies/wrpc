// Types for `src/errors.js`: the classes wrpc's entries share. Every one
// takes `(message, options)`, as `Error` does: `options` holds `cause` and the
// class's own fields. Re-exported by `src/client.d.ts` (ConfigError,
// ArgumentError, WrpcError) and by the WebTransport and WebRTC typings
// (FramingError, TransportError).

/**
 * The string `error.code`s wrpc throws, each a table row in
 * `docs/reference/errors.md`. A numeric `code` is something else: the wire
 * status of a `WrpcError` or of an error a handler or hook throws.
 */
export type ErrorCode =
  | 'ALX_ERR_CONFIG'
  | 'ALX_ERR_ARGUMENT'
  | 'ALX_ERR_CLI'
  | 'ALX_ERR_FRAMING_EMPTY'
  | 'ALX_ERR_FRAMING_RESERVED'
  | 'ALX_ERR_FRAMING_KIND'
  | 'ALX_ERR_FRAMING_TOO_LARGE'
  | 'ALX_ERR_FRAMING_FRAGMENTS'
  | 'ALX_ERR_FRAMING_UTF8'
  | 'ALX_ERR_FRAMING_MESSAGE_SIZE'
  | 'ALX_ERR_FRAMING_INFLATE'
  | 'ALX_ERR_BACKPRESSURE'
  | 'ALX_ERR_CHANNEL_SEND'
  | 'ALX_ERR_CHANNEL_DESYNC'
  | 'ALX_ERR_DEFLATE_TRUNCATED'
  | 'ALX_ERR_DEFLATE_HUFFMAN'
  | 'ALX_ERR_DEFLATE_STORED'
  | 'ALX_ERR_DEFLATE_DISTANCE'
  | 'ALX_ERR_DEFLATE_BLOCK'
  | 'ALX_ERR_DEFLATE_TOO_LARGE'
  | 'ALX_ERR_PARSE_MESSAGE_TOO_BIG'
  | 'ALX_ERR_PARSE_PROTOCOL'
  | 'ALX_ERR_PARSE_RSV'
  | 'ALX_ERR_PARSE_CONTROL_TOO_LONG'
  | 'ALX_ERR_PARSE_INVALID_PAYLOAD'
  | 'ALX_ERR_ASSERTION_MALFORMED'
  | 'ALX_ERR_ASSERTION_TYP'
  | 'ALX_ERR_ASSERTION_ALG'
  | 'ALX_ERR_ASSERTION_KID'
  | 'ALX_ERR_ASSERTION_SIGNATURE'
  | 'ALX_ERR_ASSERTION_SUBJECT'
  | 'ALX_ERR_ASSERTION_FINGERPRINT'
  | 'ALX_ERR_ASSERTION_EXPIRED'
  | 'ALX_ERR_ASSERTION_ISSUER'
  | 'ALX_ERR_ASSERTION_MISSING'
  | 'ALX_ERR_OPEN'
  | 'ALX_ERR_ENCRYPTION_REFUSED';

/**
 * An option, a definition (a router, a procedure, a binding table) or an
 * injected host that is missing or invalid. A `TypeError`, as before 2.3.
 */
export declare class ConfigError extends TypeError {
  readonly name: 'ConfigError';
  readonly code: 'ALX_ERR_CONFIG';
  constructor(message?: string, options?: ErrorOptions);
}

/**
 * An argument of a call made while running — `createStream(name, size)`,
 * `setHeader`, `sendTo`, the arguments a peer gave a signaling procedure.
 * A `TypeError`, as before 2.3.
 */
export declare class ArgumentError extends TypeError {
  readonly name: 'ArgumentError';
  readonly code: 'ALX_ERR_ARGUMENT';
  constructor(message?: string, options?: ErrorOptions);
}

/** What a peer's frame did wrong, on WebTransport (`KIND`, `TOO_LARGE`, `UTF8`) or WebRTC (all of them). */
export type FramingErrorCode =
  | 'ALX_ERR_FRAMING_EMPTY'
  | 'ALX_ERR_FRAMING_RESERVED'
  | 'ALX_ERR_FRAMING_KIND'
  | 'ALX_ERR_FRAMING_TOO_LARGE'
  | 'ALX_ERR_FRAMING_FRAGMENTS'
  | 'ALX_ERR_FRAMING_UTF8'
  | 'ALX_ERR_FRAMING_MESSAGE_SIZE'
  | 'ALX_ERR_FRAMING_INFLATE';

/** A peer's frame that breaks the framing of a WebTransport stream or a WebRTC data channel: one class for both. */
export declare class FramingError extends Error {
  readonly name: 'FramingError';
  readonly code: FramingErrorCode;
  constructor(message: string, options: ErrorOptions & { code: FramingErrorCode });
}

/** Why a carrier gave up on its own side. */
export type TransportErrorCode = 'ALX_ERR_BACKPRESSURE' | 'ALX_ERR_CHANNEL_SEND' | 'ALX_ERR_CHANNEL_DESYNC';

/**
 * A WebTransport session or a WebRTC channel past `maxBackpressure`
 * (`ALX_ERR_BACKPRESSURE`), or a data channel whose send threw
 * (`ALX_ERR_CHANNEL_SEND`; `ALX_ERR_CHANNEL_DESYNC` mid-message, which closes
 * it). The `cause` is the platform's own error, when there was one.
 */
export declare class TransportError extends Error {
  readonly name: 'TransportError';
  readonly code: TransportErrorCode;
  constructor(message: string, options: ErrorOptions & { code: TransportErrorCode });
}

/** What `new WrpcError(message, options)` takes besides `cause`. */
export interface WrpcErrorOptions extends ErrorOptions {
  /** The wire status: an HTTP status number. 500 when left out. */
  code?: number;
  /** Structured issue lists and the like; an optional wire field. */
  details?: unknown;
  /** Lets a 5xx's message and details travel to the peer, as a 4xx's always do. */
  expose?: boolean;
}

/**
 * An error with a numeric wire status, on both ends: what a call rejects
 * with on the client (the server's answer, or the client's own 408, 499,
 * 503), and what a handler or a hook throws on the server to answer the
 * peer with that status — `throw new WrpcError('No such document', { code: 404 })`.
 */
export declare class WrpcError extends Error {
  readonly name: 'WrpcError';
  code: number;
  /** Structured issue lists the server attached; an optional wire field. */
  details?: unknown;
  expose?: boolean;
  constructor(message?: string, options?: WrpcErrorOptions);
}
