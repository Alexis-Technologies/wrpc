// Types for `src/errors.js`: the classes wrpc throws for a mistake of the
// program that calls it, and the framing error both datagram carriers share.
// Re-exported by `src/client.d.ts` (ConfigError, ArgumentError) and by the
// WebTransport and WebRTC typings (FramingError).

/**
 * The string `error.code`s wrpc throws, each a table row in
 * `docs/reference/errors.md`. A numeric `code` is something else: the wire
 * status of a `WrpcError` or of an error a handler or hook throws.
 */
export type ErrorCode = 'ALX_ERR_CONFIG' | 'ALX_ERR_ARGUMENT' | 'ALX_ERR_CLI';

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

/** What a peer's frame did wrong, on WebTransport (`kind`, `too-large`, `utf8`) or WebRTC (all of them). */
export type FramingErrorCode =
  | 'empty'
  | 'reserved'
  | 'kind'
  | 'too-large'
  | 'fragments'
  | 'utf8'
  | 'message-size'
  | 'inflate';

/** A peer's frame that breaks the framing of a WebTransport stream or a WebRTC data channel: one class for both. */
export declare class FramingError extends Error {
  name: 'FramingError';
  code: FramingErrorCode;
  constructor(message: string, code: FramingErrorCode);
}
