'use strict';

// The errors wrpc's entries share, coded as every Alexis library codes them
// (`ALX_ERR_*`). Every class takes `(message, options = {})`, as `Error`
// does: `options` holds `cause` and the class's own fields, and goes to
// `super` whole, so `cause` is set only when given. Require-free: every
// entry, browser ones included, throws them.

// An option, a definition (a router, a procedure, a binding table) or an
// injected host that is missing or invalid: a mistake of the setup code.
// Extends TypeError, which is what wrpc threw for it before 2.3.
class ConfigError extends TypeError {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'ConfigError';
    this.code = 'ALX_ERR_CONFIG';
  }
}

// An argument of a call made while running (`createStream(name, size)`,
// `setHeader`, `sendTo`, a peer's arguments to a signaling procedure).
class ArgumentError extends TypeError {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'ArgumentError';
    this.code = 'ALX_ERR_ARGUMENT';
  }
}

// A peer's frame that breaks the framing of a WebTransport stream or a
// WebRTC data channel. One class for both carriers; `options.code` says what
// the frame did wrong (`ALX_ERR_FRAMING_*`).
class FramingError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'FramingError';
    this.code = options.code;
  }
}

// A carrier that gave up on its own side: a WebTransport session or a WebRTC
// channel past `maxBackpressure` (`ALX_ERR_BACKPRESSURE`), a data channel
// whose send threw (`ALX_ERR_CHANNEL_SEND`; `ALX_ERR_CHANNEL_DESYNC` when it
// threw mid-message). `options.code` says which.
class TransportError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'TransportError';
    this.code = options.code;
  }
}

// An error with a numeric wire status, the one class for it on both ends:
// what a call rejects with on the client (the server's answer, or the
// client's own 408/499/503), and what wrpc — or a handler or a hook — throws
// on the server to answer a peer with that status. `details` is the optional
// wire field, `expose` lets a 5xx's message and details travel
// (`publicErrorMessage` in src/rpc/errors.js); both are set only when given.
class WrpcError extends Error {
  constructor(message, options = {}) {
    const { code = 500, details, expose } = options;
    super(message, options);
    this.name = 'WrpcError';
    this.code = code;
    if (details !== undefined) this.details = details;
    if (expose !== undefined) this.expose = expose;
  }
}

module.exports = { ConfigError, ArgumentError, FramingError, TransportError, WrpcError };
