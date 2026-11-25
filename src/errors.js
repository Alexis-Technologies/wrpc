'use strict';

// The errors wrpc throws for a mistake of the program that calls it, coded
// as every Alexis library codes them (`ALX_ERR_*`). Both extend TypeError,
// which is what wrpc threw for them before, so `instanceof TypeError` keeps
// working. Require-free: every entry, browser ones included, throws them.

// An option, a definition (a router, a procedure, a binding table) or an
// injected host that is missing or invalid: a mistake of the setup code.
class ConfigError extends TypeError {
  constructor(message, options) {
    super(message, options);
    this.name = 'ConfigError';
    this.code = 'ALX_ERR_CONFIG';
  }
}

// An argument of a call made while running (`createStream(name, size)`,
// `setHeader`, `sendTo`, a peer's arguments to a signaling procedure).
class ArgumentError extends TypeError {
  constructor(message, options) {
    super(message, options);
    this.name = 'ArgumentError';
    this.code = 'ALX_ERR_ARGUMENT';
  }
}

// A peer's frame that breaks the framing of a WebTransport stream or a
// WebRTC data channel. One class for both carriers; the codes are the
// framings' own ('truncated', 'utf8', 'message-size', …), kept as 2.x has
// them (CLAUDE.md, Conventions).
class FramingError extends Error {
  constructor(message, code) {
    super(message);
    this.name = 'FramingError';
    this.code = code;
  }
}

module.exports = { ConfigError, ArgumentError, FramingError };
