'use strict';

// A logger façade with no imports at all — that is what keeps this file
// browser-safe and what lets scripts/size.js bundle it without reaching for
// anything outside src/. It normalizes three shapes into one writer:
//
//   - a pino-like structured logger, called as (mergingObject, message)
//   - a Console-like object, called as (message)
//   - nothing at all, in which case every method is a no-op
//
// The writer that comes back always has the same shape, so no call site in
// the codebase ever has to branch on whether logging is configured.

// Marks a value as already being a writer. Every internal hand-off from a
// parent to a child component passes a writer where the public constructor
// documents a raw logger; the brand is what makes running it back through
// createLoggerWriter free instead of wrapping a wrapper.
const LOG_WRITER = Symbol.for('wrpc.logWriter');

const LEVELS = ['log', 'info', 'debug', 'warn', 'error'];

// Where each level goes when the sink does not implement it. Console always
// has all five; a pino at a raised level still has all five; a partial
// object handed over by a test may have one.
const FALLBACKS = {
  log: ['log', 'info', 'debug'],
  info: ['info', 'log', 'debug'],
  debug: ['debug', 'log', 'info'],
  warn: ['warn', 'error', 'log'],
  error: ['error', 'warn', 'log'],
};

const hasMethod = (value, name) => typeof value?.[name] === 'function';

// `child` (and pino's `level`) is what separates a structured logger, which
// takes (mergingObject, message), from a Console, which takes (message).
// Every structured logger worth injecting — pino, bunyan, winston, roarr —
// has one or both; Console has neither. Guessing "structured" wrong prints
// an object where a string belongs, so a sink that gives no signal is
// treated as a Console: the entry is dropped rather than shown raw.
const isStructured = (logger) => hasMethod(logger, 'child') || 'level' in logger;

const isLoggerLike = (logger) =>
  LEVELS.some((level) => hasMethod(logger, level)) || hasMethod(logger, 'table') || hasMethod(logger, 'group');

// Resolves a level to the first method the sink actually has, bound once at
// construction rather than looked up per call.
const bindLevel = (sink, level) => {
  for (const name of FALLBACKS[level]) {
    if (hasMethod(sink, name)) return sink[name].bind(sink);
  }
  return null;
};

const noop = () => {};

const DISABLED = Object.freeze({
  [LOG_WRITER]: true,
  enabled: false,
  // Returning `this` is what makes the per-connection and per-call children
  // free when logging is off: no allocation, no closure, no branch.
  child() {
    return this;
  },
  log: noop,
  info: noop,
  debug: noop,
  warn: noop,
  error: noop,
});

// Console takes a message, not an entry. An Error with no message of its own
// is passed through as the argument so the host prints its stack, which is
// what the plain `console.error(error)` calls this replaced used to do.
const createConsoleWriter = (sink) => {
  const writer = {
    [LOG_WRITER]: true,
    enabled: true,
    child() {
      return this;
    },
  };
  for (const level of LEVELS) {
    const method = bindLevel(sink, level);
    writer[level] = method
      ? (entry, message) => {
          // A logger that throws must never break the request path, so the
          // guard lives here — once — instead of at every call site.
          try {
            method(message ?? entry?.err ?? entry);
          } catch {}
        }
      : noop;
  }
  return writer;
};

const createStructuredWriter = (sink) => {
  const writer = {
    [LOG_WRITER]: true,
    enabled: true,
    child(bindings) {
      if (!hasMethod(sink, 'child')) return this;
      try {
        return createStructuredWriter(sink.child(bindings));
      } catch {
        return this;
      }
    },
  };
  for (const level of LEVELS) {
    const method = bindLevel(sink, level);
    writer[level] = method
      ? (entry, message) => {
          try {
            method(entry, message);
          } catch {}
        }
      : noop;
  }
  return writer;
};

/**
 * Normalizes a `logger` option into a writer.
 *
 * `false`/`null`/`undefined` disable logging outright; `true` logs to the
 * global console. An unrecognized object disables logging rather than
 * throwing — an observability option must never be the thing that stops a
 * server from booting.
 */
const createLoggerWriter = (logger) => {
  if (logger?.[LOG_WRITER] === true) return logger;
  if (!logger) return DISABLED;
  if (logger === true) return createConsoleWriter(globalThis.console);
  if (typeof logger !== 'object') return DISABLED;
  if (!isLoggerLike(logger)) return DISABLED;
  return isStructured(logger) ? createStructuredWriter(logger) : createConsoleWriter(logger);
};

module.exports = { createLoggerWriter, LOG_WRITER };
