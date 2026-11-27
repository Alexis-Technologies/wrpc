'use strict';

// The wire-error builders, shared by the dispatcher, the subscription pump
// and every ServerTransport. Split out of transport.js so the dispatcher's
// only dependency on that file is gone: transport.js carries the HTTP and
// WebSocket transports (Buffer, cookies), none of which a browser-side
// peer host needs, while these three functions are pure.

const { STATUS_CODES } = require('../status.js');

// A peer-controlled string on its way into a log line — a method, an id,
// a packet type: bounded before it becomes a record, because the line is
// the one place where "as long as maxPayload allows" is too long, and with
// nothing in it that ends or splits a line. A method named
// `x\n2026-11-16 ERROR admin logged in` used to be TWO lines of a console
// log, the second one the peer's own; a tab moved the columns of the line it
// was in. Every control character becomes a space (as `shown` does for a
// stream's name in src/streams.js). Anything that is not a string passes
// through as it is.
const CONTROL = /\p{Cc}|\u2028|\u2029/gu;

// The level of a line about something a PEER did and can do in a loop — a
// refusal, a malformed frame, an envelope that does not open: a warning the
// first time a key is seen, debug for `interval` after it. Whoever can send
// one can send it as fast as the link takes it, and a line per message made
// the log the thing that fell over. `max` bounds the keys where a peer has a
// say in them (a channel, a sender): past it the table starts over, which
// costs a repeated warning, never memory. One table per call.
const LOUD_INTERVAL = 10_000;

// What the peer is told. 4xx messages are written for the caller
// (validation, quotas, refusals) and travel as-is; a 5xx message is a server
// internal — an uncaught exception's text can carry paths, queries or stack
// fragments — so the peer gets the status line and the details stay in the
// server log, unless the error opts in with `expose = true` (which the
// router's own coded errors do: their messages are part of the protocol).
// The packet id is the correlation: the same id is on the server log line.
const publicErrorMessage = (code, error) => {
  const status = STATUS_CODES[code] || 'Unknown error';
  if (!error) return status;
  if (code < 500 || error.expose === true) return error.message;
  return status;
};

// `details` follows the exact same rule as the message: structured issue
// lists (validation paths, quota numbers) are part of the 4xx conversation,
// while a 5xx's internals stay in the log unless the error opts in.
const publicErrorDetails = (code, error) => {
  if (!error || error.details === undefined) return undefined;
  if (code < 500 || error.expose === true) return error.details;
  return undefined;
};

// The one builder for the wire error object, so every packet that carries
// an error ({type:'callback'} and {type:'end'} alike) redacts identically.
// The `details` key is omitted entirely when there is nothing to say —
// an optional field, absent rather than null, per the protocol's
// additive-fields rule.
const wireError = (code, error) => {
  const wire = { message: publicErrorMessage(code, error), code };
  const details = publicErrorDetails(code, error);
  if (details !== undefined) wire.details = details;
  return wire;
};
const clip = (text, max = 128) => {
  if (typeof text !== 'string') return text;
  return (text.length > max ? `${text.slice(0, max)}…` : text).replace(CONTROL, ' ');
};
const lineLevels = (interval = LOUD_INTERVAL, max = 0) => {
  const said = new Map();
  return (key) => {
    const now = Date.now();
    const last = said.get(key);
    if (last !== undefined && now - last < interval) return 'debug';
    if (max > 0 && last === undefined && said.size >= max) said.clear();
    said.set(key, now);
    return 'warn';
  };
};

module.exports = { LOUD_INTERVAL, publicErrorMessage, publicErrorDetails, wireError, clip, lineLevels };
