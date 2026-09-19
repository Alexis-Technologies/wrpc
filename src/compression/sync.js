'use strict';

// The Node-only helpers over the shared seam (index.js), for the carriers
// that are Node↔Node by construction — the broker binding and the rooms
// backplane / cluster envelopes. Two things set them apart from a socket
// transport: the codec must answer synchronously (a backplane handler and
// a session's frame sequence have no ordering queue to hide a promise
// behind, and every Node codec is synchronous anyway), and a backplane
// carries STRINGS, so a compressed envelope rides as base64 under a marker
// a receiver can tell from JSON at a glance.

const { normalizeCompression, negotiate, isPromise } = require('./index.js');

// The inflated-size cap, the same 16 MiB the WebTransport and WebRTC
// carriers bound one message at.
const DEFAULT_MAX_MESSAGE = 16 * 1024 * 1024;
const EMPTY = new Uint8Array(0);

/**
 * `normalizeCompression`, then two checks at construction rather than at
 * the first message: a codec that declares `async` (the built-in ones
 * with that option — they answer synchronously on a small probe and a
 * promise past the threshold), and a probe for one that answers a promise
 * outright.
 */
const normalizeSyncCompression = (value, name) => {
  const normalized = normalizeCompression(value, name);
  if (normalized === null) return null;
  if (normalized.codec.async !== undefined && normalized.codec.async !== null) {
    throw new TypeError(`${name}: compression.codec declares async — this carrier has no ordering queue for a promise`);
  }
  if (isPromise(normalized.codec.encode(EMPTY))) {
    throw new TypeError(`${name}: compression.codec must answer synchronously on this carrier`);
  }
  return normalized;
};

const maxMessageOf = (value, name) => {
  if (value === undefined || value === null) return DEFAULT_MAX_MESSAGE;
  if (!(Number.isInteger(value) && value > 0)) throw new TypeError(`${name}: maxMessage must be a positive integer`);
  return value;
};

const asBuffer = (bytes) =>
  Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);

/**
 * `body` (text or bytes) compressed with `active`, or null when it is under
 * the threshold, the codec failed, or the output would not be smaller — the
 * caller then sends it plain, unmarked.
 */
const encodeIfSmaller = (active, body) => {
  const bytes = typeof body === 'string' ? Buffer.from(body) : body;
  if (bytes.length < active.threshold) return null;
  let out;
  try {
    out = active.codec.encode(bytes);
  } catch {
    return null;
  }
  return out.length < bytes.length ? out : null;
};

/** The inflated bytes, or null when the codec refused them (the cap included). */
const decodeOrNull = (active, bytes, maxMessage) => {
  try {
    return active.codec.decode(bytes, maxMessage);
  } catch {
    return null;
  }
};

// A backplane message is a string. A compressed envelope is the JSON text
// (signed already, when the cluster signs) deflated and base64'd under
// `wrpc-enc:<id>:` — JSON never starts with a `w`, so the first character
// tells the two apart, and a receiver that names another codec, or none,
// knows it holds something it cannot read rather than garbage.
const ENVELOPE_PREFIX = 'wrpc-enc:';

const isEncodedEnvelope = (message) => typeof message === 'string' && message.startsWith(ENVELOPE_PREFIX);

/**
 * `{ id, encode(text) -> text, decode(text) -> text | null }` over the
 * option, or null for off. `encode` leaves a message under the threshold
 * (or one the codec would not shrink) as it is; `decode` answers a plain
 * message unchanged, an encoded one inflated, and null for one it cannot
 * read (another codec's, a body that does not inflate under `maxMessage`).
 */
const createEnvelopeCodec = (option, name, maxMessage = DEFAULT_MAX_MESSAGE) => {
  const active = normalizeSyncCompression(option, name);
  if (active === null) return null;
  const marker = `${ENVELOPE_PREFIX}${active.id}:`;
  return {
    id: active.id,
    encode(text) {
      const out = encodeIfSmaller(active, text);
      return out === null ? text : marker + asBuffer(out).toString('base64');
    },
    decode(message) {
      if (!message.startsWith(ENVELOPE_PREFIX)) return message;
      if (!message.startsWith(marker)) return null;
      const out = decodeOrNull(active, Buffer.from(message.slice(marker.length), 'base64'), maxMessage);
      return out === null ? null : asBuffer(out).toString();
    },
  };
};

module.exports = {
  DEFAULT_MAX_MESSAGE,
  ENVELOPE_PREFIX,
  normalizeSyncCompression,
  negotiate,
  maxMessageOf,
  encodeIfSmaller,
  decodeOrNull,
  isEncodedEnvelope,
  createEnvelopeCodec,
};
