'use strict';

// The Node-only helpers over the shared seam (index.js), for the carriers
// that are Node↔Node by construction — the broker binding and the rooms
// backplane / cluster envelopes. Two things set them apart from a socket
// transport: the codec must answer synchronously (a backplane handler and
// a session's frame sequence have no ordering queue to hide a promise
// behind, and every Node codec is synchronous anyway), and a backplane
// carries STRINGS, so a compressed envelope rides as base64 under a marker
// a receiver can tell from JSON at a glance.

const { normalizeCompression, negotiate, codecById, isPromise } = require('./index.js');

// The inflated-size cap, the same 16 MiB the WebTransport and WebRTC
// carriers bound one message at.
const DEFAULT_MAX_MESSAGE = 16 * 1024 * 1024;
const EMPTY = new Uint8Array(0);

/**
 * `normalizeCompression`, then two checks at construction rather than at
 * the first message, on EVERY codec of the list — any of them may be the
 * one a peer's list selects: a codec that declares `async` (the built-in
 * ones with that option — they answer synchronously on a small probe and a
 * promise past the threshold), and a probe for one that answers a promise
 * outright.
 */
const normalizeSyncCompression = (value, name) => {
  const normalized = normalizeCompression(value, name);
  if (normalized === null) return null;
  for (const { codec } of normalized.codecs) {
    if (codec.async !== undefined && codec.async !== null) {
      throw new TypeError(
        `${name}: compression.codec declares async — this carrier has no ordering queue for a promise`,
      );
    }
    if (isPromise(codec.encode(EMPTY))) {
      throw new TypeError(`${name}: compression.codec must answer synchronously on this carrier`);
    }
  }
  return normalized;
};

/**
 * `negotiate` for a carrier whose list rides a header — the broker
 * binding's `wrpc-enc`: ids joined by commas (an id holds none,
 * normalizeCompression sees to that). A request names its list on every
 * message, so the last header seen is remembered: a fleet of clients on
 * one configuration costs one split, not one per request.
 */
const headerNegotiator = (local) => {
  let lastHeader = null;
  let lastAgreed = null;
  return (header) => {
    if (local === null || typeof header !== 'string' || header.length === 0) return null;
    if (header === lastHeader) return lastAgreed;
    lastHeader = header;
    lastAgreed = negotiate(local, header.split(','));
    return lastAgreed;
  };
};

const maxMessageOf = (value, name) => {
  if (value === undefined || value === null) return DEFAULT_MAX_MESSAGE;
  if (!(Number.isInteger(value) && value > 0)) throw new TypeError(`${name}: maxMessage must be a positive integer`);
  return value;
};

const asBuffer = (bytes) =>
  Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);

/**
 * `body` (text or bytes) compressed with `active` — one entry of the list,
 * `{ codec, id, threshold }` — or null when it is under the threshold, the
 * codec failed, or the output would not be smaller: the caller then sends
 * it plain, unmarked. `onError(error, active)` hears the failure — the only
 * one of the three that is not a decision (cold: called from the catch
 * alone, so one receiver serves every codec of a list).
 */
const encodeIfSmaller = (active, body, onError = null) => {
  const bytes = typeof body === 'string' ? Buffer.from(body) : body;
  if (bytes.length < active.threshold) return null;
  let out;
  try {
    out = active.codec.encode(bytes);
  } catch (error) {
    if (onError !== null) onError(error, active);
    return null;
  }
  return out.length < bytes.length ? out : null;
};

/**
 * The inflated bytes, or null when the codec refused them (the cap
 * included); `onError(error, active)` hears why — `ERR_BUFFER_TOO_LARGE` is
 * the cap, anything else the bytes.
 */
const decodeOrNull = (active, bytes, maxMessage, onError = null) => {
  try {
    return active.codec.decode(bytes, maxMessage);
  } catch (error) {
    if (onError !== null) onError(error, active);
    return null;
  }
};

// A codec that fails loses nothing: the message leaves plain, or a frame is
// refused and answered. Which is why nothing used to notice — a dictionary
// that stopped matching, a codec out of memory, and the traffic is several
// times what it was with not a line or a point to show for it. So every
// failure is COUNTED, and said once per (carrier, direction, codec) for the
// life of the reporter — one line a codec, not one a message.
const createFailureReporter = (log, otel = null) => {
  const said = new Set();
  return (carrier, direction, codec, error) => {
    otel?.recordCompressionFailure(carrier, direction);
    const key = `${carrier}\0${direction}\0${codec}`;
    if (said.has(key)) return;
    said.add(key);
    log.warn({ event: 'compression.failed', carrier, direction, codec, code: error?.code, err: error });
  };
};

// A backplane message is a string. A compressed envelope is the JSON text
// (signed already, when the cluster signs) deflated and base64'd under
// `wrpc-enc:<id>:` — JSON never starts with a `w`, so the first character
// tells the two apart, and a receiver that holds no such codec, or none,
// knows it holds something it cannot read rather than garbage.
//
// Nothing is negotiated on a backplane, so the list means something else
// here: an instance ENCODES with the head of its list and DECODES any
// codec on it — the marker names which. That is what makes a change of
// codec a rollout without a lost message: first every instance lists both
// (`['deflate-raw', 'zstd']`), then the order is swapped.
const ENVELOPE_PREFIX = 'wrpc-enc:';

const isEncodedEnvelope = (message) => typeof message === 'string' && message.startsWith(ENVELOPE_PREFIX);

/**
 * `{ id, ids, encode(text) -> text, decode(text) -> text | null }` over the
 * option, or null for off. `encode` leaves a message under the threshold
 * (or one the codec would not shrink) as it is; `decode` answers a plain
 * message unchanged, an encoded one inflated, and null for one it cannot
 * read (a codec not on the list, a body that does not inflate under
 * `maxMessage`).
 */
const createEnvelopeCodec = (option, name, maxMessage = DEFAULT_MAX_MESSAGE, failed = null) => {
  const local = normalizeSyncCompression(option, name);
  if (local === null) return null;
  const head = local.codecs[0];
  // `failed(direction, codec id, error)`, when the caller wants to hear:
  // one receiver per codec and direction, built here — never per message.
  const hear = (direction, id) => (failed === null ? null : (error) => failed(direction, id, error));
  const encodeFailed = hear('encode', head.id);
  // An id may hold a colon (`deflate-raw+dict:<hash>`), so a marker is
  // matched whole, never parsed — longest first, so one id that is a prefix
  // of another cannot claim its envelopes.
  const markers = local.codecs
    .map((entry) => ({ entry, marker: `${ENVELOPE_PREFIX}${entry.id}:`, failed: hear('decode', entry.id) }))
    .sort((a, b) => b.marker.length - a.marker.length);
  const headMarker = `${ENVELOPE_PREFIX}${head.id}:`;
  return {
    id: head.id,
    ids: local.ids,
    encode(text) {
      const out = encodeIfSmaller(head, text, encodeFailed);
      return out === null ? text : headMarker + asBuffer(out).toString('base64');
    },
    decode(message) {
      if (!message.startsWith(ENVELOPE_PREFIX)) return message;
      for (let i = 0; i < markers.length; i++) {
        const { entry, marker, failed: decodeFailed } = markers[i];
        if (!message.startsWith(marker)) continue;
        const out = decodeOrNull(entry, Buffer.from(message.slice(marker.length), 'base64'), maxMessage, decodeFailed);
        return out === null ? null : asBuffer(out).toString();
      }
      return null;
    },
  };
};

module.exports = {
  DEFAULT_MAX_MESSAGE,
  ENVELOPE_PREFIX,
  normalizeSyncCompression,
  negotiate,
  codecById,
  headerNegotiator,
  maxMessageOf,
  encodeIfSmaller,
  decodeOrNull,
  createFailureReporter,
  isEncodedEnvelope,
  createEnvelopeCodec,
};
