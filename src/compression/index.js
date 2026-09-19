'use strict';

// Per-message compression for the transports that have nothing under them
// — a WebRTC data channel (SCTP over DTLS) and a WebTransport stream (QUIC)
// carry exactly the bytes handed to them. One message, one deflate: no
// context across messages, so a peer holds no window per connection and
// the frames of a fan-out stay shareable.
//
// Off by default, like every compression knob in wrpc. When on, both ends
// announce the codec's `id` — in the WebTransport capabilities message, in
// the WebRTC description signal — and compress only once the peer named
// the same one; a peer that announced nothing gets plain frames, as before.
//
// The codec is a structural seam, checked by `isCompressor`: the platform
// default (native.js — node:zlib on Node, CompressionStream in a browser,
// swapped through package.json#browser) or an injected one — the
// dictionary codec of @alexify/wrpc/deflate, or anything else that answers
// `encode(bytes)` and `decode(bytes, maxOutput)`. Either may answer a
// promise: CompressionStream can only, and the Sequencer below keeps
// messages in order around that.
//
// Browser-budgeted: this file lands in the main and webrtc browser entries
// through the client transports. Manual checks, no spread on a hot path.

const { nativeCompressor } = require('./native.js');

const DEFAULT_THRESHOLD = 1024;
// A dictionary codec's id: the prefix plus `dictionaryId` of the bytes —
// shared by the Node zlib codec and the pure-JS one so the two negotiate.
const DICTIONARY_ID_PREFIX = 'deflate-raw+dict:';

const isPromise = (value) => value !== null && typeof value === 'object' && typeof value.then === 'function';

/**
 * `{ id, encode(bytes) -> bytes | Promise<bytes>, decode(bytes, maxOutput)
 * -> bytes | Promise<bytes>, threshold? }`. `id` is what the two ends
 * compare: a peer compresses only for a peer that named the same codec.
 */
const isCompressor = (value) =>
  typeof value === 'object' &&
  value !== null &&
  typeof value.encode === 'function' &&
  typeof value.decode === 'function' &&
  typeof value.id === 'string' &&
  value.id.length > 0;

/**
 * `compression: true | { codec, threshold }` into the frozen shape the
 * transports read — `{ codec, id, threshold }` — or null for off, which is
 * also what a platform with no native codec and nothing injected gets.
 * Strict on what IS given: a bad codec or threshold is a TypeError at
 * construction, not a message that quietly went out plain.
 */
// The size from which the Node platform codec hands a message to zlib's
// threadpool when `async` is on: the hand-off costs a fixed ~20 µs per
// call and is level with the synchronous deflate at ~256 KB
// (bench/zlib-async.js) — the same default as permessage-deflate's and the
// HTTP encoder's `async`.
const DEFAULT_ASYNC_THRESHOLD = 256 * 1024;

/**
 * `async: true | { threshold }` into the byte threshold, `null` for off in
 * every spelling of off. Shared by `compression.async` and the dictionary
 * codec's own option.
 */
const normalizeAsync = (value, name) => {
  if (value === undefined || value === null || value === false) return null;
  const options = value === true ? {} : value;
  if (typeof options !== 'object' || Array.isArray(options)) {
    throw new TypeError(`${name}: async must be true, false or { threshold }`);
  }
  const threshold = options.threshold ?? DEFAULT_ASYNC_THRESHOLD;
  if (!(Number.isInteger(threshold) && threshold > 0)) {
    throw new TypeError(`${name}: async.threshold must be a positive integer`);
  }
  return threshold;
};

const normalizeCompression = (value, name) => {
  if (value === undefined || value === null || value === false) return null;
  const options = value === true ? {} : value;
  if (typeof options !== 'object' || Array.isArray(options)) {
    throw new TypeError(`${name}: compression must be true, false or an options object`);
  }
  let codec = options.codec ?? null;
  if (codec !== null && !isCompressor(codec)) {
    throw new TypeError(`${name}: compression.codec must provide an id, encode(bytes) and decode(bytes, maxOutput)`);
  }
  // `async` shapes the platform codec; an injected codec decides that for
  // itself (the dictionary codec takes the same option on its factory).
  const async = normalizeAsync(options.async, `${name}: compression`);
  if (codec === null) codec = nativeCompressor({ async });
  else if (async !== null) {
    throw new TypeError(`${name}: compression.async applies to the platform codec — set it on the codec's factory`);
  }
  if (codec === null) return null;
  const threshold = options.threshold ?? codec.threshold ?? DEFAULT_THRESHOLD;
  if (!(Number.isInteger(threshold) && threshold >= 0)) {
    throw new TypeError(`${name}: compression.threshold must be a non-negative integer`);
  }
  return Object.freeze({ codec, id: codec.id, threshold });
};

/**
 * A short, deterministic id for dictionary bytes — FNV-1a over the bytes,
 * 64 bits as 16 hex characters — what a dictionary codec's `id` carries so
 * two ends compress against the same bytes or not at all. A fingerprint
 * for negotiation, not a security property; it runs in a browser too,
 * where a hash from crypto.subtle would be asynchronous.
 */
const dictionaryId = (bytes) => {
  let h1 = 0x811c9dc5;
  let h2 = 0x01000193 ^ bytes.length;
  for (let i = 0; i < bytes.length; i++) {
    h1 = Math.imul(h1 ^ bytes[i], 0x01000193) >>> 0;
    h2 = Math.imul(h2 ^ bytes[i], 0x0100019d) >>> 0;
  }
  return h1.toString(16).padStart(8, '0') + h2.toString(16).padStart(8, '0');
};

/** The local setting when the peer announced the same codec, null otherwise. */
const negotiate = (local, peerId) =>
  local !== null && typeof peerId === 'string' && peerId === local.id ? local : null;

/**
 * Keeps messages in order around a codec that may answer asynchronously.
 * `push(value, deliver, recover)`: `value` is the message, or a promise of
 * it; `deliver` runs with the settled value in push order; `recover` runs
 * with the error when the promise rejected (send it plain, hang up). A
 * push with nothing in flight and a plain value delivers synchronously —
 * the common path costs no promise — and once something is in flight
 * every later push waits behind it, plain or not, which is what keeps the
 * wire ordered. Errors thrown by deliver/recover go to `onError`.
 */
class Sequencer {
  #tail = null;
  #pending = 0;
  #onError;

  constructor(onError = null) {
    this.#onError = onError;
  }

  /** Pushes waiting on an earlier one; 0 when nothing is in flight. */
  get pending() {
    return this.#pending;
  }

  push(value, deliver, recover) {
    const async = isPromise(value);
    if (this.#tail === null && !async) return void deliver(value);
    this.#pending++;
    const run = () => (async ? value.then(deliver, recover) : deliver(value));
    const previous = this.#tail;
    const step = previous === null ? new Promise((resolve) => resolve(run())) : previous.then(run, run);
    const settled = () => {
      this.#pending--;
      if (this.#tail === done) this.#tail = null;
    };
    const done = step.then(settled, (error) => {
      settled();
      if (this.#onError) this.#onError(error);
    });
    this.#tail = done;
  }
}

module.exports = {
  DEFAULT_THRESHOLD,
  DEFAULT_ASYNC_THRESHOLD,
  DICTIONARY_ID_PREFIX,
  normalizeAsync,
  isCompressor,
  isPromise,
  normalizeCompression,
  negotiate,
  nativeCompressor,
  dictionaryId,
  Sequencer,
};
