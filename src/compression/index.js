'use strict';

// Per-message compression for the transports that have nothing under them
// — a WebRTC data channel (SCTP over DTLS) and a WebTransport stream (QUIC)
// carry exactly the bytes handed to them. One message, one deflate: no
// context across messages, so a peer holds no window per connection and
// the frames of a fan-out stay shareable.
//
// Off by default, like every compression knob in wrpc. When on, each end
// announces the ids of the codecs it holds, in its order of preference —
// in the WebTransport capabilities message, in the WebRTC description
// signal, in a ws ping, in the broker's `wrpc-enc` header — and a sender
// compresses with the FIRST codec of ITS OWN list the peer announced. The
// receiver holds both lists, so it knows which one that is: no frame names
// its codec, the two directions choose independently (a Node server may
// answer in zstd a browser that sends deflate), and there is no tie to
// break. No codec in common, or a peer that announced nothing: plain
// frames, as before.
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

const { nativeCompressor, NATIVE_ID } = require('./native.js');

const { dictionaryId, DICTIONARY_ID_PREFIX, isPromise } = require('./ids.js');

const DEFAULT_THRESHOLD = 1024;

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
 * `compression: true | { codec, threshold, async }` into the frozen shape
 * the transports read — `{ codec, id, threshold }` — or null for off, which
 * is also what a platform with no native codec and nothing injected gets.
 * `codec` is a Compressor, or the name of a platform one ('deflate-raw',
 * 'brotli', 'zstd' — native.js). Strict on what IS given: a bad codec or
 * threshold is a TypeError at construction, not a message that quietly
 * went out plain.
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

// One entry of the list: a Compressor, or the name of a platform codec. A
// name the platform lacks (zstd on an older Node, a format this browser's
// CompressionStream has not) is skipped in a LIST — that is what a list is
// for — and refused when it is the only thing asked for (native.js).
const resolveCodec = (value, async, optional, name) => {
  if (typeof value === 'string') return nativeCompressor({ algorithm: value, async, optional });
  if (!isCompressor(value)) {
    throw new TypeError(
      `${name}: compression.codec must provide an id, encode(bytes) and decode(bytes, maxOutput), or name an algorithm`,
    );
  }
  // An id travels in a comma-separated header and a space-free marker.
  if (/[,\s]/.test(value.id)) {
    throw new TypeError(`${name}: compression.codec id must not contain a comma or whitespace`);
  }
  return value;
};

const normalizeCompression = (value, name) => {
  if (value === undefined || value === null || value === false) return null;
  const options = value === true ? {} : value;
  if (typeof options !== 'object' || Array.isArray(options)) {
    throw new TypeError(`${name}: compression must be true, false or an options object`);
  }
  // `async` shapes the platform codecs; an injected codec decides that for
  // itself (the codec factories take the same option).
  const async = normalizeAsync(options.async, `${name}: compression`);
  const asked = options.codec ?? null;
  const list = Array.isArray(asked);
  if (list && asked.length === 0) throw new TypeError(`${name}: compression.codec must not be an empty list`);
  const wanted = list ? asked : [asked ?? NATIVE_ID];
  if (async !== null && !wanted.some((entry) => typeof entry === 'string')) {
    throw new TypeError(`${name}: compression.async applies to the platform codec — set it on the codec's factory`);
  }
  const override = options.threshold;
  if (override !== undefined && !(Number.isInteger(override) && override >= 0)) {
    throw new TypeError(`${name}: compression.threshold must be a non-negative integer`);
  }
  const codecs = [];
  const ids = [];
  for (const entry of wanted) {
    const codec = resolveCodec(entry, async, list, name);
    if (codec === null) continue;
    if (ids.includes(codec.id)) throw new TypeError(`${name}: compression.codec names ${codec.id} twice`);
    const threshold = override ?? codec.threshold ?? DEFAULT_THRESHOLD;
    if (!(Number.isInteger(threshold) && threshold >= 0)) {
      throw new TypeError(`${name}: compression.threshold must be a non-negative integer`);
    }
    ids.push(codec.id);
    codecs.push(Object.freeze({ codec, id: codec.id, threshold }));
  }
  if (codecs.length === 0) return null;
  // `codec` / `id` / `threshold` are the head's — what a carrier with no
  // negotiation (a raw data channel, the backplane) encodes with.
  const head = codecs[0];
  return Object.freeze({
    codec: head.codec,
    id: head.id,
    threshold: head.threshold,
    codecs: Object.freeze(codecs),
    ids: Object.freeze(ids),
  });
};

// A peer's list is peer-controlled: only so many entries are looked at, and
// an id is only ever COMPARED with ours — never a key into anything.
const MAX_PEER_IDS = 16;

/** The entry of `local` named `id`, or null. A handful of codecs: a scan. */
const codecById = (local, id) => {
  const { codecs } = local;
  for (let i = 0; i < codecs.length; i++) if (codecs[i].id === id) return codecs[i];
  return null;
};

const announced = (peer, count, id) => {
  for (let i = 0; i < count; i++) if (peer[i] === id) return true;
  return false;
};

/**
 * What the two lists agree on, or null when they share nothing (or the
 * peer announced nothing): `{ encode, decode }`, each an entry of `local` —
 * `encode` the first of OUR codecs the peer announced, which is what we
 * send with; `decode` the first of THE PEER's we hold, which by the same
 * rule is what it sends with. `peer` is a list of ids, or one id.
 */
const negotiate = (local, peer) => {
  if (local === null) return null;
  const ids = typeof peer === 'string' ? [peer] : peer;
  if (!Array.isArray(ids)) return null;
  const count = Math.min(ids.length, MAX_PEER_IDS);
  let decode = null;
  for (let i = 0; i < count && decode === null; i++) {
    if (typeof ids[i] === 'string') decode = codecById(local, ids[i]);
  }
  if (decode === null) return null;
  const { codecs } = local;
  let encode = decode;
  for (let i = 0; i < codecs.length; i++) {
    if (announced(ids, count, codecs[i].id)) {
      encode = codecs[i];
      break;
    }
  }
  return { encode, decode };
};

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
  codecById,
  nativeCompressor,
  dictionaryId,
  Sequencer,
};
