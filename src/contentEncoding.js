'use strict';

// `Content-Encoding` for the two HTTP-shaped transports — a one-shot gzip of
// a packet-mode or REST response body, and a persistent gzip member over an
// SSE stream flushed after every event. Both are opt-in (`http.compression`,
// `sse.compression`) and off by default: a deflate per response is CPU spent
// for every peer to save bytes only some of them need, and the server's
// first commitment is the cost per request. Node-only by construction —
// `src/transport.js` and `src/sse/server.js` are never bundled for the
// browser, and the client side needs nothing: `fetch` announces
// `Accept-Encoding` and inflates by itself, in browsers and in Node alike.
//
// gzip only. Every client that compresses at all accepts it, it is what
// `fetch` sends first, and the zlib-wrapped `deflate` token has a history of
// peers that read it as raw deflate. Brotli would cost more CPU per byte than
// this path is meant to spend.

const zlib = require('node:zlib');

const DEFAULT_THRESHOLD = 1024;
// The threadpool hand-off costs a fixed amount per call: the same shape as
// permessage-deflate's `async`, with the same default (bench/deflate-context.js).
const DEFAULT_ASYNC_THRESHOLD = 256 * 1024;

const isPositiveInteger = (value) => Number.isInteger(value) && value > 0;

// `compression: true | { threshold, filter, level, memLevel, async }` into
// the frozen shape the transports read, or null for off. Strict: this option
// is new, so a bad value is a TypeError at construction rather than a
// response that quietly went out uncompressed.
const normalizeCompression = (value, name) => {
  if (value === undefined || value === null || value === false) return null;
  const options = value === true ? {} : value;
  if (typeof options !== 'object' || Array.isArray(options)) {
    throw new TypeError(`${name}: compression must be true, false or an options object`);
  }
  const { threshold = DEFAULT_THRESHOLD, filter = null, level, memLevel, async: asyncOption = null } = options;
  if (!(Number.isInteger(threshold) && threshold >= 0)) {
    throw new TypeError(`${name}: compression.threshold must be a non-negative integer`);
  }
  if (filter !== null && typeof filter !== 'function') {
    throw new TypeError(`${name}: compression.filter must be a function`);
  }
  if (level !== undefined && !(Number.isInteger(level) && level >= -1 && level <= 9)) {
    throw new TypeError(`${name}: compression.level must be an integer from -1 to 9`);
  }
  if (memLevel !== undefined && !(Number.isInteger(memLevel) && memLevel >= 1 && memLevel <= 9)) {
    throw new TypeError(`${name}: compression.memLevel must be an integer from 1 to 9`);
  }
  let async = null;
  if (asyncOption !== null && asyncOption !== false) {
    const raw = asyncOption === true ? {} : asyncOption;
    if (typeof raw !== 'object') throw new TypeError(`${name}: compression.async must be an object`);
    async = { threshold: isPositiveInteger(raw.threshold) ? raw.threshold : DEFAULT_ASYNC_THRESHOLD };
  }
  return Object.freeze({ threshold, filter, level, memLevel, async });
};

// RFC 9110 §12.5.3: a comma-separated list of codings, each with an optional
// `;q=` weight; `*` stands for any coding not named. gzip is acceptable when
// it — or the wildcard — appears with a weight above zero. A hand-rolled
// scan rather than a regex: this runs per HTTP response once the option is
// on, and the header is peer-controlled, so no split-per-token allocation
// on a value an attacker can make long.
const acceptsGzip = (header) => {
  if (typeof header !== 'string' || header.length === 0) return false;
  const length = header.length;
  let wildcard = false;
  let start = 0;
  while (start <= length) {
    let end = header.indexOf(',', start);
    if (end < 0) end = length;
    let tokenEnd = header.indexOf(';', start);
    if (tokenEnd < 0 || tokenEnd > end) tokenEnd = end;
    const token = header.slice(start, tokenEnd).trim().toLowerCase();
    const params = tokenEnd < end ? header.slice(tokenEnd + 1, end) : '';
    const zero = params.length > 0 && /^\s*q\s*=\s*0(?:\.0{0,3})?\s*$/i.test(params);
    if (token === 'gzip' || token === 'x-gzip') return !zero;
    if (token === '*' && !zero) wildcard = true;
    start = end + 1;
  }
  return wildcard;
};

// Whether a response may be encoded at all: the peer accepts gzip, nothing
// upstream already encoded the body (a route's own `Content-Encoding`, or a
// framework plugin's), and the per-request `filter` — when there is one —
// says yes. The header scan is case-insensitive because `headers` mixes the
// transport's own spelling with whatever `setHeader` was handed.
const shouldEncode = (compression, call, headers) => {
  if (!acceptsGzip(call.headers?.['accept-encoding'])) return false;
  for (const name in headers) {
    if (name.length === 16 && name.toLowerCase() === 'content-encoding') return false;
  }
  return compression.filter === null || compression.filter(call) === true;
};

// `Vary: Accept-Encoding` on an encoded response, joined onto the CORS
// `Vary: Origin` when both apply — a shared cache must key on the request
// header that decided the representation.
const markEncoded = (headers) => {
  headers['Content-Encoding'] = 'gzip';
  const vary = headers['Vary'];
  headers['Vary'] = vary ? `${vary}, Accept-Encoding` : 'Accept-Encoding';
};

const gzipOptions = (compression) => {
  const options = {};
  if (compression.level !== undefined) options.level = compression.level;
  if (compression.memLevel !== undefined) options.memLevel = compression.memLevel;
  return options;
};

// The SSE half: one gzip member for the life of the response, every event
// sync-flushed so it leaves the process the moment it is written — the
// stream's own history is what a later event compresses against, which is
// context takeover for free (bench/http-compression.js). Wraps the host's
// `{ write, end, onClose, onDrain }` writer with the same shape, so
// SseChannels needs to know nothing about it.
//
// Backpressure: the host's `write` answer is remembered from the last chunk
// zlib produced and reported on the next `write`, and its 'drain' is passed
// through — the signal moves one stage later, as it does for permessage-
// deflate's async path, and the subscription pump waits on it exactly as
// before. The gzip stream's own high-water mark counts too.
const gzipWriter = (writer, compression) => {
  const gzip = zlib.createGzip({ ...gzipOptions(compression), flush: zlib.constants.Z_SYNC_FLUSH });
  let writable = true;
  let ended = false;
  const end = () => {
    if (ended) return;
    ended = true;
    try {
      writer.end();
    } catch {
      // The response died first; its close listener owns the cleanup.
    }
  };
  gzip.on('data', (chunk) => {
    try {
      writable = writer.write(chunk) !== false;
    } catch {
      writable = false;
    }
  });
  gzip.once('end', end);
  gzip.once('error', end);
  writer.onClose?.(() => gzip.destroy());
  return {
    write: (text) => gzip.write(text) && writable,
    end: () => void gzip.end(),
    onClose: (listener) => writer.onClose?.(listener),
    onDrain: (listener) => {
      writer.onDrain?.(() => {
        writable = true;
        listener();
      });
      gzip.on('drain', listener);
    },
  };
};

module.exports = {
  DEFAULT_THRESHOLD,
  DEFAULT_ASYNC_THRESHOLD,
  normalizeCompression,
  acceptsGzip,
  shouldEncode,
  markEncoded,
  gzipOptions,
  gzipWriter,
};
