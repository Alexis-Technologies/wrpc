'use strict';

// `Content-Encoding` for the two HTTP-shaped transports — a one-shot encode
// of a packet-mode or REST response body, and a persistent encoder over an
// SSE stream flushed after every event. Both are opt-in (`http.compression`,
// `sse.compression`) and off by default: a deflate per response is CPU spent
// for every peer to save bytes only some of them need, and the server's
// first commitment is the cost per request. Node-only by construction —
// `src/transport.js` and `src/sse/server.js` are never bundled for the
// browser, and the client side needs nothing: `fetch` announces
// `Accept-Encoding` and inflates by itself, in browsers and in Node alike.
//
// WHICH coding is `encodings`, the server's list in its order of preference:
// the first one on it the request accepts is used. It defaults to gzip
// alone, and for an SSE stream that is also the recommendation
// (bench/algorithms.js): flushed per event, gzip, Brotli and zstd come to
// 20, 23 and 18 B an event, and the latter two hold 570 and 930 KB per open
// response against gzip's 320. For a one-shot answer the others earn their
// place from ~16 KB: at 27 KB zstd level 1 costs a third of gzip and is 10%
// smaller, Brotli quality 4 is 19% smaller at gzip's cost. The zlib-wrapped
// `deflate` token is not offered at all — it has a history of peers that
// read it as raw deflate. Browsers announce `br` and `zstd` over HTTPS only.
//
// The levels are the measured ones of src/compression/native.js, not
// zlib's: Brotli's own default, quality 11, takes 33 ms on that 27 KB body.

const zlib = require('node:zlib');

const { hasZstd } = require('./compression/native.js');

const DEFAULT_THRESHOLD = 1024;
// The threadpool hand-off costs a fixed amount per call: the same shape as
// permessage-deflate's `async`, with the same default (bench/deflate-context.js).
const DEFAULT_ASYNC_THRESHOLD = 256 * 1024;
const DEFAULT_BROTLI_QUALITY = 4;
const DEFAULT_ZSTD_LEVEL = 1;

const isPositiveInteger = (value) => Number.isInteger(value) && value > 0;
const integerIn = (value, low, high) => Number.isInteger(value) && value >= low && value <= high;
// RFC 9110 §5.6.2 `token` — what a content-coding is.
const TOKEN = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

// One coding, in the shape the transports read:
//   token                 the Content-Encoding value
//   encode(body)          the whole body at once; a custom one may answer a promise
//   encodeAsync(body, cb) the same through zlib's threadpool, or null
//   createStream()        a persistent encoder that emits each write's output
//                         as it is written (SSE), or null
const gzipEncoder = ({ level, memLevel }, name) => {
  if (level !== undefined && !integerIn(level, -1, 9)) {
    throw new TypeError(`${name}: gzip level must be an integer from -1 to 9`);
  }
  if (memLevel !== undefined && !integerIn(memLevel, 1, 9)) {
    throw new TypeError(`${name}: gzip memLevel must be an integer from 1 to 9`);
  }
  const options = {};
  if (level !== undefined) options.level = level;
  if (memLevel !== undefined) options.memLevel = memLevel;
  return {
    token: 'gzip',
    encode: (body) => zlib.gzipSync(body, options),
    encodeAsync: (body, callback) => zlib.gzip(body, options, callback),
    createStream: () => zlib.createGzip({ ...options, flush: zlib.constants.Z_SYNC_FLUSH }),
  };
};

const brotliEncoder = ({ quality = DEFAULT_BROTLI_QUALITY }, name) => {
  if (!integerIn(quality, 0, 11)) throw new TypeError(`${name}: br quality must be an integer from 0 to 11`);
  const { BROTLI_PARAM_QUALITY, BROTLI_PARAM_MODE, BROTLI_MODE_TEXT, BROTLI_PARAM_SIZE_HINT } = zlib.constants;
  const optionsOf = (body) => ({
    params: {
      [BROTLI_PARAM_QUALITY]: quality,
      [BROTLI_PARAM_MODE]: BROTLI_MODE_TEXT,
      [BROTLI_PARAM_SIZE_HINT]: body.length,
    },
  });
  return {
    token: 'br',
    encode: (body) => zlib.brotliCompressSync(body, optionsOf(body)),
    encodeAsync: (body, callback) => zlib.brotliCompress(body, optionsOf(body), callback),
    createStream: () =>
      zlib.createBrotliCompress({
        params: { [BROTLI_PARAM_QUALITY]: quality, [BROTLI_PARAM_MODE]: BROTLI_MODE_TEXT },
        flush: zlib.constants.BROTLI_OPERATION_FLUSH,
      }),
  };
};

const zstdEncoder = ({ level = DEFAULT_ZSTD_LEVEL }, name, lib) => {
  if (!integerIn(level, 1, 22)) throw new TypeError(`${name}: zstd level must be an integer from 1 to 22`);
  if (!hasZstd(lib)) throw new TypeError(`${name}: this Node has no zstd in node:zlib (22.15+ / 23.8+)`);
  const params = { [lib.constants.ZSTD_c_compressionLevel]: level };
  return {
    token: 'zstd',
    encode: (body) => lib.zstdCompressSync(body, { params, pledgedSrcSize: body.length }),
    encodeAsync: (body, callback) => lib.zstdCompress(body, { params, pledgedSrcSize: body.length }, callback),
    createStream: () => lib.createZstdCompress({ params, flush: lib.constants.ZSTD_e_flush }),
  };
};

// An application's own coding: `{ encoding, encode(bytes), createStream? }`.
// `encode` may answer a promise; `createStream` answers a Node Transform
// that emits every write's output without waiting for more (for a zlib
// class, its `flush` option) — without one the coding serves one-shot
// answers only, and an SSE option refuses it.
const customEncoder = (entry, name) => {
  const { encoding, encode, createStream = null } = entry;
  if (typeof encode !== 'function') throw new TypeError(`${name}: a custom encoding must provide encode(bytes)`);
  if (createStream !== null && typeof createStream !== 'function') {
    throw new TypeError(`${name}: a custom encoding's createStream must be a function`);
  }
  return { token: encoding.toLowerCase(), encode, encodeAsync: null, createStream };
};

const toEncoder = (entry, name, lib) => {
  const options = typeof entry === 'string' ? { encoding: entry } : entry;
  if (typeof options !== 'object' || options === null || typeof options.encoding !== 'string') {
    throw new TypeError(`${name}: an encoding is 'gzip', 'br', 'zstd' or an object with \`encoding\``);
  }
  if (!TOKEN.test(options.encoding)) throw new TypeError(`${name}: encoding must be an HTTP token`);
  if (options.encode !== undefined) return customEncoder(options, name);
  const token = options.encoding.toLowerCase();
  if (token === 'gzip') return gzipEncoder(options, name);
  if (token === 'br') return brotliEncoder(options, name);
  if (token === 'zstd') return zstdEncoder(options, name, lib);
  throw new TypeError(
    `${name}: unknown encoding ${JSON.stringify(options.encoding)} — gzip, br, zstd, or one with encode()`,
  );
};

// `compression: true | { threshold, filter, encodings, async }` into the
// frozen shape the transports read, or null for off. Strict: this option is
// new, so a bad value is a TypeError at construction rather than a response
// that quietly went out uncompressed. `streaming` is the SSE half asking
// for every coding to be able to stream. `zlib` is the seam the tests
// answer "a Node without zstd" through.
const normalizeCompression = (value, name, { streaming = false, zlib: lib = zlib } = {}) => {
  if (value === undefined || value === null || value === false) return null;
  const options = value === true ? {} : value;
  if (typeof options !== 'object' || Array.isArray(options)) {
    throw new TypeError(`${name}: compression must be true, false or an options object`);
  }
  const { threshold = DEFAULT_THRESHOLD, filter = null, encodings = ['gzip'], async: asyncOption = null } = options;
  if (!(Number.isInteger(threshold) && threshold >= 0)) {
    throw new TypeError(`${name}: compression.threshold must be a non-negative integer`);
  }
  if (filter !== null && typeof filter !== 'function') {
    throw new TypeError(`${name}: compression.filter must be a function`);
  }
  if (options.level !== undefined || options.memLevel !== undefined) {
    throw new TypeError(
      `${name}: compression.level / memLevel belong to the coding — encodings: [{ encoding: 'gzip', level }]`,
    );
  }
  if (!Array.isArray(encodings) || encodings.length === 0 || encodings.length > 8) {
    throw new TypeError(`${name}: compression.encodings must be a list of one to eight encodings`);
  }
  const encoders = encodings.map((entry) => toEncoder(entry, `${name}: compression.encodings`, lib));
  for (let i = 0; i < encoders.length; i++) {
    const { token } = encoders[i];
    if (token === 'identity' || token === '*') throw new TypeError(`${name}: ${token} is not an encoding to apply`);
    if (encoders.findIndex((other) => other.token === token) !== i) {
      throw new TypeError(`${name}: compression.encodings names ${token} twice`);
    }
    if (streaming && encoders[i].createStream === null) {
      throw new TypeError(`${name}: the ${token} encoding has no createStream() — an SSE response needs one`);
    }
  }
  let async = null;
  if (asyncOption !== null && asyncOption !== false) {
    const raw = asyncOption === true ? {} : asyncOption;
    if (typeof raw !== 'object') throw new TypeError(`${name}: compression.async must be an object`);
    async = { threshold: isPositiveInteger(raw.threshold) ? raw.threshold : DEFAULT_ASYNC_THRESHOLD };
  }
  return Object.freeze({ threshold, filter, async, encoders: Object.freeze(encoders) });
};

// RFC 9110 §12.5.3: a comma-separated list of codings, each with an optional
// `;q=` weight; `*` stands for any coding not named. Answers the FIRST
// encoder of the server's list the request accepts — named with a weight
// above zero, or covered by such a wildcard and not itself refused. The
// server's order decides, as in nginx: a client's weights say what it
// accepts, and which acceptable coding costs this server least is not the
// client's to know. A hand-rolled scan rather than a regex: this runs per
// HTTP response once the option is on, and the header is peer-controlled,
// so no split-per-token allocation on a value an attacker can make long
// (bench/http-compression.js). The encoders are a handful, so a bit each.
const pickEncoding = (header, encoders) => {
  if (typeof header !== 'string' || header.length === 0) return null;
  const length = header.length;
  const count = encoders.length;
  let accepted = 0;
  let refused = 0;
  let wildcard = false;
  let start = 0;
  while (start <= length) {
    let end = header.indexOf(',', start);
    if (end < 0) end = length;
    let tokenEnd = header.indexOf(';', start);
    if (tokenEnd < 0 || tokenEnd > end) tokenEnd = end;
    let token = header.slice(start, tokenEnd).trim().toLowerCase();
    const params = tokenEnd < end ? header.slice(tokenEnd + 1, end) : '';
    const zero = params.length > 0 && /^\s*q\s*=\s*0(?:\.0{0,3})?\s*$/i.test(params);
    if (token === '*') wildcard = !zero;
    else {
      if (token === 'x-gzip') token = 'gzip';
      for (let i = 0; i < count; i++) {
        if (encoders[i].token !== token) continue;
        if (zero) refused |= 1 << i;
        else accepted |= 1 << i;
        break;
      }
    }
    start = end + 1;
  }
  for (let i = 0; i < count; i++) {
    const bit = 1 << i;
    if ((accepted & bit) !== 0 && (refused & bit) === 0) return encoders[i];
    if (wildcard && ((accepted | refused) & bit) === 0) return encoders[i];
  }
  return null;
};

// The encoder a response is to be encoded with, or null: the peer accepts
// one of ours, nothing upstream already encoded the body (a route's own
// `Content-Encoding`, or a framework plugin's), and the per-request `filter`
// — when there is one — says yes. The header scan is case-insensitive
// because `headers` mixes the transport's own spelling with whatever
// `setHeader` was handed.
const chooseEncoding = (compression, call, headers) => {
  const encoder = pickEncoding(call.headers?.['accept-encoding'], compression.encoders);
  if (encoder === null) return null;
  for (const name in headers) {
    if (name.length === 16 && name.toLowerCase() === 'content-encoding') return null;
  }
  return compression.filter === null || compression.filter(call) === true ? encoder : null;
};

// `Vary: Accept-Encoding` on an encoded response, joined onto the CORS
// `Vary: Origin` when both apply — a shared cache must key on the request
// header that decided the representation.
const markEncoded = (headers, token) => {
  headers['Content-Encoding'] = token;
  const vary = headers['Vary'];
  headers['Vary'] = vary ? `${vary}, Accept-Encoding` : 'Accept-Encoding';
};

// The SSE half: one encoder for the life of the response (for gzip, one
// member), every event flushed so it leaves the process the moment it is
// written — the stream's own history is what a later event compresses
// against, which is context takeover for free (bench/http-compression.js).
// Wraps the host's `{ write, end, onClose, onDrain }` writer with the same
// shape, so SseChannels needs to know nothing about it.
//
// Backpressure: the host's `write` answer is remembered from the last chunk
// zlib produced and reported on the next `write`, and its 'drain' is passed
// through — the signal moves one stage later, as it does for permessage-
// deflate's async path, and the subscription pump waits on it exactly as
// before. The encoder stream's own high-water mark counts too.
const encodedWriter = (writer, encoder) => {
  const stream = encoder.createStream();
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
  stream.on('data', (chunk) => {
    try {
      writable = writer.write(chunk) !== false;
    } catch {
      writable = false;
    }
  });
  stream.once('end', end);
  stream.once('error', end);
  writer.onClose?.(() => stream.destroy());
  return {
    write: (text) => stream.write(text) && writable,
    end: () => void stream.end(),
    onClose: (listener) => writer.onClose?.(listener),
    onDrain: (listener) => {
      writer.onDrain?.(() => {
        writable = true;
        listener();
      });
      stream.on('drain', listener);
    },
  };
};

module.exports = {
  DEFAULT_THRESHOLD,
  DEFAULT_ASYNC_THRESHOLD,
  normalizeCompression,
  pickEncoding,
  chooseEncoding,
  markEncoded,
  encodedWriter,
};
