'use strict';

// The platform's own per-message codecs, Node half: what node:zlib ships,
// named as CompressionStream names its formats — 'deflate-raw', 'brotli',
// 'zstd' — so a Node peer and a browser peer negotiate the same id with no
// table between them. The browser half is native.browser.js, swapped in
// through package.json#browser.
//
// 'deflate-raw' is what `compression: true` means, and stays that: it is
// level with zstd in bytes and CPU up to ~2 KB, where RPC messages live, and
// it is the one format every CompressionStream and every Node has
// (bench/algorithms.js). The other two are a choice — `codec: 'zstd'` for
// Node↔Node answers of 16 KB and up (a third of deflate's cost at 27 KB,
// 10% smaller), `codec: 'brotli'` for the smallest bytes at deflate's old
// price — and zstd exists in node:zlib only since 22.15 / 23.8, which
// `hasZstd` answers rather than a version check.
//
// The default levels are measured, not zlib's (bench/algorithms.js):
//   - deflate 3, the knee — zlib's levels 1–3 are its fast strategy, 4+ the
//     lazy one: at 27 KB level 3 takes 46 µs for 3,351 B against level 6's
//     118 µs for 3,196 B, and level 4 is slower AND larger than 3;
//   - Brotli quality 4 (93 µs / 2,601 B there). zlib's own default is
//     quality 11 — 33 MILLISECONDS on the same message — which is why a
//     Brotli codec is built here and not left to a caller's two lines;
//   - zstd level 1, with the source size pledged (36 µs / 2,848 B; level 3
//     is slower and no smaller on JSON).
//
// Synchronous on purpose, and `async` is a threshold, not a switch. zlib's
// threadpool hand-off costs a fixed ~20 µs per call (bench/zlib-async.js):
// on a 300 B message the async deflate takes 28 µs to the sync one's 9,
// at 27 KB 120 to 100, and the two are level at ~256 KB — only past that
// is the event loop the thing being bought. Inflate never earns it: it is
// eight times faster than deflate, so the hand-off loses at every size up
// to the 16 MiB cap (1.1 MB: 578 µs sync, 1,136 async). So `decode` is
// always synchronous, `encode` hands a message of `async` bytes or more to
// the threadpool and answers a promise for it — which only the carriers
// with an ordering queue (WebTransport, WebRTC) can take; the Node↔Node
// carriers refuse a codec that declares `async` at construction.
//
// `maxOutput` is zlib's maxOutputLength: an inflate that would exceed it
// fails after at most one chunk past it, which is what bounds a
// compression bomb to the cap the transport already enforces on plain
// messages — for all three families alike.

const zlib = require('node:zlib');

const ID = 'deflate-raw';
const THRESHOLD = 1024;

// [default, lowest, highest] of each family's level — Brotli calls it
// quality. Closed: a name outside it is a TypeError on both platforms.
const LEVELS = Object.freeze({
  __proto__: null,
  'deflate-raw': Object.freeze([3, -1, 9]),
  brotli: Object.freeze([4, 0, 11]),
  zstd: Object.freeze([1, 1, 22]),
});

/** Whether this zlib has Zstandard — node:zlib does since 22.15 / 23.8. */
const hasZstd = (lib) => typeof lib.zstdCompressSync === 'function' && typeof lib.zstdDecompressSync === 'function';

const promised = (run, bytes, options) =>
  new Promise((resolve, reject) => {
    run(bytes, options, (error, out) => (error ? reject(error) : resolve(out)));
  });

/**
 * An `encode` that is synchronous under `asyncAbove` bytes (or always, for
 * null) and a promise from the threadpool at and past it. `optionsOf(bytes)`
 * answers the zlib options of one call — Brotli's size hint and zstd's
 * pledged size are per message.
 */
const thresholdEncoder = (sync, callback, optionsOf, asyncAbove) => {
  if (asyncAbove === null) return (bytes) => sync(bytes, optionsOf(bytes));
  return (bytes) =>
    bytes.length >= asyncAbove ? promised(callback, bytes, optionsOf(bytes)) : sync(bytes, optionsOf(bytes));
};

/**
 * A raw-deflate `encode` over fixed zlib `options`. Shared with the
 * dictionary codec, whose options carry the dictionary.
 */
const deflateEncoder = (asyncAbove, options = undefined) =>
  thresholdEncoder(zlib.deflateRawSync, zlib.deflateRaw, () => options, asyncAbove);

const families = {
  __proto__: null,
  'deflate-raw': (lib, level, async) => {
    const options = { level };
    return {
      encode: thresholdEncoder(lib.deflateRawSync, lib.deflateRaw, () => options, async),
      decode: (bytes, maxOutput) => lib.inflateRawSync(bytes, { maxOutputLength: maxOutput }),
    };
  },
  brotli: (lib, level, async) => {
    const { BROTLI_PARAM_QUALITY, BROTLI_PARAM_MODE, BROTLI_MODE_TEXT, BROTLI_PARAM_SIZE_HINT } = lib.constants;
    const optionsOf = (bytes) => ({
      params: {
        [BROTLI_PARAM_QUALITY]: level,
        [BROTLI_PARAM_MODE]: BROTLI_MODE_TEXT,
        [BROTLI_PARAM_SIZE_HINT]: bytes.length,
      },
    });
    return {
      encode: thresholdEncoder(lib.brotliCompressSync, lib.brotliCompress, optionsOf, async),
      decode: (bytes, maxOutput) => lib.brotliDecompressSync(bytes, { maxOutputLength: maxOutput }),
    };
  },
  zstd: (lib, level, async) => {
    const { ZSTD_c_compressionLevel } = lib.constants;
    const optionsOf = (bytes) => ({ params: { [ZSTD_c_compressionLevel]: level }, pledgedSrcSize: bytes.length });
    return {
      encode: thresholdEncoder(lib.zstdCompressSync, lib.zstdCompress, optionsOf, async),
      decode: (bytes, maxOutput) => lib.zstdDecompressSync(bytes, { maxOutputLength: maxOutput }),
    };
  },
};

/**
 * A platform codec. `algorithm` is 'deflate-raw' (default), 'brotli' or
 * 'zstd'; `level` the family's own (Brotli's quality), its measured default
 * when absent; `async` a normalized byte threshold or null. An algorithm
 * this Node lacks (zstd before 22.15) is a TypeError — or null under
 * `optional`, which is how a preference list skips it. `zlib` is the seam
 * the tests answer both of those through.
 */
const nativeCompressor = ({ algorithm = ID, level, async = null, optional = false, zlib: lib = zlib } = {}) => {
  if (typeof algorithm !== 'string' || !(algorithm in LEVELS)) {
    throw new TypeError(`compression: unknown algorithm ${JSON.stringify(algorithm)} — deflate-raw, brotli or zstd`);
  }
  const [standard, lowest, highest] = LEVELS[algorithm];
  if (level !== undefined && !(Number.isInteger(level) && level >= lowest && level <= highest)) {
    throw new TypeError(`compression: ${algorithm} level must be an integer from ${lowest} to ${highest}`);
  }
  if (algorithm === 'zstd' && !hasZstd(lib)) {
    if (optional) return null;
    throw new TypeError('compression: this Node has no zstd in node:zlib (22.15+ / 23.8+)');
  }
  const { encode, decode } = families[algorithm](lib, level ?? standard, async);
  return { id: algorithm, threshold: THRESHOLD, async, encode, decode };
};

module.exports = { nativeCompressor, deflateEncoder, hasZstd, NATIVE_ID: ID, ALGORITHMS: Object.keys(LEVELS) };
