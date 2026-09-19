'use strict';

// The platform codecs with their knobs out: `compression: { codec: 'zstd' }`
// takes the measured defaults of native.js, and these are for the caller
// who wants another level, threshold or `async` per codec — most usefully
// inside a preference list, where each entry carries its own. Node-only:
// a browser's CompressionStream has no level to turn.
//
//   compression: { codec: [zstdCompressor({ level: 3 }), 'deflate-raw'] }
//
// An `id` names the FORMAT, never the level — any level's output is read
// by the same decoder — so two ends on different levels still negotiate.

const { normalizeAsync } = require('./index.js');
const { nativeCompressor } = require('./native.js');

const build = (name, algorithm, level, { threshold, async: asyncOption } = {}) => {
  if (threshold !== undefined && !(Number.isInteger(threshold) && threshold >= 0)) {
    throw new TypeError(`${name}: threshold must be a non-negative integer`);
  }
  const codec = nativeCompressor({ algorithm, level, async: normalizeAsync(asyncOption, name) });
  if (threshold !== undefined) codec.threshold = threshold;
  return codec;
};

/** Raw deflate at zlib `level` (-1..9, default 3 — bench/algorithms.js). */
const deflateCompressor = (options = {}) => build('deflateCompressor', 'deflate-raw', options.level, options);

/**
 * Brotli at `quality` (0..11, default 4). Past 5 the cost climbs faster
 * than the bytes fall, and zlib's own default, 11, is milliseconds a
 * message (bench/algorithms.js) — an archive setting, not a wire one.
 */
const brotliCompressor = (options = {}) => build('brotliCompressor', 'brotli', options.quality, options);

/** Zstandard at `level` (1..22, default 1). A TypeError where node:zlib has none (before 22.15 / 23.8). */
const zstdCompressor = (options = {}) => build('zstdCompressor', 'zstd', options.level, options);

module.exports = { deflateCompressor, brotliCompressor, zstdCompressor };
