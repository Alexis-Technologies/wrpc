'use strict';

// The platform's own per-message codec, Node half: raw deflate through
// node:zlib, synchronous — 7.5 µs on a 90 B message, 146 µs at 28 KB
// (bench/message-compression.js). The browser half is native.browser.js,
// swapped in through package.json#browser; both answer the same `id`, so a
// Node peer and a browser peer negotiate it with each other.
//
// `maxOutput` is zlib's maxOutputLength: an inflate that would exceed it
// fails after at most one chunk past it, which is what bounds a
// compression bomb to the cap the transport already enforces on plain
// messages.

const zlib = require('node:zlib');

const ID = 'deflate-raw';

const nativeCompressor = () => ({
  id: ID,
  threshold: 1024,
  encode: (bytes) => zlib.deflateRawSync(bytes),
  decode: (bytes, maxOutput) => zlib.inflateRawSync(bytes, { maxOutputLength: maxOutput }),
});

module.exports = { nativeCompressor, NATIVE_ID: ID };
