'use strict';

// The platform's own per-message codec, Node half: raw deflate through
// node:zlib, synchronous — 7.5 µs on a 90 B message, 146 µs at 28 KB
// (bench/message-compression.js). The browser half is native.browser.js,
// swapped in through package.json#browser; both answer the same `id`, so a
// Node peer and a browser peer negotiate it with each other.
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
// messages.

const zlib = require('node:zlib');

const ID = 'deflate-raw';

const deflateAsync = (bytes, options) =>
  new Promise((resolve, reject) => {
    zlib.deflateRaw(bytes, options, (error, out) => (error ? reject(error) : resolve(out)));
  });

/**
 * A raw-deflate `encode`: synchronous under `asyncAbove` bytes (or always,
 * for null), a promise from the threadpool at and past it. Shared with the
 * dictionary codec, whose `options` carry the dictionary.
 */
const deflateEncoder = (asyncAbove, options = undefined) => {
  if (asyncAbove === null) return (bytes) => zlib.deflateRawSync(bytes, options);
  return (bytes) => (bytes.length >= asyncAbove ? deflateAsync(bytes, options) : zlib.deflateRawSync(bytes, options));
};

/** The platform codec; `async` is a normalized byte threshold or null. */
const nativeCompressor = ({ async = null } = {}) => ({
  id: ID,
  threshold: 1024,
  async,
  encode: deflateEncoder(async),
  decode: (bytes, maxOutput) => zlib.inflateRawSync(bytes, { maxOutputLength: maxOutput }),
});

module.exports = { nativeCompressor, deflateEncoder, NATIVE_ID: ID };
