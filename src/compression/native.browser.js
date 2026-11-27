'use strict';

const { ConfigError } = require('../errors.js');

// The platform's own per-message codecs, browser half: CompressionStream /
// DecompressionStream, which is all a page has — asynchronous, no level, no
// flush control, no preset dictionary (a dictionary needs the codec of
// @alexify/wrpc/deflate, injected). 'deflate-raw' is in every one of them;
// 'brotli' and 'zstd' only in some, which the constructor answers — so a
// format this browser lacks is null in a preference list, which moves on to
// the next, and a TypeError when it is the only one asked for. Where the
// globals are missing the platform has no codec at all and compression
// stays off unless one is injected.
//
// The threshold is higher than Node's 1 KiB: a CompressionStream costs
// ~46 µs per message against zlib's 7.5 µs, and without a dictionary a
// small message barely shrinks — under 4 KiB the call is not worth it.

const ID = 'deflate-raw';

// Writes `bytes` through one transform and collects what comes out. The
// write is NOT awaited before reading: a large input under the stream's
// own backpressure would otherwise wait for a reader that never starts.
const through = async (stream, bytes, maxOutput) => {
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader();
  const written = writer.write(bytes).then(() => writer.close());
  // A write that fails errors the readable too: the read loop below is
  // where that is thrown, once.
  written.catch(() => {});
  const chunks = [];
  let total = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    total += value.length;
    if (total > maxOutput) {
      // The cap is what is thrown; a cancel that fails changes nothing.
      reader.cancel().catch(() => {});
      // zlib's own error for the cap, code included, as the Node half throws
      // it: decodeOrNull in sync.js tells the cap from bad bytes by it.
      const error = new RangeError('inflated message exceeds the cap');
      error.code = 'ERR_BUFFER_TOO_LARGE';
      throw error;
    }
    chunks.push(value);
  }
  await written;
  if (chunks.length === 1) return chunks[0];
  const out = new Uint8Array(total);
  let offset = 0;
  for (let i = 0; i < chunks.length; i++) {
    out.set(chunks[i], offset);
    offset += chunks[i].length;
  }
  return out;
};

// `optional`: the entry of a preference list — a format this browser lacks
// answers null there, and the list moves on. Asked for ALONE, a format that
// is not here is a TypeError, as on Node (native.js): it used to answer null
// whatever was asked, so `codec: 'zstd'` in a browser without it built a
// client that compressed nothing and said nothing. The default format is
// the one exception, and stays null: with no CompressionStream at all the
// platform has no codec, and `compression: true` is off until one is
// injected.
const nativeCompressor = ({ algorithm = ID, optional = false } = {}) => {
  if (algorithm !== ID && algorithm !== 'brotli' && algorithm !== 'zstd') {
    throw new ConfigError(`compression: unknown algorithm ${JSON.stringify(algorithm)} — deflate-raw, brotli or zstd`);
  }
  try {
    void new CompressionStream(algorithm);
    void new DecompressionStream(algorithm);
  } catch {
    if (optional || algorithm === ID) return null;
    throw new ConfigError(`compression: this platform's CompressionStream has no ${algorithm}`);
  }
  return {
    id: algorithm,
    threshold: 4096,
    encode: (bytes) => through(new CompressionStream(algorithm), bytes, Infinity),
    decode: (bytes, maxOutput) => through(new DecompressionStream(algorithm), bytes, maxOutput),
  };
};

module.exports = { nativeCompressor, NATIVE_ID: ID };
