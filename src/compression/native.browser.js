'use strict';

// The platform's own per-message codecs, browser half: CompressionStream /
// DecompressionStream, which is all a page has — asynchronous, no level, no
// flush control, no preset dictionary (a dictionary needs the codec of
// @alexify/wrpc/deflate, injected). 'deflate-raw' is in every one of them;
// 'brotli' and 'zstd' only in some, which the constructor answers — so a
// format this browser lacks is null, never a throw, and a preference list
// moves on to the next. Where the globals are missing the platform has no
// codec at all and compression stays off unless one is injected.
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
  written.catch(() => {});
  const chunks = [];
  let total = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    total += value.length;
    if (total > maxOutput) {
      reader.cancel().catch(() => {});
      throw new RangeError('inflated message exceeds the cap');
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

const nativeCompressor = ({ algorithm = ID } = {}) => {
  if (algorithm !== ID && algorithm !== 'brotli' && algorithm !== 'zstd') {
    throw new TypeError(`compression: unknown algorithm ${JSON.stringify(algorithm)} — deflate-raw, brotli or zstd`);
  }
  try {
    void new CompressionStream(algorithm);
    void new DecompressionStream(algorithm);
  } catch {
    return null;
  }
  return {
    id: algorithm,
    threshold: 4096,
    encode: (bytes) => through(new CompressionStream(algorithm), bytes, Infinity),
    decode: (bytes, maxOutput) => through(new DecompressionStream(algorithm), bytes, maxOutput),
  };
};

module.exports = { nativeCompressor, NATIVE_ID: ID };
