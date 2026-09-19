'use strict';

// The platform's own per-message codec, browser half: raw deflate through
// CompressionStream / DecompressionStream, which is all a page has —
// asynchronous, no flush control, no preset dictionary (a dictionary needs
// the codec of @alexify/wrpc/deflate, injected). Where the globals are
// missing the platform has no codec and `nativeCompressor()` answers null:
// compression stays off unless one is injected.
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

const nativeCompressor = () => {
  if (typeof CompressionStream !== 'function' || typeof DecompressionStream !== 'function') return null;
  return {
    id: ID,
    threshold: 4096,
    encode: (bytes) => through(new CompressionStream('deflate-raw'), bytes, Infinity),
    decode: (bytes, maxOutput) => through(new DecompressionStream('deflate-raw'), bytes, maxOutput),
  };
};

module.exports = { nativeCompressor, NATIVE_ID: ID };
