/**
 * The browser half of the chunk codec (src/chunks.browser.js): a stream
 * chunk is `u8 idLength ‖ id ‖ payload`, and reading one back means decoding
 * its id — once per chunk a page receives.
 *
 * It used to build a TextDecoder for every chunk it read and a TextEncoder
 * for every one it wrote. Neither holds state between calls (no `stream`
 * option is passed), so one of each for the module does the same work. The
 * rows compare the two spellings; the "per chunk" ones are the old code,
 * kept here. On the read side the constructor was a fifth of the call
 * whatever the payload — the payload is a view, never copied:
 *
 *   decode, a TextDecoder per chunk    4.1 M ops/s
 *   decode, one TextDecoder            5.2 M ops/s
 *
 * On the write side it does not show: the copy of the payload into the
 * frame is what an encode costs (317,000 ops/s at 64 KiB either way). The
 * encoder is shared for the symmetry, not for a number.
 *
 * These are Node's TextDecoder and TextEncoder; a browser's are the same
 * interface over its own implementation, so read the ratio, not the rate.
 *
 * Run with `node bench/chunk-codec.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');
const { chunkEncode, chunkDecode } = require('../src/chunks.browser.js');

const encodePerChunk = (id, payload) => {
  const idBuffer = new TextEncoder().encode(id);
  const chunk = new Uint8Array(1 + idBuffer.length + payload.length);
  chunk[0] = idBuffer.length;
  chunk.set(idBuffer, 1);
  chunk.set(payload, 1 + idBuffer.length);
  return chunk;
};

const decodePerChunk = (chunk) => {
  const idLength = chunk[0];
  const id = new TextDecoder().decode(chunk.subarray(1, 1 + idLength));
  return { id, payload: chunk.subarray(1 + idLength) };
};

const ID = '3f2b8c1e-5d6a-4b7c-9e8f-0a1b2c3d4e5f';

let sink = 0;

const main = async () => {
  console.log(`Chunk codec (browser half) benchmark — Node ${process.version}\n`);
  for (const size of [256, 4096, 65_536]) {
    const payload = new Uint8Array(size).fill(3);
    const frame = chunkEncode(ID, payload);
    await bench(`decode ${size} B — a TextDecoder per chunk`, () => {
      sink += decodePerChunk(frame).id.length;
    });
    await bench(`decode ${size} B — one TextDecoder`, () => {
      sink += chunkDecode(frame).id.length;
    });
    await bench(`encode ${size} B — a TextEncoder per chunk`, () => {
      sink += encodePerChunk(ID, payload).length;
    });
    await bench(`encode ${size} B — one TextEncoder`, () => {
      sink += chunkEncode(ID, payload).length;
    });
  }
  if (sink < 0) console.log(sink);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
