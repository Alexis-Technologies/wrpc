'use strict';

const { ArgumentError } = require('./errors.js');

const ID_LENGTH_BYTES = 1;

// One of each for the module, not one per chunk: neither keeps state
// between calls (no `stream` option), and building a TextDecoder was a fifth
// of reading a chunk back — 4.1 M → 5.2 M ops/s (bench/chunk-codec.js). The
// encoder is shared for the symmetry: writing a chunk costs its copy.
const encoder = new TextEncoder();
const decoder = new TextDecoder();

const chunkEncode = (id, payload) => {
  const idBuffer = encoder.encode(id);
  const idLength = idBuffer.length;
  if (idLength > 255) {
    throw new ArgumentError(`ID length ${idLength} exceeds maximum of 255 characters`);
  }
  const chunk = new Uint8Array(ID_LENGTH_BYTES + idLength + payload.length);
  chunk[0] = idLength;
  chunk.set(idBuffer, ID_LENGTH_BYTES);
  chunk.set(payload, ID_LENGTH_BYTES + idLength);
  return chunk;
};

const chunkDecode = (chunk) => {
  const idLength = chunk[0];
  const idBuffer = chunk.subarray(ID_LENGTH_BYTES, ID_LENGTH_BYTES + idLength);
  const id = decoder.decode(idBuffer);
  const payload = chunk.subarray(ID_LENGTH_BYTES + idLength);
  return { id, payload };
};

module.exports = { chunkEncode, chunkDecode };
