'use strict';

// @alexify/wrpc/deflate — a per-message DEFLATE codec in plain JavaScript,
// for the one thing a browser cannot do otherwise: compress against a
// preset dictionary (CompressionStream takes none), and synchronously.
// Injected wherever a codec goes — `compression: { codec }` on the
// WebTransport or WebRTC client, a browser peer — and its own subpath, so
// a page that does not want it never loads a byte of it.
//
// The policy, from bench/deflate-js.js: small messages go through the own
// encoder (fixed Huffman + LZ77 against the dictionary — the whole win is
// there, and fixed codes cost nothing on so little input); past
// `nativeAbove` a message goes to the platform's CompressionStream where
// there is one, dynamic Huffman and no dictionary — a large payload has
// all the history it needs inside itself, and the output still inflates on
// a peer holding the dictionary, since a stream that never reaches back
// into it does not care. Decoding is always the own inflater: complete
// (stored, fixed, dynamic), dictionary-aware, capped.
//
// The id matches the Node dictionary codec's for the same bytes
// (`deflate-raw+dict:<id>`), so a Node peer on node:zlib and a browser peer
// on this codec negotiate with each other; without a dictionary it is the
// platform id, and the platform codecs read it.

const { inflateRaw, DeflateError } = require('./inflate.js');
const { deflateRaw } = require('./deflate.js');
const { dictionaryId, DICTIONARY_ID_PREFIX, isPromise } = require('../compression/ids.js');

const NATIVE_ID = 'deflate-raw';
const DEFAULT_NATIVE_ABOVE = 4096;

const toBytes = (input) => {
  if (input instanceof Uint8Array) return input;
  if (typeof input === 'string') return new TextEncoder().encode(input);
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  throw new TypeError('createDeflateCodec: dictionary must be bytes or a string');
};

// The platform's CompressionStream, for the large messages — asynchronous.
const nativeEncode = async (bytes) => {
  const stream = new CompressionStream('deflate-raw');
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader();
  const written = writer.write(bytes).then(() => writer.close());
  written.catch(() => {});
  const chunks = [];
  let total = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    chunks.push(value);
    total += value.length;
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

// In Node the platform codec is node:zlib and every carrier there may need
// a synchronous answer, so the native hand-off is a browser default only.
const inBrowser = () =>
  typeof CompressionStream === 'function' && !(typeof process === 'object' && process?.versions?.node);

/**
 * `{ id, threshold, dictionary, encode, decode }` — a Compressor.
 * `dictionary`: bytes both ends hold (`buildDictionary(router)`), or none
 * for a plain raw-deflate codec. `threshold`: the size under which a
 * message goes plain (64 B with a dictionary, 1 KiB without). `native`:
 * hand messages of `nativeAbove` bytes (4 KiB) and more to
 * CompressionStream — on by default in a browser, off in Node, and then
 * `encode` may answer a promise for those. `level`: the hash-chain depth,
 * 1–9.
 */
const createDeflateCodec = ({
  dictionary = null,
  threshold,
  native,
  nativeAbove = DEFAULT_NATIVE_ABOVE,
  level = 6,
} = {}) => {
  const dict = dictionary === null || dictionary === undefined ? null : toBytes(dictionary);
  if (dict !== null && dict.length === 0) throw new TypeError('createDeflateCodec: dictionary must not be empty');
  if (threshold !== undefined && !(Number.isInteger(threshold) && threshold >= 0)) {
    throw new TypeError('createDeflateCodec: threshold must be a non-negative integer');
  }
  if (!(Number.isInteger(nativeAbove) && nativeAbove > 0)) {
    throw new TypeError('createDeflateCodec: nativeAbove must be a positive integer');
  }
  if (!(Number.isInteger(level) && level >= 1 && level <= 9)) {
    throw new TypeError('createDeflateCodec: level must be an integer from 1 to 9');
  }
  const useNative = (native ?? inBrowser()) && typeof CompressionStream === 'function';
  return {
    id: dict === null ? NATIVE_ID : DICTIONARY_ID_PREFIX + dictionaryId(dict),
    threshold: threshold ?? (dict === null ? 1024 : 64),
    dictionary: dict,
    encode: (bytes) => {
      if (useNative && bytes.length >= nativeAbove) return nativeEncode(bytes);
      return deflateRaw(bytes, { dictionary: dict, level });
    },
    decode: (bytes, maxOutput) => inflateRaw(bytes, { dictionary: dict, maxOutput }),
  };
};

module.exports = { createDeflateCodec, inflateRaw, deflateRaw, DeflateError, isPromise };
