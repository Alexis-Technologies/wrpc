'use strict';

// The dictionary codec, Node half: raw deflate through node:zlib with a
// preset dictionary — the codec a Node↔Node carrier (the broker binding,
// the backplane envelopes, a Node ws client, a Node peer on WebRTC or
// WebTransport) injects as `compression: { codec }`. Its id carries the
// dictionary's hash, so two ends compress against the same bytes or not
// at all: an instance whose router differs (a rolling deploy) names
// another id, the negotiation fails, and the wire stays plain rather than
// corrupt. The browser half is the pure-JS codec of @alexify/wrpc/deflate
// — CompressionStream takes no dictionary.
//
// The threshold is lower than the plain codec's 1 KiB: with the history
// preloaded, a 90 B event compresses to a third (bench/dictionary.js), so
// small messages are exactly what the dictionary is for.

const zlib = require('node:zlib');

const { dictionaryId, DICTIONARY_ID_PREFIX: ID_PREFIX } = require('./index.js');

const DEFAULT_THRESHOLD = 64;

const toBuffer = (dictionary) => {
  if (Buffer.isBuffer(dictionary)) return dictionary;
  if (dictionary instanceof Uint8Array) return Buffer.from(dictionary.buffer, dictionary.byteOffset, dictionary.length);
  if (typeof dictionary === 'string') return Buffer.from(dictionary);
  throw new TypeError('dictionaryCompressor: dictionary must be bytes or a string');
};

/**
 * `{ id, threshold, dictionary, encode, decode }` — a Compressor over
 * `dictionary` (bytes from `buildDictionary`, or any bytes both ends
 * hold). `level` is zlib's; `threshold` the size under which a message
 * goes plain (64 B).
 */
const dictionaryCompressor = (dictionary, { level, threshold = DEFAULT_THRESHOLD } = {}) => {
  const dict = toBuffer(dictionary);
  if (dict.length === 0) throw new TypeError('dictionaryCompressor: dictionary must not be empty');
  if (!(Number.isInteger(threshold) && threshold >= 0)) {
    throw new TypeError('dictionaryCompressor: threshold must be a non-negative integer');
  }
  if (level !== undefined && !(Number.isInteger(level) && level >= -1 && level <= 9)) {
    throw new TypeError('dictionaryCompressor: level must be an integer from -1 to 9');
  }
  const deflate = { dictionary: dict };
  if (level !== undefined) deflate.level = level;
  return {
    id: ID_PREFIX + dictionaryId(dict),
    threshold,
    dictionary: dict,
    encode: (bytes) => zlib.deflateRawSync(bytes, deflate),
    decode: (bytes, maxOutput) => zlib.inflateRawSync(bytes, { dictionary: dict, maxOutputLength: maxOutput }),
  };
};

module.exports = { dictionaryCompressor, DICTIONARY_ID_PREFIX: ID_PREFIX };
