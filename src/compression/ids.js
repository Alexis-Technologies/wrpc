'use strict';

// The leaf the pure-JS codec of @alexify/wrpc/deflate needs from this
// directory and nothing more: a page that injects that codec pays for the
// id of its dictionary, not for the negotiation and the platform codecs
// (index.js re-exports all three).

// A dictionary codec's id: the prefix plus `dictionaryId` of the bytes —
// shared by the Node zlib codec and the pure-JS one so the two negotiate.
const DICTIONARY_ID_PREFIX = 'deflate-raw+dict:';

const isPromise = (value) => value !== null && typeof value === 'object' && typeof value.then === 'function';

/**
 * A short, deterministic id for dictionary bytes — FNV-1a over the bytes,
 * 64 bits as 16 hex characters — what a dictionary codec's `id` carries so
 * two ends compress against the same bytes or not at all. A fingerprint
 * for negotiation, not a security property; it runs in a browser too,
 * where a hash from crypto.subtle would be asynchronous.
 */
const dictionaryId = (bytes) => {
  let h1 = 0x811c9dc5;
  let h2 = 0x01000193 ^ bytes.length;
  for (let i = 0; i < bytes.length; i++) {
    h1 = Math.imul(h1 ^ bytes[i], 0x01000193) >>> 0;
    h2 = Math.imul(h2 ^ bytes[i], 0x0100019d) >>> 0;
  }
  return h1.toString(16).padStart(8, '0') + h2.toString(16).padStart(8, '0');
};

module.exports = { dictionaryId, DICTIONARY_ID_PREFIX, isPromise };
