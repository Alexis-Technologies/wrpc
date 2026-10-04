'use strict';

// SHA-256, HMAC and HKDF (RFC 5869), Node half: node:crypto. The browser
// half (hkdf.browser.js, swapped through package.json#browser) is
// crypto.subtle, which Node has too — but every HMAC through it is a
// threadpool hand-off, and a handshake or an HPKE key schedule is a dozen of
// them: a sealed HTTP request costs 209 µs for both ends with this file
// against 565 µs over subtle (bench/encryption.js). The methods answer
// promises although nothing here waits: one contract for both platforms.
//
// Extract and Expand are separate on purpose: Noise chains HMACs off a
// chaining key, HPKE's LabeledExtract / LabeledExpand feed different labels
// to each half.

const crypto = require('node:crypto');

const HASH_LENGTH = 32;
// RFC 5869 §2.2: an absent salt is HashLen zeros.
const ZEROS = Buffer.alloc(HASH_LENGTH);

const hmacOf = (key, data) =>
  crypto
    .createHmac('sha256', key.length === 0 ? ZEROS : key)
    .update(data)
    .digest();

const expandOf = (prk, info, length) => {
  if (!Number.isInteger(length) || length < 1 || length > 255 * HASH_LENGTH) {
    throw new RangeError('hkdf: length out of range');
  }
  const out = Buffer.alloc(length);
  const key = prk.length === 0 ? ZEROS : prk;
  let previous = ZEROS.subarray(0, 0);
  for (let offset = 0, counter = 1; offset < length; offset += HASH_LENGTH, counter++) {
    previous = crypto.createHmac('sha256', key).update(previous).update(info).update(Buffer.of(counter)).digest();
    previous.copy(out, offset, 0, Math.min(HASH_LENGTH, length - offset));
  }
  return out;
};

const createKdf = () =>
  Object.freeze({
    id: 'SHA256',
    hashLength: HASH_LENGTH,
    hash: async (data) => crypto.createHash('sha256').update(data).digest(),
    hmac: async (key, data) => hmacOf(key, data),
    extract: async (salt, ikm) => hmacOf(salt, ikm),
    expand: async (prk, info, length) => expandOf(prk, info, length),
    derive: async (ikm, salt, info, length) => expandOf(hmacOf(salt, ikm), info, length),
  });

module.exports = { createKdf, HASH_LENGTH };
