'use strict';

// SHA-256, HMAC and HKDF (RFC 5869) over crypto.subtle — one file for both
// platforms (Node >= 22 has the same WebCrypto a browser does), and
// asynchronous because subtle is. That is fine where this is used: a
// handshake, an HPKE context, a key schedule — never per message. The
// Node-only envelope code derives its subkeys with node:crypto's hkdfSync
// instead, because a backplane's receive path cannot wait.
//
// Extract and Expand are separate on purpose. subtle's own 'HKDF' algorithm
// only does the two together, and both consumers need them apart: Noise
// chains HMACs off a chaining key, HPKE's LabeledExtract / LabeledExpand
// feed different labels to each half.

const { requireSubtle, concat } = require('./bytes.js');

const HASH_LENGTH = 32;
const HMAC = { name: 'HMAC', hash: 'SHA-256' };
// RFC 5869 §2.2: an absent salt is HashLen zeros — and HMAC pads a short key
// with zeros anyway, so this is the empty key crypto.subtle refuses to import.
const ZEROS = new Uint8Array(HASH_LENGTH);

const createKdf = ({ subtle = globalThis.crypto?.subtle } = {}) => {
  requireSubtle(subtle, 'encryption');

  const hash = async (data) => new Uint8Array(await subtle.digest('SHA-256', data));

  const hmacKey = (key) => subtle.importKey('raw', key.length === 0 ? ZEROS : key, HMAC, false, ['sign']);

  const sign = async (key, data) => new Uint8Array(await subtle.sign('HMAC', key, data));

  const hmac = async (key, data) => sign(await hmacKey(key), data);

  const extract = (salt, ikm) => hmac(salt, ikm);

  const expand = async (prk, info, length) => {
    if (!Number.isInteger(length) || length < 1 || length > 255 * HASH_LENGTH) {
      throw new RangeError('hkdf: length out of range');
    }
    const key = await hmacKey(prk);
    const out = new Uint8Array(length);
    const counter = new Uint8Array(1);
    let previous = new Uint8Array(0);
    for (let offset = 0; offset < length; offset += HASH_LENGTH) {
      counter[0]++;
      previous = await sign(key, concat(previous, info, counter));
      out.set(offset + HASH_LENGTH > length ? previous.subarray(0, length - offset) : previous, offset);
    }
    return out;
  };

  const derive = async (ikm, salt, info, length) => expand(await extract(salt, ikm), info, length);

  return Object.freeze({ id: 'SHA256', hashLength: HASH_LENGTH, hash, hmac, extract, expand, derive });
};

module.exports = { createKdf, HASH_LENGTH };
