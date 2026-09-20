'use strict';

// The platform's AEADs, Node half: node:crypto, synchronously. A seal of
// 1 KB is 2.4 µs here against 14 µs through crypto.subtle on the same
// machine (bench/encryption.js) — subtle pays a threadpool hand-off per
// call, the argument bench/zlib-async.js makes for zlib — and the Node↔Node
// carriers (the backplane, the brokers) have no ordering queue to put a
// promise in. The browser half
// (aead.browser.js, swapped through package.json#browser) has
// crypto.subtle only, and so AES-GCM only.
//
// Ids are the cipher names both platforms' documentation uses.
// 'aes-256-gcm' is what `true` means wherever a cipher is chosen: the one
// AEAD every browser has, hardware-accelerated on anything recent.
// 'chacha20-poly1305' is Node-only — constant-time without AES
// instructions, the better choice between two Node processes on hardware
// that lacks them.

const { createCipheriv, createDecipheriv, createSecretKey } = require('node:crypto');
const { OpenError } = require('./contracts.js');

const DEFAULT_ID = 'aes-256-gcm';
const KEY_LENGTH = 32;
const NONCE_LENGTH = 12;
const TAG_LENGTH = 16;
const TAG_OPTIONS = { authTagLength: TAG_LENGTH };

const ALGORITHMS = Object.freeze([DEFAULT_ID, 'chacha20-poly1305']);

const checkKey = (raw) => {
  if (!(raw instanceof Uint8Array) || raw.length !== KEY_LENGTH) {
    throw new TypeError(`encryption: a key is ${KEY_LENGTH} bytes`);
  }
};

const checkNonce = (nonce) => {
  // GCM takes an IV of any length without complaint, and two ends that
  // disagree on it would simply never open each other's messages.
  if (nonce.length !== NONCE_LENGTH) throw new TypeError(`encryption: a nonce is ${NONCE_LENGTH} bytes`);
};

const cipherKey = (algorithm, raw) => {
  checkKey(raw);
  // A KeyObject is no faster than the raw bytes (bench/encryption.js); it
  // is a COPY, so the caller may wipe what it passed in.
  const key = createSecretKey(raw);
  return {
    seal(nonce, plaintext, aad) {
      checkNonce(nonce);
      const cipher = createCipheriv(algorithm, key, nonce, TAG_OPTIONS);
      if (aad !== undefined && aad !== null) cipher.setAAD(aad);
      return Buffer.concat([cipher.update(plaintext), cipher.final(), cipher.getAuthTag()]);
    },
    open(nonce, sealed, aad) {
      checkNonce(nonce);
      if (sealed.length < TAG_LENGTH) throw new OpenError();
      const split = sealed.length - TAG_LENGTH;
      const decipher = createDecipheriv(algorithm, key, nonce, TAG_OPTIONS);
      try {
        decipher.setAuthTag(sealed.subarray(split));
        if (aad !== undefined && aad !== null) decipher.setAAD(aad);
        // Both are stream modes: update() answers every byte and final()
        // only verifies, so the body is returned as it came rather than
        // copied through Buffer.concat — 4.7 µs against 5.9 on 16 KB
        // (bench/encryption.js, the open rows).
        const body = decipher.update(sealed.subarray(0, split));
        decipher.final();
        return body;
      } catch {
        throw new OpenError();
      }
    },
  };
};

/**
 * A platform AEAD by name. An unknown name is a TypeError; one this
 * platform lacks answers `null` under `optional` (a preference list moves
 * on) and throws otherwise — Node has both, so that only happens in the
 * browser half.
 */
const aead = ({ algorithm = DEFAULT_ID } = {}) => {
  if (!ALGORITHMS.includes(algorithm)) {
    throw new TypeError(`encryption: unknown cipher ${JSON.stringify(algorithm)} — ${ALGORITHMS.join(' or ')}`);
  }
  return Object.freeze({
    id: algorithm,
    keyLength: KEY_LENGTH,
    nonceLength: NONCE_LENGTH,
    tagLength: TAG_LENGTH,
    key: (raw) => cipherKey(algorithm, raw),
  });
};

module.exports = { aead, ALGORITHMS, DEFAULT_ID, KEY_LENGTH, NONCE_LENGTH, TAG_LENGTH };
