'use strict';

// X25519 (RFC 7748), browser half: crypto.subtle, in the shape Noise and
// HPKE's DHKEM consume — Chrome 133, Firefox 130 and Safari 17 have it. The
// id is the name Noise gives the function. The Node half (dh.js, swapped
// through package.json#browser) is node:crypto: the same algorithm through
// subtle prints an ExperimentalWarning on the early Node 22 releases that
// `engines` still admits.
//
// Private keys stay opaque: a generated one is a non-extractable CryptoKey
// whose bytes never exist in script; one derived from a seed is imported
// and the PKCS#8 scratch copy wiped. Public keys are the 32 bytes that
// travel.

const { requireSubtle, isZero } = require('./bytes.js');

const ALGORITHM = { name: 'X25519' };
const LENGTH = 32;

// RFC 8410: the PKCS#8 wrapping of a raw X25519 private key is this prefix
// and the 32 bytes — the only form crypto.subtle imports one in.
const PKCS8_PREFIX = Uint8Array.of(
  ...[0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x6e, 0x04, 0x22, 0x04, 0x20],
);

// The public key IS the function applied to the base point (RFC 7748 §6.1),
// which is how one is had from a seed without ever exporting the private key.
const BASE_POINT = new Uint8Array(LENGTH);
BASE_POINT[0] = 9;

const x25519 = ({ subtle = globalThis.crypto?.subtle } = {}) => {
  requireSubtle(subtle, 'encryption');

  const dh = async (privateKey, publicKey) => {
    if (!(publicKey instanceof Uint8Array) || publicKey.length !== LENGTH) {
      throw new TypeError(`encryption: an X25519 public key is ${LENGTH} bytes`);
    }
    // A low-order point makes the output zero whatever the private key is
    // (RFC 7748 §6.1) — an attacker's way to force a known secret. Some
    // platforms throw for it, some answer the zeros; both end here, as one
    // error that says nothing about which.
    let shared = null;
    try {
      const peer = await subtle.importKey('raw', publicKey, ALGORITHM, false, []);
      shared = new Uint8Array(await subtle.deriveBits({ name: 'X25519', public: peer }, privateKey, LENGTH * 8));
    } catch {
      shared = null;
    }
    if (shared === null || isZero(shared)) throw new Error('encryption: invalid public key');
    return shared;
  };

  const generateKeyPair = async () => {
    const pair = await subtle.generateKey(ALGORITHM, false, ['deriveBits']);
    const publicKey = new Uint8Array(await subtle.exportKey('raw', pair.publicKey));
    return { publicKey, privateKey: pair.privateKey };
  };

  const keyPair = async (seed) => {
    if (!(seed instanceof Uint8Array) || seed.length !== LENGTH) {
      throw new TypeError(`encryption: an X25519 private key is ${LENGTH} bytes`);
    }
    const pkcs8 = new Uint8Array(PKCS8_PREFIX.length + LENGTH);
    pkcs8.set(PKCS8_PREFIX, 0);
    pkcs8.set(seed, PKCS8_PREFIX.length);
    try {
      const privateKey = await subtle.importKey('pkcs8', pkcs8, ALGORITHM, false, ['deriveBits']);
      return { publicKey: await dh(privateKey, BASE_POINT), privateKey };
    } finally {
      pkcs8.fill(0);
    }
  };

  return Object.freeze({ id: '25519', publicLength: LENGTH, generateKeyPair, keyPair, dh });
};

module.exports = { x25519 };
