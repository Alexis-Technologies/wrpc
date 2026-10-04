'use strict';

// X25519 (RFC 7748), Node half: node:crypto KeyObjects, in the shape Noise
// and HPKE's DHKEM consume. The browser half (dh.browser.js, swapped through
// package.json#browser) is crypto.subtle, which Node has too — but through
// it X25519 prints an ExperimentalWarning on the early Node 22 releases
// `engines` admits (22.10 does, 22.23 does not), and a library does not get
// to write on its host's stderr. The two halves answer the same RFC 7748
// vectors and each other (tests/encryption/dh.test.js).
//
// The methods answer promises although nothing here waits: one contract for
// both platforms, on a path that runs per handshake, never per message.

const nodeCrypto = require('node:crypto');
const { isZero } = require('./bytes.js');

const LENGTH = 32;

// RFC 8410: a raw X25519 key in its DER wrapping is a fixed prefix and the
// 32 bytes — SubjectPublicKeyInfo for a public key, PKCS#8 for a private one.
const SPKI_PREFIX = Buffer.from('302a300506032b656e032100', 'hex');
const PKCS8_PREFIX = Buffer.from('302e020100300506032b656e04220420', 'hex');

const x25519 = ({ crypto = nodeCrypto } = {}) => {
  // createPublicKey derives the public half of a PRIVATE KeyObject and
  // refuses a public one, so a generated pair's public key is exported as is.
  const publicBytes = (key) => {
    const publicKey = key.type === 'public' ? key : crypto.createPublicKey(key);
    return Uint8Array.from(publicKey.export({ type: 'spki', format: 'der' }).subarray(SPKI_PREFIX.length));
  };

  const dh = async (privateKey, publicKey) => {
    if (!(publicKey instanceof Uint8Array) || publicKey.length !== LENGTH) {
      throw new TypeError(`encryption: an X25519 public key is ${LENGTH} bytes`);
    }
    // A low-order point makes the output zero whatever the private key is
    // (RFC 7748 §6.1) — an attacker's way to force a known secret. OpenSSL
    // throws for it; an implementation that answered the zeros would end
    // here as well, as one error that says nothing about which.
    let shared = null;
    try {
      const peer = crypto.createPublicKey({
        key: Buffer.concat([SPKI_PREFIX, publicKey]),
        format: 'der',
        type: 'spki',
      });
      shared = Uint8Array.from(crypto.diffieHellman({ privateKey, publicKey: peer }));
    } catch {
      shared = null;
    }
    if (shared === null || isZero(shared)) throw new Error('encryption: invalid public key');
    return shared;
  };

  const generateKeyPair = async () => {
    const { privateKey, publicKey } = crypto.generateKeyPairSync('x25519');
    return { publicKey: publicBytes(publicKey), privateKey };
  };

  const keyPair = async (seed) => {
    if (!(seed instanceof Uint8Array) || seed.length !== LENGTH) {
      throw new TypeError(`encryption: an X25519 private key is ${LENGTH} bytes`);
    }
    const pkcs8 = Buffer.concat([PKCS8_PREFIX, seed]);
    try {
      const privateKey = crypto.createPrivateKey({ key: pkcs8, format: 'der', type: 'pkcs8' });
      return { publicKey: publicBytes(privateKey), privateKey };
    } finally {
      pkcs8.fill(0);
    }
  };

  return Object.freeze({ id: '25519', publicLength: LENGTH, generateKeyPair, keyPair, dh });
};

module.exports = { x25519 };
