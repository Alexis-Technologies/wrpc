'use strict';

// A server's long-lived keys, from ONE configured secret per key id. The
// session handshake (Noise) and the per-request one (HPKE) must never share
// a private key — two protocols over one key is how a proof about either
// stops holding — so each is derived under its own label, and an operator
// still stores, rotates and pins a single 32-byte value.
//
//   noise static = X25519( HKDF(secret, "wrpc noise static v1") )
//   hpke  static = X25519( HKDF(secret, "wrpc hpke static v1") )
//
// What a client pins is the BUNDLE, public and safe to publish:
//
//   <kid>:<noise public, base64url>:<hpke public, base64url>
//
// One file for both platforms: a Node server derives, a client only parses.

const { utf8, toBase64Url, fromBase64 } = require('./bytes.js');
const { isKid } = require('./keyring.js');
const { ConfigError } = require('../errors.js');

const EMPTY = new Uint8Array(0);
const NOISE_LABEL = utf8('wrpc noise static v1');
const HPKE_LABEL = utf8('wrpc hpke static v1');

const isKey = (key) => key instanceof Uint8Array && key.length === 32;

/** `{ noise, hpke }` key pairs for one keyring secret. */
const deriveStatics = async (secret, { dh, kdf }) => {
  const seeds = await Promise.all([
    kdf.derive(secret, EMPTY, NOISE_LABEL, 32),
    kdf.derive(secret, EMPTY, HPKE_LABEL, 32),
  ]);
  const noiseSeed = seeds[0];
  const hpkeSeed = seeds[1];
  try {
    const pairs = await Promise.all([dh.keyPair(noiseSeed), dh.keyPair(hpkeSeed)]);
    return { noise: pairs[0], hpke: pairs[1] };
  } finally {
    noiseSeed.fill(0);
    hpkeSeed.fill(0);
  }
};

const formatBundle = (kid, statics) =>
  `${kid}:${toBase64Url(statics.noise.publicKey)}:${toBase64Url(statics.hpke.publicKey)}`;

/** `{ kid, noise, hpke }` (public keys as bytes) from a bundle string or the same object. */
const parseBundle = (value, name = 'serverKey') => {
  let kid = '';
  let noise = null;
  let hpke = null;
  if (typeof value === 'string') {
    const parts = value.split(':');
    if (parts.length === 3) {
      kid = parts[0];
      noise = fromBase64(parts[1]);
      hpke = fromBase64(parts[2]);
    }
  } else if (typeof value === 'object' && value !== null) {
    ({ kid, noise, hpke } = value);
    if (typeof noise === 'string') noise = fromBase64(noise);
    if (typeof hpke === 'string') hpke = fromBase64(hpke);
  }
  if (!isKid(kid) || !isKey(noise) || !isKey(hpke)) {
    throw new ConfigError(`encryption: ${name} must be a key bundle — "<kid>:<noise key>:<hpke key>"`);
  }
  return { kid, noise, hpke };
};

module.exports = { deriveStatics, formatBundle, parseBundle };
