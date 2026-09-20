'use strict';

// @alexify/wrpc/encryption, the half a browser gets — and the whole of the
// primitives: every file required here runs on both platforms (aead.js is
// swapped for aead.browser.js through package.json#browser). index.js adds
// what only a Node process does.
//
// Opt-in like every knob in wrpc, and never a substitute for TLS: `wss://`
// first. This is for where TLS ends before the data does — a backplane, a
// broker's log, a TLS-terminating proxy, a relay that should not read what
// it relays. See docs/guide/encryption.md.

const { aead, ALGORITHMS } = require('./aead.js');
const { x25519 } = require('./dh.js');
const { createKdf } = require('./hkdf.js');
const { normalizeKeys, generateKey, isKid } = require('./keyring.js');
const { OpenError, isCipher, isCipherKey, isDh, isKeyProvider } = require('./contracts.js');
const { toBase64Url, fromBase64, equal } = require('./bytes.js');
const { createEncryption, isEncryption } = require('./client.js');
const { createNoise, PATTERN_NAMES } = require('./noise.js');
const { parseBundle } = require('./statics.js');

module.exports = {
  createEncryption,
  isEncryption,
  createNoise,
  PATTERN_NAMES,
  parseBundle,
  aead,
  ALGORITHMS,
  x25519,
  createKdf,
  normalizeKeys,
  generateKey,
  isKid,
  OpenError,
  isCipher,
  isCipherKey,
  isDh,
  isKeyProvider,
  toBase64Url,
  fromBase64,
  equal,
};
