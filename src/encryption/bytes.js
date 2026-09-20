'use strict';

// Byte plumbing for @alexify/wrpc/encryption — browser-safe and require-free
// (no Buffer: this file is bundled for a page). Everything here is a cold
// path: key parsing, handshake assembly, labels. The per-message paths build
// their frames by hand where they live.

const encoder = new TextEncoder();

const utf8 = (text) => encoder.encode(text);

const isBytes = (value) => value instanceof Uint8Array;

/** One array out of several; `concat()` is the empty array. */
const concat = (...parts) => {
  let total = 0;
  for (let i = 0; i < parts.length; i++) total += parts[i].length;
  const out = new Uint8Array(total);
  let offset = 0;
  for (let i = 0; i < parts.length; i++) {
    out.set(parts[i], offset);
    offset += parts[i].length;
  }
  return out;
};

/**
 * Constant-time equality: the XOR of every pair is folded, so the time
 * taken says nothing about WHERE two values differ. A length mismatch
 * answers at once — lengths are public here (a tag, a key, a digest).
 */
const equal = (a, b) => {
  if (a.length !== b.length) return false;
  let folded = 0;
  for (let i = 0; i < a.length; i++) folded |= a[i] ^ b[i];
  return folded === 0;
};

const isZero = (bytes) => {
  let folded = 0;
  for (let i = 0; i < bytes.length; i++) folded |= bytes[i];
  return folded === 0;
};

// base64url without Buffer — the spelling of src/webrtc/assertions.js:
// btoa/atob exist in every browser and in Node, the values are a key or a
// handshake message, and never `String.fromCharCode(...bytes)` (apply in
// disguise).
const toBase64Url = (bytes) => {
  let binary = '';
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
};

const BASE64 = /^[A-Za-z0-9+/_-]*={0,2}$/;

/** base64 or base64url, padded or not → bytes; null when it is neither. */
const fromBase64 = (text) => {
  if (typeof text !== 'string' || !BASE64.test(text)) return null;
  const bare = text.replace(/=+$/, '');
  if (bare.length % 4 === 1) return null;
  let binary;
  try {
    binary = atob(bare.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - (bare.length % 4)) % 4));
  } catch {
    return null;
  }
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
};

const HEX = /^(?:[0-9a-fA-F]{2})*$/;

const fromHex = (text) => {
  if (typeof text !== 'string' || !HEX.test(text)) return null;
  const bytes = new Uint8Array(text.length / 2);
  for (let i = 0; i < bytes.length; i++) bytes[i] = Number.parseInt(text.slice(i * 2, i * 2 + 2), 16);
  return bytes;
};

const toHex = (bytes) => {
  let text = '';
  for (let i = 0; i < bytes.length; i++) text += bytes[i].toString(16).padStart(2, '0');
  return text;
};

/**
 * A 96-bit AEAD nonce from a message counter: four zero bytes, then the
 * counter as 64 bits big-endian — the layout Noise gives AESGCM, used for
 * every counter nonce in this package. A counter is a Number, exact to
 * 2^53; every caller rekeys or reseeds long before that.
 */
const counterNonce = (counter, out = new Uint8Array(12)) => {
  const high = Math.floor(counter / 0x100000000);
  const low = counter >>> 0;
  out[0] = out[1] = out[2] = out[3] = 0;
  out[4] = high >>> 24;
  out[5] = (high >>> 16) & 0xff;
  out[6] = (high >>> 8) & 0xff;
  out[7] = high & 0xff;
  out[8] = low >>> 24;
  out[9] = (low >>> 16) & 0xff;
  out[10] = (low >>> 8) & 0xff;
  out[11] = low & 0xff;
  return out;
};

/**
 * The platform's CSPRNG, or a refusal. `Math.random` is never a fallback
 * here (src/runtime/browser.js has one, for correlation ids): a key or a
 * salt from it would be a vulnerability that looks like a feature.
 */
const randomSource = (crypto = globalThis.crypto) => {
  if (!crypto || typeof crypto.getRandomValues !== 'function') {
    throw new TypeError('encryption: crypto.getRandomValues is required (a secure context in a browser)');
  }
  return (length) => crypto.getRandomValues(new Uint8Array(length));
};

/** `crypto.subtle`, or the refusal every WebCrypto-backed factory shares. */
const requireSubtle = (subtle, name) => {
  if (!subtle || typeof subtle.importKey !== 'function') {
    throw new TypeError(`${name}: WebCrypto (crypto.subtle) is required — a secure context in a browser`);
  }
  return subtle;
};

module.exports = {
  utf8,
  isBytes,
  concat,
  equal,
  isZero,
  toBase64Url,
  fromBase64,
  fromHex,
  toHex,
  counterNonce,
  randomSource,
  requireSubtle,
};
