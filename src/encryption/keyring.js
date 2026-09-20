'use strict';

// Keys by id. Everything symmetric in @alexify/wrpc/encryption — a backplane
// envelope, a broker body, a sealed session — is sealed under the keyring's
// CURRENT key and names that key's id beside the ciphertext, so the ring can
// hold the previous keys for as long as messages sealed under them may still
// arrive. Rotation is then a deploy that adds a key, a deploy that makes it
// current, and a deploy that drops the old one — never a flag day.
//
// A kid travels in the clear and comes back from a peer, so it is a closed
// alphabet and the ring is a Map: `ring['__proto__']` must be a miss, not
// an object. A kid SELECTS the key; nothing here ever tries key after key
// on one message (a partitioning oracle against a non-committing AEAD).

const { isKeyProvider } = require('./contracts.js');
const { fromBase64, fromHex, randomSource } = require('./bytes.js');

const KEY_LENGTH = 32;
const KID = /^[A-Za-z0-9._-]{1,32}$/;
// What a single bare key is filed under.
const DEFAULT_KID = '0';

const isKid = (value) => typeof value === 'string' && KID.test(value);

const isObject = (value) => typeof value === 'object' && value !== null && !Array.isArray(value);

/**
 * 32 bytes from bytes (copied — the caller may wipe its own), 64 hex
 * characters, or base64 / base64url. Hex first: 64 hex characters are
 * valid base64 too, and would decode to 48 bytes of the wrong key.
 */
const parseKey = (value, name) => {
  let bytes = null;
  if (value instanceof Uint8Array) bytes = Uint8Array.from(value);
  else if (typeof value === 'string') bytes = value.length === KEY_LENGTH * 2 ? fromHex(value) : fromBase64(value);
  if (bytes === null || bytes.length !== KEY_LENGTH) {
    throw new TypeError(`${name} must be ${KEY_LENGTH} bytes — a Uint8Array, 64 hex characters or base64`);
  }
  return bytes;
};

const checkedKid = (value, name) => {
  if (!isKid(value)) throw new TypeError(`${name} must be a key id: 1-32 of A-Z a-z 0-9 . _ -`);
  return value;
};

const fromProvider = (provider, name) =>
  Object.freeze({
    get current() {
      return checkedKid(provider.current(), `${name}.current()`);
    },
    get kids() {
      const kids = typeof provider.kids === 'function' ? provider.kids() : null;
      return Array.isArray(kids) ? kids.filter(isKid) : [this.current];
    },
    get(kid) {
      if (!isKid(kid)) return null;
      const key = provider.get(kid);
      return key instanceof Uint8Array && key.length === KEY_LENGTH ? key : null;
    },
  });

/**
 * `keys` in every spelling → `{ current, kids, get(kid) }`:
 * one key (filed under '0'), `{ current, ring: { kid: key } }`, or a
 * provider `{ current(), get(kid), kids?() }`. Strict: a ring without its
 * current key, a bad kid or a key of the wrong size is a TypeError at
 * construction, not a message that silently never opens.
 */
const normalizeKeys = (value, name = 'keys') => {
  if (isKeyProvider(value)) return fromProvider(value, name);
  const ring = new Map();
  let current = DEFAULT_KID;
  if (isObject(value) && !(value instanceof Uint8Array)) {
    if (!isObject(value.ring)) throw new TypeError(`${name}.ring must be an object of kid → key`);
    for (const kid of Object.keys(value.ring)) {
      ring.set(
        checkedKid(kid, `${name}.ring: ${JSON.stringify(kid)}`),
        parseKey(value.ring[kid], `${name}.ring.${kid}`),
      );
    }
    current = checkedKid(value.current, `${name}.current`);
    if (!ring.has(current)) throw new TypeError(`${name}.current names a key the ring does not hold`);
  } else {
    ring.set(current, parseKey(value, name));
  }
  const kids = Object.freeze(Array.from(ring.keys()));
  return Object.freeze({ current, kids, get: (kid) => ring.get(kid) ?? null });
};

/** A fresh 32-byte key from the platform's CSPRNG. */
const generateKey = ({ crypto = globalThis.crypto } = {}) => randomSource(crypto)(KEY_LENGTH);

module.exports = { normalizeKeys, parseKey, generateKey, isKid, KEY_LENGTH, DEFAULT_KID };
