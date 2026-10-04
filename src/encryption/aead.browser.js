'use strict';

// The platform's AEADs, browser half: crypto.subtle, which is all a page
// has — asynchronous, and AES-GCM only ('chacha20-poly1305' is a WICG
// proposal no browser ships; a page that wants it injects a Cipher). A key
// is imported once into a NON-EXTRACTABLE CryptoKey: script that runs later
// in the page can use it, never read it.
//
// `subtle` is a parameter, as in src/webrtc/assertions.js: the default is
// the platform's, and a page served over plain http has none — which is
// refused where the cipher is built, not at the first message.

const { OpenError } = require('./contracts.js');
const { requireSubtle } = require('./bytes.js');

const DEFAULT_ID = 'aes-256-gcm';
const KEY_LENGTH = 32;
const NONCE_LENGTH = 12;
const TAG_LENGTH = 16;

const ALGORITHMS = Object.freeze([DEFAULT_ID, 'chacha20-poly1305']);

const EMPTY = new Uint8Array(0);

const params = (nonce, aad) => {
  if (nonce.length !== NONCE_LENGTH) throw new TypeError(`encryption: a nonce is ${NONCE_LENGTH} bytes`);
  return { name: 'AES-GCM', iv: nonce, additionalData: aad ?? EMPTY, tagLength: TAG_LENGTH * 8 };
};

const asBytes = (buffer) => new Uint8Array(buffer);

const refuse = () => {
  throw new OpenError();
};

const cipherKey = async (subtle, raw) => {
  if (!(raw instanceof Uint8Array) || raw.length !== KEY_LENGTH) {
    throw new TypeError(`encryption: a key is ${KEY_LENGTH} bytes`);
  }
  const key = await subtle.importKey('raw', raw, 'AES-GCM', false, ['encrypt', 'decrypt']);
  return {
    seal: async (nonce, plaintext, aad) => asBytes(await subtle.encrypt(params(nonce, aad), key, plaintext)),
    open: async (nonce, sealed, aad) => {
      const algorithm = params(nonce, aad);
      if (sealed.length < TAG_LENGTH) refuse();
      return subtle.decrypt(algorithm, key, sealed).then(asBytes, refuse);
    },
  };
};

const aead = ({ algorithm = DEFAULT_ID, optional = false, subtle = globalThis.crypto?.subtle } = {}) => {
  if (!ALGORITHMS.includes(algorithm)) {
    throw new TypeError(`encryption: unknown cipher ${JSON.stringify(algorithm)} — ${ALGORITHMS.join(' or ')}`);
  }
  if (algorithm !== DEFAULT_ID) {
    if (optional) return null;
    throw new TypeError(`encryption: ${algorithm} is not in this platform's WebCrypto — inject a Cipher`);
  }
  requireSubtle(subtle, 'encryption');
  return Object.freeze({
    id: algorithm,
    keyLength: KEY_LENGTH,
    nonceLength: NONCE_LENGTH,
    tagLength: TAG_LENGTH,
    key: (raw) => cipherKey(subtle, raw),
  });
};

module.exports = { aead, ALGORITHMS, DEFAULT_ID, KEY_LENGTH, NONCE_LENGTH, TAG_LENGTH };
