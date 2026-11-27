'use strict';

// The structural seams of @alexify/wrpc/encryption. Like a Compressor or a
// Backplane, a primitive is whatever answers the right methods: the
// platform ones of this directory, or an injected one — XChaCha20 from
// @noble/ciphers, AES-GCM-SIV, AEGIS, a hardware module; ML-KEM or a hybrid
// behind the key-agreement shape. wrpc imports none of them.
//
// A Cipher is an AEAD:
//   { id, keyLength, nonceLength, tagLength, key(raw) -> CipherKey | Promise }
// and a CipherKey is one key, ready to use:
//   { seal(nonce, plaintext, aad) -> bytes | Promise   // ciphertext ‖ tag
//     open(nonce, sealed, aad)    -> bytes | Promise } // throws / rejects
// `key()` exists because of the browser: crypto.subtle imports a key ONCE
// into a non-extractable CryptoKey (a promise), and a contract of
// `seal(rawKey, …)` would pay that import per message — half as much again
// (bench/encryption.js). On Node the prepared key saves nothing measurable;
// it is the same shape so one caller serves both. From key() on, `raw`
// belongs to the cipher: wrpc neither reuses nor wipes it, so a cipher may
// keep the reference (the guide's closure form) or copy it, as it likes.
// Either method may answer a promise; a carrier with no ordering queue
// refuses a cipher that does.
//
// A Dh is a Diffie-Hellman function in the shape Noise and HPKE's DHKEM
// consume, keys as bytes on the wire and opaque in memory:
//   { id, publicLength,
//     generateKeyPair()      -> Promise<{ publicKey: bytes, privateKey }>
//     keyPair(seed)          -> Promise<{ publicKey: bytes, privateKey }>
//     dh(privateKey, public) -> Promise<bytes> }        // rejects all-zero
//
// A key provider is the keyring's injected form — a KMS or Vault client
// that unwrapped its data keys at boot and refreshes them on its own clock:
//   { current() -> kid, get(kid) -> 32 bytes | null }
// Both are SYNCHRONOUS: they are read where an envelope is opened, and a
// backplane's receive path cannot wait.

// The one failure `open` has: a wrong key, a wrong nonce, a flipped bit and
// a truncated message are indistinguishable on purpose — whoever sent the
// bytes learns nothing from which it was. Callers answer it uniformly too.
// `options.reason` is for the log alone (an envelope's `replay`, `kid`, …).
class OpenError extends Error {
  constructor(message = 'encryption: the message does not open', options = {}) {
    super(message, options);
    this.name = 'OpenError';
    this.code = 'ALX_ERR_OPEN';
    if (options.reason !== undefined) this.reason = options.reason;
  }
}

// Every other failure of the encryption layer, with what went wrong as its
// code: a handshake that broke or ran out of order (`ALX_ERR_ENCRYPTION_
// HANDSHAKE`), a key that is missing, not the pinned one or not accepted
// (`ALX_ERR_ENCRYPTION_KEY`), a sealed frame that is none (`ALX_ERR_
// ENCRYPTION_FRAME`), a session used out of its state — not established, a
// spent message counter (`ALX_ERR_ENCRYPTION_STATE`). `options.code` says
// which; a message that does not open stays an OpenError.
class EncryptionError extends Error {
  constructor(message, options = {}) {
    super(message, options);
    this.name = 'EncryptionError';
    this.code = options.code;
  }
}

const isObject = (value) => typeof value === 'object' && value !== null;

const isLength = (value) => Number.isInteger(value) && value > 0;

const isCipher = (value) =>
  isObject(value) &&
  typeof value.id === 'string' &&
  value.id.length > 0 &&
  isLength(value.keyLength) &&
  isLength(value.nonceLength) &&
  isLength(value.tagLength) &&
  typeof value.key === 'function';

const isCipherKey = (value) => isObject(value) && typeof value.seal === 'function' && typeof value.open === 'function';

const isDh = (value) =>
  isObject(value) &&
  typeof value.id === 'string' &&
  value.id.length > 0 &&
  isLength(value.publicLength) &&
  typeof value.generateKeyPair === 'function' &&
  typeof value.keyPair === 'function' &&
  typeof value.dh === 'function';

const isKeyProvider = (value) =>
  isObject(value) && typeof value.current === 'function' && typeof value.get === 'function';

module.exports = { OpenError, EncryptionError, isCipher, isCipherKey, isDh, isKeyProvider };
