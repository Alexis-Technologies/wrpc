'use strict';

// The Noise Protocol Framework (revision 34), the part of it a wrpc session
// uses: the one-round-trip patterns NN, NK and XX, the psk0 modifier on NN,
// over X25519, AES-256-GCM or ChaCha20-Poly1305, and SHA-256 — canonical
// names, checked against the cacophony vectors (tests/encryption/noise.test.js).
// One file for both platforms: the primitives arrive as the structural
// contracts of contracts.js (a Dh, a Cipher, a Kdf), so every step that
// touches one is awaited — crypto.subtle answers promises — and an
// application injects its own the same way (ML-KEM is not a Dh; a hybrid
// handshake is a different `Handshake`, not a parameter of this one).
//
//   NN      -> e                     anonymous: encrypted, nobody authenticated
//           <- e, ee
//   NK   <- s                        the client pinned the server's static key:
//        ...                         the server is authenticated, the client is
//           -> e, es                 anyone — what TLS gives a browser
//           <- e, ee
//   XX      -> e                     both statics travel (the client's under
//           <- e, ee, s, es          encryption): mutual authentication, the
//           -> s, se                 server learns WHO in `authorize`
//   NNpsk0  -> psk, e                NN under a pre-shared key: two Node
//           <- e, ee                 processes with a secret and no PKI
//
// A handshake runs per handshake; what runs per message is a CipherState
// (the last class here), and it is written for that.

const { concat, utf8, isZero } = require('./bytes.js');
const { isPromise } = require('../compression/ids.js');

const EMPTY = new Uint8Array(0);
const KEY_LENGTH = 32;
const ZERO_KEY = new Uint8Array(KEY_LENGTH);
// Noise caps a HANDSHAKE message at 65535 bytes; wrpc's transport messages
// are bounded by `maxMessage` instead, and the cipher does not care.
const MAX_HANDSHAKE_MESSAGE = 65535;

// Pre-messages and message patterns. `i`/`r` is who writes.
const PATTERNS = Object.freeze({
  __proto__: null,
  NN: { pre: [], messages: [['e'], ['e', 'ee']] },
  NK: {
    pre: ['rs'],
    messages: [
      ['e', 'es'],
      ['e', 'ee'],
    ],
  },
  XX: { pre: [], messages: [['e'], ['e', 'ee', 's', 'es'], ['s', 'se']] },
  NNpsk0: {
    pre: [],
    psk: true,
    messages: [
      ['psk', 'e'],
      ['e', 'ee'],
    ],
  },
});

// The name a cipher has in a Noise protocol name, and the way ITS nonce
// holds the 64-bit counter: big-endian for AESGCM, little-endian for
// ChaChaPoly (Noise §12), after four zero bytes either way. An injected
// cipher names itself and gets the big-endian layout at the end of whatever
// nonce length it declared.
const CIPHER_NAMES = Object.freeze({ __proto__: null, 'aes-256-gcm': 'AESGCM', 'chacha20-poly1305': 'ChaChaPoly' });

const cipherName = (cipher) => CIPHER_NAMES[cipher.id] ?? cipher.id;

const MAX_NONCE = Number.MAX_SAFE_INTEGER;

const nonceOf = (cipher, counter) => {
  const nonce = new Uint8Array(cipher.nonceLength);
  const end = cipher.nonceLength;
  const high = Math.floor(counter / 0x100000000);
  const low = counter >>> 0;
  if (cipher.id === 'chacha20-poly1305') {
    nonce[4] = low & 0xff;
    nonce[5] = (low >>> 8) & 0xff;
    nonce[6] = (low >>> 16) & 0xff;
    nonce[7] = low >>> 24;
    nonce[8] = high & 0xff;
    nonce[9] = (high >>> 8) & 0xff;
    nonce[10] = (high >>> 16) & 0xff;
    nonce[11] = high >>> 24;
    return nonce;
  }
  nonce[end - 8] = high >>> 24;
  nonce[end - 7] = (high >>> 16) & 0xff;
  nonce[end - 6] = (high >>> 8) & 0xff;
  nonce[end - 5] = high & 0xff;
  nonce[end - 4] = low >>> 24;
  nonce[end - 3] = (low >>> 16) & 0xff;
  nonce[end - 2] = (low >>> 8) & 0xff;
  nonce[end - 1] = low & 0xff;
  return nonce;
};

// The nonce Noise reserves for REKEY: 2^64 - 1, which no message counter
// reaches — eight 0xff bytes in either byte order.
const rekeyNonce = (cipher) => {
  const nonce = new Uint8Array(cipher.nonceLength);
  nonce.fill(0xff, cipher.nonceLength - 8);
  return nonce;
};

/**
 * One direction of an established session: a key, a message counter as the
 * nonce, and a deterministic rekey every `rekeyAfter` messages — both ends
 * count the same messages, so nothing announces it. `encrypt`/`decrypt`
 * answer bytes when the cipher does and a promise when it does (or while a
 * rekey over crypto.subtle is still resolving); the COUNTER is taken
 * synchronously either way, so calls made in order are sealed in order and
 * a Sequencer only has to keep the results so. Which is also why it
 * advances on a decrypt that FAILS — Noise §5.1 leaves n unchanged there
 * — a deliberate departure: an asynchronous cipher answers after the next
 * call took its nonce, and a failed open ends the session in every
 * caller (client.js, server.js), so no later frame is opened under it.
 */
class CipherState {
  #cipher;
  #key;
  #counter = 0;
  #rekeyAfter;
  #sinceRekey = 0;

  constructor(cipher, key, rekeyAfter = 0) {
    this.#cipher = cipher;
    this.#key = key;
    this.#rekeyAfter = rekeyAfter;
  }

  get counter() {
    return this.#counter;
  }

  #next() {
    if (this.#counter >= MAX_NONCE) throw new Error('encryption: the message counter is spent');
    if (this.#rekeyAfter > 0 && this.#sinceRekey === this.#rekeyAfter) this.rekey();
    this.#sinceRekey++;
    return nonceOf(this.#cipher, this.#counter++);
  }

  encrypt(plaintext, ad = EMPTY) {
    const nonce = this.#next();
    const key = this.#key;
    return isPromise(key) ? key.then((k) => k.seal(nonce, plaintext, ad)) : key.seal(nonce, plaintext, ad);
  }

  decrypt(sealed, ad = EMPTY) {
    const nonce = this.#next();
    const key = this.#key;
    return isPromise(key) ? key.then((k) => k.open(nonce, sealed, ad)) : key.open(nonce, sealed, ad);
  }

  /** Noise §11.3: k = ENCRYPT(k, 2^64 - 1, "", zeros). One-way: the old key cannot be had from the new. */
  rekey() {
    const cipher = this.#cipher;
    const keyOf = (sealed) => cipher.key(sealed.subarray(0, KEY_LENGTH));
    const derive = (key) => {
      const sealed = key.seal(rekeyNonce(cipher), ZERO_KEY, EMPTY);
      return isPromise(sealed) ? sealed.then(keyOf) : keyOf(sealed);
    };
    const current = this.#key;
    const next = isPromise(current) ? current.then(derive) : derive(current);
    this.#key = next;
    this.#sinceRekey = 0;
    // Back to the synchronous path once the promise has settled.
    if (isPromise(next)) {
      next.then(
        (key) => {
          if (this.#key === next) this.#key = key;
        },
        () => {},
      );
    }
  }
}

class SymmetricState {
  #kdf;
  #cipher;
  ck;
  h;
  #key = null;
  #n = 0;

  constructor(kdf, cipher) {
    this.#kdf = kdf;
    this.#cipher = cipher;
  }

  async initialize(name) {
    const bytes = utf8(name);
    this.h =
      bytes.length <= this.#kdf.hashLength
        ? concat(bytes, new Uint8Array(this.#kdf.hashLength - bytes.length))
        : await this.#kdf.hash(bytes);
    this.ck = this.h;
  }

  async mixHash(data) {
    this.h = await this.#kdf.hash(concat(this.h, data));
  }

  async #hkdf(ikm, outputs) {
    // Noise's HKDF is RFC 5869 with the chaining key as the salt and no info.
    return this.#kdf.expand(await this.#kdf.extract(this.ck, ikm), EMPTY, outputs * this.#kdf.hashLength);
  }

  async #setKey(bytes) {
    this.#key = await this.#cipher.key(bytes.slice(0, KEY_LENGTH));
    this.#n = 0;
  }

  async mixKey(ikm) {
    const out = await this.#hkdf(ikm, 2);
    this.ck = out.slice(0, 32);
    await this.#setKey(out.subarray(32, 64));
  }

  async mixKeyAndHash(ikm) {
    const out = await this.#hkdf(ikm, 3);
    this.ck = out.slice(0, 32);
    await this.mixHash(out.subarray(32, 64));
    await this.#setKey(out.subarray(64, 96));
  }

  async encryptAndHash(plaintext) {
    const out =
      this.#key === null ? plaintext : await this.#key.seal(nonceOf(this.#cipher, this.#n++), plaintext, this.h);
    await this.mixHash(out);
    return out;
  }

  async decryptAndHash(sealed) {
    const out = this.#key === null ? sealed : await this.#key.open(nonceOf(this.#cipher, this.#n++), sealed, this.h);
    await this.mixHash(sealed);
    return out;
  }

  get keyed() {
    return this.#key !== null;
  }

  async split(rekeyAfter) {
    const out = await this.#hkdf(EMPTY, 2);
    const first = new CipherState(this.#cipher, await this.#cipher.key(out.slice(0, 32)), rekeyAfter);
    const second = new CipherState(this.#cipher, await this.#cipher.key(out.slice(32, 64)), rekeyAfter);
    return [first, second];
  }
}

class HandshakeState {
  #symmetric;
  #dh;
  #pattern;
  #initiator;
  #s;
  #e = null;
  #rs;
  #re = null;
  #psk;
  #fixedEphemeral;
  #index = 0;
  #rekeyAfter;
  #tagLength;

  constructor(noise, initiator, options) {
    this.#symmetric = new SymmetricState(noise.kdf, noise.cipher);
    this.#dh = noise.dh;
    this.#pattern = PATTERNS[noise.pattern];
    this.#initiator = initiator;
    this.#s = options.staticKey ?? null;
    this.#rs = options.remoteStatic ?? null;
    this.#psk = options.psk ?? null;
    this.#fixedEphemeral = options.ephemeral ?? null;
    this.#rekeyAfter = options.rekeyAfter ?? 0;
    this.#tagLength = noise.cipher.tagLength;
  }

  async initialize(name, prologue) {
    await this.#symmetric.initialize(name);
    await this.#symmetric.mixHash(prologue);
    // The one pre-message in use: the responder's static, known to both.
    if (this.#pattern.pre.includes('rs')) {
      await this.#symmetric.mixHash(this.#initiator ? this.#rs : this.#s.publicKey);
    }
    return this;
  }

  /** True while it is this side's turn to write. */
  get writing() {
    return !this.done && (this.#index % 2 === 0) === this.#initiator;
  }

  get done() {
    return this.#index >= this.#pattern.messages.length;
  }

  /** The peer's static public key, once it has arrived (XX) or as pinned (NK). */
  get remoteStatic() {
    return this.#rs;
  }

  // A DH token names the initiator's key, then the responder's: 'es' is the
  // initiator's ephemeral with the responder's static, whoever computes it.
  async #dhToken(token) {
    const localKind = this.#initiator ? token[0] : token[1];
    const remoteKind = this.#initiator ? token[1] : token[0];
    const local = localKind === 'e' ? this.#e : this.#s;
    const remote = remoteKind === 'e' ? this.#re : this.#rs;
    await this.#symmetric.mixKey(await this.#dh.dh(local.privateKey, remote));
  }

  async write(payload = EMPTY) {
    if (!this.writing) throw new Error("encryption: not this side's turn to write");
    const parts = [];
    for (const token of this.#pattern.messages[this.#index]) {
      if (token === 'e') {
        this.#e = this.#fixedEphemeral
          ? await this.#dh.keyPair(this.#fixedEphemeral)
          : await this.#dh.generateKeyPair();
        parts.push(this.#e.publicKey);
        await this.#symmetric.mixHash(this.#e.publicKey);
        if (this.#pattern.psk) await this.#symmetric.mixKey(this.#e.publicKey);
      } else if (token === 's') {
        parts.push(await this.#symmetric.encryptAndHash(this.#s.publicKey));
      } else if (token === 'psk') {
        await this.#symmetric.mixKeyAndHash(this.#psk);
      } else {
        await this.#dhToken(token);
      }
    }
    parts.push(await this.#symmetric.encryptAndHash(payload));
    this.#index++;
    const message = concat(...parts);
    if (message.length > MAX_HANDSHAKE_MESSAGE) throw new RangeError('encryption: handshake message too long');
    return message;
  }

  async read(message) {
    if (this.done || this.writing) throw new Error("encryption: not this side's turn to read");
    if (!(message instanceof Uint8Array) || message.length > MAX_HANDSHAKE_MESSAGE) {
      throw new Error('encryption: malformed handshake message');
    }
    const length = this.#dh.publicLength;
    let offset = 0;
    const take = (size) => {
      if (offset + size > message.length) throw new Error('encryption: malformed handshake message');
      const part = message.subarray(offset, offset + size);
      offset += size;
      return part;
    };
    for (const token of this.#pattern.messages[this.#index]) {
      if (token === 'e') {
        this.#re = Uint8Array.from(take(length));
        if (isZero(this.#re)) throw new Error('encryption: malformed handshake message');
        await this.#symmetric.mixHash(this.#re);
        if (this.#pattern.psk) await this.#symmetric.mixKey(this.#re);
      } else if (token === 's') {
        const size = this.#symmetric.keyed ? length + this.#tagLength : length;
        this.#rs = Uint8Array.from(await this.#symmetric.decryptAndHash(take(size)));
      } else if (token === 'psk') {
        await this.#symmetric.mixKeyAndHash(this.#psk);
      } else {
        await this.#dhToken(token);
      }
    }
    const payload = await this.#symmetric.decryptAndHash(message.subarray(offset));
    this.#index++;
    return payload;
  }

  /**
   * After the last message: `{ send, receive, handshakeHash, remoteStatic }`.
   * The hash is unique to this handshake and both sides hold the same one —
   * what an application binds a credential to (channel binding).
   */
  async finish() {
    if (!this.done) throw new Error('encryption: the handshake is not finished');
    const [first, second] = await this.#symmetric.split(this.#rekeyAfter);
    return {
      send: this.#initiator ? first : second,
      receive: this.#initiator ? second : first,
      handshakeHash: this.#symmetric.h,
      remoteStatic: this.#rs,
    };
  }
}

const checkKey = (value, name) => {
  if (!(value instanceof Uint8Array) || value.length !== KEY_LENGTH) {
    throw new TypeError(`noise: ${name} must be 32 bytes`);
  }
};

/**
 * `{ name, pattern, initiator(options), responder(options) }` for one Noise
 * protocol. Options: `prologue` (bytes both sides must agree on — anything
 * negotiated before the handshake goes here, so tampering with it fails the
 * handshake), `staticKey` (a Dh key pair: the responder's in NK, both in
 * XX), `remoteStatic` (the initiator's pinned server key in NK), `psk`
 * (NNpsk0), `rekeyAfter`. `ephemeral` fixes the ephemeral key — for test
 * vectors, and nothing else.
 */
const createNoise = ({ pattern, dh, cipher, kdf }) => {
  if (!(pattern in PATTERNS)) {
    throw new TypeError(`noise: unknown pattern ${JSON.stringify(pattern)} — ${Object.keys(PATTERNS).join(', ')}`);
  }
  const name = `Noise_${pattern}_${dh.id}_${cipherName(cipher)}_${kdf.id}`;
  const noise = { pattern, dh, cipher, kdf };
  const start = (initiator, options = {}) => {
    const needsStatic = pattern === 'XX' || (pattern === 'NK' && !initiator);
    if (needsStatic && !options.staticKey) throw new TypeError(`noise: ${pattern} needs a staticKey on this side`);
    if (pattern === 'NK' && initiator) checkKey(options.remoteStatic, 'remoteStatic');
    if (PATTERNS[pattern].psk) checkKey(options.psk, 'psk');
    return new HandshakeState(noise, initiator, options).initialize(name, options.prologue ?? EMPTY);
  };
  return Object.freeze({
    name,
    pattern,
    /** The cipher as the protocol name spells it: 'AESGCM', 'ChaChaPoly', or an injected cipher's id. */
    cipher: cipherName(cipher),
    initiator: (options) => start(true, options),
    responder: (options) => start(false, options),
  });
};

const PATTERN_NAMES = Object.freeze(Object.keys(PATTERNS));

module.exports = { createNoise, CipherState, PATTERN_NAMES, cipherName, MAX_HANDSHAKE_MESSAGE };
