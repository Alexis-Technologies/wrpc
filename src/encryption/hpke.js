'use strict';

// HPKE (RFC 9180) — "encrypt this to a public key", the standard under MLS,
// Oblivious HTTP and ECH — base, psk, auth and auth_psk modes. One file
// for both platforms, composed from the contracts of this directory (a Dh,
// a Kdf, a Cipher) and checked against the RFC's vectors
// (tests/encryption/hpke.test.js).
//
// It is what a REQUEST needs where a session is what a connection needs: no
// round trip, one message sealed to the recipient's static key — and, from
// the same context, an EXPORTER, which is how the answer to that message is
// sealed without a second key exchange (the Oblivious HTTP construction,
// RFC 9458 §4.4).
//
// The KEM is a structural seam of its own (`isKem`): `dhKem(dh, kdf)` is
// DHKEM over the platform's X25519, and an application that needs ML-KEM or
// a hybrid injects one — wrpc ships none and depends on none.
//
// What HPKE does not give: forward secrecy on the recipient's side (the
// static key opens every message ever sent to it — rotate it), and replay
// protection (the binding that uses this adds its own).

const { concat, utf8 } = require('./bytes.js');

const EMPTY = new Uint8Array(0);
const VERSION = utf8('HPKE-v1');
const MODE_BASE = 0;
const MODE_PSK = 1;
// The auth modes: the recipient also learns WHICH static key sealed the
// message (§5.1.3) — what a relayed end-to-end payload needs, where "sealed
// to me" says nothing about who by. The +1 of a psk is the low bit.
const MODE_AUTH = 2;
const KDF_HKDF_SHA256 = 0x0001;
const KEM_X25519_HKDF_SHA256 = 0x0020;
const AEAD_IDS = Object.freeze({ __proto__: null, 'aes-256-gcm': 0x0002, 'chacha20-poly1305': 0x0003 });

const i2osp = (value, length) => {
  const out = new Uint8Array(length);
  for (let i = length - 1; i >= 0; i--) {
    out[i] = value & 0xff;
    value = Math.floor(value / 256);
  }
  return out;
};

// RFC 9180 §4: every KDF call is labelled with the suite it serves.
const labelled = (kdf, suite) => ({
  extract: (salt, label, ikm) => kdf.extract(salt, concat(VERSION, suite, utf8(label), ikm)),
  expand: (prk, label, info, length) =>
    kdf.expand(prk, concat(i2osp(length, 2), VERSION, suite, utf8(label), info), length),
});

/**
 * DHKEM(dh, HKDF-SHA256) — RFC 9180 §4.1. `id` defaults to the registered
 * one for X25519; another Dh needs its own registered id.
 */
const dhKem = (dh, kdf, id = KEM_X25519_HKDF_SHA256) => {
  const kem = labelled(kdf, concat(utf8('KEM'), i2osp(id, 2)));
  const extractAndExpand = async (shared, context) =>
    kem.expand(await kem.extract(EMPTY, 'eae_prk', shared), 'shared_secret', context, 32);
  return Object.freeze({
    id,
    publicLength: dh.publicLength,
    encLength: dh.publicLength,
    secretLength: 32,
    generateKeyPair: () => dh.generateKeyPair(),
    keyPair: (seed) => dh.keyPair(seed),
    /** DeriveKeyPair (§7.1.3) for X25519: a key pair from any key material. */
    async deriveKeyPair(ikm) {
      const seed = await kem.expand(await kem.extract(EMPTY, 'dkp_prk', ikm), 'sk', EMPTY, 32);
      return dh.keyPair(seed);
    },
    /**
     * → `{ sharedSecret, enc }`. With `sender` (a key pair) it is AuthEncap:
     * the secret also depends on the sender's static key, so only its holder
     * could have produced it. `ephemeral` fixes the ephemeral key pair — for
     * test vectors, and nothing else.
     */
    async encap(recipientPublicKey, ephemeral = null, sender = null) {
      const pair = ephemeral ?? (await dh.generateKeyPair());
      const enc = pair.publicKey;
      const first = await dh.dh(pair.privateKey, recipientPublicKey);
      if (sender === null) {
        return { sharedSecret: await extractAndExpand(first, concat(enc, recipientPublicKey)), enc };
      }
      const second = await dh.dh(sender.privateKey, recipientPublicKey);
      const context = concat(enc, recipientPublicKey, sender.publicKey);
      return { sharedSecret: await extractAndExpand(concat(first, second), context), enc };
    },
    /** With `senderPublicKey` it is AuthDecap: a message the named sender did not seal does not open. */
    async decap(enc, recipient, senderPublicKey = null) {
      const first = await dh.dh(recipient.privateKey, enc);
      if (senderPublicKey === null) return extractAndExpand(first, concat(enc, recipient.publicKey));
      const second = await dh.dh(recipient.privateKey, senderPublicKey);
      return extractAndExpand(concat(first, second), concat(enc, recipient.publicKey, senderPublicKey));
    },
  });
};

const isKem = (value) =>
  typeof value === 'object' &&
  value !== null &&
  Number.isInteger(value.id) &&
  Number.isInteger(value.encLength) &&
  value.encLength > 0 &&
  typeof value.encap === 'function' &&
  typeof value.decap === 'function';

/**
 * One direction of an HPKE context: `seal`/`open` with the sequence number
 * as the nonce (XORed into the base nonce, §5.2), and `export`. The counter
 * is taken synchronously, so calls made in order are sealed in order even
 * over an asynchronous cipher.
 */
class HpkeContext {
  #key;
  #baseNonce;
  #exporterSecret;
  #expand;
  #sequence = 0;

  constructor(key, baseNonce, exporterSecret, expand) {
    this.#key = key;
    this.#baseNonce = baseNonce;
    this.#exporterSecret = exporterSecret;
    this.#expand = expand;
  }

  #nonce() {
    if (this.#sequence >= Number.MAX_SAFE_INTEGER) throw new Error('hpke: the message counter is spent');
    const nonce = Uint8Array.from(this.#baseNonce);
    const sequence = i2osp(this.#sequence++, nonce.length);
    for (let i = 0; i < nonce.length; i++) nonce[i] ^= sequence[i];
    return nonce;
  }

  seal(aad, plaintext) {
    return this.#key.seal(this.#nonce(), plaintext, aad ?? EMPTY);
  }

  open(aad, sealed) {
    return this.#key.open(this.#nonce(), sealed, aad ?? EMPTY);
  }

  /** A secret both ends can derive and nobody else: `length` bytes bound to `context`. */
  export(context, length) {
    return this.#expand(this.#exporterSecret, 'sec', context, length);
  }
}

/**
 * `{ suite, setupSender(recipientPublicKey, options), setupRecipient(enc,
 * recipientKeyPair, options) }`. Options: `info` (bytes both ends agree on —
 * what the message is FOR, so one sealed for one purpose does not open for
 * another), `psk` + `pskId` for psk mode, where the recipient also learns
 * that the sender held the pre-shared key, and `senderKey` (a key pair, on
 * the sender) with `senderPublicKey` (on the recipient) for auth mode, where
 * it learns WHICH static key sealed the message.
 */
const createHpke = ({ kem, kdf, cipher }) => {
  if (!isKem(kem)) throw new TypeError('hpke: kem must be a Kem');
  const aeadId = AEAD_IDS[cipher.id];
  if (aeadId === undefined) throw new TypeError(`hpke: no registered AEAD id for ${JSON.stringify(cipher.id)}`);
  const suite = concat(utf8('HPKE'), i2osp(kem.id, 2), i2osp(KDF_HKDF_SHA256, 2), i2osp(aeadId, 2));
  const { extract, expand } = labelled(kdf, suite);

  const schedule = async (sharedSecret, { info = EMPTY, psk = null, pskId = null } = {}, auth = false) => {
    const hasPsk = psk !== null && psk !== undefined;
    if (hasPsk !== (pskId !== null && pskId !== undefined)) throw new TypeError('hpke: psk and pskId go together');
    if (hasPsk && psk.length < 32) throw new TypeError('hpke: a psk is 32 bytes or more');
    const mode = (auth ? MODE_AUTH : MODE_BASE) + (hasPsk ? MODE_PSK : 0);
    const [pskIdHash, infoHash] = await Promise.all([
      extract(EMPTY, 'psk_id_hash', hasPsk ? pskId : EMPTY),
      extract(EMPTY, 'info_hash', info),
    ]);
    const context = concat(Uint8Array.of(mode), pskIdHash, infoHash);
    const secret = await extract(sharedSecret, 'secret', hasPsk ? psk : EMPTY);
    const [key, baseNonce, exporterSecret] = await Promise.all([
      expand(secret, 'key', context, cipher.keyLength),
      expand(secret, 'base_nonce', context, cipher.nonceLength),
      expand(secret, 'exp', context, kdf.hashLength),
    ]);
    return new HpkeContext(await cipher.key(key), baseNonce, exporterSecret, expand);
  };

  return Object.freeze({
    suite,
    aeadId,
    encLength: kem.encLength,
    async setupSender(recipientPublicKey, options = {}) {
      const sender = options.senderKey ?? null;
      const { sharedSecret, enc } = await kem.encap(recipientPublicKey, options.ephemeral ?? null, sender);
      return { enc, context: await schedule(sharedSecret, options, sender !== null) };
    },
    async setupRecipient(enc, recipient, options = {}) {
      const senderPublicKey = options.senderPublicKey ?? null;
      return schedule(await kem.decap(enc, recipient, senderPublicKey), options, senderPublicKey !== null);
    },
  });
};

module.exports = { createHpke, dhKem, isKem, AEAD_IDS, KEM_X25519_HKDF_SHA256 };
