'use strict';

// End-to-end sealing between two CLIENTS, through a server that relays what
// it cannot read: one message, sealed to the recipient's public key with
// HPKE (hpke.js) — and, with a sender identity, in AUTH mode, so that
// "sealed to me" also says by whom. The sealed value is bytes; wrpc carries
// bytes as they are — in a call, an event, a room broadcast, across the
// backplane — so the relay is an ordinary handler that forwards `data`.
//
//   sealed = enc (32) ‖ AEAD( plaintext )
//
// What this is NOT: a messaging protocol. One key pair per identity, no
// forward secrecy on the recipient's side (a stolen identity key opens every
// message ever sealed to it), no group key, no replay memory — an
// application that needs those runs Double Ratchet or MLS over these same
// bytes. And in a browser it protects against a server that READS, not one
// that serves the page a different script: the origin that ships the code
// can ship other code.

const { aead } = require('./aead.js');
const { x25519 } = require('./dh.js');
const { createKdf } = require('./hkdf.js');
const { createHpke, dhKem } = require('./hpke.js');
const { utf8, concat, randomSource } = require('./bytes.js');

const DEFAULT_INFO = utf8('wrpc e2ee v1');
const KEY_LENGTH = 32;

const suite = (options) => {
  const kdf = createKdf();
  const kem = dhKem(options.dh ?? x25519(), kdf);
  return { kem, hpke: createHpke({ kem, kdf, cipher: options.cipher ?? aead() }) };
};

const asBytes = (data) => (typeof data === 'string' ? utf8(data) : data);

const infoOf = (info) => (info === undefined || info === null ? DEFAULT_INFO : concat(DEFAULT_INFO, asBytes(info)));

const checkKey = (value, name) => {
  if (!(value instanceof Uint8Array) || value.length !== KEY_LENGTH) {
    throw new TypeError(`e2ee: ${name} must be ${KEY_LENGTH} bytes`);
  }
  return value;
};

/**
 * An identity: `{ seed, publicKey, keyPair }`. The SEED is the secret — 32
 * bytes to keep wherever this client keeps secrets (and the same identity
 * again from the same seed); the public key is what others seal to and
 * verify against; `keyPair` is what `createSealer`/`createOpener` take.
 */
const createIdentity = async (seed = null, options = {}) => {
  const secret = seed === null ? randomSource(options.crypto)(KEY_LENGTH) : Uint8Array.from(checkKey(seed, 'seed'));
  const keyPair = await suite(options).kem.keyPair(secret);
  return Object.freeze({ seed: secret, publicKey: keyPair.publicKey, keyPair });
};

/**
 * `seal(data, aad?) -> Promise<bytes>` to one recipient. With `senderKey`
 * (an identity's `keyPair`) the recipient can check who sealed it. `info`
 * says what the messages are FOR — a room, a conversation id — and one
 * sealed for one purpose does not open for another.
 */
const createSealer = (options = {}) => {
  const recipientPublicKey = checkKey(options.recipientPublicKey, 'recipientPublicKey');
  const { hpke } = suite(options);
  const setup = { info: infoOf(options.info), senderKey: options.senderKey ?? null };
  return Object.freeze({
    async seal(data, aad = null) {
      // A context per message: nothing to keep in step between two clients
      // that may never be online together.
      const { enc, context } = await hpke.setupSender(recipientPublicKey, setup);
      return concat(enc, await context.seal(aad === null ? null : asBytes(aad), asBytes(data)));
    },
  });
};

/**
 * `open(sealed, aad?) -> Promise<bytes>` with this client's identity. With
 * `senderPublicKey`, a message that identity did not seal does not open —
 * without it, anyone who knows the public key could have sent it.
 */
const createOpener = (options = {}) => {
  const { keyPair } = options;
  if (!keyPair || !(keyPair.publicKey instanceof Uint8Array)) throw new TypeError('e2ee: keyPair must be an identity');
  const senderPublicKey =
    options.senderPublicKey === undefined || options.senderPublicKey === null
      ? null
      : checkKey(options.senderPublicKey, 'senderPublicKey');
  const { hpke } = suite(options);
  const setup = { info: infoOf(options.info), senderPublicKey };
  return Object.freeze({
    async open(sealed, aad = null) {
      if (!(sealed instanceof Uint8Array) || sealed.length <= hpke.encLength) {
        throw new TypeError('e2ee: a sealed message is bytes');
      }
      const context = await hpke.setupRecipient(sealed.subarray(0, hpke.encLength), keyPair, setup);
      return context.open(aad === null ? null : asBytes(aad), sealed.subarray(hpke.encLength));
    },
  });
};

module.exports = { createIdentity, createSealer, createOpener };
