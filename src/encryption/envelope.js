'use strict';

// Sealed envelopes for the Node↔Node carriers — the rooms backplane, the
// cluster channels, the brokers: many senders, many receivers, one keyring,
// no handshake, no ordering across senders. Node-only, and synchronous end
// to end: a backplane's receive path cannot wait (src/compression/sync.js
// makes the same demand of a codec).
//
//   u8 version (1) ‖ u8 suite ‖ salt16 ‖ u64 counter ‖ ciphertext ‖ tag
//
// A RANDOM 96-bit nonce is what this layout exists to avoid: GCM's bound
// for random nonces is 2^32 messages a key, which a busy fan-out reaches in
// hours. So every sender derives ITS OWN key — HKDF(master, salt) with a
// salt drawn once per process (and again after 2^32 messages) — and counts
// under it: a (key, nonce) pair cannot repeat without two processes
// drawing the same 16 random bytes. A receiver derives the same key from
// the salt it reads, once per sender (5.7 µs — bench/encryption.js), and
// keeps it.
//
// What the receiver keeps is inserted only AFTER a tag verified, so a flood
// of made-up salts costs an attacker's HKDF each and evicts nobody. The kid
// SELECTS the key and the suite byte the cipher — nothing is ever tried
// until it fits (a partitioning oracle against a non-committing AEAD). The
// additional data binds the layer, the kid and the CHANNEL: an envelope
// lifted from one room's channel does not open on another's.

const crypto = require('node:crypto');
const { aead } = require('./aead.js');
const { isCipher, OpenError } = require('./contracts.js');
const { normalizeKeys } = require('./keyring.js');
const { isPromise } = require('../compression/ids.js');

const VERSION = 1;
const SALT_LENGTH = 16;
const HEADER_LENGTH = 2 + SALT_LENGTH + 8;
const SUITE_AES = 1;
const SUITE_CHACHA = 2;
// An injected cipher: both ends were handed the same one, so the byte only
// says "not a built-in".
const SUITE_INJECTED = 0xff;
const RESEED_AFTER = 0x100000000;
const DEFAULT_REPLAY_WINDOW = 1024;
const MAX_SENDERS = 1024;

const SUITES = Object.freeze({ __proto__: null, 'aes-256-gcm': SUITE_AES, 'chacha20-poly1305': SUITE_CHACHA });

const resolveCipher = (value, name) => {
  if (value === undefined || value === null || value === true) return aead();
  if (typeof value === 'string') return aead({ algorithm: value });
  if (!isCipher(value)) throw new TypeError(`${name}: cipher must be a cipher name or a Cipher`);
  // The nonce is the message counter, 64 bits at its end.
  if (value.nonceLength < 8) throw new TypeError(`${name}: cipher needs a nonce of 8 bytes or more`);
  // Probed where it is built, like a codec in normalizeSyncCompression: a
  // cipher over crypto.subtle answers promises, and nothing here can wait.
  const key = value.key(new Uint8Array(value.keyLength));
  if (isPromise(key) || isPromise(key.seal(new Uint8Array(value.nonceLength), new Uint8Array(1), null))) {
    throw new TypeError(`${name}: cipher must answer synchronously on this carrier`);
  }
  return value;
};

/**
 * `encryption` of a Node↔Node carrier → frozen `{ keys, cipher, seal,
 * acceptPlaintext, replayWindow }`, or null for off. `seal` and
 * `acceptPlaintext` are the rollout: a pub/sub backplane delivers at most
 * once, so switching every instance at the same instant is not an option —
 * first every instance learns to OPEN (`seal: false, acceptPlaintext:
 * true`), then every instance seals (`acceptPlaintext: true`), then the
 * plaintext is refused (neither).
 */
const normalizeEnvelopeEncryption = (value, name) => {
  if (value === undefined || value === null || value === false) return null;
  if (typeof value !== 'object' || Array.isArray(value) || value.keys === undefined) {
    throw new TypeError(`${name}: encryption must be { keys, cipher?, seal?, acceptPlaintext?, replayWindow? }`);
  }
  const { seal = true, acceptPlaintext = false, replayWindow = DEFAULT_REPLAY_WINDOW } = value;
  if (typeof seal !== 'boolean') throw new TypeError(`${name}: encryption.seal must be a boolean`);
  if (typeof acceptPlaintext !== 'boolean') {
    throw new TypeError(`${name}: encryption.acceptPlaintext must be a boolean`);
  }
  if (replayWindow !== false && !(Number.isInteger(replayWindow) && replayWindow > 0 && replayWindow <= 65536)) {
    throw new TypeError(`${name}: encryption.replayWindow must be false or an integer from 1 to 65536`);
  }
  if (!seal && !acceptPlaintext) {
    throw new TypeError(`${name}: encryption with seal: false must accept plaintext — it sends nothing else`);
  }
  return Object.freeze({
    keys: normalizeKeys(value.keys, `${name}: encryption.keys`),
    cipher: resolveCipher(value.cipher, `${name}: encryption`),
    seal,
    acceptPlaintext,
    replayWindow: replayWindow === false ? 0 : replayWindow,
  });
};

/**
 * A sliding anti-replay window over one sender's counters, as IPsec and
 * DTLS keep: the highest counter seen and a ring of flags under it. A
 * receiver hears only the channels it subscribed to, so GAPS are normal and
 * cost nothing; what is refused is a counter seen before, or one older than
 * the window.
 */
class ReplayWindow {
  #seen;
  #size;
  #top = -1;

  constructor(size) {
    this.#size = size;
    this.#seen = new Uint8Array(size);
  }

  /** True when `counter` is fresh — and marks it; call only after the tag verified. */
  accept(counter) {
    const size = this.#size;
    const top = this.#top;
    if (counter > top) {
      const advance = counter - top;
      if (advance >= size) this.#seen.fill(0);
      else for (let c = top + 1; c < counter; c++) this.#seen[c % size] = 0;
      this.#seen[counter % size] = 1;
      this.#top = counter;
      return true;
    }
    if (top - counter >= size) return false;
    if (this.#seen[counter % size] === 1) return false;
    this.#seen[counter % size] = 1;
    return true;
  }
}

const writeCounter = (buffer, offset, counter) => {
  buffer.writeUInt32BE(Math.floor(counter / 0x100000000), offset);
  buffer.writeUInt32BE(counter >>> 0, offset + 4);
};

const readCounter = (buffer, offset) => buffer.readUInt32BE(offset) * 0x100000000 + buffer.readUInt32BE(offset + 4);

/**
 * `{ seal(bytes, channel) -> { kid, sealed }, open(kid, sealed, channel) ->
 * bytes }` for one layer. `open` throws OpenError for every refusal, with
 * `reason` ('format', 'kid', 'open', 'replay') set for the SERVER's log —
 * it never travels. An envelope sealed by this same sealer answers null:
 * a pub/sub backplane echoes every publish to its publisher, which would
 * otherwise decrypt and parse each of its own messages only to drop them.
 */
const createEnvelopeSealer = ({ encryption, layer, randomBytes = crypto.randomBytes }) => {
  const { keys, cipher, replayWindow } = encryption;
  const ciphers = new Map([[SUITES[cipher.id] ?? SUITE_INJECTED, cipher]]);
  // A built-in suite is opened whichever one this instance seals with, so a
  // change of cipher is a config change, not a rollout.
  for (const id in SUITES) if (!ciphers.has(SUITES[id])) ciphers.set(SUITES[id], aead({ algorithm: id }));
  const suite = SUITES[cipher.id] ?? SUITE_INJECTED;
  const label = `wrpc ${layer} v1`;
  const aadPrefix = `wrpc-sealed v1\0${layer}\0`;

  const derive = (kid, suiteByte, salt) => {
    const master = keys.get(kid);
    if (master === null) return null;
    const active = ciphers.get(suiteByte);
    const info = Buffer.from(`${label}\0${kid}\0${active.id}`);
    const subkey = Buffer.from(crypto.hkdfSync('sha256', master, salt, info, active.keyLength));
    try {
      return active.key(subkey);
    } finally {
      subkey.fill(0);
    }
  };

  const nonceOf = (active, counter) => {
    const nonce = Buffer.alloc(active.nonceLength);
    writeCounter(nonce, active.nonceLength - 8, counter);
    return nonce;
  };

  // The sending half: one salt, one derived key, one counter — rebuilt when
  // the current kid changes (a provider rotated) or the counter is spent.
  let sender = null;
  const mine = new Set();

  const senderFor = (kid) => {
    if (sender !== null && sender.kid === kid && sender.counter < RESEED_AFTER) return sender;
    const salt = randomBytes(SALT_LENGTH);
    const key = derive(kid, suite, salt);
    if (key === null) throw new Error(`encryption: the keyring does not hold its current key ${JSON.stringify(kid)}`);
    const header = Buffer.alloc(HEADER_LENGTH);
    header[0] = VERSION;
    header[1] = suite;
    salt.copy(header, 2);
    mine.add(salt.toString('latin1'));
    sender = { kid, key, header, counter: 0 };
    return sender;
  };

  const seal = (bytes, channel) => {
    const kid = keys.current;
    const state = senderFor(kid);
    const counter = state.counter++;
    const body = state.key.seal(nonceOf(cipher, counter), bytes, Buffer.from(`${aadPrefix}${kid}\0${channel}`));
    const sealed = Buffer.allocUnsafe(HEADER_LENGTH + body.length);
    state.header.copy(sealed, 0);
    writeCounter(sealed, 2 + SALT_LENGTH, counter);
    sealed.set(body, HEADER_LENGTH);
    return { kid, sealed };
  };

  // The receiving half: one entry per (kid, sender salt), oldest first.
  // Senders are processes, so the map only turns over with restarts, and a
  // forgotten sender is derived again at its next message — with a fresh
  // replay window, which is why the cap is generous rather than tight.
  const senders = new Map();

  const refuse = (reason) => {
    const error = new OpenError();
    error.reason = reason;
    throw error;
  };

  const remember = (id, entry) => {
    if (senders.size >= MAX_SENDERS) senders.delete(senders.keys().next().value);
    senders.set(id, entry);
  };

  const open = (kid, sealed, channel) => {
    if (sealed.length < HEADER_LENGTH || sealed[0] !== VERSION) refuse('format');
    const active = ciphers.get(sealed[1]);
    if (active === undefined || sealed.length < HEADER_LENGTH + active.tagLength) refuse('format');
    const saltKey = sealed.latin1Slice(2, 2 + SALT_LENGTH);
    if (mine.has(saltKey)) return null;
    const counter = readCounter(sealed, 2 + SALT_LENGTH);
    const id = `${kid}\0${saltKey}`;
    let entry = senders.get(id);
    const known = entry !== undefined;
    if (!known) {
      const key = derive(kid, sealed[1], sealed.subarray(2, 2 + SALT_LENGTH));
      if (key === null) refuse('kid');
      entry = { key, window: replayWindow === 0 ? null : new ReplayWindow(replayWindow) };
    }
    let bytes;
    try {
      bytes = entry.key.open(
        nonceOf(active, counter),
        sealed.subarray(HEADER_LENGTH),
        Buffer.from(`${aadPrefix}${kid}\0${channel}`),
      );
    } catch {
      refuse('open');
    }
    // Only now is the counter the sender's own, and the salt worth keeping.
    if (entry.window !== null && !entry.window.accept(counter)) refuse('replay');
    if (!known) remember(id, entry);
    return bytes;
  };

  return { seal, open };
};

module.exports = {
  normalizeEnvelopeEncryption,
  createEnvelopeSealer,
  ReplayWindow,
  HEADER_LENGTH,
  DEFAULT_REPLAY_WINDOW,
  MAX_SENDERS,
};
