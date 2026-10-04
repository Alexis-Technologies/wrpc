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
// This process's own salts kept for skipping its echoes (see `mine`).
const MINE_KEPT = 16;

const SUITES = Object.freeze({ __proto__: null, 'aes-256-gcm': SUITE_AES, 'chacha20-poly1305': SUITE_CHACHA });

// → { cipher, injected }: `injected` tells the sealer whose key bytes it
// may wipe (see `owned` in createEnvelopeSealer).
const resolveCipher = (value, name) => {
  if (value === undefined || value === null || value === true) return { cipher: aead(), injected: false };
  if (typeof value === 'string') return { cipher: aead({ algorithm: value }), injected: false };
  if (!isCipher(value)) throw new TypeError(`${name}: cipher must be a cipher name or a Cipher`);
  // The nonce is the message counter, 64 bits at its end.
  if (value.nonceLength < 8) throw new TypeError(`${name}: cipher needs a nonce of 8 bytes or more`);
  // Probed where it is built, like a codec in normalizeSyncCompression: a
  // cipher over crypto.subtle answers promises, and nothing here can wait.
  // A round trip under a random key, so a cipher that seals but cannot
  // open — or opens anything — fails at boot, not in a log line later.
  const synchronous = `${name}: cipher must answer synchronously on this carrier`;
  const key = value.key(crypto.randomBytes(value.keyLength));
  if (isPromise(key)) throw new TypeError(synchronous);
  const nonce = new Uint8Array(value.nonceLength);
  const sealed = key.seal(nonce, Buffer.from('probe'), null);
  if (isPromise(sealed)) throw new TypeError(synchronous);
  let opened = null;
  try {
    opened = key.open(nonce, sealed, null);
  } catch {
    opened = null;
  }
  if (isPromise(opened)) throw new TypeError(synchronous);
  if (opened === null || Buffer.from(opened).toString() !== 'probe') {
    throw new TypeError(`${name}: cipher does not open what it sealed`);
  }
  return { cipher: value, injected: true };
};

/**
 * `encryption` of a Node↔Node carrier → frozen `{ keys, cipher, seal,
 * acceptPlaintext, replayWindow, maxSenders }`, or null for off. `seal` and
 * `acceptPlaintext` are the rollout: a pub/sub backplane delivers at most
 * once, so switching every instance at the same instant is not an option —
 * first every instance learns to OPEN (`seal: false, acceptPlaintext:
 * true`), then every instance seals (`acceptPlaintext: true`), then the
 * plaintext is refused (neither).
 */
const normalizeEnvelopeEncryption = (value, name) => {
  if (value === undefined || value === null || value === false) return null;
  if (typeof value !== 'object' || Array.isArray(value) || value.keys === undefined) {
    throw new TypeError(
      `${name}: encryption must be { keys, cipher?, seal?, acceptPlaintext?, replayWindow?, maxSenders? }`,
    );
  }
  const {
    seal = true,
    acceptPlaintext = false,
    replayWindow = DEFAULT_REPLAY_WINDOW,
    maxSenders = MAX_SENDERS,
  } = value;
  if (typeof seal !== 'boolean') throw new TypeError(`${name}: encryption.seal must be a boolean`);
  if (typeof acceptPlaintext !== 'boolean') {
    throw new TypeError(`${name}: encryption.acceptPlaintext must be a boolean`);
  }
  if (replayWindow !== false && !(Number.isInteger(replayWindow) && replayWindow > 0 && replayWindow <= 65536)) {
    throw new TypeError(`${name}: encryption.replayWindow must be false or an integer from 1 to 65536`);
  }
  if (!(Number.isInteger(maxSenders) && maxSenders > 0 && maxSenders <= 1048576)) {
    throw new TypeError(`${name}: encryption.maxSenders must be an integer from 1 to 1048576`);
  }
  if (!seal && !acceptPlaintext) {
    throw new TypeError(`${name}: encryption with seal: false must accept plaintext — it sends nothing else`);
  }
  const { cipher, injected } = resolveCipher(value.cipher, `${name}: encryption`);
  return Object.freeze({
    keys: normalizeKeys(value.keys, `${name}: encryption.keys`),
    cipher,
    injected,
    seal,
    acceptPlaintext,
    replayWindow: replayWindow === false ? 0 : replayWindow,
    maxSenders,
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
 * `echo: true` opens them like any other — a store reads back what it wrote.
 */
const createEnvelopeSealer = ({ encryption, layer, echo = false, randomBytes = crypto.randomBytes }) => {
  const { keys, cipher, injected = false, replayWindow, maxSenders = MAX_SENDERS } = encryption;
  const ciphers = new Map([[SUITES[cipher.id] ?? SUITE_INJECTED, cipher]]);
  // The ciphers built HERE — whose key() copies the bytes into a KeyObject
  // — are the ones whose subkey may be wiped after key(). An injected
  // cipher may close over `raw` (the guide's form does) and OWNS it from
  // key() on: wiping it left such a cipher sealing under all zeros, with
  // nothing to notice — its own opener saw the same zeros. Decided by how
  // the cipher arrived, never by its id: an injected one may name a suite.
  const owned = new WeakSet();
  if (!injected) owned.add(cipher);
  // A built-in suite is opened whichever one this instance seals with, so a
  // change of cipher is a config change, not a rollout.
  for (const id in SUITES) {
    if (ciphers.has(SUITES[id])) continue;
    const built = aead({ algorithm: id });
    ciphers.set(SUITES[id], built);
    owned.add(built);
  }
  const suite = SUITES[cipher.id] ?? SUITE_INJECTED;
  const label = `wrpc ${layer} v1`;
  const aadPrefix = `wrpc-sealed v1\0${layer}\0`;

  const derive = (kid, suiteByte, salt) => {
    const master = keys.get(kid);
    if (master === null) return null;
    const active = ciphers.get(suiteByte);
    const info = Buffer.from(`${label}\0${kid}\0${active.id}`);
    const subkey = Buffer.from(crypto.hkdfSync('sha256', master, salt, info, active.keyLength));
    const key = active.key(subkey);
    if (owned.has(active)) subkey.fill(0);
    return key;
  };

  const nonceOf = (active, counter) => {
    const nonce = Buffer.alloc(active.nonceLength);
    writeCounter(nonce, active.nonceLength - 8, counter);
    return nonce;
  };

  // The sending half: one salt, one derived key, one counter — rebuilt when
  // the current kid changes (a provider rotated) or the counter is spent.
  let sender = null;
  // The salts this process sealed under, to skip its own echoes: the last
  // MINE_KEPT — an echo arrives within moments of its send, and the set
  // grew by one salt every reseed and every rotation, for the life of the
  // process.
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
    mine.add(header.latin1Slice(1, 2 + SALT_LENGTH));
    if (mine.size > MINE_KEPT) mine.delete(mine.values().next().value);
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

  // The receiving half: one entry per (kid, suite, sender salt), least
  // recently opened first. The SUITE byte is part of the key because it is part of what the
  // subkey was derived for: were a known salt looked up without it, a copy
  // with the byte changed would be opened under the cached key by an
  // instance that knows the sender and refused by one that does not.
  // Senders are processes — and, on a broker's RPC address, every client
  // process — so the map turns over with restarts and with them; a
  // forgotten sender is derived again at its next message, with a FRESH
  // replay window. That is what `maxSenders` bounds: memory on one side,
  // how many live senders keep their window on the other. Least recently
  // used, not oldest: a busy sender evicted by a newcomer came back with a
  // fresh window, and a frame of its that had just been refused as a
  // replay opened a second time. The touch is one delete and one set on a
  // hit: 2.66 -> 2.71 µs at 64 B, 2.80 -> 2.88 at 1 KB (medians of five
  // processes, the hit path the decode row of bench/encryption.js runs).
  const senders = new Map();

  const refuse = (reason) => {
    const error = new OpenError();
    error.reason = reason;
    throw error;
  };

  const remember = (id, entry) => {
    if (senders.size >= maxSenders) senders.delete(senders.keys().next().value);
    senders.set(id, entry);
  };

  const open = (kid, sealed, channel) => {
    if (sealed.length < HEADER_LENGTH || sealed[0] !== VERSION) refuse('format');
    const active = ciphers.get(sealed[1]);
    if (active === undefined || sealed.length < HEADER_LENGTH + active.tagLength) refuse('format');
    // suite ‖ salt: one slice, 17 bytes where it was 16.
    const saltKey = sealed.latin1Slice(1, 2 + SALT_LENGTH);
    if (!echo && mine.has(saltKey)) return null;
    const counter = readCounter(sealed, 2 + SALT_LENGTH);
    const id = `${kid}\0${saltKey}`;
    let entry = senders.get(id);
    const known = entry !== undefined;
    if (!known) {
      const key = derive(kid, sealed[1], sealed.subarray(2, 2 + SALT_LENGTH));
      if (key === null) refuse('kid');
      entry = { key, window: replayWindow === 0 ? null : new ReplayWindow(replayWindow) };
    } else if (keys.get(kid) === null) {
      // The ring is asked for a KNOWN sender too: its salt is on the wire,
      // so whoever holds a withdrawn key can seal under a salt this instance
      // remembers — and the cached subkey would open it until the restart.
      // One lookup a message, inside the noise of the open itself: 2.6 µs
      // at 64 B before and after on a ring, +0.04 µs through a provider
      // (medians of separate processes, the sealed row of
      // bench/encryption.js).
      senders.delete(id);
      refuse('kid');
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
    else {
      senders.delete(id);
      senders.set(id, entry);
    }
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
