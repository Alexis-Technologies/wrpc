'use strict';

// The server half of session encryption. Node-only, and deliberately NOT a
// change to the dispatcher: `SealedSocket` wraps the engine's socket and is
// what RpcServer.attachSocket is handed instead. It runs the handshake,
// then re-announces every decrypted message exactly as an engine socket
// would — ('message', data, isBinary) — so the framed kinds, compression,
// attachments and stream chunks below it work unchanged, and compression
// lands inside the sealed frame (compress, then seal) by construction. One
// wrapper serves the built-in engine, uWebSockets.js and WebTransport.
//
// What it costs, and the bench that says so (bench/encryption.js): every
// recipient of a broadcast has its own key, so the single prepared frame a
// fan-out shares (`sendPrepared`) cannot be used — a sealed socket does not
// offer it, and an emit to N clients is N seals. permessage-deflate is told
// to leave the frames alone: ciphertext does not compress.

const { EventEmitter } = require('node:events');
const { aead, ALGORITHMS } = require('./aead.js');
const { x25519 } = require('./dh.js');
const { createKdf } = require('./hkdf.js');
const { normalizeKeys, parseKey, isKid } = require('./keyring.js');
const { createNoise, PATTERN_NAMES } = require('./noise.js');
const { deriveStatics, formatBundle } = require('./statics.js');
const { createHpke, dhKem, AEAD_IDS } = require('./hpke.js');
const { createHttpSealing, DEFAULT_MAX_SKEW } = require('./httpServer.js');
const { randomSource } = require('./bytes.js');
const { Sequencer } = require('../sequencer.js');
const { ENCRYPTION_PARAM } = require('../wire.js');
const {
  SecureChannel,
  parseHello,
  prologueOf,
  frame,
  isFrame,
  FRAME_HANDSHAKE,
  DEFAULT_REKEY_AFTER,
  DEFAULT_HANDSHAKE_TIMEOUT,
} = require('./session.js');

const DEFAULT_PATTERNS = Object.freeze(['NK', 'XX']);
// What may pile up behind an unfinished handshake: an onConnect hook or a
// broadcast can send before the peer has said anything.
const MAX_QUEUED = 256;
// What a Noise protocol name looks like — the shape a log line may carry.
const PROTOCOL_NAME = /^Noise_[A-Za-z0-9]+_[A-Za-z0-9]+_[A-Za-z0-9-]+_[A-Za-z0-9]+$/;
const NO_COMPRESS = Object.freeze({ compress: false });
const CLOSE_POLICY = 1008;
const CLOSE_PROTOCOL = 1002;

const listOf = (value, allowed, fallback, name) => {
  if (value === undefined || value === null) return fallback;
  if (!Array.isArray(value) || value.length === 0 || !value.every((entry) => allowed.includes(entry))) {
    throw new TypeError(`${name} must be a non-empty list of ${allowed.join(', ')}`);
  }
  return Object.freeze([...new Set(value)]);
};

/**
 * `encryption` of an RpcServer → the normalized option, or null for off:
 * `{ keys, required, patterns, ciphers, psk, authorize, handshakeTimeout,
 * rekeyAfter, statics(kid), bundle() }`. `keys` is the usual keyring — one
 * secret per kid, from which the static key pairs are derived (statics.js);
 * the CURRENT kid is what discovery publishes, and a client names the kid it
 * pinned, so an old pin keeps working while its key is on the ring.
 */
const normalizeServerEncryption = (value, name) => {
  if (value === undefined || value === null || value === false) return null;
  if (typeof value !== 'object' || Array.isArray(value) || value.keys === undefined) {
    throw new TypeError(`${name}: encryption must be { keys, required?, patterns?, ciphers?, psk?, authorize?, … }`);
  }
  const {
    required = false,
    authorize = null,
    psk = null,
    handshakeTimeout = DEFAULT_HANDSHAKE_TIMEOUT,
    rekeyAfter = DEFAULT_REKEY_AFTER,
    maxSkew = DEFAULT_MAX_SKEW,
    replay = null,
    discovery = true,
  } = value;
  if (!(Number.isInteger(maxSkew) && maxSkew > 0)) {
    throw new TypeError(`${name}: encryption.maxSkew must be a positive integer (ms)`);
  }
  // A shared memory (`seen`), or the built-in cache's own knobs.
  if (replay !== null && (typeof replay !== 'object' || Array.isArray(replay))) {
    throw new TypeError(`${name}: encryption.replay must be { seen(id, ttl) } or { max, overflow }`);
  }
  if (replay !== null && replay.seen !== undefined && typeof replay.seen !== 'function') {
    throw new TypeError(`${name}: encryption.replay must be { seen(id, ttl) } or { max, overflow }`);
  }
  if (replay !== null && typeof replay.seen !== 'function') {
    const { max, overflow } = replay;
    if (max !== undefined && !(Number.isInteger(max) && max > 0)) {
      throw new TypeError(`${name}: encryption.replay.max must be a positive integer`);
    }
    if (overflow !== undefined && overflow !== 'refuse' && overflow !== 'evict') {
      throw new TypeError(`${name}: encryption.replay.overflow must be 'refuse' or 'evict'`);
    }
  }
  if (typeof discovery !== 'boolean') throw new TypeError(`${name}: encryption.discovery must be a boolean`);
  if (typeof required !== 'boolean') throw new TypeError(`${name}: encryption.required must be a boolean`);
  if (authorize !== null && typeof authorize !== 'function') {
    throw new TypeError(`${name}: encryption.authorize must be a function`);
  }
  if (!(Number.isInteger(handshakeTimeout) && handshakeTimeout > 0)) {
    throw new TypeError(`${name}: encryption.handshakeTimeout must be a positive integer`);
  }
  if (!(Number.isInteger(rekeyAfter) && rekeyAfter >= 0)) {
    throw new TypeError(`${name}: encryption.rekeyAfter must be a non-negative integer`);
  }
  const patterns = listOf(value.patterns, PATTERN_NAMES, DEFAULT_PATTERNS, `${name}: encryption.patterns`);
  const ciphers = listOf(value.ciphers, ALGORITHMS, ALGORITHMS, `${name}: encryption.ciphers`);
  const sharedKey = psk === null ? null : parseKey(psk, `${name}: encryption.psk`);
  if (patterns.includes('NNpsk0') && sharedKey === null) {
    throw new TypeError(`${name}: encryption.patterns lists NNpsk0, which needs encryption.psk`);
  }
  const keys = normalizeKeys(value.keys, `${name}: encryption.keys`);
  const dh = x25519();
  const kdf = createKdf();
  // One Noise object per protocol name a client may send, by that name: a
  // name arrives from the peer and is looked up, never parsed.
  const protocols = new Map();
  for (const pattern of patterns) {
    for (const id of ciphers) {
      const noise = createNoise({ pattern, dh, cipher: aead({ algorithm: id }), kdf });
      protocols.set(noise.name, noise);
    }
  }
  // The per-request binding's suites, by the AEAD id a request names.
  const kem = dhKem(dh, kdf);
  const suites = new Map();
  for (const id of ciphers) {
    const cipher = aead({ algorithm: id });
    suites.set(AEAD_IDS[id], { cipher, hpke: createHpke({ kem, kdf, cipher }) });
  }
  // kid → promise of the derived static key pairs, derived once.
  const derived = new Map();
  const statics = (kid) => {
    let pending = derived.get(kid);
    if (pending === undefined) {
      const secret = keys.get(kid);
      if (secret === null) return null;
      pending = deriveStatics(secret, { dh, kdf });
      derived.set(kid, pending);
      // A derivation that failed is not the answer for this kid forever:
      // forgotten, so the next hello derives again (and the cached
      // rejection is not an unhandled one when nobody is waiting on it).
      pending.catch(() => {
        if (derived.get(kid) === pending) derived.delete(kid);
      });
    }
    return pending;
  };
  return Object.freeze({
    keys,
    required,
    patterns,
    ciphers,
    psk: sharedKey,
    authorize,
    handshakeTimeout,
    rekeyAfter,
    protocols,
    statics,
    discovery,
    /** The per-request (HPKE) half, built by the core with its logger and its way of refusing. */
    http: ({ log, refuse, reserved = null }) =>
      createHttpSealing({
        suites,
        statics,
        kdf,
        maxSkew,
        replay: replay ?? undefined,
        random: randomSource(),
        log,
        refuse,
        reserved,
      }),
    /** The public bundle of the current key — what a client pins, safe to publish. */
    bundle: async () => formatBundle(keys.current, await statics(keys.current)),
  });
};

/** True when the connect URL announces session encryption. */
const wantsEncryption = (url) => {
  if (typeof url !== 'string') return false;
  const query = url.indexOf('?');
  if (query === -1) return false;
  return new URLSearchParams(url.slice(query + 1)).get(ENCRYPTION_PARAM) === '1';
};

const asBuffer = (bytes) =>
  Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);

/**
 * An engine socket under session encryption. `ready` resolves with the
 * session's facts — `{ protocol, pattern, cipher, kid, remoteStatic,
 * handshakeHash }` — and never rejects: a connection that fails or is
 * refused resolves `null` after the socket was closed, so whoever awaits it
 * (the session restore) needs no error path of its own.
 */
class SealedSocket extends EventEmitter {
  #socket;
  #encryption;
  #kind;
  #log;
  #channel = null;
  #handshake = null;
  #noise = null;
  #kid = '';
  #outbound;
  #inbound;
  #queue = [];
  // The queue holds direct sends and shared (broadcast) messages in one
  // order, counted apart: a direct flood before the handshake closes the
  // connection, a broadcast flood drops for THIS client and is said once.
  #queuedShared = 0;
  #droppedShared = 0;
  #pending = 0;
  #chain = Promise.resolve();
  #dead = false;
  #record;
  #timer;
  #resolve;

  constructor(socket, { encryption, kind = 'ws', log, refuse = null, record = null }) {
    super();
    this.#socket = socket;
    this.#encryption = encryption;
    this.#kind = kind;
    this.#log = log;
    this.#record = record;
    this.ready = new Promise((resolve) => {
      this.#resolve = resolve;
    });
    const violation = () => this.#end(CLOSE_PROTOCOL, 'crypto');
    this.#outbound = new Sequencer(violation);
    this.#inbound = new Sequencer(violation);
    socket.on('close', (...args) => {
      this.#dead = true;
      clearTimeout(this.#timer);
      this.#resolve(null);
      this.emit('close', ...args);
    });
    socket.on('drain', () => this.emit('drain'));
    socket.on('error', (error) => this.emit('error', error));
    if (refuse !== null) return void this.#end(CLOSE_POLICY, refuse);
    this.#timer = setTimeout(() => this.#end(CLOSE_POLICY, 'timeout'), encryption.handshakeTimeout);
    if (typeof this.#timer.unref === 'function') this.#timer.unref();
    socket.on('message', (data, isBinary) => this.#onMessage(data, isBinary));
  }

  get remoteAddress() {
    return this.#socket.remoteAddress;
  }

  get protocol() {
    return this.#socket.protocol;
  }

  get bufferedAmount() {
    return this.#socket.bufferedAmount ?? 0;
  }

  // One reason on the wire for every failure — which check it was is for
  // this log only. 1008 is policy (refused, timed out, not allowed), 1002
  // anything that did not verify, open or parse.
  // ONE line per refusal, `encryption.refused`, at the level the reason
  // deserves: a peer's doing is warn; a failure on this side — a key
  // provider that threw, an authorize hook that threw — is error, with the
  // err. `extra` carries what is safe to say (a kid that IS a kid, a
  // protocol name that IS one; else their lengths — peer text stays out).
  #end(code, reason, extra = null, level = 'warn') {
    if (this.#dead) return;
    this.#dead = true;
    clearTimeout(this.#timer);
    this.#queue.length = 0;
    this.#queuedShared = 0;
    this.#droppedShared = 0;
    this.#log[level]({ ...extra, event: 'encryption.refused', reason, kind: this.#kind });
    this.#record?.(reason);
    this.#resolve(null);
    if (typeof this.#socket.close === 'function') this.#socket.close(code, 'encryption');
    else this.#socket.terminate();
  }

  #onMessage(data, isBinary) {
    if (this.#dead) return;
    if (!isBinary) return void this.#end(CLOSE_PROTOCOL, 'plaintext');
    const bytes = asBuffer(data);
    // Established and nothing of the handshake still in flight: the hot path.
    if (this.#channel !== null && this.#pending === 0) return void this.#open(bytes);
    this.#pending++;
    this.#chain = this.#chain
      .then(() => this.#step(bytes))
      .then(
        () => void this.#pending--,
        (error) => {
          this.#log.debug?.({ event: 'encryption.handshake', err: error });
          this.#end(CLOSE_PROTOCOL, 'handshake');
        },
      );
  }

  #open(bytes) {
    let message;
    try {
      message = this.#channel.open(bytes);
    } catch {
      return void this.#end(CLOSE_PROTOCOL, 'crypto');
    }
    this.#inbound.push(
      message,
      (value) => {
        if (!this.#dead) this.emit('message', value, typeof value !== 'string');
      },
      () => this.#end(CLOSE_PROTOCOL, 'crypto'),
    );
  }

  async #step(bytes) {
    if (this.#dead) return;
    if (this.#channel !== null) return void this.#open(bytes);
    if (this.#handshake === null) return this.#hello(bytes);
    if (!isFrame(bytes, FRAME_HANDSHAKE)) throw new Error('not a handshake frame');
    await this.#handshake.read(bytes.subarray(2));
    await this.#establish();
  }

  async #hello(bytes) {
    const hello = parseHello(bytes);
    if (hello === null) throw new Error('malformed hello');
    const encryption = this.#encryption;
    const noise = encryption.protocols.get(hello.name);
    // Refused, never negotiated: answering with what WOULD be accepted is
    // the downgrade this design does not have. The name goes in the line
    // only when it is shaped like one; anything else is its length.
    if (noise === undefined) {
      const named = PROTOCOL_NAME.test(hello.name) && hello.name.length <= 64;
      return void this.#end(
        CLOSE_POLICY,
        'protocol',
        named ? { protocol: hello.name } : { nameLength: hello.name.length },
      );
    }
    // NN and NNpsk0 name no server key; the others name the one they pinned.
    const usesStatic = noise.pattern === 'NK' || noise.pattern === 'XX';
    const kid = usesStatic && hello.kid === '' ? encryption.keys.current : hello.kid;
    let statics = null;
    if (usesStatic) {
      // A key provider that throws is this side's failure, not the peer's.
      try {
        statics = await encryption.statics(kid);
      } catch (error) {
        return void this.#end(CLOSE_POLICY, 'keys', { err: error, ...(isKid(kid) ? { kid } : {}) }, 'error');
      }
      if (statics === null) {
        return void this.#end(CLOSE_POLICY, 'kid', isKid(kid) ? { kid } : { kidLength: kid.length });
      }
    }
    this.#noise = noise;
    this.#kid = usesStatic ? kid : '';
    this.#handshake = await noise.responder({
      prologue: prologueOf(this.#kind, hello.header),
      staticKey: statics === null ? undefined : statics.noise,
      psk: encryption.psk ?? undefined,
      rekeyAfter: encryption.rekeyAfter,
    });
    await this.#handshake.read(hello.message);
    this.#socket.send(asBuffer(frame(FRAME_HANDSHAKE, await this.#handshake.write())), NO_COMPRESS);
    if (this.#handshake.done) await this.#establish();
  }

  async #establish() {
    const done = await this.#handshake.finish();
    const noise = this.#noise;
    const info = Object.freeze({
      protocol: noise.name,
      pattern: noise.pattern,
      cipher: noise.cipher,
      kid: this.#kid,
      remoteStatic: done.remoteStatic,
      handshakeHash: done.handshakeHash,
    });
    const { authorize } = this.#encryption;
    if (authorize !== null) {
      let allowed;
      try {
        allowed = await authorize(info);
      } catch (error) {
        // The hook threw: this side's failure, said as one — not a peer's
        // 'handshake' at debug, which is where it used to land.
        return void this.#end(CLOSE_POLICY, 'hook', { err: error }, 'error');
      }
      if (allowed === false) return void this.#end(CLOSE_POLICY, 'authorize');
    }
    if (this.#dead) return;
    clearTimeout(this.#timer);
    this.#channel = new SecureChannel(done);
    this.#handshake = null;
    this.#log.debug?.({ event: 'encryption.established', protocol: noise.name, kind: this.#kind });
    this.#record?.('established');
    const queue = this.#queue;
    this.#queue = [];
    this.#queuedShared = 0;
    for (let i = 0; i < queue.length; i++) this.send(queue[i]);
    if (this.#droppedShared > 0) {
      this.#log.warn({ event: 'encryption.queue.dropped', count: this.#droppedShared, kind: this.#kind });
      this.#droppedShared = 0;
    }
    this.#resolve(info);
  }

  // A message prepared once for a fan-out (Broadcast.emit): the engine's
  // sendPrepared would write its frames as they are, which a sealed
  // connection cannot — every recipient seals its own copy. Before the
  // handshake it is held like a direct send, but under its own bound: a
  // busy room must not cost a connecting client its connection, so past
  // the bound the rest is dropped for this client, counted, and said once
  // when the handshake completes. A client is a member of nothing before
  // open() resolves; a broadcast during its handshake is best effort.
  sendPrepared(message) {
    if (this.#dead) return false;
    if (this.#channel !== null) return this.send(message.text);
    if (this.#queuedShared >= MAX_QUEUED) {
      this.#droppedShared++;
      return false;
    }
    this.#queuedShared++;
    this.#queue.push(message.text);
    return true;
  }

  send(data) {
    if (this.#dead) return false;
    if (this.#channel === null) {
      // Sent before the peer finished the handshake — an onConnect hook's
      // event, an answer: held, in order, up to a bound; past it the
      // connection is not worth the memory. Shared messages are counted
      // apart (sendPrepared).
      if (this.#queue.length - this.#queuedShared >= MAX_QUEUED) {
        this.#end(CLOSE_POLICY, 'queue');
        return false;
      }
      this.#queue.push(data);
      return true;
    }
    let sent = true;
    this.#outbound.push(
      this.#channel.seal(data),
      (sealed) => {
        if (!this.#dead) sent = this.#socket.send(asBuffer(sealed), NO_COMPRESS);
      },
      () => this.#end(CLOSE_PROTOCOL, 'crypto'),
    );
    return sent;
  }

  close(code, reason) {
    if (typeof this.#socket.close === 'function') this.#socket.close(code, reason);
    else this.#socket.terminate();
  }

  terminate() {
    this.#socket.terminate();
  }

  pause() {
    if (typeof this.#socket.pause === 'function') this.#socket.pause();
  }

  resume() {
    if (typeof this.#socket.resume === 'function') this.#socket.resume();
  }
}

module.exports = { normalizeServerEncryption, wantsEncryption, SealedSocket, DEFAULT_PATTERNS, MAX_QUEUED };
