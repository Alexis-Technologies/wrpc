'use strict';

// The client half of session encryption: `createEncryption(options)` builds
// the object a client is handed as its `encryption` option —
//
//   const client = await connect(url, { encryption: createEncryption({ serverKey }) });
//
// — the way @alexify/wrpc/deflate builds a codec: the base entry holds a
// seam of a few lines (src/client/transports.js), and only a page that
// encrypts carries the handshake. Browser-safe; every primitive is the
// platform's or injected.
//
// A client that was given this NEVER falls back to plaintext: a server that
// does not speak it, a handshake that fails, a transport that cannot carry
// it — each is a failed connection. That strictness is the anchor of the
// whole feature: the `wrpc_e` flag and key discovery travel in the clear
// and can be tampered with, but only into a refusal.

const { aead } = require('./aead.js');
const { x25519 } = require('./dh.js');
const { createKdf } = require('./hkdf.js');
const { isCipher, isDh } = require('./contracts.js');
const { createNoise, PATTERN_NAMES } = require('./noise.js');
const { parseBundle } = require('./statics.js');
const { createHpke, dhKem, AEAD_IDS } = require('./hpke.js');
const { sealedFetch } = require('./http.js');
const { equal } = require('./bytes.js');
const { Sequencer } = require('../sequencer.js');
const { ENCRYPTION_PARAM } = require('../wire.js');
const {
  SecureChannel,
  helloHeader,
  prologueOf,
  frame,
  isFrame,
  asBytes,
  FRAME_HANDSHAKE,
  DEFAULT_REKEY_AFTER,
  DEFAULT_HANDSHAKE_TIMEOUT,
} = require('./session.js');

const resolveCipher = (value) => {
  if (value === undefined || value === null) return aead();
  if (typeof value === 'string') return aead({ algorithm: value });
  if (!isCipher(value)) throw new TypeError('createEncryption: cipher must be a cipher name or a Cipher');
  return value;
};

const keyBytes = (value, name) => {
  if (!(value instanceof Uint8Array) || value.length !== 32) {
    throw new TypeError(`createEncryption: ${name} must be 32 bytes`);
  }
  return value;
};

const createEncryption = (options = {}) => {
  const {
    serverKey = null,
    verifyServer = null,
    staticKey = null,
    psk = null,
    rekeyAfter = DEFAULT_REKEY_AFTER,
    handshakeTimeout = DEFAULT_HANDSHAKE_TIMEOUT,
  } = options;
  const pinned = serverKey === null ? null : parseBundle(serverKey, 'createEncryption: serverKey');
  const pattern = options.pattern ?? (pinned === null ? null : 'NK');
  if (!PATTERN_NAMES.includes(pattern)) {
    // NN authenticates nobody, so it is never what a missing option means.
    throw new TypeError(`createEncryption: a serverKey, or an explicit pattern — ${PATTERN_NAMES.join(', ')}`);
  }
  if (pattern === 'NK' && pinned === null) throw new TypeError('createEncryption: NK needs the serverKey it pins');
  if (pattern === 'XX') {
    keyBytes(staticKey, 'staticKey (XX)');
    if (pinned === null && typeof verifyServer !== 'function') {
      // Trust on first use is a decision, so it is spelled: verifyServer.
      throw new TypeError('createEncryption: XX needs a serverKey to pin, or verifyServer(publicKey)');
    }
  }
  if (pattern === 'NNpsk0') keyBytes(psk, 'psk (NNpsk0)');
  if (!(Number.isInteger(rekeyAfter) && rekeyAfter >= 0)) {
    throw new TypeError('createEncryption: rekeyAfter must be a non-negative integer');
  }
  const dh = options.dh ?? x25519();
  if (!isDh(dh)) throw new TypeError('createEncryption: dh must be a Dh');
  const cipher = resolveCipher(options.cipher);
  const kdf = createKdf();
  const noise = createNoise({ pattern, dh, cipher, kdf });
  // The per-request half, for the transports that have no connection to
  // hold a session (http): HPKE to the pinned server key — which is why it
  // exists only with a `serverKey`, and only for a cipher HPKE registers.
  const fetch =
    pinned === null || AEAD_IDS[cipher.id] === undefined
      ? null
      : sealedFetch({ hpke: createHpke({ kem: dhKem(dh, kdf), kdf, cipher }), kdf, cipher, serverKey: pinned });
  const kid = pinned === null ? '' : pinned.kid;
  let identity = null;

  /**
   * One connection. `link` is the transport's side of the seam:
   * `{ kind, write(bytes), deliver(message), fail(error) }`. Answers
   * `{ ready, send(data), receive(data) }` — `ready` resolves with what the
   * application may read as `client.encryption`.
   */
  const secure = (link) => {
    let handshake = null;
    let channel = null;
    let failed = false;
    const fail = (error) => {
      if (failed) return;
      failed = true;
      clearTimeout(timer);
      link.fail(error);
    };
    const timer = setTimeout(() => abort(new Error('encryption: handshake timed out')), handshakeTimeout);
    timer.unref?.();
    const outbound = new Sequencer(fail);
    const inbound = new Sequencer(fail);
    let resolveReady;
    let rejectReady;
    const ready = new Promise((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
    });
    const abort = (error) => {
      rejectReady(error);
      fail(error);
    };

    // Who answered, checked BEFORE anything more is written: in XX the next
    // message carries this client's own static key, and it is encrypted to
    // whoever the responder is — an impostor must not get to read it.
    const verifyPeer = async () => {
      if (pinned !== null && !equal(handshake.remoteStatic ?? pinned.noise, pinned.noise)) {
        throw new Error('encryption: the server key is not the pinned one');
      }
      if (pinned === null && pattern === 'XX' && (await verifyServer(handshake.remoteStatic)) !== true) {
        throw new Error('encryption: the server key was not accepted');
      }
    };

    const establish = async () => {
      const done = await handshake.finish();
      channel = new SecureChannel(done);
      clearTimeout(timer);
      resolveReady(
        Object.freeze({
          protocol: noise.name,
          pattern,
          cipher: noise.cipher,
          kid,
          remoteStatic: done.remoteStatic,
          handshakeHash: done.handshakeHash,
        }),
      );
    };

    const begin = async () => {
      if (pattern === 'XX') identity ??= await dh.keyPair(staticKey);
      const header = helloHeader(noise.name, kid);
      handshake = await noise.initiator({
        prologue: prologueOf(link.kind, header),
        remoteStatic: pattern === 'NK' ? pinned.noise : undefined,
        staticKey: identity ?? undefined,
        psk: psk ?? undefined,
        rekeyAfter,
      });
      link.write(frame(FRAME_HANDSHAKE, header, await handshake.write()));
    };

    // Nothing reaches the application after a frame that did not open. The
    // frames behind it are already in the inbound queue when the cipher is
    // asynchronous — subtle resolves them after the one that failed — and
    // the queue delivers in order: without this gate they would land on
    // the application AFTER the session was declared over.
    const deliver = (message) => {
      if (!failed) link.deliver(message);
    };

    const open = (bytes) => {
      try {
        inbound.push(channel.open(bytes), deliver, fail);
      } catch (error) {
        fail(error);
      }
    };

    // One inbound frame while the handshake is still running. The steps are
    // asynchronous and the server does not wait for them: it is established
    // the moment it wrote its last message, and its first sealed frames can
    // arrive while this side is still finishing — so frames are taken in
    // order through `chain` until it has drained, and only then directly.
    const step = async (bytes) => {
      if (failed) return;
      if (channel !== null) return void open(bytes);
      if (!isFrame(bytes, FRAME_HANDSHAKE)) throw new Error('encryption: unexpected message during the handshake');
      await handshake.read(bytes.subarray(2));
      await verifyPeer();
      if (!handshake.done) link.write(frame(FRAME_HANDSHAKE, await handshake.write()));
      await establish();
    };

    let queued = 0;
    let chain = begin();
    chain.catch(abort);

    return {
      ready,
      send(data) {
        if (failed) return;
        if (channel === null) throw new Error('encryption: not established');
        outbound.push(channel.seal(data), link.write, fail);
      },
      /**
       * The transport is gone — a close, a terminate: nothing more is
       * written or delivered, and a pending `ready` rejects NOW rather
       * than at the handshake timeout. Without `link.fail`: what is closed
       * is not closed again.
       */
      cancel(error) {
        if (failed) return;
        failed = true;
        clearTimeout(timer);
        rejectReady(error ?? new Error('Connection closed'));
      },
      receive(data) {
        if (failed) return;
        // Text is plaintext: before the handshake it is a server that does
        // not speak this, after it an injection. Either way, the end.
        if (typeof data === 'string') return void abort(new Error('encryption: plaintext on an encrypted connection'));
        const bytes = asBytes(data);
        if (channel !== null && queued === 0) return void open(bytes);
        queued++;
        chain = chain.then(() => step(bytes)).then(() => void queued--);
        chain.catch(abort);
      },
    };
  };

  return Object.freeze({ param: ENCRYPTION_PARAM, protocol: noise.name, pattern, secure, fetch });
};

/**
 * The server's key bundle from its discovery endpoint — `GET
 * <url>/encryption-key`. TRUST ON FIRST USE: the answer is only as
 * trustworthy as the connection it came over, so a client that can be
 * shipped the bundle (a Node service, a mobile app, a build-time constant)
 * should be, and this is for the ones that cannot.
 */
const fetchServerKey = async (url, { fetch = globalThis.fetch } = {}) => {
  const base = String(url).replace(/^ws/, 'http').replace(/\/+$/, '');
  const response = await fetch(`${base}/encryption-key`);
  if (!response.ok) throw new Error(`encryption: no key at ${base}/encryption-key (${response.status})`);
  const { key } = await response.json();
  parseBundle(key, 'the discovered key');
  return key;
};

/** What the base entry checks before it trusts an `encryption` option. */
const isEncryption = (value) =>
  typeof value === 'object' && value !== null && typeof value.secure === 'function' && typeof value.param === 'string';

module.exports = { createEncryption, isEncryption, fetchServerKey };
