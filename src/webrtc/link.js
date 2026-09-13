'use strict';

// One WebRTC link between two wrpc peers: an RTCPeerConnection carrying two
// negotiated data channels — one per direction of the protocol's
// client → server relationship — negotiated through an injected signaling
// function, kept alive through ICE restarts, and re-dialled from scratch
// when a restart cannot save it.
//
// Roles are a function of the two ids, so both ends compute them without a
// message: the peer whose id sorts first is the INITIATOR — it makes the
// offer and is the impolite side of perfect negotiation
// (https://w3c.github.io/webrtc-pc/#perfect-negotiation-example); the
// other is polite. The initiator's client writes on the `initiator`
// channel, the responder's on the `responder` channel.
//
// Written against the port (src/webrtc/port.js), so it runs unchanged in a
// browser and over an injected Node implementation. Two things the port's
// contract suite taught about real implementations are honoured here: a pc
// may already be offering the moment a channel is created (libdatachannel),
// so the initiator offers explicitly and negotiationneeded is a guarded
// second trigger; and signalingState is not trusted after close() —
// the link keeps its own state.
//
// Everything asynchronous surfaces through events, never a thrown promise
// nobody awaits: 'state' on every transition, 'open' when both channels
// are open, 'close' once when the link is finished for good, 'error' for
// background failures. A missing 'error' listener downgrades to a log line
// (Emitter throws on an unheard 'error').

const { Emitter } = require('../utils.js');
const { createLoggerWriter } = require('../logging.js');
const { isRtcAdapter, isRtcPeerConnection } = require('./port.js');
const { negotiateMessageSize, MIN_MESSAGE_SIZE } = require('./framing.js');

// Negotiated data channels are not described in the SDP: both peers MUST
// be configured identically, or a channel never opens (it surfaces as the
// connect timeout). Configurable for an application keeping its own
// channels on the same connection.
const DEFAULT_CHANNELS = Object.freeze({ initiator: 0, responder: 1, label: 'wrpc' });
const MAX_CHANNEL_ID = 65534;

const DEFAULT_CONNECT_TIMEOUT = 30 * 1000;
const DEFAULT_RESTART_TIMEOUT = 15 * 1000;

const STATES = ['new', 'connecting', 'connected', 'reconnecting', 'failed', 'closed'];

const normalizeChannels = (channels = {}) => {
  if (typeof channels !== 'object' || channels === null) throw new TypeError('channels must be an object');
  const {
    initiator = DEFAULT_CHANNELS.initiator,
    responder = DEFAULT_CHANNELS.responder,
    label = DEFAULT_CHANNELS.label,
  } = channels;
  for (const [name, id] of [
    ['initiator', initiator],
    ['responder', responder],
  ]) {
    if (!Number.isInteger(id) || id < 0 || id > MAX_CHANNEL_ID) {
      throw new TypeError(`channels.${name} must be an integer in 0..${MAX_CHANNEL_ID}`);
    }
  }
  if (initiator === responder) throw new TypeError('channels.initiator and channels.responder must differ');
  if (typeof label !== 'string' || label.length === 0) throw new TypeError('channels.label must be a non-empty string');
  return Object.freeze({ initiator, responder, label });
};

// In a browser setTimeout returns a number and there is nothing to unref;
// in Node a link's timers must not keep a process alive on their own.
const unref = (timer) => {
  if (timer && typeof timer.unref === 'function') timer.unref();
  return timer;
};

// What travels through signaling: plain data, never the platform object.
const describe = (description) => ({ type: description.type, sdp: description.sdp });
const candidateJson = (candidate) => {
  if (candidate === null || candidate === undefined) return null;
  if (typeof candidate.toJSON === 'function') return candidate.toJSON();
  const { candidate: text, sdpMid = null, sdpMLineIndex = null, usernameFragment } = candidate;
  return usernameFragment === undefined
    ? { candidate: text, sdpMid, sdpMLineIndex }
    : { candidate: text, sdpMid, sdpMLineIndex, usernameFragment };
};

class RtcLink extends Emitter {
  #localId;
  #remoteId;
  #adapter;
  #configuration;
  #signal;
  #channels;
  #connectTimeout;
  #restartTimeout;
  #log;

  #state = 'new';
  #pc = null;
  #clientChannel = null;
  #hostChannel = null;
  #unbind = null;
  #maxMessageSize = MIN_MESSAGE_SIZE;

  // Perfect negotiation bookkeeping, per pc.
  #makingOffer = false;
  #ignoreOffer = false;
  #restarting = false;
  #pendingCandidates = [];

  #opened = null; // { promise, resolve, reject } for the current dial
  #connectTimer = null;
  #restartTimer = null;
  #closeSent = false;

  constructor({
    localId,
    remoteId,
    adapter,
    signal,
    configuration = {},
    channels = DEFAULT_CHANNELS,
    connectTimeout = DEFAULT_CONNECT_TIMEOUT,
    restartTimeout = DEFAULT_RESTART_TIMEOUT,
    log = null,
  }) {
    super();
    if (typeof localId !== 'string' || localId.length === 0) throw new TypeError('localId must be a non-empty string');
    if (typeof remoteId !== 'string' || remoteId.length === 0) {
      throw new TypeError('remoteId must be a non-empty string');
    }
    if (localId === remoteId) throw new TypeError('localId and remoteId must differ');
    if (!isRtcAdapter(adapter)) throw new TypeError('adapter must satisfy the RtcAdapter contract');
    if (typeof signal !== 'function') throw new TypeError('signal must be a function');
    this.#localId = localId;
    this.#remoteId = remoteId;
    this.#adapter = adapter;
    this.#signal = signal;
    this.#configuration = configuration;
    this.#channels = normalizeChannels(channels);
    this.#connectTimeout = connectTimeout;
    this.#restartTimeout = restartTimeout;
    this.#log = createLoggerWriter(log ?? globalThis.console).child({ peer: remoteId });
  }

  get localId() {
    return this.#localId;
  }

  get remoteId() {
    return this.#remoteId;
  }

  /** The peer whose id sorts first offers; it is also the impolite side. */
  get initiator() {
    return this.#localId < this.#remoteId;
  }

  get polite() {
    return !this.initiator;
  }

  get state() {
    return this.#state;
  }

  get channels() {
    return this.#channels;
  }

  get pc() {
    return this.#pc;
  }

  /** The channel MY client writes on (the peer's host reads it). */
  get clientChannel() {
    return this.#clientChannel;
  }

  /** The channel MY host reads (the peer's client writes on it). */
  get hostChannel() {
    return this.#hostChannel;
  }

  /** What the pair negotiated (framing splits at it); the floor until connected. */
  get maxMessageSize() {
    return this.#maxMessageSize;
  }

  get open() {
    return this.#state === 'connected';
  }

  /** Dials: creates the connection and both channels; the initiator offers. */
  start() {
    if (this.#state !== 'new') throw new Error(`RtcLink.start(): already ${this.#state}`);
    this.#dial('connecting');
  }

  /**
   * A fresh connection after a failure: same ids, same roles, a new pc.
   * Only from 'failed' — a link that is dialling or connected is left alone,
   * so a responder that already re-dialled on the initiator's new offer is
   * not torn down by its own owner's redial.
   */
  redial() {
    if (this.#state !== 'failed') return false;
    this.#dial('reconnecting');
    return true;
  }

  /** Resolves when both channels are open; rejects when this dial fails or the link closes. */
  waitOpen() {
    if (this.#state === 'connected') return Promise.resolve();
    if (this.#state === 'closed') return Promise.reject(new Error('RtcLink is closed'));
    if (this.#state === 'failed') return Promise.reject(new Error('RtcLink failed'));
    if (this.#state === 'new') return Promise.reject(new Error('RtcLink not started'));
    return this.#opened.promise;
  }

  /**
   * An ICE restart on the live connection (a NAT rebinding, a network
   * switch): the channels survive it. Either side may ask; the initiator
   * does so on its own when ICE fails.
   */
  restart() {
    const pc = this.#pc;
    if (!pc || this.#state === 'closed' || this.#state === 'failed') return;
    this.#restarting = true;
    this.#armRestartTimer();
    if (typeof pc.restartIce === 'function') {
      pc.restartIce();
      // An implementation without negotiationneeded would leave restartIce
      // pending forever; a polite peer waits for the initiator's offer, an
      // initiator offers itself either way.
      if (this.initiator) void this.#offer();
    } else {
      void this.#offer({ iceRestart: true });
    }
  }

  /** From the signaler: a description, a candidate, or the peer's goodbye. */
  async receive(message) {
    if (this.#state === 'closed') return;
    if (typeof message !== 'object' || message === null) return void this.#log.warn({ event: 'rtc.signal.malformed' });
    const { type } = message;
    if (type === 'close') return void this.#finish(false);
    if (type === 'description') return void (await this.#receiveDescription(message.description));
    if (type === 'candidate') return void (await this.#receiveCandidate(message.candidate));
    this.#log.warn({ event: 'rtc.signal.unknown', type });
  }

  /** Tells the peer, closes the connection, emits 'close' once. */
  close() {
    this.#finish(true);
  }

  // ---- dialling

  #dial(state) {
    // A dial still in progress is replaced: whoever waited on it learns so.
    const replaced = this.#opened;
    this.#opened = null;
    this.#teardownPc();
    if (replaced) replaced.reject(new Error('RtcLink re-dialled'));
    const pc = this.#adapter.createPeerConnection(this.#configuration);
    if (!isRtcPeerConnection(pc)) {
      throw new TypeError('adapter.createPeerConnection() did not return a peer connection');
    }
    this.#pc = pc;
    this.#makingOffer = false;
    this.#ignoreOffer = false;
    this.#restarting = false;
    this.#pendingCandidates = [];
    this.#maxMessageSize = MIN_MESSAGE_SIZE;
    this.#opened = this.#deferred();
    const { initiator, responder, label } = this.#channels;
    const mine = this.initiator ? initiator : responder;
    const theirs = this.initiator ? responder : initiator;
    // Both channels exist before the first offer: the SDP needs an
    // m=application section, and a negotiated channel is never announced.
    const clientChannel = pc.createDataChannel(label, { negotiated: true, id: mine, ordered: true });
    const hostChannel = pc.createDataChannel(label, { negotiated: true, id: theirs, ordered: true });
    clientChannel.binaryType = 'arraybuffer';
    hostChannel.binaryType = 'arraybuffer';
    this.#clientChannel = clientChannel;
    this.#hostChannel = hostChannel;
    this.#bind(pc, clientChannel, hostChannel);
    this.#setState(state);
    this.#armConnectTimer();
    if (this.initiator) void this.#offer();
  }

  #bind(pc, clientChannel, hostChannel) {
    const onNegotiationNeeded = () => {
      if (this.#pc !== pc) return;
      // The responder only offers for a restart it asked for itself;
      // otherwise the initiator's offer is the one that counts.
      if (!this.initiator && !this.#restarting) return;
      void this.#offer();
    };
    const onIceCandidate = ({ candidate }) => {
      if (this.#pc !== pc) return;
      this.#send({ type: 'candidate', candidate: candidateJson(candidate) });
    };
    const onConnectionState = () => {
      if (this.#pc !== pc) return;
      this.#onConnectionState(pc);
    };
    const onOpen = () => {
      if (this.#pc !== pc) return;
      if (clientChannel.readyState === 'open' && hostChannel.readyState === 'open') this.#onOpen(pc);
    };
    const onChannelClose = (which) => () => {
      if (this.#pc !== pc) return;
      void this.emit('channel-close', { which }).catch((error) => this.#error(error, 'listener.channel-close'));
      // A channel does not come back on its own: the link is down until a
      // redial, whichever side closed it.
      if (this.#state === 'connected' || this.#state === 'connecting' || this.#state === 'reconnecting') {
        this.#fail(new Error(`data channel '${which}' closed`));
      }
    };
    const onChannelError = (event) => {
      if (this.#pc !== pc) return;
      this.#error(event?.error ?? new Error('data channel error'), 'channel');
    };
    const onClientClose = onChannelClose('client');
    const onHostClose = onChannelClose('host');
    pc.addEventListener('negotiationneeded', onNegotiationNeeded);
    pc.addEventListener('icecandidate', onIceCandidate);
    pc.addEventListener('connectionstatechange', onConnectionState);
    pc.addEventListener('iceconnectionstatechange', onConnectionState);
    clientChannel.addEventListener('open', onOpen);
    hostChannel.addEventListener('open', onOpen);
    clientChannel.addEventListener('close', onClientClose);
    hostChannel.addEventListener('close', onHostClose);
    clientChannel.addEventListener('error', onChannelError);
    hostChannel.addEventListener('error', onChannelError);
    this.#unbind = () => {
      pc.removeEventListener('negotiationneeded', onNegotiationNeeded);
      pc.removeEventListener('icecandidate', onIceCandidate);
      pc.removeEventListener('connectionstatechange', onConnectionState);
      pc.removeEventListener('iceconnectionstatechange', onConnectionState);
      clientChannel.removeEventListener('open', onOpen);
      hostChannel.removeEventListener('open', onOpen);
      clientChannel.removeEventListener('close', onClientClose);
      hostChannel.removeEventListener('close', onHostClose);
      clientChannel.removeEventListener('error', onChannelError);
      hostChannel.removeEventListener('error', onChannelError);
    };
  }

  #teardownPc() {
    if (this.#unbind) this.#unbind();
    this.#unbind = null;
    const pc = this.#pc;
    this.#pc = null;
    this.#clientChannel = null;
    this.#hostChannel = null;
    this.#clearTimers();
    if (pc) {
      try {
        pc.close();
      } catch {
        // already closed by the implementation
      }
    }
  }

  // ---- perfect negotiation

  async #offer(options = undefined) {
    const pc = this.#pc;
    if (!pc || this.#makingOffer) return;
    this.#makingOffer = true;
    try {
      if (options?.iceRestart) {
        const offer = await pc.createOffer({ iceRestart: true });
        if (this.#pc !== pc) return;
        await pc.setLocalDescription(offer);
      } else {
        await pc.setLocalDescription();
      }
      if (this.#pc !== pc) return;
      await this.#send({ type: 'description', description: describe(pc.localDescription) });
    } catch (error) {
      if (this.#pc === pc) this.#error(error, 'offer');
    } finally {
      if (this.#pc === pc) this.#makingOffer = false;
    }
  }

  async #receiveDescription(description) {
    if (typeof description !== 'object' || description === null || typeof description.type !== 'string') {
      return void this.#log.warn({ event: 'rtc.signal.malformed', what: 'description' });
    }
    // An offer for a link that failed (or never dialled on this side) is the
    // initiator's redial arriving first: follow it onto a fresh pc.
    if (description.type === 'offer' && (this.#state === 'failed' || this.#pc === null)) {
      if (this.#state === 'new' || this.#state === 'closed') return;
      this.#dial('reconnecting');
    }
    const pc = this.#pc;
    if (!pc) return;
    const offerCollision = description.type === 'offer' && (this.#makingOffer || pc.signalingState !== 'stable');
    this.#ignoreOffer = !this.polite && offerCollision;
    if (this.#ignoreOffer) return;
    try {
      await pc.setRemoteDescription(description);
      if (this.#pc !== pc) return;
      if (description.type === 'offer') {
        await pc.setLocalDescription();
        if (this.#pc !== pc) return;
        await this.#send({ type: 'description', description: describe(pc.localDescription) });
      }
      await this.#flushCandidates(pc);
    } catch (error) {
      if (this.#pc === pc) this.#error(error, 'description');
    }
  }

  async #receiveCandidate(candidate) {
    const pc = this.#pc;
    if (!pc) return;
    if (!pc.remoteDescription) {
      this.#pendingCandidates.push(candidate);
      return;
    }
    await this.#addCandidate(pc, candidate);
  }

  async #flushCandidates(pc) {
    const pending = this.#pendingCandidates;
    this.#pendingCandidates = [];
    for (let i = 0; i < pending.length; i++) {
      if (this.#pc !== pc) return;
      await this.#addCandidate(pc, pending[i]);
    }
  }

  async #addCandidate(pc, candidate) {
    try {
      await pc.addIceCandidate(candidate ?? null);
    } catch (error) {
      // A candidate for an offer we ignored, or an end-of-candidates an
      // implementation refuses: both harmless, per the recipe.
      if (candidate === null || candidate === undefined || this.#ignoreOffer) return;
      if (this.#pc === pc) this.#error(error, 'candidate');
    }
  }

  // ---- connection lifecycle

  #onConnectionState(pc) {
    const ice = pc.iceConnectionState;
    const connection = pc.connectionState;
    if (ice === 'failed' || connection === 'failed') {
      if (this.#restarting) return; // the restart timer decides
      this.#log.warn({ event: 'rtc.ice.failed', state: this.#state });
      // Only the initiator restarts, so the two sides never race two
      // restarts; the responder waits for it under the same timer.
      if (this.initiator) this.restart();
      else {
        this.#restarting = true;
        this.#armRestartTimer();
      }
      return;
    }
    if (connection === 'closed') {
      if (this.#state !== 'closed' && this.#state !== 'failed') this.#fail(new Error('peer connection closed'));
      return;
    }
    if ((ice === 'connected' || ice === 'completed' || connection === 'connected') && this.#restarting) {
      this.#restarting = false;
      this.#clearRestartTimer();
      this.#log.info({ event: 'rtc.ice.restarted' });
    }
  }

  #onOpen(pc) {
    if (this.#state === 'connected') return;
    this.#clearTimers();
    this.#restarting = false;
    this.#maxMessageSize = negotiateMessageSize(pc.sctp ?? null);
    this.#setState('connected');
    const opened = this.#opened;
    if (opened) opened.resolve();
    void this.emit('open').catch((error) => this.#error(error, 'listener.open'));
  }

  #fail(error) {
    if (this.#state === 'closed' || this.#state === 'failed') return;
    this.#log.warn({ event: 'rtc.link.failed', reason: error.message });
    const opened = this.#opened;
    this.#opened = null;
    this.#teardownPc();
    this.#setState('failed');
    if (opened) opened.reject(error);
  }

  #finish(tellPeer) {
    if (this.#state === 'closed') return;
    if (tellPeer && !this.#closeSent) {
      this.#closeSent = true;
      this.#send({ type: 'close' });
    }
    const opened = this.#opened;
    this.#opened = null;
    this.#teardownPc();
    this.#setState('closed');
    if (opened) opened.reject(new Error('RtcLink is closed'));
    void this.emit('close').catch((error) => this.#error(error, 'listener.close'));
  }

  // ---- timers

  #armConnectTimer() {
    this.#clearConnectTimer();
    if (!(this.#connectTimeout > 0)) return;
    this.#connectTimer = unref(
      setTimeout(() => {
        this.#connectTimer = null;
        this.#fail(new Error(`RtcLink connect timeout after ${this.#connectTimeout} ms`));
      }, this.#connectTimeout),
    );
  }

  #armRestartTimer() {
    this.#clearRestartTimer();
    if (!(this.#restartTimeout > 0)) return;
    this.#restartTimer = unref(
      setTimeout(() => {
        this.#restartTimer = null;
        this.#fail(new Error(`ICE restart did not reconnect within ${this.#restartTimeout} ms`));
      }, this.#restartTimeout),
    );
  }

  #clearConnectTimer() {
    clearTimeout(this.#connectTimer);
    this.#connectTimer = null;
  }

  #clearRestartTimer() {
    clearTimeout(this.#restartTimer);
    this.#restartTimer = null;
  }

  #clearTimers() {
    this.#clearConnectTimer();
    this.#clearRestartTimer();
  }

  // ---- plumbing

  #setState(state) {
    const previous = this.#state;
    if (previous === state) return;
    this.#state = state;
    this.#log.debug({ event: 'rtc.link.state', state, previous });
    void this.emit('state', state).catch((error) => this.#error(error, 'listener.state'));
  }

  #send(message) {
    let result;
    try {
      result = this.#signal(message);
    } catch (error) {
      this.#error(error, 'signal');
      return Promise.resolve();
    }
    if (result && typeof result.then === 'function') {
      return result.then(
        () => undefined,
        (error) => this.#error(error, 'signal'),
      );
    }
    return Promise.resolve();
  }

  #error(error, origin) {
    this.#log.error({ event: 'rtc.link.error', origin, err: error });
    if (this.listenerCount('error') === 0) return;
    void this.emit('error', error).catch(() => {});
  }

  #deferred() {
    let resolve = null;
    let reject = null;
    const promise = new Promise((_resolve, _reject) => {
      resolve = _resolve;
      reject = _reject;
    });
    // A dial that fails with nobody waiting must not be an unhandled
    // rejection; waitOpen() callers attach their own handlers.
    promise.catch(() => {});
    return { promise, resolve, reject };
  }
}

module.exports = { RtcLink, normalizeChannels, DEFAULT_CHANNELS, MAX_CHANNEL_ID, STATES };
