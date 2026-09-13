'use strict';

// An in-repo fake of the W3C RTCPeerConnection / RTCDataChannel subset the
// port names (src/webrtc/port.js) — enough of the real state machines for
// perfect negotiation, trickle ICE, ICE restart, negotiated channels, the
// per-message size limit and bufferedAmount backpressure. Two fakes from
// one createFakeRtc() world connect the way real ones do: by exchanging
// descriptions — the fake sdp names its peer, so there is no explicit
// pairing call and the signaling path is exercised for real.
//
// Deliberately faithful in the places a transport gets wrong:
//   - a data channel's binaryType defaults to 'blob', like a browser;
//   - send() above maxMessageSize errors and closes the channel (Chrome);
//   - addIceCandidate before a remote description rejects (spec);
//   - setRemoteDescription(offer) in have-local-offer rolls back (spec —
//     what the polite peer of perfect negotiation relies on);
//   - a peer's close() shows up here as channel closes and 'disconnected'.
//
// Not a *.test.js — a helper for tests/webrtc/*.test.js.

const { isRtcPeerConnection } = require('../../src/webrtc/port.js');

const invalidState = (message) => new DOMException(message, 'InvalidStateError');

const byteLength = (data) => {
  if (typeof data === 'string') return Buffer.byteLength(data);
  if (data instanceof ArrayBuffer) return data.byteLength;
  if (ArrayBuffer.isView(data)) return data.byteLength;
  throw new TypeError('fake: unsupported message type');
};

// Delivered as a fresh ArrayBuffer (or Blob under the browser default), so
// a receiver that retains a view never aliases the sender's memory.
const toDelivered = (data, binaryType) => {
  if (typeof data === 'string') return data;
  const view =
    data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  const copy = view.slice().buffer;
  return binaryType === 'blob' ? new Blob([copy]) : copy;
};

class FakeDataChannel extends EventTarget {
  readyState = 'connecting';
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;
  binaryType = 'blob';
  #pc;
  #world;
  #peer = null;
  #aboveLow = false;
  // Test-visible counters.
  sent = 0;

  constructor(pc, world, label, init) {
    super();
    this.#pc = pc;
    this.#world = world;
    this.label = label;
    this.negotiated = init.negotiated === true;
    this.ordered = init.ordered !== false;
    this.id = this.negotiated ? init.id : world.allocateChannelId();
  }

  get peer() {
    return this.#peer;
  }

  // Called by the world once both ends exist and the connection is up.
  open(peer) {
    if (this.readyState !== 'connecting') return;
    this.#peer = peer;
    this.readyState = 'open';
    this.dispatchEvent(new Event('open'));
  }

  send(data) {
    if (this.readyState !== 'open') throw invalidState('RTCDataChannel.send: channel is not open');
    const size = byteLength(data);
    if (size > this.#world.maxMessageSize) {
      // Chrome: the SCTP association drops the message and the channel dies.
      this.#world.later(() => {
        this.dispatchEvent(Object.assign(new Event('error'), { error: new Error('message too large') }));
        this.#close(true);
      });
      return;
    }
    this.sent++;
    this.bufferedAmount += size;
    if (this.bufferedAmount > this.bufferedAmountLowThreshold) this.#aboveLow = true;
    const peer = this.#peer;
    const linked = this.#pc.linkedTo(peer?.pcOf());
    this.#world.later(() => {
      this.bufferedAmount -= size;
      if (this.#aboveLow && this.bufferedAmount <= this.bufferedAmountLowThreshold) {
        this.#aboveLow = false;
        this.dispatchEvent(new Event('bufferedamountlow'));
      }
      // A severed ICE link (failIce) or a closed peer eats the bytes.
      if (!linked || !peer || peer.readyState !== 'open') return;
      peer.dispatchEvent(new MessageEvent('message', { data: toDelivered(data, peer.binaryType) }));
    });
  }

  close() {
    this.#close(false);
  }

  pcOf() {
    return this.#pc;
  }

  #close(abrupt) {
    if (this.readyState === 'closed' || this.readyState === 'closing') return;
    this.readyState = 'closing';
    const peer = this.#peer;
    this.#world.later(() => {
      this.readyState = 'closed';
      this.dispatchEvent(new Event('close'));
      if (peer && !abrupt) peer.remoteClosed();
      if (peer && abrupt) peer.remoteClosed();
    });
  }

  remoteClosed() {
    if (this.readyState === 'closed' || this.readyState === 'closing') return;
    this.readyState = 'closing';
    this.#world.later(() => {
      this.readyState = 'closed';
      this.dispatchEvent(new Event('close'));
    });
  }
}

class FakePeerConnection extends EventTarget {
  signalingState = 'stable';
  connectionState = 'new';
  iceConnectionState = 'new';
  iceGatheringState = 'new';
  localDescription = null;
  remoteDescription = null;
  sctp = null;
  // Test-visible: candidates the peer handed us, and restarts asked for.
  remoteCandidates = [];
  endOfCandidates = false;
  restarts = 0;
  #world;
  #configuration;
  #channels = new Map();
  #peer = null;
  #linked = false;
  #closed = false;
  #negotiationQueued = false;
  #descriptionSeq = 0;

  constructor(world, configuration = {}) {
    super();
    this.#world = world;
    this.#configuration = configuration;
    this.id = `pc${world.nextId()}`;
    world.peers.set(this.id, this);
  }

  getConfiguration() {
    return this.#configuration;
  }

  get channels() {
    return this.#channels;
  }

  get peer() {
    return this.#peer;
  }

  get linked() {
    return this.#linked;
  }

  linkedTo(pc) {
    return this.#linked && pc !== null && pc !== undefined && this.#peer === pc && pc.linked;
  }

  createDataChannel(label, init = {}) {
    if (this.#closed) throw invalidState('RTCPeerConnection is closed');
    if (init.negotiated === true && !Number.isInteger(init.id)) {
      throw new TypeError('fake: a negotiated channel needs an integer id');
    }
    const channel = new FakeDataChannel(this, this.#world, label, init);
    if (this.#channels.has(channel.id)) throw new DOMException('fake: channel id in use', 'OperationError');
    this.#channels.set(channel.id, channel);
    if (this.#linked) this.#world.later(() => this.#tryOpen(channel));
    else this.#queueNegotiation();
    return channel;
  }

  async createOffer(options = {}) {
    if (this.#closed) throw invalidState('RTCPeerConnection is closed');
    const restart = options.iceRestart === true ? ' ice-restart' : '';
    return { type: 'offer', sdp: `v=0 fake ${this.id} o=${++this.#descriptionSeq}${restart}` };
  }

  async createAnswer() {
    if (this.#closed) throw invalidState('RTCPeerConnection is closed');
    if (this.signalingState !== 'have-remote-offer') throw invalidState('createAnswer: no remote offer');
    return { type: 'answer', sdp: `v=0 fake ${this.id} a=${++this.#descriptionSeq}` };
  }

  async setLocalDescription(description) {
    if (this.#closed) throw invalidState('RTCPeerConnection is closed');
    let desc = description;
    if (desc === undefined || desc === null) {
      const state = this.signalingState;
      if (state === 'stable' || state === 'have-local-offer') {
        desc = await this.createOffer();
      } else if (state === 'have-remote-offer') {
        desc = await this.createAnswer();
      } else {
        throw invalidState('setLocalDescription: nothing implicit to do');
      }
    }
    if (desc.type === 'offer') {
      if (this.signalingState !== 'stable' && this.signalingState !== 'have-local-offer') {
        throw invalidState(`setLocalDescription(offer) in ${this.signalingState}`);
      }
      this.signalingState = 'have-local-offer';
    } else if (desc.type === 'answer') {
      if (this.signalingState !== 'have-remote-offer') {
        throw invalidState(`setLocalDescription(answer) in ${this.signalingState}`);
      }
      this.signalingState = 'stable';
    } else if (desc.type === 'rollback') {
      this.signalingState = 'stable';
      this.localDescription = null;
      return;
    } else {
      throw new TypeError(`fake: unknown description type '${desc.type}'`);
    }
    this.localDescription = desc;
    this.#gather();
    this.dispatchEvent(new Event('signalingstatechange'));
    if (this.signalingState === 'stable') this.#negotiated();
  }

  async setRemoteDescription(description) {
    if (this.#closed) throw invalidState('RTCPeerConnection is closed');
    const match = /fake (pc\d+)/.exec(description?.sdp ?? '');
    const peer = match ? this.#world.peers.get(match[1]) : null;
    if (!peer) throw new Error('fake: the sdp names no known peer');
    if (description.type === 'offer') {
      // The spec's implicit rollback: an offer arriving while we hold our
      // own un-answered offer replaces it (the polite peer's path).
      if (this.signalingState === 'have-local-offer') {
        this.localDescription = null;
      } else if (this.signalingState !== 'stable') {
        throw invalidState(`setRemoteDescription(offer) in ${this.signalingState}`);
      }
      this.signalingState = 'have-remote-offer';
    } else if (description.type === 'answer') {
      if (this.signalingState !== 'have-local-offer') {
        throw invalidState(`setRemoteDescription(answer) in ${this.signalingState}`);
      }
      this.signalingState = 'stable';
    } else {
      throw new TypeError(`fake: unknown description type '${description.type}'`);
    }
    this.remoteDescription = description;
    this.#peer = peer;
    this.dispatchEvent(new Event('signalingstatechange'));
    if (this.signalingState === 'stable') this.#negotiated();
  }

  async addIceCandidate(candidate) {
    if (this.#closed) throw invalidState('RTCPeerConnection is closed');
    if (!this.remoteDescription) throw invalidState('addIceCandidate: no remote description');
    if (candidate === null || candidate === undefined || candidate.candidate === '') {
      this.endOfCandidates = true;
      return;
    }
    this.remoteCandidates.push(candidate);
  }

  restartIce() {
    if (this.#closed) return;
    this.restarts++;
    this.#queueNegotiation();
  }

  // Test hook: the ICE link dies on both ends, the way a NAT rebinding
  // looks — channels stay 'open' but nothing gets through until a
  // renegotiation (an ICE restart) reconnects.
  failIce() {
    const peer = this.#peer;
    this.#fail();
    if (peer && peer.#peer === this) peer.#fail();
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    this.#world.peers.delete(this.id);
    this.signalingState = 'closed';
    this.connectionState = 'closed';
    this.iceConnectionState = 'closed';
    this.#linked = false;
    for (const channel of this.#channels.values()) channel.close();
    const peer = this.#peer;
    if (peer && peer.#peer === this) {
      peer.#linked = false;
      this.#world.later(() => {
        if (peer.#closed) return;
        peer.#setStates('disconnected', 'disconnected');
      });
    }
  }

  // ---- internals

  #queueNegotiation() {
    if (this.#negotiationQueued) return;
    this.#negotiationQueued = true;
    this.#world.later(() => {
      this.#negotiationQueued = false;
      if (this.#closed || this.signalingState !== 'stable') return;
      this.dispatchEvent(new Event('negotiationneeded'));
    });
  }

  #gather() {
    this.iceGatheringState = 'gathering';
    const candidate = {
      candidate: `candidate:${this.id} 1 udp 2113937151 192.0.2.1 5000 typ host`,
      sdpMid: '0',
      sdpMLineIndex: 0,
      toJSON() {
        return { candidate: this.candidate, sdpMid: this.sdpMid, sdpMLineIndex: this.sdpMLineIndex };
      },
    };
    this.#world.later(() => {
      if (this.#closed) return;
      this.dispatchEvent(Object.assign(new Event('icecandidate'), { candidate }));
      this.#world.later(() => {
        if (this.#closed) return;
        this.iceGatheringState = 'complete';
        this.dispatchEvent(Object.assign(new Event('icecandidate'), { candidate: null }));
      });
    });
  }

  // Both sides stable with each other as remote: the link comes up after a
  // hop, like ICE checks would.
  #negotiated() {
    const peer = this.#peer;
    if (!peer || peer.#peer !== this || peer.signalingState !== 'stable') return;
    this.#world.later(() => {
      if (this.#closed || peer.#closed) return;
      this.#connect();
      peer.#connect();
    });
  }

  #connect() {
    if (this.#linked) return;
    this.#linked = true;
    this.sctp = { maxMessageSize: this.#world.maxMessageSize };
    this.#setStates('connected', 'connected');
    for (const channel of this.#channels.values()) this.#tryOpen(channel);
  }

  #fail() {
    if (!this.#linked) return;
    this.#linked = false;
    this.#setStates('failed', 'failed');
  }

  #setStates(ice, connection) {
    if (this.iceConnectionState !== ice) {
      this.iceConnectionState = ice;
      this.dispatchEvent(new Event('iceconnectionstatechange'));
    }
    if (this.connectionState !== connection) {
      this.connectionState = connection;
      this.dispatchEvent(new Event('connectionstatechange'));
    }
  }

  #tryOpen(channel) {
    if (!this.#linked || !this.#peer) return;
    const other = this.#peer.#channels.get(channel.id);
    if (!other || channel.negotiated !== other.negotiated) return;
    channel.open(other);
    other.open(channel);
  }
}

class FakeWorld {
  peers = new Map();
  #seq = 0;
  #channelSeq = 0;
  #pending = new Set();

  constructor({ maxMessageSize = 65536, latency = 0 } = {}) {
    this.maxMessageSize = maxMessageSize;
    this.latency = latency;
  }

  nextId() {
    return ++this.#seq;
  }

  allocateChannelId() {
    return 1000 + this.#channelSeq++;
  }

  // Everything asynchronous goes through one timer so a test on fake timers
  // (t.mock.timers) drives the fake deterministically with tick().
  later(fn) {
    const timer = setTimeout(() => {
      this.#pending.delete(timer);
      fn();
    }, this.latency);
    this.#pending.add(timer);
  }

  get pending() {
    return this.#pending.size;
  }

  close() {
    for (const timer of this.#pending) clearTimeout(timer);
    this.#pending.clear();
    for (const pc of [...this.peers.values()]) pc.close();
  }
}

/**
 * A world of fakes plus the adapter that creates peer connections in it.
 * `maxMessageSize` is what every sctp reports; `latency` (ms) is the hop
 * every asynchronous step takes.
 */
const createFakeRtc = (options = {}) => {
  const world = new FakeWorld(options);
  const adapter = {
    name: 'fake',
    world,
    createPeerConnection(configuration) {
      const pc = new FakePeerConnection(world, configuration);
      if (!isRtcPeerConnection(pc)) throw new Error('fake does not satisfy its own port');
      return pc;
    },
  };
  return { adapter, world };
};

module.exports = { createFakeRtc, FakePeerConnection, FakeDataChannel, FakeWorld };
