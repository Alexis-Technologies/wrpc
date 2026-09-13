'use strict';

// The two halves of a wrpc peer on one RtcLink: ClientRtcTransport is the
// client-side transport (what my WrpcClient writes on — `link.clientChannel`)
// and RtcPeerTransport the server-side one (what my PeerHost reads —
// `link.hostChannel`). Both speak the data-channel framing (framing.js) and
// both defer the link's lifecycle to the link: a data channel is one
// direction of a link that a PeerLink owns, so close() on either half
// closes the LINK (goodbye to the peer, no redial), never just its channel
// — closing one channel would look like a failure to the other side and
// trigger a redial of a link that was meant to end.
//
// Only terminate() is local: the client core calls it on a heartbeat or
// connect timeout, and the answer to "this direction looks dead" is the
// client's reconnect cycle — open() again, which waits for the link to be
// (re)connected — not the end of the link.

// The client CORE, not the client barrel: the barrel also registers the
// ws/http/event transports and the Service Worker proxy, none of which a
// peer needs — and every byte here lands in the webrtc browser bundle.
const { ClientTransport, WrpcClient } = require('../client/core.js');
const { ServerTransport } = require('../rpc/serverTransport.js');
const { FrameEncoder, FrameDecoder, KIND_TEXT, KIND_BINARY } = require('./framing.js');

// Outbound flow control, in bytes queued on the channel: write() answers
// false above the high-water mark, and 'drain' fires once the channel is
// back under the low-water mark (bufferedAmountLowThreshold).
const DEFAULT_HIGH_WATER_MARK = 1024 * 1024;
const DEFAULT_LOW_WATER_MARK = 256 * 1024;

const toBytes = (data) => {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  throw new TypeError('write() takes a string or bytes');
};

// One place for what both halves do with a channel: encode outbound frames
// into it and decode inbound ones from it. `sink` is bound once so the hot
// path allocates nothing per call.
class ChannelCodec {
  #channel;
  #encoder;
  #decoder;
  #sink;

  constructor(channel, maxMessageSize, framing) {
    this.#channel = channel;
    this.#encoder = new FrameEncoder(maxMessageSize);
    this.#decoder = new FrameDecoder(framing);
    this.#sink = (frame) => channel.send(frame);
  }

  send(data) {
    if (typeof data === 'string') this.#encoder.encodeText(data, this.#sink);
    else this.#encoder.encode(KIND_BINARY, toBytes(data), this.#sink);
    return this.#channel.bufferedAmount;
  }

  /** null while a message is still being reassembled. */
  receive(data) {
    return this.#decoder.push(data);
  }
}

class ClientRtcTransport extends ClientTransport {
  // A data channel can die silently exactly like a socket (ICE stalls, the
  // peer's tab is suspended): the client's app-level ping/pong is the
  // detector, and the core owns every timer of it.
  heartbeat = true;
  persistent = true;

  #link = null;
  #framing;
  #highWater;
  #lowWater;
  #codec = null;
  #detach = null;
  #opening = null;
  // Bumped by terminate(): an open() that was waiting on the link when the
  // core gave up must not come back to life on top of a later attempt.
  #attempt = 0;
  #onLinkState = (state) => {
    if (state === 'failed' || state === 'closed') this.#down();
  };

  constructor(url, { link = null, framing = {}, highWaterMark, lowWaterMark } = {}) {
    super(url);
    this.#link = link;
    this.#framing = framing;
    this.#highWater = highWaterMark ?? DEFAULT_HIGH_WATER_MARK;
    this.#lowWater = lowWaterMark ?? DEFAULT_LOW_WATER_MARK;
    if (link) this.#bindLink(link);
  }

  get link() {
    return this.#link;
  }

  /**
   * Waits for the link to be connected and takes over its client channel.
   * Re-entrant: the core re-opens the same transport on every reconnect,
   * and after a redial the link hands out a new channel. The link may also
   * arrive here, as `options.link`, when the transport was picked by name
   * through connect() — the same way the event transport receives
   * `options.worker`.
   */
  async open(options = {}) {
    const link = this.#link ?? options.link ?? null;
    if (!link) throw new Error('WebRTC transport needs a link: pass options.link to connect()');
    if (this.#link !== link) {
      this.#link = link;
      this.#bindLink(link);
    }
    if (this.active) return;
    if (this.#opening) return this.#opening;
    this.#opening = this.#open(link);
    try {
      await this.#opening;
    } finally {
      this.#opening = null;
    }
  }

  async #open(link) {
    const attempt = ++this.#attempt;
    await link.waitOpen();
    if (attempt !== this.#attempt) throw new Error('Connection terminated');
    const channel = link.clientChannel;
    if (!channel || channel.readyState !== 'open') throw new Error('The link has no open client channel');
    this.#codec = new ChannelCodec(channel, link.maxMessageSize, this.#framing);
    channel.bufferedAmountLowThreshold = this.#lowWater;
    // Scoped to THIS channel: a stale channel's late events after a redial
    // must not reach a transport that has moved on.
    const onMessage = ({ data }) => this.#receive(data);
    const onClose = () => this.#down();
    const onDrain = () => void this.emit('drain').catch((error) => this.#escalate(error));
    const onError = (event) => this.#escalate(event?.error ?? new Error('data channel error'));
    channel.addEventListener('message', onMessage);
    channel.addEventListener('close', onClose);
    channel.addEventListener('bufferedamountlow', onDrain);
    channel.addEventListener('error', onError);
    this.#detach = () => {
      channel.removeEventListener('message', onMessage);
      channel.removeEventListener('close', onClose);
      channel.removeEventListener('bufferedamountlow', onDrain);
      channel.removeEventListener('error', onError);
    };
    this.active = true;
    // Announced before open() resolves — the core's 'open' handler runs
    // synchronously here, which is the invariant every transport keeps.
    this.emit('open');
  }

  /**
   * A string is a packet, bytes are a stream chunk. Throws when not
   * connected (the core turns that into a coded 503); answers false above
   * the high-water mark, after which 'drain' follows.
   */
  write(data) {
    if (!this.active) throw new Error('Not connected');
    return this.#codec.send(data) <= this.#highWater;
  }

  /** Ends the LINK: goodbye to the peer, both directions, no redial. */
  close() {
    const link = this.#link;
    this.#down();
    if (link) link.close();
  }

  /** Local only: this direction is considered dead; the link is the owner's. */
  terminate() {
    this.#attempt++;
    this.#down();
  }

  #bindLink(link) {
    link.on('state', this.#onLinkState);
  }

  #receive(data) {
    let message;
    try {
      message = this.#codec.receive(data);
    } catch (error) {
      // A protocol error on the wire — the data-channel 1002: the peer's
      // framing is broken, so is the link.
      this.#escalate(error);
      this.close();
      return;
    }
    if (message === null) return;
    this.emit('message', message.kind === KIND_TEXT ? message.data : message.data);
  }

  #down() {
    if (this.#detach) this.#detach();
    this.#detach = null;
    this.#codec = null;
    if (!this.active) return;
    this.active = false;
    this.emit('close');
  }

  #escalate(error) {
    // The core binds 'error' and routes it through its own escalation; a
    // transport used bare (tests) may have no listener, and Emitter throws
    // on an unheard 'error'.
    if (this.listenerCount('error') === 0) return;
    void this.emit('error', error).catch(() => {});
  }
}

class RtcPeerTransport extends ServerTransport {
  kind = 'webrtc';
  #link;
  #channel;
  #codec;
  #highWater;
  #up = true;
  #detach = null;
  #onError;
  #onLinkState = (state) => {
    if (state === 'failed' || state === 'closed') this.#down();
  };

  /**
   * `peer` is the remote peer id — the transport's `source`, what every log
   * line and the Client's identity carry. Inbound traffic is announced as
   * 'packet' (text) and 'chunk' (bytes) events; PeerHost.attach binds them.
   */
  constructor(link, { peer, framing = {}, highWaterMark, lowWaterMark, onError = null } = {}) {
    super(peer);
    const channel = link.hostChannel;
    if (!channel) throw new Error('RtcPeerTransport: the link has no host channel');
    this.#link = link;
    this.#channel = channel;
    this.#onError = onError;
    this.#highWater = highWaterMark ?? DEFAULT_HIGH_WATER_MARK;
    this.#codec = new ChannelCodec(channel, link.maxMessageSize, framing);
    // What Client.persistent checks: a channel stays open like a socket.
    this.connection = this;
    channel.bufferedAmountLowThreshold = lowWaterMark ?? DEFAULT_LOW_WATER_MARK;
    const onMessage = ({ data }) => this.#receive(data);
    const onClose = () => this.#down();
    const onDrain = () => void this.emit('drain');
    const onChannelError = (event) => this.#error(event?.error ?? new Error('data channel error'));
    channel.addEventListener('message', onMessage);
    channel.addEventListener('close', onClose);
    channel.addEventListener('bufferedamountlow', onDrain);
    channel.addEventListener('error', onChannelError);
    link.on('state', this.#onLinkState);
    this.#detach = () => {
      channel.removeEventListener('message', onMessage);
      channel.removeEventListener('close', onClose);
      channel.removeEventListener('bufferedamountlow', onDrain);
      channel.removeEventListener('error', onChannelError);
      link.off('state', this.#onLinkState);
    };
  }

  get link() {
    return this.#link;
  }

  get channel() {
    return this.#channel;
  }

  /** The backpressure boolean the dispatcher and Broadcast read; false once down. */
  write(data) {
    if (!this.#up) return false;
    return this.#codec.send(data) <= this.#highWater;
  }

  /** Ends the link — the peer's client sees its transport close too. */
  close() {
    this.#down();
    this.#link.close();
  }

  #receive(data) {
    let message;
    try {
      message = this.#codec.receive(data);
    } catch (error) {
      this.#error(error);
      this.close();
      return;
    }
    if (message === null) return;
    void this.emit(message.kind === KIND_TEXT ? 'packet' : 'chunk', message.data);
  }

  #down() {
    if (this.#detach) this.#detach();
    this.#detach = null;
    if (!this.#up) return;
    this.#up = false;
    void this.emit('close');
  }

  #error(error) {
    if (this.#onError) this.#onError(error);
  }
}

// Registered under the name connect() looks up: `transport: 'webrtc'`
// with `link` in the options. The PeerLink constructs it directly.
WrpcClient.transport.webrtc = ClientRtcTransport;
// What the client barrel does once: online/offline listeners. Idempotent —
// addEventListener ignores a duplicate of the same listener.
WrpcClient.initialize();

module.exports = { ClientRtcTransport, RtcPeerTransport, DEFAULT_HIGH_WATER_MARK, DEFAULT_LOW_WATER_MARK };
