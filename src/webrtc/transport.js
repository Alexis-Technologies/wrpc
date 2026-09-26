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
//
// Both halves also run without a link, over a data channel the application
// hands over — the level under RtcLink, the way the event transport takes
// a worker's port: the application owns the peer connection, its signaling
// and its recovery, and wrpc only speaks on the channel it was given. There
// the transport IS the channel's owner as far as wrpc goes: close() and
// terminate() close it (nobody else would restart its ICE), and the client
// half's `channel` may be a factory — how such an application plugs its own
// reconnect into the core's cycle: every re-open asks for the next channel.

// The client CORE, not the client barrel: the barrel also registers the
// ws/http/event transports and the Service Worker proxy, none of which a
// peer needs — and every byte here lands in the webrtc browser bundle.
const { ClientTransport, WrpcClient } = require('../client/core.js');
const { ServerTransport } = require('../rpc/serverTransport.js');
const { isRtcDataChannel } = require('./port.js');
const {
  FrameEncoder,
  FrameDecoder,
  FramingError,
  KIND_TEXT,
  KIND_BINARY,
  FLAG_COMPRESSED,
  MIN_MESSAGE_SIZE,
  DEFAULT_MAX_REASSEMBLY,
  decodeText,
} = require('./framing.js');
const { normalizeCompression, negotiate, Sequencer } = require('../compression/index.js');

const TEXT_ENCODER = new TextEncoder();

// Outbound flow control, in bytes queued on the channel: write() answers
// false above the high-water mark, and 'drain' fires once the channel is
// back under the low-water mark (bufferedAmountLowThreshold).
const DEFAULT_HIGH_WATER_MARK = 1024 * 1024;
const DEFAULT_LOW_WATER_MARK = 256 * 1024;

const positiveInteger = (value) => Number.isInteger(value) && value > 0;

// The cap behind the high-water mark: a message that would put the channel
// past it — its own buffer, the codec's pending bytes and the message
// together — is refused and the CHANNEL is closed, locally: over a link
// that is a redial, over a raw channel the end of it. A browser closes the
// channel itself somewhere past 16 MiB of bufferedAmount, with an exception
// out of send(); this is the same outcome, announced. 0 switches it off.
const DEFAULT_MAX_BACKPRESSURE = 64 * 1024 * 1024;

const normalizeBackpressure = (value, label) => {
  if (value === undefined) return DEFAULT_MAX_BACKPRESSURE;
  if (!Number.isInteger(value) || value < 0) {
    throw new TypeError(`${label}: maxBackpressure must be a non-negative integer of bytes (0 = off)`);
  }
  return value;
};

// The two marks, checked: positive integers, the low one no higher than the
// high one, and a low default that follows a high mark set below it — a
// low mark above the high one used to mean a 'drain' that never came.
const normalizeWaterMarks = (highWaterMark, lowWaterMark, label) => {
  if (highWaterMark !== undefined && !positiveInteger(highWaterMark)) {
    throw new TypeError(`${label}: highWaterMark must be a positive integer of bytes`);
  }
  if (lowWaterMark !== undefined && !positiveInteger(lowWaterMark)) {
    throw new TypeError(`${label}: lowWaterMark must be a positive integer of bytes`);
  }
  const high = highWaterMark ?? DEFAULT_HIGH_WATER_MARK;
  if (lowWaterMark === undefined) return { high, low: Math.min(DEFAULT_LOW_WATER_MARK, high) };
  if (lowWaterMark > high) throw new TypeError(`${label}: lowWaterMark must not exceed highWaterMark`);
  return { high, low: lowWaterMark };
};

const toBytes = (data) => {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  throw new TypeError('write() takes a string or bytes');
};

const CLOSED_CHANNEL = 'The data channel is closed; pass a factory as `channel` to reconnect';

// A raw channel may still be connecting when it is handed over (created
// before the peer connection came up); an already closed one is refused,
// not waited on — nothing would ever open it again.
const waitChannelOpen = (channel) =>
  new Promise((resolve, reject) => {
    if (channel.readyState === 'open') return void resolve();
    if (channel.readyState !== 'connecting') return void reject(new Error(CLOSED_CHANNEL));
    const settle = (fn, value) => {
      channel.removeEventListener('open', onOpen);
      channel.removeEventListener('close', onClose);
      channel.removeEventListener('error', onError);
      fn(value);
    };
    const onOpen = () => settle(resolve);
    const onClose = () => settle(reject, new Error(CLOSED_CHANNEL));
    const onError = (event) => settle(reject, event?.error ?? new Error('data channel error'));
    channel.addEventListener('open', onOpen);
    channel.addEventListener('close', onClose);
    channel.addEventListener('error', onError);
  });

// The per-message compression in effect on a channel, `{ encode, decode }`:
// over a link, what the two lists agree on once the peer's description
// announced its own (src/compression negotiate); over a raw channel the
// HEAD of the local list both ways — there is no handshake on a raw channel
// to negotiate through, so both applications name the same first codec, or
// neither turns it on.
const activeCompression = (compression, link) => {
  if (compression === null) return null;
  if (link !== null) return negotiate(compression, link.peerCaps?.enc);
  const head = compression.codecs[0];
  return { encode: head, decode: head };
};

// One place for what both halves do with a channel: encode outbound frames
// into it and decode inbound ones from it. `sink` is bound once so the hot
// path allocates nothing per call. With compression in effect a message
// past the threshold is compressed BEFORE fragmentation — here, the one
// place it exists whole — and the COMPRESSED flag rides every fragment; the
// two Sequencers keep each direction in order around a codec that answers
// asynchronously, and cost nothing while nothing is in flight.
class ChannelCodec {
  #channel;
  #encoder;
  #decoder;
  #sink;
  #compression;
  #maxInflate;
  #onMessage;
  #onError;
  #outbound;
  #inbound;
  // Bytes accepted by send() and not yet framed: counted with the
  // channel's bufferedAmount so backpressure sees them.
  #pending = 0;
  // The marks, and whether a send() answered false since the last drain:
  // the codec owns the 'drain' because it owns half of what is buffered.
  // The channel's bufferedamountlow alone could not say it — bytes waiting
  // in an asynchronous codec never cross the channel's threshold, so a
  // false answered for them used to be a false with no drain ever after.
  #highWater;
  #lowWater;
  #onDrain;
  #blocked = false;
  // The cap, the fault it reports through, and whether it fired: after it
  // the channel is closing and send() answers false without touching it.
  #maxBackpressure;
  #onFault;
  #faulted = false;

  constructor(
    channel,
    maxMessageSize,
    framing,
    { compression = null, onMessage, onError, highWater, lowWater, onDrain, maxBackpressure = 0, onFault = null },
  ) {
    this.#channel = channel;
    this.#encoder = new FrameEncoder(maxMessageSize);
    this.#decoder = new FrameDecoder(framing);
    this.#decoder.compressed = compression !== null;
    this.#sink = (frame) => channel.send(frame);
    this.#compression = compression;
    this.#maxInflate = framing?.maxReassembly ?? DEFAULT_MAX_REASSEMBLY;
    this.#onMessage = onMessage;
    this.#onError = onError;
    this.#highWater = highWater;
    this.#lowWater = lowWater;
    this.#onDrain = onDrain;
    this.#maxBackpressure = maxBackpressure;
    this.#onFault = onFault;
    this.#outbound = new Sequencer(onError);
    this.#inbound = new Sequencer(onError);
  }

  /** The codec ids in effect — `{ encode, decode }` — or null. */
  get compression() {
    const active = this.#compression;
    return active === null ? null : { encode: active.encode.id, decode: active.decode.id };
  }

  /**
   * A string is a packet, bytes are a chunk; `options.compress === false`
   * sends this one plain. Answers true under the high-water mark — the
   * bytes still being compressed counted — and false past it, after which
   * exactly one 'drain' follows.
   */
  send(data, options = null) {
    if (this.#faulted) return false;
    const text = typeof data === 'string';
    const bytes = text ? null : toBytes(data);
    const size = text ? data.length : bytes.length;
    const max = this.#maxBackpressure;
    if (max !== 0 && this.#channel.bufferedAmount + this.#pending + size > max) {
      this.#faulted = true;
      const error = new Error(`Backpressure limit exceeded (${size} bytes over ${max}), closing the channel`);
      error.code = 'backpressure';
      if (this.#onFault !== null) this.#onFault(error);
      this.#channel.close();
      return false;
    }
    const compression = this.#compression;
    const plain = compression === null || (options !== null && options.compress === false);
    if (text) {
      if (plain || size < compression.encode.threshold) this.#enqueueText(data);
      else this.#compress(KIND_TEXT, TEXT_ENCODER.encode(data));
    } else if (plain || size < compression.encode.threshold) this.#enqueue(KIND_BINARY, bytes);
    else this.#compress(KIND_BINARY, bytes);
    if (this.#channel.bufferedAmount + this.#pending <= this.#highWater) return true;
    this.#blocked = true;
    return false;
  }

  /**
   * Something buffered was taken — the channel's bufferedamountlow, or a
   * codec settling: the one 'drain' after a false, once under the low mark.
   */
  drained() {
    if (!this.#blocked || this.#channel.bufferedAmount + this.#pending > this.#lowWater) return;
    this.#blocked = false;
    this.#onDrain();
  }

  #enqueueText(text) {
    if (this.#outbound.pending === 0) return void this.#encoder.encodeText(text, this.#sink);
    this.#pending += text.length;
    this.#outbound.push(text, (ready) => {
      this.#pending -= text.length;
      this.#encoder.encodeText(ready, this.#sink);
      this.drained();
    });
  }

  #enqueue(kind, bytes) {
    if (this.#outbound.pending === 0) return void this.#encoder.encode(kind, bytes, this.#sink);
    this.#pending += bytes.length;
    this.#outbound.push(bytes, (ready) => {
      this.#pending -= bytes.length;
      this.#encoder.encode(kind, ready, this.#sink);
      this.drained();
    });
  }

  // Compressed under the flag when the codec shrank it, plain when it did
  // not or failed — a message is never lost to compression.
  #compress(kind, bytes) {
    const size = bytes.length;
    this.#pending += size;
    const plain = () => {
      this.#pending -= size;
      this.#encoder.encode(kind, bytes, this.#sink);
      this.drained();
    };
    let encoded;
    try {
      encoded = this.#compression.encode.codec.encode(bytes);
    } catch {
      return void plain();
    }
    this.#outbound.push(
      encoded,
      (out) => {
        if (out.length >= size) return void plain();
        this.#pending -= size;
        this.#encoder.encode(kind | FLAG_COMPRESSED, out, this.#sink);
        this.drained();
      },
      plain,
    );
  }

  /**
   * Feeds one channel message; a completed message reaches `onMessage(kind,
   * data)` in wire order — at once on the plain path, after its inflate
   * (and behind whatever is still inflating) otherwise. Throws FramingError
   * on a malformed frame, exactly as the decoder does; an inflate that
   * fails or blows the cap reaches `onError` as one instead.
   */
  receive(data) {
    const message = this.#decoder.push(data);
    if (message === null) return;
    const { kind } = message;
    if (!message.compressed) {
      if (this.#inbound.pending === 0) return void this.#onMessage(kind, message.data);
      return void this.#inbound.push(message.data, (bytes) => this.#onMessage(kind, bytes));
    }
    let inflated;
    try {
      inflated = this.#compression.decode.codec.decode(message.data, this.#maxInflate);
    } catch (error) {
      return void this.#onError(inflateError(error));
    }
    this.#inbound.push(
      inflated,
      (bytes) => this.#onMessage(kind, kind === KIND_TEXT ? decodeText(bytes) : bytes),
      (error) => this.#onError(inflateError(error)),
    );
  }
}

// An inflate failure is the peer's protocol violation, reported in the
// decoder's own shape so both transports treat it as they treat a bad frame.
const inflateError = (error) => {
  const wrapped = new FramingError(`inflate failed: ${error?.message ?? error}`, 'inflate');
  wrapped.cause = error;
  return wrapped;
};

class ClientRtcTransport extends ClientTransport {
  // A data channel can die silently exactly like a socket (ICE stalls, the
  // peer's tab is suspended): the client's app-level ping/pong is the
  // detector, and the core owns every timer of it.
  heartbeat = true;
  persistent = true;

  #link = null;
  // Raw-channel mode: the channel or factory handed over, and the channel
  // currently spoken on (set before its open is awaited, so a terminate()
  // during that wait closes it).
  #source = null;
  #channel = null;
  #maxMessageSize;
  #framing;
  #highWater;
  #lowWater;
  #maxBackpressure;
  #compression = null;
  #codec = null;
  #detach = null;
  #opening = null;
  // Bumped by terminate(): an open() that was waiting on the link when the
  // core gave up must not come back to life on top of a later attempt.
  #attempt = 0;
  #onLinkState = (state) => {
    if (state === 'failed' || state === 'closed') this.#down();
  };

  constructor(
    url,
    {
      link = null,
      channel = null,
      maxMessageSize = MIN_MESSAGE_SIZE,
      framing = {},
      highWaterMark,
      lowWaterMark,
      maxBackpressure,
      compression = null,
    } = {},
  ) {
    super(url);
    if (link && channel) throw new TypeError('ClientRtcTransport: link and channel are mutually exclusive');
    this.#maxBackpressure = normalizeBackpressure(maxBackpressure, 'ClientRtcTransport: options');
    this.#link = link;
    this.#source = channel;
    this.#maxMessageSize = maxMessageSize;
    this.#framing = framing;
    const marks = normalizeWaterMarks(highWaterMark, lowWaterMark, 'ClientRtcTransport: options');
    this.#highWater = marks.high;
    this.#lowWater = marks.low;
    this.#compression = normalizeCompression(compression, 'ClientRtcTransport: options');
    if (link) this.#bindLink(link);
  }

  get link() {
    return this.#link;
  }

  /** The compression codec id in effect on the channel — both ends named it — or null. */
  get compression() {
    return this.#codec === null ? null : this.#codec.compression;
  }

  /** The data channel spoken on — a link's client channel or the raw one; null before open(). */
  get channel() {
    return this.#channel;
  }

  /**
   * Waits for the link to be connected and takes over its client channel —
   * or, without a link, for the channel handed over (asking the factory
   * for it first). Re-entrant: the core re-opens the same transport on
   * every reconnect, and after a redial the link hands out a new channel
   * (the factory, a new one). The link or channel may also arrive here, as
   * `options.link` / `options.channel`, when the transport was picked by
   * name through connect() — the same way the event transport receives
   * `options.worker`.
   */
  async open(options = {}) {
    const link = this.#link ?? options.link ?? null;
    const source = link ? null : (this.#source ?? options.channel ?? null);
    if (!link && !source) {
      throw new Error('WebRTC transport needs a link or a channel: pass options.link or options.channel to connect()');
    }
    if (link && this.#link !== link) {
      this.#link = link;
      this.#bindLink(link);
    }
    if (source) {
      this.#source = source;
      if (options.maxMessageSize !== undefined) this.#maxMessageSize = options.maxMessageSize;
    }
    // connect()'s own `compression` — the option a Node ws client and a
    // WebTransport client share — resolved per open like link/channel.
    if (options.compression !== undefined) {
      this.#compression = normalizeCompression(options.compression, 'webrtc transport: options');
    }
    if (this.active) return;
    if (this.#opening) return this.#opening;
    this.#opening = link ? this.#openLink(link) : this.#openChannel(source);
    try {
      await this.#opening;
    } finally {
      this.#opening = null;
    }
  }

  async #openLink(link) {
    const attempt = ++this.#attempt;
    await link.waitOpen();
    if (attempt !== this.#attempt) throw new Error('Connection terminated');
    const channel = link.clientChannel;
    if (!channel || channel.readyState !== 'open') throw new Error('The link has no open client channel');
    this.#attach(channel, link.maxMessageSize, activeCompression(this.#compression, link));
  }

  async #openChannel(source) {
    const attempt = ++this.#attempt;
    const channel = typeof source === 'function' ? await source() : source;
    if (attempt !== this.#attempt) throw new Error('Connection terminated');
    if (!isRtcDataChannel(channel)) {
      throw new TypeError('WebRTC transport: channel must be a data channel, or a factory returning one');
    }
    this.#channel = channel;
    await waitChannelOpen(channel);
    if (attempt !== this.#attempt) throw new Error('Connection terminated');
    // A browser's default is 'blob'; the framing reads bytes.
    channel.binaryType = 'arraybuffer';
    this.#attach(channel, this.#maxMessageSize, activeCompression(this.#compression, null));
  }

  #attach(channel, maxMessageSize, compression) {
    this.#channel = channel;
    const codec = new ChannelCodec(channel, maxMessageSize, this.#framing, {
      compression,
      onMessage: (_kind, data) => void this.emit('message', data),
      onError: (error) => this.#violation(error),
      highWater: this.#highWater,
      lowWater: this.#lowWater,
      onDrain: () => void this.emit('drain').catch((error) => this.#escalate(error)),
      maxBackpressure: this.#maxBackpressure,
      onFault: (error) => this.#escalate(error),
    });
    this.#codec = codec;
    channel.bufferedAmountLowThreshold = this.#lowWater;
    // Scoped to THIS channel: a stale channel's late events after a redial
    // must not reach a transport that has moved on.
    const onMessage = ({ data }) => this.#receive(data);
    const onClose = () => this.#down();
    const onDrain = () => codec.drained();
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
    return this.#codec.send(data);
  }

  /**
   * Ends the LINK: goodbye to the peer, both directions, no redial. Over a
   * raw channel, closes that channel — the host half on the other end sees
   * its close.
   */
  close() {
    const link = this.#link;
    const channel = this.#channel;
    this.#down();
    if (link) link.close();
    else if (channel) channel.close();
  }

  /**
   * Local only: this direction is considered dead; the link is the
   * owner's. The core terminates on a heartbeat timeout — a path that is
   * silently dead while the link still reads 'connected' — and the RTC
   * remedy for that is an ICE restart: it either heals the path under the
   * open channels or fails the link, whose owner then redials. A raw
   * channel has no such owner: a dead one is closed, and the next open()
   * asks the factory for its successor.
   */
  terminate() {
    this.#attempt++;
    const link = this.#link;
    const channel = this.#channel;
    this.#down();
    if (link) {
      if (link.state === 'connected') link.restart();
    } else if (channel) {
      channel.close();
    }
  }

  #bindLink(link) {
    link.on('state', this.#onLinkState);
  }

  #receive(data) {
    try {
      this.#codec.receive(data);
    } catch (error) {
      this.#violation(error);
    }
  }

  // A protocol error on the wire — the data-channel 1002: the peer's
  // framing is broken (or what it compressed does not inflate), so is the
  // link. Scoped to the codec that saw it: a late inflate on a channel
  // already replaced must not close its successor.
  #violation(error) {
    if (this.#codec === null) return;
    this.#escalate(error);
    this.close();
  }

  #down() {
    if (this.#detach) this.#detach();
    this.#detach = null;
    this.#codec = null;
    this.#channel = null;
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
  #up = true;
  #detach = null;
  #onError;
  #onLinkState = (state) => {
    if (state === 'failed' || state === 'closed') this.#down();
  };

  /**
   * Over a link (its host channel) or over a raw data channel. `peer` is
   * the remote peer id — the transport's `source`, what every log line and
   * the Client's identity carry; over a raw channel it defaults to the
   * channel's label. Inbound traffic is announced as 'packet' (text) and
   * 'chunk' (bytes) events; PeerHost.attach and RpcServer.attach bind them.
   */
  constructor(
    source,
    {
      peer,
      maxMessageSize = MIN_MESSAGE_SIZE,
      framing = {},
      highWaterMark,
      lowWaterMark,
      maxBackpressure,
      onError = null,
      compression = null,
    } = {},
  ) {
    const raw = isRtcDataChannel(source);
    const channel = raw ? source : (source?.hostChannel ?? null);
    if (!channel) throw new Error('RtcPeerTransport: no host channel — pass a data channel or a link that has one');
    super(raw ? (peer ?? (channel.label || 'data channel')) : peer);
    const link = raw ? null : source;
    this.#link = link;
    this.#channel = channel;
    this.#onError = onError;
    const marks = normalizeWaterMarks(highWaterMark, lowWaterMark, 'RtcPeerTransport: options');
    // A link's channels are already binary; a raw one carries the browser
    // default ('blob') until told otherwise.
    if (raw) channel.binaryType = 'arraybuffer';
    const normalized = normalizeCompression(compression, 'RtcPeerTransport: options');
    const codec = new ChannelCodec(channel, raw ? maxMessageSize : link.maxMessageSize, framing, {
      compression: activeCompression(normalized, link),
      onMessage: (kind, data) => void this.emit(kind === KIND_TEXT ? 'packet' : 'chunk', data),
      onError: (error) => this.#violation(error),
      highWater: marks.high,
      lowWater: marks.low,
      onDrain: () => void this.emit('drain'),
      maxBackpressure: normalizeBackpressure(maxBackpressure, 'RtcPeerTransport: options'),
      onFault: (error) => this.#error(error),
    });
    this.#codec = codec;
    // What Client.persistent checks: a channel stays open like a socket.
    this.connection = this;
    channel.bufferedAmountLowThreshold = marks.low;
    const onMessage = ({ data }) => this.#receive(data);
    const onClose = () => this.#down();
    const onDrain = () => codec.drained();
    const onChannelError = (event) => this.#error(event?.error ?? new Error('data channel error'));
    channel.addEventListener('message', onMessage);
    channel.addEventListener('close', onClose);
    channel.addEventListener('bufferedamountlow', onDrain);
    channel.addEventListener('error', onChannelError);
    if (link) link.on('state', this.#onLinkState);
    this.#detach = () => {
      channel.removeEventListener('message', onMessage);
      channel.removeEventListener('close', onClose);
      channel.removeEventListener('bufferedamountlow', onDrain);
      channel.removeEventListener('error', onChannelError);
      if (link) link.off('state', this.#onLinkState);
    };
  }

  /** null over a raw channel. */
  get link() {
    return this.#link;
  }

  get channel() {
    return this.#channel;
  }

  /** The compression codec id in effect on the channel — both ends named it — or null. */
  get compression() {
    return this.#codec.compression;
  }

  /** The backpressure boolean the dispatcher and Broadcast read; false once down. */
  write(data) {
    if (!this.#up) return false;
    return this.#codec.send(data);
  }

  // A write with per-message options (`compress: false`) — what
  // Client.sendRaw and a Broadcast use, as on a WebSocket.
  writeWith(text, options) {
    if (!this.#up) return false;
    return this.#codec.send(text, options);
  }

  /** Ends the link (or closes the raw channel) — the peer's client sees its transport close too. */
  close() {
    this.#down();
    if (this.#link) this.#link.close();
    else this.#channel.close();
  }

  #receive(data) {
    try {
      this.#codec.receive(data);
    } catch (error) {
      this.#violation(error);
    }
  }

  #violation(error) {
    if (!this.#up) return;
    this.#error(error);
    this.close();
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

module.exports = {
  ClientRtcTransport,
  RtcPeerTransport,
  normalizeWaterMarks,
  normalizeBackpressure,
  DEFAULT_HIGH_WATER_MARK,
  DEFAULT_LOW_WATER_MARK,
  DEFAULT_MAX_BACKPRESSURE,
};
