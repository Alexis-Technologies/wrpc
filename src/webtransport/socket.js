'use strict';

// A WebTransport session as a WrpcSocket — the engine-port socket contract
// (engine.d.ts, docs/reference/engine.md) over one bidirectional stream, the
// control stream the client opened. That shape is what RpcServer.attachSocket
// takes, and going through it rather than RpcServer.attach is the point: the
// session restore, the declared-headers parse, the receive-side flow control
// and ServerWsTransport itself all come for free, and a WebTransport client
// lands in the same RpcServer, rooms and cluster as a WebSocket one.
//
// Framing is src/webtransport/framing.js on both directions. Outbound flow
// control counts the bytes handed to the stream's writer and not yet taken
// by it (write() on a WHATWG writer resolves when the sink took the chunk):
// send() answers false above the high-water mark and 'drain' fires under the
// low-water mark. Inbound, pause() stops pulling the reader, and QUIC's own
// stream flow control carries the pressure to the peer — the one place a
// WebTransport socket is simpler than a TCP one.
//
// Close codes: `close(code, reason)` maps to `session.close({ closeCode:
// code, reason })`, so the WebSocket close code IS the WebTransport close
// code (1001 on server shutdown, 1002 on a framing violation), and the
// client reads it back from `closed`. A poisoned handle stays quiet: after
// the session is gone every method is a no-op that answers false/0, never a
// throw — the engine contract's rule, same as UwsSocket.

// A node EventEmitter, like Connection and UwsSocket: the engine port's
// 'message' carries two arguments (data, isBinary), which wrpc's own
// single-value Emitter cannot.
const { EventEmitter } = require('node:events');
const {
  StreamParser,
  frame,
  frameText,
  frameCaps,
  datagramText,
  parseDatagram,
  datagramWriter,
  toBytes,
  decodeText,
  parseCaps,
  KIND_TEXT,
  KIND_BINARY,
  KIND_CAPS,
  KIND_TEXT_DEFLATE,
  KIND_BINARY_DEFLATE,
  DEFAULT_MAX_MESSAGE,
} = require('./framing.js');
const { StreamMux } = require('./streams.js');
const { normalizeCompression, negotiate, Sequencer } = require('../compression/index.js');

const TEXT_ENCODER = new TextEncoder();

const DEFAULT_HIGH_WATER_MARK = 1024 * 1024;
const DEFAULT_LOW_WATER_MARK = 256 * 1024;

const closeQuietly = (session, info) => {
  try {
    session.close(info);
  } catch {
    // Already closed.
  }
};

class WtSocket extends EventEmitter {
  // What attachSocket reads into the client's meta; a WebTransport session
  // negotiates no subprotocol, so `protocol` stays ''.
  remoteAddress;
  protocol = '';

  #session;
  #stream;
  #writer;
  #datagrams = null;
  #mux;
  #parser;
  #queued = 0;
  #pressured = false;
  #closed = false;
  #paused = false;
  #resume = null;
  #highWater;
  #lowWater;
  #maxMessage;
  #idle = 0;
  #idleTimer = null;
  // Per-message compression (src/compression): the normalized option, and
  // what is in effect once the peer named the same codec — null until then,
  // and forever when the option is off.
  #compression;
  #active = null;
  // Order around a codec that may answer asynchronously, one per direction.
  #outbound = new Sequencer((error) => this.#error(error));
  #inbound = new Sequencer((error) => this.#violation(error));

  /**
   * `stream` is the control stream — the first bidirectional stream the
   * client opened, which attachSession() waits for. Reading starts at once.
   *
   * `idleTimeout` (ms, 0 = off) terminates the session when nothing arrives
   * on the control stream for that long — the liveness the WebSocket engine
   * gets from its own ping frames. A wrpc client pings on its heartbeat, so
   * a live one is never idle; a host that reports no session end (quico
   * 0.4 does not) needs this to shed a peer that vanished.
   */
  constructor(
    session,
    stream,
    { remoteAddress = '', highWaterMark, lowWaterMark, maxMessage, idleTimeout = 0, compression = null } = {},
  ) {
    super();
    this.#session = session;
    this.#stream = stream;
    this.remoteAddress = remoteAddress;
    this.#highWater = highWaterMark ?? DEFAULT_HIGH_WATER_MARK;
    this.#lowWater = lowWaterMark ?? DEFAULT_LOW_WATER_MARK;
    this.#maxMessage = maxMessage ?? DEFAULT_MAX_MESSAGE;
    this.#compression = normalizeCompression(compression, 'WtSocket: options');
    this.#idle = idleTimeout;
    this.#touch();
    const datagrams = session.datagrams;
    const writer = datagramWriter(datagrams);
    if (writer && typeof datagrams.readable?.getReader === 'function') {
      this.#datagrams = writer;
      void this.#readDatagrams(datagrams.readable);
    }
    this.#writer = stream.writable.getWriter();
    // Binary streams on their own WebTransport streams, negotiated through
    // the capabilities message each end sends first (streams.js). A client
    // under a wire codec announces none, which switches both directions off.
    const mux = new StreamMux(session, {
      emitPacket: (text) => this.emit('message', text, false),
      emitChunk: (chunk) => this.emit('message', chunk, true),
      onQueued: (size) => {
        this.#queued += size;
      },
      onSent: (size) => this.#sent(size),
      // Through the outbound order, not straight to the writer: a stream
      // packet must not overtake a message still being compressed.
      writeControl: (chunk) => this.#enqueue(frame(KIND_BINARY, chunk)),
      // No codec where the mux is on (the peer announced streams only
      // without one), so a packet is its JSON.
      sendControl: (packet) => this.#enqueue(frameText(JSON.stringify(packet))),
    });
    this.#mux = mux;
    this.#parser = new StreamParser({
      maxMessage: this.#maxMessage,
      onMessage: (kind, data) => {
        if (kind === KIND_CAPS) {
          mux.peerCaps(data);
          return void this.#negotiate(data);
        }
        this.#receive(kind, data);
      },
    });
    this.#writer.write(frameCaps(this.#caps(session))).catch(() => {});
    if (typeof session.incomingUnidirectionalStreams?.getReader === 'function') {
      void this.#readUni(session.incomingUnidirectionalStreams);
    }
    // The session's own end — the peer closed, the transport failed — is a
    // close here; our own close() settles it too, by then a no-op.
    session.closed.then(
      (info) => this.#down(info?.closeCode ?? 1006, info?.reason ?? ''),
      (error) => {
        this.#error(error);
        this.#down(1006, '');
      },
    );
    void this.#read();
  }

  get session() {
    return this.#session;
  }

  get stream() {
    return this.#stream;
  }

  /** Bytes handed to the stream and not yet taken by it; 0 once closed. */
  get bufferedAmount() {
    return this.#queued;
  }

  /** The largest datagram the session carries; 0 when it carries none. */
  get maxDatagramSize() {
    const datagrams = this.#session.datagrams;
    if (!datagrams || !this.#datagrams || this.#closed) return 0;
    const size = datagrams.maxDatagramSize;
    return typeof size === 'number' && size > 0 ? size : 1200;
  }

  get isPaused() {
    return this.#paused;
  }

  /** The codec id in effect — both ends named it — or null. */
  get compression() {
    return this.#active === null ? null : this.#active.id;
  }

  // What we announce: the mux's streams, plus the codec when the option is
  // on. The peer compresses only once it has read this and named the same.
  #caps(session) {
    if (this.#compression === null) return StreamMux.caps(session);
    const caps = parseCaps(StreamMux.caps(session)) ?? {};
    caps.deflate = this.#compression.id;
    return JSON.stringify(caps);
  }

  #negotiate(text) {
    const active = negotiate(this.#compression, parseCaps(text)?.deflate);
    this.#active = active;
    this.#parser.deflate = active !== null;
  }

  // An inbound message past the capabilities: plain kinds are delivered at
  // once while nothing is being inflated ahead of them; a compressed kind
  // is inflated — possibly asynchronously — and everything behind it waits
  // its turn, which is what keeps the wire's order.
  #receive(kind, data) {
    const active = this.#active;
    if (kind <= KIND_BINARY) {
      if (active === null || this.#inbound.pending === 0) return void this.#deliver(kind, data);
      return void this.#inbound.push(data, (bytes) => this.#deliver(kind, bytes));
    }
    const plainKind = kind === KIND_TEXT_DEFLATE ? KIND_TEXT : KIND_BINARY;
    let inflated;
    try {
      inflated = active.codec.decode(data, this.#maxMessage);
    } catch (error) {
      return void this.#violation(error);
    }
    this.#inbound.push(
      inflated,
      (bytes) => this.#deliver(plainKind, plainKind === KIND_TEXT ? decodeText(bytes) : bytes),
      (error) => this.#violation(error),
    );
  }

  #deliver(kind, data) {
    if (this.#closed) return;
    if (kind === KIND_TEXT && this.#mux.packet(data)) return;
    this.emit('message', data, kind === KIND_BINARY);
  }

  // A message the peer sent that cannot be read — an inflate that failed
  // or blew the cap, invalid UTF-8 underneath — is its protocol violation,
  // the 1002 a malformed frame gets.
  #violation(error) {
    if (this.#closed) return;
    this.#error(error);
    this.close(1002, 'Protocol error');
  }

  async #read() {
    const reader = this.#stream.readable.getReader();
    try {
      for (;;) {
        if (this.#paused) {
          await new Promise((resolve) => {
            this.#resume = resolve;
          });
        }
        const { value, done } = await reader.read();
        if (done || this.#closed) break;
        this.#touch();
        this.#parser.push(value);
      }
    } catch (error) {
      if (this.#closed) return;
      // A read error is the session's to report through `closed`; a
      // FramingError is the peer's protocol violation — the 1002 of it.
      this.#error(error);
      this.close(1002, 'Protocol error');
      return;
    }
    if (this.#closed) return;
    // The peer ended the control stream: the connection is over. The close
    // is reported through `closed` — with the peer's code when the stream
    // ended because the peer closed the session, with 1000 when only the
    // stream ended and this close() is what ends the session.
    closeQuietly(this.#session, { closeCode: 1000, reason: '' });
  }

  async #readUni(streams) {
    const reader = streams.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done || this.#closed) break;
        this.#mux.accept(value);
      }
    } catch {
      // The session's end, reported through `closed`.
    }
  }

  /**
   * Told about an outbound stream packet before it is serialized (by
   * ServerWtTransport): true when the packet must not go on the control
   * stream — its stream's own FIN or RESET carries it.
   */
  streamControl(packet) {
    return !this.#closed && this.#mux.control(packet);
  }

  async #readDatagrams(readable) {
    const reader = readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done || this.#closed) break;
        this.#touch();
        const text = parseDatagram(value);
        if (text !== null) this.emit('message', text, false);
      }
    } catch {
      // The session's end, reported through `closed`.
    }
  }

  /**
   * A packet as ONE datagram — unreliable, unordered, at most
   * maxDatagramSize bytes: true when it went out, false when the session
   * has no datagrams, the packet does not fit, or the socket is closed —
   * the caller's cue to send() it on the control stream instead.
   */
  sendUnreliable(data) {
    if (this.#closed || !this.#datagrams || typeof data !== 'string') return false;
    const bytes = datagramText(data);
    if (bytes.length > this.maxDatagramSize) return false;
    this.#datagrams.write(bytes).catch(() => {});
    return true;
  }

  /**
   * A string is a packet, bytes are a stream chunk. False above the
   * high-water mark (then 'drain'), and false, quietly, once closed.
   * `options.compress === false` (the per-message opt-out Client.sendRaw
   * passes through writeWith) sends this one plain whatever was negotiated.
   */
  send(data, options = null) {
    if (this.#closed) return false;
    const active = this.#active;
    const plain = active === null || (options !== null && options.compress === false);
    if (typeof data === 'string') {
      if (plain || data.length < active.threshold) return this.#enqueue(frameText(data));
      return this.#compress(KIND_TEXT, TEXT_ENCODER.encode(data));
    }
    const chunk = toBytes(data);
    if (this.#mux.chunk(chunk)) return this.#queued <= this.#highWater;
    if (plain || chunk.length < active.threshold) return this.#enqueue(frame(KIND_BINARY, chunk));
    return this.#compress(KIND_BINARY, chunk);
  }

  // A ready frame, in order: straight to the writer while nothing is being
  // compressed ahead of it, behind the queue otherwise. `#queued` counts it
  // from here on either way.
  #enqueue(bytes) {
    if (this.#outbound.pending === 0) return this.#writeFrame(bytes);
    const size = bytes.length;
    this.#queued += size;
    this.#outbound.push(bytes, (ready) => {
      this.#queued -= size;
      this.#writeFrame(ready);
    });
    return this.#queued <= this.#highWater;
  }

  // Compresses one message past the threshold. The codec may answer at
  // once (zlib) or later (CompressionStream); either way the frame goes
  // out under the compressed kind when it is smaller, plain when it is not
  // or the codec failed — the message is never lost to compression.
  #compress(kind, bytes) {
    const size = bytes.length;
    this.#queued += size;
    const deflated = kind === KIND_TEXT ? KIND_TEXT_DEFLATE : KIND_BINARY_DEFLATE;
    const plain = () => {
      this.#queued -= size;
      this.#writeFrame(frame(kind, bytes));
    };
    let encoded;
    try {
      encoded = this.#active.codec.encode(bytes);
    } catch {
      plain();
      return this.#queued <= this.#highWater;
    }
    this.#outbound.push(
      encoded,
      (out) => {
        if (out.length >= size) return void plain();
        this.#queued -= size;
        this.#writeFrame(frame(deflated, out));
      },
      plain,
    );
    return this.#queued <= this.#highWater;
  }

  #writeFrame(bytes) {
    if (this.#closed) return false;
    const size = bytes.length;
    this.#queued += size;
    // A rejected write is the session failing, which `closed` reports.
    this.#writer.write(bytes).then(
      () => this.#sent(size),
      () => {},
    );
    if (this.#queued > this.#highWater) {
      this.#pressured = true;
      return false;
    }
    return true;
  }

  #sent(size) {
    this.#queued -= size;
    if (this.#closed || !this.#pressured || this.#queued > this.#lowWater) return;
    this.#pressured = false;
    this.emit('drain');
  }

  /** Stops pulling the control stream; QUIC flow control does the rest. */
  pause() {
    this.#paused = true;
  }

  resume() {
    if (!this.#paused) return;
    this.#paused = false;
    const resume = this.#resume;
    this.#resume = null;
    if (resume) resume();
  }

  /** Graceful: the peer's `closed` carries the code and reason. */
  close(code = 1000, reason = '') {
    if (this.#closed) return;
    this.#down(code, reason);
    closeQuietly(this.#session, { closeCode: code, reason });
  }

  /** Hard: no reason travels; the peer sees 1006. */
  terminate() {
    if (this.#closed) return;
    this.#down(1006, '');
    closeQuietly(this.#session);
  }

  // Re-arms the idle timer on every read; unref'd, so an idle socket never
  // keeps a process alive on its own.
  #touch() {
    if (this.#idle <= 0) return;
    clearTimeout(this.#idleTimer);
    this.#idleTimer = setTimeout(() => {
      this.#idleTimer = null;
      this.#error(new Error(`No data for ${this.#idle} ms`));
      this.terminate();
    }, this.#idle);
    this.#idleTimer.unref?.();
  }

  #down(code, reason) {
    if (this.#closed) return;
    this.#closed = true;
    clearTimeout(this.#idleTimer);
    this.#idleTimer = null;
    this.#mux.close();
    this.#queued = 0;
    this.#pressured = false;
    this.resume();
    this.emit('close', code, reason);
  }

  #error(error) {
    // attachSocket binds 'error'; a socket used bare (tests) may not have,
    // and Emitter throws on an unheard 'error'.
    if (this.listenerCount('error') === 0) return;
    this.emit('error', error);
  }
}

module.exports = { WtSocket, DEFAULT_HIGH_WATER_MARK, DEFAULT_LOW_WATER_MARK };
