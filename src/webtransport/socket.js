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
  KIND_TEXT,
  KIND_BINARY,
  KIND_CAPS,
  DEFAULT_MAX_MESSAGE,
} = require('./framing.js');
const { StreamMux } = require('./streams.js');

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
  #idle = 0;
  #idleTimer = null;

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
  constructor(session, stream, { remoteAddress = '', highWaterMark, lowWaterMark, maxMessage, idleTimeout = 0 } = {}) {
    super();
    this.#session = session;
    this.#stream = stream;
    this.remoteAddress = remoteAddress;
    this.#highWater = highWaterMark ?? DEFAULT_HIGH_WATER_MARK;
    this.#lowWater = lowWaterMark ?? DEFAULT_LOW_WATER_MARK;
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
      writeControl: (chunk) => this.#writeFrame(frame(KIND_BINARY, chunk)),
      // No codec where the mux is on (the peer announced streams only
      // without one), so a packet is its JSON.
      sendControl: (packet) => this.#writeFrame(frameText(JSON.stringify(packet))),
    });
    this.#mux = mux;
    this.#parser = new StreamParser({
      maxMessage: maxMessage ?? DEFAULT_MAX_MESSAGE,
      onMessage: (kind, data) => {
        if (kind === KIND_CAPS) return void mux.peerCaps(data);
        if (kind === KIND_TEXT && mux.packet(data)) return;
        this.emit('message', data, kind === KIND_BINARY);
      },
    });
    this.#writer.write(frameCaps(StreamMux.caps(session))).catch(() => {});
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
   */
  send(data) {
    if (this.#closed) return false;
    let bytes;
    if (typeof data === 'string') {
      bytes = frameText(data);
    } else {
      const chunk = toBytes(data);
      if (this.#mux.chunk(chunk)) return this.#queued <= this.#highWater;
      bytes = frame(KIND_BINARY, chunk);
    }
    return this.#writeFrame(bytes);
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
