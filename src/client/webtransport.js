'use strict';

// The WebTransport client transport — `transport: 'wt'`. In the base client
// entry next to ws/http/event rather than in the '@alexify/wrpc/wt' subpath,
// so that the fallback list a browser app wants, `transport: ['wt', 'ws']`,
// needs no extra import: the subpath is the SERVER half (the session
// contract, the socket shim, the host adapters), which a page never loads.
//
// One client-opened bidirectional stream — the control stream — carries
// every packet and every stream chunk under the length-prefixed framing of
// src/webtransport/framing.js: a QUIC stream is a byte stream, so a message
// needs a boundary, and a single ordered stream is what keeps the wire
// identical to a WebSocket's (a `stream` packet precedes its first chunk,
// ping/pong per direction, resume across reconnects). Datagrams and
// per-stream WebTransport streams are additive capabilities on top of it,
// never a replacement of the packets — see docs/guide/wt.md.
//
// The session comes from `globalThis.WebTransport` (a browser) or from
// `options.wt.WebTransport` (an injected implementation — a Node client
// over @fails-components/webtransport, a fake in tests). Where neither
// exists open() throws, and connect() moves on to the next candidate of a
// fallback list at once — that is the whole reason the list exists.

const { WrpcClient, ClientTransport, connectUrl } = require('./core.js');
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
} = require('../webtransport/framing.js');
const { StreamMux } = require('../webtransport/streams.js');

// Outbound flow control, in bytes handed to the writer and not yet taken
// by it: write() answers false above the high-water mark, and 'drain'
// fires once the queue is back under the low-water mark.
const DEFAULT_HIGH_WATER_MARK = 1024 * 1024;
const DEFAULT_LOW_WATER_MARK = 256 * 1024;

// The `WebTransportOptions` handed to the constructor as-is.
const INIT_KEYS = ['serverCertificateHashes', 'congestionControl', 'allowPooling', 'requireUnreliable', 'protocols'];

const UNAVAILABLE =
  'WebTransport is not available here: pass options.wt.WebTransport (an implementation), ' +
  "or name a fallback — transport: ['wt', 'ws']";

// close() on a closed session is a no-op by spec; an implementation that
// throws instead must not turn a teardown into an error.
const closeQuietly = (session, info) => {
  try {
    session.close(info);
  } catch {
    // Already closed.
  }
};

class ClientWtTransport extends ClientTransport {
  // A session can die silently exactly like a socket (a NAT rebinding, a
  // suspended tab): the client's app-level ping/pong is the detector, and
  // the core owns every timer of it.
  heartbeat = true;
  persistent = true;

  #options;
  #session = null;
  #writer = null;
  #datagrams = null;
  #mux = null;
  #parser = null;
  #opening = null;
  // Bumped by terminate(): an open() that was waiting on the handshake when
  // the core gave up must not come back to life on top of a later attempt.
  #attempt = 0;
  #queued = 0;
  #pressured = false;
  #highWater;
  #lowWater;
  #maxMessage;

  /**
   * `options` are the same `wt` options connect() takes, for a transport
   * constructed by hand; the ones handed to open() win per open.
   */
  constructor(url, options = {}) {
    super(url);
    this.#options = options;
    this.#highWater = options.highWaterMark ?? DEFAULT_HIGH_WATER_MARK;
    this.#lowWater = options.lowWaterMark ?? DEFAULT_LOW_WATER_MARK;
    this.#maxMessage = options.maxMessage ?? DEFAULT_MAX_MESSAGE;
  }

  /** The WebTransport session spoken on; null before open() and after close. */
  get session() {
    return this.#session;
  }

  /**
   * The largest datagram the session carries, 0 when it carries none —
   * what the core checks before sending an event unreliably.
   */
  get maxDatagramSize() {
    const datagrams = this.#session?.datagrams;
    if (!datagrams || !this.#datagrams) return 0;
    const size = datagrams.maxDatagramSize;
    return typeof size === 'number' && size > 0 ? size : 1200;
  }

  async open(options = {}) {
    if (this.active) return;
    if (this.#opening) return this.#opening;
    this.#opening = this.#open(options);
    try {
      await this.#opening;
    } finally {
      this.#opening = null;
    }
  }

  async #open(options) {
    const wt = options.wt ? { ...this.#options, ...options.wt } : this.#options;
    const WebTransport = wt.WebTransport ?? globalThis.WebTransport;
    if (typeof WebTransport !== 'function') throw new Error(UNAVAILABLE);
    const attempt = ++this.#attempt;
    const init = {};
    for (let i = 0; i < INIT_KEYS.length; i++) {
      const key = INIT_KEYS[i];
      if (wt[key] !== undefined) init[key] = wt[key];
    }
    const session = new WebTransport(connectUrl(this.url, options.headers, options.meta, this.log), init);
    this.#session = session;
    // The session's own end — a peer close, a transport failure, our own
    // close() — is one 'close' here; a failure while still opening is
    // open()'s rejection instead (ready rejects too).
    session.closed.then(
      () => this.#down(session),
      (error) => this.#down(session, error),
    );
    try {
      await session.ready;
      if (attempt !== this.#attempt) throw new Error('Connection terminated');
      const stream = await session.createBidirectionalStream();
      if (attempt !== this.#attempt) throw new Error('Connection terminated');
      this.#attach(session, stream);
    } catch (error) {
      if (this.#session === session) {
        this.#session = null;
        closeQuietly(session);
      }
      throw error;
    }
  }

  #attach(session, stream) {
    this.#writer = stream.writable.getWriter();
    this.#queued = 0;
    this.#pressured = false;
    // Binary streams on their own WebTransport streams, negotiated through
    // the capabilities message each end sends first. Not under a codec: the
    // mux reads stream packets off the wire, which only JSON allows.
    const mux = this.codec
      ? null
      : new StreamMux(session, {
          emitPacket: (text) => void this.emit('message', text),
          emitChunk: (chunk) => void this.emit('message', chunk),
          onQueued: (size) => {
            this.#queued += size;
          },
          onSent: (size) => this.#sent(size),
          writeControl: (chunk) => this.#writeFrame(frame(KIND_BINARY, chunk)),
          sendControl: (packet) => super.send(packet),
        });
    this.#mux = mux;
    this.#parser = new StreamParser({
      maxMessage: this.#maxMessage,
      onMessage: (kind, data) => {
        if (kind === KIND_CAPS) return void mux?.peerCaps(data);
        if (kind === KIND_TEXT && mux !== null && mux.packet(data)) return;
        this.emit('message', data);
      },
    });
    this.#writer.write(frameCaps(mux ? StreamMux.caps(session) : '{}')).catch(() => {});
    void this.#read(session, stream.readable);
    if (mux && typeof session.incomingUnidirectionalStreams?.getReader === 'function') {
      void this.#readUni(session, session.incomingUnidirectionalStreams, mux);
    }
    // Datagrams, where the session has them: a writer to send unreliable
    // events on, a reader loop that hands the packets they carry to the
    // same 'message' path a stream packet takes.
    const datagrams = session.datagrams;
    const writer = datagramWriter(datagrams);
    if (writer && typeof datagrams.readable?.getReader === 'function') {
      this.#datagrams = writer;
      void this.#readDatagrams(session, datagrams.readable);
    }
    this.active = true;
    // Announced before open() resolves — the core's 'open' handler runs
    // synchronously here, which is the invariant every transport keeps.
    this.emit('open');
  }

  async #read(session, readable) {
    const reader = readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done || this.#session !== session) break;
        this.#parser.push(value);
      }
    } catch (error) {
      // A read error is the session's to report through `closed`; a
      // FramingError is the peer's protocol violation — the WebTransport
      // analogue of a 1002, and the session is hung up below.
      if (this.#session !== session) return;
      this.#escalate(error);
    }
    if (this.#session !== session) return;
    this.#down(session);
    closeQuietly(session);
  }

  async #readUni(session, streams, mux) {
    const reader = streams.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done || this.#session !== session) break;
        mux.accept(value);
      }
    } catch {
      // The session's end, reported through `closed`.
    }
  }

  /** A stream packet is seen before serialization: the mux may take it. */
  send(obj) {
    if (this.#mux !== null && this.#mux.control(obj)) return;
    super.send(obj);
  }

  async #readDatagrams(session, readable) {
    const reader = readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done || this.#session !== session) break;
        const text = parseDatagram(value);
        if (text !== null) this.emit('message', text);
      }
    } catch {
      // The session's end, reported through `closed`.
    }
  }

  /**
   * Sends a packet as ONE datagram — unreliable, unordered, at most
   * maxDatagramSize bytes — and answers true when it went out; false when
   * the session has no datagrams or the packet does not fit, which is the
   * caller's cue to send it on the control stream instead. Never throws
   * for a lost datagram: losing one is the point.
   */
  writeUnreliable(data) {
    if (!this.active || !this.#datagrams || typeof data !== 'string') return false;
    const bytes = datagramText(data);
    if (bytes.length > this.maxDatagramSize) return false;
    this.#datagrams.write(bytes).catch(() => {});
    return true;
  }

  /**
   * A string is a packet, bytes are a stream chunk. Throws when not
   * connected (the core turns that into a coded 503); answers false above
   * the high-water mark, after which 'drain' follows.
   */
  write(data) {
    if (!this.active) throw new Error('Not connected');
    let bytes;
    if (typeof data === 'string') {
      bytes = frameText(data);
    } else {
      const chunk = toBytes(data);
      // A chunk of a stream that has its own WebTransport stream goes there.
      if (this.#mux !== null && this.#mux.chunk(chunk)) return this.#queued <= this.#highWater;
      bytes = frame(KIND_BINARY, chunk);
    }
    return this.#writeFrame(bytes);
  }

  #writeFrame(bytes) {
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
    if (!this.#pressured || this.#queued > this.#lowWater) return;
    this.#pressured = false;
    void this.emit('drain').catch((error) => this.#escalate(error));
  }

  /** A graceful goodbye: the peer's `closed` settles with the close info. */
  close() {
    const session = this.#session;
    if (!session) return;
    this.#down(session);
    closeQuietly(session, { closeCode: 0, reason: '' });
  }

  /** Reports the close now and drops the session; a handshake in flight is abandoned. */
  terminate() {
    this.#attempt++;
    const session = this.#session;
    if (!session) return;
    this.#down(session);
    closeQuietly(session);
  }

  #down(session, error) {
    if (this.#session !== session) return;
    this.#session = null;
    this.#writer = null;
    this.#datagrams = null;
    this.#mux?.close();
    this.#mux = null;
    this.#parser = null;
    if (error) this.#escalate(error);
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

WrpcClient.transport.wt = ClientWtTransport;

module.exports = { ClientWtTransport, DEFAULT_HIGH_WATER_MARK, DEFAULT_LOW_WATER_MARK, UNAVAILABLE };
