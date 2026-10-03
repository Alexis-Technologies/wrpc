'use strict';

// One WebTransport channel: a session's control stream, its datagrams and
// its side streams as ONE ordered, flow-controlled message pipe — the part
// of the protocol the two ends share. `WtSocket` (the server's engine-port
// socket) and `ClientWtTransport` (the client's transport) are adapters
// over it: each keeps what is its own — events and their shapes, pause and
// the idle timer and the log on the server; the open lifecycle, the wire
// codec and the session-encryption seam on the client — and neither frames,
// negotiates, compresses or counts a byte itself. They used to be mirror
// copies of all that, and a change of the channel (a new kind, the
// accounting of `bufferedAmount`, the graceful close, the datagram drop
// policy) was two edits that could disagree.
//
// Browser-safe — the client transport is in the main browser entry — so no
// node:events: the adapter is told through callbacks, set once, called per
// frame without an allocation. The six every adapter has are POSITIONAL
// (a minifier renames a parameter, never an option's key — measured in the
// main entry's budget, scripts/size.js):
//
//   onMessage(data, isBinary)  a packet (string) or a stream chunk (bytes)
//   onDrain()                  the queue fell back under the low-water mark
//   onViolation(error)         the peer sent what cannot be read
//   onOverflow(error)          the queue passed `maxBackpressure`
//   onError(error)             a send-side step failed out of band
//   onEnd()                    the peer ended the control stream
//
// and what only the server's socket has is in `options`: `gate()`,
// `onActivity()` (bytes arrived — the control stream, a side stream, a
// datagram), `onCodecError`, `onDatagramDrop`, and the mux's own options.
//
// Outbound flow control counts the bytes handed to the stream's writer and
// not yet taken by it (write() on a WHATWG writer resolves when the sink
// took the chunk): send() answers false above the high-water mark, and
// onDrain follows under the low-water mark. Inbound, `gate()` is what the
// reads wait on while the adapter is paused, and QUIC's own flow control
// carries the pressure to the peer.

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
  KIND_TEXT_COMPRESSED,
  KIND_BINARY_COMPRESSED,
  DEFAULT_MAX_MESSAGE,
} = require('./framing.js');
const { StreamMux } = require('./streams.js');
const { negotiate, Sequencer, INFLIGHT_LIMIT } = require('../compression/index.js');

const TEXT_ENCODER = new TextEncoder();

const DEFAULT_HIGH_WATER_MARK = 1024 * 1024;
const DEFAULT_LOW_WATER_MARK = 256 * 1024;
// The cap behind the high-water mark: a peer that never drains is hung up
// on once this much is queued for it, as the WebSocket engine's
// maxBackpressure does. 0 switches it off.
const DEFAULT_MAX_BACKPRESSURE = 64 * 1024 * 1024;
// A graceful close ends the control stream first and the session after it:
// how long the stream may take to hand over what was already written (the
// WebSocket engine's CLOSE_TIMEOUT), and how long the END of the peer's
// control stream is given to be followed by its session close.
const DEFAULT_CLOSE_TIMEOUT = 1000;
const CLOSE_GRACE = 200;

const normalizeBackpressure = (value, label) => {
  if (value === undefined) return DEFAULT_MAX_BACKPRESSURE;
  if (!Number.isInteger(value) || value < 0) {
    throw new TypeError(`${label}: maxBackpressure must be a non-negative integer of bytes (0 = off)`);
  }
  return value;
};

// close() on a closed session is a no-op by spec; an implementation that
// throws instead must not turn a teardown into an error. Here rather than
// in port.js: the client transport is in the browser bundle, which the
// port's validators have no business in.
const closeQuietly = (session, info) => {
  try {
    session.close(info);
  } catch {
    // Already closed.
  }
};

class WtChannel {
  #session;
  #writer;
  #datagrams = null;
  #datagramsInFlight = 0;
  #datagramsDropped = 0;
  #maxDatagrams;
  #mux = null;
  #parser;
  #queued = 0;
  #pressured = false;
  #closed = false;
  #highWater;
  #lowWater;
  #maxBackpressure;
  #maxMessage;
  #closeTimeout;
  // Per-message compression (src/compression): the normalized option, and
  // what is in effect once the peer named a codec of the list — null until
  // then, and forever when the option is off.
  #compression;
  #active = null;
  // Whether this end reads framed messages — announced as `f` — and what
  // to tell once the peer's capabilities said whether IT reads them.
  #frames;
  #onFrames;
  // Order around a codec that may answer asynchronously, one per direction.
  #outbound;
  #inbound;
  #gate;
  #onMessage;
  #onDrain;
  #onViolation;
  #onOverflow;
  #onActivity;
  #onCodecError;
  #onDatagramDrop;

  /**
   * `stream` is the control stream; reading starts at once, and the
   * capabilities message is the first thing written. `mux: false` keeps
   * every chunk on the control stream and announces no streams (a wire
   * codec, a sealed session); an object is the StreamMux's own options
   * (`onRefused`, `onFallback`, `maxHeldStreams`, `holdTimeout`).
   * `datagrams: false` leaves the session's datagrams alone.
   * `maxDatagramsInFlight` (0 = no bound) is how many datagrams may wait
   * for the session before a further one is dropped. `frames` says this end
   * reads framed messages (revision 2); `onFrames(peerReads)` hears the
   * peer's answer — until it, an end sends none.
   */
  constructor(session, stream, options, onMessage, onDrain, onViolation, onOverflow, onError, onEnd) {
    const {
      mux = true,
      datagrams = true,
      compression = null,
      maxDatagramsInFlight = 0,
      gate = null,
      onActivity = null,
      onCodecError = null,
      onDatagramDrop = null,
      frames = false,
      onFrames = null,
    } = options;
    const { highWaterMark, lowWaterMark, maxBackpressure, maxMessage, closeTimeout } = options;
    this.#session = session;
    this.#highWater = highWaterMark ?? DEFAULT_HIGH_WATER_MARK;
    this.#lowWater = lowWaterMark ?? DEFAULT_LOW_WATER_MARK;
    this.#maxBackpressure = normalizeBackpressure(maxBackpressure, 'WebTransport: options');
    this.#maxMessage = maxMessage ?? DEFAULT_MAX_MESSAGE;
    this.#closeTimeout = closeTimeout ?? DEFAULT_CLOSE_TIMEOUT;
    this.#compression = compression;
    this.#maxDatagrams = maxDatagramsInFlight;
    this.#gate = gate;
    this.#onMessage = onMessage;
    this.#onDrain = onDrain;
    this.#onViolation = onViolation;
    this.#onOverflow = onOverflow;
    this.#onActivity = onActivity;
    this.#onCodecError = onCodecError;
    this.#onDatagramDrop = onDatagramDrop;
    this.#frames = frames;
    this.#onFrames = onFrames;
    this.#outbound = new Sequencer(onError);
    this.#inbound = new Sequencer((error) => this.#violation(error));
    this.#writer = stream.writable.getWriter();
    // Binary streams on their own WebTransport streams, negotiated through
    // the capabilities message each end sends first (streams.js).
    if (mux) {
      this.#mux = new StreamMux(session, {
        emitPacket: (text) => onMessage(text, false),
        emitChunk: (chunk) => onMessage(chunk, true),
        onQueued: (size) => {
          this.#queued += size;
        },
        onSent: (size) => this.#sent(size),
        // Through the outbound order, not straight to the writer: a stream
        // packet must not overtake a message still being compressed.
        writeControl: (chunk) => {
          if (!this.exceedsBackpressure()) this.#enqueue(frame(KIND_BINARY, chunk));
        },
        // No codec where the mux is on, so a stream packet is its JSON.
        sendControl: (packet) => {
          if (!this.exceedsBackpressure()) this.#enqueue(frameText(JSON.stringify(packet)));
        },
        // A pause stops the side streams too, and their bytes are liveness
        // as much as the control stream's.
        gate,
        onActivity,
        ...(mux === true ? null : mux),
      });
    }
    const parser = new StreamParser({
      maxMessage: this.#maxMessage,
      onMessage: (kind, data) => {
        if (kind !== KIND_CAPS) return void this.#receive(kind, data);
        this.#mux?.peerCaps(data);
        const caps = parseCaps(data);
        // What the two lists share: this side compresses with the first
        // codec of ITS list the peer announced, and reads the peer's.
        this.#active = negotiate(this.#compression, caps?.enc);
        parser.compressed = this.#active !== null;
        // The revision, as a worker port settles it on its first ping: a
        // frame goes only to a peer that said it reads one. Two ends whose
        // `attachments` disagree used to meet a frame the other refused —
        // an id-less error, and a call that timed out.
        this.#onFrames?.(caps?.f === 1);
      },
    });
    this.#parser = parser;
    this.#writer.write(frameCaps(this.#caps())).catch(() => {});
    void this.#read(stream.readable, onEnd);
    if (this.#mux !== null && typeof session.incomingUnidirectionalStreams?.getReader === 'function') {
      void this.#readUni(session.incomingUnidirectionalStreams);
    }
    // Datagrams, where the session has them: a writer to send unreliable
    // events on, a reader loop that hands the packets they carry to the
    // same onMessage a stream packet takes.
    const duplex = session.datagrams;
    const writer = datagrams ? datagramWriter(duplex) : null;
    if (writer && typeof duplex.readable?.getReader === 'function') {
      this.#datagrams = writer;
      void this.#readDatagrams(duplex.readable);
    }
  }

  get closed() {
    return this.#closed;
  }

  /** Bytes handed to the stream and not yet taken by it; 0 once shut. */
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

  /** The codec ids in effect — `{ encode, decode }` — or null while the two lists share none. */
  get compression() {
    const active = this.#active;
    return active === null ? null : { encode: active.encode.id, decode: active.decode.id };
  }

  /** Datagrams dropped because the session had not taken the ones before them. */
  get droppedDatagrams() {
    return this.#datagramsDropped;
  }

  // What we announce: the mux's streams, plus the codecs we hold, in our
  // order, when the option is on, and `f` when this end reads frames. The peer compresses only once it has read
  // this, with the first of ITS list found here.
  #caps() {
    const text = this.#mux === null ? '{}' : StreamMux.caps(this.#session);
    if (this.#compression === null && !this.#frames) return text;
    const caps = parseCaps(text) ?? {};
    if (this.#compression !== null) caps.enc = this.#compression.ids;
    if (this.#frames) caps.f = 1;
    return JSON.stringify(caps);
  }

  // --- inbound --------------------------------------------------------

  // A message past the capabilities: plain kinds are delivered at once
  // while nothing is being inflated ahead of them; a compressed kind is
  // inflated — possibly asynchronously — and everything behind it waits its
  // turn, which is what keeps the wire's order.
  #receive(kind, data) {
    const active = this.#active;
    if (kind <= KIND_BINARY) {
      if (active === null || this.#inbound.pending === 0) return void this.#deliver(kind, data);
      return void this.#inbound.push(data, (bytes) => this.#deliver(kind, bytes));
    }
    const plainKind = kind === KIND_TEXT_COMPRESSED ? KIND_TEXT : KIND_BINARY;
    const codec = active.decode.codec;
    const decode = () => codec.decode(data, this.#maxMessage);
    // Started now while few are in flight, in its slot past the limit: a
    // burst of compressed frames is not a burst of parallel inflates.
    let inflated = decode;
    if (this.#inbound.pending < INFLIGHT_LIMIT) {
      try {
        inflated = decode();
      } catch (error) {
        return void this.#violation(error);
      }
    }
    this.#inbound.push(
      inflated,
      (bytes) => this.#deliver(plainKind, plainKind === KIND_TEXT ? decodeText(bytes) : bytes),
      (error) => this.#violation(error),
    );
  }

  #deliver(kind, data) {
    if (this.#closed) return;
    if (kind === KIND_TEXT && this.#mux !== null && this.#mux.packet(data)) return;
    this.#onMessage(data, kind === KIND_BINARY);
  }

  // A message that cannot be read — a malformed frame, an inflate that
  // failed or blew the cap, invalid UTF-8 underneath — is the peer's
  // protocol violation, and the adapter's to answer.
  #violation(error) {
    if (!this.#closed) this.#onViolation(error);
  }

  async #read(readable, onEnd) {
    const reader = readable.getReader();
    try {
      for (;;) {
        const wait = this.#gate === null ? null : this.#gate();
        if (wait !== null) await wait;
        // Enough inflates in flight: the next read waits for them — the
        // bytes wait in the stream, under QUIC's flow control.
        if (this.#inbound.pending >= INFLIGHT_LIMIT) await this.#inbound.idle;
        const { value, done } = await reader.read();
        if (done || this.#closed) break;
        if (this.#onActivity !== null) this.#onActivity();
        this.#parser.push(value);
      }
    } catch (error) {
      // A read error is the session's to report through `closed`; a
      // FramingError is the peer's protocol violation.
      return void this.#violation(error);
    }
    if (!this.#closed) onEnd();
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

  async #readDatagrams(readable) {
    const reader = readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done || this.#closed) break;
        if (this.#onActivity !== null) this.#onActivity();
        const text = parseDatagram(value);
        if (text !== null) this.#onMessage(text, false);
      }
    } catch {
      // The session's end, reported through `closed`.
    }
  }

  // --- outbound -------------------------------------------------------

  /**
   * Told about an outbound stream packet before it is serialized: true
   * when the packet must not go on the control stream — its stream's own
   * FIN or RESET carries it.
   */
  control(packet) {
    return !this.#closed && this.#mux !== null && this.#mux.control(packet);
  }

  /**
   * A packet as ONE datagram — unreliable, unordered, at most
   * maxDatagramSize bytes: true when it went out, false when the session
   * has no datagrams, the packet does not fit, or the channel is shut — the
   * caller's cue to send() it on the control stream instead.
   */
  sendUnreliable(data) {
    if (this.#closed || !this.#datagrams || typeof data !== 'string') return false;
    const bytes = datagramText(data);
    if (bytes.length > this.maxDatagramSize) return false;
    // The session has not taken the ones before it: this one is DROPPED,
    // not queued behind them — and answered true. False would have the
    // caller send it reliably on the control stream, which is head-of-line
    // blocking exactly when the link is congested, for a value the next
    // datagram supersedes anyway. (Not `desiredSize`: on a synchronous sink
    // it reads 0 after the first write of a tick, and the second datagram
    // of a healthy session would be dropped.)
    if (this.#maxDatagrams > 0 && this.#datagramsInFlight >= this.#maxDatagrams) {
      if (this.#datagramsDropped++ === 0 && this.#onDatagramDrop !== null) this.#onDatagramDrop();
      return true;
    }
    this.#datagramsInFlight++;
    const settled = () => void this.#datagramsInFlight--;
    this.#datagrams.write(bytes).then(settled, settled);
    return true;
  }

  /**
   * A string is a packet, bytes are a stream chunk. False above the
   * high-water mark (then onDrain), and false, quietly, once shut.
   * `options.compress === false` sends this one plain whatever was
   * negotiated.
   */
  send(data, options = null) {
    if (this.#closed || this.exceedsBackpressure()) return false;
    const active = this.#active;
    const plain = active === null || (options !== null && options.compress === false);
    if (typeof data === 'string') {
      if (plain || data.length < active.encode.threshold) return this.#enqueue(frameText(data));
      return this.#compress(KIND_TEXT, TEXT_ENCODER.encode(data));
    }
    const chunk = toBytes(data);
    // A chunk of a stream that has its own WebTransport stream goes there.
    if (this.#mux !== null && this.#mux.chunk(chunk)) return this.accepted();
    if (plain || chunk.length < active.encode.threshold) return this.#enqueue(frame(KIND_BINARY, chunk));
    return this.#compress(KIND_BINARY, chunk);
  }

  // What is queued already, against the cap — before this frame is added,
  // as the WebSocket engine counts it: one frame past the cap on an empty
  // queue is sent, a queue the peer never drains is not.
  exceedsBackpressure() {
    const max = this.#maxBackpressure;
    if (max === 0 || this.#queued <= max) return false;
    const error = new Error(`Backpressure limit exceeded (${this.#queued} > ${max} bytes), terminating session`);
    error.code = 'backpressure';
    this.#onOverflow(error);
    return true;
  }

  // The answer to a send: true under the high-water mark, false past it —
  // and with a false, the promise of an onDrain. Every path answers through
  // here: a false from the side-stream path, a compress in flight or a
  // frame queued behind one used to set no mark, so a caller waiting for
  // 'drain' after it waited forever.
  accepted() {
    if (this.#queued <= this.#highWater) return true;
    this.#pressured = true;
    return false;
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
    return this.accepted();
  }

  // Compresses one message past the threshold. The codec may answer at
  // once (zlib) or later (CompressionStream); either way the frame goes
  // out under the compressed kind when it is smaller, plain when it is not
  // or the codec failed — the message is never lost to compression.
  #compress(kind, bytes) {
    const size = bytes.length;
    this.#queued += size;
    const compressed = kind === KIND_TEXT ? KIND_TEXT_COMPRESSED : KIND_BINARY_COMPRESSED;
    const plain = () => {
      this.#queued -= size;
      this.#writeFrame(frame(kind, bytes));
    };
    // A codec that failed — at once, or later — is said (the session's
    // owner counts it and logs it once a codec); the frame leaves plain.
    const failed = (error) => {
      if (this.#onCodecError !== null) this.#onCodecError(this.#active.encode.id, error);
      plain();
    };
    let encoded;
    try {
      encoded = this.#active.encode.codec.encode(bytes);
    } catch (error) {
      failed(error);
      return this.accepted();
    }
    this.#outbound.push(
      encoded,
      (out) => {
        if (out.length >= size) return void plain();
        this.#queued -= size;
        this.#writeFrame(frame(compressed, out));
      },
      failed,
    );
    return this.accepted();
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
    return this.accepted();
  }

  // A write the stream took. After the shut nothing is counted: the count
  // was zeroed, and a late settlement used to take it negative.
  #sent(size) {
    if (this.#closed) return;
    this.#queued -= size;
    if (!this.#pressured || this.#queued > this.#lowWater) return;
    this.#pressured = false;
    this.#onDrain();
  }

  // --- the end --------------------------------------------------------

  /** This end is over: nothing more is delivered, sent or counted. Says nothing to the peer. */
  shut() {
    if (this.#closed) return;
    this.#closed = true;
    this.#mux?.close();
    this.#queued = 0;
    this.#pressured = false;
  }

  /**
   * The graceful end, after shut(): what send() already handed to the
   * control stream reaches the peer, then the session closes with `info`.
   * Closing a SESSION resets its streams and drops whatever they still
   * hold, so the stream is closed first — its close resolves once the
   * queued bytes were taken and the FIN sent — and the session after it,
   * `closeTimeout` at most. A message still being compressed
   * asynchronously is not waited for.
   */
  finish(info) {
    const session = this.#session;
    let timer = null;
    const end = () => {
      clearTimeout(timer);
      closeQuietly(session, info);
    };
    timer = setTimeout(end, this.#closeTimeout);
    timer.unref?.();
    this.#writer.close().then(end, end);
  }

  /**
   * The peer ended the control stream — its graceful close: the session
   * close that follows carries its code, and is given a moment to arrive
   * before this end closes the session itself, with `info`.
   */
  expectClose(info) {
    const session = this.#session;
    const timer = setTimeout(() => closeQuietly(session, info), CLOSE_GRACE);
    timer.unref?.();
    const settled = () => clearTimeout(timer);
    session.closed.then(settled, settled);
  }
}

module.exports = {
  WtChannel,
  closeQuietly,
  normalizeBackpressure,
  DEFAULT_HIGH_WATER_MARK,
  DEFAULT_LOW_WATER_MARK,
  DEFAULT_MAX_BACKPRESSURE,
  DEFAULT_CLOSE_TIMEOUT,
};
