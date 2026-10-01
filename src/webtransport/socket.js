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
//
// The channel itself — framing, the capabilities exchange, compression, the
// stream mux, the datagrams, the byte accounting — is src/webtransport/
// channel.js, shared with the client transport. What is here is what makes
// it a server socket: the events, pause(), the idle timer, the log, the
// close codes.
const { EventEmitter } = require('node:events');
const { clip } = require('../rpc/errors.js');
const { normalizeCompression } = require('../compression/index.js');
const {
  WtChannel,
  closeQuietly,
  normalizeBackpressure,
  DEFAULT_HIGH_WATER_MARK,
  DEFAULT_LOW_WATER_MARK,
  DEFAULT_MAX_BACKPRESSURE,
  DEFAULT_CLOSE_TIMEOUT,
} = require('./channel.js');

// Datagrams handed to the session and not yet taken by it. On the hosts
// wrpc is run against the sink is synchronous and this never passes one;
// the cap is for a host whose sink holds its promise — a W3C-shaped one
// under congestion — where an unbounded queue of positions nobody wants
// any more is the opposite of what a datagram is for.
const MAX_DATAGRAMS_IN_FLIGHT = 64;

class WtSocket extends EventEmitter {
  // What attachSocket reads into the client's meta; a WebTransport session
  // negotiates no subprotocol, so `protocol` stays ''.
  remoteAddress;
  protocol = '';

  #session;
  #stream;
  #channel;
  #closed = false;
  #paused = false;
  // While paused: the promise every read — the control stream's and the
  // mux's side streams' — waits on, and what resume() settles it with.
  #gate = null;
  #release = null;
  #log;
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
  constructor(
    session,
    stream,
    {
      remoteAddress = '',
      highWaterMark,
      lowWaterMark,
      maxBackpressure,
      maxMessage,
      idleTimeout = 0,
      closeTimeout = DEFAULT_CLOSE_TIMEOUT,
      compression = null,
      maxHeldStreams,
      holdTimeout,
      // The host's writer, already bound to the peer (attachSession): what
      // a violation, an idle, a failed session and a close write to. Null
      // for a socket used bare.
      log = null,
      // `(codec id, error)`: a codec that failed on the way out — the frame
      // left plain. attachSession hands it to the server's reporter.
      onCodecError = null,
    } = {},
  ) {
    super();
    this.#session = session;
    this.#stream = stream;
    this.#log = log;
    this.remoteAddress = remoteAddress;
    if (!Number.isInteger(closeTimeout) || closeTimeout < 0) {
      throw new TypeError('WtSocket: options.closeTimeout must be a non-negative integer of milliseconds');
    }
    this.#idle = idleTimeout;
    this.#touch();
    this.#channel = new WtChannel(
      session,
      stream,
      {
        highWaterMark,
        lowWaterMark,
        maxBackpressure: normalizeBackpressure(maxBackpressure, 'WtSocket: options'),
        maxMessage,
        closeTimeout,
        compression: normalizeCompression(compression, 'WtSocket: options'),
        maxDatagramsInFlight: MAX_DATAGRAMS_IN_FLIGHT,
        mux: {
          // An inbound stream cancelled unread (streams.js): announced for
          // the host to log — a peer opening streams it never names is a
          // signal.
          onRefused: (reason, id) => void this.emit('stream-refused', { reason, id }),
          onFallback: () => void this.#log?.info({ event: 'wt.mux.fallback' }),
          ...(maxHeldStreams === undefined ? null : { maxHeldStreams }),
          ...(holdTimeout === undefined ? null : { holdTimeout }),
        },
        // pause() stops the side streams too, and their bytes are liveness
        // as much as the control stream's — an upload used to keep flowing
        // around a pause, and a session busy with one used to idle out.
        gate: () => this.#gate,
        onActivity: () => this.#touch(),
        onCodecError,
        onDatagramDrop: () => void this.#log?.warn({ event: 'wt.datagram.dropped' }),
      },
      (data, isBinary) => this.emit('message', data, isBinary),
      () => this.emit('drain'),
      // A message the peer sent that cannot be read is its protocol
      // violation: the 1002 a malformed frame gets, and its line.
      (error) => {
        this.#fault('wt.violation', error, { code: typeof error?.code === 'string' ? error.code : null });
        this.close(1002, 'Protocol error');
      },
      // Past maxBackpressure: a peer that never drains.
      (error) => {
        this.#error(error);
        this.terminate();
      },
      (error) => this.#error(error),
      // The peer ended the control stream: the connection is over. Its
      // session close follows with its code; 1000 when it does not.
      () => this.#channel.expectClose({ closeCode: 1000, reason: '' }),
    );
    // The session's own end — the peer closed, the transport failed — is a
    // close here; our own close() settles it too, by then a no-op.
    session.closed.then(
      (info) => this.#down(info?.closeCode ?? 1006, info?.reason ?? ''),
      (error) => {
        this.#fault('wt.session.error', error);
        this.#down(1006, '');
      },
    );
  }

  get session() {
    return this.#session;
  }

  get stream() {
    return this.#stream;
  }

  /** Bytes handed to the stream and not yet taken by it; 0 once closed. */
  get bufferedAmount() {
    return this.#channel.bufferedAmount;
  }

  /** The largest datagram the session carries; 0 when it carries none. */
  get maxDatagramSize() {
    return this.#channel.maxDatagramSize;
  }

  get isPaused() {
    return this.#paused;
  }

  /**
   * The codec ids in effect — `{ encode, decode }`, what this side sends
   * with and what the peer does — or null while the two lists share none.
   */
  get compression() {
    return this.#channel.compression;
  }

  /** Datagrams dropped because the session had not taken the ones before them. */
  get droppedDatagrams() {
    return this.#channel.droppedDatagrams;
  }

  // The mirror of Connection.#fault: one line, then the 'error' event a
  // bound socket has a listener for.
  #fault(event, error, extra = null) {
    this.#log?.warn({ ...extra, err: error, event });
    this.#error(error);
  }

  /**
   * Told about an outbound stream packet before it is serialized (by
   * ServerWtTransport): true when the packet must not go on the control
   * stream — its stream's own FIN or RESET carries it.
   */
  streamControl(packet) {
    return this.#channel.control(packet);
  }

  /**
   * A packet as ONE datagram — unreliable, unordered, at most
   * maxDatagramSize bytes: true when it went out, false when the session
   * has no datagrams, the packet does not fit, or the socket is closed —
   * the caller's cue to send() it on the control stream instead. A datagram
   * the session has not kept up with is dropped, and answered true.
   */
  sendUnreliable(data) {
    return this.#channel.sendUnreliable(data);
  }

  /**
   * A string is a packet, bytes are a stream chunk. False above the
   * high-water mark (then 'drain'), and false, quietly, once closed.
   * `options.compress === false` (the per-message opt-out Client.sendRaw
   * passes through writeWith) sends this one plain whatever was negotiated.
   */
  send(data, options = null) {
    return this.#channel.send(data, options);
  }

  /** Stops pulling the control stream and the side streams; QUIC flow control does the rest. */
  pause() {
    if (this.#paused || this.#closed) return;
    this.#paused = true;
    this.#gate = new Promise((resolve) => {
      this.#release = resolve;
    });
  }

  resume() {
    if (!this.#paused) return;
    this.#paused = false;
    const release = this.#release;
    this.#gate = null;
    this.#release = null;
    if (release) release();
  }

  /**
   * Graceful: what send() already handed to the control stream reaches the
   * peer, and the peer's `closed` carries the code and reason — the stream
   * is closed first and the session after it, at most `closeTimeout` later
   * (channel.js). This side is closed synchronously either way: 'close'
   * fires here, nothing more is accepted.
   */
  close(code = 1000, reason = '') {
    if (this.#closed) return;
    this.#down(code, reason);
    this.#channel.finish({ closeCode: code, reason });
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
      this.#fault('wt.idle', new Error(`No data for ${this.#idle} ms`), { idle: this.#idle });
      this.terminate();
    }, this.#idle);
    this.#idleTimer.unref?.();
  }

  #down(code, reason) {
    if (this.#closed) return;
    this.#closed = true;
    // The peer's reason is its text: clipped, as a field, at debug — a
    // routine end is not an alert, but a code an operator can grep for.
    const dropped = this.#channel.droppedDatagrams;
    this.#log?.debug({ event: 'wt.close', code, reason: clip(reason), ...(dropped > 0 ? { dropped } : null) });
    clearTimeout(this.#idleTimer);
    this.#idleTimer = null;
    this.#channel.shut();
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

module.exports = {
  WtSocket,
  normalizeBackpressure,
  DEFAULT_HIGH_WATER_MARK,
  DEFAULT_LOW_WATER_MARK,
  DEFAULT_MAX_BACKPRESSURE,
  DEFAULT_CLOSE_TIMEOUT,
};
