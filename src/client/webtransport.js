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
const { normalizeCompression } = require('../compression/index.js');
const { WtChannel, closeQuietly, normalizeBackpressure } = require('../webtransport/channel.js');

// The `WebTransportOptions` handed to the constructor as-is.
const INIT_KEYS = ['serverCertificateHashes', 'congestionControl', 'allowPooling', 'requireUnreliable', 'protocols'];

const UNAVAILABLE =
  'WebTransport is not available here: pass options.wt.WebTransport (an implementation), ' +
  "or name a fallback — transport: ['wt', 'ws']";

// The channel itself — framing, the capabilities exchange, compression, the
// stream mux, the datagrams, flow control — is src/webtransport/channel.js,
// shared with the server's socket. What is here is what makes it a client
// transport: the open lifecycle, one channel per session, the wire codec
// and the session-encryption seam.
class ClientWtTransport extends ClientTransport {
  // Carries `options.encryption`: the handshake runs over the control
  // stream inside open(), and every message after it is sealed.
  static encrypts = true;

  // A session can die silently exactly like a socket (a NAT rebinding, a
  // suspended tab): the client's app-level ping/pong is the detector, and
  // the core owns every timer of it.
  heartbeat = true;
  persistent = true;

  #options;
  #session = null;
  #channel = null;
  // Session encryption (`options.encryption`): `{ ready, send, receive }`
  // for this session, null otherwise. Under it everything rides the control
  // stream sealed — no per-stream transport, no datagrams, no compression
  // of what is already ciphertext: each would be a way around the channel.
  #secure = null;

  /** The session's facts once established (see WrpcClient#encryption), or null. */
  encryption = null;

  #opening = null;
  // Bumped by terminate(): an open() that was waiting on the handshake when
  // the core gave up must not come back to life on top of a later attempt.
  #attempt = 0;

  /**
   * `options` are the same `wt` options connect() takes, for a transport
   * constructed by hand; the ones handed to open() win per open.
   */
  constructor(url, options = {}) {
    super(url);
    this.#options = options;
    // Said where the transport is built; the `wt` bag of a later open() is
    // checked by the channel it opens.
    normalizeBackpressure(options.maxBackpressure, 'wt transport: options');
  }

  /** The WebTransport session spoken on; null before open() and after close. */
  get session() {
    return this.#session;
  }

  /** Bytes handed to the session and not yet taken by it; 0 between sessions. */
  get bufferedAmount() {
    return this.#channel === null ? 0 : this.#channel.bufferedAmount;
  }

  /**
   * The largest datagram the session carries, 0 when it carries none —
   * what the core checks before sending an event unreliably.
   */
  get maxDatagramSize() {
    return this.#channel === null ? 0 : this.#channel.maxDatagramSize;
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
    const encryption = options.encryption ?? null;
    // Per-message compression (src/compression): connect()'s `compression`,
    // else the `wt` bag's — and none under encryption.
    const compression =
      encryption === null ? normalizeCompression(options.compression ?? wt.compression, 'wt transport: options') : null;
    const attempt = ++this.#attempt;
    const init = {};
    for (let i = 0; i < INIT_KEYS.length; i++) {
      const key = INIT_KEYS[i];
      if (wt[key] !== undefined) init[key] = wt[key];
    }
    // Announced in the URL, as on ws: the server may be the first to send.
    // Before the declared bags, so it is counted in the one query budget.
    let target = this.url;
    if (encryption !== null) target += `${target.includes('?') ? '&' : '?'}${encryption.param}=1`;
    target = connectUrl(target, options.headers, options.meta, this.log);
    const session = new WebTransport(target, init);
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
      await this.#attach(session, stream, wt, compression, encryption);
      if (attempt !== this.#attempt) throw new Error('Connection terminated');
    } catch (error) {
      if (this.#session === session) {
        this.#session = null;
        closeQuietly(session);
      }
      throw error;
    }
  }

  #attach(session, stream, wt, compression, encryption) {
    // Binary streams on their own WebTransport streams, and datagrams: not
    // under a wire codec (the mux reads stream packets off the wire, which
    // only JSON allows), and neither under encryption.
    const plain = encryption === null;
    // Frames go only to a server whose capabilities said it reads them —
    // and from a client that reads them itself: revision 1 until then.
    const frames = this.attachments !== false && !this.codec;
    this.revision = 1;
    const channel = new WtChannel(
      session,
      stream,
      {
        mux: plain && !this.codec,
        datagrams: plain,
        highWaterMark: wt.highWaterMark,
        lowWaterMark: wt.lowWaterMark,
        maxBackpressure: wt.maxBackpressure,
        maxMessage: wt.maxMessage,
        closeTimeout: wt.closeTimeout,
        compression,
        frames,
        onFrames: (reads) => {
          if (this.#channel === channel) this.revision = reads && frames ? 2 : 1;
        },
      },
      (data) => {
        // Sealed: every message is the channel's to open, from the first
        // byte — the handshake answer arrives before this transport is
        // active.
        if (this.#secure !== null) return void this.#secure.receive(data);
        if (this.active && this.#channel === channel) this.emit('message', data);
      },
      () => void this.emit('drain').catch((error) => this.#escalate(error)),
      // A message the server sent that cannot be read is its protocol
      // violation: reported, and the session hung up.
      (error) => {
        this.#escalate(error);
        this.#down(session);
        closeQuietly(session);
      },
      // Past maxBackpressure: a server that never drains.
      (error) => {
        this.#escalate(error);
        this.terminate();
      },
      (error) => this.#escalate(error),
      // The server ended the control stream — its graceful close: the
      // session close follows, and is given a moment before this end hangs
      // up itself.
      () => {
        this.#down(session);
        channel.expectClose();
      },
    );
    this.#channel = channel;
    const established = () => {
      this.active = true;
      // Announced before open() resolves — the core's 'open' handler runs
      // synchronously here, which is the invariant every transport keeps.
      this.emit('open');
    };
    if (plain) return void established();
    // The handshake, over the control stream, before anything else of this
    // session: a failure hangs the session up, which is open()'s rejection.
    const secure = encryption.secure({
      kind: 'wt',
      write: (bytes) => void channel.send(bytes),
      deliver: (data) => {
        if (this.#session === session) this.emit('message', data);
      },
      fail: (error) => {
        this.log?.warn({ event: 'encryption.failed', err: error });
        closeQuietly(session);
      },
    });
    this.#secure = secure;
    return secure.ready.then((info) => {
      if (this.#session !== session) throw new Error('Connection terminated');
      this.encryption = info;
      established();
    });
  }

  /** A stream packet is seen before serialization: the mux may take it. */
  send(obj) {
    if (this.#channel !== null && this.#channel.control(obj)) return;
    super.send(obj);
  }

  /** The codec ids in effect — `{ encode, decode }` — or null while the two lists share none. */
  get compression() {
    return this.#channel === null ? null : this.#channel.compression;
  }

  /**
   * Sends a packet as ONE datagram — unreliable, unordered, at most
   * maxDatagramSize bytes — and answers true when it went out; false when
   * the session has no datagrams or the packet does not fit, which is the
   * caller's cue to send it on the control stream instead. Never throws
   * for a lost datagram: losing one is the point.
   */
  writeUnreliable(data) {
    return this.active && this.#channel.sendUnreliable(data);
  }

  /**
   * A string is a packet, bytes are a stream chunk. Throws when not
   * connected (the core turns that into a coded 503); answers false above
   * the high-water mark, after which 'drain' follows.
   */
  write(data, options = null) {
    if (!this.active) throw new Error('Not connected');
    const channel = this.#channel;
    if (this.#secure === null) return channel.send(data, options);
    if (channel.exceedsBackpressure()) return false;
    this.#secure.send(data);
    return channel.accepted();
  }

  /**
   * A graceful goodbye: what was written reaches the server, then its
   * `closed` settles with the close info — the control stream is closed
   * first and the session after it, `closeTimeout` at most (channel.js).
   */
  close() {
    const session = this.#session;
    if (!session) return;
    const channel = this.#channel;
    this.#down(session);
    const info = { closeCode: 0, reason: '' };
    // Still opening: there is no stream to drain.
    if (channel === null) closeQuietly(session, info);
    else channel.finish(info);
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
    this.#channel?.shut();
    this.#channel = null;
    // A handshake still running is over with the session: open() rejects
    // now, not at the handshake timeout.
    this.#secure?.cancel(error);
    this.#secure = null;
    this.encryption = null;
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

module.exports = { ClientWtTransport, UNAVAILABLE };
