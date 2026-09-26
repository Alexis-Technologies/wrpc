'use strict';

// Binary streams on their own WebTransport streams — the one place the
// wire departs from "everything on the control stream", and the reason to:
// a 64 MiB upload's chunks on the control stream sit in front of every
// callback and event behind them, and QUIC's independent streams are
// exactly what removes that head-of-line wait. The mapping is symmetric —
// one StreamMux per end, the client transport's and the server socket's.
//
// Negotiated, never assumed: each end's first message on the control
// stream is a KIND 2 capabilities message (`{"streams":true}`); a peer that
// never sends one gets every chunk on the control stream, as revision 1
// reads it. Then, per wrpc binary stream (a `stream` packet with an id):
//
//   sender    the OPEN packet stays on the control stream (it carries name
//             and size, and its order against the call that names the id
//             matters); the chunks go on ONE unidirectional WebTransport
//             stream whose first bytes are the chunk header — [idLen][id]
//             — once, then raw payload; FIN is the end, a RESET (abort) the
//             termination, and the `end`/`terminate` packets are NOT sent.
//   receiver  a QUIC stream is ordered only against itself, so chunks may
//             arrive before the open packet and FIN before the last read
//             of another stream: chunks are held until the open packet has
//             passed, and the `end`/`terminate` packet is synthesized only
//             once the unidirectional stream has ended — after every chunk.
//
// Packet inspection is the cost: the receiver looks at the first bytes of
// every inbound packet for a `stream` packet (a prefix test, never a parse
// of anything else), and the sender is told the object before it is
// serialized. Under an injected wire codec neither is possible, and the
// capability is not offered. Browser-budgeted: manual loops, no spread on
// the hot path.

const { chunkEncode } = require('../chunks.js');

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();
// What JSON.stringify makes of every wrpc stream packet's first key.
const STREAM_PREFIX = '{"type":"stream"';
const CAPS_STREAMS = '{"streams":true}';
// An inbound stream whose open packet has not passed is HELD: read no
// further than the read that carried its header (the rest waits in the
// peer's stream, under QUIC's own per-stream flow control), for at most
// `holdTimeout`, and at most `maxHeldStreams` of them at once — a peer
// used to be able to open streams for ids it never named and have every
// byte of them buffered here, before any authentication, past maxMessage.
const DEFAULT_MAX_HELD_STREAMS = 32;
const DEFAULT_HOLD_TIMEOUT = 10_000;
// How long createUnidirectionalStream() may take before the stream — and
// every one after it — goes on the control stream instead. A browser parks
// the promise until the peer grants stream credit, forever against a host
// that never does; the chunks held for it were the only bound.
const DEFAULT_OPEN_TIMEOUT = 5_000;

// The chunk header a unidirectional stream opens with — chunkEncode's,
// without the payload.
const idHeader = (id) => {
  const bytes = TEXT_ENCODER.encode(id);
  if (bytes.length > 255) throw new Error(`ID length ${bytes.length} exceeds maximum of 255 characters`);
  const out = new Uint8Array(1 + bytes.length);
  out[0] = bytes.length;
  out.set(bytes, 1);
  return out;
};

// The id a chunkEncode frame names, and where its payload starts.
const readId = (frame) => {
  const idLength = frame[0];
  return { id: TEXT_DECODER.decode(frame.subarray(1, 1 + idLength)), offset: 1 + idLength };
};

class StreamMux {
  #session;
  #emitPacket;
  #emitChunk;
  #onQueued;
  #onSent;
  #writeControl;
  #sendControl;
  #onRefused;
  #maxHeld;
  #holdTimeout;
  #openTimeout;
  #enabled = false;
  // Whether the peer announced streams — what makes an inbound
  // unidirectional stream one of ours to read. Kept apart from #enabled,
  // which the fallback in #open switches off for OUR sending only.
  #peerStreams = false;
  #closed = false;
  // Inbound streams held for their open packet, against #maxHeld.
  #held = 0;
  // Outbound: id -> { writer, chain, pending, ended } — pending holds the
  // chunk frames written while the stream is still opening, ended the
  // status that arrived meanwhile.
  #out = new Map();
  // Inbound: id -> { opened, queue, ended, stream, release, released,
  // timer, refused, cancel } (see #entry).
  #in = new Map();

  /**
   * `emitPacket(text)` and `emitChunk(frame)` deliver inbound messages in
   * the transport's own way; `onQueued(n)`/`onSent(n)` keep its outbound
   * byte accounting honest for chunks that left through a side stream;
   * `writeControl(frame)` and `sendControl(packet)` put a chunk frame or a
   * stream packet on the control stream — where a stream's chunks go when
   * its own WebTransport stream could not be opened (a host that grants no
   * unidirectional streams; quico 0.4 does not). `onRefused(reason, id)`
   * hears of an inbound stream cancelled unread: `'unannounced'` (the peer
   * announced no streams), `'id'` (an empty one), `'duplicate'` (a second
   * stream for an id), `'held'` (past maxHeldStreams), `'timeout'` (its
   * open packet never came within holdTimeout). `openTimeout` bounds a
   * createUnidirectionalStream() that never settles.
   */
  constructor(
    session,
    {
      emitPacket,
      emitChunk,
      onQueued,
      onSent,
      writeControl = null,
      sendControl = null,
      onRefused = null,
      maxHeldStreams = DEFAULT_MAX_HELD_STREAMS,
      holdTimeout = DEFAULT_HOLD_TIMEOUT,
      openTimeout = DEFAULT_OPEN_TIMEOUT,
    },
  ) {
    if (!Number.isInteger(maxHeldStreams) || maxHeldStreams <= 0) {
      throw new TypeError('StreamMux: maxHeldStreams must be a positive integer');
    }
    if (!Number.isInteger(holdTimeout) || holdTimeout <= 0) {
      throw new TypeError('StreamMux: holdTimeout must be a positive integer (ms)');
    }
    if (!Number.isInteger(openTimeout) || openTimeout <= 0) {
      throw new TypeError('StreamMux: openTimeout must be a positive integer (ms)');
    }
    this.#session = session;
    this.#emitPacket = emitPacket;
    this.#emitChunk = emitChunk;
    this.#onQueued = onQueued;
    this.#onSent = onSent;
    this.#writeControl = writeControl;
    this.#sendControl = sendControl;
    this.#onRefused = onRefused;
    this.#maxHeld = maxHeldStreams;
    this.#holdTimeout = holdTimeout;
    this.#openTimeout = openTimeout;
  }

  /** What we announce: streams, when the session can open unidirectional ones. */
  static caps(session) {
    return typeof session?.createUnidirectionalStream === 'function' ? CAPS_STREAMS : '{}';
  }

  /** Whether the peer takes chunks on unidirectional streams. */
  get enabled() {
    return this.#enabled;
  }

  /** The peer's capabilities message (KIND 2). */
  peerCaps(text) {
    let caps = null;
    try {
      caps = JSON.parse(text);
    } catch {
      return;
    }
    this.#peerStreams = Boolean(caps?.streams);
    this.#enabled =
      this.#peerStreams &&
      typeof this.#session.createUnidirectionalStream === 'function' &&
      typeof this.#session.incomingUnidirectionalStreams?.getReader === 'function';
  }

  // --- outbound -------------------------------------------------------

  /**
   * Told about every outbound packet BEFORE it is serialized. Answers true
   * when the packet must NOT go on the control stream — an `end` or
   * `terminate` of a stream that went out on its own WebTransport stream,
   * which its FIN or RESET carries instead.
   */
  control(packet) {
    if (!this.#enabled || packet.type !== 'stream' || typeof packet.id !== 'string') return false;
    const { id, status } = packet;
    if (status === undefined) {
      if (!this.#out.has(id)) this.#open(id);
      return false;
    }
    const entry = this.#out.get(id);
    if (!entry) return false;
    if (entry.writer === null) {
      // Still opening: the status waits with the chunks.
      entry.ended = status;
      return true;
    }
    this.#out.delete(id);
    this.#finishOut(entry, status);
    return true;
  }

  #finishOut(entry, status) {
    if (status === 'end') {
      entry.chain = entry.chain.then(() => entry.writer.close()).catch(() => {});
    } else {
      entry.chain.then(() => entry.writer.abort()).catch(() => {});
    }
  }

  #open(id) {
    const entry = { writer: null, chain: Promise.resolve(), pending: [], ended: null };
    this.#out.set(id, entry);
    let expired = false;
    const timer = setTimeout(() => {
      expired = true;
      this.#fallback(id, entry);
    }, this.#openTimeout);
    timer.unref?.();
    this.#session.createUnidirectionalStream().then(
      (writable) => {
        clearTimeout(timer);
        if (expired || this.#closed || this.#out.get(id) !== entry) {
          // Granted too late, or to a stream that is gone: reset unused.
          const abort = writable.abort();
          if (abort && typeof abort.catch === 'function') abort.catch(() => {});
          return;
        }
        entry.writer = writable.getWriter();
        entry.chain = entry.writer.write(idHeader(id)).catch(() => {});
        const pending = entry.pending;
        entry.pending = null;
        // Counted when held (chunk()): routed without counting again.
        for (let i = 0; i < pending.length; i++) this.#route(entry, pending[i], true);
        if (entry.ended) {
          this.#out.delete(id);
          this.#finishOut(entry, entry.ended);
        }
      },
      () => {
        clearTimeout(timer);
        if (!expired) this.#fallback(id, entry);
      },
    );
  }

  // No stream to be had (the host grants none, or not within openTimeout):
  // this stream, and every one after it, goes on the control stream — what
  // was held for it is replayed there in order, its end packet last.
  #fallback(id, entry) {
    if (this.#closed || this.#out.get(id) !== entry) return;
    this.#enabled = false;
    this.#out.delete(id);
    const pending = entry.pending;
    entry.pending = null;
    for (let i = 0; i < pending.length; i++) {
      const frame = pending[i];
      // Uncounted here; the control stream counts it as its own.
      this.#onQueued(readId(frame).offset - frame.length);
      if (this.#writeControl) this.#writeControl(frame);
    }
    if (entry.ended && this.#sendControl) this.#sendControl({ type: 'stream', id, status: entry.ended });
  }

  #route(entry, frame, counted) {
    const { offset } = readId(frame);
    const payload = frame.subarray(offset);
    const size = payload.length;
    if (!counted) this.#onQueued(size);
    entry.chain = entry.chain
      .then(() => entry.writer.write(payload))
      .then(
        () => this.#onSent(size),
        () => this.#onSent(size),
      );
  }

  /**
   * An outbound chunk frame (chunkEncode). Answers true when it was routed
   * to the stream's own WebTransport stream, false when it belongs on the
   * control stream (no side stream for this id).
   */
  chunk(frame) {
    if (this.#out.size === 0) return false;
    const { id, offset } = readId(frame);
    const entry = this.#out.get(id);
    if (!entry) return false;
    if (entry.writer === null) {
      // Held for the stream to open, and counted against the transport's
      // marks from now: what waits for the open is as buffered as what
      // waits in a writer — it used to be invisible to bufferedAmount.
      entry.pending.push(frame);
      this.#onQueued(frame.length - offset);
    } else this.#route(entry, frame, false);
    return true;
  }

  // --- inbound --------------------------------------------------------

  /**
   * Every inbound packet passes here first. Answers true when the mux
   * delivered it (or will, in order) — an open packet that releases held
   * chunks — and false when the caller should deliver it as usual.
   */
  packet(text) {
    if (!text.startsWith(STREAM_PREFIX)) return false;
    let packet = null;
    try {
      packet = JSON.parse(text);
    } catch {
      return false;
    }
    if (typeof packet?.id !== 'string') return false;
    const { id, status } = packet;
    if (status !== undefined) {
      // An end or terminate on the control stream: the peer sent this
      // stream's chunks there too — nothing of ours is pending.
      this.#in.delete(id);
      return false;
    }
    const entry = this.#in.get(id);
    if (!entry) {
      this.#in.set(id, this.#entry(true));
      return false;
    }
    entry.opened = true;
    this.#emitPacket(text);
    const queue = entry.queue;
    for (let i = 0; i < queue.length; i++) this.#emitChunk(chunkEncode(id, queue[i]));
    queue.length = 0;
    this.#unhold(entry);
    if (entry.ended) this.#finish(id, entry.ended);
    return true;
  }

  // An inbound stream's record. `stream`: a unidirectional stream claimed
  // the id; `release`/`released`: the hold on a stream read ahead of its
  // open packet; `timer`: the hold's deadline; `refused`: cancelled unread;
  // `cancel`: how (the reader's STOP_SENDING).
  #entry(opened) {
    return {
      opened,
      queue: [],
      ended: null,
      stream: false,
      release: null,
      released: null,
      timer: null,
      refused: false,
      cancel: null,
    };
  }

  /**
   * An incoming unidirectional stream: reads it to the end — once its open
   * packet has passed; only what the first read carried is held before.
   */
  accept(readable) {
    // A peer that announced no streams sends nothing on one: cancelled
    // unread, never held.
    if (!this.#peerStreams) {
      try {
        const cancelled = readable.cancel();
        if (cancelled && typeof cancelled.catch === 'function') cancelled.catch(() => {});
      } catch {
        // Not cancellable: nothing is read from it either way.
      }
      this.#onRefused?.('unannounced', null);
      return;
    }
    void this.#consume(readable);
  }

  async #consume(readable) {
    const reader = readable.getReader();
    let id = null;
    let header = null;
    let entry = null;
    let refused = null;
    try {
      for (;;) {
        // Held for its open packet: only what the read that carried the
        // header brought is queued; the rest waits in the peer's stream,
        // under QUIC's own per-stream flow control, never in this process.
        if (entry !== null && !entry.opened) await entry.released;
        if (entry !== null && entry.refused) return;
        const { value, done } = await reader.read();
        if (done) break;
        if (this.#closed) return;
        let bytes = value;
        if (id === null) {
          header = header === null ? bytes : concat(header, bytes);
          const need = 1 + header[0];
          if (header.length < need) continue;
          id = TEXT_DECODER.decode(header.subarray(1, need));
          bytes = header.subarray(need);
          header = null;
          const claimed = this.#claim(id);
          if (typeof claimed === 'string') {
            refused = claimed;
            break;
          }
          entry = claimed;
          entry.cancel = () => reader.cancel().catch(() => {});
          if (bytes.length === 0) continue;
        }
        this.#inbound(id, entry, bytes);
      }
      if (refused !== null) {
        await reader.cancel().catch(() => {});
        this.#onRefused?.(refused, id);
        return;
      }
      if (id !== null && !entry.refused) this.#ended(id, 'end');
    } catch {
      // A RESET from the peer: the stream was terminated. (A hold that
      // expired cancelled the reader itself and has said so already.)
      if (id !== null && entry !== null && !entry.refused) this.#ended(id, 'terminate');
    }
  }

  // The record for a unidirectional stream naming `id`, or why it is
  // refused: an empty id, a second stream for an id (the first is the
  // stream), more streams held for their open packet than the cap.
  #claim(id) {
    if (id.length === 0) return 'id';
    const existing = this.#in.get(id);
    if (existing) {
      if (existing.stream) return 'duplicate';
      // The open packet came first: nothing to hold.
      existing.stream = true;
      return existing;
    }
    if (this.#held >= this.#maxHeld) return 'held';
    const entry = this.#entry(false);
    entry.stream = true;
    entry.released = new Promise((resolve) => {
      entry.release = resolve;
    });
    this.#held++;
    entry.timer = setTimeout(() => this.#expire(id, entry), this.#holdTimeout);
    entry.timer.unref?.();
    this.#in.set(id, entry);
    return entry;
  }

  #unhold(entry) {
    if (entry.timer !== null) {
      clearTimeout(entry.timer);
      entry.timer = null;
    }
    if (entry.release !== null) {
      this.#held--;
      const release = entry.release;
      entry.release = null;
      release();
    }
  }

  // Its open packet never came: the stream is cancelled unread — what was
  // held is dropped, and the peer sees STOP_SENDING.
  #expire(id, entry) {
    if (this.#in.get(id) !== entry || entry.opened) return;
    this.#in.delete(id);
    entry.refused = true;
    entry.queue.length = 0;
    this.#unhold(entry);
    entry.cancel?.();
    this.#onRefused?.('timeout', id);
  }

  #inbound(id, entry, bytes) {
    if (entry.opened) return void this.#emitChunk(chunkEncode(id, bytes));
    entry.queue.push(bytes);
  }

  #ended(id, status) {
    const entry = this.#in.get(id);
    if (!entry) return this.#finish(id, status);
    if (entry.opened) return this.#finish(id, status);
    entry.ended = status;
  }

  #finish(id, status) {
    this.#in.delete(id);
    this.#emitPacket(JSON.stringify({ type: 'stream', id, status }));
  }

  close() {
    this.#closed = true;
    for (const entry of this.#out.values()) {
      entry.chain.then(() => entry.writer?.abort()).catch(() => {});
      entry.pending = null;
    }
    this.#out.clear();
    for (const entry of this.#in.values()) this.#unhold(entry);
    this.#in.clear();
  }
}

const concat = (a, b) => {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
};

module.exports = {
  StreamMux,
  STREAM_PREFIX,
  CAPS_STREAMS,
  DEFAULT_MAX_HELD_STREAMS,
  DEFAULT_HOLD_TIMEOUT,
  DEFAULT_OPEN_TIMEOUT,
  idHeader,
  readId,
};
