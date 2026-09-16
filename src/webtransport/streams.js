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
  #enabled = false;
  #closed = false;
  // Outbound: id -> { writer, chain, pending, ended } — pending holds the
  // chunk frames written while the stream is still opening, ended the
  // status that arrived meanwhile.
  #out = new Map();
  // Inbound: id -> { opened, queue, ended }.
  #in = new Map();

  /**
   * `emitPacket(text)` and `emitChunk(frame)` deliver inbound messages in
   * the transport's own way; `onQueued(n)`/`onSent(n)` keep its outbound
   * byte accounting honest for chunks that left through a side stream;
   * `writeControl(frame)` and `sendControl(packet)` put a chunk frame or a
   * stream packet on the control stream — where a stream's chunks go when
   * its own WebTransport stream could not be opened (a host that grants no
   * unidirectional streams; quico 0.4 does not).
   */
  constructor(session, { emitPacket, emitChunk, onQueued, onSent, writeControl = null, sendControl = null }) {
    this.#session = session;
    this.#emitPacket = emitPacket;
    this.#emitChunk = emitChunk;
    this.#onQueued = onQueued;
    this.#onSent = onSent;
    this.#writeControl = writeControl;
    this.#sendControl = sendControl;
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
    this.#enabled =
      Boolean(caps?.streams) &&
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
    this.#session.createUnidirectionalStream().then(
      (writable) => {
        if (this.#closed || this.#out.get(id) !== entry) return;
        entry.writer = writable.getWriter();
        entry.chain = entry.writer.write(idHeader(id)).catch(() => {});
        const pending = entry.pending;
        entry.pending = null;
        for (let i = 0; i < pending.length; i++) this.#route(entry, pending[i]);
        if (entry.ended) {
          this.#out.delete(id);
          this.#finishOut(entry, entry.ended);
        }
      },
      () => {
        // No stream to be had (the host grants none): this stream, and
        // every one after it, goes on the control stream — what was held
        // for it is replayed there in order, its end packet last.
        if (this.#closed || this.#out.get(id) !== entry) return;
        this.#enabled = false;
        this.#out.delete(id);
        const pending = entry.pending;
        entry.pending = null;
        if (this.#writeControl) for (let i = 0; i < pending.length; i++) this.#writeControl(pending[i]);
        if (entry.ended && this.#sendControl) this.#sendControl({ type: 'stream', id, status: entry.ended });
      },
    );
  }

  #route(entry, frame) {
    const { offset } = readId(frame);
    const payload = frame.subarray(offset);
    const size = payload.length;
    this.#onQueued(size);
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
    const { id } = readId(frame);
    const entry = this.#out.get(id);
    if (!entry) return false;
    if (entry.writer === null) entry.pending.push(frame);
    else this.#route(entry, frame);
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
      this.#in.set(id, { opened: true, queue: [], ended: null });
      return false;
    }
    entry.opened = true;
    this.#emitPacket(text);
    const queue = entry.queue;
    for (let i = 0; i < queue.length; i++) this.#emitChunk(chunkEncode(id, queue[i]));
    queue.length = 0;
    if (entry.ended) this.#finish(id, entry.ended);
    return true;
  }

  /** An incoming unidirectional stream: reads it to the end. */
  accept(readable) {
    void this.#consume(readable);
  }

  async #consume(readable) {
    const reader = readable.getReader();
    let id = null;
    let header = null;
    try {
      for (;;) {
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
          if (bytes.length === 0) continue;
        }
        this.#inbound(id, bytes);
      }
      if (id !== null) this.#ended(id, 'end');
    } catch {
      // A RESET from the peer: the stream was terminated.
      if (id !== null) this.#ended(id, 'terminate');
    }
  }

  #inbound(id, bytes) {
    let entry = this.#in.get(id);
    if (!entry) {
      entry = { opened: false, queue: [], ended: null };
      this.#in.set(id, entry);
    }
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
    this.#in.clear();
  }
}

const concat = (a, b) => {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
};

module.exports = { StreamMux, STREAM_PREFIX, CAPS_STREAMS, idHeader, readId };
