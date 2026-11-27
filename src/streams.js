'use strict';

const { Emitter } = require('./utils.js');
const { chunkEncode } = require('./chunks.js');
const { ArgumentError, WrpcError } = require('./errors.js');

const PUSH_EVENT = Symbol('push');
const PULL_EVENT = Symbol('pull');
const DEFAULT_HIGH_WATER_MARK = 32;
const MAX_LISTENERS = 10;
const MAX_HIGH_WATER_MARK = 1000;
// What writeAll() writes at a time: what a browser's blob.stream() hands the
// Blob uploader, and under every carrier's message limit.
const CHUNK_SIZE = 64 * 1024;
// How far the head of a readable's queue runs before what is left is moved
// down to the front of its array (see pull()).
const COMPACT_AT = 1024;
// What a queued chunk costs to hold besides its bytes: the views over it and
// its slot. Measured at about 200 bytes a chunk on a server — a million
// one-byte chunks grew the heap by 201 MiB — and counted with a margin, so
// that a cap on what streams hold (heldBytes) is a cap on memory, not on
// payload: 0.95 MiB of one-byte chunks used to pass under a 1 MiB cap.
const CHUNK_COST = 256;
// How long a stream may go without a reader and without a byte before it is
// failed and forgotten (sweepStreams), on both ends, unless an option says
// otherwise.
const STREAM_IDLE_TIMEOUT = 30_000;
// What a receiver lets one connection hold of its peer's streams, on both
// ends, unless an option says otherwise: how many are open at once — each
// holds a readable, and what was sent into it, until somebody reads it, and
// the peer alone decides how many it opens — and the bytes of them that no
// reader has taken yet (overCap). 64 MiB: room for an upload that runs ahead
// of the call that reads it, and the same order as what the WebSocket engine
// already lets one connection buffer for a message (`maxBuffer`, 100 MiB).
const MAX_STREAMS = 256;
const MAX_STREAM_BUFFER = 64 * 1024 * 1024;

// The writables waiting to hear that their transport closed, by transport,
// behind ONE 'close' listener on it. Each writable used to add its own: the
// eleventh upload in flight on a client printed a MaxListenersExceededWarning
// (a client is an Emitter of ten), and every listener added was compared with
// all the others first. The listener is there while a writable waits and no
// longer: a transport outlives its streams — a client's for every reconnect —
// and is left as it was found.
const closing = new WeakMap();

// A stream's name as a message may carry it. The name is the SENDER's: a
// megabyte of it, with a line break in the middle, was a megabyte in the
// receiver's error log with a line of the sender's own writing in it. A
// message takes sixty-four characters of it, and nothing that starts a line.
const shown = (name) => {
  const text = `${name}`;
  return (text.length > 64 ? `${text.slice(0, 64)}…` : text).replace(/\p{Cc}|\u2028|\u2029/gu, ' ');
};

const noop = () => {};

// What a stream announces to whoever watches it ('end', 'close', 'drain'): a
// listener that throws is told to the stream's 'error' listeners when there
// are any — a stream has no logger — and never fails the stream it watched,
// nor becomes an unhandled rejection of it.
const announce = (stream, name) => {
  stream.emit(name).catch((error) => {
    if (stream.listenerCount('error') > 0) stream.emit('error', error).catch(noop);
  });
};
const awaitClose = (transport, shut) => {
  let hub = closing.get(transport);
  if (hub === undefined) {
    const waiting = new Set();
    const listener = () => {
      closing.delete(transport);
      transport.off?.('close', listener);
      // A copy: each of them lets go of its own entry as it shuts.
      const shuts = Array.from(waiting);
      for (let i = 0; i < shuts.length; i++) shuts[i]();
    };
    hub = { waiting, listener };
    closing.set(transport, hub);
    transport.on('close', listener);
  }
  hub.waiting.add(shut);
  return hub;
};
// A writable that finished is not waiting any more; the last one out takes
// the listener with it.
const forgetClose = (transport, hub, shut) => {
  if (!hub.waiting.delete(shut) || hub.waiting.size > 0 || closing.get(transport) !== hub) return;
  closing.delete(transport);
  transport.off?.('close', hub.listener);
};

// What a set of readables holds for readers that have not taken it: the
// bytes sent into them and not read yet, and what each queued chunk costs
// to keep (CHUNK_COST).
//
// `paced` says that the carrier can hold its peer back — a socket that is
// paused while a chunk waits for room. A stream that has a reader is then
// bounded by its high-water mark and is not counted: it is paced, not held.
// Where the carrier cannot (uWebSockets.js, a WebRTC channel, a broker
// session, a browser's WebSocket) whatever the peer sends arrives, reader or
// not, so what a stream's reader is behind by counts like an unread
// stream's — it used to be bounded by nothing but a thousand chunks.
//
// Summed when asked, over a connection's open streams (`maxStreams` of them
// at most) and only for a chunk of a stream whose reader is not keeping up —
// a count kept in step would need every readable to report its reads and
// its end back, for a sum that is one addition per open stream. One function
// for whoever feeds readables and caps what they hold: the server's
// dispatcher, a client.
const heldBytes = (streams, paced) => {
  let bytes = 0;
  for (const stream of streams.values()) {
    if (!stream.streaming || (paced && stream.consuming)) continue;
    bytes += stream.bytesReceived - stream.bytesRead + stream.queued * CHUNK_COST;
  }
  return bytes;
};

// Why `stream` may not take one more chunk of `length` bytes under `cap` —
// the message it fails with (413) — or null when it may. The one rule for
// every feeder of readables: a server's dispatcher, a client.
//
// Asked for a chunk of a stream nobody reads (every one of them is kept),
// and, where the carrier cannot pause, of one whose reader is behind by more
// than a high-water mark of chunks. A reader that keeps up costs nothing
// here: the sum is not taken — 15 million a second whether 1 or 256 streams
// are open. The sum is an addition per open stream: 14 million a second for
// a chunk of the one stream nobody reads, 1.1 million with 256 of them
// (bench/stream-buffer.js) — the cost of a peer that opened every stream it
// may and read none, and not worth a count kept in step on every chunk of
// every stream.
const overCap = (streams, stream, length, cap, paced) => {
  const isUnread = !stream.consuming;
  if (!stream.streaming || !(isUnread || (!paced && stream.queued > DEFAULT_HIGH_WATER_MARK))) return null;
  if (heldBytes(streams, paced) + length + CHUNK_COST <= cap) return null;
  if (isUnread) return 'Too many bytes in streams nobody reads yet';
  return `Stream ${stream.label} is too far ahead of its reader`;
};

// Ends what nobody came to read: a stream that has had no reader and no
// byte since the last sweep. An upload whose call was refused before its
// handler read it, a download no page asked for, a stream its sender
// announced and left — each held its bytes, and its slot, for as long as the
// connection lived; enough of them and the connection's next upload was
// refused for room they were never going to give back.
//
// The stream FAILS (408): what it held is let go of, and its sender is told
// to stop — unless it arrived whole, when there is nothing left to stop.
// Then the sweep reads it, as the reader that comes for a stream nobody came
// for: a failed stream is kept until a reader has been told why, and it is
// that wait — the one its sender's `end` is parked in — that forgets the
// stream. One whose sender has not ended it yet stays, failed and empty,
// until it does: what is still on its way is dropped, not answered.
const sweepStreams = (streams) => {
  for (const stream of streams.values()) {
    if (!stream.idle()) continue;
    if (stream.streaming) {
      if (stream.bytesReceived === stream.size) stream.onCancel = null;
      stream.fail(`Stream ${stream.label} was never read`, 408);
    }
    if (stream.error !== null) void stream.read();
  }
};

class WrpcReadable extends Emitter {
  #consuming = false;
  // What waits for the reader, read from `#head`: the chunks before it were
  // taken. A cursor, not `shift()` — how long this gets is the SENDER's
  // choice (a stream nobody reads yet keeps every chunk, and a megabyte a
  // byte at a time is a million of them), and past a few thousand elements
  // every `shift()` copies the rest: a million queued chunks read back that
  // way stalled the event loop for 350 seconds. 229 thousand chunks a second
  // against 36 million at 65,536 queued (bench/stream-queue.js).
  #chunks = [];
  #head = 0;
  // What `bytesReceived` read when idle() was last asked.
  #mark = -1;
  streaming = true;
  status = 'active';
  bytesRead = 0;
  // What the sender put in so far, read or not: held against `size`.
  bytesReceived = 0;
  // Why the stream failed (see fail()): what its reader's iteration throws.
  error = null;
  // Who hears that the stream is over on this side before its sender ended
  // it: set by whoever feeds the stream and can reach its sender, called
  // once and dropped with the stream — with the error of a stream that
  // failed (`error` is set), or what its reader let go of it with
  // (terminate(): its reason, null when it gave none). A feeder that ends
  // the stream itself (the sender's own terminate, a connection that
  // closed) takes it off first.
  onCancel = null;
  highWaterMark = DEFAULT_HIGH_WATER_MARK;

  constructor(id, name, size, options = {}) {
    // Its waiters are chunks waiting for room, bounded by checkStreamLimits:
    // the Emitter's leak warning would be a console line per waiting chunk —
    // a thousand of them for one slow reader, and a peer's to print.
    super({ maxListeners: Infinity });
    this.id = id;
    this.name = name;
    this.size = size;
    const { highWaterMark } = options;
    if (highWaterMark) this.highWaterMark = highWaterMark;
  }

  // Whether anybody reads it yet. Until then nothing below holds a chunk
  // back, so whoever feeds the stream bounds what it holds (the server's
  // `maxStreamBuffer`).
  get consuming() {
    return this.#consuming;
  }

  // Whether nothing happened to it since this was last asked: no reader
  // came, no byte arrived. What a sweep asks of every stream it visits
  // (sweepStreams) — so a stream is idle for one whole interval at least
  // before it answers yes.
  idle() {
    if (this.#consuming) return false;
    if (this.#mark === this.bytesReceived) return true;
    this.#mark = this.bytesReceived;
    return false;
  }

  // The name, as a message may carry it (shown).
  get label() {
    return shown(this.name);
  }

  // How many chunks wait for the reader.
  get queued() {
    return this.#chunks.length - this.#head;
  }

  // The chunks that wait, as an array of their own: a snapshot, built when
  // asked — what the stream holds is not reachable through it.
  get queue() {
    return this.#chunks.slice(this.#head);
  }

  // The high-water mark applies only once a consumer attached: before that
  // nobody emits PULL_EVENT, so blocking here would deadlock producers whose
  // consumer starts from a later packet on the same paused socket (the
  // upload-then-call wire pattern).
  //
  // A stream that is no longer streaming takes nothing: its reader let go
  // of it, or it failed, and the sender — who has not heard, or does not
  // care — goes on until its own `end`. Those chunks used to be queued, a
  // whole upload of them. And the announced size is a promise: the chunk
  // that breaks it fails the stream and rejects here, once, for whoever
  // feeds it to tell the sender.
  //
  // A chunk of no bytes is nothing to read and is not kept: it counted for
  // nothing against the size or against a cap, and each one took a slot —
  // a million and a half of them grew a server by 305 MiB.
  async push(data) {
    if (!this.streaming || data.length === 0) return;
    this.bytesReceived += data.length;
    if (this.bytesReceived > this.size) {
      throw this.fail(`Stream ${this.label} exceeds its announced size of ${this.size} bytes`);
    }
    while (this.#consuming && this.queued > this.highWaterMark) {
      this.checkStreamLimits();
      await this.waitEvent(PULL_EVENT);
    }
    // Ended while this chunk waited for room.
    if (!this.streaming) return;
    this.#chunks.push(data);
    if (this.queued === 1) this.emit(PUSH_EVENT);
    return data;
  }

  async finalize(writable) {
    const onError = () => this.terminate();
    writable.once('error', onError);
    for await (const chunk of this) {
      // A closed sink accepts nothing and will never emit 'drain'
      if (writable.closed) {
        await this.terminate();
        return void writable.removeListener('error', onError);
      }
      const needDrain = !writable.write(chunk);
      if (needDrain && !writable.closed) await writable.waitEvent('drain');
    }
    announce(this, 'end');
    writable.end();
    await writable.waitEvent('close');
    await this.close();
    writable.removeListener('error', onError);
  }

  // A stream that failed takes its sink with it — the node way; the 'error'
  // a node sink then emits is heard by the listener finalize() left on it.
  // On the readable it is announced only to someone listening: a sender
  // that broke its announcement must not be able to make an unhandled
  // 'error' of it.
  pipe(writable) {
    this.finalize(writable).catch((error) => {
      if (error === this.error) {
        writable.destroy?.(error);
        if (this.listenerCount('error') === 0) return;
      }
      // An 'error' listener that throws has nowhere left to go.
      return this.emit('error', error).catch(noop);
    });
    return writable;
  }

  async toBlob(type = '') {
    const chunks = [];
    for await (const chunk of this) {
      chunks.push(chunk);
    }
    return new Blob(chunks, { type });
  }

  // The sender's `end`. Short of the announced size the stream FAILS — its
  // reader used to wait for the missing bytes for as long as the connection
  // lived — and this rejects, at once, for whoever feeds the stream to tell
  // the sender; otherwise it resolves as close() does.
  async end() {
    if (this.streaming && this.bytesReceived !== this.size) {
      throw this.fail(`Stream ${this.label} ended at ${this.bytesReceived} of ${this.size} bytes`);
    }
    await this.stop(false);
  }

  // Resolves once the reader has all of it: every announced byte — of an
  // empty stream, once it has a reader at all, so the stream is still there
  // when its reader comes for it — and of a failed one, once a reader came
  // to be told why. A terminated stream has nothing left to wait for.
  async close() {
    await this.stop(false);
  }

  // The reader's own end, now and quietly: its loop stops where it is,
  // with no error. Whoever feeds the stream tells its sender (onCancel) —
  // `message`, with `code` (499 unless given), is what the sender hears in
  // place of the plain "no longer read": a handler refusing an upload with
  // something its uploader can show. Only a string is a reason: terminate
  // is also handed over as a listener, and called with whatever that emits.
  // The reason is for the sender, whatever its code: `expose` lets a 5xx's
  // message reach it (tellSender in src/rpc/dispatcher.js).
  async terminate(message, code = 499) {
    this.#finish('terminated', typeof message === 'string' ? new WrpcError(message, { code, expose: true }) : null);
  }

  // Ends the stream for its reader with an error it can see: the iteration
  // throws it, toBlob() rejects with it, pipe() destroys its sink with it.
  // For a sender that did not keep its announcement; terminate() is the
  // quiet end — the reader stops where it is. Answers the error.
  fail(message, code = 400) {
    const error = new WrpcError(message, { code });
    if (this.streaming) {
      this.error = error;
      this.#finish('terminated');
    }
    return error;
  }

  // Forced, now; otherwise once the reader has all of it (see close()).
  async stop(force = false) {
    if (!force) {
      while (this.streaming ? !(this.#consuming && this.bytesRead === this.size) : this.error && !this.#consuming) {
        await this.waitEvent(PULL_EVENT);
      }
    }
    this.#finish(force ? 'terminated' : 'closed');
  }

  // Once: the first end decides the status. What is queued is let go of,
  // and whoever is parked — a reader, a chunk waiting for room, a close()
  // waiting for the last byte — wakes to find the stream over.
  #finish(status, reason = null) {
    if (!this.streaming) return;
    this.streaming = false;
    this.status = status;
    this.#chunks.length = 0;
    this.#head = 0;
    const { onCancel } = this;
    this.onCancel = null;
    if (onCancel && status !== 'closed') onCancel(this.error ?? reason);
    this.emit(PUSH_EVENT, null);
    this.emit(PULL_EVENT);
    // For whoever counts streams: it is over, `status` and `error` say how.
    announce(this, 'close');
  }

  async read() {
    if (this.queued > 0) return this.pull();
    // A reader with nothing to read is a reader all the same, and a close()
    // may be waiting for one: an empty stream's, a failed stream's.
    if (!this.#consuming) {
      this.#consuming = true;
      this.emit(PULL_EVENT);
    }
    if (!this.streaming) return null;
    const finisher = await this.waitEvent(PUSH_EVENT);
    if (finisher === null) return null;
    return this.pull();
  }

  // The slot is let go of as it is read. The array is emptied when the head
  // reaches its end — what a stream that is being read does every few
  // chunks — and what is left is moved down once the head has passed half of
  // a long one, so a queue that never runs dry does not grow by what was
  // read.
  pull() {
    this.#consuming = true;
    const chunks = this.#chunks;
    /* c8 ignore next -- read()/waitEvent(PUSH_EVENT) callers already guard against an empty queue */
    if (this.#head === chunks.length) return undefined;
    const data = chunks[this.#head];
    chunks[this.#head++] = null;
    if (this.#head === chunks.length) {
      chunks.length = 0;
      this.#head = 0;
    } else if (this.#head >= COMPACT_AT && this.#head * 2 >= chunks.length) {
      chunks.copyWithin(0, this.#head);
      chunks.length -= this.#head;
      this.#head = 0;
    }
    this.bytesRead += data.length;
    this.emit(PULL_EVENT);
    return data;
  }

  // A reader that has ten chunks waiting for room behind it is given a
  // longer queue rather than more waiters — and one that falls a thousand
  // chunks behind FAILS the stream: on a carrier that cannot hold its peer
  // back (uWebSockets.js, a WebRTC channel, a broker session, a browser's
  // WebSocket) a slow reader gets here in a second or two. It used to throw
  // a plain error for the chunk alone — which push() had already counted, so
  // the `end` behind it found every announced byte "received" and the reader
  // waited for the dropped ones for as long as the connection lived.
  checkStreamLimits() {
    if (this.listenerCount(PULL_EVENT) >= MAX_LISTENERS) {
      ++this.highWaterMark;
    }
    if (this.highWaterMark > MAX_HIGH_WATER_MARK) {
      throw this.fail(`Stream ${this.label} is too far ahead of its reader`, 413);
    }
  }

  waitEvent(event) {
    return new Promise((resolve) => this.once(event, resolve));
  }

  async *[Symbol.asyncIterator]() {
    for (;;) {
      const chunk = await this.read();
      if (!chunk) break;
      yield chunk;
    }
    if (this.error) throw this.error;
  }
}

class WrpcWritable extends Emitter {
  #waitingDrain = false;
  #closed = false;
  #ended = false;
  #onClose = null;
  // Where #onClose waits for the transport's close (awaitClose).
  #closing = null;
  #registry;
  // Why the receiver ended the stream (see cancel()), or null: a transport
  // that closed gives no reason.
  error = null;

  // `registry`: where the transport's owner looks a stream up by id when
  // its receiver answers on that id (a client, a server-side Client). The
  // stream puts itself in and takes itself out — at its end, its terminate,
  // a cancel, the transport's close — so an owner that hands a Map over
  // keeps nothing past a stream's life. Without one nobody routes an answer
  // here, and cancel() is never called.
  //
  // What a stream is announced with is checked HERE, for both ends: a
  // server-side createStream() used to throw a plain Error for a size that
  // was none (and take a number for a name), a client's threw for nothing —
  // it announced whatever it was given and was closed by the server's 400 a
  // round trip later. A mistake in a program is told where it is made.
  constructor(id, name, size, transport, otel = null, registry = null) {
    if (typeof name !== 'string' || name === '') {
      throw new ArgumentError('createStream: name must be a non-empty string');
    }
    if (!Number.isSafeInteger(size) || size < 0) {
      throw new ArgumentError('createStream: size must be a non-negative integer of bytes');
    }
    super();
    this.id = id;
    this.name = name;
    this.size = size;
    this.transport = transport;
    // Null on the client, where there is no server-side byte counter to
    // feed and the bytes would only cost the browser bundle. On the server
    // it is the writer, so `wrpc.server.stream.bytes` finally has a `send`
    // direction — the attribute existed from the start with exactly one
    // value ever recorded, which made it a lie.
    this.otel = otel;
    this.#registry = registry;
    registry?.set(id, this);
    // Armed HERE, not lazily inside the backpressure branch: a stream that
    // never happened to stall used to learn nothing when the transport
    // closed — `closed` stayed false and a producer loop kept writing into
    // the void. Now every writable notices the disconnect, backpressured
    // or not — and lets go of the transport once finished (#detach).
    if (typeof transport.on === 'function') {
      this.#onClose = () => {
        this.#onClose = null;
        this.#shut();
      };
      this.#closing = awaitClose(transport, this.#onClose);
    }
    this.init();
  }

  get closed() {
    return this.#closed;
  }

  // Closed under its writer — by the transport, or by its receiver's word:
  // nothing more is taken, and a producer parked on 'drain' is let go.
  #shut() {
    if (this.#closed) return;
    this.#closed = true;
    // Under a writer that had not ended it: the transport went away.
    if (!this.#ended) this.otel?.recordStream('send', 'lost');
    this.#registry?.delete(this.id);
    this.#releaseDrain();
    announce(this, 'close');
  }

  #releaseDrain() {
    if (!this.#waitingDrain) return;
    this.#waitingDrain = false;
    this.#detach();
    announce(this, 'drain');
  }

  // The transport outlives its streams — a client's for every reconnect —
  // so a listener left on it per stream piled up for the connection's whole
  // life. Taken off once the stream is finished: ended, and no 'drain' still
  // owed to a producer, which only the close listener could release on a
  // transport that dies first.
  #detach() {
    if (!this.#ended || this.#waitingDrain || this.#onClose === null) return;
    forgetClose(this.transport, this.#closing, this.#onClose);
    this.#onClose = null;
  }

  // `cancel`: this end listens for its receiver's word on the stream (see
  // cancel()), which only an owner that routes answers by id can deliver.
  // A receiver tells a sender that its READER let go only where it asked —
  // one that does not know the answer reports a callback nobody waited for.
  init() {
    const { id, name, size } = this;
    // One shape or the other, never one grown into the other.
    const type = 'stream';
    this.transport.send(this.#registry ? { type, id, name, size, cancel: true } : { type, id, name, size });
  }

  // Reports the transport's real acceptance: false means the socket buffer
  // is above its high-water mark — wait for this stream's 'drain' event.
  // A false return can also mean the transport closed: check `closed` (or
  // listen for 'close') — after it, no 'drain' will ever follow.
  // Transports without flow-control reporting (browser WebSocket, HTTP)
  // always count as accepted.
  write(data) {
    if (this.#closed || this.#ended) return false;
    const chunk = chunkEncode(this.id, data);
    this.otel?.recordStreamBytes('send', chunk.byteLength ?? chunk.length);
    const accepted = this.transport.write(chunk) !== false;
    if (!accepted && !this.#waitingDrain && typeof this.transport.once === 'function') {
      this.#waitingDrain = true;
      this.transport.once('drain', () => this.#releaseDrain());
    }
    return accepted;
  }

  // Every byte of `data`, in pieces of `chunkSize`, each handed over when
  // the transport takes it — a write it did not accept is waited out on
  // 'drain'. The loop every application writing a buffer hand-rolled, or
  // did not: written whole, a payload is ONE message that nothing paces and
  // an engine's message limit refuses. Answers whether all of it went:
  // false once the transport closed, or the stream was ended or terminated
  // under it (where a hand-rolled loop waits for a 'drain' that never
  // comes). The end is the caller's — several buffers make one stream.
  //
  // What it is handed is checked, because what it did with anything else
  // was worse than throwing: an ArrayBuffer or a DataView has no `length`,
  // so nothing was written and the answer was `true`; a `chunkSize` of zero
  // never advanced, and wrote empty chunks until the process died.
  async writeAll(data, chunkSize = CHUNK_SIZE) {
    if (!(data instanceof Uint8Array)) throw new ArgumentError('writeAll: data must be a Uint8Array');
    if (!Number.isInteger(chunkSize) || chunkSize <= 0) {
      throw new ArgumentError('writeAll: chunkSize must be a positive integer of bytes');
    }
    for (let at = 0; at < data.length; at += chunkSize) {
      if (this.write(data.subarray(at, at + chunkSize))) continue;
      if (this.#closed || this.#ended) return false;
      await new Promise((resolve) => this.once('drain', resolve));
      if (this.#closed) return false;
    }
    return true;
  }

  // Idempotent, and a no-op on a dead transport: the peer that would have
  // read the status packet is gone.
  #finish(status, outcome = status === 'end' ? 'closed' : 'terminated', code = undefined) {
    if (this.#closed || this.#ended) return;
    this.#ended = true;
    this.otel?.recordStream('send', outcome, code);
    this.#registry?.delete(this.id);
    this.transport.send({ type: 'stream', id: this.id, status });
    this.#detach();
  }

  end() {
    this.#finish('end');
  }

  terminate() {
    this.#finish('terminate');
  }

  // The receiver's word that the stream is over on its side — it refused
  // the announcement, the stream failed there, or its reader let go of it —
  // handed over by the owner of the transport, which found the stream by
  // the id the answer came on. The writer stops: write() answers false and
  // writeAll() resolves false, `closed` is true and 'close' is announced,
  // as for a transport that went away — with `error` saying why, which a
  // lost connection never does. The terminate that goes out is what lets
  // the receiver forget the stream: it keeps a stream that is over until
  // its sender ends it, to drop what was still on its way. A stream its
  // writer already ended has nothing left to stop.
  cancel(error) {
    if (this.#closed || this.#ended) return;
    this.error = error;
    this.#finish('terminate', 'cancelled', error.code);
    this.#shut();
  }
}

module.exports = {
  CHUNK_COST,
  STREAM_IDLE_TIMEOUT,
  MAX_STREAMS,
  MAX_STREAM_BUFFER,
  shown,
  heldBytes,
  overCap,
  sweepStreams,
  WrpcReadable,
  WrpcWritable,
};
