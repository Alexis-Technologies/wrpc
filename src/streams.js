'use strict';

const { Emitter } = require('./utils.js');
const { chunkEncode } = require('./chunks.js');

const PUSH_EVENT = Symbol('push');
const PULL_EVENT = Symbol('pull');
const DEFAULT_HIGH_WATER_MARK = 32;
const MAX_LISTENERS = 10;
const MAX_HIGH_WATER_MARK = 1000;
// What writeAll() writes at a time: what a browser's blob.stream() hands the
// Blob uploader, and under every carrier's message limit.
const CHUNK_SIZE = 64 * 1024;

class WrpcReadable extends Emitter {
  #consuming = false;
  queue = [];
  streaming = true;
  status = 'active';
  bytesRead = 0;
  // What the sender put in so far, read or not: held against `size`.
  bytesReceived = 0;
  // Why the stream failed (see fail()): what its reader's iteration throws.
  error = null;
  highWaterMark = DEFAULT_HIGH_WATER_MARK;

  constructor(id, name, size, options = {}) {
    super();
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
  async push(data) {
    if (!this.streaming) return;
    this.bytesReceived += data.length;
    if (this.bytesReceived > this.size) {
      throw this.fail(`Stream ${this.name} exceeds its announced size of ${this.size} bytes`);
    }
    while (this.#consuming && this.queue.length > this.highWaterMark) {
      this.checkStreamLimits();
      await this.waitEvent(PULL_EVENT);
    }
    // Ended while this chunk waited for room.
    if (!this.streaming) return;
    this.queue.push(data);
    if (this.queue.length === 1) this.emit(PUSH_EVENT);
    return data;
  }

  async finalize(writable) {
    const onError = () => this.terminate();
    writable.once('error', onError);
    for await (const chunk of this) {
      // A closed sink accepts nothing and will never emit 'drain'
      if (writable.closed) {
        await this.terminate();
        writable.removeListener('error', onError);
        return;
      }
      const needDrain = !writable.write(chunk);
      if (needDrain && !writable.closed) await writable.waitEvent('drain');
    }
    this.emit('end');
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
      return this.emit('error', error);
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
      throw this.fail(`Stream ${this.name} ended at ${this.bytesReceived} of ${this.size} bytes`);
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

  async terminate() {
    await this.stop(true);
  }

  // Ends the stream for its reader with an error it can see: the iteration
  // throws it, toBlob() rejects with it, pipe() destroys its sink with it.
  // For a sender that did not keep its announcement; terminate() is the
  // quiet end — the reader stops where it is. Answers the error.
  fail(message, code = 400) {
    const error = new Error(message);
    error.code = code;
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
  #finish(status) {
    if (!this.streaming) return;
    this.streaming = false;
    this.status = status;
    this.queue.length = 0;
    this.emit(PUSH_EVENT, null);
    this.emit(PULL_EVENT);
  }

  async read() {
    if (this.queue.length > 0) return this.pull();
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

  pull() {
    this.#consuming = true;
    const data = this.queue.shift();
    /* c8 ignore next -- read()/waitEvent(PUSH_EVENT) callers already guard against an empty queue */
    if (!data) return data;
    this.bytesRead += data.length;
    this.emit(PULL_EVENT);
    return data;
  }

  checkStreamLimits() {
    if (this.listenerCount(PULL_EVENT) >= MAX_LISTENERS) {
      ++this.highWaterMark;
    }
    /* c8 ignore start -- defensive guard; needs 1000+ concurrent stalled backpressure waiters to reach */
    if (this.highWaterMark > MAX_HIGH_WATER_MARK) {
      throw new Error('Stream overflow occurred');
    }
    /* c8 ignore stop */
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
  constructor(id, name, size, transport, otel = null, registry = null) {
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
    if (typeof transport.once === 'function') {
      this.#onClose = () => {
        this.#onClose = null;
        this.#shut();
      };
      transport.once('close', this.#onClose);
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
    this.#registry?.delete(this.id);
    this.#releaseDrain();
    this.emit('close');
  }

  #releaseDrain() {
    if (!this.#waitingDrain) return;
    this.#waitingDrain = false;
    this.#detach();
    this.emit('drain');
  }

  // The transport outlives its streams — a client's for every reconnect —
  // so a listener left on it per stream piled up for the connection's whole
  // life. Taken off once the stream is finished: ended, and no 'drain' still
  // owed to a producer, which only the close listener could release on a
  // transport that dies first.
  #detach() {
    if (!this.#ended || this.#waitingDrain || this.#onClose === null) return;
    this.transport.off?.('close', this.#onClose);
    this.#onClose = null;
  }

  init() {
    const { id, name, size } = this;
    const packet = { type: 'stream', id, name, size };
    this.transport.send(packet);
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
  async writeAll(data, chunkSize = CHUNK_SIZE) {
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
  #finish(status) {
    if (this.#closed || this.#ended) return;
    this.#ended = true;
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
    this.#finish('terminate');
    this.#shut();
  }
}

module.exports = { WrpcReadable, WrpcWritable };
