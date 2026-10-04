'use strict';

const zlib = require('node:zlib');

const { TRAILER } = require('./permessageDeflate.js');

// The per-connection half of permessage-deflate with context takeover
// (RFC 7692 7.1.1): one long-lived deflate stream, one long-lived inflate
// stream, each keeping its window across messages so a message may
// reference the bytes of the ones before it — the ratio the stateless
// one-shot path gives up.
//
// zlib exposes no synchronous API on a live stream (deflateRawSync builds
// a throwaway one), so both directions are the public write() + flush()
// pair, asynchronous, and the Connection's ordering queues keep frames in
// order around them. One operation at a time per stream, enforced HERE: a
// second operation started while the first is still flushing would collect
// the first one's output too, so each direction keeps its own FIFO and
// starts the next operation when the previous one settles.
//
// Memory per connection: the deflate window (1 << windowBits, up to 32 KiB)
// plus its hash tables (1 << (memLevel + 9), 128 KiB at the default 8) —
// what docs/reference/wire-format.md weighs against the ratio.
class DeflateContext {
  #deflate = null;
  #inflate = null;
  #options;
  #closed = false;
  // Per-direction operation queues: [{ input, maxLength, cb }], head in flight.
  #deflateOps = [];
  #inflateOps = [];

  constructor({ windowBits = 15, level, memLevel, server = true, client = true } = {}) {
    this.#options = { windowBits, level, memLevel, server, client };
  }

  get closed() {
    return this.#closed;
  }

  // The stream is created on first use so a connection that negotiated
  // takeover but never sends a large message pays nothing.
  #deflateStream() {
    if (this.#deflate === null) {
      const { windowBits, level, memLevel } = this.#options;
      const options = { windowBits };
      if (level !== undefined) options.level = level;
      if (memLevel !== undefined) options.memLevel = memLevel;
      this.#deflate = zlib.createDeflateRaw(options);
    }
    return this.#deflate;
  }

  #inflateStream() {
    if (this.#inflate === null) this.#inflate = zlib.createInflateRaw({ windowBits: 15 });
    return this.#inflate;
  }

  // Compresses one message on the live stream: write, then a sync flush so
  // the output ends on a byte boundary with the empty stored block RFC 7692
  // 7.2.1 says to strip. cb(error, buffer).
  compress(payload, cb) {
    if (this.#closed) return void cb(new Error('deflate context closed'));
    this.#schedule(
      this.#deflateOps,
      () => this.#deflateStream(),
      payload,
      Infinity,
      (error, output) => {
        if (error) return void cb(error);
        cb(null, output.subarray(0, output.length - TRAILER.length));
      },
    );
  }

  // Inflates one message on the live stream, the peer's stripped trailer
  // re-appended. `maxLength` is the inflated-size cap: the stream is
  // watched as it produces output, and a message that would exceed the cap
  // fails with ERR_BUFFER_TOO_LARGE — the code the sync path's
  // maxOutputLength uses — after at most one chunk past it.
  decompress(payload, maxLength, cb) {
    if (this.#closed) return void cb(new Error('deflate context closed'));
    this.#schedule(this.#inflateOps, () => this.#inflateStream(), Buffer.concat([payload, TRAILER]), maxLength, cb);
  }

  #schedule(ops, stream, input, maxLength, cb) {
    ops.push({ stream, input, maxLength, cb });
    if (ops.length === 1) this.#next(ops);
  }

  #next(ops) {
    if (ops.length === 0 || this.#closed) return;
    const op = ops[0];
    run(op.stream(), op.input, op.maxLength, (error, output) => {
      ops.shift();
      op.cb(error, output);
      this.#next(ops);
    });
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    // Whatever was still queued never lands: the connection is gone.
    this.#deflateOps.length = 0;
    this.#inflateOps.length = 0;
    if (this.#deflate !== null) this.#deflate.close();
    if (this.#inflate !== null) this.#inflate.close();
    this.#deflate = null;
    this.#inflate = null;
  }
}

// One write + Z_SYNC_FLUSH on a zlib stream, collecting the output. The
// stream's 'data' and 'error' listeners are attached for the duration of
// the operation only: a context serves one message at a time.
const run = (stream, input, maxLength, cb) => {
  const chunks = [];
  let total = 0;
  let settled = false;
  const finish = (error, output) => {
    if (settled) return;
    settled = true;
    stream.off('data', onData);
    stream.off('error', onError);
    cb(error, output);
  };
  const onData = (chunk) => {
    total += chunk.length;
    if (total > maxLength) {
      const error = new RangeError('Inflated message exceeds the size cap');
      error.code = 'ERR_BUFFER_TOO_LARGE';
      // The stream holds the bomb's state; it is not reused after this.
      stream.close();
      return void finish(error);
    }
    chunks.push(chunk);
  };
  const onError = (error) => finish(error);
  stream.on('data', onData);
  stream.on('error', onError);
  stream.write(input);
  stream.flush(zlib.constants.Z_SYNC_FLUSH, () => {
    finish(null, chunks.length === 1 ? chunks[0] : Buffer.concat(chunks));
  });
};

module.exports = { DeflateContext };
