'use strict';

const { OPCODES, RSV1 } = require('./constants.js');
const { encodeFrame } = require('./frame.js');
const { compress, compressAsync, MIN_WINDOW_BITS, MAX_WINDOW_BITS } = require('./permessageDeflate.js');

// The engine-owned half of a shared message (see Connection.sendPrepared):
// one text, encoded to the wire ONCE for however many connections write it.
//
// The plain frame is header + utf8 payload in one buffer. The deflated
// frames are keyed by the recipient's negotiated windowBits: with both
// no-context-takeover params pinned (permessageDeflate.js) the compressed
// bytes are a pure function of (payload, windowBits), so two connections
// that negotiated the same window get byte-identical frames — which is what
// turns a room fan-out's N deflates into at most one per distinct window.
// Without this a room of 50 paid 50 deflateRawSync calls per emit
// (bench/send-path.js, "room fan-out +deflate").
//
// Server frames are never masked, so a prepared frame can be written to any
// number of sockets; a client Connection falls back to its own encoding.
const SLOTS = MAX_WINDOW_BITS - MIN_WINDOW_BITS + 1;

class PreparedFrames {
  #plain = null;
  #deflated = null;

  constructor(text) {
    this.payload = Buffer.from(text, 'utf8');
  }

  get length() {
    return this.payload.length;
  }

  plain() {
    let frame = this.#plain;
    if (frame === null) frame = this.#plain = encodeFrame(OPCODES.TEXT, 0, this.payload);
    return frame;
  }

  #slots() {
    let frames = this.#deflated;
    if (frames === null) {
      frames = this.#deflated = new Array(SLOTS);
      for (let i = 0; i < SLOTS; i++) frames[i] = null;
    }
    return frames;
  }

  deflated(windowBits) {
    const frames = this.#slots();
    const slot = windowBits - MIN_WINDOW_BITS;
    let frame = frames[slot];
    // A slot may hold the waiters of an in-flight async deflate (below);
    // a synchronous caller then computes the same bytes itself rather than
    // block on the threadpool.
    if (frame === null || !Buffer.isBuffer(frame)) {
      frame = encodeFrame(OPCODES.TEXT, RSV1, compress(this.payload, windowBits));
      if (frames[slot] === null) frames[slot] = frame;
    }
    return frame;
  }

  // The off-loop variant (`perMessageDeflate.async`): the first recipient
  // to need a window's frame starts ONE zlib.deflateRaw, and every later
  // recipient of the same emit waits on that same result — still one
  // deflate per window per fan-out, just not on the event loop.
  deflatedAsync(windowBits, cb) {
    const frames = this.#slots();
    const slot = windowBits - MIN_WINDOW_BITS;
    const current = frames[slot];
    if (current !== null) {
      if (Buffer.isBuffer(current)) return void cb(null, current);
      return void current.push(cb);
    }
    const waiters = [cb];
    frames[slot] = waiters;
    compressAsync(this.payload, windowBits, (error, compressed) => {
      const frame = error ? null : encodeFrame(OPCODES.TEXT, RSV1, compressed);
      frames[slot] = frame;
      for (let i = 0; i < waiters.length; i++) waiters[i](error, frame);
    });
  }
}

module.exports = { PreparedFrames };
