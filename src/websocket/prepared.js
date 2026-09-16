'use strict';

const { OPCODES, RSV1 } = require('./constants.js');
const { encodeFrame } = require('./frame.js');
const { compress, MIN_WINDOW_BITS, MAX_WINDOW_BITS } = require('./permessageDeflate.js');

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

  deflated(windowBits) {
    let frames = this.#deflated;
    if (frames === null) {
      frames = this.#deflated = new Array(SLOTS);
      for (let i = 0; i < SLOTS; i++) frames[i] = null;
    }
    const slot = windowBits - MIN_WINDOW_BITS;
    let frame = frames[slot];
    if (frame === null) {
      frame = frames[slot] = encodeFrame(OPCODES.TEXT, RSV1, compress(this.payload, windowBits));
    }
    return frame;
  }
}

module.exports = { PreparedFrames };
