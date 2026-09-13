'use strict';

// Framing for an RTCDataChannel: how wrpc packets and stream chunks ride a
// channel whose messages have a size limit.
//
// A WebSocket carries a JSON packet as one text frame and a stream chunk as
// one binary frame, however large. A data channel message is capped —
// 16 KiB is the only size every implementation agrees on, a modern browser
// negotiates more through `sctp.maxMessageSize` — and wrpc's batch frames
// and 64 KiB stream chunks are routinely over it. So every message is sent
// as binary (the channel's `binaryType` is set to 'arraybuffer'), split into
// fragments that fit, each with a one-byte header:
//
//   bit 0   KIND   0 = a wrpc packet (UTF-8 JSON, what a WebSocket text frame carries)
//                  1 = a binary stream chunk (a chunkEncode frame)
//   bit 1   FIN    1 = the last fragment of this message
//   bit 2-7 reserved, MUST be 0 — a set bit is a protocol error
//
// No message id, no sequence number: the channel is ordered and reliable,
// all fragments of one message are sent back to back, and the KIND of a
// continuation must match the message it continues. The format is
// documented in docs/reference/protocol.md (WebRTC) and wire-format.md.
//
// This file is on the hot path (once per packet, once per chunk) and
// browser-budgeted: manual loops, no generators, no spread.

const KIND_TEXT = 0;
const KIND_BINARY = 1;
const FLAG_FIN = 0b10;
const KIND_MASK = 0b01;
const RESERVED_MASK = 0b11111100;
const HEADER_BYTES = 1;

// The interop floor (RFC 8831 §6.6, what old Firefox honoured) — used when
// the implementation reports no size at all; and the ceiling above which
// one message would hold too much memory on the receiving side at once.
const MIN_MESSAGE_SIZE = 16 * 1024;
const MAX_MESSAGE_SIZE = 256 * 1024;
// A peer that never sends FIN would otherwise grow the reassembly buffer
// without bound.
const DEFAULT_MAX_REASSEMBLY = 16 * 1024 * 1024;

class FramingError extends Error {
  constructor(message, code) {
    super(message);
    this.name = 'FramingError';
    this.code = code;
  }
}

/**
 * The message size to fragment at. `sctp.maxMessageSize` already reflects
 * the REMOTE side's `a=max-message-size` (W3C §5.5), so it is the
 * agreed-upon limit — capped at `ceiling`, and the interop floor when
 * nothing usable is reported (no sctp, 0, Infinity). A reported size UNDER
 * the floor is honoured as-is: the floor is a fallback, never an override.
 */
const negotiateMessageSize = (sctp, ceiling = MAX_MESSAGE_SIZE) => {
  const advertised = sctp !== null && sctp !== undefined ? sctp.maxMessageSize : 0;
  const usable = typeof advertised === 'number' && Number.isFinite(advertised) && advertised > HEADER_BYTES;
  if (!usable) return Math.min(MIN_MESSAGE_SIZE, ceiling);
  return Math.min(advertised, ceiling);
};

const TEXT_ENCODER = new TextEncoder();
// fatal: an invalid byte sequence in a text frame is a protocol error, not
// a U+FFFD the JSON parser then trips over one layer up.
const TEXT_DECODER = new TextDecoder('utf-8', { fatal: true });

const toBytes = (input) => {
  if (input instanceof Uint8Array) return input;
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  throw new TypeError('framing: expected an ArrayBuffer or an ArrayBufferView');
};

class FrameEncoder {
  #room;
  // One buffer per encoder, reused for every fragment: bench/rtc-framing.js
  // measures a fresh Uint8Array per fragment at 3-7x LESS throughput (3.5M
  // vs 25M frames/s at 64 B, 10K vs 34K at 1 MiB / 16 KiB fragments) — the
  // allocation, not the copy, is the cost. The price is a contract — see
  // encode().
  #scratch;

  constructor(maxMessageSize) {
    if (!Number.isInteger(maxMessageSize) || maxMessageSize <= HEADER_BYTES) {
      throw new TypeError(`framing: maxMessageSize must be an integer above ${HEADER_BYTES}`);
    }
    this.maxMessageSize = maxMessageSize;
    this.#room = maxMessageSize - HEADER_BYTES;
    this.#scratch = new Uint8Array(maxMessageSize);
  }

  /**
   * Frames `bytes` as `kind`, handing each fragment to `sink` in order,
   * synchronously — the caller sends them back to back, which is what makes
   * the header's FIN bit enough. Returns the fragment count.
   *
   * CONTRACT: the frame handed to `sink` is a view over a buffer the NEXT
   * fragment overwrites. Consume it inside the call — RTCDataChannel.send
   * copies synchronously, which is the consumer this exists for — and never
   * retain it; a sink that must keep it takes `frame.slice()`.
   */
  encode(kind, bytes, sink) {
    const total = bytes.length;
    const room = this.#room;
    const scratch = this.#scratch;
    if (total <= room) {
      scratch[0] = kind | FLAG_FIN;
      scratch.set(bytes, HEADER_BYTES);
      sink(scratch.subarray(0, total + HEADER_BYTES));
      return 1;
    }
    let offset = 0;
    let count = 0;
    while (offset < total) {
      const size = total - offset < room ? total - offset : room;
      scratch[0] = offset + size === total ? kind | FLAG_FIN : kind;
      scratch.set(bytes.subarray(offset, offset + size), HEADER_BYTES);
      sink(scratch.subarray(0, size + HEADER_BYTES));
      offset += size;
      count++;
    }
    return count;
  }

  /**
   * A packet. UTF-8 is at most 3 bytes per UTF-16 code unit, so a short
   * enough string is known to fit and is encoded straight into the scratch
   * frame — no intermediate byte array (bench/rtc-framing.js: 8.3M vs 2M
   * packets/s at 64 B; the common case is a packet well under the limit).
   * Anything longer is encoded first and fragmented like bytes. Same sink
   * contract as encode().
   */
  encodeText(text, sink) {
    if (text.length * 3 <= this.#room) {
      const scratch = this.#scratch;
      const { written } = TEXT_ENCODER.encodeInto(text, scratch.subarray(HEADER_BYTES));
      scratch[0] = KIND_TEXT | FLAG_FIN;
      sink(scratch.subarray(0, written + HEADER_BYTES));
      return 1;
    }
    return this.encode(KIND_TEXT, TEXT_ENCODER.encode(text), sink);
  }
}

class FrameDecoder {
  #maxReassembly;
  #parts = null;
  #kind = -1;
  #size = 0;

  constructor({ maxReassembly = DEFAULT_MAX_REASSEMBLY } = {}) {
    if (!Number.isInteger(maxReassembly) || maxReassembly <= 0) {
      throw new TypeError('framing: maxReassembly must be a positive integer');
    }
    this.#maxReassembly = maxReassembly;
  }

  /** Bytes of the message being reassembled; 0 between messages. */
  get pending() {
    return this.#size;
  }

  /**
   * Feeds one channel message. Returns `{ kind, data }` when it completes a
   * message — `data` is a string for KIND_TEXT and a Uint8Array for
   * KIND_BINARY — or null while one is still being reassembled. A
   * single-fragment binary message is answered as a view over the input
   * (no copy): every channel message is a fresh buffer, so nothing aliases.
   * Throws FramingError on a malformed frame; the decoder is reset so the
   * caller can terminate cleanly.
   */
  push(input) {
    const frame = toBytes(input);
    if (frame.length < HEADER_BYTES) throw this.#fail('empty frame', 'empty');
    const header = frame[0];
    if ((header & RESERVED_MASK) !== 0) throw this.#fail('reserved header bits set', 'reserved');
    const kind = header & KIND_MASK;
    const fin = (header & FLAG_FIN) !== 0;
    const payload = frame.subarray(HEADER_BYTES);
    if (this.#parts === null) {
      if (fin) return this.#finish(kind, payload);
      this.#parts = [payload];
      this.#kind = kind;
      this.#size = payload.length;
      this.#guard();
      return null;
    }
    if (kind !== this.#kind) throw this.#fail('continuation kind does not match the message', 'kind');
    this.#parts.push(payload);
    this.#size += payload.length;
    this.#guard();
    if (!fin) return null;
    const parts = this.#parts;
    const joined = new Uint8Array(this.#size);
    let offset = 0;
    for (let i = 0; i < parts.length; i++) {
      joined.set(parts[i], offset);
      offset += parts[i].length;
    }
    this.reset();
    return this.#finish(kind, joined);
  }

  reset() {
    this.#parts = null;
    this.#kind = -1;
    this.#size = 0;
  }

  #guard() {
    if (this.#size > this.#maxReassembly) throw this.#fail('message exceeds maxReassembly', 'too-large');
  }

  #fail(message, code) {
    this.reset();
    return new FramingError(message, code);
  }

  #finish(kind, bytes) {
    if (kind === KIND_BINARY) return { kind, data: bytes };
    try {
      return { kind, data: TEXT_DECODER.decode(bytes) };
    } catch {
      throw this.#fail('invalid UTF-8 in a text frame', 'utf8');
    }
  }
}

module.exports = {
  KIND_TEXT,
  KIND_BINARY,
  FLAG_FIN,
  HEADER_BYTES,
  MIN_MESSAGE_SIZE,
  MAX_MESSAGE_SIZE,
  DEFAULT_MAX_REASSEMBLY,
  FramingError,
  negotiateMessageSize,
  FrameEncoder,
  FrameDecoder,
};
