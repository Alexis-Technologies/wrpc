'use strict';

// Framing for a WebTransport stream: how wrpc packets and stream chunks ride
// a QUIC stream, which is a byte stream with no message boundaries.
//
// A WebSocket frame and a data-channel message each carry ONE packet or
// chunk. A WebTransport stream carries bytes, so every message on it is
// prefixed with a five-byte header:
//
//   bytes 0-3  LENGTH  payload byte length, unsigned big-endian
//   byte  4    KIND    0 = a wrpc packet (UTF-8 JSON, what a WebSocket text frame carries)
//                      1 = a binary stream chunk (a chunkEncode frame)
//                      2 = a capabilities message (UTF-8 JSON), each end's first message
//                      3 = a packet, compressed (src/compression) — only once negotiated
//                      4 = a chunk, compressed — likewise
//                      5-255 reserved — a protocol error, as are 3 and 4 before negotiation
//
// No fragmentation and no FIN bit: a QUIC stream is ordered and reliable and
// a message of any size is one contiguous run of bytes. The receiver's cap
// (maxMessage, 16 MiB by default — the engine's maxPayload) bounds what it
// buffers for one message; a LENGTH past it is a protocol error, not a
// bigger allocation. The format is documented in docs/reference/protocol.md
// (WebTransport) and wire-format.md.
//
// Frames are FRESH buffers, never a reused scratch: a WHATWG writer takes
// the chunk by reference and may process it after write() returns (a
// queued write under backpressure), so a buffer the next frame overwrites
// would corrupt what is still in flight — the opposite of a data channel's
// synchronous send(). This file is on the hot path (once per packet, once
// per chunk) and browser-budgeted: manual loops, no generators, no spread.

const HEADER_BYTES = 5;
const KIND_TEXT = 0;
const KIND_BINARY = 1;
const KIND_CAPS = 2;
// Compressed twins of KIND_TEXT and KIND_BINARY: the payload is what the
// negotiated codec produced, and the receiver inflates before it reads.
const KIND_TEXT_COMPRESSED = 3;
const KIND_BINARY_COMPRESSED = 4;
const DEFAULT_MAX_MESSAGE = 16 * 1024 * 1024;
// A packet up to this many UTF-16 code units is encoded straight into a
// frame sized for the worst case (three bytes per code unit) with
// encodeInto — one allocation, one pass, and up to 2/3 of it unused. Past
// it the text is encoded first and copied, so a large batch frame does not
// hold three times its size. The cap is a memory choice, not a speed one:
// bench/wt-framing.js has encodeInto ahead at every size (3.3M vs 2.1M
// packets/s at 64 B, 992K vs 827K at 1 KiB, 92K vs 71K at 16 KiB).
const INLINE_TEXT = 4096;
const EMPTY = new Uint8Array(0);

const TEXT_ENCODER = new TextEncoder();
// fatal: an invalid byte sequence in a packet is a protocol error, not a
// U+FFFD the JSON parser then trips over one layer up.
const TEXT_DECODER = new TextDecoder('utf-8', { fatal: true });

class FramingError extends Error {
  constructor(message, code) {
    super(message);
    this.name = 'FramingError';
    this.code = code;
  }
}

const toBytes = (input) => {
  if (input instanceof Uint8Array) return input;
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  throw new TypeError('framing: expected an ArrayBuffer or an ArrayBufferView');
};

/** A packet's bytes as text — the parser's own decode, for a packet inflated after parsing. */
const decodeText = (bytes) => {
  try {
    return TEXT_DECODER.decode(bytes);
  } catch {
    throw new FramingError('invalid UTF-8 in a packet', 'utf8');
  }
};

/** The object a capabilities message carries, or null when it is not one. */
const parseCaps = (text) => {
  try {
    const caps = JSON.parse(text);
    return typeof caps === 'object' && caps !== null ? caps : null;
  } catch {
    return null;
  }
};

const writeHeader = (out, length, kind) => {
  out[0] = length >>> 24;
  out[1] = (length >>> 16) & 255;
  out[2] = (length >>> 8) & 255;
  out[3] = length & 255;
  out[4] = kind;
};

/** One message of `kind` over `bytes`: a fresh frame, header included. */
const frame = (kind, bytes) => {
  const out = new Uint8Array(HEADER_BYTES + bytes.length);
  writeHeader(out, bytes.length, kind);
  out.set(bytes, HEADER_BYTES);
  return out;
};

/** A capabilities message: UTF-8 JSON under a KIND 2 header. */
const frameCaps = (text) => frame(KIND_CAPS, TEXT_ENCODER.encode(text));

/** A packet: UTF-8 under a KIND 0 header. */
const frameText = (text) => {
  if (text.length <= INLINE_TEXT) {
    const out = new Uint8Array(HEADER_BYTES + text.length * 3);
    const { written } = TEXT_ENCODER.encodeInto(text, out.subarray(HEADER_BYTES));
    writeHeader(out, written, KIND_TEXT);
    return out.subarray(0, HEADER_BYTES + written);
  }
  return frame(KIND_TEXT, TEXT_ENCODER.encode(text));
};

// A datagram is one whole message with no length to announce: its header
// is the KIND byte alone. Only packets ride datagrams today (KIND_TEXT);
// the receiver ignores any other kind, since a datagram may be dropped
// anyway and a wrong one is not worth a hangup.
const DATAGRAM_HEADER_BYTES = 1;

/** A packet as one datagram: `[KIND_TEXT][UTF-8]`, a fresh buffer. */
const datagramText = (text) => {
  const out = new Uint8Array(DATAGRAM_HEADER_BYTES + text.length * 3);
  const { written } = TEXT_ENCODER.encodeInto(text, out.subarray(DATAGRAM_HEADER_BYTES));
  out[0] = KIND_TEXT;
  return out.subarray(0, DATAGRAM_HEADER_BYTES + written);
};

// The datagram writer: the specification moved from `datagrams.writable`
// to `datagrams.createWritable()` (the former still works in browsers, and
// a host may log it as deprecated); take the newer one where it exists.
const datagramWriter = (datagrams) => {
  if (!datagrams) return null;
  if (typeof datagrams.createWritable === 'function') {
    try {
      return datagrams.createWritable().getWriter();
    } catch {
      // Fall through to the legacy stream.
    }
  }
  return typeof datagrams.writable?.getWriter === 'function' ? datagrams.writable.getWriter() : null;
};

/**
 * The packet a datagram carries, or null when it carries anything else
 * (an empty datagram, a reserved kind, invalid UTF-8): lossy by nature, so
 * an unreadable one is dropped, not answered with a hangup.
 */
const parseDatagram = (input) => {
  const bytes = toBytes(input);
  if (bytes.length < DATAGRAM_HEADER_BYTES || bytes[0] !== KIND_TEXT) return null;
  try {
    return TEXT_DECODER.decode(bytes.subarray(DATAGRAM_HEADER_BYTES));
  } catch {
    return null;
  }
};

/**
 * The receiving half: feed it whatever the stream's reader yields, in
 * order, and it calls `onMessage(kind, data)` once per completed message —
 * `data` a string for KIND_TEXT and a Uint8Array for KIND_BINARY. A message
 * contained in one read is answered as a view over that read's buffer (no
 * copy: every read is a fresh buffer, so nothing aliases); one spanning
 * reads is joined once. Throws FramingError on a malformed header; the
 * parser is reset so the caller can hang up cleanly.
 */
class StreamParser {
  // Whether the compressed kinds (3, 4) are accepted: set by the transport
  // once both ends named the same codec, a protocol error before.
  compressed = false;

  #max;
  #onMessage;
  // Buffered reads, the index of the first live one and the read cursor
  // within it — a FIFO byte queue in the shape of src/websocket/segments.js,
  // over Uint8Array so it runs in a browser, and with a head index instead
  // of shift(): a message assembled from many small reads would otherwise
  // pay O(n) per read consumed, O(n²) per message.
  #reads = [];
  #head = 0;
  #offset = 0;
  #buffered = 0;
  // What the next step needs: a header, or the payload the header announced.
  #need = HEADER_BYTES;
  #length = -1;
  #kind = -1;

  constructor({ maxMessage = DEFAULT_MAX_MESSAGE, onMessage } = {}) {
    if (!Number.isInteger(maxMessage) || maxMessage <= 0) {
      throw new TypeError('framing: maxMessage must be a positive integer');
    }
    if (typeof onMessage !== 'function') throw new TypeError('framing: onMessage must be a function');
    this.#max = maxMessage;
    this.#onMessage = onMessage;
  }

  /** Bytes buffered towards the next message; 0 between messages. */
  get pending() {
    return this.#buffered;
  }

  push(input) {
    const bytes = toBytes(input);
    if (bytes.length === 0) return;
    this.#reads.push(bytes);
    this.#buffered += bytes.length;
    while (this.#buffered >= this.#need) {
      if (this.#length < 0) {
        const header = this.#take(HEADER_BYTES);
        const length = ((header[0] << 24) >>> 0) + (header[1] << 16) + (header[2] << 8) + header[3];
        const kind = header[4];
        if (kind > KIND_CAPS && !(this.compressed && kind <= KIND_BINARY_COMPRESSED)) {
          throw this.#fail('unknown message kind', 'kind');
        }
        if (length > this.#max) throw this.#fail('message exceeds maxMessage', 'too-large');
        if (length === 0) {
          this.#deliver(kind, EMPTY);
          continue;
        }
        this.#length = length;
        this.#kind = kind;
        this.#need = length;
      } else {
        this.#deliver(this.#kind, this.#take(this.#length));
      }
    }
  }

  reset() {
    this.#reads.length = 0;
    this.#head = 0;
    this.#offset = 0;
    this.#buffered = 0;
    this.#need = HEADER_BYTES;
    this.#length = -1;
    this.#kind = -1;
  }

  #deliver(kind, payload) {
    this.#need = HEADER_BYTES;
    this.#length = -1;
    this.#kind = -1;
    // Bytes as they are for a chunk and for either compressed kind — a
    // compressed packet is text only once the transport has inflated it.
    if (kind === KIND_BINARY || kind >= KIND_TEXT_COMPRESSED) return void this.#onMessage(kind, payload);
    let text;
    try {
      text = TEXT_DECODER.decode(payload);
    } catch {
      throw this.#fail('invalid UTF-8 in a packet', 'utf8');
    }
    this.#onMessage(kind, text);
  }

  // Consumes exactly `size` leading bytes: a view when they lie within the
  // first read, one copy when they span reads.
  #take(size) {
    const reads = this.#reads;
    let head = this.#head;
    const first = reads[head];
    const available = first.length - this.#offset;
    let out;
    if (available > size) {
      out = first.subarray(this.#offset, this.#offset + size);
      this.#offset += size;
    } else if (available === size) {
      out = this.#offset === 0 ? first : first.subarray(this.#offset);
      reads[head++] = null;
      this.#offset = 0;
    } else {
      out = new Uint8Array(size);
      out.set(first.subarray(this.#offset), 0);
      let copied = available;
      reads[head++] = null;
      this.#offset = 0;
      while (copied < size) {
        const read = reads[head];
        const take = read.length < size - copied ? read.length : size - copied;
        out.set(take === read.length ? read : read.subarray(0, take), copied);
        copied += take;
        if (take === read.length) reads[head++] = null;
        else this.#offset = take;
      }
    }
    // Compact once the consumed prefix outweighs what is left.
    if (head === reads.length) {
      reads.length = 0;
      head = 0;
    } else if (head > 32 && head * 2 > reads.length) {
      reads.splice(0, head);
      head = 0;
    }
    this.#head = head;
    this.#buffered -= size;
    return out;
  }

  #fail(message, code) {
    this.reset();
    return new FramingError(message, code);
  }
}

module.exports = {
  HEADER_BYTES,
  KIND_TEXT,
  KIND_BINARY,
  KIND_CAPS,
  KIND_TEXT_COMPRESSED,
  KIND_BINARY_COMPRESSED,
  DEFAULT_MAX_MESSAGE,
  INLINE_TEXT,
  FramingError,
  toBytes,
  decodeText,
  parseCaps,
  frame,
  frameText,
  frameCaps,
  DATAGRAM_HEADER_BYTES,
  datagramText,
  parseDatagram,
  datagramWriter,
  StreamParser,
};
