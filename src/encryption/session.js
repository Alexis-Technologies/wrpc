'use strict';

// A wrpc session over a finished Noise handshake — the framing both ends
// share, on both platforms (no Buffer: this is bundled for a page).
//
//   handshake, first   00 05 ‖ u8 version (1) ‖ u8 nameLength ‖ protocol name
//                            ‖ u8 kidLength ‖ kid ‖ noise message
//   handshake, later   00 05 ‖ noise message
//   sealed             00 06 ‖ AEAD( u8 inner ‖ payload )      inner: 0 text, 1 bytes
//
// The first message NAMES the protocol — pattern, DH, cipher, hash — and the
// key id of the server static it was written for; nothing is negotiated
// back. Noise has no negotiation on purpose: the name initializes the
// handshake hash, and the whole header is fed to the PROLOGUE with the wire
// revision and the transport kind, so a header rewritten in flight, or a
// handshake replayed onto another kind of connection, fails the handshake
// rather than weakening it. A server that does not hold what was named
// refuses; it never answers "try this instead" — that reply would be the
// downgrade.
//
// Everything after the handshake rides sealed: text packets, stream chunks
// and the framed kinds of wire.js alike, as the payload of a kind-6 frame.
// Compression therefore happens INSIDE (compress, then seal).

const { FRAME_MARK, FRAME_HANDSHAKE, FRAME_SEALED, WRPC_PROTOCOL } = require('../wire.js');
const { concat, utf8 } = require('./bytes.js');
const { isPromise } = require('../compression/ids.js');

const HELLO_VERSION = 1;
const INNER_TEXT = 0;
const INNER_BYTES = 1;
const DEFAULT_REKEY_AFTER = 1 << 20;
const DEFAULT_HANDSHAKE_TIMEOUT = 10000;

const decoder = new TextDecoder('utf-8', { fatal: true });
const encoder = new TextEncoder();

/** The part of the first handshake message before the Noise bytes — and of the prologue. */
const helloHeader = (name, kid) => {
  const nameBytes = utf8(name);
  const kidBytes = utf8(kid);
  if (nameBytes.length > 255 || kidBytes.length > 255) throw new TypeError('encryption: protocol name or kid too long');
  return concat(Uint8Array.of(HELLO_VERSION, nameBytes.length), nameBytes, Uint8Array.of(kidBytes.length), kidBytes);
};

const prologueOf = (kind, header) => concat(utf8(`${WRPC_PROTOCOL}\0${kind}\0`), header);

const isFrame = (bytes, kind) => bytes.length >= 2 && bytes[0] === FRAME_MARK && bytes[1] === kind;

const frame = (kind, ...parts) => concat(Uint8Array.of(FRAME_MARK, kind), ...parts);

/** `{ name, kid, header, message }` from a first handshake frame, or null. */
const parseHello = (bytes) => {
  if (!isFrame(bytes, FRAME_HANDSHAKE) || bytes.length < 5 || bytes[2] !== HELLO_VERSION) return null;
  const nameEnd = 4 + bytes[3];
  if (nameEnd >= bytes.length) return null;
  const kidEnd = nameEnd + 1 + bytes[nameEnd];
  if (kidEnd > bytes.length) return null;
  try {
    return {
      name: decoder.decode(bytes.subarray(4, nameEnd)),
      kid: decoder.decode(bytes.subarray(nameEnd + 1, kidEnd)),
      header: bytes.subarray(2, kidEnd),
      message: bytes.subarray(kidEnd),
    };
  } catch {
    return null;
  }
};

const asBytes = (data) => {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
};

// Node's own string writer, where there is one: it sizes the buffer exactly,
// where encodeInto needs room for three bytes per UTF-16 unit — 4.8 µs
// against 10.7 on a 16 KB packet, 0.4 against 1.0 on 1 KB
// (bench/encryption.js, the inner-frame rows). A `typeof` guard, so a
// browser bundle neither needs nor polyfills it.
const NodeBuffer = typeof Buffer === 'undefined' ? null : Buffer;

// One message as the plaintext of a sealed frame: the inner kind, then the
// text as UTF-8 or the bytes as they are.
const innerOf = (data, buffer = NodeBuffer) => {
  if (typeof data === 'string' && buffer !== null) {
    const inner = buffer.allocUnsafe(1 + buffer.byteLength(data));
    inner[0] = INNER_TEXT;
    inner.write(data, 1);
    return inner;
  }
  if (typeof data === 'string') {
    // encodeInto writes at most three bytes per UTF-16 unit.
    const scratch = new Uint8Array(1 + data.length * 3);
    scratch[0] = INNER_TEXT;
    return scratch.subarray(0, 1 + encoder.encodeInto(data, scratch.subarray(1)).written);
  }
  const bytes = asBytes(data);
  const inner = new Uint8Array(1 + bytes.length);
  inner[0] = INNER_BYTES;
  inner.set(bytes, 1);
  return inner;
};

const sealedFrame = (sealed) => {
  const out = new Uint8Array(2 + sealed.length);
  out[0] = FRAME_MARK;
  out[1] = FRAME_SEALED;
  out.set(sealed, 2);
  return out;
};

// A sealed frame's plaintext back into what was sent: a string, or bytes.
const messageOf = (inner) => {
  if (inner.length === 0 || inner[0] > INNER_BYTES) throw new Error('encryption: malformed sealed message');
  return inner[0] === INNER_TEXT ? decoder.decode(inner.subarray(1)) : inner.subarray(1);
};

/**
 * The two directions of an established session. `seal(data)` answers the
 * frame to put on the wire and `open(frame)` the message that was sent —
 * each a plain value when the cipher is synchronous (node:crypto) and a
 * promise when it is not (crypto.subtle); the nonce is taken when the call
 * is made, so the CALLER keeps the results in call order (a Sequencer).
 */
class SecureChannel {
  #send;
  #receive;

  constructor({ send, receive }) {
    this.#send = send;
    this.#receive = receive;
  }

  seal(data) {
    return this.sealInner(innerOf(data));
  }

  sealInner(inner) {
    const sealed = this.#send.encrypt(inner);
    return isPromise(sealed) ? sealed.then(sealedFrame) : sealedFrame(sealed);
  }

  open(bytes) {
    if (!isFrame(bytes, FRAME_SEALED)) throw new Error('encryption: not a sealed frame');
    const inner = this.#receive.decrypt(bytes.subarray(2));
    return isPromise(inner) ? inner.then(messageOf) : messageOf(inner);
  }
}

module.exports = {
  SecureChannel,
  innerOf,
  helloHeader,
  prologueOf,
  parseHello,
  frame,
  isFrame,
  asBytes,
  FRAME_HANDSHAKE,
  FRAME_SEALED,
  DEFAULT_REKEY_AFTER,
  DEFAULT_HANDSHAKE_TIMEOUT,
};
