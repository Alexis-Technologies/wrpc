'use strict';

// What a backplane envelope goes through on its way out of the process and
// back in — the rooms backplane and the cluster channels both: compression,
// then sealing, behind the one seam those two classes already hold
// (`encode(text, channel) -> text`, `decode(message, channel) -> text`).
// Node-only and built by the core, so the browser-bundled rooms.js carries
// none of it.
//
// Without `encryption` this is createEnvelopeCodec — plus, always, the
// way an envelope whose data holds BYTES crosses a string carrier. With it the
// two compose INSIDE one frame rather than one marker inside another — a
// backplane carries strings, and base64 twice is 78 % overhead where once
// is 33 %:
//
//   wrpc-sealed:<kid>:base64( sealed( u8 flags ‖ [u8 idLength ‖ codec id] ‖ body ) )
//   flags: bit 0 compressed, bit 1 the body is an attachments frame
//
// Compress-then-encrypt, because ciphertext does not compress. The codec id
// travels inside, as the `wrpc-enc:<id>:` marker names it outside, so the
// codec list still means "encode with the head, decode any".
//
// `decode` answers the text, `null` for an encoded envelope it cannot read
// (the callers' own `*.encoded` warning), and `undefined` for one this file
// refused AND reported — the refusals of a sealed layer are its own events:
// `*.unsealed` (plaintext where none is accepted), `*.open` (a sealed
// envelope that does not open — unknown kid, another key, a flipped bit, a
// replay; the reason is for this log only and never travels).

const {
  createEnvelopeCodec,
  normalizeSyncCompression,
  encodeIfSmaller,
  decodeOrNull,
  codecById,
  isEncodedEnvelope,
} = require('../compression/sync.js');
const { normalizeEnvelopeEncryption, createEnvelopeSealer } = require('../encryption/envelope.js');
const { isKid } = require('../encryption/keyring.js');
const { encodeAttachments, decodeAttachments } = require('../attachments.js');

const SEALED_PREFIX = 'wrpc-sealed:';
// An envelope whose event data holds BYTES: a backplane carries strings and
// JSON has no bytes, so the envelope rides as the binary attachments frame
// of src/attachments.js (its encoder is generic over any object), base64'd
// under this marker — or inside the sealed frame, flagged, when sealing.
const BINARY_PREFIX = 'wrpc-bin:';
const FLAG_COMPRESSED = 1;
const FLAG_BINARY = 2;

const isSealedEnvelope = (message) => typeof message === 'string' && message.startsWith(SEALED_PREFIX);

const asBuffer = (bytes) => Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);

// The envelope object an attachments frame carries, or null: a frame that
// does not decode is a peer's malformed message, never a throw in a handler.
const binaryEnvelope = (bytes) => {
  try {
    return decodeAttachments(bytes);
  } catch {
    return null;
  }
};

// What every backplane gets, options or not: bytes across instances. `inner`
// is the text codec underneath (compression), or null.
const withBytes = (inner) => ({
  ...inner,
  encode: inner === null ? (text) => text : (text) => inner.encode(text),
  encodeBytes: (envelope) => BINARY_PREFIX + asBuffer(encodeAttachments(envelope)).toString('base64'),
  // The same for a frame the caller already encoded — the cluster signs the
  // frame's bytes, so it holds them before this is asked for anything.
  encodeFrame: (frame) => BINARY_PREFIX + asBuffer(frame).toString('base64'),
  decode(message) {
    // JSON never starts with a `w`: one compare on the common path.
    if (message.charCodeAt(0) !== 119) return message;
    if (message.startsWith(BINARY_PREFIX)) {
      return binaryEnvelope(Buffer.from(message.slice(BINARY_PREFIX.length), 'base64'));
    }
    if (inner !== null) return inner.decode(message);
    // Compressed, and this instance holds no codec: unreadable, and known
    // to be — the callers' `*.encoded` warning.
    return isEncodedEnvelope(message) ? null : message;
  },
});

/**
 * `{ encode(text, channel), decode(message, channel) }`, or null when
 * neither option is on. `layer` separates the keys of the two users
 * ('rooms', 'cluster'); `event` prefixes the log events ('backplane',
 * 'cluster').
 */
// A refusal is said at warn once per kind and channel in this long, debug in
// between: whoever can publish one envelope can publish it in a loop, and a
// line per message made the log the thing that fell over. Capped, as the
// channels are many.
const REFUSAL_INTERVAL = 10_000;
const MAX_REFUSAL_KEYS = 1024;

const createEnvelope = ({ compression, encryption, maxMessage, name, layer, event, log, failed = null }) => {
  const said = new Map();
  const level = (key) => {
    const now = Date.now();
    const last = said.get(key);
    if (last !== undefined && now - last < REFUSAL_INTERVAL) return 'debug';
    if (last === undefined && said.size >= MAX_REFUSAL_KEYS) said.clear();
    said.set(key, now);
    return 'warn';
  };
  const sealing = normalizeEnvelopeEncryption(encryption, name);
  if (sealing === null) return withBytes(createEnvelopeCodec(compression, name, maxMessage, failed));
  const codecs = normalizeSyncCompression(compression, name);
  const head = codecs === null ? null : codecs.codecs[0];
  // `failed(direction, codec id, error)`: a codec that threw, said by the
  // core's reporter. One receiver for the head, built once.
  const encodeFailed = head === null || failed === null ? null : (error) => failed('encode', head.id, error);
  // What an instance still mid-rollout sends, and what `seal: false` sends.
  const plain = withBytes(createEnvelopeCodec(compression, name, maxMessage, failed));
  const sealer = createEnvelopeSealer({ encryption: sealing, layer });

  // `text` is the JSON envelope, or — with `binary` — the attachments frame
  // of an envelope that holds bytes.
  const frame = (text, binary = 0) => {
    const packed = head === null ? null : encodeIfSmaller(head, text, encodeFailed);
    if (packed === null) {
      const body = Buffer.allocUnsafe(1 + Buffer.byteLength(text));
      body[0] = binary;
      if (typeof text === 'string') body.write(text, 1);
      else body.set(text, 1);
      return body;
    }
    const idLength = Buffer.byteLength(head.id);
    const body = Buffer.allocUnsafe(2 + idLength + packed.length);
    body[0] = FLAG_COMPRESSED | binary;
    body[1] = idLength;
    body.write(head.id, 2);
    body.set(packed, 2 + idLength);
    return body;
  };

  const unframe = (body) => {
    if (body.length === 0) return null;
    const binary = (body[0] & FLAG_BINARY) !== 0;
    let bytes;
    if ((body[0] & FLAG_COMPRESSED) === 0) bytes = body.subarray(1);
    else {
      if (codecs === null || body.length < 2) return null;
      const end = 2 + body[1];
      const entry = end > body.length ? null : codecById(codecs, body.toString('utf8', 2, end));
      // Cold: only a compressed frame inside a sealed one gets here, and
      // the receiver is only called when it does not inflate.
      const heard = entry === null || failed === null ? null : (error) => failed('decode', entry.id, error);
      const out = entry === null ? null : decodeOrNull(entry, body.subarray(end), maxMessage, heard);
      if (out === null) return null;
      bytes = asBuffer(out);
    }
    return binary ? binaryEnvelope(bytes) : bytes.toString();
  };

  return {
    sealed: true,
    encode(text, channel) {
      if (!sealing.seal) return plain.encode(text);
      const { kid, sealed } = sealer.seal(frame(text), channel);
      return `${SEALED_PREFIX}${kid}:${sealed.toString('base64')}`;
    },
    encodeBytes(envelope, channel) {
      return this.encodeFrame(encodeAttachments(envelope), channel);
    },
    encodeFrame(bytes, channel) {
      if (!sealing.seal) return plain.encodeFrame(bytes);
      const { kid, sealed } = sealer.seal(frame(asBuffer(bytes), FLAG_BINARY), channel);
      return `${SEALED_PREFIX}${kid}:${sealed.toString('base64')}`;
    },
    decode(message, channel) {
      if (!message.startsWith(SEALED_PREFIX)) {
        if (sealing.acceptPlaintext) return plain.decode(message);
        return void log[level(`unsealed\0${channel}`)]({ event: `${event}.unsealed`, channel });
      }
      const colon = message.indexOf(':', SEALED_PREFIX.length);
      const kid = colon === -1 ? '' : message.slice(SEALED_PREFIX.length, colon);
      let body = null;
      let reason = 'format';
      if (colon !== -1) {
        try {
          body = sealer.open(kid, Buffer.from(message.slice(colon + 1), 'base64'), channel);
          // Our own publish, echoed back: nothing to read, nothing to report.
          if (body === null) return undefined;
        } catch (error) {
          // A refusal has a reason (OpenError, or the sealer's); anything
          // else is THIS side's failure — a key provider that threw — and
          // is an error line with the err, not a refusal without a reason.
          if (typeof error?.reason !== 'string') {
            log.error({ err: error, event: `${event}.keys`, channel, ...(isKid(kid) ? { kid } : {}) });
            return undefined;
          }
          reason = error.reason;
        }
      }
      const text = body === null ? null : unframe(body);
      if (text !== null) return text;
      // A frame that opened but names a codec this instance does not hold.
      if (body !== null) reason = 'codec';
      // The kid goes on the line only when it is one (peer text stays out).
      return void log[level(`open\0${reason}\0${channel}`)]({
        event: `${event}.open`,
        channel,
        reason,
        ...(isKid(kid) ? { kid } : {}),
      });
    },
  };
};

module.exports = { createEnvelope, isSealedEnvelope, SEALED_PREFIX, BINARY_PREFIX };
