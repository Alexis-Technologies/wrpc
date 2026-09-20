'use strict';

// What a backplane envelope goes through on its way out of the process and
// back in — the rooms backplane and the cluster channels both: compression,
// then sealing, behind the one seam those two classes already hold
// (`encode(text, channel) -> text`, `decode(message, channel) -> text`).
// Node-only and built by the core, so the browser-bundled rooms.js carries
// none of it.
//
// Without `encryption` this is createEnvelopeCodec, unchanged. With it the
// two compose INSIDE one frame rather than one marker inside another — a
// backplane carries strings, and base64 twice is 78 % overhead where once
// is 33 %:
//
//   wrpc-sealed:<kid>:base64( sealed( u8 flags ‖ [u8 idLength ‖ codec id] ‖ body ) )
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
} = require('../compression/sync.js');
const { normalizeEnvelopeEncryption, createEnvelopeSealer } = require('../encryption/envelope.js');

const SEALED_PREFIX = 'wrpc-sealed:';
const FLAG_COMPRESSED = 1;

const isSealedEnvelope = (message) => typeof message === 'string' && message.startsWith(SEALED_PREFIX);

/**
 * `{ encode(text, channel), decode(message, channel) }`, or null when
 * neither option is on. `layer` separates the keys of the two users
 * ('rooms', 'cluster'); `event` prefixes the log events ('backplane',
 * 'cluster').
 */
const createEnvelope = ({ compression, encryption, maxMessage, name, layer, event, log }) => {
  const sealing = normalizeEnvelopeEncryption(encryption, name);
  if (sealing === null) return createEnvelopeCodec(compression, name, maxMessage);
  const codecs = normalizeSyncCompression(compression, name);
  const head = codecs === null ? null : codecs.codecs[0];
  // What an instance still mid-rollout sends, and what `seal: false` sends.
  const plain = createEnvelopeCodec(compression, name, maxMessage);
  const sealer = createEnvelopeSealer({ encryption: sealing, layer });

  const frame = (text) => {
    const packed = head === null ? null : encodeIfSmaller(head, text);
    if (packed === null) {
      const body = Buffer.allocUnsafe(1 + Buffer.byteLength(text));
      body[0] = 0;
      body.write(text, 1);
      return body;
    }
    const idLength = Buffer.byteLength(head.id);
    const body = Buffer.allocUnsafe(2 + idLength + packed.length);
    body[0] = FLAG_COMPRESSED;
    body[1] = idLength;
    body.write(head.id, 2);
    body.set(packed, 2 + idLength);
    return body;
  };

  const unframe = (body) => {
    if (body.length === 0) return null;
    if ((body[0] & FLAG_COMPRESSED) === 0) return body.toString('utf8', 1);
    if (codecs === null || body.length < 2) return null;
    const end = 2 + body[1];
    const entry = end > body.length ? null : codecById(codecs, body.toString('utf8', 2, end));
    const out = entry === null ? null : decodeOrNull(entry, body.subarray(end), maxMessage);
    return out === null ? null : Buffer.from(out.buffer, out.byteOffset, out.byteLength).toString();
  };

  return {
    sealed: true,
    encode(text, channel) {
      if (!sealing.seal) return plain === null ? text : plain.encode(text);
      const { kid, sealed } = sealer.seal(frame(text), channel);
      return `${SEALED_PREFIX}${kid}:${sealed.toString('base64')}`;
    },
    decode(message, channel) {
      if (!message.startsWith(SEALED_PREFIX)) {
        if (sealing.acceptPlaintext) return plain === null ? message : plain.decode(message);
        return void log.warn({ event: `${event}.unsealed`, channel });
      }
      const colon = message.indexOf(':', SEALED_PREFIX.length);
      let body = null;
      let reason = 'format';
      if (colon !== -1) {
        try {
          body = sealer.open(
            message.slice(SEALED_PREFIX.length, colon),
            Buffer.from(message.slice(colon + 1), 'base64'),
            channel,
          );
          // Our own publish, echoed back: nothing to read, nothing to report.
          if (body === null) return undefined;
        } catch (error) {
          reason = error.reason;
        }
      }
      const text = body === null ? null : unframe(body);
      if (text !== null) return text;
      // A frame that opened but names a codec this instance does not hold.
      if (body !== null) reason = 'codec';
      return void log.warn({ event: `${event}.open`, channel, reason });
    },
  };
};

module.exports = { createEnvelope, isSealedEnvelope, SEALED_PREFIX };
