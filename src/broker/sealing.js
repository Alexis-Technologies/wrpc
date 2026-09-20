'use strict';

// Sealed broker messages: the broker family's use of the keyring envelopes
// (src/encryption/envelope.js). A broker is a third party that KEEPS what it
// carries — a Kafka topic, a stream, a quorum queue hold every message for
// their retention, for whoever can read the topic or its backups — and until
// now that was every RPC packet, every published event, and the bearer token
// a client's headers carried.
//
//   headers   wrpc-sealed: <kid>        + whatever the binding routes by
//   body      sealed( u32 headerLength ‖ JSON(headers) ‖ body )
//
// The HEADERS move inside with the body: `authorization`, `x-wrpc-meta`, the
// trace context, the codec id. What stays outside is what a carrier or the
// binding needs before it can open anything — and that is bound into the
// additional data, so an outer header cannot be rewritten to make a sealed
// body mean something else.
//
// Node-only, synchronous, one shared keyring — the model of a backplane, not
// of a session: every service holding the key reads every message.

const { normalizeEnvelopeEncryption, createEnvelopeSealer } = require('../encryption/envelope.js');
const { toBytes } = require('./ids.js');

const HEADER_SEALED = 'wrpc-sealed';

const pack = (headers, body) => {
  const json = Buffer.from(JSON.stringify(headers));
  const bytes = toBytes(body);
  const out = Buffer.allocUnsafe(4 + json.length + bytes.length);
  out.writeUInt32BE(json.length, 0);
  json.copy(out, 4);
  out.set(bytes, 4 + json.length);
  return out;
};

const unpack = (bytes) => {
  if (bytes.length < 4) return null;
  const length = bytes.readUInt32BE(0);
  if (4 + length > bytes.length) return null;
  let headers;
  try {
    headers = JSON.parse(bytes.toString('utf8', 4, 4 + length));
  } catch {
    return null;
  }
  if (typeof headers !== 'object' || headers === null || Array.isArray(headers)) return null;
  // A null-prototype map, like every header bag the broker family hands out.
  const safe = Object.create(null);
  for (const name of Object.keys(headers)) if (typeof headers[name] === 'string') safe[name] = headers[name];
  return { headers: safe, body: bytes.subarray(4 + length) };
};

/**
 * `{ sealing, seal(context, headers, body, outer?), open(context, message) }`
 * or null for off. `context` is what the message is bound to — a topic, or
 * an address with its kind, correlation id and sequence — and must be the
 * same string on both ends. `outer` are the headers that stay readable;
 * `text` carries the sealed body as base64 for a carrier that keeps strings.
 *
 * `open` answers `{ headers, body, sealed }`; for a message that is refused
 * it answers `{ refused: reason }` — 'unsealed' (plaintext where none is
 * accepted), or why it did not open — and the caller decides what a refusal
 * means on its carrier (a dropped frame, a dead letter, a skipped entry).
 */
const createBrokerSealing = (option, name, { layer, replay, text = false }) => {
  const encryption = normalizeEnvelopeEncryption(option, name);
  if (encryption === null) return null;
  const sealer = createEnvelopeSealer({
    encryption: replay ? encryption : { ...encryption, replayWindow: 0 },
    layer,
    // A service may well consume what it published.
    echo: true,
  });
  return {
    sealing: encryption.seal,
    seal(context, headers, body, outer = null) {
      if (!encryption.seal) return { headers: { ...headers, ...outer }, body };
      const { kid, sealed } = sealer.seal(pack(headers ?? {}, body), context);
      return { headers: { ...outer, [HEADER_SEALED]: kid }, body: text ? sealed.toString('base64') : sealed };
    },
    open(context, message) {
      const kid = message.headers?.[HEADER_SEALED];
      if (kid === undefined || kid === null || kid === '') {
        if (!encryption.acceptPlaintext) return { refused: 'unsealed' };
        return { headers: message.headers, body: message.body, sealed: false };
      }
      let opened;
      try {
        // `text`: a log or a queue is only promised to keep a STRING as it
        // was (a Redis stream field is one), so the sealed body rides base64.
        const bytes = text ? Buffer.from(String(message.body), 'base64') : toBytes(message.body);
        const view = Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
        opened = unpack(sealer.open(String(kid), view, context));
      } catch (error) {
        return { refused: error.reason ?? 'open' };
      }
      if (opened === null) return { refused: 'format' };
      return { headers: opened.headers, body: opened.body, sealed: true };
    },
  };
};

module.exports = { createBrokerSealing, HEADER_SEALED };
