'use strict';

// Names and ids at the broker boundary. Two jobs, both security-relevant:
//
// 1. Channel, queue and topic names are application strings (a room is any
//    non-empty string), and every broker has its own alphabet. Worse, some
//    alphabets have METACHARACTERS: NATS accepts `room:*` as a subject and
//    subscribes to every room (tests in the phase-0 spike). encodeToken maps
//    an arbitrary name into one safe token, reversibly and injectively.
//
// 2. A subscription's `lastEventId` comes back from the peer. signId/openId
//    let a feed refuse ids it never issued (a client asking to replay a
//    whole topic from offset 0, or an offset it made up).

const { createHmac, createHash, timingSafeEqual } = require('node:crypto');

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// Coded like the router's errors: `expose` because the message is written
// for the caller.
const codedError = (message, code) => {
  const error = new Error(message);
  error.code = code;
  error.expose = true;
  return error;
};

const toText = (body) => {
  if (typeof body === 'string') return body;
  if (body instanceof Uint8Array) return decoder.decode(body);
  if (ArrayBuffer.isView(body)) return decoder.decode(new Uint8Array(body.buffer, body.byteOffset, body.byteLength));
  if (body instanceof ArrayBuffer) return decoder.decode(new Uint8Array(body));
  return body === undefined || body === null ? '' : String(body);
};

const toBytes = (body) => {
  if (body instanceof Uint8Array) return body;
  if (typeof body === 'string') return encoder.encode(body);
  if (ArrayBuffer.isView(body)) return new Uint8Array(body.buffer, body.byteOffset, body.byteLength);
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  return encoder.encode(body === undefined || body === null ? '' : String(body));
};

// Headers normalized to a null-prototype string map: brokers hand back
// Buffers (Kafka), arrays (AMQP tables) or their own header objects, and a
// peer-controlled key like `__proto__` must stay a plain entry.
const toHeaders = (value) => {
  const headers = Object.create(null);
  if (!value || typeof value !== 'object') return headers;
  for (const key of Object.keys(value)) {
    const entry = value[key];
    if (entry === undefined || entry === null) continue;
    headers[key] = typeof entry === 'string' ? entry : toText(Array.isArray(entry) ? entry[0] : entry);
  }
  return headers;
};

const HEX = '0123456789ABCDEF';

/**
 * Encodes `name` into a single token of the alphabet `safe` (a RegExp that
 * tests ONE character), escaping every other UTF-8 byte as `<escape>XX`.
 * The escape character itself is always escaped, which is what makes the
 * mapping injective. Past `maxLength` characters the token is shortened to
 * a prefix plus a SHA-256 digest — still injective in practice, and within
 * the broker's length limit (AMQP routing keys: 255 bytes, Kafka topics:
 * 249 characters).
 */
const encodeToken = (name, { safe = /[A-Za-z0-9_-]/, escape = '~', maxLength = 200 } = {}) => {
  const text = String(name);
  let out = '';
  for (const char of text) {
    if (char !== escape && char.length === 1 && safe.test(char)) {
      out += char;
      continue;
    }
    for (const byte of encoder.encode(char)) out += `${escape}${HEX[byte >> 4]}${HEX[byte & 15]}`;
  }
  if (out.length <= maxLength) return out;
  const digest = createHash('sha256')
    .update(text)
    .digest('base64url')
    .replace(/[^A-Za-z0-9]/g, '');
  const keep = Math.max(0, maxLength - digest.length - 2);
  return `${out.slice(0, keep)}${escape}${escape}${digest}`.slice(0, maxLength);
};

// Signed ids: `<id>!<mac>`. The mac is base64url (no `!`), so the split on
// the LAST `!` is unambiguous whatever the id itself contains.
const SIGN_SEPARATOR = '!';

const mac = (secret, id) => createHmac('sha256', secret).update(id).digest('base64url').slice(0, 32);

const signId = (secret, id) => `${id}${SIGN_SEPARATOR}${mac(secret, id)}`;

/** The id inside a signed one, or null when the signature does not verify. */
const openId = (secret, signed) => {
  if (typeof signed !== 'string') return null;
  const at = signed.lastIndexOf(SIGN_SEPARATOR);
  if (at <= 0) return null;
  const id = signed.slice(0, at);
  const given = Buffer.from(signed.slice(at + 1));
  const expected = Buffer.from(mac(secret, id));
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) return null;
  return id;
};

module.exports = { codedError, toText, toBytes, toHeaders, encodeToken, signId, openId };
