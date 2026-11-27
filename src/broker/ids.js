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

// A dead-letter reason is written into a broker header, and a header is a
// line: NATS refuses a value holding CR or LF outright (nats-core's
// validHeaderValue), so a handler error whose message spanned two lines — a
// validator listing its failures — made deadLetter() reject, the settlement
// never landed, and the message came back forever. One bounded line
// whatever came in: every run of control characters (CR/LF, the separators
// JSON does not escape, NUL) becomes a space.
// eslint-disable-next-line no-control-regex
const CONTROL = /[\x00-\x1f\x7f\u2028\u2029]+/g;

const HEX = '0123456789ABCDEF';

// encodeToken for the names an adapter is asked about on every message — a
// channel, a topic, a queue, an address — remembered. The encoding walks the
// name a character at a time and builds the token by concatenation: 0.2 µs
// for `orders`, 1.7 µs for `wrpc:rooms:tenant-42:chat/general`, 3 µs for a
// name outside ASCII, on every publish and every append; remembered, it is a
// Map lookup (bench/broker-token.js — a Redis log append over an in-process
// server went from 450,000 to 1.3 M a second on the second of those names).
// The names are the application's, and a room can be a peer's to name: the
// memo holds TOKEN_MEMO of them and starts over when it is full, and a name
// longer than TOKEN_MEMO_NAME is encoded every time.
const TOKEN_MEMO = 1024;
const TOKEN_MEMO_NAME = 256;

// Signed ids: `<id>!<mac>`. The mac is base64url (no `!`), so the split on
// the LAST `!` is unambiguous whatever the id itself contains.
const SIGN_SEPARATOR = '!';

// The MAC binds the id to its SCOPE — the topic the feed issued it for —
// under a versioned, length-prefixed layout (injective: no scope/id pair
// can spell another): one secret may serve every feed of a deployment
// without a token issued by one feed positioning a reader on another.
const MAC_PREFIX = 'wrpc feed v1\0';

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
  const keys = Object.keys(value);
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    const entry = value[key];
    if (entry === undefined || entry === null) continue;
    headers[key] = typeof entry === 'string' ? entry : toText(Array.isArray(entry) ? entry[0] : entry);
  }
  return headers;
};
const reasonText = (value, max = 512) => {
  const text = (value === undefined || value === null ? '' : String(value)).replace(CONTROL, ' ').trim();
  return text.length > max ? text.slice(0, max) : text;
};

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
    const bytes = encoder.encode(char);
    for (let i = 0; i < bytes.length; i++) out += `${escape}${HEX[bytes[i] >> 4]}${HEX[bytes[i] & 15]}`;
  }
  if (out.length <= maxLength) return out;
  const digest = createHash('sha256')
    .update(text)
    .digest('base64url')
    .replace(/[^A-Za-z0-9]/g, '');
  const keep = Math.max(0, maxLength - digest.length - 2);
  return `${out.slice(0, keep)}${escape}${escape}${digest}`.slice(0, maxLength);
};

const tokenEncoder = (options) => {
  let memo = new Map();
  return (name) => {
    if (typeof name !== 'string' || name.length > TOKEN_MEMO_NAME) return encodeToken(name, options);
    let token = memo.get(name);
    if (token !== undefined) return token;
    token = encodeToken(name, options);
    if (memo.size >= TOKEN_MEMO) memo = new Map();
    memo.set(name, token);
    return token;
  };
};

const mac = (secret, id, scope) =>
  createHmac('sha256', secret).update(`${MAC_PREFIX}${scope.length}:${scope}\0${id}`).digest('base64url').slice(0, 32);

const signId = (secret, id, scope = '') => `${id}${SIGN_SEPARATOR}${mac(secret, id, scope)}`;

/** The id inside a signed one, or null when the signature does not verify for `scope`. */
const openId = (secret, signed, scope = '') => {
  if (typeof signed !== 'string') return null;
  const at = signed.lastIndexOf(SIGN_SEPARATOR);
  if (at <= 0) return null;
  const id = signed.slice(0, at);
  const given = Buffer.from(signed.slice(at + 1));
  const expected = Buffer.from(mac(secret, id, scope));
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) return null;
  return id;
};

// A session id is a credential (whoever knows it may speak on the
// session): what a log line or a transport's `source` carries is this
// fingerprint of it — enough to correlate, useless to replay.
const fingerprint = (id) => createHash('sha256').update(`${id}`).digest('hex').slice(0, 12);

module.exports = {
  TOKEN_MEMO,
  toText,
  toBytes,
  toHeaders,
  reasonText,
  encodeToken,
  tokenEncoder,
  signId,
  openId,
  fingerprint,
};
