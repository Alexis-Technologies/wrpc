'use strict';

const zlib = require('node:zlib');

const EXTENSION_NAME = 'permessage-deflate';
const TRAILER = Buffer.from([0x00, 0x00, 0xff, 0xff]);
const DEFAULT_THRESHOLD = 1024;
// The threadpool hand-off of zlib's async API costs a fixed amount per
// message: sequential throughput is a quarter of the sync path at 4 KB, a
// half at 32 KB and level at 256 KB (bench/deflate-context.js) — so below
// this a synchronous deflate is cheaper, and above it the hand-off is free
// and the event loop is the thing being bought.
const DEFAULT_ASYNC_THRESHOLD = 256 * 1024;
// zlib cannot produce a true 8-bit raw-deflate window, so an offer
// demanding server_max_window_bits=8 cannot be honored and is skipped.
const MIN_WINDOW_BITS = 9;
const MAX_WINDOW_BITS = 15;

const KNOWN_PARAMS = new Set([
  'server_no_context_takeover',
  'client_no_context_takeover',
  'server_max_window_bits',
  'client_max_window_bits',
]);

const parseParam = (token, offer) => {
  const eq = token.indexOf('=');
  const key = (eq < 0 ? token : token.slice(0, eq)).trim();
  if (!key) return false;
  let value = true;
  if (eq >= 0) {
    value = token.slice(eq + 1).trim();
    if (value.startsWith('"') && value.endsWith('"')) value = value.slice(1, -1);
    if (!value) return false;
  }
  // A duplicate param is grammar-valid but makes the offer unacceptable —
  // RFC 7692 7 requires declining it, not failing the handshake
  if (offer.params.has(key)) offer.valid = false;
  else offer.params.set(key, value);
  return true;
};

// Parses a Sec-WebSocket-Extensions header value into
// [{ name, params: Map<name, value | true>, valid }]. Returns null only on
// true grammar violations (RFC 6455 9.1) — the caller must then fail the
// handshake rather than silently ignore the header.
const parseExtensions = (header) => {
  const offers = [];
  for (const part of header.split(',')) {
    const tokens = part.split(';');
    const name = tokens[0].trim();
    if (!name) return null;
    const offer = { name, params: new Map(), valid: true };
    for (let i = 1; i < tokens.length; i++) {
      if (!parseParam(tokens[i], offer)) return null;
    }
    offers.push(offer);
  }
  return offers;
};

const parseWindowBits = (value) => {
  if (!/^\d{1,2}$/.test(value)) return null;
  const bits = Number(value);
  if (bits < 8 || bits > MAX_WINDOW_BITS) return null;
  return bits;
};

// `contextTakeover`: false (default) pins both directions to no context
// takeover; 'server' keeps a deflate context for what WE send, 'client'
// keeps an inflate context for what the peer sends, true both. A peer's
// own `*_no_context_takeover` request is always honoured (RFC 7692
// 7.1.1.1), so the ACCEPTED state can be narrower than the option.
const takeoverOf = (options) => {
  const value = options.contextTakeover;
  if (value === true) return { server: true, client: true };
  if (value === 'server') return { server: true, client: false };
  if (value === 'client') return { server: false, client: true };
  return { server: false, client: false };
};

// `async: { threshold }` moves the deflate/inflate of messages at or over
// `threshold` bytes off the event loop (zlib's threadpool API); null keeps
// every message synchronous.
const asyncOf = (options) => {
  const value = options.async;
  if (!value || typeof value !== 'object') return null;
  const threshold = value.threshold;
  return { threshold: typeof threshold === 'number' && threshold > 0 ? threshold : DEFAULT_ASYNC_THRESHOLD };
};

const acceptOffer = (offer, options) => {
  if (!offer.valid) return null;
  let windowBits = MAX_WINDOW_BITS;
  for (const [key, value] of offer.params) {
    if (!KNOWN_PARAMS.has(key)) return null;
    if (key === 'server_max_window_bits') {
      if (value === true) return null; // value is required for this param
      const bits = parseWindowBits(value);
      if (bits === null || bits < MIN_WINDOW_BITS) return null;
      windowBits = bits;
    } else if (key === 'client_max_window_bits' && value !== true) {
      if (parseWindowBits(value) === null) return null;
    }
  }
  const takeover = takeoverOf(options);
  const serverTakeover = takeover.server && !offer.params.has('server_no_context_takeover');
  const clientTakeover = takeover.client && !offer.params.has('client_no_context_takeover');
  const response = [EXTENSION_NAME];
  if (!serverTakeover) response.push('server_no_context_takeover');
  if (!clientTakeover) response.push('client_no_context_takeover');
  if (offer.params.has('server_max_window_bits')) {
    response.push(`server_max_window_bits=${windowBits}`);
  }
  return {
    response: response.join('; '),
    threshold: options.threshold ?? DEFAULT_THRESHOLD,
    windowBits,
    serverTakeover,
    clientTakeover,
    level: options.level,
    memLevel: options.memLevel,
    async: asyncOf(options),
  };
};

// Server-side negotiation: accepts the first honorable permessage-deflate
// offer. By default both directions are pinned to no context takeover, so
// every message is a self-contained deflate stream — compression state
// never spans messages, one-shot zlib calls suffice, and the compressed
// bytes are a function of (payload, window) alone (what lets a fan-out
// share one frame). `contextTakeover` opts a direction into a live stream
// per connection (deflateContext.js).
// Returns the accepted params, null to decline, or { malformed: true }
// when the header violates the grammar — the handshake must then fail
// with 400 (RFC 6455 4.2.1), not proceed without the extension.
const negotiate = (header, options = {}) => {
  if (!header) return null;
  const offers = parseExtensions(header);
  if (!offers) return { malformed: true };
  for (const offer of offers) {
    if (offer.name !== EXTENSION_NAME) continue;
    const accepted = acceptOffer(offer, options);
    if (accepted) return accepted;
  }
  return null;
};

const compress = (payload, windowBits = MAX_WINDOW_BITS) => {
  const compressed = zlib.deflateRawSync(payload, {
    windowBits,
    finishFlush: zlib.constants.Z_SYNC_FLUSH,
  });
  // RFC 7692 7.2.1: strip the trailing empty block (00 00 ff ff)
  return compressed.subarray(0, compressed.length - TRAILER.length);
};

// finishFlush Z_SYNC_FLUSH: the stream has no final block — it ends at the
// flush point left by the peer's compressor (RFC 7692 7.2.2).
const decompress = (payload, maxLength) =>
  zlib.inflateRawSync(Buffer.concat([payload, TRAILER]), {
    windowBits: MAX_WINDOW_BITS,
    maxOutputLength: maxLength,
    finishFlush: zlib.constants.Z_SYNC_FLUSH,
  });

// The same two, off the event loop: zlib's callback API runs in libuv's
// threadpool. Same bytes as the sync pair for the same input.
const compressAsync = (payload, windowBits, cb) => {
  zlib.deflateRaw(payload, { windowBits, finishFlush: zlib.constants.Z_SYNC_FLUSH }, (error, compressed) => {
    if (error) return void cb(error);
    cb(null, compressed.subarray(0, compressed.length - TRAILER.length));
  });
};

const decompressAsync = (payload, maxLength, cb) => {
  zlib.inflateRaw(
    Buffer.concat([payload, TRAILER]),
    { windowBits: MAX_WINDOW_BITS, maxOutputLength: maxLength, finishFlush: zlib.constants.Z_SYNC_FLUSH },
    cb,
  );
};

module.exports = {
  EXTENSION_NAME,
  DEFAULT_THRESHOLD,
  DEFAULT_ASYNC_THRESHOLD,
  MIN_WINDOW_BITS,
  MAX_WINDOW_BITS,
  TRAILER,
  parseExtensions,
  negotiate,
  compress,
  decompress,
  compressAsync,
  decompressAsync,
};
