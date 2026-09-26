'use strict';

// What a peer DECLARED on a persistent handshake, from every carrier it may
// have used: the subprotocol offers of a browser WebSocket (`wrpc.h.<b64u>`,
// `wrpc.m.<b64u>`), the connect-URL parameters (`wrpc_h`, `wrpc_meta` — the
// `carrier: 'query'` opt-out, and WebTransport, which has neither headers nor
// subprotocols), and the real `x-wrpc-meta` headers a Node client sends.
// Node-only on purpose: the offers are base64url and rpc/meta.js, which this
// file feeds, is bundled into the WebRTC browser entry where no Buffer exists.
// The core and an application's `verifyClient` gate call the SAME function,
// so what a gate saw is what the connection gets.

const { jsonParse, toKebab } = require('../utils.js');
const { HEADERS_PARAM, HEADERS_PROTOCOL, META_PROTOCOL, CARRIER_PROTOCOL } = require('../wire.js');
const { split } = require('./dispatcher.js');
const { DEFAULT_META_MAX, declaredData } = require('./meta.js');

const PROTOCOL_HEADER = 'sec-websocket-protocol';

// PEER-CONTROLLED, whichever carrier brought it, so every step below is a
// refusal rather than a throw — an oversize or malformed label leaves the
// connection with no label, never without a connection. Observed upgrade
// headers always win the merge: a declaration can only ADD names the request
// did not carry, and the names it could spoof are dropped outright. The list
// is about a hostile PAGE, not a hostile process — a non-browser peer sets
// any real header it likes — and a page controls exactly the URL and the
// offers while the victim's cookie rides along by itself: hence the ambient
// credentials, the fetch-metadata and wrpc namespaces, and every name a
// deployment reads the caller's address from when no proxy has set it.
// The deny list lives in reserved.js, beside the sealed request's.
const { RESERVED_DECLARED } = require('./reserved.js');

const NO_LOG = { warn() {} };

// One sanitizer for both declared-header sources: a flat string map under
// kebab names — node lowercases observed header names, and schema.headers
// validation must see one casing convention, not two (kebab rather than a
// bare lowercase because `xAppVersion` would otherwise land as
// `xappversion`, a key nobody would write in a schema). The deny list is
// tested AFTER the transform: toKebab can only lowercase and insert hyphens,
// so it cannot turn a permitted name into a reserved one, but the honest
// order is to check the name that will actually be kept.
const sanitizeDeclared = (value) => {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  let declared = null;
  for (const key of Object.keys(value)) {
    if (typeof value[key] !== 'string') continue;
    const name = toKebab(key);
    if (name === '__proto__' || RESERVED_DECLARED.test(name)) continue;
    (declared ??= { __proto__: null })[name] = value[key];
  }
  return declared;
};

// The offers, split once. `headers`/`meta` are `undefined` when that token was
// not offered, `null` when it was and is refused, else the JSON text. ONE
// budget covers both tokens, measured on the encoded text — the string the
// peer controls — before any decoding work; the client spends it the same
// way (headers first), and it keeps a handshake clear of the smallest header
// limit in the fleet (uWebSockets.js: 4096 bytes for ALL request headers).
// `rest` is the offer an application may see: carrier tokens removed.
const carriedBags = (offer, limit, log) => {
  if (typeof offer !== 'string' || !offer.includes('wrpc.')) return null;
  const carried = { headers: undefined, meta: undefined, rest: [] };
  let budget = limit;
  for (const part of offer.split(',')) {
    const token = part.trim();
    if (token.length === 0) continue;
    if (!CARRIER_PROTOCOL.test(token)) {
      carried.rest.push(token);
      continue;
    }
    const field = token.startsWith(HEADERS_PROTOCOL) ? 'headers' : token.startsWith(META_PROTOCOL) ? 'meta' : null;
    // The first token of a kind wins; a Bearer token is the auth strategy's.
    if (field === null || carried[field] !== undefined) continue;
    if (token.length > budget) {
      log.warn({ event: 'meta.oversize', carrier: 'protocol', bytes: token.length });
      carried[field] = null;
      continue;
    }
    budget -= token.length;
    // Lenient by design: Buffer decodes what it can of a malformed token,
    // and the garbage then fails jsonParse — the intended refusal.
    const prefix = field === 'headers' ? HEADERS_PROTOCOL : META_PROTOCOL;
    carried[field] = Buffer.from(token.slice(prefix.length), 'base64url').toString('utf8');
  }
  return carried;
};

const queryHeaders = (query, limit, log) => {
  if (!query) return null;
  // Capped on the ENCODED length, before any decoding work.
  if (query.length > limit) {
    log.warn({ event: 'meta.oversize', carrier: 'query', bytes: query.length });
    return null;
  }
  const raw = new URLSearchParams(query).get(HEADERS_PARAM);
  return raw ? sanitizeDeclared(jsonParse(raw)) : null;
};

// `{ headers, meta }` — the header bag a procedure will see (declared names
// UNDER the observed ones) and the sanitized connection-metadata bag. A
// carrier is chosen, never merged: an offered token, valid or refused, means
// the query is not consulted for that bag. The carrier tokens are taken out
// of the `sec-websocket-protocol` the application sees — the bag is what
// handlers log, and a Bearer credential has no business sitting in it twice.
const readDeclared = (observed, url, limit, log) => {
  const carried = carriedBags(observed?.[PROTOCOL_HEADER], limit, log);
  const query = split(url ?? '', '?')[1];
  const declared =
    carried === null || carried.headers === undefined
      ? queryHeaders(query, limit, log)
      : sanitizeDeclared(jsonParse(carried.headers));
  const meta = declaredData(observed, query, limit, log, carried === null ? undefined : carried.meta);
  if (carried === null && declared === null) return { headers: observed, meta };
  const headers = { ...declared, ...observed };
  if (carried !== null) {
    if (carried.rest.length > 0) headers[PROTOCOL_HEADER] = carried.rest.join(', ');
    else delete headers[PROTOCOL_HEADER];
  }
  return { headers, meta };
};

/**
 * What the peer declared on an upgrade request — for a `verifyClient` gate,
 * which runs before any `Client` exists. The same read `attachSocket` does.
 */
const readHandshake = (req, { metaMaxBytes = DEFAULT_META_MAX, log = NO_LOG } = {}) => {
  const { headers, meta } = readDeclared(req?.headers ?? {}, req?.url, metaMaxBytes, log);
  return { headers, meta: meta ?? {} };
};

module.exports = { RESERVED_DECLARED, sanitizeDeclared, carriedBags, readDeclared, readHandshake };
