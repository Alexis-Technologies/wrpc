'use strict';

// The connection-metadata parser: the declared-headers bag (the ws wrpc_h
// connect-URL parameter) and the connection-phase `meta` bag in both its
// spellings. Split out of rpc/core.js — this is a self-contained pure
// module the core, and nothing else, feeds raw requests through; the token
// transports receive its OUTPUT (see sessions.js), never re-run it.

const { jsonParse, toKebab } = require('../utils.js');
const { HEADERS_PARAM, META_PARAM, META_HEADER, META_PREFIX } = require('../wire.js');
const { sanitizeMeta, split } = require('./dispatcher.js');

// Connection-phase headers on the ws leg: the WHATWG WebSocket constructor
// cannot set real upgrade headers, so the client carries declared ones as
// ONE query parameter on the connect URL. PEER-CONTROLLED, so every step
// below is a refusal rather than a throw — an oversize or malformed label
// leaves the connection with no label, never without a connection. Observed
// upgrade headers always win the merge: the query can only ADD names the
// request did not carry, and the reserved names it could spoof are dropped
// outright (on http/sse, fetch itself refuses to send them, so the deny
// list exists exactly for this query path).
const DEFAULT_META_MAX = 2048;
const RESERVED_DECLARED = /^(?:cookie|host|origin)$|^(?:sec-|content-|proxy-|x-wrpc-)/;

const declaredHeaders = (url, limit, log) => {
  const query = split(url ?? '', '?')[1];
  if (!query) return null;
  // Capped on the ENCODED length, before any decoding work — that is the
  // string the peer actually controls.
  if (query.length > limit) {
    log.warn({ event: 'meta.oversize', bytes: query.length });
    return null;
  }
  const raw = new URLSearchParams(query).get(HEADERS_PARAM);
  if (!raw) return null;
  const value = jsonParse(raw);
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  let declared = null;
  for (const key of Object.keys(value)) {
    if (typeof value[key] !== 'string') continue; // a flat string map only
    // Kebab-cased: node lowercases observed header names, and schema.headers
    // validation must see one casing convention, not two. Kebab rather than a
    // bare lowercase because `xAppVersion` would otherwise land as
    // `xappversion` — a key nobody would write in a schema.
    // Tested AFTER the transform: toKebab can only lowercase and insert
    // hyphens, so it can never turn a permitted name into a reserved one, but
    // the honest order is to check the name that will actually be kept.
    const name = toKebab(key);
    if (name === '__proto__' || RESERVED_DECLARED.test(name)) continue;
    (declared ??= { __proto__: null })[name] = value[key];
  }
  return declared;
};

// The connection-phase half of `meta` — the x-wrpc-meta request header
// (http/sse; percent-encoded JSON, since header values must stay latin-1),
// its per-key x-wrpc-meta-<name> spelling, or the wrpc_meta connect-URL
// parameter (ws); the wire names themselves live in ../wire.js. Same
// sanitizer as the per-packet field, same refusal-not-throw discipline.
// Unlike headers this bag is deliberately outside schema validation: it is
// a label for cross-cutting hooks, not procedure input.

// The prefixed spelling — the S3 x-amz-meta-* idiom: `x-wrpc-meta-idem: 9f3c`
// is a header a human can type and a gateway can inject, strip or route on,
// where the percent-encoded JSON one is not. wrpc's own client emits it when
// asked (`metaFormat: 'prefixed'`); an external HTTP caller can always use it.
// Values stay STRINGS — the same by-design semantics as REST query args — so
// the canonical JSON header remains the type-faithful form and wins a key
// collision. Names are kebab-normalized to match what the client sends, but
// see toKebab: HTTP has already lowercased them, so a caller who writes
// `x-wrpc-meta-userId` by hand gets `userid` and no transform can undo it.
const prefixedData = (headers, limit) => {
  let data = null;
  let bytes = 0;
  for (const key in headers) {
    if (!key.startsWith(META_PREFIX)) continue;
    const name = key.slice(META_PREFIX.length);
    // A duplicated header arrives as an array — skipped, refusal-style,
    // like every other malformed label; '__proto__' never carries over.
    if (name.length === 0 || name === '__proto__' || typeof headers[key] !== 'string') continue;
    bytes += name.length + headers[key].length;
    if (bytes > limit) return null;
    (data ??= { __proto__: null })[toKebab(name)] = headers[key];
  }
  return data;
};

// Copies `source` onto `target` under normalized names. `target` is always a
// bag this function's caller just built, never one it was handed, so writing
// through it is safe; '__proto__' is skipped before it can be a key at all.
const kebabKeys = (source, target) => {
  for (const key in source) {
    if (key === '__proto__') continue;
    target[toKebab(key)] = source[key];
  }
  return target;
};

// `query` is the request's ALREADY-SPLIT query text: every HTTP caller has
// it from handleHttpCall's one split, so this function re-splitting the URL
// was pure rework — and the URLSearchParams parse behind it is gated on a
// substring probe, because the common REST/curl request carries no
// wrpc_meta at all (bench/meta.js measures both).
const declaredData = (headers, query, limit, log) => {
  const prefixed = headers ? prefixedData(headers, limit) : null;
  let raw = null;
  const header = headers?.[META_HEADER];
  if (typeof header === 'string' && header.length > 0) {
    if (header.length > limit) {
      log.warn({ event: 'meta.oversize', bytes: header.length });
      return sanitizeMeta(prefixed, limit);
    }
    try {
      raw = decodeURIComponent(header);
    } catch {
      return sanitizeMeta(prefixed, limit);
    }
  } else if (query && query.length <= limit && query.includes(META_PARAM)) {
    raw = new URLSearchParams(query).get(META_PARAM);
  }
  const declared = raw ? jsonParse(raw) : null;
  const canonical = typeof declared === 'object' && declared !== null && !Array.isArray(declared) ? declared : null;
  if (!canonical) return sanitizeMeta(prefixed, limit);
  // Normalized as it merges, so the two spellings reduce to the SAME key and
  // the collision is real: without this, `x-wrpc-meta: {"userId":1}` and
  // `x-wrpc-meta-user-id: 2` would sit side by side as lookalike keys and
  // nothing would win. Prefixed is the (already-kebab) seed, canonical
  // overwrites it — the JSON header is the type-faithful form, so it wins.
  return sanitizeMeta(kebabKeys(canonical, prefixed ?? { __proto__: null }), limit);
};

// The REST leg's trace context arrives as REAL HTTP headers (the client's
// #restCall injects traceparent/tracestate — there is no packet on the wire
// to carry tp/ts). Mapped onto the synthetic packet's tp/ts fields, the
// ordinary extract path in the telemetry writer picks them up, so a REST

module.exports = { DEFAULT_META_MAX, RESERVED_DECLARED, declaredHeaders, declaredData };
