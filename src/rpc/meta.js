'use strict';

// The connection-metadata parser: the connection-phase `meta` bag in every
// spelling it travels under. A self-contained pure module the core, and
// nothing else, feeds raw requests through; the token transports receive its
// OUTPUT (see sessions.js), never re-run it. Buffer-free: it is bundled into
// the WebRTC browser entry through PeerHost — the declared HEADERS bag and
// the base64url subprotocol carrier live in the Node-only rpc/handshake.js.

const { jsonParse, toKebab } = require('../utils.js');
const { META_PARAM, META_HEADER, META_PREFIX } = require('../wire.js');
const { sanitizeMeta } = require('./dispatcher.js');

const DEFAULT_META_MAX = 2048;

// The connection-phase half of `meta` — the x-wrpc-meta request header
// (http/sse; percent-encoded JSON, since header values must stay latin-1),
// its per-key x-wrpc-meta-<name> spelling, or the wrpc_meta connect-URL
// parameter (the ws query carrier, WebTransport), or the `wrpc.m.` subprotocol
// offer of a browser WebSocket, which arrives here ALREADY DECODED as `carried`
// (see rpc/handshake.js); the wire names themselves live in ../wire.js. Same
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
// `carried` is `undefined` when no `wrpc.m.` token was offered, else its JSON
// text or `null` for a refused one. A carrier is chosen, never merged: the
// real header first (only a non-browser peer can send it), then the offer,
// and the query only when neither was used.
const declaredData = (headers, query, limit, log, carried) => {
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
  } else if (carried !== undefined) {
    raw = carried;
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

module.exports = { DEFAULT_META_MAX, declaredData };
