'use strict';

// The ws handshake of a client whose WebSocket constructor takes no headers
// — a browser, by specification; `fetch` refuses to perform the upgrade by
// hand, and no other API reaches the request. What a page DOES control is
// the subprotocol offer, a real handshake header: the declared bags ride it
// as `wrpc.h.<base64url>` / `wrpc.m.<base64url>` next to the revision, and
// the connect URL — which lands in proxy access logs — stays clean. The
// query carrier remains for `carrier: 'query'` (an intermediary that mangles
// Sec-WebSocket-Protocol) and for an empty offer. wsHandshake.js is the Node
// half, which sends real headers and falls back to this one.

const { connectUrl, META_MAX } = require('./core.js');
const { HEADERS_PROTOCOL, META_PROTOCOL, BEARER_PROTOCOL } = require('../wire.js');

// An offer is an RFC 7230 token: no raw JSON, no base64 '=' padding.
const TOKEN = /^[!#$%&'*+.^_`|~A-Za-z0-9-]+$/;

// base64url of the UTF-8 text. The spread is bounded: the caller refuses a
// text longer than META_MAX before it gets here.
const encode = (text) =>
  btoa(String.fromCharCode(...new TextEncoder().encode(text)))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');

const openSocket = (WebSocket, url, protocols, options, log) => {
  let headers = options.headers ?? null;
  let meta = options.meta ?? null;
  let offer = protocols;
  // Carrier tokens go only INTO an offer the server can answer: it never
  // echoes one, and a client fails a handshake whose every offer went
  // unanswered (Chrome closes 1006) — so `protocols: []`, the escape hatch
  // for a proxy that mangles the header, means the query for everything.
  if (offer.length > 0) {
    // A Bearer credential rides bare, `wrpc.bearer.<token>`: shorter than
    // its base64 and outside the budget (a JWT alone can outgrow it). The
    // server's bearer transport reads it from the offer.
    const auth = headers?.authorization;
    const bearer = typeof auth === 'string' && auth.startsWith('Bearer ') ? auth.slice(7) : '';
    if (TOKEN.test(bearer)) {
      offer = [...offer, BEARER_PROTOCOL + bearer];
      headers = { ...headers };
      delete headers.authorization;
      if (Object.keys(headers).length === 0) headers = null;
    }
    if (options.carrier !== 'query') {
      // ONE budget for both tokens, spent headers first — the server
      // measures them the same way, and it keeps the handshake clear of the
      // smallest header limit around (uWebSockets.js: 4096 bytes for ALL
      // request headers). A refused bag leaves the connection unlabelled and
      // says so, instead of failing a handshake the application cannot see.
      let budget = META_MAX;
      for (const [prefix, bag] of [
        [HEADERS_PROTOCOL, headers],
        [META_PROTOCOL, meta],
      ]) {
        if (!bag) continue;
        // Measured twice: the text first, because base64 only grows it and
        // an oversize bag is not worth encoding (nor safe to spread).
        const text = JSON.stringify(bag);
        const token = text.length > budget ? null : prefix + encode(text);
        if (token === null || token.length > budget) {
          log?.warn({ event: 'meta.oversize', carrier: 'protocol', bytes: (token ?? text).length });
          continue;
        }
        budget -= token.length;
        offer = [...offer, token];
      }
      headers = meta = null;
    }
  }
  // Loud on purpose: the connect URL is what access logs keep.
  if (headers?.authorization) log?.warn({ event: 'declared.exposed', key: 'authorization', carrier: 'query' });
  const target = connectUrl(url, headers, meta, log);
  return offer.length > 0 ? new WebSocket(target, offer) : new WebSocket(target);
};

module.exports = { openSocket };
