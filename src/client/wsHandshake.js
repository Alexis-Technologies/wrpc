'use strict';

// The ws handshake of a Node client: REAL request headers. The built-in
// WebSocket (undici) takes an init bag — `new WebSocket(url, { protocols,
// headers })` — so the declared headers travel as themselves and the declared
// meta as `x-wrpc-meta`, exactly as on http and sse: no query, no carrier
// token, no Bearer lift, and nothing for the server to decode. The browser
// half (swapped in through package.json#browser) holds the carriers a page
// is left with, and this half falls back to it.

const { metaHeaders, META_MAX } = require('./core.js');
const carriers = require('./wsHandshake.browser.js');

// Names the handshake itself owns: declared, they would corrupt it.
const UNSENDABLE = /^(?:host|connection|upgrade|content-length|transfer-encoding)$|^sec-websocket-/;

const openSocket = (WebSocket, url, protocols, options, log) => {
  if ((options.carrier ?? 'auto') !== 'auto') return carriers.openSocket(WebSocket, url, protocols, options, log);
  let headers = null;
  for (const name in options.headers) {
    if (UNSENDABLE.test(name)) log?.warn({ event: 'declared.unsendable', key: name });
    else (headers ??= {})[name] = options.headers[name];
  }
  if (options.meta) {
    // Capped like the http leg (#requestMeta): past metaMaxBytes the server
    // drops the header, a silent loss on the side that cannot see it.
    const block = metaHeaders(options.meta, options.metaPrefixed === true);
    let bytes = 0;
    for (const key in block) bytes += key.length + String(block[key]).length;
    if (bytes > META_MAX) log?.warn({ event: 'meta.oversize', bytes });
    else headers = { ...headers, ...block };
  }
  if (headers === null) return carriers.openSocket(WebSocket, url, protocols, {}, log);
  try {
    return new WebSocket(url, { protocols, headers });
  } catch (error) {
    // A runtime whose WebSocket has no init bag reads it as a subprotocol and
    // throws (Deno), and undici throws on a name or value it will not send:
    // either way the carriers still work, and a second throw is the caller's.
    // The error's name only: undici repeats the offending header's VALUE
    // in its message, and a bearer token is not for the log.
    log?.warn({ event: 'handshake.fallback', reason: error?.name ?? 'Error' });
    return carriers.openSocket(WebSocket, url, protocols, options, log);
  }
};

module.exports = { openSocket };
