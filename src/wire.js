'use strict';

// The wire names of the connection-metadata carriers, in ONE place. Five
// modules speak them — the server parser (rpc/core), the client emitter
// (client/core), the ws connect-URL builder (client/transports), the CORS
// allowlist (transport) and the auth strategies (auth) — and a spelling
// change used to be a five-file grep with nothing to catch a miss.
// Zero-require beyond sse/constants.js (itself import-free) and
// browser-safe by construction: esbuild inlines these strings, so pulling
// this file into a budgeted entry costs nothing.

const { CHANNEL_HEADER } = require('./sse/constants.js');

// The connect-URL query parameters carrying the declared bags on a browser
// WebSocket, whose constructor cannot set real headers: the headers bag and
// the connection-phase meta bag, each one percent-encoded JSON value.
const HEADERS_PARAM = 'wrpc_h';
const META_PARAM = 'wrpc_meta';

// The canonical single meta header (percent-encoded JSON, type-faithful,
// one CORS entry) and the per-key x-amz-meta-style prefix spelling (string
// values, hand-typeable, one CORS entry per key).
const META_HEADER = 'x-wrpc-meta';
const META_PREFIX = 'x-wrpc-meta-';

module.exports = { HEADERS_PARAM, META_PARAM, META_HEADER, META_PREFIX, CHANNEL_HEADER };
