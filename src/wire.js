'use strict';

// The wire names of the connection-metadata carriers, in ONE place. The
// server parsers (rpc/meta, rpc/handshake), the client emitter (client/core),
// the ws handshake builders (client/wsHandshake*), the two subprotocol
// negotiators (websocket/server, adapters/uws), the CORS allowlist
// (transport) and the auth strategies (auth) all speak them — and a spelling
// change used to be a multi-file grep with nothing to catch a miss.
// Zero-require beyond sse/constants.js (itself import-free) and
// browser-safe by construction: esbuild inlines these strings, so pulling
// this file into a budgeted entry costs nothing.

const { CHANNEL_HEADER } = require('./sse/constants.js');

// The connect-URL query parameters carrying the declared bags on a browser
// WebSocket, whose constructor cannot set real headers: the headers bag and
// the connection-phase meta bag, each one percent-encoded JSON value.
const HEADERS_PARAM = 'wrpc_h';
const META_PARAM = 'wrpc_meta';

// The protocol revision a wrpc client OFFERS as a WebSocket subprotocol and
// the server echoes (protocol.md#versioning).
const WRPC_PROTOCOL = 'wrpc.v1';

// The same two bags, and the Bearer credential, as subprotocol OFFERS:
// Sec-WebSocket-Protocol is the one handshake header a page controls, so on
// a browser WebSocket the bags ride it instead of the URL — base64url JSON
// without padding, because an offer is an RFC 7230 token. Carrier tokens are
// read, never selected: the negotiators drop them from the offer before an
// application sees it, and refuse to echo one (a credential would come back
// in the response headers).
const HEADERS_PROTOCOL = 'wrpc.h.';
const META_PROTOCOL = 'wrpc.m.';
const BEARER_PROTOCOL = 'wrpc.bearer.';
const CARRIER_PROTOCOL = /^wrpc\.(?:h|m|bearer)\./;

// The canonical single meta header (percent-encoded JSON, type-faithful,
// one CORS entry) and the per-key x-amz-meta-style prefix spelling (string
// values, hand-typeable, one CORS entry per key).
const META_HEADER = 'x-wrpc-meta';
const META_PREFIX = 'x-wrpc-meta-';

// A binary WebSocket frame whose first byte is 0 is not a stream chunk (a
// chunk's first byte is its id length, at least 1) but a FRAMED MESSAGE,
// its second byte the kind: a packet or a chunk compressed with the codec
// negotiated on ping/pong (protocol.md#binary-chunks), or a packet whose
// byte values travel as binary attachments (src/attachments.js). Kind 2
// stays reserved for a binary packet codec, should one ever be needed.
const FRAME_MARK = 0;
const FRAME_ATTACHMENTS = 1;
const FRAME_PACKET_COMPRESSED = 3;
const FRAME_CHUNK_COMPRESSED = 4;

module.exports = {
  HEADERS_PARAM,
  META_PARAM,
  WRPC_PROTOCOL,
  HEADERS_PROTOCOL,
  META_PROTOCOL,
  BEARER_PROTOCOL,
  CARRIER_PROTOCOL,
  META_HEADER,
  META_PREFIX,
  CHANNEL_HEADER,
  FRAME_MARK,
  FRAME_ATTACHMENTS,
  FRAME_PACKET_COMPRESSED,
  FRAME_CHUNK_COMPRESSED,
};
