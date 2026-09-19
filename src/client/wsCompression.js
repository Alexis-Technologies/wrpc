'use strict';

// The Node half of the WebSocket client's per-message compression: Node's
// built-in WebSocket only ever inflates, so a Node client's uploads arrive
// as they are whatever the server negotiated. With `compression` on, the
// client offers its codec in a ping right at open, and once the server's
// pong names the same one, every packet or chunk past the threshold leaves
// as a binary frame under the 0x00 marker (src/wire.js) the server
// inflates. The browser half (wsCompression.browser.js, swapped through
// package.json#browser) is a stub: a browser compresses both directions
// itself under permessage-deflate, and its bundle carries none of this.

const { normalizeSyncCompression, negotiate, encodeIfSmaller } = require('../compression/sync.js');
const { FRAME_MARK, FRAME_PACKET_DEFLATE, FRAME_CHUNK_DEFLATE } = require('../wire.js');
const { jsonParse } = require('../utils.js');

const PONG_PREFIX = '{"type":"pong"';

/**
 * `{ offer, accept(text), encode(data) }` over the option, or null for off:
 * `offer` is the ping to send on open; `accept` reads a pong and answers
 * whether the server agreed (call it until it has answered once — a plain
 * pong is a no); `encode` frames a packet (string) or chunk (bytes) past
 * the threshold, or answers null to send it as it is.
 */
const createWsCompression = (option) => {
  const local = normalizeSyncCompression(option, 'ws transport: options');
  if (local === null) return null;
  let active = null;
  return {
    id: local.id,
    offer: JSON.stringify({ type: 'ping', enc: local.id }),
    get active() {
      return active !== null;
    },
    accept(text) {
      if (typeof text !== 'string' || !text.startsWith(PONG_PREFIX)) return null;
      const packet = jsonParse(text);
      if (!packet || packet.type !== 'pong') return null;
      active = negotiate(local, packet.enc);
      return active !== null;
    },
    encode(data) {
      if (active === null) return null;
      const out = encodeIfSmaller(active, data);
      if (out === null) return null;
      const frame = Buffer.allocUnsafe(2 + out.length);
      frame[0] = FRAME_MARK;
      frame[1] = typeof data === 'string' ? FRAME_PACKET_DEFLATE : FRAME_CHUNK_DEFLATE;
      frame.set(out, 2);
      return frame;
    },
  };
};

module.exports = { createWsCompression };
