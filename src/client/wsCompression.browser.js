'use strict';

// The browser half of the WebSocket client's per-message compression: a
// stub. A browser's WebSocket compresses both directions itself under
// permessage-deflate once the server enables it, so `compression` on the
// ws transport has nothing to add here — and the bundle carries none of
// the Node half (wsCompression.js).

const createWsCompression = () => null;

module.exports = { createWsCompression };
