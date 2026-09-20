'use strict';

// @alexify/wrpc/encryption on Node: the primitives of browser.js (with the
// synchronous node:crypto AEADs in place of crypto.subtle's), plus — as
// they land — what only a Node process does: the backplane and broker
// envelopes, the sealed session store, the server halves of the session
// handshake and of HPKE.

const { sealedStore } = require('./store.js');
const { createReplayCache } = require('./httpServer.js');

module.exports = { ...require('./browser.js'), sealedStore, createReplayCache };
