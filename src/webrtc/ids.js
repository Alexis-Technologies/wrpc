'use strict';

// Peer ids and instances become Map keys, session tokens, log fields and
// URLs on every peer, and a roster holds them by the hundred: bounded the
// same way everywhere — the signaling unit refuses a longer one, the
// client half and WrpcPeer ignore it. Require-free, so the browser bundle
// and the Node-only unit share this one number.

const MAX_ID_LENGTH = 256;

const isPeerId = (value, max = MAX_ID_LENGTH) => typeof value === 'string' && value.length > 0 && value.length <= max;

module.exports = { MAX_ID_LENGTH, isPeerId };
