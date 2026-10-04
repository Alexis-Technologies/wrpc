'use strict';

// Peer ids and instances become Map keys, session tokens, log fields and
// URLs on every peer, and a roster holds them by the hundred: bounded the
// same way everywhere — the signaling unit refuses a longer one, the
// client half and WrpcPeer ignore it. Require-free, so the browser bundle
// and the Node-only unit share this one number — and the one other leaf
// helper the link layers share, `deferred`.

const MAX_ID_LENGTH = 256;

const isPeerId = (value, max = MAX_ID_LENGTH) => typeof value === 'string' && value.length > 0 && value.length <= max;

// A promise with its two handles, for what is settled from elsewhere (a
// link's open, a peer link's ready). Handled from birth: one that rejects
// with nobody waiting — a dial that fails before anyone asked — must not be
// an unhandled rejection; whoever awaits it attaches their own handlers.
const deferred = () => {
  let resolve = null;
  let reject = null;
  const promise = new Promise((_resolve, _reject) => {
    resolve = _resolve;
    reject = _reject;
  });
  promise.catch(() => {});
  return { promise, resolve, reject };
};

module.exports = { MAX_ID_LENGTH, isPeerId, deferred };
