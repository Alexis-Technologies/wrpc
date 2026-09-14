'use strict';

// The browser half of the './webrtc' subpath: the peer, its link, host and
// signaler client; the server-side signaling unit stays in the Node barrel.
module.exports = require('./src/webrtc/browser.js');
