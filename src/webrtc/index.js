'use strict';

// The Node barrel of @alexify/wrpc/webrtc: the browser surface plus what
// only a server needs (the signaling unit lands here in a later phase).

module.exports = { ...require('./browser.js') };
