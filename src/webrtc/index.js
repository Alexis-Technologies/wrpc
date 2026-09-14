'use strict';

// The Node barrel of @alexify/wrpc/webrtc: the browser surface plus what
// only a server needs — the signaling unit and its connection hooks.

const { createSignalingUnit, createSignalingHooks } = require('./signaling.js');

module.exports = { ...require('./browser.js'), createSignalingUnit, createSignalingHooks };
