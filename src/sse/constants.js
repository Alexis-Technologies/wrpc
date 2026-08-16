'use strict';

// Shared by the server half and the (browser-bundled) client half; nothing
// here may import anything.

// The header a POST (or a re-attaching GET) names its channel with.
const CHANNEL_HEADER = 'x-wrpc-channel';

module.exports = { CHANNEL_HEADER };
