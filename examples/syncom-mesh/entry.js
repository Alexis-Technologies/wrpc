'use strict';

// The one module the page imports: the client (`connect`) from the main
// browser entry and the peer half of the webrtc subpath, bundled together
// by build.js the way a real app's bundler would resolve the two imports.
module.exports = { ...require('../../browser.js'), ...require('../../webrtc.browser.js') };
