'use strict';

// The browser half of the './encryption' subpath: the primitives and the
// client halves — the envelopes and the sealed store are a server's.
module.exports = require('./src/encryption/browser.js');
