'use strict';

// The browser half of the './sse' subpath: registering the client transport
// is all a browser needs, and the server half would drag node:http in.
module.exports = require('./src/sse/client.js');
