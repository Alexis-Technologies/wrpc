'use strict';

// The signaling server: any wrpc server with the built-in signaling unit.
// It never sees the peers' traffic — only descriptions and candidates while
// a link is (re)negotiated, and the roster of who is in which room.

const { Server, defineRouter } = require('../../index.js');
const { createSignalingUnit, createSignalingHooks } = require('../../webrtc.js');

const router = defineRouter(
  { ...createSignalingUnit({ access: 'public' }) },
  { hooks: createSignalingHooks() },
);

const server = new Server({ router, host: '127.0.0.1', port: 8000, protocol: 'http' });

server.listen().then(() => {
  console.log('wrpc signaling server listening on http://127.0.0.1:8000/api');
  console.log('run `node build.js` once, then open public/index.html in two or more tabs');
});
