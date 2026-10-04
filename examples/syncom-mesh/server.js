'use strict';

// The signaling server: any wrpc server with the built-in signaling unit.
// It never sees the peers' traffic — only descriptions and candidates while
// a link is (re)negotiated, and the roster of who is in which room. Here it
// also decides who a peer IS (the name the tab proposes; a real server would
// take it from the session) and vouches for it: every peer gets a signed
// assertion for the certificate it dials with, and the other peers verify
// it against the server's public key before they let the link in.

const { Server, defineRouter } = require('../../index.js');
const { createSignalingUnit, createSignalingHooks, generateAssertionKeys } = require('../../webrtc.js');

const main = async () => {
  // A fresh key pair per boot is fine for a demo; a deployment loads a
  // stable one from its secret store so a restart does not break open links.
  const keys = await generateAssertionKeys();
  const router = defineRouter(
    {
      ...createSignalingUnit({
        access: 'public',
        identity: (_context, { proposed }) => proposed ?? _context.client.id,
        assertions: { key: keys.privateKey, ttl: 300, issuer: 'syncom-mesh', claims: () => ({ demo: true }) },
      }),
    },
    { hooks: createSignalingHooks() },
  );
  const server = new Server({ router, host: '127.0.0.1', port: 8000, protocol: 'http' });
  await server.listen();
  console.log('wrpc signaling server listening on http://127.0.0.1:8000/api');
  console.log(`assertion key ${keys.kid}`);
  console.log('run `node build.js` once, then open public/index.html in two or more tabs');
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
