'use strict';

// The WebTransport server half against a REAL HTTP/3 stack: quico, the
// pure-JS one — its request handler hands fromQuico() a session, the wrpc
// client on the other end is @fails-components/webtransport's Node client
// (W3C-shaped, injected as `wt.WebTransport`) when its native binary is
// present, quico's own otherwise for a transport-level echo. Guarded on
// WRPC_WT=quico so `pnpm test` stays self-contained: both packages are
// devDependencies used only here and in the example. Run by hand:
//
//   node scripts/wt-cert.js certs && WRPC_WT=quico node --test tests/wt/quico.integration.test.js
//
// The certificate: ECDSA P-256, at most 14 days — what scripts/wt-cert.js
// writes to ./certs (WRPC_WT_CERTS points elsewhere).

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');

const { defineRouter, procedure } = require('../../index.js');
const { attachSession, fromQuico } = require('../../wt.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const enabled = process.env.WRPC_WT === 'quico';
const certsDir = path.resolve(process.env.WRPC_WT_CERTS ?? 'certs');
const certs = () => ({
  key: fs.readFileSync(path.join(certsDir, 'wt-key.pem')),
  cert: fs.readFileSync(path.join(certsDir, 'wt-cert.pem')),
  info: JSON.parse(fs.readFileSync(path.join(certsDir, 'wt-cert.json'), 'utf8')),
});
const PORT = 4478;

const skip = !enabled
  ? 'set WRPC_WT=quico'
  : !fs.existsSync(path.join(certsDir, 'wt-cert.pem'))
    ? `no certificate: node scripts/wt-cert.js ${certsDir}`
    : false;

test('wt quico: a wrpc client over a real HTTP/3 session', { skip }, async (t) => {
  const quico = await import('quico');
  const { WebTransport, quicheLoaded } = await import('@fails-components/webtransport');
  await quicheLoaded;
  const { key, cert, info } = certs();
  const router = defineRouter({
    chat: {
      hello: procedure({
        access: 'public',
        handler: async (ctx, { name }) => `hi ${name} over ${ctx.client.transportKind}`,
      }),
      echo: procedure({
        access: 'public',
        handler: async (ctx, { stream }) => {
          let bytes = 0;
          for await (const chunk of ctx.client.getStream(stream)) bytes += chunk.length;
          return bytes;
        },
      }),
    },
  });
  const { server } = await bootServer(t, { router });
  const errors = [];
  const h3 = quico.createServer({ key, cert, http1: false, http2: false }, (req, res) => {
    if (req.headers[':protocol'] !== 'webtransport') return void res.end();
    const { session, ...meta } = fromQuico(req, res);
    // quico reports no session end: the idle timer is what detaches a gone peer.
    attachSession(server, session, { ...meta, idleTimeout: 2000 }).catch((error) => errors.push(error));
  });
  await new Promise((resolve) => h3.listen(PORT, '127.0.0.1', resolve));
  t.after(() => h3.close?.());

  const client = await connectClient(t, `https://127.0.0.1:${PORT}/api`, {
    transport: 'wt',
    wt: { WebTransport, serverCertificateHashes: [{ algorithm: 'sha-256', value: Buffer.from(info.hash, 'base64') }] },
    headers: { 'x-device': 'node' },
    // The client's pings are what keep the server's idle timer quiet.
    heartbeat: { interval: 250, timeout: 1000 },
  });
  await client.load('chat');
  assert.strictEqual(await client.api.chat.hello({ name: 'ann' }), 'hi ann over wt');
  await waitFor(() => server.rpc.clients.size === 1, 'attached');
  const [attached] = server.rpc.clients;
  assert.strictEqual(attached.meta.headers['x-device'], 'node');
  assert.strictEqual(attached.meta.url, '/api?wrpc_h=%7B%22x-device%22%3A%22node%22%7D');
  const upload = client.createStream('blob', 200_000);
  upload.write(new Uint8Array(200_000));
  upload.end();
  assert.strictEqual(await client.api.chat.echo({ stream: upload.id }), 200_000);
  assert.deepStrictEqual(errors, []);
  client.close();
  await waitFor(() => server.rpc.clients.size === 0, 'detached by the idle timer');
});
