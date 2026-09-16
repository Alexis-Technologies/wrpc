'use strict';

// A wrpc server on WebSocket AND WebTransport at once: the ordinary Server
// on TCP (HTTP/1.1 for the page and the REST routes, WebSocket for the
// fallback), an HTTP/3 host on UDP for `transport: 'wt'` — both attached to
// ONE RpcServer, so a WebTransport tab and a WebSocket tab share the room.
//
//   node scripts/wt-cert.js certs      # once: a 13-day ECDSA certificate + its hash
//   node examples/wt/build.js          # bundles the browser client
//   node examples/wt/server.js         # http://127.0.0.1:8001 — open it in Chrome
//
// The HTTP/3 host is @fails-components/webtransport (a devDependency of the
// repository, never of the package); swap the `acceptSessions` line for
// fromQuico() in a quico handler to run the pure-JS stack instead.

const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');

const { Server, defineRouter, procedure } = require('../../index.js');
const { acceptSessions, failsRequestCallback } = require('../../wt.js');
const { bearerTransport } = require('../../auth.js');

const certsDir = path.resolve(process.env.WRPC_WT_CERTS ?? 'certs');
const cert = fs.readFileSync(path.join(certsDir, 'wt-cert.pem'), 'utf8');
const privKey = fs.readFileSync(path.join(certsDir, 'wt-key.pem'), 'utf8');
const info = JSON.parse(fs.readFileSync(path.join(certsDir, 'wt-cert.json'), 'utf8'));

const HTTP_PORT = 8000; // the wrpc Server: WebSocket + REST
const H3_PORT = 4433; // the HTTP/3 host: WebTransport
const PAGE_PORT = 8001; // the page and its bundle, on a server of their own
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8' };

// The page: a static server of its own, so nothing sits in front of the
// wrpc Server's request handling. It also hands the page the certificate
// hash and the ports.
const servePage = () =>
  new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      const pathname = new URL(req.url, 'http://x').pathname;
      if (pathname === '/cert-hash.json') {
        res.writeHead(200, { 'content-type': 'application/json' });
        return void res.end(JSON.stringify({ hash: info.hash, h3Port: H3_PORT, wsPort: HTTP_PORT }));
      }
      const root = path.join(__dirname, 'public');
      const file = path.join(root, pathname === '/' ? 'index.html' : pathname);
      if (!file.startsWith(root) || !fs.existsSync(file)) {
        res.writeHead(404);
        return void res.end();
      }
      res.writeHead(200, { 'content-type': MIME[path.extname(file)] ?? 'application/octet-stream' });
      res.end(fs.readFileSync(file));
    });
    server.listen(PAGE_PORT, '127.0.0.1', () => resolve(server));
  });

const router = defineRouter({
  chat: {
    join: procedure({
      access: 'public',
      handler: async (ctx, { name }) => {
        ctx.client.join('lobby');
        ctx.server.to('lobby').emit('chat/joined', { name, transport: ctx.client.transportKind });
        return { transport: ctx.client.transportKind, clients: ctx.server.clients.size };
      },
    }),
    say: procedure({
      access: 'public',
      handler: async (ctx, { name, text }) => void ctx.server.to('lobby').emit('chat/message', { name, text }),
    }),
  },
});

const main = async () => {
  const { Http3Server, quicheLoaded } = await import('@fails-components/webtransport');
  await quicheLoaded;

  const server = new Server({ router, host: '127.0.0.1', port: HTTP_PORT, sessions: { transport: bearerTransport() } });
  await server.listen();
  const page = await servePage();

  const h3 = new Http3Server({ port: H3_PORT, host: '127.0.0.1', secret: 'changeit', cert, privKey });
  // sessionStream() matches the request path literally, query included; the
  // wrpc client puts its declared headers in the query, so route by pathname.
  h3.setRequestCallback(failsRequestCallback);
  h3.startServer();
  await h3.ready;
  const acceptor = acceptSessions(server, h3.sessionStream(server.rpc.basePath), {
    onError: (error) => console.error('wt session refused', error),
  });

  console.log(`page       http://127.0.0.1:${PAGE_PORT}/`);
  console.log(`websocket  ws://127.0.0.1:${HTTP_PORT}${server.rpc.basePath}`);
  console.log(
    `webtransport https://127.0.0.1:${H3_PORT}${server.rpc.basePath}  (cert ${info.hash}, until ${info.notAfter})`,
  );

  const stop = async () => {
    await acceptor.stop();
    await h3.stopServer();
    await server.close();
    page.close();
    process.exit(0);
  };
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
