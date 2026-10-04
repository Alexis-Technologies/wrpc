'use strict';

// The WebTransport server half against a REAL HTTP/3 stack:
// @fails-components/webtransport (Google's libquiche behind a native
// binding) on both ends — its Http3Server hands acceptSessions() the
// sessions, its Node WebTransport client is injected into the wrpc client
// as `wt.WebTransport`. Guarded on WRPC_WT=fails so `pnpm test` stays
// self-contained: the package and its `-transport-http3-quiche` binary are
// devDependencies used only here and in the example (pnpm needs the
// binary's install script allowed — package.json#pnpm.onlyBuiltDependencies).
// Run by hand:
//
//   node scripts/wt-cert.js certs && WRPC_WT=fails node --test tests/wt/fails.integration.test.js

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');

const { defineRouter, procedure, tracked, createEventLog } = require('../../index.js');
const { acceptSessions, failsRequestCallback } = require('../../wt.js');
const { bearerAuth, bearerTransport, memoryStore } = require('../../auth.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const enabled = process.env.WRPC_WT === 'fails';
const certsDir = path.resolve(process.env.WRPC_WT_CERTS ?? 'certs');
const skip = !enabled
  ? 'set WRPC_WT=fails'
  : !fs.existsSync(path.join(certsDir, 'wt-cert.pem'))
    ? `no certificate: node scripts/wt-cert.js ${certsDir}`
    : false;

const boot = async (t, options = {}) => {
  const { Http3Server, WebTransport, quicheLoaded } = await import('@fails-components/webtransport');
  await quicheLoaded;
  const cert = fs.readFileSync(path.join(certsDir, 'wt-cert.pem'), 'utf8');
  const privKey = fs.readFileSync(path.join(certsDir, 'wt-key.pem'), 'utf8');
  const info = JSON.parse(fs.readFileSync(path.join(certsDir, 'wt-cert.json'), 'utf8'));
  const log = createEventLog({ size: 16 });
  const router = defineRouter({
    chat: {
      hello: procedure({
        access: 'public',
        handler: async (ctx, { name }) => `hi ${name} over ${ctx.client.transportKind}`,
      }),
      login: procedure({
        access: 'public',
        handler: async (ctx, { user }) => {
          ctx.client.startSession(undefined, { user });
          return { access: ctx.session.token };
        },
      }),
      whoami: procedure({ access: 'public', handler: async (ctx) => ctx.session?.state?.user ?? null }),
      ticks: procedure.subscription({
        access: 'public',
        handler: async function* (_ctx, _args, { lastEventId }) {
          for (const event of log.since(lastEventId) ?? []) yield event;
          for (let i = 0; i < 3; i++) yield tracked(log.push({ i }), { i });
        },
      }),
      echo: procedure({
        access: 'public',
        handler: async (ctx, { stream }) => {
          let bytes = 0;
          for await (const chunk of ctx.client.getStream(stream)) bytes += chunk.length;
          const back = ctx.client.createStream('back', bytes);
          back.write(new Uint8Array(bytes));
          back.end();
          return back.id;
        },
      }),
    },
  });
  const { server } = await bootServer(t, { router, ...options });
  const h3 = new Http3Server({ port: 0, host: '127.0.0.1', secret: 'changeit', cert, privKey });
  h3.setRequestCallback(failsRequestCallback);
  h3.startServer();
  await h3.ready;
  const errors = [];
  const acceptor = acceptSessions(server, h3.sessionStream('/api'), { onError: (error) => errors.push(error) });
  t.after(async () => {
    await acceptor.stop();
    await h3.stopServer();
  });
  const { port } = h3.address();
  const wt = {
    WebTransport,
    serverCertificateHashes: [{ algorithm: 'sha-256', value: Buffer.from(info.hash, 'base64') }],
  };
  const url = `https://127.0.0.1:${port}/api`;
  return { server, errors, url, wt };
};

test('wt fails: calls, subscriptions and binary streams over a real HTTP/3 session', { skip }, async (t) => {
  const { server, errors, url, wt } = await boot(t);
  const client = await connectClient(t, url, { transport: 'wt', wt, headers: { 'x-device': 'node' } });
  await client.load('chat');
  assert.strictEqual(await client.api.chat.hello({ name: 'ann' }), 'hi ann over wt');
  await waitFor(() => server.rpc.clients.size === 1, 'attached');
  const [attached] = server.rpc.clients;
  assert.strictEqual(attached.transportKind, 'wt');
  assert.strictEqual(attached.meta.headers['x-device'], 'node');
  assert.ok(attached.meta.remoteAddress, 'the peer address is observed');
  const seen = [];
  for await (const value of client.api.chat.ticks.iterate({})) {
    seen.push(value.i);
    if (seen.length === 3) break;
  }
  assert.deepStrictEqual(seen, [0, 1, 2]);
  const upload = client.createStream('blob', 1_000_000);
  upload.write(new Uint8Array(1_000_000));
  upload.end();
  const back = await client.api.chat.echo({ stream: upload.id });
  let received = 0;
  for await (const chunk of client.getStream(back)) received += chunk.length;
  assert.strictEqual(received, 1_000_000);
  assert.deepStrictEqual(errors, []);
  client.close();
  await waitFor(() => server.rpc.clients.size === 0, 'detached');
});

test('wt fails: a bearer session survives a reconnect over HTTP/3', { skip }, async (t) => {
  const { server, url, wt } = await boot(t, { sessions: { transport: bearerTransport() } });
  const store = memoryStore();
  const client = await connectClient(t, url, {
    transport: 'wt',
    wt,
    reconnect: { minDelay: 5, maxDelay: 20, jitter: false, retries: 5 },
    ...bearerAuth({ store, signIn: (c) => c.call('chat/login', { user: 'ann' }) }),
  });
  await client.load('chat');
  assert.strictEqual(await client.api.chat.whoami({}), 'ann');
  const reconnected = new Promise((resolve) => client.once('reconnect', resolve));
  // The server drops the connection (1001); the client comes back over a
  // fresh HTTP/3 session and presents the stored token again.
  const [attached] = server.rpc.clients;
  attached.close();
  await reconnected;
  assert.strictEqual(await client.api.chat.whoami({}), 'ann');
});
