'use strict';

// The protocol revision of a connection (protocol.md#versioning): what the
// two ends negotiate, and what each one sends once it is known. Revision 2
// is the framed messages — a packet whose bytes travel as bytes; a peer that
// negotiated revision 1 is sent the JSON 1.0 made of them. The published 1.0
// itself is the other end in tests/interop/; here a 2.x end plays it by
// offering, or selecting, `wrpc.v1`.

const { test } = require('node:test');
const assert = require('node:assert');

const { RpcServer, defineRouter, procedure } = require('../../index.js');
const { WebsocketServer } = require('#ws');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const BYTES = Uint8Array.of(1, 2, 3);
// What JSON.stringify makes of a Uint8Array — the form 1.0 sent and read.
const JSON_BYTES = { 0: 1, 1: 2, 2: 3 };

const boot = async (t, options = {}) => {
  const seen = { args: [], clients: [] };
  const router = defineRouter({
    files: {
      get: procedure({ access: 'public', handler: async () => ({ blob: BYTES }) }),
      put: procedure({
        access: 'public',
        handler: async (context, args) => {
          seen.args.push(args);
          seen.clients.push(context.client);
          return true;
        },
      }),
      join: procedure({
        access: 'public',
        handler: async (context) => {
          context.client.join('room');
          return context.client.revision;
        },
      }),
    },
  });
  const booted = await bootServer(t, { router, ...options });
  return { ...booted, seen };
};

const connect = async (t, url, options) => {
  const client = await connectClient(t, url, options);
  await client.load('files');
  return client;
};

test('revision: two 2.x ends negotiate wrpc.v2 — bytes travel as bytes, both ways', async (t) => {
  const { url, seen } = await boot(t);
  const client = await connect(t, url);
  assert.strictEqual(client.revision, 2);
  assert.strictEqual(await client.api.files.join(), 2, 'the server-side client says the same');
  assert.deepStrictEqual(await client.api.files.get(), { blob: BYTES });
  await client.api.files.put({ blob: BYTES });
  assert.ok(seen.args[0].blob instanceof Uint8Array);
});

test('revision: a client that offers wrpc.v1 alone is spoken to as 1.0 was', async (t) => {
  const { url, seen } = await boot(t);
  const client = await connect(t, url, { protocols: ['wrpc.v1'] });
  assert.strictEqual(client.revision, 1);
  assert.strictEqual(await client.api.files.join(), 1);
  // The result a 1.0 client could not have read as a frame arrives as JSON…
  assert.deepStrictEqual(await client.api.files.get(), { blob: JSON_BYTES });
  // …and what the client sends is JSON too: no frame leaves a revision-1 end.
  await client.api.files.put({ blob: BYTES });
  assert.deepStrictEqual(seen.args[0], { blob: JSON_BYTES });
});

test('revision: a peer that offers nothing negotiated nothing — revision 1', async (t) => {
  const { url } = await boot(t);
  const client = await connect(t, url, { protocols: [] });
  assert.strictEqual(client.revision, 1);
  assert.deepStrictEqual(await client.api.files.get(), { blob: JSON_BYTES });
});

test('revision: `attachments: false` on the server selects wrpc.v1 — a default client sends it no frame', async (t) => {
  const { server, url, seen } = await boot(t, { attachments: false });
  assert.strictEqual(server.rpc.revision, 1);
  const client = await connect(t, url);
  assert.strictEqual([...server.wsServer.connections][0].protocol, 'wrpc.v1');
  assert.strictEqual(client.revision, 1);
  // The call that used to be answered an id-less 500 and time out.
  assert.strictEqual(await client.api.files.put({ blob: BYTES }), true);
  assert.deepStrictEqual(seen.args[0], { blob: JSON_BYTES });
  assert.deepStrictEqual(await client.api.files.get(), { blob: JSON_BYTES });
});

test('revision: `attachments: false` on the client offers wrpc.v1 — a default server sends it no frame', async (t) => {
  const { server, url, seen } = await boot(t);
  assert.strictEqual(server.rpc.revision, 2);
  const client = await connect(t, url, { attachments: false });
  assert.strictEqual([...server.wsServer.connections][0].protocol, 'wrpc.v1');
  assert.strictEqual(client.revision, 1);
  // The result that used to arrive as a frame this client does not read.
  assert.deepStrictEqual(await client.api.files.get(), { blob: JSON_BYTES });
  await client.api.files.put({ blob: BYTES });
  assert.deepStrictEqual(seen.args[0], { blob: JSON_BYTES });
});

test('revision: an application that configured its own protocols owns the negotiation', async (t) => {
  // `attachments: false` narrows the engine only when the app did not.
  const { server, url } = await boot(t, { attachments: false, ws: { protocols: ['chat'] } });
  const client = await connect(t, url, { protocols: ['chat'] });
  assert.strictEqual([...server.wsServer.connections][0].protocol, 'chat');
  assert.strictEqual(client.revision, 1);
});

test('revision: a room of both revisions — one event, a frame for the 2.x client and JSON for the 1.0 one', async (t) => {
  const { server, url } = await boot(t);
  const modern = await connect(t, url);
  const old = await connect(t, url, { protocols: ['wrpc.v1'] });
  const second = await connect(t, url, { protocols: ['wrpc.v1'] });
  const got = { modern: [], old: [], second: [] };
  modern.api.files.on('changed', (data) => got.modern.push(data));
  old.api.files.on('changed', (data) => got.old.push(data));
  second.api.files.on('changed', (data) => got.second.push(data));
  await Promise.all([modern.api.files.join(), old.api.files.join(), second.api.files.join()]);
  assert.strictEqual(server.rpc.to('room').emit('files/changed', { blob: BYTES }), 3);
  await waitFor(() => got.modern.length === 1 && got.old.length === 1 && got.second.length === 1, 'all three hear it');
  assert.deepStrictEqual(got.modern, [{ blob: BYTES }]);
  assert.deepStrictEqual(got.old, [{ blob: JSON_BYTES }]);
  assert.deepStrictEqual(got.second, [{ blob: JSON_BYTES }]);
  // An event without bytes is the same text for everyone.
  server.rpc.to('room').emit('files/changed', { n: 1 });
  await waitFor(() => got.modern.length === 2 && got.old.length === 2, 'the plain event arrives');
  assert.deepStrictEqual([got.modern[1], got.old[1]], [{ n: 1 }, { n: 1 }]);
});

test('revision: a direct event to a revision-1 client is JSON', async (t) => {
  const { url, seen } = await boot(t);
  const old = await connect(t, url, { protocols: ['wrpc.v1'] });
  const got = [];
  old.api.files.on('changed', (data) => got.push(data));
  await old.api.files.put({});
  seen.clients[0].sendEvent('files/changed', { blob: BYTES });
  await waitFor(() => got.length === 1, 'the event arrives');
  assert.deepStrictEqual(got, [{ blob: JSON_BYTES }]);
});

test('revision: an engine composed by hand that selects wrpc.v2 for a frameless server is named in the log', async (t) => {
  // The shells narrow their engine to wrpc.v1 (revisionProtocols); a server
  // wired to a bare WebsocketServer has nobody to do it, and says so.
  const lines = [];
  const logger = { debug() {}, info() {}, error() {}, warn: (line) => lines.push(line) };
  const rpc = new RpcServer({
    router: defineRouter({ unit: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } }),
    attachments: false,
    logger,
  });
  const http = require('node:http').createServer();
  const wsServer = new WebsocketServer({ server: http, logger: false });
  wsServer.on('connection', (socket, req) => rpc.attachSocket(socket, { headers: req.headers, url: req.url }));
  await new Promise((resolve) => http.listen(0, '127.0.0.1', resolve));
  t.after(() => {
    wsServer.close();
    http.close();
    rpc.close();
  });
  const client = await connectClient(t, `ws://127.0.0.1:${http.address().port}/api`);
  assert.strictEqual(client.revision, 2, 'the bare engine selected the newest revision offered');
  await waitFor(() => lines.some((line) => line.event === 'revision.mismatch'), 'the mismatch is logged');
});
