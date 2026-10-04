'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure } = require('../../index.js');
const { acceptSessions } = require('../../wt.js');
const { createEncryption, generateKey } = require('../../encryption.js');
const { FRAME_MARK, FRAME_HANDSHAKE, FRAME_SEALED } = require('../../src/wire.js');
const { createFakeWt } = require('./fakeWebTransport.js');
const { ClientWtTransport } = require('../../src/client/webtransport.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const SECRET = 'not for the edge that terminates QUIC: 4111 1111 1111 1111';

const router = defineRouter({
  files: {
    echo: procedure({ access: 'public', handler: async (_ctx, args) => args }),
    session: procedure({ access: 'public', handler: async (ctx) => ctx.client.encryption?.protocol ?? null }),
    kind: procedure({ access: 'public', handler: async (ctx) => ctx.client.transportKind }),
    upload: procedure({
      access: 'public',
      handler: async (ctx, { stream }) => {
        let bytes = 0;
        for await (const chunk of ctx.client.getStream(stream)) bytes += chunk.length;
        return bytes;
      },
    }),
    download: procedure({
      access: 'public',
      handler: async (ctx, { size }) => {
        const stream = ctx.client.createStream('blob', size);
        stream.write(new Uint8Array(size).fill(3));
        stream.end();
        return stream.id;
      },
    }),
    nudge: procedure({
      access: 'public',
      handler: async (ctx) => {
        ctx.client.sendEvent('files/nudged', { note: SECRET }, { unreliable: true });
        return true;
      },
    }),
  },
});

const boot = async (t, serverOptions = {}) => {
  const { server, url } = await bootServer(t, { router, encryption: { keys: generateKey() }, ...serverOptions });
  const world = createFakeWt();
  const sessions = [];
  const frames = [];
  const attachSocket = server.rpc.attachSocket.bind(server.rpc);
  server.rpc.attachSocket = (socket, meta) => {
    socket.on('message', (data, isBinary) => frames.push({ isBinary, bytes: Buffer.from(data) }));
    return attachSocket(socket, meta);
  };
  const acceptor = acceptSessions(server, world.sessions, { onClient: (_c, session) => sessions.push(session) });
  t.after(() => acceptor.stop());
  const serverKey = await server.rpc.encryptionKey();
  const connect = (options = {}) =>
    connectClient(t, url, { transport: 'wt', wt: { WebTransport: world.WebTransport }, ...options });
  return { server, url, world, sessions, frames, serverKey, connect };
};

test('wt encryption: the same session over WebTransport — calls, both stream directions, an event', async (t) => {
  const { connect, serverKey, sessions, frames } = await boot(t);
  const client = await connect({ encryption: createEncryption({ serverKey }) });
  await client.load('files');
  assert.strictEqual(await client.api.files.kind(), 'wt');
  assert.strictEqual(await client.api.files.session(), 'Noise_NK_25519_AESGCM_SHA256');
  assert.strictEqual(client.encryption.protocol, 'Noise_NK_25519_AESGCM_SHA256');
  assert.deepStrictEqual(await client.api.files.echo({ note: SECRET }), { note: SECRET });
  const upload = client.createStream('data', 200_000);
  const uploaded = client.api.files.upload({ stream: upload.id });
  upload.write(new Uint8Array(120_000).fill(1));
  upload.write(new Uint8Array(80_000).fill(2));
  upload.end();
  assert.strictEqual(await uploaded, 200_000);
  const id = await client.api.files.download({ size: 50_000 });
  let received = 0;
  for await (const chunk of client.getStream(id)) received += chunk.length;
  assert.strictEqual(received, 50_000);
  const nudged = new Promise((resolve) => client.api.files.on('nudged', resolve));
  await client.api.files.nudge();
  assert.deepStrictEqual(await nudged, { note: SECRET });

  // Nothing went around the channel: no stream of its own, no datagram, no plaintext
  await waitFor(() => sessions.length === 1);
  assert.strictEqual(sessions[0].uniOpened ?? 0, 0, 'no per-stream transport under encryption');
  assert.ok(frames.length > 5);
  for (const { isBinary, bytes } of frames) {
    assert.ok(isBinary);
    assert.strictEqual(bytes[0], FRAME_MARK);
    assert.ok(bytes[1] === FRAME_HANDSHAKE || bytes[1] === FRAME_SEALED);
    assert.ok(!bytes.includes('4111') && !bytes.includes('echo'));
  }
});

test('wt encryption: a broadcast reaches sealed WebTransport sessions and a plain one alike — text and bytes, in order', async (t) => {
  const { server, connect, serverKey } = await boot(t);
  const clients = [
    await connect({ encryption: createEncryption({ serverKey }) }),
    await connect({ encryption: createEncryption({ serverKey }) }),
    await connect(),
  ];
  for (const client of clients) await client.load('files');
  const heard = clients.map(
    (client) =>
      new Promise((resolve) => {
        const got = [];
        client.api.files.on('news', (data) => void (got.push(data), got.length === 2 && resolve(got)));
      }),
  );
  const blob = Uint8Array.from({ length: 400 }, (_, i) => i % 250);
  server.rpc.broadcast('files/news', { n: 1, note: 'first' });
  server.rpc.broadcast('files/news', { n: 2, blob });
  for (const got of await Promise.all(heard)) {
    assert.deepStrictEqual([got[0].n, got[0].note, got[1].n], [1, 'first', 2]);
    assert.deepStrictEqual(Buffer.from(got[1].blob), Buffer.from(blob));
  }
});

test('wt encryption: the handshake is bound to the transport kind — a ws handshake does not finish on wt', async (t) => {
  const { connect, serverKey } = await boot(t);
  // A link that claims to be a WebSocket while riding WebTransport: the
  // prologues differ, so the server's answer does not open.
  const honest = createEncryption({ serverKey });
  const lying = { ...honest, secure: (link) => honest.secure({ ...link, kind: 'ws' }) };
  await assert.rejects(connect({ encryption: lying, reconnect: false }), /./);
});

test('wt encryption: compression is left off — what would be compressed is ciphertext', async (t) => {
  const { connect, serverKey, frames } = await boot(t, { compression: true });
  const client = await connect({ encryption: createEncryption({ serverKey }), compression: true });
  await client.load('files');
  const text = 'z'.repeat(40_000);
  assert.strictEqual((await client.api.files.echo({ text })).text.length, 40_000);
  assert.ok(
    frames.some((f) => f.bytes.length > 40_000),
    'the call travelled whole, sealed',
  );
});

test('wt encryption: a server that refuses the handshake is a failed open at once, not at the timeout', async (t) => {
  const { connect, serverKey } = await boot(t);
  // A pin under a kid the server's ring does not hold: refused at the
  // hello, and the session hung up — which is when open() must reject,
  // not when the handshake timeout runs out.
  const pinned = serverKey.replace(/^[^:]+:/, 'retired:');
  const started = Date.now();
  await assert.rejects(
    connect({ reconnect: false, encryption: createEncryption({ serverKey: pinned, handshakeTimeout: 5_000 }) }),
  );
  assert.ok(Date.now() - started < 2_000, 'rejected when the session closed');
});

test('wt encryption: a sealed write past the high-water mark answers false, and drain follows', async (t) => {
  const { url, serverKey, world } = await boot(t);
  // The transport by hand, its session sealed by the server boot() accepts on.
  const transport = new ClientWtTransport(url.replace(/^http/, 'https'), {
    WebTransport: world.WebTransport,
    highWaterMark: 100,
    lowWaterMark: 20,
  });
  t.after(() => transport.close());
  await transport.open({ encryption: createEncryption({ serverKey }) });
  assert.ok(transport.encryption, 'sealed');
  const drains = [];
  transport.on('drain', () => drains.push(1));
  const release = world.hold();
  // Sealing is asynchronous: the bytes are counted once the channel hands
  // them over, so the first write answers on what was queued before it.
  transport.write('x'.repeat(200));
  await waitFor(() => transport.bufferedAmount > 100, 'sealed and queued');
  assert.strictEqual(transport.write('y'), false, 'past the mark on the sealed path');
  await timers.setTimeout(10);
  assert.deepStrictEqual(drains, []);
  release();
  await waitFor(() => drains.length === 1, 'drain after a sealed false');
  assert.strictEqual(transport.bufferedAmount, 0);
});

test('wt encryption: required — a plaintext WebTransport session is hung up on', async (t) => {
  const { connect, serverKey } = await boot(t, { encryption: { keys: generateKey(), required: true } });
  // An upgrade is not a session: connect() may resolve before the hang-up lands
  const plain = await connect({ reconnect: false }).catch(() => null);
  if (plain !== null) await waitFor(() => plain.active === false, 'the plaintext session was closed');
  const sealed = await connect({ encryption: createEncryption({ serverKey }) });
  await sealed.load('files');
  assert.deepStrictEqual(await sealed.api.files.echo({ ok: 1 }), { ok: 1 });
});
