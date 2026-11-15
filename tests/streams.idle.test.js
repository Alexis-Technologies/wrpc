'use strict';

// A stream nobody comes to read. An upload whose call was refused before
// its handler read it, a stream its sender announced and left, a download no
// page asked for — each kept its bytes and its slot for as long as the
// connection lived, and enough of them took the room the connection's next
// upload needed. `streamIdleTimeout` ends them. Each test below failed
// before it.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure } = require('../index.js');
const { bootServer, connectClient, waitFor } = require('./helpers/server.js');
const { recorder } = require('./helpers/recorder.js');
const { KIB, MIB, bytes, drain, rawPeer } = require('./helpers/streams.js');

const IDLE = 40;

const fixture = () => {
  const downloads = new Map();
  const router = defineRouter({
    files: {
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
      read: procedure({ access: 'public', handler: async (context, { id }) => drain(context.client.getStream(id)) }),
      // Refused before the handler looks at the stream: a 403, a validation
      // error, a quota — the upload is on its way all the same.
      refused: procedure({
        access: 'public',
        handler: async () => {
          throw Object.assign(new Error('Not allowed'), { code: 403 });
        },
      }),
      // A sink slower than the sweep.
      slow: procedure({
        access: 'public',
        handler: async (context, { id, pause }) => {
          let size = 0;
          for await (const chunk of context.client.getStream(id)) {
            size += chunk.length;
            await timers.setTimeout(pause);
          }
          return size;
        },
      }),
      held: procedure({
        access: 'public',
        handler: async (context) => {
          const streams = [];
          for (const stream of context.client.streams.values()) {
            streams.push({
              name: stream.name,
              status: stream.status,
              code: stream.error === null ? null : stream.error.code,
            });
          }
          return streams;
        },
      }),
      // A download of `size` bytes, `sent` of them written, ended or not.
      download: procedure({
        access: 'public',
        handler: async (context, { size, sent, end }) => {
          const stream = context.client.createStream('down.bin', size);
          downloads.set(stream.id, stream);
          await stream.writeAll(bytes(sent));
          if (end) stream.end();
          return stream.id;
        },
      }),
    },
  });
  return { router, downloads };
};

const held = (client) => client.api.files.held({});

test('server: uploads nobody read give their bytes and slots back', async (t) => {
  const { router } = fixture();
  const { writer, loud } = recorder();
  const { url } = await bootServer(t, { router, logger: writer, maxStreamBuffer: 1 * MIB, streamIdleTimeout: IDLE });
  const client = await connectClient(t, url);
  const escalated = [];
  client.on('error', (error) => escalated.push(error));
  await client.load('files');
  for (let i = 0; i < 3; i++) {
    const upload = client.createStream(`orphan-${i}`, 300 * KIB);
    const refused = client.api.files.refused({ id: upload.id }).catch((error) => error.code);
    await upload.writeAll(bytes(300 * KIB));
    upload.end();
    assert.strictEqual(await refused, 403);
  }
  assert.strictEqual((await held(client)).length, 3, 'the three uploads are held to begin with');
  await waitFor(async () => (await held(client)).length === 0, 'uploads nobody read were kept');
  // The megabyte they held is there again: an upload sent whole ahead of its call fits.
  const upload = client.createStream('next', 512 * KIB);
  await upload.writeAll(bytes(512 * KIB));
  upload.end();
  assert.strictEqual(await client.api.files.read({ id: upload.id }), 512 * KIB);
  // Each arrived whole: there was nothing left to stop, and nobody was told.
  assert.deepStrictEqual(escalated, []);
  // Nor is a stream nobody read a line to be paged for (the 403s are the handler's own).
  assert.deepStrictEqual(
    loud().filter((entry) => entry.code !== 403),
    [],
  );
});

test('server: empty streams nobody read give their slots back', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router, maxStreams: 4, streamIdleTimeout: IDLE });
  const client = await connectClient(t, url);
  await client.load('files');
  for (let i = 0; i < 4; i++) client.createStream(`empty-${i}`, 0).end();
  await waitFor(async () => (await held(client)).length === 0, 'four empty streams kept every slot');
  const upload = client.createStream('fifth', 4);
  upload.write(bytes(4));
  upload.end();
  assert.strictEqual(await client.api.files.read({ id: upload.id }), 4);
});

test('server: a stream its sender left unfinished is failed, and the sender told to stop', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router, streamIdleTimeout: IDLE });
  const client = await connectClient(t, url);
  await client.load('files');
  const upload = client.createStream('left.bin', 1 * MIB);
  upload.write(bytes(64 * KIB));
  await waitFor(() => upload.closed, 'the sender of a stream nobody read was never told');
  assert.deepStrictEqual([upload.error.code, upload.error.message], [408, 'Stream left.bin was never read']);
  // Its terminate is what makes the server forget it.
  await waitFor(async () => (await held(client)).length === 0, 'the server kept a stream both ends gave up');
});

test('server: a sender that does not listen is answered once; a late reader is told why', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router, streamIdleTimeout: IDLE });
  const peer = await rawPeer(t, url);
  peer.announce('old', 1 * MIB);
  peer.chunk('old', bytes(64 * KIB));
  await waitFor(() => peer.answer('old'), 'a failed stream is answered whoever sent it');
  assert.strictEqual(peer.answer('old').error.code, 408);
  // What it still sends is dropped, not answered again.
  for (let i = 0; i < 5; i++) peer.chunk('old', bytes(1 * KIB));
  peer.call('late', 'files/read', { id: 'old' });
  await waitFor(() => peer.answer('late'), 'the late reader');
  assert.strictEqual(peer.answer('late').error.code, 408);
  assert.strictEqual(peer.answers.filter((packet) => packet.id === 'old').length, 1);
  peer.end('old', 'terminate');
  peer.call('h', 'files/held');
  await waitFor(() => peer.answer('h'), 'held');
  assert.deepStrictEqual(peer.answer('h').result, []);
});

test('server: a stream that is being read, or still arriving, is not idle', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router, streamIdleTimeout: IDLE });
  const client = await connectClient(t, url);
  const escalated = [];
  client.on('error', (error) => escalated.push(error));
  await client.load('files');
  // A sink that takes three sweeps a chunk.
  const slow = client.createStream('slow.bin', 4 * KIB);
  const stored = client.api.files.slow({ id: slow.id, pause: IDLE * 3 });
  for (let i = 0; i < 4; i++) slow.write(bytes(1 * KIB));
  slow.end();
  // An upload sent ahead of its call, a piece every half sweep.
  const ahead = client.createStream('ahead.bin', 12 * KIB);
  for (let i = 0; i < 12; i++) {
    ahead.write(bytes(1 * KIB));
    await timers.setTimeout(IDLE / 2);
  }
  ahead.end();
  assert.strictEqual(await client.api.files.read({ id: ahead.id }), 12 * KIB);
  assert.strictEqual(await stored, 4 * KIB);
  assert.deepStrictEqual(escalated, []);
});

test('server: streamIdleTimeout: Infinity keeps what nobody reads', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router, streamIdleTimeout: Infinity });
  const client = await connectClient(t, url);
  await client.load('files');
  const upload = client.createStream('kept.bin', 4);
  upload.write(bytes(4));
  upload.end();
  await timers.setTimeout(IDLE * 4);
  assert.deepStrictEqual(await held(client), [{ name: 'kept.bin', status: 'active', code: null }]);
  assert.strictEqual(await client.api.files.read({ id: upload.id }), 4);
});

test('client: a download nobody read is forgotten; one still arriving is stopped', async (t) => {
  const { router, downloads } = fixture();
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url, { streamIdleTimeout: IDLE });
  const escalated = [];
  client.on('error', (error) => escalated.push(error));
  await client.load('files');
  const whole = await client.api.files.download({ size: 64 * KIB, sent: 64 * KIB, end: true });
  const forgotten = () => {
    try {
      client.getStream(whole);
      return false;
    } catch {
      return true;
    }
  };
  await waitFor(forgotten, 'a download nobody read was kept');
  // It had arrived whole: its server is told nothing.
  assert.strictEqual(downloads.get(whole).error, null);
  // One the server is still writing is stopped — it asked to hear.
  const open = await client.api.files.download({ size: 1 * MIB, sent: 64 * KIB, end: false });
  await waitFor(() => downloads.get(open).closed, 'the server went on with a download nobody reads');
  assert.strictEqual(downloads.get(open).error.code, 408);
  assert.deepStrictEqual(escalated, []);
});
