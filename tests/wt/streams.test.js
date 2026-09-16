'use strict';

// Binary streams on their own WebTransport streams (src/webtransport/streams.js):
// the client transport and the server socket over the fake, end to end
// through a real Server, and the mux's ordering guarantees on their own.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure } = require('../../index.js');
const { acceptSessions, StreamParser, frameCaps, KIND_CAPS, KIND_TEXT } = require('../../wt.js');
const { StreamMux } = require('../../src/webtransport/streams.js');
const { ClientWtTransport } = require('../../src/client/webtransport.js');
const { chunkEncode, chunkDecode } = require('../../src/chunks.js');
const { createFakeWt } = require('./fakeWebTransport.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const router = () =>
  defineRouter({
    files: {
      upload: procedure({
        access: 'public',
        handler: async (ctx, { stream }) => {
          let bytes = 0;
          let chunks = 0;
          for await (const chunk of ctx.client.getStream(stream)) {
            bytes += chunk.length;
            chunks++;
          }
          return { bytes, chunks };
        },
      }),
      download: procedure({
        access: 'public',
        handler: async (ctx, { size, parts }) => {
          const stream = ctx.client.createStream('blob', size);
          for (let i = 0; i < parts; i++) stream.write(new Uint8Array(size / parts).fill(i));
          stream.end();
          return stream.id;
        },
      }),
      abandon: procedure({
        access: 'public',
        handler: async (ctx, { size }) => {
          const stream = ctx.client.createStream('blob', size);
          stream.write(new Uint8Array(size / 2));
          stream.terminate();
          return stream.id;
        },
      }),
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
    },
  });

const boot = async (t, options = {}) => {
  const { server, url } = await bootServer(t, { router: router() });
  const world = createFakeWt();
  const sessions = [];
  const acceptor = acceptSessions(server, world.sessions, { onClient: (_c, session) => sessions.push(session) });
  t.after(() => acceptor.stop());
  const client = await connectClient(t, url, { transport: 'wt', wt: { WebTransport: world.WebTransport }, ...options });
  await client.load('files');
  await waitFor(() => sessions.length === 1, 'attached');
  return { server, client, world, session: sessions[0], clientSession: client.transport?.session ?? null };
};

test('wt streams: an upload rides its own unidirectional stream, in order, ending with its FIN', async (t) => {
  const { client, session } = await boot(t);
  const before = session.uniOpened;
  const upload = client.createStream('data', 300_000);
  for (let i = 0; i < 3; i++) upload.write(new Uint8Array(100_000).fill(i));
  upload.end();
  assert.deepStrictEqual(await client.api.files.upload({ stream: upload.id }), { bytes: 300_000, chunks: 3 });
  // The server opened no stream of its own for an inbound upload.
  assert.strictEqual(session.uniOpened, before);
});

test('wt streams: a download rides the server-opened stream; a terminated one arrives as a termination', async (t) => {
  const { client, session } = await boot(t);
  const id = await client.api.files.download({ size: 200_000, parts: 4 });
  const readable = client.getStream(id);
  const seen = [];
  for await (const chunk of readable) seen.push(chunk.length, chunk[0]);
  assert.deepStrictEqual(seen, [50_000, 0, 50_000, 1, 50_000, 2, 50_000, 3]);
  assert.strictEqual(session.uniOpened, 1, 'one unidirectional stream for the download');

  // A terminate() on the server resets its stream: the client's readable
  // ends with what arrived before it — the half that was written — the way
  // a terminate packet on the control stream ends it.
  const abandoned = await client.api.files.abandon({ size: 100_000 });
  const stream = client.getStream(abandoned);
  let received = 0;
  try {
    for await (const chunk of stream) received += chunk.length;
  } catch {
    // A terminated readable may end with an error; the byte count is the check.
  }
  assert.strictEqual(received, 50_000);
  assert.strictEqual(session.uniOpened, 2);
});

test('wt streams: chunks in flight do not sit in front of calls on the control stream', async (t) => {
  const { client, world } = await boot(t);
  // Hold the world's writes: chunks queue on the upload's own stream, and
  // the control stream's call is not behind them once writes flow again.
  const release = world.hold();
  const upload = client.createStream('data', 4_000_000);
  for (let i = 0; i < 40; i++) upload.write(new Uint8Array(100_000));
  upload.end();
  const pong = client.api.files.ping({});
  const done = client.api.files.upload({ stream: upload.id });
  release();
  assert.strictEqual(await pong, 'pong');
  assert.deepStrictEqual(await done, { bytes: 4_000_000, chunks: 40 });
});

test('wt streams: a peer that announces no streams gets every chunk on the control stream', async (t) => {
  const world = createFakeWt();
  const kinds = [];
  // A hand-rolled server end that answers `{}` as its capabilities — set up
  // as the session arrives, since the control stream only exists once the
  // client transport has opened it.
  const served = (async () => {
    const session = await world.next();
    const reader = session.incomingBidirectionalStreams.getReader();
    const { value: control } = await reader.read();
    const writer = control.writable.getWriter();
    await writer.write(frameCaps('{}'));
    const parser = new StreamParser({
      onMessage: (kind, data) => kinds.push([kind, typeof data === 'string' ? data.slice(0, 16) : data.length]),
    });
    const r = control.readable.getReader();
    for (;;) {
      const { value, done } = await r.read();
      if (done) return;
      parser.push(value);
    }
  })();
  served.catch(() => {});
  const transport = new ClientWtTransport('https://h/api', { WebTransport: world.WebTransport });
  t.after(() => transport.close());
  await transport.open();
  await waitFor(() => kinds.length === 1, 'our capabilities arrived');
  transport.send({ type: 'stream', id: 's1', name: 'blob', size: 3 });
  transport.write(chunkEncode('s1', new Uint8Array([1, 2, 3])));
  transport.send({ type: 'stream', id: 's1', status: 'end' });
  await waitFor(() => kinds.length === 4, 'all on the control stream');
  assert.deepStrictEqual(kinds, [
    [KIND_CAPS, '{"streams":true}'],
    [KIND_TEXT, '{"type":"stream"'],
    [1, 6],
    [KIND_TEXT, '{"type":"stream"'],
  ]);
  assert.strictEqual(transport.session.uniOpened, 0);
});

// The mux alone, driven by hand: the ordering rules it exists for.
const muxPair = async () => {
  const world = createFakeWt();
  const a = new world.WebTransport('https://h/api');
  await a.ready;
  const b = await world.next();
  const make = (session) => {
    const out = [];
    const mux = new StreamMux(session, {
      emitPacket: (text) => out.push(JSON.parse(text)),
      emitChunk: (frame) => {
        const { id, payload } = chunkDecode(frame);
        out.push({ chunk: id, bytes: Array.from(payload) });
      },
      onQueued() {},
      onSent() {},
    });
    mux.peerCaps('{"streams":true}');
    void (async () => {
      const reader = session.incomingUnidirectionalStreams.getReader();
      for (;;) {
        const { value, done } = await reader.read();
        if (done) return;
        mux.accept(value);
      }
    })();
    return { mux, out };
  };
  return { a: make(a), b: make(b), sessions: [a, b] };
};

test('wt streams: the mux holds early chunks until the open packet passes, and ends after the last chunk', async () => {
  const { a, b } = await muxPair();
  const open = { type: 'stream', id: 'x', name: 'blob', size: 4 };
  assert.strictEqual(a.mux.control(open), false, 'the open packet still goes on the control stream');
  assert.strictEqual(a.mux.chunk(chunkEncode('x', new Uint8Array([1, 2]))), true);
  assert.strictEqual(a.mux.chunk(chunkEncode('x', new Uint8Array([3, 4]))), true);
  assert.strictEqual(a.mux.control({ type: 'stream', id: 'x', status: 'end' }), true, 'the end is the FIN');
  // The chunks arrive at b before the open packet is "seen" there.
  await waitFor(() => b.out.length === 0 && true, 'nothing yet');
  await timers.setTimeout(20);
  assert.deepStrictEqual(b.out, [], 'held: the open packet has not passed');
  assert.strictEqual(b.mux.packet(JSON.stringify(open)), true, 'delivered by the mux, in order');
  await waitFor(() => b.out.length === 4, 'released');
  assert.deepStrictEqual(b.out, [
    open,
    { chunk: 'x', bytes: [1, 2] },
    { chunk: 'x', bytes: [3, 4] },
    { type: 'stream', id: 'x', status: 'end' },
  ]);
  // A chunk of a stream the mux never opened belongs on the control stream.
  assert.strictEqual(a.mux.chunk(chunkEncode('other', new Uint8Array(1))), false);
  // An end/terminate of a stream on the control stream passes through.
  assert.strictEqual(b.mux.packet('{"type":"stream","id":"y","status":"end"}'), false);
  assert.strictEqual(b.mux.packet('{"type":"call","id":"1"}'), false);
  assert.strictEqual(b.mux.packet('{"type":"stream" broken'), false);
});

test('wt streams: a peer without the capability disables the mux; close() resets what is open', async () => {
  const { a, b, sessions } = await muxPair();
  a.mux.peerCaps('{}');
  assert.strictEqual(a.mux.enabled, false);
  assert.strictEqual(a.mux.control({ type: 'stream', id: 'z', name: 'n', size: 1 }), false);
  assert.strictEqual(a.mux.chunk(chunkEncode('z', new Uint8Array(1))), false);
  a.mux.peerCaps('not json');
  assert.strictEqual(a.mux.enabled, false);
  a.mux.peerCaps('{"streams":true}');
  assert.strictEqual(a.mux.enabled, true);
  assert.strictEqual(a.mux.control({ type: 'stream', id: 'z', name: 'n', size: 1 }), false);
  a.mux.chunk(chunkEncode('z', new Uint8Array([9])));
  b.mux.packet(JSON.stringify({ type: 'stream', id: 'z', name: 'n', size: 1 }));
  await waitFor(() => b.out.length === 1, 'chunk');
  a.mux.close();
  await waitFor(() => b.out.length === 2, 'reset');
  assert.deepStrictEqual(b.out[1], { type: 'stream', id: 'z', status: 'terminate' });
  sessions[0].close();
});

test('wt streams: a host that grants no unidirectional streams gets the chunks on the control stream, in order', async (t) => {
  const { server, url } = await bootServer(t, { router: router() });
  const world = createFakeWt();
  world.uniQuota = 0;
  const acceptor = acceptSessions(server, world.sessions);
  t.after(() => acceptor.stop());
  const client = await connectClient(t, url, { transport: 'wt', wt: { WebTransport: world.WebTransport } });
  await client.load('files');
  // Client -> server: the open fails, the held chunks and the end replay on
  // the control stream — the upload completes, and later streams skip the
  // attempt altogether.
  const upload = client.createStream('data', 30_000);
  for (let i = 0; i < 3; i++) upload.write(new Uint8Array(10_000).fill(i));
  upload.end();
  assert.deepStrictEqual(await client.api.files.upload({ stream: upload.id }), { bytes: 30_000, chunks: 3 });
  const again = client.createStream('data', 5);
  again.write(new Uint8Array(5));
  again.end();
  assert.deepStrictEqual(await client.api.files.upload({ stream: again.id }), { bytes: 5, chunks: 1 });
  // Server -> client likewise.
  const id = await client.api.files.download({ size: 20_000, parts: 2 });
  let received = 0;
  for await (const chunk of client.getStream(id)) received += chunk.length;
  assert.strictEqual(received, 20_000);
});
