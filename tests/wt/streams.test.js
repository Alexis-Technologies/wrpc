'use strict';

// Binary streams on their own WebTransport streams (src/webtransport/streams.js):
// the client transport and the server socket over the fake, end to end
// through a real Server, and the mux's ordering guarantees on their own.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure } = require('../../index.js');
const { acceptSessions, StreamParser, frameCaps, KIND_CAPS, KIND_TEXT } = require('../../wt.js');
const { StreamMux, idHeader } = require('../../src/webtransport/streams.js');
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
    [KIND_CAPS, '{"streams":true,'],
    [KIND_TEXT, '{"type":"stream"'],
    [1, 6],
    [KIND_TEXT, '{"type":"stream"'],
  ]);
  assert.strictEqual(transport.session.uniOpened, 0);
});

// The mux alone, driven by hand: the ordering rules it exists for.
const muxPair = async (options = {}) => {
  const world = createFakeWt();
  const a = new world.WebTransport('https://h/api');
  await a.ready;
  const b = await world.next();
  const make = (session) => {
    const out = [];
    const refused = [];
    const mux = new StreamMux(session, {
      emitPacket: (text) => out.push(JSON.parse(text)),
      emitChunk: (frame) => {
        const { id, payload } = chunkDecode(frame);
        out.push({ chunk: id, bytes: Array.from(payload) });
      },
      onQueued() {},
      onSent() {},
      onRefused: (reason, id) => refused.push([reason, id]),
      ...options,
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
    return { mux, out, refused };
  };
  return { a: make(a), b: make(b), sessions: [a, b], world };
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

test('wt streams: a stream for an id the peer never names is held no longer than holdTimeout, and no more than the cap', async () => {
  const { a, b, world } = await muxPair({ holdTimeout: 40, maxHeldStreams: 2 });
  // a opens three streams whose open packets b never sees — what a peer
  // that wants the receiver's memory does, before any authentication.
  for (const id of ['h1', 'h2', 'h3']) {
    a.mux.control({ type: 'stream', id, name: 'n', size: 1 });
    a.mux.chunk(chunkEncode(id, new Uint8Array([1])));
  }
  await waitFor(() => b.refused.length === 1, 'the third is past the cap');
  assert.strictEqual(b.refused[0][0], 'held');
  assert.deepStrictEqual(b.out, [], 'nothing delivered');
  await waitFor(() => b.refused.length === 3, 'the two held time out');
  assert.deepStrictEqual(
    b.refused.slice(1).map(([reason]) => reason),
    ['timeout', 'timeout'],
  );
  assert.ok(world.cancelled >= 3, `STOP_SENDING reached the sender: ${world.cancelled}`);
  // A late open packet opens nothing: what was held is gone.
  const late = b.refused[1][1];
  assert.strictEqual(b.mux.packet(JSON.stringify({ type: 'stream', id: late, name: 'n', size: 1 })), false);
  await timers.setTimeout(10);
  assert.deepStrictEqual(b.out, []);
  // The mux still serves: a stream whose open packet comes is delivered.
  a.mux.control({ type: 'stream', id: 'ok', name: 'n', size: 1 });
  a.mux.chunk(chunkEncode('ok', new Uint8Array([7])));
  await timers.setTimeout(10);
  assert.strictEqual(b.mux.packet(JSON.stringify({ type: 'stream', id: 'ok', name: 'n', size: 1 })), true);
  await waitFor(() => b.out.length === 2, 'delivered');
  assert.deepStrictEqual(b.out[1], { chunk: 'ok', bytes: [7] });
});

test('wt streams: an empty id, a second stream for an id, and a stream from a peer that announced none are cancelled unread', async () => {
  const { a, b, sessions, world } = await muxPair();
  const raw = async (bytes) => {
    const writable = await sessions[0].createUnidirectionalStream();
    const writer = writable.getWriter();
    await writer.write(bytes);
    return writer;
  };
  await raw(Uint8Array.of(0));
  await waitFor(() => b.refused.length === 1, 'an empty id');
  assert.deepStrictEqual(b.refused[0], ['id', '']);
  // A stream for an id another stream already claimed.
  a.mux.control({ type: 'stream', id: 'd', name: 'n', size: 1 });
  a.mux.chunk(chunkEncode('d', new Uint8Array([1])));
  await timers.setTimeout(10);
  assert.strictEqual(b.mux.packet(JSON.stringify({ type: 'stream', id: 'd', name: 'n', size: 1 })), true);
  await waitFor(() => b.out.length === 2, 'the first stream is the stream');
  await raw(idHeader('d'));
  await waitFor(() => b.refused.length === 2, 'a duplicate');
  assert.deepStrictEqual(b.refused[1], ['duplicate', 'd']);
  // A peer that announced no streams: not read at all.
  b.mux.peerCaps('{}');
  await raw(idHeader('u'));
  await waitFor(() => b.refused.length === 3, 'unannounced');
  assert.deepStrictEqual(b.refused[2], ['unannounced', null]);
  assert.ok(world.cancelled >= 3);
  assert.throws(() => new StreamMux(sessions[1], { maxHeldStreams: 0 }), TypeError);
  assert.throws(() => new StreamMux(sessions[1], { holdTimeout: 0 }), TypeError);
  assert.throws(() => new StreamMux(sessions[1], { openTimeout: 0 }), TypeError);
});

test('wt streams: chunks held for a stream still opening are counted once, when held', async () => {
  const queued = [];
  const sent = [];
  const { a, b, world } = await muxPair({ onQueued: (n) => queued.push(n), onSent: (n) => sent.push(n) });
  world.uniQuota = 'hang';
  const open = { type: 'stream', id: 'h', name: 'blob', size: 5 };
  assert.strictEqual(a.mux.control(open), false);
  assert.strictEqual(a.mux.chunk(chunkEncode('h', new Uint8Array([1, 2]))), true);
  assert.strictEqual(a.mux.chunk(chunkEncode('h', new Uint8Array([3, 4, 5]))), true);
  // Counted the moment they are held — what waits for the open used to be
  // invisible to the transport's bufferedAmount, without bound.
  assert.deepStrictEqual(queued, [2, 3]);
  assert.deepStrictEqual(sent, []);
  world.grant();
  // The stream reaches the peer once granted; its open packet passes after.
  await timers.setTimeout(20);
  assert.strictEqual(b.mux.packet(JSON.stringify(open)), true);
  await waitFor(() => b.out.length === 3, 'delivered once the stream opened');
  await waitFor(() => sent.length === 2, 'taken');
  // Routed when the stream opened, without a second count.
  assert.deepStrictEqual(queued, [2, 3]);
  assert.deepStrictEqual(
    sent.slice().sort((x, y) => x - y),
    [2, 3],
  );
});

test('wt streams: an open the host never answers falls back to the control stream after openTimeout; a late grant is reset unused', async () => {
  const control = [];
  const packets = [];
  const queued = [];
  const { a, b, sessions, world } = await muxPair({
    openTimeout: 30,
    onQueued: (n) => queued.push(n),
    writeControl: (frame) => control.push(Array.from(chunkDecode(frame).payload)),
    sendControl: (packet) => packets.push(packet),
  });
  world.uniQuota = 'hang';
  const open = { type: 'stream', id: 'p', name: 'blob', size: 3 };
  assert.strictEqual(a.mux.control(open), false);
  a.mux.chunk(chunkEncode('p', new Uint8Array([1])));
  a.mux.chunk(chunkEncode('p', new Uint8Array([2, 3])));
  assert.strictEqual(a.mux.control({ type: 'stream', id: 'p', status: 'end' }), true, 'the end waits with the chunks');
  assert.strictEqual(a.mux.enabled, true);
  await waitFor(() => packets.length === 1, 'the deadline passed');
  // In order, the end packet last, and this stream's side of the mux off
  // for every stream after it — exactly what a refused open does.
  assert.deepStrictEqual(control, [[1], [2, 3]]);
  assert.deepStrictEqual(packets, [{ type: 'stream', id: 'p', status: 'end' }]);
  assert.strictEqual(a.mux.enabled, false);
  // Counted when held, uncounted when handed to the control stream (which
  // counts them as its own): net nothing.
  assert.strictEqual(
    queued.reduce((sum, n) => sum + n, 0),
    0,
  );
  assert.strictEqual(a.mux.chunk(chunkEncode('p', new Uint8Array([4]))), false, 'gone from the mux');
  // The host grants the stream after all: reset unused, nothing reaches the peer.
  world.grant();
  await waitFor(() => sessions[0].uniOpened === 1, 'granted late');
  await timers.setTimeout(20);
  assert.deepStrictEqual(b.out, []);
  assert.deepStrictEqual(b.refused, []);
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

test('wt streams: gate() holds every inbound read while it answers a promise; onActivity hears each read', async () => {
  const world = createFakeWt();
  const a = new world.WebTransport('https://h/api');
  await a.ready;
  const b = await world.next();
  // The sender's mux is the plain one; only the receiver is gated.
  const sender = new StreamMux(a, { emitPacket() {}, emitChunk() {}, onQueued() {}, onSent() {} });
  sender.peerCaps('{"streams":true}');
  let held = null;
  let activity = 0;
  const out = [];
  const receiver = new StreamMux(b, {
    emitPacket: (text) => out.push(JSON.parse(text)),
    emitChunk: (frame) => out.push(Array.from(chunkDecode(frame).payload)),
    onQueued() {},
    onSent() {},
    gate: () => held,
    onActivity: () => activity++,
  });
  receiver.peerCaps('{"streams":true}');
  void (async () => {
    const reader = b.incomingUnidirectionalStreams.getReader();
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      receiver.accept(value);
    }
  })();
  const open = { type: 'stream', id: 'g', name: 'blob', size: 5 };
  assert.strictEqual(receiver.packet(JSON.stringify(open)), false, 'the open packet came first');
  sender.control(open);
  sender.chunk(chunkEncode('g', new Uint8Array([1])));
  sender.chunk(chunkEncode('g', new Uint8Array([2])));
  await waitFor(() => out.length === 2, 'flowing');
  assert.ok(activity >= 2, `every read with bytes is activity (${activity})`);
  let release;
  held = new Promise((resolve) => {
    release = resolve;
  });
  sender.chunk(chunkEncode('g', new Uint8Array([3])));
  sender.chunk(chunkEncode('g', new Uint8Array([4])));
  await timers.setTimeout(20);
  assert.ok(out.length <= 3, 'gated: at most the read in flight');
  held = null;
  release();
  await waitFor(() => out.length === 4, 'released');
  assert.deepStrictEqual(out, [[1], [2], [3], [4]]);
  // Closed while gated: the read is let go, and delivers nothing. The
  // read in flight may still deliver one chunk (5); the next (6) finds
  // the gate, then the close.
  held = new Promise((resolve) => {
    release = resolve;
  });
  sender.chunk(chunkEncode('g', new Uint8Array([5])));
  await timers.setTimeout(10);
  sender.chunk(chunkEncode('g', new Uint8Array([6])));
  await timers.setTimeout(10);
  receiver.close();
  release();
  await timers.setTimeout(10);
  assert.ok(out.length <= 5, `${out.length} delivered`);
  assert.ok(!out.some((chunk) => chunk[0] === 6), 'nothing after the close');
  a.close();
});
