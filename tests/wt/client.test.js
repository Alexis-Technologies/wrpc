'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { WrpcClient } = require('../../src/client.js');
const { defineRouter, procedure } = require('../../src/rpc/router.js');
const { ClientWtTransport, UNAVAILABLE } = require('../../src/client/webtransport.js');
const {
  StreamParser,
  frame,
  frameText,
  datagramText,
  KIND_BINARY,
  KIND_TEXT,
  KIND_CAPS,
} = require('../../src/webtransport/framing.js');
const { chunkEncode } = require('../../src/chunks.js');
const { createFakeWt } = require('./fakeWebTransport.js');
const { runTransportContract } = require('../client/transportContract.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const ENDPOINT = 'https://127.0.0.1:4433/api';

// The server side of a control stream, by hand: what WtSocket does in
// src/webtransport/socket.js, reduced to what these tests need.
const controlStream = async (session) => {
  const reader = session.incomingBidirectionalStreams.getReader();
  const { value: stream } = await reader.read();
  reader.releaseLock();
  const received = [];
  // The capabilities message each end sends first is the mux's, not a test's.
  const parser = new StreamParser({
    onMessage: (kind, data) => {
      if (kind !== KIND_CAPS) received.push({ kind, data });
    },
  });
  void (async () => {
    const streamReader = stream.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await streamReader.read();
        if (done) return;
        parser.push(value);
      }
    } catch {
      // The session closed under the read.
    }
  })();
  const writer = stream.writable.getWriter();
  return { stream, received, writer };
};

const opened = async (t, world, options = {}) => {
  const transport = new ClientWtTransport(ENDPOINT, { WebTransport: world.WebTransport, ...options });
  t.after(() => transport.close());
  await transport.open();
  const session = await world.next();
  return { transport, session };
};

test('wt transport: registered in the base client entry and passes the shared contract', async (t) => {
  assert.strictEqual(WrpcClient.transport.wt, ClientWtTransport);
  await runTransportContract(t, 'wt', ClientWtTransport);
  const bare = new ClientWtTransport('x://host/wt');
  assert.strictEqual(bare.heartbeat, true);
  assert.strictEqual(bare.persistent, true);
  assert.strictEqual(bare.session, null);
  // No global WebTransport in Node, nothing injected: a clear refusal.
  assert.strictEqual(typeof globalThis.WebTransport, 'undefined');
  await assert.rejects(bare.open(), { message: UNAVAILABLE });
  assert.throws(() => bare.write('x'), /Not connected/);
  bare.close(); // never opened: a no-op
  bare.terminate();
});

test('wt transport: open() dials, announces open before resolving, is idempotent', async (t) => {
  const world = createFakeWt();
  const transport = new ClientWtTransport(ENDPOINT);
  t.after(() => transport.close());
  const events = [];
  transport.on('open', () => events.push('open'));
  let resolvedBeforeOpen = null;
  const opening = transport
    .open({ wt: { WebTransport: world.WebTransport, serverCertificateHashes: [{ algorithm: 'sha-256', value: 'x' }] } })
    .then(() => {
      resolvedBeforeOpen = events.length === 0;
    });
  const again = transport.open(); // the same in-flight promise
  await Promise.all([opening, again]);
  assert.strictEqual(resolvedBeforeOpen, false, "'open' was emitted before open() resolved");
  assert.strictEqual(transport.active, true);
  assert.deepStrictEqual(events, ['open']);
  await transport.open();
  assert.deepStrictEqual(events, ['open']);
  // The session was constructed with the url as given and the init options only.
  assert.strictEqual(transport.session.url, ENDPOINT);
  assert.deepStrictEqual(transport.session.init, { serverCertificateHashes: [{ algorithm: 'sha-256', value: 'x' }] });
});

test('wt transport: declared headers and meta ride the connect URL, as on ws', async (t) => {
  const world = createFakeWt();
  const transport = new ClientWtTransport(ENDPOINT, { WebTransport: world.WebTransport });
  t.after(() => transport.close());
  await transport.open({ headers: { 'x-device': 'tablet' }, meta: { tenant: 'acme' } });
  const session = await world.next();
  const path = session.header[':path'];
  const query = new URL(`https://h${path}`).searchParams;
  assert.deepStrictEqual(JSON.parse(query.get('wrpc_h')), { 'x-device': 'tablet' });
  assert.deepStrictEqual(JSON.parse(query.get('wrpc_meta')), { tenant: 'acme' });
  assert.strictEqual(session.header.origin, 'https://app.example');
});

test('wt transport: packets and chunks cross the control stream in order, both ways', async (t) => {
  const world = createFakeWt();
  const { transport, session } = await opened(t, world);
  const { received, writer } = await controlStream(session);
  assert.strictEqual(transport.write('{"type":"ping"}'), true);
  const big = new Uint8Array(70_000).map((_, i) => i & 255);
  transport.write(chunkEncode('s1', big));
  transport.write('{"type":"pong"}');
  await waitFor(() => received.length === 3, 'delivery');
  assert.deepStrictEqual(
    received.map((m) => m.kind),
    [KIND_TEXT, KIND_BINARY, KIND_TEXT],
  );
  assert.deepStrictEqual(received[0].data, '{"type":"ping"}');
  assert.deepStrictEqual(Buffer.from(received[1].data), Buffer.from(chunkEncode('s1', big)));
  // Server -> client: a string for a packet, bytes for a chunk.
  const messages = [];
  transport.on('message', (data) => messages.push(data));
  await writer.write(frameText('{"type":"event"}'));
  await writer.write(frame(KIND_BINARY, chunkEncode('s2', new Uint8Array([9, 8, 7]))));
  await waitFor(() => messages.length === 2, 'inbound');
  assert.strictEqual(messages[0], '{"type":"event"}');
  assert.ok(messages[1] instanceof Uint8Array);
  assert.deepStrictEqual(Array.from(messages[1]), Array.from(chunkEncode('s2', new Uint8Array([9, 8, 7]))));
});

test('wt transport: terminate() during the handshake rejects open() and closes the session', async (t) => {
  const world = createFakeWt();
  const transport = new ClientWtTransport(ENDPOINT, { WebTransport: world.WebTransport });
  const closes = [];
  transport.on('close', () => closes.push(1));
  const opening = transport.open();
  transport.terminate();
  await assert.rejects(opening);
  assert.strictEqual(transport.active, false);
  assert.strictEqual(transport.session, null);
  assert.deepStrictEqual(closes, [], 'never opened: no close announced');
  // A refused handshake rejects open() with the session's error.
  world.refuse(new Error('nope'));
  await assert.rejects(transport.open(), /nope/);
  // And a later open() works — the transport is re-entrant.
  await transport.open();
  t.after(() => transport.close());
  assert.strictEqual(transport.active, true);
});

test('wt transport: a peer close is one close event; close() tells the peer; terminate() is local', async (t) => {
  const world = createFakeWt();
  const { transport, session } = await opened(t, world);
  const closes = [];
  transport.on('close', () => closes.push(1));
  session.close({ closeCode: 42, reason: 'bye' });
  await waitFor(() => closes.length === 1, 'close');
  assert.strictEqual(transport.active, false);
  assert.throws(() => transport.write('x'), /Not connected/);
  transport.close();
  transport.terminate();
  await timers.setImmediate();
  assert.deepStrictEqual(closes, [1]);

  const second = await opened(t, world);
  const closeInfo = second.session.closed;
  second.transport.close();
  assert.deepStrictEqual(await closeInfo, { closeCode: 0, reason: '' });

  const third = await opened(t, world);
  const events = [];
  third.transport.on('close', () => events.push('close'));
  third.transport.terminate();
  assert.deepStrictEqual(events, ['close'], 'terminate reports synchronously');
  assert.ok(await third.session.closed);
});

test('wt transport: write() reports backpressure and drain follows', async (t) => {
  const world = createFakeWt();
  const { transport, session } = await opened(t, world, { highWaterMark: 100, lowWaterMark: 20 });
  await controlStream(session);
  const release = world.hold();
  const drains = [];
  transport.on('drain', () => drains.push(1));
  assert.strictEqual(transport.write('x'.repeat(50)), true);
  assert.strictEqual(transport.write('y'.repeat(50)), false, 'above the high-water mark');
  assert.strictEqual(transport.write('z'), false, 'still above');
  await timers.setImmediate();
  assert.deepStrictEqual(drains, []);
  release();
  await waitFor(() => drains.length === 1, 'drain');
});

test("wt transport: a peer's framing violation is escalated and hangs up", async (t) => {
  const world = createFakeWt();
  const { transport, session } = await opened(t, world);
  const { writer } = await controlStream(session);
  const errors = [];
  const closes = [];
  transport.on('error', (error) => errors.push(error));
  transport.on('close', () => closes.push(1));
  await writer.write(frame(9, new Uint8Array(1)));
  await waitFor(() => closes.length === 1, 'close');
  assert.strictEqual(errors[0].name, 'FramingError');
  assert.ok(await session.closed);
  // With no error listener the escalation is swallowed, not thrown.
  const quiet = await opened(t, world);
  const { writer: w2 } = await controlStream(quiet.session);
  await w2.write(frame(9, new Uint8Array(1)));
  await waitFor(() => quiet.transport.active === false, 'close');
});

test('wt transport: the reader ending (peer closed the control stream) is a close', async (t) => {
  const world = createFakeWt();
  const { transport, session } = await opened(t, world);
  const { writer } = await controlStream(session);
  await writer.close();
  await waitFor(() => transport.active === false, 'close');
  assert.ok(await session.closed);
});

// The fallback list at connect time: no WebTransport in this runtime, so
// 'wt' hands over to 'ws' at once and the client lands on a real server.
test('wt transport: connect() falls through the list when wt is unavailable', async (t) => {
  const router = defineRouter({ echo: { ping: procedure(async () => 'pong') } });
  const { url, server } = await bootServer(t, { router });
  const fallbacks = [];
  const client = await connectClient(t, url, {
    transport: ['wt', 'ws'],
    reconnect: { retries: 0 },
  });
  client.on('transport-fallback', (info) => fallbacks.push(info));
  assert.strictEqual(client.active, true);
  await waitFor(() => server.rpc.clients.size === 1, 'connected over ws');
  // The last candidate's failure is connect()'s.
  await assert.rejects(WrpcClient.connect(url, { transport: ['wt'], reconnect: false }), { message: UNAVAILABLE });
});

test('wt transport: an unreliable packet is one datagram, inbound datagrams are messages, a big one falls back', async (t) => {
  const world = createFakeWt({ maxDatagramSize: 64 });
  const { transport, session } = await opened(t, world);
  await controlStream(session);
  assert.strictEqual(transport.maxDatagramSize, 64);
  const got = [];
  const reader = session.datagrams.readable.getReader();
  void (async () => {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      got.push(value);
    }
  })();
  assert.strictEqual(transport.writeUnreliable('{"type":"event","name":"a/b","data":1}'), true);
  await waitFor(() => got.length === 1, 'datagram');
  assert.strictEqual(got[0][0], KIND_TEXT);
  assert.strictEqual(Buffer.from(got[0].subarray(1)).toString(), '{"type":"event","name":"a/b","data":1}');
  // Too large for one datagram: not sent, the caller falls back.
  assert.strictEqual(transport.writeUnreliable('x'.repeat(100)), false);
  // Bytes never ride datagrams (a chunk needs order).
  assert.strictEqual(transport.writeUnreliable(new Uint8Array(3)), false);
  // Inbound: a datagram's packet is a 'message'; an unreadable one is dropped.
  const messages = [];
  transport.on('message', (data) => messages.push(data));
  const writer = session.datagrams.writable.getWriter();
  await writer.write(datagramText('{"type":"event","name":"c/d"}'));
  await writer.write(new Uint8Array([7, 1, 2]));
  await writer.write(new Uint8Array([KIND_TEXT, 0xff]));
  await writer.write(datagramText('{"type":"event","name":"e/f"}'));
  await waitFor(() => messages.length === 2, 'inbound datagrams');
  assert.deepStrictEqual(messages, ['{"type":"event","name":"c/d"}', '{"type":"event","name":"e/f"}']);
  transport.close();
  assert.strictEqual(transport.writeUnreliable('{}'), false);
  assert.strictEqual(transport.maxDatagramSize, 0);
});

test('wt transport: without datagrams on the session, writeUnreliable answers false', async (t) => {
  const world = createFakeWt();
  const NoDatagrams = class extends world.WebTransport {
    constructor(url, init) {
      super(url, init);
      this.datagrams = undefined;
    }
  };
  const transport = new ClientWtTransport(ENDPOINT, { WebTransport: NoDatagrams });
  t.after(() => transport.close());
  await transport.open();
  assert.strictEqual(transport.maxDatagramSize, 0);
  assert.strictEqual(transport.writeUnreliable('{}'), false);
});
