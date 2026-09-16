'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { WtSocket } = require('../../src/webtransport/socket.js');
const {
  StreamParser,
  frame,
  frameText,
  datagramText,
  KIND_BINARY,
  KIND_TEXT,
  KIND_CAPS,
} = require('../../src/webtransport/framing.js');
const { isWtSession, isWtStream, isWtDatagrams } = require('../../src/webtransport/port.js');
const { createFakeWt } = require('./fakeWebTransport.js');
const { waitFor } = require('../helpers/server.js');

// A client end by hand: the session, its control stream, a parser over
// what the socket sends and a writer to talk to it.
const pair = async (t, options = {}) => {
  const world = createFakeWt();
  const client = new world.WebTransport('https://h/api');
  await client.ready;
  const session = await world.next();
  const stream = await client.createBidirectionalStream();
  const reader = session.incomingBidirectionalStreams.getReader();
  const { value: control } = await reader.read();
  reader.releaseLock();
  const socket = new WtSocket(session, control, options);
  t.after(() => socket.terminate());
  const received = [];
  const parser = new StreamParser({
    onMessage: (kind, data) => {
      if (kind !== KIND_CAPS) received.push({ kind, data });
    },
  });
  void (async () => {
    const r = stream.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await r.read();
        if (done) return;
        parser.push(value);
      }
    } catch {
      // closed under the read
    }
  })();
  return { world, client, session, socket, received, writer: stream.writable.getWriter() };
};

test('wt port: the validators are structural', async () => {
  const world = createFakeWt();
  const client = new world.WebTransport('https://h/api');
  assert.strictEqual(isWtSession(client), true);
  await client.ready;
  const session = await world.next();
  assert.strictEqual(isWtSession(session), true);
  assert.strictEqual(isWtSession({}), false);
  assert.strictEqual(isWtSession(null), false);
  assert.strictEqual(isWtSession({ ...session, datagrams: { readable: 1 } }), false);
  const stream = await client.createBidirectionalStream();
  assert.strictEqual(isWtStream(stream), true);
  assert.strictEqual(isWtStream({ readable: stream.readable }), false);
  assert.strictEqual(isWtDatagrams(session.datagrams), true);
  assert.strictEqual(isWtDatagrams({ ...session.datagrams, maxDatagramSize: '1' }), false);
  client.close();
});

test('wt socket: the engine-port shape — text and binary both ways, boolean send, close codes', async (t) => {
  const { session, socket, received, writer } = await pair(t);
  assert.strictEqual(socket.remoteAddress, '');
  assert.strictEqual(socket.protocol, '');
  assert.strictEqual(socket.bufferedAmount, 0);
  const messages = [];
  socket.on('message', (data, isBinary) => messages.push({ data, isBinary }));
  await writer.write(frameText('{"type":"ping"}'));
  await writer.write(frame(KIND_BINARY, new Uint8Array([1, 2, 3])));
  await waitFor(() => messages.length === 2, 'inbound');
  assert.deepStrictEqual(messages[0], { data: '{"type":"ping"}', isBinary: false });
  assert.strictEqual(messages[1].isBinary, true);
  assert.deepStrictEqual(Array.from(messages[1].data), [1, 2, 3]);
  assert.strictEqual(socket.send('{"type":"pong"}'), true);
  assert.strictEqual(socket.send(Buffer.from([9, 9])), true);
  assert.strictEqual(socket.send(new Uint8Array([7]).buffer), true);
  await waitFor(() => received.length === 3, 'outbound');
  assert.deepStrictEqual(received[0], { kind: KIND_TEXT, data: '{"type":"pong"}' });
  assert.deepStrictEqual(Array.from(received[1].data), [9, 9]);
  assert.deepStrictEqual(Array.from(received[2].data), [7]);
  // The payload survives the callback: the socket handed over a view over
  // its own read, and the fake delivered a copy.
  await writer.write(frame(KIND_BINARY, new Uint8Array([5, 5, 5])));
  await waitFor(() => messages.length === 3, 'inbound');
  const kept = messages[2].data;
  await writer.write(frame(KIND_BINARY, new Uint8Array([6, 6, 6])));
  await waitFor(() => messages.length === 4, 'inbound');
  assert.deepStrictEqual(Array.from(kept), [5, 5, 5]);

  const closes = [];
  socket.on('close', (code, reason) => closes.push([code, reason]));
  socket.close(1001, 'Server is closing');
  assert.deepStrictEqual(closes, [[1001, 'Server is closing']], 'reported synchronously');
  assert.deepStrictEqual(await session.closed, { closeCode: 1001, reason: 'Server is closing' });
  // Poisoned handle: quiet, false, 0.
  assert.strictEqual(socket.send('x'), false);
  assert.strictEqual(socket.bufferedAmount, 0);
  socket.close();
  socket.terminate();
  socket.pause();
  socket.resume();
  await timers.setImmediate();
  assert.strictEqual(closes.length, 1);
});

test('wt socket: a peer close, a terminate and a peer framing violation', async (t) => {
  const first = await pair(t);
  const closes = [];
  first.socket.on('close', (code, reason) => closes.push([code, reason]));
  first.client.close({ closeCode: 4000, reason: 'bye' });
  await waitFor(() => closes.length === 1, 'close');
  assert.deepStrictEqual(closes, [[4000, 'bye']]);

  const second = await pair(t);
  const events = [];
  second.socket.on('close', (code) => events.push(code));
  second.socket.terminate();
  assert.deepStrictEqual(events, [1006]);
  assert.ok(await second.client.closed);

  const third = await pair(t);
  const errors = [];
  const ends = [];
  third.socket.on('error', (error) => errors.push(error));
  third.socket.on('close', (code, reason) => ends.push([code, reason]));
  await third.writer.write(frame(5, new Uint8Array(1)));
  await waitFor(() => ends.length === 1, 'close');
  assert.strictEqual(errors[0].name, 'FramingError');
  assert.deepStrictEqual(ends, [[1002, 'Protocol error']]);
  assert.deepStrictEqual(await third.client.closed, { closeCode: 1002, reason: 'Protocol error' });

  // The peer ending the control stream ends the connection, with 1000.
  const fourth = await pair(t);
  const done = [];
  fourth.socket.on('close', (code) => done.push(code));
  await fourth.writer.close();
  await waitFor(() => done.length === 1, 'close');
  assert.deepStrictEqual(done, [1000]);
});

test('wt socket: backpressure — send() false above the mark, drain under it; pause() stops reading', async (t) => {
  const { world, socket, received, writer } = await pair(t, { highWaterMark: 100, lowWaterMark: 20 });
  const release = world.hold();
  const drains = [];
  socket.on('drain', () => drains.push(1));
  assert.strictEqual(socket.send('x'.repeat(50)), true);
  assert.strictEqual(socket.send('y'.repeat(50)), false);
  assert.ok(socket.bufferedAmount > 100);
  await timers.setImmediate();
  assert.deepStrictEqual(drains, []);
  release();
  await waitFor(() => drains.length === 1, 'drain');
  assert.strictEqual(socket.bufferedAmount, 0);
  await waitFor(() => received.length === 2, 'delivered');

  const messages = [];
  socket.on('message', (data) => messages.push(data));
  socket.pause();
  assert.strictEqual(socket.isPaused, true);
  await writer.write(frameText('a'));
  await writer.write(frameText('b'));
  await timers.setTimeout(20);
  assert.ok(messages.length <= 1, 'paused: at most the read already in flight');
  socket.resume();
  await waitFor(() => messages.length === 2, 'resumed');
  assert.deepStrictEqual(messages, ['a', 'b']);
});

test('wt socket: idleTimeout terminates a silent peer and every read re-arms it', async (t) => {
  const { socket, session, writer } = await pair(t, { idleTimeout: 60 });
  const closes = [];
  const errors = [];
  socket.on('error', (error) => errors.push(error.message));
  socket.on('close', (code) => closes.push(code));
  for (let i = 0; i < 4; i++) {
    await timers.setTimeout(30);
    await writer.write(frameText('{"type":"ping"}'));
  }
  assert.deepStrictEqual(closes, [], 'a talking peer is never idle');
  await waitFor(() => closes.length === 1, 'idle');
  assert.deepStrictEqual(closes, [1006]);
  assert.deepStrictEqual(errors, ['No data for 60 ms']);
  assert.ok(await session.closed);
});

test('wt socket: datagrams — sendUnreliable is one datagram, an inbound one is a text message', async (t) => {
  const { client, socket, session } = await pair(t);
  assert.strictEqual(socket.maxDatagramSize, 1200);
  const got = [];
  const reader = client.datagrams.readable.getReader();
  void (async () => {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      got.push(Buffer.from(value.subarray(1)).toString());
    }
  })();
  assert.strictEqual(socket.sendUnreliable('{"type":"event","name":"x/y"}'), true);
  assert.strictEqual(socket.sendUnreliable('x'.repeat(2000)), false, 'too large for one datagram');
  assert.strictEqual(socket.sendUnreliable(new Uint8Array(1)), false, 'bytes never ride datagrams');
  await waitFor(() => got.length === 1, 'datagram');
  assert.deepStrictEqual(got, ['{"type":"event","name":"x/y"}']);
  const messages = [];
  socket.on('message', (data, isBinary) => messages.push([data, isBinary]));
  const writer = client.datagrams.writable.getWriter();
  await writer.write(datagramText('{"type":"ping"}'));
  await writer.write(new Uint8Array([9]));
  await waitFor(() => messages.length === 1, 'inbound');
  assert.deepStrictEqual(messages, [['{"type":"ping"}', false]]);
  socket.close();
  assert.strictEqual(socket.sendUnreliable('{}'), false);
  assert.strictEqual(socket.maxDatagramSize, 0);
  assert.ok(await session.closed);
});
