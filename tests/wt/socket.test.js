'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { WtSocket } = require('../../src/webtransport/socket.js');
const {
  StreamParser,
  frame,
  frameText,
  frameCaps,
  datagramText,
  KIND_BINARY,
  KIND_TEXT,
  KIND_CAPS,
} = require('../../src/webtransport/framing.js');
const { isWtSession, isWtStream, isWtDatagrams } = require('../../src/webtransport/port.js');
const { idHeader } = require('../../src/webtransport/streams.js');
const { chunkDecode } = require('../../src/chunks.js');
const { createFakeWt } = require('./fakeWebTransport.js');
const { runChannelContract, peerEnd } = require('./channelContract.js');
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
  const { recorder } = require('../helpers/recorder.js');
  const log = recorder();
  const first = await pair(t, { log: log.writer });
  const closes = [];
  first.socket.on('close', (code, reason) => closes.push([code, reason]));
  first.client.close({ closeCode: 4000, reason: 'bye' });
  await waitFor(() => closes.length === 1, 'close');
  assert.deepStrictEqual(closes, [[4000, 'bye']]);
  // A routine end is a debug line with the code and the peer's reason.
  assert.deepStrictEqual(log.all('wt.close'), [{ level: 'debug', event: 'wt.close', code: 4000, reason: 'bye' }]);

  const second = await pair(t);
  const events = [];
  second.socket.on('close', (code) => events.push(code));
  second.socket.terminate();
  assert.deepStrictEqual(events, [1006]);
  assert.ok(await second.client.closed);

  const violations = recorder();
  const third = await pair(t, { log: violations.writer });
  const errors = [];
  const ends = [];
  third.socket.on('error', (error) => errors.push(error));
  third.socket.on('close', (code, reason) => ends.push([code, reason]));
  await third.writer.write(frame(5, new Uint8Array(1)));
  await waitFor(() => ends.length === 1, 'close');
  assert.strictEqual(errors[0].name, 'FramingError');
  const line = violations.find('wt.violation');
  assert.deepStrictEqual([line.level, line.code, line.err.name], ['warn', errors[0].code, 'FramingError']);
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

// The channel contract shared with the client transport: send/drain on
// every path, order under an async codec, the count after terminate().
test('wt socket: the channel contract', async (t) => {
  await runChannelContract(t, 'wt socket', {
    async open(sub, options = {}) {
      const world = createFakeWt();
      const client = new world.WebTransport('https://h/api');
      await client.ready;
      const session = await world.next();
      const stream = await client.createBidirectionalStream();
      const reader = session.incomingBidirectionalStreams.getReader();
      const { value: control } = await reader.read();
      reader.releaseLock();
      const socket = new WtSocket(session, control, options);
      sub.after(() => socket.terminate());
      const end = {
        session,
        send: (data, sendOptions) => socket.send(data, sendOptions),
        // What ServerWtTransport does with an outbound stream packet: the
        // mux is told first, the control stream carries it unless the mux
        // took it.
        stream: (packet) => {
          if (!socket.streamControl(packet)) socket.send(JSON.stringify(packet));
        },
        on: (event, listener) => socket.on(event, listener),
        get bufferedAmount() {
          return socket.bufferedAmount;
        },
        get compression() {
          return socket.compression;
        },
        terminate: () => socket.terminate(),
      };
      return { world, end, peer: peerEnd(client, stream) };
    },
  });
});

test('wt socket: pause() stops reading the control stream, resume() takes it up', async (t) => {
  const { socket, writer } = await pair(t);
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

test('wt socket: a stream opened for an id the peer never names is cancelled after holdTimeout, and said so', async (t) => {
  await assert.rejects(pair(t, { maxHeldStreams: 0 }), TypeError);
  await assert.rejects(pair(t, { holdTimeout: -1 }), TypeError);
  const { client, socket, writer } = await pair(t, { holdTimeout: 30 });
  const refused = [];
  socket.on('stream-refused', (info) => refused.push(info));
  // The client announces streams, then opens one for an id it never names
  // on the control stream — before any packet, before any session.
  await writer.write(frameCaps('{"streams":true}'));
  const uni = await client.createUnidirectionalStream();
  const w = uni.getWriter();
  await w.write(idHeader('ghost'));
  await w.write(new Uint8Array(1000));
  await waitFor(() => refused.length === 1);
  assert.deepStrictEqual(refused, [{ reason: 'timeout', id: 'ghost' }]);
  // The session lives on: it is the stream that was refused, not the peer.
  assert.strictEqual(socket.send('still here'), true);
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

// A client end that announces streams and uploads on a unidirectional
// stream of its own: the open packet on the control stream, the chunk
// header then raw payload on the side stream (src/webtransport/streams.js).
const sideStream = async ({ client, writer }, id, size) => {
  await writer.write(frameCaps('{"streams":true}'));
  await writer.write(frameText(JSON.stringify({ type: 'stream', id, name: 'blob', size })));
  const uni = (await client.createUnidirectionalStream()).getWriter();
  await uni.write(idHeader(id));
  return uni;
};

test('wt socket: pause() stops the side streams too; resume() takes them up; a close under pause lets the reads go', async (t) => {
  const end = await pair(t);
  const chunks = [];
  end.socket.on('message', (data, isBinary) => {
    if (isBinary) chunks.push(chunkDecode(data).payload[0]);
  });
  const uni = await sideStream(end, 'up', 6);
  await uni.write(new Uint8Array([1]));
  await waitFor(() => chunks.length === 1, 'flowing');
  end.socket.pause();
  for (let i = 2; i <= 5; i++) await uni.write(new Uint8Array([i]));
  await timers.setTimeout(30);
  // At most the read already in flight: the pause used to stop the
  // control stream only, and an upload kept flowing around it.
  assert.ok(chunks.length <= 2, `paused: ${chunks.length} chunks arrived`);
  end.socket.resume();
  await waitFor(() => chunks.length === 5, 'resumed');
  assert.deepStrictEqual(chunks, [1, 2, 3, 4, 5]);
  // A close under a pause releases the gated reads, and nothing arrives
  // after it: the read in flight may still deliver one chunk (6), the
  // next one (7) finds the gate and then the close.
  end.socket.pause();
  await uni.write(new Uint8Array([6]));
  await timers.setTimeout(20);
  await uni.write(new Uint8Array([7]));
  end.socket.terminate();
  await timers.setTimeout(20);
  assert.ok(chunks.length <= 6, `${chunks.length} chunks`);
  assert.ok(!chunks.includes(7), 'nothing after the close');
  assert.ok(await end.client.closed);
});

test('wt socket: bytes on a side stream re-arm idleTimeout like bytes on the control stream', async (t) => {
  const end = await pair(t, { idleTimeout: 80 });
  const closes = [];
  end.socket.on('close', (code) => closes.push(code));
  const uni = await sideStream(end, 'live', 100);
  // Nothing on the control stream for 300 ms, a chunk on the side stream
  // every 20: a session busy with an upload used to idle out.
  const started = Date.now();
  while (Date.now() - started < 300) {
    await uni.write(new Uint8Array([7]));
    await timers.setTimeout(20);
  }
  assert.deepStrictEqual(closes, [], 'an upload in progress is not idle');
  await waitFor(() => closes.length === 1, 'idle once the upload stopped');
  assert.deepStrictEqual(closes, [1006]);
});
