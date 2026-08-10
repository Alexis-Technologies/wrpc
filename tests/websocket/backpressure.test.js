'use strict';

const test = require('node:test');
const assert = require('node:assert');

const { Connection, FrameParser } = require('#ws');
const { MockSocket } = require('./mockSocket.js');
const { ServerTransport } = require('../../src/transport.js');
const { WrpcWritable } = require('../../src/streams.js');

const ServerWsTransport = ServerTransport.transport.ws;

const wsTransport = (connection) => new ServerWsTransport({ socket: { remoteAddress: '127.0.0.1' } }, connection);

test('Connection: bufferedAmount mirrors the socket writable buffer', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));
  assert.strictEqual(conn.bufferedAmount, 0);
  socket.writableLength = 1234;
  assert.strictEqual(conn.bufferedAmount, 1234);
  conn.terminate();
});

test('Connection: data sends report socket pressure and drain restores flow', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));

  assert.strictEqual(conn.sendText('fits'), true);

  socket.writeResult = false;
  assert.strictEqual(conn.sendText('overflows'), false);
  assert.strictEqual(conn.sendBinary(Buffer.from('overflows')), false);

  let drained = 0;
  conn.on('drain', () => drained++);
  socket.drain();
  assert.strictEqual(drained, 1);

  // drain is only re-emitted after another failed write
  socket.drain();
  assert.strictEqual(drained, 1);

  assert.strictEqual(conn.sendText('fits again'), true);
  conn.terminate();
});

test('Connection: maxBackpressure cap terminates the connection', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { maxBackpressure: 100 });
  const errors = [];
  conn.on('error', (error) => errors.push(error));

  socket.writableLength = 101;
  assert.strictEqual(conn.sendText('too much'), false);
  assert.strictEqual(errors.length, 1);
  assert.match(errors[0].message, /Backpressure limit exceeded/);
  assert.strictEqual(socket.destroyed, true);
});

test('Connection: pause/resume delegate to the socket', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));
  conn.pause();
  assert.strictEqual(socket.paused, true);
  conn.resume();
  assert.strictEqual(socket.paused, false);
  conn.terminate();
});

test('ServerWsTransport: write returns connection acceptance and re-emits drain', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));
  const transport = wsTransport(conn);

  assert.strictEqual(transport.write('data'), true);
  socket.writeResult = false;
  assert.strictEqual(transport.write('data'), false);

  let drained = 0;
  transport.on('drain', () => drained++);
  socket.drain();
  assert.strictEqual(drained, 1);
  conn.terminate();
});

test('WrpcWritable: write reports transport pressure and emits drain once released', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));
  const transport = wsTransport(conn);
  const writable = new WrpcWritable('stream-1', 'name', 100, transport);

  assert.strictEqual(writable.write(new Uint8Array([1, 2, 3])), true);

  socket.writeResult = false;
  assert.strictEqual(writable.write(new Uint8Array([4, 5, 6])), false);

  let drained = 0;
  writable.on('drain', () => drained++);
  socket.drain();
  assert.strictEqual(drained, 1);

  assert.strictEqual(writable.write(new Uint8Array([7])), true);
  conn.terminate();
});

test('WrpcWritable: eleven concurrently stalled streams on one transport do not throw', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));
  const transport = wsTransport(conn);

  socket.writeResult = false;
  const writables = [];
  for (let i = 0; i < 12; i++) {
    const writable = new WrpcWritable(`stream-${i}`, 'name', 100, transport);
    writables.push(writable);
    // Regression: the 11th stalled stream used to hit the transport
    // Emitter's default maxListeners cap of 10 and throw synchronously.
    assert.doesNotThrow(() => {
      assert.strictEqual(writable.write(new Uint8Array([i])), false);
    });
  }

  let drained = 0;
  for (const writable of writables) writable.on('drain', () => drained++);
  socket.drain();
  assert.strictEqual(drained, 12);
  conn.terminate();
});

test('WrpcWritable: transport close releases a stalled stream instead of hanging', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));
  const transport = wsTransport(conn);
  const writable = new WrpcWritable('stream-1', 'name', 100, transport);

  socket.writeResult = false;
  assert.strictEqual(writable.write(new Uint8Array([1])), false);

  const events = [];
  writable.on('drain', () => events.push('drain'));
  writable.on('close', () => events.push('close'));
  transport.emit('close'); // the peer went away mid-stall

  assert.deepStrictEqual(events, ['drain', 'close']);
  assert.strictEqual(writable.closed, true);
  // After close, writes are refused and never arm another drain wait
  socket.writeResult = true;
  assert.strictEqual(writable.write(new Uint8Array([2])), false);
  conn.terminate();
});

test('Connection: fragmented send splits payload at fragmentThreshold', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { fragmentThreshold: 4 });

  const message = '0123456789'; // 10 bytes -> 4 + 4 + 2
  assert.strictEqual(conn.sendText(message), true);
  assert.strictEqual(socket.writtenData.length, 3);

  const frames = socket.writtenData.map((buf) => FrameParser.parse(buf).value.frame);
  assert.deepStrictEqual(
    frames.map((f) => ({ fin: f.fin, opcode: f.opcode })),
    [
      { fin: false, opcode: 0x01 },
      { fin: false, opcode: 0x00 },
      { fin: true, opcode: 0x00 },
    ],
  );
  const payload = Buffer.concat(frames.map((f) => f.payload)).toString();
  assert.strictEqual(payload, message);
  conn.terminate();
});

test('Connection: payload at the threshold stays a single frame', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { fragmentThreshold: 10 });
  conn.sendText('0123456789');
  assert.strictEqual(socket.writtenData.length, 1);
  const frame = FrameParser.parse(socket.writtenData[0]).value.frame;
  assert.strictEqual(frame.fin, true);
  assert.strictEqual(frame.opcode, 0x01);
  conn.terminate();
});

test('Connection: emits ping event for inbound pings and still auto-pongs', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));
  const pings = [];
  conn.on('ping', (payload) => pings.push(Buffer.from(payload)));

  const { Frame } = require('#ws');
  const ping = Frame.ping(Buffer.from('hb'));
  ping.maskPayload();
  socket.emit('data', ping.toBuffer());

  assert.strictEqual(pings.length, 1);
  assert.strictEqual(pings[0].toString(), 'hb');
  const pong = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(pong.opcode, 0x0a);
  assert.strictEqual(pong.payload.toString(), 'hb');
  conn.terminate();
});
