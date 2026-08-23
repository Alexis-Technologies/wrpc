'use strict';

const test = require('node:test');
const assert = require('node:assert');

const ws = require('#ws');
const { Connection, Frame, FrameParser } = ws;
const { OPCODES, CLOSE_TIMEOUT, CLOSE_CODES } = ws;
const { MockSocket } = require('./mockSocket.js');

test('Connection: should emit message on text frame', async () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));

  await new Promise((resolve) => {
    conn.on('message', (msg, isBinary) => {
      assert.ok(Buffer.isBuffer(msg));
      assert.ok(msg.equals(Buffer.from('hello')));
      assert.strictEqual(isBinary, false);
      resolve();
    });

    conn.on('error', (err) => {
      assert.fail(`Unexpected error: ${err.message}`);
    });

    const frame = Frame.text('hello');
    frame.maskPayload();
    socket.emit('data', frame.toBuffer());
  });

  conn.terminate();
});

test('Connection: should send pong when ping received', async () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), {});

  const ping = Frame.ping();
  ping.maskPayload();
  socket.emit('data', ping.toBuffer());

  const lastWrite = socket.writtenData[socket.writtenData.length - 1];
  const frame = FrameParser.parse(lastWrite).value.frame;
  assert.strictEqual(frame.opcode, OPCODES.PONG);
  conn.terminate();
});

test('Connection: should close on close frame', async () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), {});

  return new Promise((resolve) => {
    conn.on('close', () => {
      assert.strictEqual(socket.ended, true);
      resolve();
    });
    const close = Frame.close();
    close.maskPayload();
    socket.emit('data', close.toBuffer());
  });
});

test('Connection: sendClose triggers socket end after CLOSE_TIMEOUT', () => {
  const sock = new MockSocket();
  const conn = new Connection(sock, Buffer.alloc(0), { closeTimeout: 100 });

  conn.sendClose(1000, 'bye');

  return new Promise((resolve) => {
    setTimeout(() => {
      assert.strictEqual(sock.destroyed, true);
      resolve();
    }, CLOSE_TIMEOUT + 100);
  });
});

test('Connection: answering a peer close hangs up immediately (RFC 6455 5.5.1)', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { closeTimeout: 60_000 });

  const close = Frame.close(1000, 'bye');
  close.maskPayload();
  socket.emit('data', close.toBuffer());

  const echo = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(echo.opcode, OPCODES.CLOSE);
  // The point of the change: the answering side does not wait out
  // closeTimeout for the initiator to hang up. Both peers waiting is what
  // used to cost a full second on every graceful disconnect.
  assert.strictEqual(socket.ended, true, 'the socket is half-closed as soon as the echo is written');
  conn.terminate();
});

test('Connection: a frame pipelined behind a peer close is not acted on', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { closeTimeout: 50 });
  const messages = [];
  conn.on('message', (data) => void messages.push(data));

  // One TCP segment carrying CLOSE then PING then a data frame. Answering
  // the ping would write past the end() the close echo just performed.
  const close = Frame.close(1000, 'bye');
  close.maskPayload();
  const ping = Frame.ping(Buffer.from('hi'));
  ping.maskPayload();
  const text = Frame.text('late');
  text.maskPayload();
  socket.emit('data', Buffer.concat([close.toBuffer(), ping.toBuffer(), text.toBuffer()]));

  const written = socket.writtenData.map((data) => FrameParser.parse(data).value.frame.opcode);
  assert.deepStrictEqual(written, [OPCODES.CLOSE], 'the close echo is the last thing written');
  assert.deepStrictEqual(messages, [], 'and nothing queued behind the close is delivered');
  conn.terminate();
});

test('Connection: rejects fragmented message exceeding maxBuffer', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { maxBuffer: 10 });
  conn.on('error', () => {});

  const first = Frame.text('abcde', false);
  first.maskPayload();
  socket.emit('data', first.toBuffer());

  const cont = new Frame(false, OPCODES.CONTINUATION, false, Buffer.from('abcde'), null);
  cont.maskPayload();
  socket.emit('data', cont.toBuffer());

  const lastWrite = socket.writtenData[socket.writtenData.length - 1];
  const closeFrame = FrameParser.parse(lastWrite).value.frame;
  assert.strictEqual(closeFrame.opcode, OPCODES.CLOSE);
  assert.strictEqual(closeFrame.payload.readUInt16BE(0), CLOSE_CODES.MESSAGE_TOO_BIG);
});

test('Connection: ignores data frames after sending close', async () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { closeTimeout: 50 });
  let messages = 0;

  conn.on('message', () => {
    messages++;
  });

  conn.sendClose(1000, 'bye');

  const text = Frame.text('late');
  text.maskPayload();
  socket.emit('data', text.toBuffer());

  assert.strictEqual(messages, 0);
  conn.terminate();
});

test('Connection: responds to ping during close handshake', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { closeTimeout: 50 });

  conn.sendClose(1000, 'bye');

  const ping = Frame.ping();
  ping.maskPayload();
  socket.emit('data', ping.toBuffer());

  const lastWrite = socket.writtenData[socket.writtenData.length - 1];
  const frame = FrameParser.parse(lastWrite).value.frame;
  assert.strictEqual(frame.opcode, OPCODES.PONG);
  conn.terminate();
});

test('Connection: socket error terminates connection', async () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));

  const error = await new Promise((resolve) => {
    conn.on('error', resolve);
    socket.emit('error', new Error('socket failure'));
  });

  assert.strictEqual(error.message, 'socket failure');
  assert.strictEqual(socket.destroyed, true);
});

test('Connection: client-mode sendPing/sendPong use the masked empty-frame fast path', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { isClient: true });

  conn.sendPing();
  const pingFrame = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(pingFrame.opcode, OPCODES.PING);
  assert.strictEqual(pingFrame.masked, true);
  assert.strictEqual(pingFrame.payload.length, 0);

  conn.sendPong();
  const pongFrame = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(pongFrame.opcode, OPCODES.PONG);
  assert.strictEqual(pongFrame.masked, true);
  assert.strictEqual(pongFrame.payload.length, 0);

  conn.terminate();
});

test('Connection: send methods return uniform booleans', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { closeTimeout: 50 });

  assert.strictEqual(conn.sendPing(), true);
  assert.strictEqual(conn.sendPing(Buffer.from('x')), true);
  assert.strictEqual(conn.sendPong(), true);
  assert.strictEqual(conn.sendPong(Buffer.from('x')), true);
  assert.strictEqual(conn.sendText('x'), true);
  assert.strictEqual(conn.sendBinary(Buffer.from('x')), true);

  conn.sendClose(1000, 'bye');

  assert.strictEqual(conn.sendPing(), false);
  assert.strictEqual(conn.sendPing(Buffer.from('x')), false);
  assert.strictEqual(conn.sendText('x'), false);
  assert.strictEqual(conn.sendBinary(Buffer.from('x')), false);
  // Pong stays available during the close handshake (RFC 6455 5.5.3)
  assert.strictEqual(conn.sendPong(), true);
  assert.strictEqual(conn.sendPong(Buffer.from('x')), true);

  conn.terminate();
});

test('Connection: send() rejects non-string/non-Buffer payloads', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));
  assert.throws(() => conn.send(42), TypeError);
  conn.terminate();
});

test('Connection: client-mode raw buffer overflow sends a masked close instead of an unmasked one', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { isClient: true, maxBuffer: 4 });
  conn.on('error', () => {});

  socket.emit('data', Buffer.alloc(10));

  const closeFrame = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(closeFrame.opcode, OPCODES.CLOSE);
  assert.strictEqual(closeFrame.masked, true);
});

test('Connection: client-mode rejects a masked frame received from the server', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { isClient: true });
  conn.on('error', () => {});

  const frame = Frame.text('hi');
  frame.maskPayload(); // servers must NOT mask frames sent to clients
  socket.emit('data', frame.toBuffer());

  const closeFrame = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(closeFrame.opcode, OPCODES.CLOSE);
  closeFrame.unmaskPayload();
  assert.strictEqual(closeFrame.payload.readUInt16BE(0), CLOSE_CODES.PROTOCOL_ERROR);
});

test('Connection: receiving a close frame after we already sent our own terminates immediately', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));
  conn.sendClose(1000, 'bye');

  const closeFrame = Frame.close(1000, 'bye');
  closeFrame.maskPayload();
  socket.emit('data', closeFrame.toBuffer());

  assert.strictEqual(socket.destroyed, true);
});
