'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const ws = require('#ws');
const { Connection, Frame, FrameParser } = ws;
const { OPCODES, CLOSE_CODES } = ws;
const { MockSocket } = require('./mock-socket.js');

// --- Write coalescing -----------------------------------------------------

const tick = () => new Promise((resolve) => process.nextTick(resolve));

const parseAll = (buffer) => {
  const frames = [];
  let rest = buffer;
  while (rest.length > 0) {
    const { value } = FrameParser.parse(rest);
    frames.push(value.frame);
    rest = rest.subarray(value.bytesUsed);
  }
  return frames;
};

// --- The turn budget ---------------------------------------------------------

const textFrames = (texts) =>
  Buffer.concat(
    texts.map((text) => {
      const frame = Frame.text(text);
      frame.maskPayload();
      return frame.toBuffer();
    }),
  );
const nextTurn = () => new Promise((resolve) => setImmediate(resolve));

describe('Connection', () => {
  it('should emit message on text frame', async () => {
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

  it('should send pong when ping received', async () => {
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

  it('should close on close frame', async () => {
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

  it('should end the socket after closeTimeout, and destroy it 200 ms later when it is still open', async () => {
    const sock = new MockSocket();
    // A peer that never hangs up: end() leaves the socket half-open.
    sock.end = () => void (sock.ended = true);
    const conn = new Connection(sock, Buffer.alloc(0), { closeTimeout: 100 });

    conn.sendClose(1000, 'bye');

    await new Promise((resolve) => setTimeout(resolve, 150));
    assert.strictEqual(sock.ended, true);
    assert.strictEqual(sock.destroyed, false);
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.strictEqual(sock.destroyed, true);
  });

  // The 200 ms grace after end() was an anonymous timer: a socket that closed
  // or was terminated in between still had it pending, holding the process.
  it('should drop the destroy grace once the socket closed or was terminated', async () => {
    const closed = new MockSocket();
    new Connection(closed, Buffer.alloc(0), { closeTimeout: 50 }).sendClose(1000, 'bye');
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert.strictEqual(closed.ended, true);
    assert.strictEqual(closed.destroyed, false, 'a socket that closed on its own is not destroyed again');

    const halfOpen = new MockSocket();
    halfOpen.end = () => void (halfOpen.ended = true);
    const conn = new Connection(halfOpen, Buffer.alloc(0), { closeTimeout: 50 });
    conn.sendClose(1000, 'bye');
    await new Promise((resolve) => setTimeout(resolve, 80));
    conn.terminate();
    assert.strictEqual(halfOpen.destroyed, true);
  });

  it('should hang up immediately when answering a peer close (RFC 6455 5.5.1)', () => {
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

  it('should not act on a frame pipelined behind a peer close', () => {
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

  it('should reject a fragmented message exceeding maxBuffer', () => {
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

  it('should ignore data frames after sending close', async () => {
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

  it('should respond to ping during the close handshake', () => {
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

  it('should terminate the connection on a socket error', async () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0));

    const error = await new Promise((resolve) => {
      conn.on('error', resolve);
      socket.emit('error', new Error('socket failure'));
    });

    assert.strictEqual(error.message, 'socket failure');
    assert.strictEqual(socket.destroyed, true);
  });

  it('should use the masked empty-frame fast path for client-mode sendPing/sendPong', () => {
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

  it('should return uniform booleans from the send methods', () => {
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

  it('should reject non-string/non-Buffer payloads in send()', () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0));
    assert.throws(() => conn.send(42), TypeError);
    conn.terminate();
  });

  it('should send a masked close instead of an unmasked one on a client-mode raw buffer overflow', () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0), { isClient: true, maxBuffer: 4 });
    conn.on('error', () => {});

    socket.emit('data', Buffer.alloc(10));

    const closeFrame = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
    assert.strictEqual(closeFrame.opcode, OPCODES.CLOSE);
    assert.strictEqual(closeFrame.masked, true);
  });

  it('should reject a masked frame received from the server in client mode', () => {
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

  it('should terminate immediately on a close frame received after we already sent our own', () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0));
    conn.sendClose(1000, 'bye');

    const closeFrame = Frame.close(1000, 'bye');
    closeFrame.maskPayload();
    socket.emit('data', closeFrame.toBuffer());

    assert.strictEqual(socket.destroyed, true);
  });

  it('should batch every write of one turn into one cork/uncork with coalesce', async () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0), { coalesce: true });
    assert.strictEqual(conn.sendText('one'), true);
    assert.strictEqual(conn.sendText('two'), true);
    assert.strictEqual(conn.sendBinary(Buffer.from('three')), true);
    assert.strictEqual(socket.corks, 1, 'corked once for the whole turn');
    assert.strictEqual(socket.uncorks, 0, 'not flushed yet');
    assert.strictEqual(socket.writtenData.length, 0);
    await tick();
    assert.strictEqual(socket.uncorks, 1);
    assert.strictEqual(socket.writtenData.length, 1, 'one flush carries all three frames');
    const frames = parseAll(socket.writtenData[0]);
    assert.deepStrictEqual(
      frames.map((frame) => [frame.opcode, frame.payload.toString()]),
      [
        [OPCODES.TEXT, 'one'],
        [OPCODES.TEXT, 'two'],
        [OPCODES.BINARY, 'three'],
      ],
    );
    // The next turn starts a new cork.
    conn.sendText('four');
    assert.strictEqual(socket.corks, 2);
    await tick();
    assert.strictEqual(socket.writtenData.length, 2);
    conn.terminate();
  });

  it('should write a small frame as one write with no cork at all without coalesce', () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0));
    conn.sendText('one');
    conn.sendText('two');
    assert.strictEqual(socket.corks, 0);
    assert.strictEqual(socket.writtenData.length, 2, 'header and payload travel in one buffer');
    assert.strictEqual(FrameParser.parse(socket.writtenData[1]).value.frame.payload.toString(), 'two');
    conn.terminate();
  });

  it('should write a payload above the single-write cutoff as header + payload', () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0));
    const big = Buffer.alloc(64 * 1024, 0x61);
    conn.sendBinary(big);
    // MockSocket concatenates a corked header+payload pair into one entry.
    assert.strictEqual(socket.corks, 1);
    assert.strictEqual(socket.uncorks, 1);
    const frame = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
    assert.strictEqual(frame.payload.length, big.length);
    assert.ok(frame.payload.equals(big));
    conn.terminate();
  });

  it('should mask a client-side single-buffer frame', () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0), { isClient: true });
    conn.sendText('from the client');
    const frame = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
    assert.strictEqual(frame.masked, true);
    frame.unmaskPayload();
    assert.strictEqual(frame.payload.toString(), 'from the client');
    conn.terminate();
  });

  it('should leave no dangling uncork after terminate inside a coalesced turn', async () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0), { coalesce: true });
    conn.sendText('going');
    conn.terminate();
    assert.strictEqual(socket.destroyed, true);
    await tick();
    assert.strictEqual(socket.uncorks, 0, 'a destroyed socket is not uncorked');
  });

  it('should not answer a ping after a protocol-error close (RFC 6455 7.1.7)', () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0));
    conn.on('error', () => {});
    const text = Frame.text('Hello, world!');
    text.maskPayload();
    socket.emit('data', text.toBuffer());
    // A reserved non-control opcode fails the connection...
    const reserved = new Frame(true, 0x06, false, Buffer.from('Hello, world!'), null);
    reserved.maskPayload();
    socket.emit('data', reserved.toBuffer());
    // ...and a ping in a later segment gets no pong, unlike a graceful close.
    // (Nothing echoes the text here: the only frame written is the Close.)
    const ping = Frame.ping();
    ping.maskPayload();
    socket.emit('data', ping.toBuffer());
    const frames = socket.writtenData.map((buf) => FrameParser.parse(buf).value.frame.opcode);
    assert.deepStrictEqual(frames, [OPCODES.CLOSE]);
    conn.terminate();
  });

  it('should still answer a ping after an app-initiated close', () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0));
    conn.sendClose(1000, 'bye');
    const ping = Frame.ping();
    ping.maskPayload();
    socket.emit('data', ping.toBuffer());
    const frames = socket.writtenData.map((buf) => FrameParser.parse(buf).value.frame.opcode);
    assert.deepStrictEqual(frames, [OPCODES.CLOSE, OPCODES.PONG]);
    conn.terminate();
  });

  it('should hand over turnBudget messages a turn, in order, and hold the socket meanwhile', async () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0), { turnBudget: { messages: 4 } });
    const seen = [];
    conn.on('message', (data) => seen.push(data.toString()));
    const texts = Array.from({ length: 10 }, (_, i) => `m${i}`);
    socket.emit('data', textFrames(texts));
    assert.deepStrictEqual(seen, texts.slice(0, 4), 'the budget of one turn');
    assert.strictEqual(socket.paused, true, 'reads no more this turn');
    assert.strictEqual(conn.isPaused, true, 'a heartbeat must not take a held connection for dead');
    await nextTurn();
    assert.deepStrictEqual(seen, texts.slice(0, 8));
    await nextTurn();
    assert.deepStrictEqual(seen, texts);
    await nextTurn();
    assert.strictEqual(socket.paused, false, 'taken up again');
    assert.strictEqual(conn.isPaused, false);
    conn.terminate();
  });

  it('should count bytes beside messages in turnBudget', async () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0), { turnBudget: { messages: 1000, bytes: 100 } });
    const seen = [];
    conn.on('message', (data) => seen.push(data.length));
    socket.emit('data', textFrames(Array.from({ length: 6 }, () => 'x'.repeat(60))));
    assert.strictEqual(seen.length, 2, 'the message that crosses the mark is the last of its turn');
    await nextTurn();
    assert.strictEqual(seen.length, 4);
    await nextTurn();
    assert.strictEqual(seen.length, 6);
    conn.terminate();
  });

  it("should let the application's pause() outlive the turn hold and still read a Close behind it", async () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0), { turnBudget: { messages: 2 } });
    const seen = [];
    conn.on('message', (data) => seen.push(data.toString()));
    const close = Frame.close(1000, '');
    close.maskPayload();
    socket.emit('data', Buffer.concat([textFrames(['a', 'b', 'c']), close.toBuffer()]));
    assert.deepStrictEqual(seen, ['a', 'b']);
    conn.pause();
    conn.resume();
    assert.strictEqual(socket.paused, true, 'held for the turn, not for the application');
    conn.pause();
    await nextTurn();
    // Frames already read are taken up whatever the socket's state: the third
    // message and the peer's Close arrive, the Close is answered.
    assert.deepStrictEqual(seen, ['a', 'b', 'c']);
    assert.strictEqual(socket.ended, true, 'the Close behind the hold was answered');
    conn.terminate();
  });

  it('should have no turnBudget by default — a bare connection delivers inside the data event', () => {
    const socket = new MockSocket();
    const conn = new Connection(socket, Buffer.alloc(0));
    let seen = 0;
    conn.on('message', () => seen++);
    socket.emit('data', textFrames(Array.from({ length: 2000 }, () => 'x')));
    assert.strictEqual(seen, 2000);
    assert.strictEqual(socket.paused, false);
    conn.terminate();
  });

  it('should refuse a turnBudget that is not a boolean or positive marks', () => {
    for (const turnBudget of [0, 'on', { messages: 0 }, { bytes: -1 }, { messages: 1.5 }]) {
      assert.throws(() => new Connection(new MockSocket(), Buffer.alloc(0), { turnBudget }), /turnBudget must be/);
    }
    assert.doesNotThrow(() => new Connection(new MockSocket(), Buffer.alloc(0), { turnBudget: true }).terminate());
  });
});
