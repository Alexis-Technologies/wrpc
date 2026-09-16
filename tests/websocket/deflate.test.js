'use strict';

const test = require('node:test');
const assert = require('node:assert');
const zlib = require('node:zlib');

const { Connection, Frame, FrameParser, RSV1, OPCODES, CLOSE_CODES } = require('#ws');
const { negotiate, parseExtensions, compress, decompress } = require('../../src/websocket/permessageDeflate.js');
const { MockSocket } = require('./mockSocket.js');

const DEFLATE = { response: '', threshold: 1, windowBits: 15 };

const deliverCompressedText = (socket, text) => {
  const payload = compress(Buffer.from(text));
  const frame = new Frame(true, OPCODES.TEXT, false, payload, null, RSV1);
  frame.maskPayload();
  socket.emit('data', frame.toBuffer());
};

test('negotiate: plain offer is accepted with both no-context-takeover params', () => {
  const result = negotiate('permessage-deflate', {});
  assert.strictEqual(result.response, 'permessage-deflate; server_no_context_takeover; client_no_context_takeover');
  assert.strictEqual(result.windowBits, 15);
  assert.strictEqual(result.threshold, 1024);
});

test('negotiate: honors server_max_window_bits and echoes it back', () => {
  const result = negotiate('permessage-deflate; server_max_window_bits=10', { threshold: 64 });
  assert.match(result.response, /server_max_window_bits=10/);
  assert.strictEqual(result.windowBits, 10);
  assert.strictEqual(result.threshold, 64);
});

test('negotiate: skips unusable offers and falls back to the next one', () => {
  const header = 'permessage-deflate; server_max_window_bits=8, permessage-deflate';
  const result = negotiate(header, {});
  assert.ok(result);
  assert.strictEqual(result.windowBits, 15);
});

test('negotiate: rejects unknown params, bad window bits, and other extensions', () => {
  assert.strictEqual(negotiate('permessage-deflate; unknown_param', {}), null);
  assert.strictEqual(negotiate('permessage-deflate; server_max_window_bits', {}), null);
  assert.strictEqual(negotiate('permessage-deflate; server_max_window_bits=99', {}), null);
  assert.strictEqual(negotiate('permessage-deflate; client_max_window_bits=abc', {}), null);
  assert.strictEqual(negotiate('x-webkit-deflate-frame', {}), null);
  assert.strictEqual(negotiate('', {}), null);
});

test('parseExtensions: grammar violations return null, duplicates invalidate the offer', () => {
  // True grammar violations (RFC 6455 9.1) -> null -> handshake must 400
  assert.strictEqual(parseExtensions('permessage-deflate; ='), null);
  assert.strictEqual(parseExtensions('permessage-deflate; a='), null);
  assert.strictEqual(parseExtensions(',permessage-deflate'), null);
  // Duplicate params are grammar-valid but make the offer unacceptable
  // (RFC 7692 7): the offer is declined, not the whole handshake failed
  const offers = parseExtensions('permessage-deflate; client_no_context_takeover; client_no_context_takeover');
  assert.strictEqual(offers.length, 1);
  assert.strictEqual(offers[0].valid, false);
  assert.strictEqual(negotiate('permessage-deflate; client_no_context_takeover; client_no_context_takeover', {}), null);
});

test('negotiate: grammar-violating header reports malformed for a 400 handshake failure', () => {
  assert.deepStrictEqual(negotiate('permessage-deflate; =', {}), { malformed: true });
  assert.deepStrictEqual(negotiate(',permessage-deflate', {}), { malformed: true });
});

test('compress/decompress: round-trip with stripped trailer', () => {
  const source = Buffer.from('data '.repeat(1000));
  const compressed = compress(source);
  assert.ok(compressed.length < source.length);
  const restored = decompress(compressed, 1024 * 1024);
  assert.deepStrictEqual(restored, source);
});

test('Connection: inflates a compressed text message', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: DEFLATE });
  const messages = [];
  conn.on('message', (data, isBinary) => messages.push({ data, isBinary }));

  deliverCompressedText(socket, 'compressed hello');

  assert.strictEqual(messages.length, 1);
  assert.strictEqual(messages[0].data.toString(), 'compressed hello');
  assert.strictEqual(messages[0].isBinary, false);
  conn.terminate();
});

test('Connection: inflates a fragmented compressed message', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: DEFLATE });
  const messages = [];
  conn.on('message', (data) => messages.push(data));

  const payload = compress(Buffer.from('fragmented compressed payload'));
  const half = Math.ceil(payload.length / 2);
  const first = new Frame(false, OPCODES.TEXT, false, payload.subarray(0, half), null, RSV1);
  first.maskPayload();
  socket.emit('data', first.toBuffer());
  const second = new Frame(true, OPCODES.CONTINUATION, false, payload.subarray(half), null);
  second.maskPayload();
  socket.emit('data', second.toBuffer());

  assert.strictEqual(messages.length, 1);
  assert.strictEqual(messages[0].toString(), 'fragmented compressed payload');
  conn.terminate();
});

test('Connection: compresses outgoing messages above the threshold', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: { ...DEFLATE, threshold: 8 } });

  conn.sendText('a very repetitive payload '.repeat(50));

  const frame = FrameParser.parse(socket.writtenData.at(-1), { allowedRsv: RSV1 }).value.frame;
  assert.strictEqual(frame.rsv & RSV1, RSV1);
  const inflated = decompress(frame.payload, 1024 * 1024);
  assert.strictEqual(inflated.toString(), 'a very repetitive payload '.repeat(50));
  conn.terminate();
});

test('Connection: leaves short outgoing messages uncompressed', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: { ...DEFLATE, threshold: 1024 } });

  conn.sendText('short');

  const frame = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(frame.rsv, 0);
  assert.strictEqual(frame.payload.toString(), 'short');
  conn.terminate();
});

test('Connection: invalid deflate stream closes with 1007', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: DEFLATE });
  conn.on('error', () => {});

  const frame = new Frame(true, OPCODES.TEXT, false, Buffer.from('not deflate data'), null, RSV1);
  frame.maskPayload();
  socket.emit('data', frame.toBuffer());

  const close = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(close.opcode, OPCODES.CLOSE);
  assert.strictEqual(close.payload.readUInt16BE(0), CLOSE_CODES.INVALID_PAYLOAD);
});

test('Connection: inflated payload above maxBuffer closes with 1009', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: DEFLATE, maxBuffer: 1024 });
  conn.on('error', () => {});

  // 1 MB of zeros deflates to ~1 KB of wire bytes but inflates above maxBuffer
  const bomb = zlib.deflateRawSync(Buffer.alloc(1024 * 1024), {
    finishFlush: zlib.constants.Z_SYNC_FLUSH,
  });
  const payload = bomb.subarray(0, bomb.length - 4);
  const frame = new Frame(true, OPCODES.TEXT, false, payload, null, RSV1);
  frame.maskPayload();
  socket.emit('data', frame.toBuffer());

  const close = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(close.opcode, OPCODES.CLOSE);
  assert.strictEqual(close.payload.readUInt16BE(0), CLOSE_CODES.MESSAGE_TOO_BIG);
});

test('Connection: compressed UTF-8 is validated after inflation', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: DEFLATE });
  conn.on('error', () => {});

  const invalid = compress(Buffer.from([0xff, 0xfe, 0xfd]));
  const frame = new Frame(true, OPCODES.TEXT, false, invalid, null, RSV1);
  frame.maskPayload();
  socket.emit('data', frame.toBuffer());

  const close = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(close.opcode, OPCODES.CLOSE);
  assert.strictEqual(close.payload.readUInt16BE(0), CLOSE_CODES.INVALID_PAYLOAD);
});

test('Connection: RSV1 on a continuation frame is a protocol error', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: DEFLATE });
  conn.on('error', () => {});

  const first = new Frame(false, OPCODES.TEXT, false, compress(Buffer.from('x')), null, RSV1);
  first.maskPayload();
  socket.emit('data', first.toBuffer());
  const bad = new Frame(true, OPCODES.CONTINUATION, false, Buffer.from('y'), null, RSV1);
  bad.maskPayload();
  socket.emit('data', bad.toBuffer());

  const close = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(close.opcode, OPCODES.CLOSE);
  assert.strictEqual(close.payload.readUInt16BE(0), CLOSE_CODES.PROTOCOL_ERROR);
});

test('Connection: RSV1 without negotiated deflate stays a protocol error', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0));
  conn.on('error', () => {});

  const frame = new Frame(true, OPCODES.TEXT, false, compress(Buffer.from('x')), null, RSV1);
  frame.maskPayload();
  socket.emit('data', frame.toBuffer());

  const close = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(close.opcode, OPCODES.CLOSE);
  assert.strictEqual(close.payload.readUInt16BE(0), CLOSE_CODES.PROTOCOL_ERROR);
});

test('Connection: a compression bomb is capped by maxPayload, not maxBuffer', () => {
  const socket = new MockSocket();
  // A few KB on the wire inflating to ~4 MB; maxPayload of 1 MB must stop
  // the inflation even though maxBuffer (wire bytes) is far larger.
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: DEFLATE, maxPayload: 1024 * 1024 });
  const errors = [];
  const messages = [];
  conn.on('error', (error) => errors.push(error));
  conn.on('message', (data) => messages.push(data));

  const bomb = compress(Buffer.alloc(4 * 1024 * 1024, 0x61));
  assert.ok(bomb.length < 16 * 1024, 'the bomb must be small on the wire');
  const frame = new Frame(true, OPCODES.TEXT, false, bomb, null, RSV1);
  frame.maskPayload();
  socket.emit('data', frame.toBuffer());

  assert.strictEqual(messages.length, 0, 'the inflated payload must never surface');
  assert.strictEqual(errors.length, 1);
  const close = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(close.opcode, OPCODES.CLOSE);
  assert.strictEqual(close.payload.readUInt16BE(0), CLOSE_CODES.MESSAGE_TOO_BIG);
});

// --- Shared (prepared) frames: the fan-out path -------------------------

const { PreparedFrames } = require('../../src/websocket/prepared.js');

const lastFrame = (socket, allowedRsv = RSV1) =>
  FrameParser.parse(socket.writtenData.at(-1), { allowedRsv }).value.frame;

const sharedMessage = (text, compress = true) => ({ text, frames: null, compress });

test('sendPrepared: one message, two windows — deflated once per window, both peers decode it', () => {
  const wide = new MockSocket();
  const narrow = new MockSocket();
  const a = new Connection(wide, Buffer.alloc(0), { deflate: { ...DEFLATE, windowBits: 15 } });
  const b = new Connection(narrow, Buffer.alloc(0), { deflate: { ...DEFLATE, windowBits: 10 } });
  // A repeat 2 KB back: a 32 KB window (15) references it, a 1 KB window
  // (10) cannot — so the two windows must produce different bytes.
  const chunk = require('node:crypto').randomBytes(1536).toString('base64');
  const text = JSON.stringify({ type: 'event', name: 'chat/message', data: chunk + chunk });
  const message = sharedMessage(text);

  assert.strictEqual(a.sendPrepared(message), true);
  assert.strictEqual(b.sendPrepared(message), true);

  assert.ok(message.frames instanceof PreparedFrames, 'the engine claims the slot with its own cache');
  const wideFrame = lastFrame(wide);
  const narrowFrame = lastFrame(narrow);
  assert.strictEqual(wideFrame.rsv & RSV1, RSV1);
  assert.strictEqual(narrowFrame.rsv & RSV1, RSV1);
  assert.strictEqual(decompress(wideFrame.payload, 1 << 20).toString(), text);
  assert.strictEqual(decompress(narrowFrame.payload, 1 << 20).toString(), text);
  // Different windows, different bytes — and each window's frame is built
  // once: the same buffer comes back on every later call.
  assert.notDeepStrictEqual(message.frames.deflated(15), message.frames.deflated(10));
  assert.strictEqual(message.frames.deflated(15), message.frames.deflated(15));
  assert.strictEqual(message.frames.plain(), message.frames.plain());
  a.terminate();
  b.terminate();
});

test('sendPrepared: two connections with the same window receive byte-identical frames', () => {
  const sockets = [new MockSocket(), new MockSocket()];
  const conns = sockets.map((socket) => new Connection(socket, Buffer.alloc(0), { deflate: DEFLATE }));
  const message = sharedMessage('identical bytes '.repeat(64));
  for (const conn of conns) conn.sendPrepared(message);
  assert.deepStrictEqual(sockets[0].writtenData.at(-1), sockets[1].writtenData.at(-1));
  assert.strictEqual(lastFrame(sockets[0]).rsv & RSV1, RSV1);
  for (const conn of conns) conn.terminate();
});

test('sendPrepared: the threshold is per connection, the bytes are per window', () => {
  const eager = new MockSocket();
  const lazy = new MockSocket();
  const a = new Connection(eager, Buffer.alloc(0), { deflate: { ...DEFLATE, threshold: 8 } });
  const b = new Connection(lazy, Buffer.alloc(0), { deflate: { ...DEFLATE, threshold: 1 << 20 } });
  const message = sharedMessage('over eight bytes, under a megabyte');
  a.sendPrepared(message);
  b.sendPrepared(message);
  assert.strictEqual(lastFrame(eager).rsv & RSV1, RSV1);
  const plain = lastFrame(lazy);
  assert.strictEqual(plain.rsv, 0);
  assert.strictEqual(plain.payload.toString(), message.text);
  a.terminate();
  b.terminate();
});

test('sendPrepared: compress:false on the message sends the plain frame past the threshold', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: DEFLATE });
  const message = sharedMessage('never compressed '.repeat(64), false);
  conn.sendPrepared(message);
  const frame = lastFrame(socket);
  assert.strictEqual(frame.rsv, 0);
  assert.strictEqual(frame.payload.toString(), message.text);
  conn.terminate();
});

test('sendPrepared: a slot claimed by another engine falls back to an ordinary send', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: DEFLATE });
  const foreign = { encoded: true };
  const message = { text: 'mixed room '.repeat(32), frames: foreign, compress: true };
  assert.strictEqual(conn.sendPrepared(message), true);
  assert.strictEqual(message.frames, foreign, 'the foreign cache is left alone');
  const frame = lastFrame(socket);
  assert.strictEqual(decompress(frame.payload, 1 << 20).toString(), message.text);
  conn.terminate();
});

test('sendPrepared: a fragmenting connection fragments; a client connection masks', () => {
  const fragmenting = new MockSocket();
  const a = new Connection(fragmenting, Buffer.alloc(0), { fragmentThreshold: 16 });
  const message = sharedMessage('x'.repeat(40));
  a.sendPrepared(message);
  assert.strictEqual(message.frames, null, 'no shared frame is built for a fragmented send');
  assert.strictEqual(fragmenting.writtenData.length, 3, '40 bytes at a 16-byte threshold: three fragments');
  const first = FrameParser.parse(fragmenting.writtenData[0]).value;
  assert.strictEqual(first.frame.fin, false);
  assert.strictEqual(first.frame.opcode, OPCODES.TEXT);
  a.terminate();

  const clientSide = new MockSocket();
  const b = new Connection(clientSide, Buffer.alloc(0), { isClient: true });
  const shared = sharedMessage('masked by the client');
  b.sendPrepared(shared);
  const frame = FrameParser.parse(clientSide.writtenData.at(-1)).value.frame;
  assert.strictEqual(frame.masked, true);
  frame.unmaskPayload();
  assert.strictEqual(frame.payload.toString(), 'masked by the client');
  b.terminate();
});

test('sendPrepared: reports backpressure like send() and refuses after close', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: DEFLATE });
  socket.writeResult = false;
  assert.strictEqual(conn.sendPrepared(sharedMessage('slow peer')), false);
  socket.writeResult = true;
  conn.sendClose(1000, 'bye');
  assert.strictEqual(conn.sendPrepared(sharedMessage('too late')), false);
  conn.terminate();
});

test('send: compress:false is honoured for unicast text and binary', () => {
  const socket = new MockSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { deflate: { ...DEFLATE, threshold: 8 } });
  const text = 'a very repetitive payload '.repeat(50);
  conn.send(text, { compress: false });
  const plainText = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(plainText.rsv, 0);
  assert.strictEqual(plainText.payload.toString(), text);
  conn.sendBinary(Buffer.alloc(4096, 1), { compress: false });
  const plainBinary = FrameParser.parse(socket.writtenData.at(-1)).value.frame;
  assert.strictEqual(plainBinary.rsv, 0);
  assert.strictEqual(plainBinary.payload.length, 4096);
  conn.sendText(text);
  assert.strictEqual(lastFrame(socket).rsv & RSV1, RSV1, 'the default still compresses');
  conn.terminate();
});
