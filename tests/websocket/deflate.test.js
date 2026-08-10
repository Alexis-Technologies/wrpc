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
