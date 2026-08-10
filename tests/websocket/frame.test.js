'use strict';

const test = require('node:test');
const assert = require('node:assert');
const crypto = require('node:crypto');

const { Frame, FrameParser, OPCODES } = require('#ws');
const { applyMask } = require('../../src/websocket/frame.js');

test('applyMask: word-XOR fast path matches the byte-wise reference for every length and alignment', () => {
  const reference = (payload, mask) => {
    for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
  };
  const lengths = [0, 1, 2, 3, 4, 5, 7, 8, 9, 63, 64, 65, 1027];
  for (const length of lengths) {
    const mask = crypto.randomBytes(4);
    const data = crypto.randomBytes(length);
    const expected = Buffer.from(data);
    reference(expected, mask);
    // subarray views at every alignment — SegmentQueue hands out arbitrary offsets
    for (let offset = 0; offset < 4; offset++) {
      const buffer = Buffer.from(new ArrayBuffer(length + offset), offset, length);
      data.copy(buffer);
      applyMask(buffer, mask);
      assert.deepStrictEqual(buffer, expected, `length ${length}, byteOffset ${offset}`);
    }
  }
});

test('Frame: create and parse text frame', () => {
  const message = 'Hello tinyWS';
  const frame = Frame.text(message);

  const buffer = frame.toBuffer();
  const parsedFrame = FrameParser.parse(buffer).value.frame;

  assert.strictEqual(parsedFrame.opcode, OPCODES.TEXT);
  assert.strictEqual(parsedFrame.toString(), message);
});

test('Frame: mask and unmask payload', () => {
  const payload = Buffer.from('mask-test');
  const frame = Frame.text(payload);

  frame.maskPayload();
  assert.strictEqual(frame.masked, true);
  assert.strictEqual(frame.mask.length, 4);
  const maskedPayload = Buffer.from(frame.payload);

  frame.unmaskPayload();
  assert.strictEqual(frame.masked, false);
  assert.strictEqual(frame.toString(), 'mask-test');

  assert.notDeepStrictEqual(maskedPayload, frame.payload);
});

test('Frame: create binary frame', () => {
  const data = crypto.randomBytes(10);
  const frame = Frame.binary(data);
  const buffer = frame.toBuffer();
  const parsedFrame = FrameParser.parse(buffer).value.frame;

  assert.strictEqual(parsedFrame.opcode, OPCODES.BINARY);
  assert.deepStrictEqual(parsedFrame.payload, data);
});

test('Frame: extended 16-bit length', () => {
  const data = Buffer.alloc(200, 0x42); //B
  const frame = Frame.binary(data);
  const buffer = frame.toBuffer();
  const parsedFrame = FrameParser.parse(buffer).value.frame;

  assert.strictEqual(parsedFrame.payload.length, 200);
  assert.deepStrictEqual(parsedFrame.payload, data);
});

test('Frame: extended 64-bit length', () => {
  const size = 70 * 1024;
  const data = Buffer.alloc(size, 0x42); //B
  const frame = Frame.binary(data);
  const buffer = frame.toBuffer();
  const parsedFrame = FrameParser.parse(buffer).value.frame;

  assert.strictEqual(parsedFrame.payload.length, size);
  assert.deepStrictEqual(parsedFrame.payload, data);
});

test('Frame: truncates close reason to 123 bytes', () => {
  const reason = 'x'.repeat(200);
  const frame = Frame.close(1000, reason);

  assert.strictEqual(frame.payload.length, 2 + 123);
  const parsed = FrameParser.parse(frame.toBuffer()).value.frame;
  const check = FrameParser.checkControlFrame(parsed);
  assert.strictEqual(check.error, null);
});

test('Frame: ping/pong accept a string payload', () => {
  const ping = Frame.ping('hi');
  assert.strictEqual(ping.opcode, OPCODES.PING);
  assert.deepStrictEqual(ping.payload, Buffer.from('hi'));

  const pong = Frame.pong('yo');
  assert.strictEqual(pong.opcode, OPCODES.PONG);
  assert.deepStrictEqual(pong.payload, Buffer.from('yo'));
});

test('Frame: toString returns null for non-text frames', () => {
  const frame = Frame.binary(Buffer.from([1, 2, 3]));
  assert.strictEqual(frame.toString(), null);
});
