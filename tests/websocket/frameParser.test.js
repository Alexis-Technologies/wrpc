'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const ws = require('#ws');
const { OPCODES, PARSE_ERR_CODES } = ws;
const { Frame, FrameParser } = ws;

const FIN = 0x80;
const LEN_64_BIT = 127;

describe('FrameParser', () => {
  it('should error on a payload length > MAX_SAFE_INTEGER', () => {
    const buffer = Buffer.alloc(14);
    buffer[0] = FIN | OPCODES.BINARY;
    buffer[1] = LEN_64_BIT;
    assert.strictEqual(buffer[0] & FIN, FIN);

    const bigValue = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    buffer.writeUInt32BE(Number(bigValue >> 32n), 2);
    buffer.writeUInt32BE(Number(bigValue & 0xffffffffn), 6);

    buffer.writeUInt32BE(0, 10);

    const result = FrameParser.parse(buffer);
    assert.ok(result.error, 'Expected parse result to contain an error');
    assert.strictEqual(result.error.name, 'ParseError');
    assert.strictEqual(result.error.code, PARSE_ERR_CODES.MESSAGE_TOO_BIG);
  });

  it('should return an empty result when the buffer is smaller than the header', () => {
    const buf = Buffer.alloc(1); // less than 2 bytes header
    const res = FrameParser.parse(buf);
    assert.strictEqual(res.value, null);
    assert.strictEqual(res.error, null);
  });

  it('should return empty when the mask is set but the mask bytes are missing', () => {
    // 0x81 = FIN + TEXT opcode, 0x80 = MASK bit set + 0 payload length
    const buf = Buffer.from([0x81, 0x80]);
    const res = FrameParser.parse(buf);
    assert.strictEqual(res.value, null);
    assert.strictEqual(res.error, null);
  });

  it('should recover the original of a masked frame with unmaskPayload', () => {
    const msg = 'ok';
    const frame = Frame.text(msg);
    frame.maskPayload();
    const buf = frame.toBuffer();

    const res = FrameParser.parse(buf);
    assert.ok(res.value, 'expected a value');
    const parsed = res.value.frame;
    assert.strictEqual(parsed.fin, true);
    assert.strictEqual(parsed.masked, true);
    parsed.unmaskPayload();
    assert.strictEqual(parsed.toString(), msg);

    const expectedBytes = buf.length;
    assert.strictEqual(res.value.bytesUsed, expectedBytes);
  });

  it('should parse the first of two concatenated frames and report bytesUsed', () => {
    const f1 = Frame.text('first');
    const f2 = Frame.text('second');
    const buf = Buffer.concat([f1.toBuffer(), f2.toBuffer()]);

    const res = FrameParser.parse(buf);
    assert.ok(res.value, 'expected a value for first frame');
    const first = res.value.frame;
    assert.strictEqual(first.toString(), 'first');
    assert.strictEqual(res.value.bytesUsed, f1.toBuffer().length);
  });
});
