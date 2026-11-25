'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { isValidUTF8 } = require('../../src/websocket/frameParser.js');

describe('isValidUTF8', () => {
  it('should accept valid sequences of every length', () => {
    assert.strictEqual(isValidUTF8(Buffer.from('hello', 'utf8')), true); // 1-byte
    assert.strictEqual(isValidUTF8(Buffer.from('é', 'utf8')), true); // 2-byte
    assert.strictEqual(isValidUTF8(Buffer.from('€', 'utf8')), true); // 3-byte
    assert.strictEqual(isValidUTF8(Buffer.from('🚀', 'utf8')), true); // 4-byte
  });

  it('should reject overlong encodings', () => {
    assert.strictEqual(isValidUTF8(Buffer.from([0xc0, 0x80])), false); // overlong 2-byte NUL
    assert.strictEqual(isValidUTF8(Buffer.from([0xe0, 0x80, 0x80])), false); // overlong 3-byte
    assert.strictEqual(isValidUTF8(Buffer.from([0xf0, 0x80, 0x80, 0x80])), false); // overlong 4-byte
  });

  it('should reject truncated multi-byte sequences', () => {
    assert.strictEqual(isValidUTF8(Buffer.from([0xc3])), false); // 2-byte, missing continuation
    assert.strictEqual(isValidUTF8(Buffer.from([0xe2, 0x82])), false); // 3-byte, missing continuation
    assert.strictEqual(isValidUTF8(Buffer.from([0xf0, 0x9f, 0x9a])), false); // 4-byte, missing continuation
  });

  it('should reject invalid continuation bytes', () => {
    assert.strictEqual(isValidUTF8(Buffer.from([0xe2, 0x28, 0xac])), false); // 3-byte
    assert.strictEqual(isValidUTF8(Buffer.from([0xf0, 0x9f, 0x28, 0x80])), false); // 4-byte
  });

  it('should reject UTF-16 surrogate halves encoded as UTF-8', () => {
    assert.strictEqual(isValidUTF8(Buffer.from([0xed, 0xa0, 0x80])), false); // U+D800
  });

  it('should reject code points beyond U+10FFFF', () => {
    assert.strictEqual(isValidUTF8(Buffer.from([0xf4, 0x90, 0x80, 0x80])), false);
  });

  it('should reject an invalid leading byte', () => {
    assert.strictEqual(isValidUTF8(Buffer.from([0xff])), false);
  });
});
