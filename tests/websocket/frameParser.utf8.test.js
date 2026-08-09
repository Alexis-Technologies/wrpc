'use strict';

const test = require('node:test');
const assert = require('node:assert');

const { isValidUTF8 } = require('../../src/websocket/frameParser.js');

test('isValidUTF8: valid sequences of every length', () => {
  assert.strictEqual(isValidUTF8(Buffer.from('hello', 'utf8')), true); // 1-byte
  assert.strictEqual(isValidUTF8(Buffer.from('é', 'utf8')), true); // 2-byte
  assert.strictEqual(isValidUTF8(Buffer.from('€', 'utf8')), true); // 3-byte
  assert.strictEqual(isValidUTF8(Buffer.from('🚀', 'utf8')), true); // 4-byte
});

test('isValidUTF8: rejects overlong encodings', () => {
  assert.strictEqual(isValidUTF8(Buffer.from([0xc0, 0x80])), false); // overlong 2-byte NUL
  assert.strictEqual(isValidUTF8(Buffer.from([0xe0, 0x80, 0x80])), false); // overlong 3-byte
  assert.strictEqual(isValidUTF8(Buffer.from([0xf0, 0x80, 0x80, 0x80])), false); // overlong 4-byte
});

test('isValidUTF8: rejects truncated multi-byte sequences', () => {
  assert.strictEqual(isValidUTF8(Buffer.from([0xc3])), false); // 2-byte, missing continuation
  assert.strictEqual(isValidUTF8(Buffer.from([0xe2, 0x82])), false); // 3-byte, missing continuation
  assert.strictEqual(isValidUTF8(Buffer.from([0xf0, 0x9f, 0x9a])), false); // 4-byte, missing continuation
});

test('isValidUTF8: rejects invalid continuation bytes', () => {
  assert.strictEqual(isValidUTF8(Buffer.from([0xe2, 0x28, 0xac])), false); // 3-byte
  assert.strictEqual(isValidUTF8(Buffer.from([0xf0, 0x9f, 0x28, 0x80])), false); // 4-byte
});

test('isValidUTF8: rejects UTF-16 surrogate halves encoded as UTF-8', () => {
  assert.strictEqual(isValidUTF8(Buffer.from([0xed, 0xa0, 0x80])), false); // U+D800
});

test('isValidUTF8: rejects code points beyond U+10FFFF', () => {
  assert.strictEqual(isValidUTF8(Buffer.from([0xf4, 0x90, 0x80, 0x80])), false);
});

test('isValidUTF8: rejects an invalid leading byte', () => {
  assert.strictEqual(isValidUTF8(Buffer.from([0xff])), false);
});
