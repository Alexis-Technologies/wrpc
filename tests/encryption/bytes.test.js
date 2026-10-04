'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const bytes = require('../../src/encryption/bytes.js');

test('bytes: concat, utf8, isBytes', () => {
  assert.deepStrictEqual([...bytes.concat()], []);
  assert.deepStrictEqual([...bytes.concat(Uint8Array.of(1), new Uint8Array(0), Uint8Array.of(2, 3))], [1, 2, 3]);
  assert.deepStrictEqual([...bytes.utf8('ї')], [0xd1, 0x97]);
  assert.strictEqual(bytes.isBytes(Buffer.alloc(1)), true);
  assert.strictEqual(bytes.isBytes([1]), false);
});

test('bytes: equal folds every byte, and a length mismatch is simply unequal', () => {
  const a = Uint8Array.of(1, 2, 3, 4);
  assert.strictEqual(bytes.equal(a, Uint8Array.of(1, 2, 3, 4)), true);
  assert.strictEqual(bytes.equal(a, Uint8Array.of(0, 2, 3, 4)), false);
  assert.strictEqual(bytes.equal(a, Uint8Array.of(1, 2, 3, 5)), false);
  assert.strictEqual(bytes.equal(a, Uint8Array.of(1, 2, 3)), false);
  assert.strictEqual(bytes.equal(new Uint8Array(0), new Uint8Array(0)), true);
  assert.strictEqual(bytes.isZero(new Uint8Array(32)), true);
  assert.strictEqual(bytes.isZero(Uint8Array.of(0, 0, 1)), false);
});

test('bytes: base64url out, base64 of either alphabet in', () => {
  const raw = Uint8Array.from({ length: 64 }, (_, i) => (i * 37 + 250) & 0xff);
  const url = bytes.toBase64Url(raw);
  assert.strictEqual(url, Buffer.from(raw).toString('base64url'));
  assert.deepStrictEqual(bytes.fromBase64(url), raw);
  assert.deepStrictEqual(bytes.fromBase64(Buffer.from(raw).toString('base64')), raw);
  assert.deepStrictEqual(bytes.fromBase64('AA=='), Uint8Array.of(0));
  assert.deepStrictEqual(bytes.fromBase64(''), new Uint8Array(0));
  for (const bad of ['A', 'AAAAA', 'A=A', '@@@@', 'AA AA', 7, null, undefined]) {
    assert.strictEqual(bytes.fromBase64(bad), null, String(bad));
  }
});

test('bytes: hex both ways', () => {
  assert.strictEqual(bytes.toHex(Uint8Array.of(0, 15, 255)), '000fff');
  assert.deepStrictEqual(bytes.fromHex('000FfF'), Uint8Array.of(0, 15, 255));
  assert.deepStrictEqual(bytes.fromHex(''), new Uint8Array(0));
  for (const bad of ['0', '0g', ' 00', 0, null]) assert.strictEqual(bytes.fromHex(bad), null);
});

test('bytes: a counter nonce is four zero bytes and the counter, 64 bits big-endian', () => {
  assert.strictEqual(bytes.toHex(bytes.counterNonce(0)), '000000000000000000000000');
  assert.strictEqual(bytes.toHex(bytes.counterNonce(1)), '000000000000000000000001');
  assert.strictEqual(bytes.toHex(bytes.counterNonce(0xffffffff)), '0000000000000000ffffffff');
  assert.strictEqual(bytes.toHex(bytes.counterNonce(0x100000000)), '000000000000000100000000');
  assert.strictEqual(bytes.toHex(bytes.counterNonce(Number.MAX_SAFE_INTEGER)), '00000000001fffffffffffff');
  // A scratch buffer is reused, stale bytes and all
  const scratch = new Uint8Array(12).fill(0xee);
  assert.strictEqual(bytes.counterNonce(0x0102, scratch), scratch);
  assert.strictEqual(bytes.toHex(scratch), '000000000000000000000102');
});

test('bytes: the CSPRNG or a refusal — never Math.random', () => {
  const random = bytes.randomSource();
  assert.strictEqual(random(16).length, 16);
  assert.notDeepStrictEqual(random(16), random(16));
  for (const missing of [null, {}, { getRandomValues: true }]) {
    assert.throws(() => bytes.randomSource(missing), /crypto\.getRandomValues is required/);
  }
  assert.throws(() => bytes.requireSubtle(undefined, 'x25519'), /^TypeError: x25519: WebCrypto/);
  const subtle = globalThis.crypto.subtle;
  assert.strictEqual(bytes.requireSubtle(subtle, 'x'), subtle);
});
