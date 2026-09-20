'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const crypto = require('node:crypto');

const { createKdf, HASH_LENGTH } = require('../../src/encryption/hkdf.js');
const { fromHex, toHex, utf8 } = require('../../src/encryption/bytes.js');

const kdf = createKdf();

test('hkdf: RFC 5869 test case 1 — extract and expand apart, and together', async () => {
  const ikm = new Uint8Array(22).fill(0x0b);
  const salt = fromHex('000102030405060708090a0b0c');
  const info = fromHex('f0f1f2f3f4f5f6f7f8f9');
  const prk = await kdf.extract(salt, ikm);
  assert.strictEqual(toHex(prk), '077709362c2e32df0ddc3f0dc47bba6390b6c73bb50f9c3122ec844ad7c2b3e5');
  const okm = '3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865';
  assert.strictEqual(toHex(await kdf.expand(prk, info, 42)), okm);
  assert.strictEqual(toHex(await kdf.derive(ikm, salt, info, 42)), okm);
});

test('hkdf: RFC 5869 test case 3 — an absent salt is HashLen zeros, an empty info is fine', async () => {
  const ikm = new Uint8Array(22).fill(0x0b);
  const none = new Uint8Array(0);
  assert.strictEqual(
    toHex(await kdf.extract(none, ikm)),
    '19ef24a32c717b167f33a91d6f648bdf96596776afdb6377ac434c1c293ccb04',
  );
  assert.strictEqual(
    toHex(await kdf.derive(ikm, none, none, 42)),
    '8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8',
  );
});

test('hkdf: agrees with node:crypto at every block boundary', async () => {
  const ikm = crypto.randomBytes(32);
  const salt = crypto.randomBytes(16);
  const info = utf8('wrpc rooms v1');
  for (const length of [1, 31, 32, 33, 64, 96, 255 * 32]) {
    const expected = Buffer.from(crypto.hkdfSync('sha256', ikm, salt, info, length)).toString('hex');
    assert.strictEqual(toHex(await kdf.derive(ikm, salt, info, length)), expected, `length ${length}`);
  }
});

test('hkdf: hash and hmac are SHA-256 and HMAC-SHA-256', async () => {
  const data = utf8('wrpc');
  const key = crypto.randomBytes(32);
  assert.strictEqual(toHex(await kdf.hash(data)), crypto.createHash('sha256').update(data).digest('hex'));
  assert.strictEqual(toHex(await kdf.hmac(key, data)), crypto.createHmac('sha256', key).update(data).digest('hex'));
  // The empty key crypto.subtle refuses to import is HMAC's own zero padding
  assert.strictEqual(
    toHex(await kdf.hmac(new Uint8Array(0), data)),
    crypto.createHmac('sha256', '').update(data).digest('hex'),
  );
});

test('hkdf: a length out of range is a RangeError; the facade is frozen; no WebCrypto is refused', async () => {
  const prk = new Uint8Array(32);
  for (const length of [0, -1, 1.5, 255 * 32 + 1, '32']) {
    await assert.rejects(kdf.expand(prk, new Uint8Array(0), length), RangeError);
  }
  assert.ok(Object.isFrozen(kdf));
  assert.deepStrictEqual([kdf.id, kdf.hashLength, HASH_LENGTH], ['SHA256', 32, 32]);
  assert.throws(() => createKdf({ subtle: null }), /WebCrypto \(crypto\.subtle\) is required/);
});
