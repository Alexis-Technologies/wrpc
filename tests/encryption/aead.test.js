'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const node = require('../../src/encryption/aead.js');
const browser = require('../../src/encryption/aead.browser.js');
const { OpenError, isCipher, isCipherKey } = require('../../src/encryption/contracts.js');
const { fromHex, toHex, utf8 } = require('../../src/encryption/bytes.js');

const hex = (bytes) => toHex(new Uint8Array(bytes));

// The Galois/Counter Mode specification (McGrew & Viega), test case 16: a
// 256-bit key, a 96-bit IV, additional data.
const GCM = {
  key: fromHex('feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308'),
  nonce: fromHex('cafebabefacedbaddecaf888'),
  aad: fromHex('feedfacedeadbeeffeedfacedeadbeefabaddad2'),
  plaintext: fromHex(
    'd9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a72' +
      '1c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b39',
  ),
  sealed:
    '522dc1f099567d07f47f37a32a84427d643a8cdcbfe5c0c97598a2bd2555d1aa' +
    '8cb08e48590dbb3da7b08b1056828838c5f61e6393ba7a0abcc9f662' +
    '76fc6ece0f4e1768cddf8853bb2d551b',
};

// RFC 8439 §2.8.2.
const CHACHA = {
  key: fromHex('808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f'),
  nonce: fromHex('070000004041424344454647'),
  aad: fromHex('50515253c0c1c2c3c4c5c6c7'),
  plaintext: utf8(
    "Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.",
  ),
  head: 'd31a8d34648e60db7b86afbc53ef7ec2',
  tag: '1ae10b594f09e26a7e902ecbd0600691',
};

test('aead (node): AES-256-GCM answers the GCM specification vector, synchronously', () => {
  const key = node.aead().key(GCM.key);
  const sealed = key.seal(GCM.nonce, GCM.plaintext, GCM.aad);
  assert.ok(sealed instanceof Uint8Array, 'a plain value, not a promise');
  assert.strictEqual(hex(sealed), GCM.sealed);
  assert.strictEqual(hex(key.open(GCM.nonce, sealed, GCM.aad)), hex(GCM.plaintext));
});

test('aead (node): ChaCha20-Poly1305 answers RFC 8439 §2.8.2', () => {
  const key = node.aead({ algorithm: 'chacha20-poly1305' }).key(CHACHA.key);
  const sealed = key.seal(CHACHA.nonce, CHACHA.plaintext, CHACHA.aad);
  assert.strictEqual(sealed.length, CHACHA.plaintext.length + 16);
  assert.strictEqual(hex(sealed.subarray(0, 16)), CHACHA.head);
  assert.strictEqual(hex(sealed.subarray(sealed.length - 16)), CHACHA.tag);
  assert.strictEqual(hex(key.open(CHACHA.nonce, sealed, CHACHA.aad)), hex(CHACHA.plaintext));
});

test('aead (browser): AES-256-GCM over crypto.subtle answers the same vector, as promises', async () => {
  const pending = browser.aead().key(GCM.key);
  assert.ok(pending instanceof Promise);
  const key = await pending;
  assert.strictEqual(isCipherKey(key), true);
  const sealed = await key.seal(GCM.nonce, GCM.plaintext, GCM.aad);
  assert.strictEqual(hex(sealed), GCM.sealed);
  assert.strictEqual(hex(await key.open(GCM.nonce, sealed, GCM.aad)), hex(GCM.plaintext));
});

test('aead: the two halves open each other, with and without additional data', async () => {
  const raw = globalThis.crypto.getRandomValues(new Uint8Array(32));
  const nonce = globalThis.crypto.getRandomValues(new Uint8Array(12));
  const message = utf8(JSON.stringify({ type: 'call', id: 7, method: 'chat/send', args: { text: 'привіт' } }));
  const a = node.aead().key(raw);
  const b = await browser.aead().key(raw);
  for (const aad of [undefined, null, utf8('room:lobby')]) {
    assert.strictEqual(hex(await b.open(nonce, a.seal(nonce, message, aad), aad)), hex(message));
    assert.strictEqual(hex(a.open(nonce, await b.seal(nonce, message, aad), aad)), hex(message));
  }
  // An empty message is a tag and nothing else
  assert.strictEqual(a.seal(nonce, new Uint8Array(0)).length, 16);
  assert.strictEqual(a.open(nonce, await b.seal(nonce, new Uint8Array(0))).length, 0);
});

test('aead: anything that is not the sealed message fails as one OpenError', async () => {
  const raw = new Uint8Array(32).fill(7);
  const nonce = new Uint8Array(12).fill(1);
  const aad = utf8('channel');
  const keys = [node.aead().key(raw), node.aead({ algorithm: 'chacha20-poly1305' }).key(raw)];
  keys.push(await browser.aead().key(raw));
  for (const key of keys) {
    const sealed = Uint8Array.from(await key.seal(nonce, utf8('payload'), aad));
    const flipped = (index) => {
      const copy = Uint8Array.from(sealed);
      copy[index] ^= 1;
      return copy;
    };
    const attempts = [
      () => key.open(nonce, flipped(0), aad),
      () => key.open(nonce, flipped(sealed.length - 1), aad),
      () => key.open(nonce, sealed, utf8('another')),
      () => key.open(nonce, sealed),
      () => key.open(new Uint8Array(12).fill(2), sealed, aad),
      () => key.open(nonce, sealed.subarray(0, sealed.length - 1), aad),
      () => key.open(nonce, sealed.subarray(0, 15), aad),
      () => key.open(nonce, new Uint8Array(0), aad),
    ];
    for (const attempt of attempts) {
      // `async` folds the synchronous throw and the rejection into one shape
      const error = await (async () => attempt())().then(
        () => null,
        (reason) => reason,
      );
      assert.ok(error instanceof OpenError, `expected an OpenError, got ${error}`);
      assert.strictEqual(error.code, 'open');
      assert.strictEqual(error.message, 'encryption: the message does not open');
    }
    assert.strictEqual(hex(await key.open(nonce, sealed, aad)), hex(utf8('payload')));
  }
  const other = node.aead().key(new Uint8Array(32).fill(8));
  assert.throws(() => other.open(nonce, keys[0].seal(nonce, utf8('payload'), aad), aad), OpenError);
});

test('aead: a key is 32 bytes and a nonce 12, on both halves', async () => {
  for (const raw of [new Uint8Array(16), new Uint8Array(33), 'k'.repeat(32), null]) {
    assert.throws(() => node.aead().key(raw), /a key is 32 bytes/);
    await assert.rejects(browser.aead().key(raw), /a key is 32 bytes/);
  }
  const a = node.aead().key(new Uint8Array(32));
  const b = await browser.aead().key(new Uint8Array(32));
  assert.throws(() => a.seal(new Uint8Array(16), new Uint8Array(1)), /a nonce is 12 bytes/);
  assert.throws(() => a.open(new Uint8Array(8), new Uint8Array(32)), /a nonce is 12 bytes/);
  await assert.rejects(b.seal(new Uint8Array(16), new Uint8Array(1)), /a nonce is 12 bytes/);
  await assert.rejects(b.open(new Uint8Array(8), new Uint8Array(32)), /a nonce is 12 bytes/);
});

test('aead: the cipher is a frozen structural Cipher; an unknown name is a TypeError', () => {
  for (const half of [node, browser]) {
    const cipher = half.aead();
    assert.strictEqual(isCipher(cipher), true);
    assert.ok(Object.isFrozen(cipher));
    assert.deepStrictEqual(
      [cipher.id, cipher.keyLength, cipher.nonceLength, cipher.tagLength],
      ['aes-256-gcm', 32, 12, 16],
    );
    assert.deepStrictEqual([...half.ALGORITHMS], ['aes-256-gcm', 'chacha20-poly1305']);
    assert.throws(() => half.aead({ algorithm: 'aes-128-cbc' }), /unknown cipher "aes-128-cbc"/);
  }
  assert.strictEqual(node.aead({ algorithm: 'chacha20-poly1305' }).id, 'chacha20-poly1305');
});

test('aead (browser): ChaCha20-Poly1305 is not in WebCrypto — null under optional, a TypeError alone', () => {
  assert.strictEqual(browser.aead({ algorithm: 'chacha20-poly1305', optional: true }), null);
  assert.throws(() => browser.aead({ algorithm: 'chacha20-poly1305' }), /inject a Cipher/);
});

test('aead (browser): no WebCrypto is refused where the cipher is built', () => {
  assert.throws(() => browser.aead({ subtle: null }), /WebCrypto \(crypto\.subtle\) is required/);
  assert.throws(() => browser.aead({ subtle: {} }), /WebCrypto/);
});

test('isCipher / isCipherKey: structural', () => {
  const cipher = { id: 'x', keyLength: 32, nonceLength: 24, tagLength: 16, key: () => ({}) };
  assert.strictEqual(isCipher(cipher), true);
  for (const broken of [
    { ...cipher, id: '' },
    { ...cipher, id: 1 },
    { ...cipher, keyLength: 0 },
    { ...cipher, nonceLength: 1.5 },
    { ...cipher, tagLength: '16' },
    { ...cipher, key: null },
    null,
    'aes-256-gcm',
  ]) {
    assert.strictEqual(isCipher(broken), false);
  }
  assert.strictEqual(isCipherKey({ seal() {}, open() {} }), true);
  assert.strictEqual(isCipherKey({ seal() {} }), false);
  assert.strictEqual(isCipherKey(null), false);
});
