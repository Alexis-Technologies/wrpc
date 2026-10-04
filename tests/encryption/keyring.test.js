'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { normalizeKeys, parseKey, generateKey, isKid, DEFAULT_KID } = require('../../src/encryption/keyring.js');
const { isKeyProvider } = require('../../src/encryption/contracts.js');
const { toHex, toBase64Url } = require('../../src/encryption/bytes.js');

const bytes = (fill) => new Uint8Array(32).fill(fill);

test('keys: one bare key is filed under kid 0, in every spelling of 32 bytes', () => {
  const raw = Uint8Array.from({ length: 32 }, (_, i) => i * 7);
  const spellings = [
    raw,
    Buffer.from(raw),
    toHex(raw),
    toHex(raw).toUpperCase(),
    toBase64Url(raw),
    Buffer.from(raw).toString('base64'),
  ];
  for (const spelling of spellings) {
    const ring = normalizeKeys(spelling);
    assert.strictEqual(ring.current, DEFAULT_KID);
    assert.deepStrictEqual([...ring.kids], ['0']);
    assert.strictEqual(toHex(ring.get('0')), toHex(raw));
    assert.ok(Object.isFrozen(ring));
  }
});

test('keys: the ring copies its bytes — wiping the input does not wipe the key', () => {
  const raw = bytes(9);
  const ring = normalizeKeys(raw);
  raw.fill(0);
  assert.strictEqual(toHex(ring.get('0')), toHex(bytes(9)));
});

test('keys: a ring names its current kid and keeps the others for opening', () => {
  const ring = normalizeKeys({ current: '2026-09', ring: { '2026-08': bytes(1), '2026-09': toHex(bytes(2)) } });
  assert.strictEqual(ring.current, '2026-09');
  assert.deepStrictEqual([...ring.kids], ['2026-08', '2026-09']);
  assert.ok(Object.isFrozen(ring.kids));
  assert.strictEqual(toHex(ring.get('2026-08')), toHex(bytes(1)));
  assert.strictEqual(toHex(ring.get('2026-09')), toHex(bytes(2)));
});

test('keys: a kid comes back from a peer — an unknown or inherited one is a miss, never an object', () => {
  const ring = normalizeKeys({ current: 'k1', ring: { k1: bytes(1) } });
  for (const kid of ['k2', '__proto__', 'constructor', 'toString', 'hasOwnProperty', '', undefined, null, 7]) {
    assert.strictEqual(ring.get(kid), null, String(kid));
  }
});

test('keys: strict — every malformed ring is a TypeError where it is built', () => {
  const cases = [
    [undefined, /keys must be 32 bytes/],
    [null, /keys must be 32 bytes/],
    [new Uint8Array(16), /keys must be 32 bytes/],
    ['too short', /keys must be 32 bytes/],
    ['z'.repeat(64), /keys must be 32 bytes/],
    ['!'.repeat(44), /keys must be 32 bytes/],
    [[bytes(1)], /keys must be 32 bytes/],
    [{ current: 'k1' }, /keys\.ring must be an object/],
    [{ current: 'k1', ring: [] }, /keys\.ring must be an object/],
    [{ current: 'k2', ring: { k1: bytes(1) } }, /names a key the ring does not hold/],
    [{ ring: { k1: bytes(1) } }, /keys\.current must be a key id/],
    [{ current: 'k1', ring: { k1: bytes(1), 'k:2': bytes(2) } }, /must be a key id/],
    [{ current: 'k1', ring: { k1: bytes(1), ['k'.repeat(33)]: bytes(2) } }, /must be a key id/],
    [{ current: 'k1', ring: { k1: new Uint8Array(31) } }, /keys\.ring\.k1 must be 32 bytes/],
  ];
  for (const [value, pattern] of cases) assert.throws(() => normalizeKeys(value), pattern);
  assert.throws(() => normalizeKeys(null, 'rooms.encryption.keys'), /^TypeError: rooms\.encryption\.keys must be/);
});

test('keys: a provider is read live, and only ever answers 32 bytes for a well-formed kid', () => {
  let current = 'a';
  const asked = [];
  const held = { a: bytes(1), b: bytes(2), short: new Uint8Array(8), text: 'not bytes' };
  const provider = {
    current: () => current,
    get: (kid) => {
      asked.push(kid);
      return held[kid] ?? null;
    },
  };
  assert.strictEqual(isKeyProvider(provider), true);
  const ring = normalizeKeys(provider);
  assert.strictEqual(ring.current, 'a');
  assert.deepStrictEqual(ring.kids, ['a']);
  current = 'b';
  assert.strictEqual(ring.current, 'b');
  assert.strictEqual(toHex(ring.get('b')), toHex(bytes(2)));
  assert.strictEqual(ring.get('short'), null);
  assert.strictEqual(ring.get('text'), null);
  assert.strictEqual(ring.get('missing'), null);
  // A malformed kid never reaches the provider
  assert.strictEqual(ring.get('../etc'), null);
  assert.strictEqual(ring.get('__proto__ '), null);
  assert.deepStrictEqual(asked, ['b', 'short', 'text', 'missing']);
  current = 'not a kid';
  assert.throws(() => ring.current, /keys\.current\(\) must be a key id/);

  const listed = normalizeKeys({ ...provider, current: () => 'b', kids: () => ['b', 'a', 'no good', 7] });
  assert.deepStrictEqual(listed.kids, ['b', 'a']);
  assert.deepStrictEqual(normalizeKeys({ ...provider, current: () => 'b', kids: () => null }).kids, ['b']);
  assert.strictEqual(isKeyProvider({ current: () => 'a' }), false);
  assert.strictEqual(isKeyProvider(null), false);
});

test('keys: parseKey, isKid and generateKey', () => {
  assert.strictEqual(toHex(parseKey(toHex(bytes(3)), 'k')), toHex(bytes(3)));
  assert.strictEqual(isKid('a.b_c-9'), true);
  for (const bad of ['', 'a b', 'a:b', 'ключ', 'k'.repeat(33), 1, null]) assert.strictEqual(isKid(bad), false);
  const a = generateKey();
  const b = generateKey();
  assert.strictEqual(a.length, 32);
  assert.notStrictEqual(toHex(a), toHex(b));
  assert.throws(() => generateKey({ crypto: null }), /crypto\.getRandomValues is required/);
  assert.throws(() => generateKey({ crypto: {} }), /crypto\.getRandomValues is required/);
});
