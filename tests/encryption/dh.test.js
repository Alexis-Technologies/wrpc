'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const nodeCrypto = require('node:crypto');

const node = require('../../src/encryption/dh.js');
const browser = require('../../src/encryption/dh.browser.js');
const { isDh } = require('../../src/encryption/contracts.js');
const { fromHex, toHex } = require('../../src/encryption/bytes.js');

const HALVES = [
  ['node', node.x25519],
  ['browser', browser.x25519],
];

// RFC 7748 §6.1.
const ALICE = {
  seed: fromHex('77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a'),
  publicKey: '8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a',
};
const BOB = {
  seed: fromHex('5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb'),
  publicKey: 'de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f',
};
const SHARED = '4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742';

for (const [half, x25519] of HALVES) {
  test(`x25519 (${half}): a key pair from a seed and the shared secret answer RFC 7748 §6.1`, async () => {
    const dh = x25519();
    const seed = Uint8Array.from(ALICE.seed);
    const alice = await dh.keyPair(seed);
    const bob = await dh.keyPair(BOB.seed);
    assert.strictEqual(toHex(alice.publicKey), ALICE.publicKey);
    assert.strictEqual(toHex(bob.publicKey), BOB.publicKey);
    assert.strictEqual(toHex(await dh.dh(alice.privateKey, bob.publicKey)), SHARED);
    assert.strictEqual(toHex(await dh.dh(bob.privateKey, alice.publicKey)), SHARED);
    // The caller's seed is the caller's: not wiped, not kept
    assert.strictEqual(toHex(seed), toHex(ALICE.seed));
  });

  test(`x25519 (${half}): generated key pairs agree, and no two are alike`, async () => {
    const dh = x25519();
    const [a, b] = await Promise.all([dh.generateKeyPair(), dh.generateKeyPair()]);
    assert.ok(a.publicKey instanceof Uint8Array);
    assert.strictEqual(a.publicKey.length, 32);
    assert.notStrictEqual(toHex(a.publicKey), toHex(b.publicKey));
    assert.strictEqual(toHex(await dh.dh(a.privateKey, b.publicKey)), toHex(await dh.dh(b.privateKey, a.publicKey)));
  });

  test(`x25519 (${half}): a low-order public key is refused, with one error for every cause`, async () => {
    const dh = x25519();
    const { privateKey } = await dh.generateKeyPair();
    const one = new Uint8Array(32);
    one[0] = 1;
    for (const point of [new Uint8Array(32), one]) {
      await assert.rejects(dh.dh(privateKey, point), { message: 'encryption: invalid public key' });
    }
  });

  test(`x25519 (${half}): keys are 32 bytes`, async () => {
    const dh = x25519();
    const { privateKey } = await dh.generateKeyPair();
    for (const bad of [new Uint8Array(31), new Uint8Array(33), 'a'.repeat(32), null]) {
      await assert.rejects(dh.dh(privateKey, bad), /public key is 32 bytes/);
      await assert.rejects(dh.keyPair(bad), /private key is 32 bytes/);
    }
  });

  test(`x25519 (${half}): a frozen structural Dh, named as Noise names it`, () => {
    const dh = x25519();
    assert.strictEqual(isDh(dh), true);
    assert.ok(Object.isFrozen(dh));
    assert.deepStrictEqual([dh.id, dh.publicLength], ['25519', 32]);
  });
}

test('x25519: the two halves agree with each other — a Node server and a browser client', async () => {
  const server = node.x25519();
  const client = browser.x25519();
  const s = await server.generateKeyPair();
  const c = await client.generateKeyPair();
  assert.strictEqual(
    toHex(await server.dh(s.privateKey, c.publicKey)),
    toHex(await client.dh(c.privateKey, s.publicKey)),
  );
});

test('x25519 (browser): a private key is a non-extractable CryptoKey; no WebCrypto is refused', async () => {
  const dh = browser.x25519();
  assert.strictEqual((await dh.generateKeyPair()).privateKey.extractable, false);
  assert.strictEqual((await dh.keyPair(ALICE.seed)).privateKey.extractable, false);
  assert.throws(() => browser.x25519({ subtle: null }), /WebCrypto \(crypto\.subtle\) is required/);
});

test('x25519: a platform that ANSWERS the zeros instead of throwing ends the same way', async () => {
  const point = new Uint8Array(32).fill(3);
  const subtle = { importKey: async () => ({}), deriveBits: async () => new ArrayBuffer(32) };
  await assert.rejects(browser.x25519({ subtle }).dh({}, point), { message: 'encryption: invalid public key' });
  const crypto = { ...nodeCrypto, diffieHellman: () => Buffer.alloc(32) };
  const { privateKey } = await node.x25519().generateKeyPair();
  await assert.rejects(node.x25519({ crypto }).dh(privateKey, point), { message: 'encryption: invalid public key' });
});

test('isDh: structural', () => {
  const dh = node.x25519();
  for (const broken of [
    { ...dh, id: '' },
    { ...dh, publicLength: 0 },
    { ...dh, keyPair: null },
    { ...dh, dh: 1 },
    null,
  ]) {
    assert.strictEqual(isDh(broken), false);
  }
  assert.strictEqual(isDh({ ...dh, generateKeyPair: undefined }), false);
});
