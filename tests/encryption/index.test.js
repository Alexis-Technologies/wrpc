'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const node = require('../../encryption.js');
const browser = require('../../encryption.browser.js');

test('./encryption: the Node entry is a superset of the browser one', () => {
  for (const name of Object.keys(browser)) assert.ok(name in node, name);
  assert.strictEqual(typeof node.aead, 'function');
  assert.strictEqual(typeof node.x25519, 'function');
  assert.strictEqual(typeof node.createKdf, 'function');
  assert.strictEqual(typeof node.normalizeKeys, 'function');
});

test('./encryption: a sealed message under a keyring key, end to end over the primitives', async () => {
  const ring = node.normalizeKeys({ current: 'k1', ring: { k1: node.generateKey() } });
  const kdf = node.createKdf();
  const label = new TextEncoder().encode('wrpc test v1');
  const subkey = await kdf.derive(ring.get(ring.current), new Uint8Array(16).fill(1), label, 32);
  const key = node.aead().key(subkey);
  const nonce = new Uint8Array(12);
  const sealed = key.seal(nonce, new TextEncoder().encode('hello'), label);
  assert.strictEqual(new TextDecoder().decode(key.open(nonce, sealed, label)), 'hello');
  assert.throws(() => key.open(nonce, sealed, new Uint8Array(1)), node.OpenError);
  assert.strictEqual(node.equal(node.fromBase64(node.toBase64Url(subkey)), subkey), true);
});
