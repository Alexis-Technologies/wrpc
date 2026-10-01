'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const session = require('../../src/encryption/session.js');
const { createNoise } = require('../../src/encryption/noise.js');
const { deriveStatics, formatBundle, parseBundle } = require('../../src/encryption/statics.js');
const { wantsEncryption } = require('../../src/encryption/server.js');
const node = require('../../src/encryption/aead.js');
const browser = require('../../src/encryption/aead.browser.js');
const { x25519 } = require('../../src/encryption/dh.js');
const { createKdf } = require('../../src/encryption/hkdf.js');
const { generateKey } = require('../../src/encryption/keyring.js');
const { toHex } = require('../../src/encryption/bytes.js');

const { SecureChannel, helloHeader, prologueOf, parseHello, frame, isFrame, FRAME_HANDSHAKE, FRAME_SEALED } = session;
const kdf = createKdf();

const channels = async (cipher = node.aead()) => {
  const noise = createNoise({ pattern: 'NN', dh: x25519(), cipher, kdf });
  const [a, b] = [await noise.initiator(), await noise.responder()];
  await b.read(await a.write());
  await a.read(await b.write());
  return [new SecureChannel(await a.finish()), new SecureChannel(await b.finish())];
};

test('session: the hello names the protocol and the kid, and parses back to the bytes the prologue binds', () => {
  const name = 'Noise_NK_25519_AESGCM_SHA256';
  const header = helloHeader(name, 'k2026-09');
  const noise = Uint8Array.from({ length: 48 }, (_, i) => i);
  const hello = parseHello(frame(FRAME_HANDSHAKE, header, noise));
  assert.strictEqual(hello.name, name);
  assert.strictEqual(hello.kid, 'k2026-09');
  assert.deepStrictEqual(Buffer.from(hello.header), Buffer.from(header));
  assert.deepStrictEqual(Buffer.from(hello.message), Buffer.from(noise));
  // No kid at all (NN): an empty one
  assert.strictEqual(parseHello(frame(FRAME_HANDSHAKE, helloHeader(name, ''), noise)).kid, '');
  assert.deepStrictEqual(Buffer.from(prologueOf('wt', header)).subarray(0, 11).toString(), 'wrpc.v1\0wt\0');
  assert.throws(() => helloHeader('n'.repeat(256), ''), /too long/);
  assert.throws(() => helloHeader('n', 'k'.repeat(256)), /too long/);
});

test('session: a malformed hello is null — never an exception, never a half-read name', () => {
  const good = frame(FRAME_HANDSHAKE, helloHeader('Noise_NN_25519_AESGCM_SHA256', 'k1'), new Uint8Array(32));
  const cases = [
    new Uint8Array(0),
    Uint8Array.of(0),
    Uint8Array.of(0, 5),
    Uint8Array.of(0, 5, 1, 4),
    Uint8Array.of(0, 6, 1, 0, 0),
    Uint8Array.of(1, 5, 1, 0, 0),
    Uint8Array.of(0, 5, 2, 1, 65, 0),
    Uint8Array.of(0, 5, 1, 200, 65, 0),
    Uint8Array.of(0, 5, 1, 1, 65, 200),
    Uint8Array.of(0, 5, 1, 1, 0xff, 0),
    Uint8Array.of(0, 5, 1, 1, 65, 1, 0xfe),
  ];
  for (const bytes of cases) assert.strictEqual(parseHello(bytes), null, toHex(bytes));
  assert.notStrictEqual(parseHello(good), null);
  assert.strictEqual(isFrame(good, FRAME_HANDSHAKE), true);
  assert.strictEqual(isFrame(good, FRAME_SEALED), false);
  assert.strictEqual(isFrame(Uint8Array.of(0), FRAME_HANDSHAKE), false);
});

test('session: text comes back as text and bytes as bytes, in whatever view they were sent', async () => {
  const [client, server] = await channels();
  const text = JSON.stringify({ type: 'call', id: '7', method: 'chat/send', args: { text: 'привіт 👋' } });
  const sealed = client.seal(text);
  assert.ok(isFrame(sealed, FRAME_SEALED));
  assert.strictEqual(server.open(sealed), text);
  const bytes = Uint8Array.from({ length: 1000 }, (_, i) => i % 256);
  for (const view of [bytes, Buffer.from(bytes), bytes.buffer, new DataView(bytes.buffer)]) {
    const opened = client.open(server.seal(view));
    assert.ok(opened instanceof Uint8Array);
    assert.deepStrictEqual(Buffer.from(opened), Buffer.from(bytes));
  }
  assert.strictEqual(server.open(client.seal('')), '');
  assert.strictEqual(server.open(client.seal(new Uint8Array(0))).length, 0);
});

test('session: one inner frame sealed by two channels — each opens it, neither changes it, the ciphertexts differ', async () => {
  const [first, firstPeer] = await channels();
  const [second, secondPeer] = await channels(node.aead({ algorithm: 'chacha20-poly1305' }));
  const text = JSON.stringify({ type: 'event', name: 'chat/message', data: { text: 'привіт 👋' } });
  for (const message of [text, Uint8Array.from({ length: 300 }, (_, i) => i % 256)]) {
    // What a fan-out shares between its sealed recipients: the plaintext.
    const inner = session.innerOf(message);
    const before = Buffer.from(inner);
    const sealed = [first.sealInner(inner), second.sealInner(inner)];
    assert.ok(sealed.every((bytes) => isFrame(bytes, FRAME_SEALED)));
    assert.notDeepStrictEqual(Buffer.from(sealed[0]), Buffer.from(sealed[1]));
    assert.deepStrictEqual(Buffer.from(inner), before, 'the shared plaintext is read, never written');
    const opened = [firstPeer.open(sealed[0]), secondPeer.open(sealed[1])];
    for (const value of opened) {
      if (typeof message === 'string') assert.strictEqual(value, message);
      else assert.deepStrictEqual(Buffer.from(value), Buffer.from(message));
    }
  }
  // seal(data) is sealInner(innerOf(data)): one path, and the counters agree.
  assert.strictEqual(firstPeer.open(first.seal('after')), 'after');
  // Over crypto.subtle it answers a promise, like seal.
  const [page, server] = await channels(browser.aead());
  const pending = page.sealInner(session.innerOf('x'));
  assert.ok(pending instanceof Promise);
  assert.strictEqual(await server.open(await pending), 'x');
});

test('session: what is not a sealed frame, or not a message inside one, is refused', async () => {
  const [, server] = await channels();
  assert.throws(() => server.open(Uint8Array.of(0, 5, 1)), /not a sealed frame/);
  assert.throws(() => server.open(new Uint8Array(0)), /not a sealed frame/);
  // Sealed correctly, but the inner kind is one nobody defined — and then invalid UTF-8
  const noise = createNoise({ pattern: 'NN', dh: x25519(), cipher: node.aead(), kdf });
  const [a, b] = [await noise.initiator(), await noise.responder()];
  await b.read(await a.write());
  await a.read(await b.write());
  const [raw, peer] = [await a.finish(), new SecureChannel(await b.finish())];
  const sealedAs = (inner) => frame(FRAME_SEALED, raw.send.encrypt(Uint8Array.from(inner)));
  assert.throws(() => peer.open(sealedAs([2, 1, 2])), /malformed sealed message/);
  assert.throws(() => peer.open(sealedAs([])), /malformed sealed message/);
  assert.throws(() => peer.open(sealedAs([0, 0xff, 0xfe])), TypeError);
});

test('session: over crypto.subtle both methods answer promises, and the two halves read each other', async () => {
  const noiseOf = (cipher) => createNoise({ pattern: 'NN', dh: x25519(), cipher, kdf });
  const [a, b] = [await noiseOf(browser.aead()).initiator(), await noiseOf(node.aead()).responder()];
  await b.read(await a.write());
  await a.read(await b.write());
  const [page, server] = [new SecureChannel(await a.finish()), new SecureChannel(await b.finish())];
  const sealed = page.seal('from a page');
  assert.ok(sealed instanceof Promise);
  assert.strictEqual(server.open(await sealed), 'from a page');
  const opened = page.open(server.seal('from the server'));
  assert.ok(opened instanceof Promise);
  assert.strictEqual(await opened, 'from the server');
});

test('statics: one secret, two key pairs that are never the same key, one bundle to pin', async () => {
  const secret = generateKey();
  const primitives = { dh: x25519(), kdf };
  const statics = await deriveStatics(secret, primitives);
  const again = await deriveStatics(secret, primitives);
  assert.strictEqual(toHex(statics.noise.publicKey), toHex(again.noise.publicKey), 'deterministic');
  assert.notStrictEqual(toHex(statics.noise.publicKey), toHex(statics.hpke.publicKey));
  assert.notStrictEqual(
    toHex((await deriveStatics(generateKey(), primitives)).noise.publicKey),
    toHex(statics.noise.publicKey),
  );
  const bundle = formatBundle('k.1_a-b', statics);
  assert.match(bundle, /^k\.1_a-b:[A-Za-z0-9_-]{43}:[A-Za-z0-9_-]{43}$/);
  const parsed = parseBundle(bundle);
  assert.strictEqual(parsed.kid, 'k.1_a-b');
  assert.strictEqual(toHex(parsed.noise), toHex(statics.noise.publicKey));
  assert.strictEqual(toHex(parsed.hpke), toHex(statics.hpke.publicKey));
  assert.deepStrictEqual(parseBundle(parsed), parsed);
  for (const bad of [
    '',
    'k1',
    'k1:a:b',
    `k 1:${'A'.repeat(43)}:${'A'.repeat(43)}`,
    `k1:${'A'.repeat(43)}`,
    null,
    7,
    {},
  ]) {
    assert.throws(() => parseBundle(bad, 'pin'), /encryption: pin must be a key bundle/);
  }
});

test('wantsEncryption: the flag, exactly', () => {
  assert.strictEqual(wantsEncryption('/?wrpc_e=1'), true);
  assert.strictEqual(wantsEncryption('/api?wrpc_meta=%7B%7D&wrpc_e=1'), true);
  for (const url of ['/', '/?wrpc_e=0', '/?wrpc_e=true', '/?wrpc_e', '/wrpc_e=1', '', undefined, null, 7]) {
    assert.strictEqual(wantsEncryption(url), false, String(url));
  }
});

test('session: the inner frame is the same bytes with the Node string writer and without it (a browser)', () => {
  for (const text of ['', 'plain ascii', 'привіт 👋 — ü', '\u0000\uffff']) {
    const node = session.innerOf(text);
    const page = session.innerOf(text, null);
    assert.ok(Buffer.isBuffer(node) && !Buffer.isBuffer(page));
    assert.deepStrictEqual(Buffer.from(page), Buffer.from(node), text);
    assert.strictEqual(node[0], 0);
    assert.strictEqual(Buffer.from(node.subarray(1)).toString(), text);
  }
  assert.deepStrictEqual([...session.innerOf(Uint8Array.of(9, 8))], [1, 9, 8]);
});
