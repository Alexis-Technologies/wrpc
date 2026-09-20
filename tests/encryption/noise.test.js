'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { createNoise, CipherState, PATTERN_NAMES } = require('../../src/encryption/noise.js');
const node = require('../../src/encryption/aead.js');
const browser = require('../../src/encryption/aead.browser.js');
const nodeDh = require('../../src/encryption/dh.js');
const browserDh = require('../../src/encryption/dh.browser.js');
const { createKdf } = require('../../src/encryption/hkdf.js');
const { OpenError } = require('../../src/encryption/contracts.js');
const { fromHex, toHex, utf8 } = require('../../src/encryption/bytes.js');
const { vectors, source } = require('./vectors/noise.json');

const kdf = createKdf();
const ALGORITHM = { AESGCM: 'aes-256-gcm', ChaChaPoly: 'chacha20-poly1305' };

// Both platform halves where they exist: the Node server's primitives and a
// browser client's must speak the same protocol, byte for byte.
const PLATFORMS = [
  { name: 'node', aead: node.aead, dh: nodeDh.x25519() },
  { name: 'browser', aead: browser.aead, dh: browserDh.x25519() },
];

const optionsOf = async (vector, side, dh) => ({
  prologue: fromHex(vector[`${side}_prologue`]),
  ephemeral: fromHex(vector[`${side}_ephemeral`]),
  staticKey: vector[`${side}_static`] ? await dh.keyPair(fromHex(vector[`${side}_static`])) : undefined,
  remoteStatic: vector[`${side}_remote_static`] ? fromHex(vector[`${side}_remote_static`]) : undefined,
  psk: vector[`${side}_psks`] ? fromHex(vector[`${side}_psks`][0]) : undefined,
});

test('noise: the vectors are the cacophony ones, for every protocol this package names', () => {
  assert.match(source, /cacophony/);
  const names = vectors.map((vector) => vector.protocol_name).sort();
  const expected = PATTERN_NAMES.flatMap((pattern) =>
    ['AESGCM', 'ChaChaPoly'].map((cipher) => `Noise_${pattern}_25519_${cipher}_SHA256`),
  ).sort();
  assert.deepStrictEqual(names, expected);
});

for (const vector of vectors) {
  const [, pattern, , cipherName] = vector.protocol_name.split('_');
  for (const platform of PLATFORMS) {
    const cipher = platform.aead({ algorithm: ALGORITHM[cipherName], optional: true });
    // ChaCha20-Poly1305 is not in WebCrypto: a browser names AESGCM.
    if (cipher === null) continue;
    test(`noise (${platform.name}): ${vector.protocol_name} — every message, the handshake hash, the transport`, async () => {
      const noise = createNoise({ pattern, dh: platform.dh, cipher, kdf });
      assert.strictEqual(noise.name, vector.protocol_name);
      assert.strictEqual(noise.cipher, cipherName);
      const initiator = await noise.initiator(await optionsOf(vector, 'init', platform.dh));
      const responder = await noise.responder(await optionsOf(vector, 'resp', platform.dh));
      let sessions = null;
      for (let i = 0; i < vector.messages.length; i++) {
        const { payload, ciphertext } = vector.messages[i];
        if (sessions === null) {
          const [writer, reader] = initiator.writing ? [initiator, responder] : [responder, initiator];
          const message = await writer.write(fromHex(payload));
          assert.strictEqual(toHex(message), ciphertext, `handshake message ${i}`);
          assert.strictEqual(toHex(await reader.read(message)), payload);
          if (initiator.done) {
            assert.ok(responder.done);
            sessions = [await initiator.finish(), await responder.finish()];
            assert.strictEqual(toHex(sessions[0].handshakeHash), vector.handshake_hash);
            assert.strictEqual(toHex(sessions[1].handshakeHash), vector.handshake_hash);
          }
        } else {
          // The vectors alternate directions after the handshake, the initiator first
          const [from, to] = i % 2 === 0 ? sessions : [sessions[1], sessions[0]];
          const sealed = await from.send.encrypt(fromHex(payload));
          assert.strictEqual(toHex(sealed), ciphertext, `transport message ${i}`);
          assert.strictEqual(toHex(await to.receive.decrypt(sealed)), payload);
        }
      }
      if (pattern === 'XX') {
        assert.strictEqual(
          toHex(sessions[1].remoteStatic),
          toHex((await optionsOf(vector, 'init', platform.dh)).staticKey.publicKey),
        );
        assert.strictEqual(
          toHex(sessions[0].remoteStatic),
          toHex((await optionsOf(vector, 'resp', platform.dh)).staticKey.publicKey),
        );
      }
    });
  }
}

const pair = async (pattern, { initiator = {}, responder = {}, cipher = node.aead(), dh = PLATFORMS[0].dh } = {}) => {
  const noise = createNoise({ pattern, dh, cipher, kdf });
  return [await noise.initiator(initiator), await noise.responder(responder), noise];
};

const run = async (a, b) => {
  while (!a.done) {
    const [writer, reader] = a.writing ? [a, b] : [b, a];
    await reader.read(await writer.write());
  }
  return [await a.finish(), await b.finish()];
};

test('noise: a Node responder and a browser initiator finish the same handshake', async () => {
  const server = createNoise({ pattern: 'NK', dh: nodeDh.x25519(), cipher: node.aead(), kdf });
  const client = createNoise({ pattern: 'NK', dh: browserDh.x25519(), cipher: browser.aead(), kdf });
  const staticKey = await nodeDh.x25519().generateKeyPair();
  const prologue = utf8('wrpc.v1\0ws\0');
  const initiator = await client.initiator({ prologue, remoteStatic: staticKey.publicKey });
  const responder = await server.responder({ prologue, staticKey });
  const [c, s] = await run(initiator, responder);
  assert.strictEqual(toHex(c.handshakeHash), toHex(s.handshakeHash));
  const sealed = await c.send.encrypt(utf8('from a page'));
  assert.ok(sealed instanceof Uint8Array);
  assert.strictEqual(Buffer.from(s.receive.decrypt(sealed)).toString(), 'from a page');
  assert.strictEqual(
    Buffer.from(await c.receive.decrypt(s.send.encrypt(utf8('from the server')))).toString(),
    'from the server',
  );
});

test('noise: a different prologue, a different pinned key or a different psk fails the handshake', async () => {
  const dh = PLATFORMS[0].dh;
  const staticKey = await dh.generateKeyPair();
  const other = await dh.generateKeyPair();
  const cases = [
    ['NK', { prologue: utf8('ws'), remoteStatic: staticKey.publicKey }, { prologue: utf8('wt'), staticKey }],
    ['NK', { remoteStatic: other.publicKey }, { staticKey }],
    ['NNpsk0', { psk: new Uint8Array(32).fill(1) }, { psk: new Uint8Array(32).fill(2) }],
  ];
  for (const [pattern, initiator, responder] of cases) {
    const [a, b] = await pair(pattern, { initiator, responder });
    const first = await a.write();
    // NK and psk0 encrypt from the first message on, so the first read fails
    await assert.rejects(b.read(first), OpenError, pattern);
  }
  // NN has no key until `ee`: a prologue mismatch surfaces on the reply
  const [a, b] = await pair('NN', { initiator: { prologue: utf8('a') }, responder: { prologue: utf8('b') } });
  await b.read(await a.write());
  await assert.rejects(async () => a.read(await b.write()), OpenError);
});

test('noise: a tampered, truncated or out-of-turn handshake message is refused', async () => {
  const dh = PLATFORMS[0].dh;
  const staticKey = await dh.generateKeyPair();
  const fresh = () => pair('XX', { initiator: { staticKey }, responder: { staticKey } });
  let [a, b] = await fresh();
  await assert.rejects(b.write(), /not this side's turn to write/);
  await assert.rejects(a.read(new Uint8Array(32)), /not this side's turn to read/);
  await assert.rejects(a.finish(), /not finished/);
  const first = await a.write();
  await assert.rejects(b.read(first.subarray(0, 31)), /malformed handshake message/);
  [a, b] = await fresh();
  await assert.rejects(b.read(new Uint8Array(32)), /malformed handshake message/, 'an all-zero ephemeral');
  [a, b] = await fresh();
  await assert.rejects(b.read('text'), /malformed handshake message/);
  await assert.rejects(b.read(new Uint8Array(65536)), /malformed handshake message/);
  [a, b] = await fresh();
  await b.read(await a.write());
  const second = await b.write();
  const flipped = Uint8Array.from(second);
  flipped[40] ^= 1;
  await assert.rejects(a.read(flipped), OpenError);
  [a, b] = await fresh();
  await b.read(await a.write());
  await assert.rejects(async () => a.read((await b.write()).subarray(0, 40)), /malformed handshake message/);
  // A payload past the handshake cap
  [a] = await fresh();
  await assert.rejects(a.write(new Uint8Array(65536)), RangeError);
});

test('noise: the options a pattern needs are checked where the handshake is built', async () => {
  const dh = PLATFORMS[0].dh;
  const base = { dh, cipher: node.aead(), kdf };
  assert.throws(() => createNoise({ ...base, pattern: 'IK' }), /unknown pattern "IK" — NN, NK, XX, NNpsk0/);
  assert.throws(() => createNoise({ ...base, pattern: 'toString' }), /unknown pattern/);
  assert.throws(() => createNoise({ ...base, pattern: 'NK' }).responder({}), /NK needs a staticKey/);
  assert.throws(() => createNoise({ ...base, pattern: 'NK' }).initiator({}), /remoteStatic must be 32 bytes/);
  assert.throws(() => createNoise({ ...base, pattern: 'XX' }).initiator(), /XX needs a staticKey/);
  assert.throws(
    () => createNoise({ ...base, pattern: 'NNpsk0' }).responder({ psk: new Uint8Array(8) }),
    /psk must be 32 bytes/,
  );
  assert.deepStrictEqual([...PATTERN_NAMES], ['NN', 'NK', 'XX', 'NNpsk0']);
  // An injected cipher names itself in the protocol name
  const injected = { ...node.aead(), id: 'XChaChaPoly' };
  assert.strictEqual(
    createNoise({ ...base, pattern: 'NN', cipher: injected }).name,
    'Noise_NN_25519_XChaChaPoly_SHA256',
  );
  assert.ok(Object.isFrozen(createNoise({ ...base, pattern: 'NN' })));
});

test('CipherState: a deterministic rekey — both ends turn at the same message, and the old key is gone', async () => {
  const [a, b] = await pair('NN', { initiator: { rekeyAfter: 3 }, responder: { rekeyAfter: 3 } });
  const [client, server] = await run(a, b);
  const sent = [];
  for (let i = 0; i < 8; i++) {
    const sealed = client.send.encrypt(utf8(`message ${i}`));
    sent.push(sealed);
    assert.strictEqual(Buffer.from(server.receive.decrypt(sealed)).toString(), `message ${i}`);
  }
  assert.strictEqual(client.send.counter, 8);
  // An end that never rekeys reads the first three and not the fourth
  const [e, f] = await pair('NN', { initiator: { rekeyAfter: 3 }, responder: { rekeyAfter: 0 } });
  const [rekeying, plain] = await run(e, f);
  for (let i = 0; i < 3; i++) plain.receive.decrypt(rekeying.send.encrypt(utf8('ok')));
  assert.throws(() => plain.receive.decrypt(rekeying.send.encrypt(utf8('after the turn'))), OpenError);
});

test('CipherState: over an asynchronous cipher the counter is still taken in call order, across a rekey', async () => {
  const dh = PLATFORMS[1].dh;
  const options = { initiator: { rekeyAfter: 2 }, responder: { rekeyAfter: 2 }, cipher: browser.aead(), dh };
  const [a, b] = await pair('NN', options);
  const [client, server] = await run(a, b);
  // Five seals started at once: promises, resolved in any order, numbered in call order
  const pending = Array.from({ length: 5 }, (_, i) => client.send.encrypt(utf8(`m${i}`)));
  assert.ok(pending.every((sealed) => sealed instanceof Promise));
  const sealed = await Promise.all(pending);
  const opened = await Promise.all(sealed.map((message) => server.receive.decrypt(message)));
  assert.deepStrictEqual(
    opened.map((bytes) => Buffer.from(bytes).toString()),
    ['m0', 'm1', 'm2', 'm3', 'm4'],
  );
  // And it agrees with the synchronous cipher about what a rekey is
  const sync = await pair('NN', {
    initiator: { rekeyAfter: 2, ephemeral: new Uint8Array(32).fill(7) },
    responder: { rekeyAfter: 2, ephemeral: new Uint8Array(32).fill(8) },
  });
  const async = await pair('NN', {
    initiator: { rekeyAfter: 2, ephemeral: new Uint8Array(32).fill(7) },
    responder: { rekeyAfter: 2, ephemeral: new Uint8Array(32).fill(8) },
    cipher: browser.aead(),
    dh,
  });
  const [syncClient] = await run(sync[0], sync[1]);
  const [asyncClient] = await run(async[0], async[1]);
  for (let i = 0; i < 5; i++) {
    assert.strictEqual(
      toHex(await asyncClient.send.encrypt(utf8('same'))),
      toHex(syncClient.send.encrypt(utf8('same'))),
      `message ${i}`,
    );
  }
});

test('CipherState: the counter is the number of messages, per direction', () => {
  const state = new CipherState(node.aead(), node.aead().key(new Uint8Array(32)));
  assert.strictEqual(state.counter, 0);
  state.encrypt(utf8('one'));
  state.encrypt(utf8('two'));
  assert.strictEqual(state.counter, 2);
});
