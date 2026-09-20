'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { createHpke, dhKem, isKem, AEAD_IDS } = require('../../src/encryption/hpke.js');
const node = require('../../src/encryption/aead.js');
const browser = require('../../src/encryption/aead.browser.js');
const nodeDh = require('../../src/encryption/dh.js');
const browserDh = require('../../src/encryption/dh.browser.js');
const { createKdf } = require('../../src/encryption/hkdf.js');
const browserKdf = require('../../src/encryption/hkdf.browser.js');
const { OpenError } = require('../../src/encryption/contracts.js');
const { fromHex, toHex, utf8 } = require('../../src/encryption/bytes.js');
const { vectors, source } = require('./vectors/hpke.json');

const kdf = createKdf();
const ALGORITHM = { 2: 'aes-256-gcm', 3: 'chacha20-poly1305' };
const PLATFORMS = [
  { name: 'node', aead: node.aead, dh: nodeDh.x25519(), kdf },
  { name: 'browser', aead: browser.aead, dh: browserDh.x25519(), kdf: browserKdf.createKdf() },
];

test('hpke: the vectors are RFC 9180, for every suite this package names', () => {
  assert.match(source, /RFC 9180/);
  assert.deepStrictEqual(vectors.map((v) => [v.mode, v.kem_id, v.kdf_id, v.aead_id].join('/')).sort(), [
    '0/32/1/2',
    '0/32/1/3',
    '1/32/1/2',
    '1/32/1/3',
  ]);
  assert.deepStrictEqual({ ...AEAD_IDS }, { 'aes-256-gcm': 2, 'chacha20-poly1305': 3 });
});

for (const vector of vectors) {
  for (const platform of PLATFORMS) {
    const cipher = platform.aead({ algorithm: ALGORITHM[vector.aead_id], optional: true });
    if (cipher === null) continue;
    const label = `mode ${vector.mode}, aead ${vector.aead_id}`;
    test(`hpke (${platform.name}): RFC 9180 ${label} — encap, key schedule, every sealed message, every export`, async () => {
      const kem = dhKem(platform.dh, platform.kdf);
      const hpke = createHpke({ kem, kdf: platform.kdf, cipher });
      // DeriveKeyPair: the vectors' key pairs come from their ikm
      const ephemeral = await kem.deriveKeyPair(fromHex(vector.ikmE));
      const recipient = await kem.deriveKeyPair(fromHex(vector.ikmR));
      assert.strictEqual(toHex(ephemeral.publicKey), vector.pkEm);
      assert.strictEqual(toHex(recipient.publicKey), vector.pkRm);
      const options = { info: fromHex(vector.info) };
      if (vector.mode === 1) Object.assign(options, { psk: fromHex(vector.psk), pskId: fromHex(vector.psk_id) });
      const sender = await hpke.setupSender(recipient.publicKey, { ...options, ephemeral });
      assert.strictEqual(toHex(sender.enc), vector.enc);
      const receiver = await hpke.setupRecipient(sender.enc, recipient, options);
      let sequence = 0;
      for (const encryption of vector.encryptions) {
        // The contexts count every message; the fixture keeps the first few and the last
        const target =
          Number.parseInt(encryption.nonce.slice(-4), 16) ^ Number.parseInt(vector.base_nonce.slice(-4), 16);
        while (sequence < target) {
          await receiver.open(null, await sender.context.seal(null, utf8('skipped')));
          sequence++;
        }
        const sealed = await sender.context.seal(fromHex(encryption.aad), fromHex(encryption.pt));
        assert.strictEqual(toHex(sealed), encryption.ct, `message ${target}`);
        assert.strictEqual(toHex(await receiver.open(fromHex(encryption.aad), sealed)), encryption.pt);
        sequence++;
      }
      for (const exported of vector.exports) {
        const context = fromHex(exported.exporter_context);
        assert.strictEqual(toHex(await sender.context.export(context, exported.L)), exported.exported_value);
        assert.strictEqual(toHex(await receiver.export(context, exported.L)), exported.exported_value);
      }
    });
  }
}

const suite = (cipher = node.aead(), dh = nodeDh.x25519(), hash = kdf) => {
  const kem = dhKem(dh, hash);
  return { kem, hpke: createHpke({ kem, kdf: hash, cipher }) };
};

test('hpke: a Node recipient opens what a browser sender sealed, and both export the same secret', async () => {
  const server = suite();
  const page = suite(browser.aead(), browserDh.x25519(), browserKdf.createKdf());
  const recipient = await server.kem.generateKeyPair();
  const info = utf8('wrpc http v1');
  const { enc, context } = await page.hpke.setupSender(recipient.publicKey, { info });
  const received = await server.hpke.setupRecipient(enc, recipient, { info });
  assert.strictEqual(
    Buffer.from(received.open(utf8('aad'), await context.seal(utf8('aad'), utf8('a request')))).toString(),
    'a request',
  );
  assert.strictEqual(
    toHex(await context.export(utf8('response'), 32)),
    toHex(await received.export(utf8('response'), 32)),
  );
  assert.notStrictEqual(
    toHex(await context.export(utf8('response'), 32)),
    toHex(await context.export(utf8('other'), 32)),
  );
});

test('hpke: another info, another recipient, another psk or a replayed message does not open', async () => {
  const { kem, hpke } = suite();
  const recipient = await kem.generateKeyPair();
  const stranger = await kem.generateKeyPair();
  const psk = new Uint8Array(32).fill(4);
  const pskId = utf8('channel-1');
  const sent = await hpke.setupSender(recipient.publicKey, { info: utf8('for http'), psk, pskId });
  const sealed = sent.context.seal(null, utf8('once'));
  const attempts = [
    { info: utf8('for sse'), psk, pskId },
    { info: utf8('for http') },
    { info: utf8('for http'), psk: new Uint8Array(32).fill(5), pskId },
    { info: utf8('for http'), psk, pskId: utf8('channel-2') },
  ];
  for (const options of attempts) {
    const context = await hpke.setupRecipient(sent.enc, recipient, options);
    assert.throws(() => context.open(null, sealed), OpenError);
  }
  const wrong = await hpke.setupRecipient(sent.enc, stranger, { info: utf8('for http'), psk, pskId });
  assert.throws(() => wrong.open(null, sealed), OpenError);
  const right = await hpke.setupRecipient(sent.enc, recipient, { info: utf8('for http'), psk, pskId });
  assert.strictEqual(Buffer.from(right.open(null, sealed)).toString(), 'once');
  assert.throws(() => right.open(null, sealed), OpenError, 'the sequence moved on');
});

test('hpke: the options and the seams are checked where they are used', async () => {
  const { kem, hpke } = suite();
  const recipient = await kem.generateKeyPair();
  await assert.rejects(hpke.setupSender(recipient.publicKey, { psk: new Uint8Array(32) }), /psk and pskId go together/);
  await assert.rejects(hpke.setupSender(recipient.publicKey, { pskId: utf8('id') }), /psk and pskId go together/);
  await assert.rejects(
    hpke.setupSender(recipient.publicKey, { psk: new Uint8Array(8), pskId: utf8('id') }),
    /32 bytes or more/,
  );
  assert.throws(() => createHpke({ kem: {}, kdf, cipher: node.aead() }), /kem must be a Kem/);
  assert.throws(() => createHpke({ kem, kdf, cipher: { ...node.aead(), id: 'xchacha20' } }), /no registered AEAD id/);
  assert.strictEqual(isKem(kem), true);
  assert.ok(Object.isFrozen(kem) && Object.isFrozen(hpke));
  assert.deepStrictEqual(
    [kem.id, kem.publicLength, kem.encLength, kem.secretLength, hpke.aeadId, hpke.encLength],
    [0x20, 32, 32, 32, 2, 32],
  );
  for (const broken of [
    { ...kem, id: 'x' },
    { ...kem, encLength: 0 },
    { ...kem, encap: null },
    { ...kem, decap: 1 },
    null,
  ]) {
    assert.strictEqual(isKem(broken), false);
  }
  // A low-order encapsulated key is the Dh's to refuse
  await assert.rejects(hpke.setupRecipient(new Uint8Array(32), recipient), /invalid public key/);
});
