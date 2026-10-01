'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { createIdentity, createSealer, createOpener, OpenError } = require('../../encryption.js');
const browserAead = require('../../src/encryption/aead.browser.js');
const browserDh = require('../../src/encryption/dh.browser.js');
const { defineRouter, procedure } = require('../../index.js');
const { MemoryBackplane } = require('../../scaling.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const text = (bytes) => Buffer.from(bytes).toString();

test('e2ee: an identity is a seed and the public key others seal to — the same seed, the same identity', async () => {
  const alice = await createIdentity();
  assert.strictEqual(alice.seed.length, 32);
  assert.strictEqual(alice.publicKey.length, 32);
  assert.ok(Object.isFrozen(alice));
  const again = await createIdentity(alice.seed);
  assert.deepStrictEqual(Buffer.from(again.publicKey), Buffer.from(alice.publicKey));
  assert.notDeepStrictEqual(Buffer.from((await createIdentity()).publicKey), Buffer.from(alice.publicKey));
  await assert.rejects(createIdentity(new Uint8Array(8)), /seed must be 32 bytes/);
  await assert.rejects(createIdentity(null, { crypto: {} }), /getRandomValues is required/);
});

test('e2ee: sealed to a public key, opened by its identity — strings and bytes, with and without additional data', async () => {
  const bob = await createIdentity();
  const sealer = createSealer({ recipientPublicKey: bob.publicKey });
  const opener = createOpener({ keyPair: bob.keyPair });
  const sealed = await sealer.seal('привіт, Bob');
  assert.ok(sealed instanceof Uint8Array);
  assert.strictEqual(sealed.length, 32 + Buffer.byteLength('привіт, Bob') + 16);
  assert.strictEqual(text(await opener.open(sealed)), 'привіт, Bob');
  const bytes = Uint8Array.from({ length: 500 }, (_, i) => i % 256);
  assert.deepStrictEqual(
    Buffer.from(await opener.open(await sealer.seal(bytes, 'msg-7'), 'msg-7')),
    Buffer.from(bytes),
  );
  // Two seals of one message share nothing
  const [a, b] = [await sealer.seal('same'), await sealer.seal('same')];
  assert.notDeepStrictEqual(Buffer.from(a), Buffer.from(b));
  await assert.rejects(opener.open(await sealer.seal('x', 'msg-7'), 'msg-8'), OpenError, 'other additional data');
});

test('e2ee: with a sender identity the recipient learns who — and a message from anyone else does not open', async () => {
  const [alice, bob, mallory] = await Promise.all([createIdentity(), createIdentity(), createIdentity()]);
  const fromAlice = createSealer({ recipientPublicKey: bob.publicKey, senderKey: alice.keyPair });
  const fromMallory = createSealer({ recipientPublicKey: bob.publicKey, senderKey: mallory.keyPair });
  const anonymous = createSealer({ recipientPublicKey: bob.publicKey });
  const expectAlice = createOpener({ keyPair: bob.keyPair, senderPublicKey: alice.publicKey });
  assert.strictEqual(text(await expectAlice.open(await fromAlice.seal('it is me'))), 'it is me');
  await assert.rejects(async () => expectAlice.open(await fromMallory.seal('it is Alice, honest')), OpenError);
  await assert.rejects(async () => expectAlice.open(await anonymous.seal('it is Alice, honest')), OpenError);
  // And an opener that expects nobody in particular does not open an authenticated one by accident
  const expectAnyone = createOpener({ keyPair: bob.keyPair });
  await assert.rejects(async () => expectAnyone.open(await fromAlice.seal('it is me')), OpenError);
});

test('e2ee: `info` says what a message is for — another room, another identity, a flipped bit: nothing opens', async () => {
  const [bob, carol] = await Promise.all([createIdentity(), createIdentity()]);
  const sealed = await createSealer({ recipientPublicKey: bob.publicKey, info: 'room:lobby' }).seal('for the lobby');
  assert.strictEqual(
    text(await createOpener({ keyPair: bob.keyPair, info: 'room:lobby' }).open(sealed)),
    'for the lobby',
  );
  await assert.rejects(createOpener({ keyPair: bob.keyPair, info: 'room:vault' }).open(sealed), OpenError);
  await assert.rejects(createOpener({ keyPair: bob.keyPair }).open(sealed), OpenError);
  await assert.rejects(createOpener({ keyPair: carol.keyPair, info: 'room:lobby' }).open(sealed), OpenError);
  const flipped = Uint8Array.from(sealed);
  flipped[40] ^= 1;
  await assert.rejects(createOpener({ keyPair: bob.keyPair, info: 'room:lobby' }).open(flipped), OpenError);
});

test('e2ee: a page seals, a Node service opens — the two primitive halves agree', async () => {
  const service = await createIdentity();
  const page = { dh: browserDh.x25519(), cipher: browserAead.aead() };
  const pageIdentity = await createIdentity(null, page);
  const sealed = await createSealer({
    ...page,
    recipientPublicKey: service.publicKey,
    senderKey: pageIdentity.keyPair,
  }).seal('from a browser');
  const opener = createOpener({ keyPair: service.keyPair, senderPublicKey: pageIdentity.publicKey });
  assert.strictEqual(text(await opener.open(sealed)), 'from a browser');
});

test('e2ee: the options are validated where the helper is built', async () => {
  const bob = await createIdentity();
  assert.throws(() => createSealer(), /recipientPublicKey must be 32 bytes/);
  assert.throws(() => createSealer({ recipientPublicKey: 'key' }), /recipientPublicKey must be 32 bytes/);
  assert.throws(() => createOpener(), /keyPair must be an identity/);
  assert.throws(() => createOpener({ keyPair: {} }), /keyPair must be an identity/);
  assert.throws(
    () => createOpener({ keyPair: bob.keyPair, senderPublicKey: new Uint8Array(4) }),
    /senderPublicKey must be 32/,
  );
  const opener = createOpener({ keyPair: bob.keyPair });
  await assert.rejects(opener.open('text'), /a sealed message is bytes/);
  await assert.rejects(opener.open(new Uint8Array(32)), /a sealed message is bytes/);
});

// The point of it: a server that relays what it cannot read — across instances.
test('e2ee: through a relaying server, room-wide and cluster-wide — the server and the backplane carry bytes they cannot open', async (t) => {
  const seen = [];
  const router = defineRouter({
    chat: {
      join: procedure({
        access: 'public',
        handler: async (ctx) => {
          ctx.client.join('lobby');
          return true;
        },
      }),
      relay: procedure({
        access: 'public',
        handler: async (ctx, { sealed }) => {
          seen.push(sealed);
          ctx.server.to('lobby').except(ctx.client).emit('chat/message', { sealed });
          return true;
        },
      }),
      whoami: procedure({ access: 'public', handler: async (ctx) => ctx.client.id }),
      // 1:1 — by id, wherever that client is connected.
      direct: procedure({
        access: 'public',
        handler: async (ctx, { to, sealed }) => ctx.server.sendTo(to, 'chat/direct', { sealed }),
      }),
    },
  });
  const backplane = new MemoryBackplane({ logger: false });
  const published = [];
  const publish = backplane.publish.bind(backplane);
  backplane.publish = (channel, message) => {
    published.push(message);
    return publish(channel, message);
  };
  const one = await bootServer(t, { router, backplane });
  const two = await bootServer(t, { router, backplane });
  const [alice, bob] = await Promise.all([createIdentity(), createIdentity()]);
  const aliceClient = await connectClient(t, one.url);
  const bobClient = await connectClient(t, two.url);
  for (const client of [aliceClient, bobClient]) {
    await client.load('chat');
    await client.api.chat.join({});
  }
  const received = [];
  const opener = createOpener({ keyPair: bob.keyPair, senderPublicKey: alice.publicKey, info: 'room:lobby' });
  bobClient.api.chat.on('message', async ({ sealed }) => received.push(text(await opener.open(sealed))));
  const sealer = createSealer({ recipientPublicKey: bob.publicKey, senderKey: alice.keyPair, info: 'room:lobby' });
  const secret = 'the server is only the postman: 4111 1111 1111 1111';
  await aliceClient.api.chat.relay({ sealed: await sealer.seal(secret) });
  await waitFor(() => received.length === 1);
  assert.deepStrictEqual(received, [secret]);
  assert.ok(seen[0] instanceof Uint8Array, 'the handler was handed bytes');
  assert.ok(!Buffer.from(seen[0]).includes('4111'));
  assert.ok(
    published.some((message) => message.startsWith('wrpc-bin:')),
    'and they crossed the backplane as bytes',
  );
  assert.ok(published.every((message) => !Buffer.from(message.slice(9), 'base64').includes('4111')));

  // The same payload 1:1: `sendTo` an id that lives on the other instance.
  const direct = [];
  const pairwise = createOpener({ keyPair: bob.keyPair, senderPublicKey: alice.publicKey, info: 'dm' });
  bobClient.api.chat.on('direct', async ({ sealed }) => direct.push(text(await pairwise.open(sealed))));
  const toBob = createSealer({ recipientPublicKey: bob.publicKey, senderKey: alice.keyPair, info: 'dm' });
  const before = published.length;
  const handed = await aliceClient.api.chat.direct({
    to: await bobClient.api.chat.whoami({}),
    sealed: await toBob.seal(secret),
  });
  assert.strictEqual(handed, true, 'handed to the backplane');
  await waitFor(() => direct.length === 1);
  assert.deepStrictEqual(direct, [secret]);
  const carried = published.slice(before).filter((message) => message.startsWith('wrpc-bin:'));
  assert.strictEqual(carried.length, 1, 'one addressed binary envelope');
  assert.ok(!Buffer.from(carried[0].slice(9), 'base64').includes('4111'));
});
