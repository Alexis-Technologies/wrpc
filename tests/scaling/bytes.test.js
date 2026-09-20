'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure } = require('../../index.js');
const { MemoryBackplane } = require('../../scaling.js');
const { generateKey } = require('../../encryption.js');
const { createEnvelope, BINARY_PREFIX } = require('../../src/rpc/envelope.js');
const { RoomsBackplane } = require('../../src/rpc/rooms.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const router = defineRouter({
  chat: {
    join: procedure({
      access: 'public',
      handler: async (ctx) => {
        ctx.client.join('lobby');
        return true;
      },
    }),
  },
});

const spied = (backplane) => {
  const published = [];
  const publish = backplane.publish.bind(backplane);
  backplane.publish = (channel, message) => {
    published.push({ channel, message });
    return publish(channel, message);
  };
  return published;
};

const blob = Uint8Array.from({ length: 2000 }, (_, i) => (i * 7) % 256);
const event = { from: 'ada', blob, nested: { parts: [Uint8Array.of(1, 2, 3), 'text'] } };

const pair = async (t, rooms = {}) => {
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  const a = await bootServer(t, { router, backplane, rooms });
  const b = await bootServer(t, { router, backplane, rooms });
  const client = await connectClient(t, b.url);
  await client.load('chat');
  await client.api.chat.join({});
  await timers.setTimeout(10);
  const heard = new Promise((resolve) => client.api.chat.on('file', resolve));
  return { a, b, client, published, heard };
};

const assertBytes = (received) => {
  assert.strictEqual(received.from, 'ada');
  assert.ok(received.blob instanceof Uint8Array, 'bytes, not the {"0":…} object JSON would make of them');
  assert.deepStrictEqual(Buffer.from(received.blob), Buffer.from(blob));
  assert.deepStrictEqual([...received.nested.parts[0]], [1, 2, 3]);
  assert.strictEqual(received.nested.parts[1], 'text');
};

test('backplane bytes: an event whose data holds bytes reaches the members on another instance as bytes', async (t) => {
  const { a, heard, published } = await pair(t);
  a.server.rpc.to('lobby').emit('chat/file', event);
  assertBytes(await heard);
  const wire = published.find((m) => m.channel.includes('lobby'));
  assert.ok(wire.message.startsWith(BINARY_PREFIX));
});

test('backplane bytes: under compression the binary envelope rides beside the compressed ones', async (t) => {
  const { a, client, heard, published } = await pair(t, { compression: { threshold: 0 } });
  const texts = [];
  client.api.chat.on('note', (data) => texts.push(data));
  a.server.rpc.to('lobby').emit('chat/note', { text: 'plain json' });
  a.server.rpc.to('lobby').emit('chat/file', event);
  assertBytes(await heard);
  await waitFor(() => texts.length === 1);
  assert.deepStrictEqual(
    published.filter((m) => m.channel.includes('lobby')).map((m) => m.message.split(':')[0]),
    ['wrpc-enc', 'wrpc-bin'],
  );
});

test('backplane bytes: sealed — what a relay would carry for an end-to-end payload is ciphertext on Redis too', async (t) => {
  for (const compression of [false, { threshold: 0 }]) {
    const { a, heard, published } = await pair(t, { encryption: { keys: generateKey() }, compression });
    a.server.rpc.to('lobby').emit('chat/file', event);
    assertBytes(await heard);
    const wire = published.find((m) => m.channel.includes('lobby'));
    assert.ok(wire.message.startsWith('wrpc-sealed:0:'));
    assert.ok(!wire.message.includes('wrpc-bin'));
  }
});

test('backplane bytes: a malformed binary envelope is dropped; a registry wired without the envelope names the loss', async () => {
  const warnings = [];
  const log = { warn: (entry) => warnings.push(entry), error() {}, info() {}, debug() {}, child: () => log };
  const envelope = createEnvelope({ maxMessage: 1 << 20, name: 'x', layer: 'rooms', event: 'backplane', log });
  assert.strictEqual(envelope.decode(`${BINARY_PREFIX}AAAA`, 'ch'), null);
  assert.strictEqual(envelope.decode(`${BINARY_PREFIX}!!!`, 'ch'), null);
  const round = envelope.decode(envelope.encodeBytes({ v: 1, name: 'x', data: { blob } }, 'ch'), 'ch');
  assert.deepStrictEqual(Buffer.from(round.data.blob), Buffer.from(blob));
  // By hand, without what the core injects
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  const binder = new RoomsBackplane({ backplane, instance: 'i1', log, deliver() {} });
  binder.publish({ rooms: ['lobby'], name: 'chat/file', data: event, binary: true });
  assert.deepStrictEqual(warnings, [{ event: 'backplane.bytes', name: 'chat/file' }]);
  assert.strictEqual(published.length, 0);
  await backplane.close();
});
