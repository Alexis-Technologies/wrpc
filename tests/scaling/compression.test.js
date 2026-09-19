'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { RpcServer, defineRouter, procedure } = require('../../index.js');
const { MemoryBackplane } = require('../../scaling.js');
const { createEnvelopeCodec, ENVELOPE_PREFIX, isEncodedEnvelope } = require('../../src/compression/sync.js');
const { waitFor } = require('../helpers/server.js');

const router = defineRouter({ test: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } });

// The messages a backplane carries, with the publisher recorded.
const spied = (backplane) => {
  const published = [];
  const publish = backplane.publish.bind(backplane);
  backplane.publish = (channel, message) => {
    published.push({ channel, message });
    return publish(channel, message);
  };
  return published;
};

// A socket-shaped stub, enough for attachSocket to hand events to.
class FakeSocket {
  constructor() {
    this.sent = [];
    this.listeners = new Map();
    this.remoteAddress = '127.0.0.1';
  }

  on(event, listener) {
    const list = this.listeners.get(event) ?? [];
    list.push(listener);
    this.listeners.set(event, list);
  }

  once(event, listener) {
    this.on(event, listener);
  }

  emit(event, ...args) {
    for (const listener of this.listeners.get(event) ?? []) listener(...args);
  }

  send(data) {
    this.sent.push(JSON.parse(data));
    return true;
  }

  close() {
    this.emit('close');
  }

  terminate() {
    this.emit('close');
  }

  get events() {
    return this.sent.filter((packet) => packet.type === 'event');
  }
}

const logs = () => {
  const warnings = [];
  const log = {
    log() {},
    info() {},
    debug() {},
    error() {},
    warn: (entry) => warnings.push(entry),
    child() {
      return log;
    },
  };
  return { log, warnings };
};

const instance = (t, backplane, options = {}, logger = false) => {
  const rpc = new RpcServer({ router, backplane, logger, sse: false, ...options });
  t.after(() => rpc.close());
  const socket = new FakeSocket();
  const client = rpc.attachSocket(socket, {});
  client.join('lobby');
  return { rpc, socket, client };
};

const bigData = { rows: Array.from({ length: 200 }, (_, i) => ({ i, name: `row-${i}` })) };

test('envelope codec: a marker JSON never starts with, the threshold, another codec, the cap', () => {
  const codec = createEnvelopeCodec(true, 'x');
  assert.strictEqual(codec.id, 'deflate-raw');
  const small = JSON.stringify({ v: 1, name: 'a' });
  assert.strictEqual(codec.encode(small), small, 'under the threshold it stays text');
  const big = JSON.stringify(bigData);
  const encoded = codec.encode(big);
  assert.ok(encoded.startsWith(`${ENVELOPE_PREFIX}deflate-raw:`));
  assert.ok(encoded.length < big.length / 3, `${big.length} -> ${encoded.length} (base64 included)`);
  assert.strictEqual(isEncodedEnvelope(encoded), true);
  assert.strictEqual(isEncodedEnvelope(big), false);
  assert.strictEqual(codec.decode(encoded), big);
  assert.strictEqual(codec.decode(small), small, 'plain text passes through');
  assert.strictEqual(codec.decode(`${ENVELOPE_PREFIX}brotli:AAAA`), null, "another codec's envelope");
  assert.strictEqual(codec.decode(`${ENVELOPE_PREFIX}deflate-raw:notbase64!!`), null, 'a body that does not inflate');
  const capped = createEnvelopeCodec(true, 'x', 1024);
  assert.strictEqual(capped.decode(encoded), null, 'past the cap');
  // A codec that does not shrink the text is not worth the marker.
  const bloat = createEnvelopeCodec(
    { codec: { id: 'bloat', threshold: 0, encode: (b) => Buffer.concat([b, b]), decode: (b) => b } },
    'x',
  );
  assert.strictEqual(bloat.encode(big), big);
  assert.strictEqual(createEnvelopeCodec(false, 'x'), null);
  assert.throws(
    () => createEnvelopeCodec({ codec: { id: 'a', encode: async (b) => b, decode: (b) => b } }, 'x'),
    /synchronously/,
  );
});

test('rooms backplane: envelopes compressed on the wire when every instance turns it on', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  const a = instance(t, backplane, { rooms: { compression: true } });
  const b = instance(t, backplane, { rooms: { compression: true } });
  await timers.setTimeout(10);
  a.rpc.to('lobby').emit('big', bigData);
  await waitFor(() => b.socket.events.length === 1);
  assert.deepStrictEqual(b.socket.events[0].data, bigData);
  const wire = published.find((m) => m.channel.includes('lobby'));
  assert.ok(wire.message.startsWith('wrpc-enc:deflate-raw:'), 'the envelope rode compressed');
  assert.ok(wire.message.length < JSON.stringify(bigData).length / 3);
  // A small event stays JSON: under the threshold.
  published.length = 0;
  a.rpc.to('lobby').emit('small', { x: 1 });
  await waitFor(() => b.socket.events.length === 2);
  assert.ok(published.find((m) => m.channel.includes('lobby')).message.startsWith('{'));
});

test('envelope codec (list): encodes with the head, decodes any codec on the list — the marker names which', () => {
  const big = JSON.stringify(bigData);
  const deflate = createEnvelopeCodec(true, 'x');
  const both = createEnvelopeCodec({ codec: ['deflate-raw', 'brotli'] }, 'x');
  const swapped = createEnvelopeCodec({ codec: ['brotli', 'deflate-raw'] }, 'x');
  assert.deepStrictEqual(both.ids, ['deflate-raw', 'brotli']);
  assert.ok(both.encode(big).startsWith(`${ENVELOPE_PREFIX}deflate-raw:`), 'the head is what goes out');
  assert.ok(swapped.encode(big).startsWith(`${ENVELOPE_PREFIX}brotli:`));
  assert.strictEqual(both.decode(swapped.encode(big)), big, 'a codec further down the list still reads');
  assert.strictEqual(swapped.decode(both.encode(big)), big);
  assert.strictEqual(deflate.decode(swapped.encode(big)), null, 'a codec not on the list: unreadable, and known to be');
  // An id that is a prefix of another (a dictionary id holds a colon) claims only its own envelopes.
  const plain = {
    id: 'deflate-raw+dict',
    threshold: 0,
    encode: (b) => b.subarray(1),
    decode: () => Buffer.from('short'),
  };
  const long = {
    id: 'deflate-raw+dict:abcd',
    threshold: 0,
    encode: (b) => b.subarray(1),
    decode: () => Buffer.from('long'),
  };
  const nested = createEnvelopeCodec({ codec: [plain, long] }, 'x');
  assert.strictEqual(nested.decode(`${ENVELOPE_PREFIX}deflate-raw+dict:abcd:AAAA`), 'long');
  assert.strictEqual(nested.decode(`${ENVELOPE_PREFIX}deflate-raw+dict:AAAA`), 'short');
});

test('rooms backplane: a change of codec is a rollout without a lost message', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  const rooms = (codec) => ({ rooms: { compression: { codec } } });
  // Mid-rollout, every stage at once: one instance still on deflate alone is
  // NOT in this picture — step 1 (everyone lists both) comes before step 2.
  const old = instance(t, backplane, rooms(['deflate-raw', 'brotli']));
  const moved = instance(t, backplane, rooms(['brotli', 'deflate-raw']));
  const { log, warnings } = logs();
  const watcher = instance(t, backplane, rooms(['deflate-raw', 'brotli']), log);
  await timers.setTimeout(10);
  old.rpc.to('lobby').emit('from-old', bigData);
  moved.rpc.to('lobby').emit('from-moved', bigData);
  // Each instance delivers its own emit locally and the other's off the backplane.
  await waitFor(() => [watcher, old, moved].every((node) => node.socket.events.length === 2));
  const markers = published.filter((m) => m.channel.includes('lobby')).map((m) => m.message.split(':', 2)[1]);
  assert.deepStrictEqual(markers.sort(), ['brotli', 'deflate-raw'], 'both codecs were on the wire at once');
  assert.deepStrictEqual(
    watcher.socket.events.map((e) => e.data),
    [bigData, bigData],
  );
  assert.strictEqual(warnings.filter((w) => w.event === 'backplane.encoded').length, 0, 'nothing was dropped');
});

test('rooms backplane: an instance without the option drops an encoded envelope loudly, not silently', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const a = instance(t, backplane, { rooms: { compression: true } });
  const { log, warnings } = logs();
  const plain = instance(t, backplane, {}, log);
  // Let both room subscriptions land before the first publish.
  await timers.setTimeout(10);
  a.rpc.to('lobby').emit('big', bigData);
  await waitFor(() => warnings.some((w) => w.event === 'backplane.encoded'));
  assert.strictEqual(plain.socket.events.length, 0);
  // And the other way: a plain instance's envelope reaches a compressing one.
  plain.rpc.to('lobby').emit('hello', { x: 1 });
  await waitFor(() => a.socket.events.length === 1);
});

test('cluster: envelopes signed, then compressed — verification still holds', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  const options = { cluster: { secret: 's3cret', compression: { threshold: 0 }, presenceInterval: 50 } };
  const a = instance(t, backplane, options);
  const b = instance(t, backplane, options);
  await waitFor(() => a.rpc.cluster.count('lobby') === 2 && b.rpc.cluster.count('lobby') === 2);
  const cluster = published.filter((m) => m.channel.includes('cluster'));
  assert.ok(cluster.length > 0);
  assert.ok(
    cluster.every((m) => m.message.startsWith('wrpc-enc:deflate-raw:')),
    'every cluster envelope left compressed',
  );
  // A node without the option cannot read them and says so.
  const { log, warnings } = logs();
  instance(t, backplane, { cluster: { secret: 's3cret', presenceInterval: 50 } }, log);
  await waitFor(() => warnings.some((w) => w.event === 'cluster.encoded'));
});

test('the options are validated at construction', () => {
  const backplane = new MemoryBackplane({ logger: false });
  assert.throws(() => new RpcServer({ router, backplane, logger: false, rooms: { compression: 'lz' } }), /compression/);
  assert.throws(() => new RpcServer({ router, backplane, logger: false, rooms: { maxMessage: -1 } }), /maxMessage/);
  assert.throws(() => new RpcServer({ router, backplane, logger: false, cluster: { compression: [] } }), /compression/);
  backplane.close();
});
