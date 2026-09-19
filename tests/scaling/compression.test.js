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
