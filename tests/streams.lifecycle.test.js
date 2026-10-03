'use strict';

// A WrpcWritable watches its transport's 'close' so a disconnect mid-stream
// is noticed — and lets go of it once the stream is finished. Before, the
// listener stayed for the transport's whole life: a client that streamed N
// times held N 'close' listeners (and printed MaxListenersExceededWarning
// from the 11th), and the server-side Client also kept every writable in
// `streams`, where the 256th download locked the peer's uploads out with 429.

const { test } = require('node:test');
const assert = require('node:assert');

const { Emitter } = require('../src/utils.js');
const { WrpcWritable } = require('../src/streams.js');
const { Client } = require('../src/rpc/client.js');
const { defineRouter, procedure } = require('../index.js');
const { bootServer, connectClient } = require('./helpers/server.js');

const ROUNDS = 12; // past the Emitter's default cap of 10

// The default cap on purpose — the one a WrpcClient has — so a leak also
// trips the warning the regression was found by.
class FakeTransport extends Emitter {
  connection = {};
  accept = true;
  sent = [];

  send(packet) {
    this.sent.push(packet);
  }

  write() {
    return this.accept;
  }
}

const leakWarnings = (t) => {
  const warn = t.mock.method(globalThis.console, 'warn', () => {});
  return () => warn.mock.calls.filter(({ arguments: [message] }) => /MaxListenersExceeded/.test(String(message)));
};

test('WrpcWritable: a finished stream takes its close listener off the transport', async (t) => {
  await t.test('ended and terminated streams leave no listener behind', (sub) => {
    const warnings = leakWarnings(sub);
    const transport = new FakeTransport();
    for (let i = 0; i < ROUNDS; i++) {
      const stream = new WrpcWritable(`s${i}`, 'blob', 1, transport);
      assert.strictEqual(transport.listenerCount('close'), 1, 'armed while open');
      stream.write(new Uint8Array([i]));
      if (i % 2 === 0) stream.end();
      else stream.terminate();
      assert.strictEqual(transport.listenerCount('close'), 0, `stream ${i} let go`);
    }
    assert.deepStrictEqual(warnings(), []);
  });

  await t.test('an unfinished stream still notices the disconnect', () => {
    const transport = new FakeTransport();
    for (let i = 0; i < ROUNDS; i++) new WrpcWritable(`done${i}`, 'blob', 1, transport).end();
    const open = new WrpcWritable('open', 'blob', 1, transport);
    let closes = 0;
    open.on('close', () => closes++);
    void transport.emit('close');
    assert.strictEqual(open.closed, true);
    assert.strictEqual(closes, 1);
    assert.strictEqual(open.write(new Uint8Array([1])), false);
  });

  await t.test("ended while a 'drain' is owed: kept until the drain releases it", () => {
    const transport = new FakeTransport();
    const stream = new WrpcWritable('s', 'blob', 2, transport);
    transport.accept = false;
    assert.strictEqual(stream.write(new Uint8Array([1])), false);
    stream.end();
    assert.strictEqual(transport.listenerCount('close'), 1, 'the close may still have a drain to release');
    let drained = 0;
    stream.on('drain', () => drained++);
    void transport.emit('drain');
    assert.strictEqual(drained, 1);
    assert.strictEqual(transport.listenerCount('close'), 0);
    assert.strictEqual(transport.listenerCount('drain'), 0);
  });

  await t.test("ended while a 'drain' is owed: a disconnect still releases it", () => {
    const transport = new FakeTransport();
    const stream = new WrpcWritable('s', 'blob', 2, transport);
    transport.accept = false;
    stream.write(new Uint8Array([1]));
    stream.terminate();
    const events = [];
    stream.on('drain', () => events.push('drain'));
    stream.on('close', () => events.push('close'));
    void transport.emit('close');
    assert.deepStrictEqual(events, ['drain', 'close']);
    assert.strictEqual(stream.closed, true);
    assert.strictEqual(transport.listenerCount('close'), 0);
  });

  await t.test('a transport with once() but no off() still ends cleanly', () => {
    const sent = [];
    const transport = { send: (packet) => sent.push(packet), write: () => true, once: () => {} };
    const stream = new WrpcWritable('s', 'blob', 1, transport);
    assert.doesNotThrow(() => stream.end());
    assert.deepStrictEqual(sent.at(-1), { type: 'stream', id: 's', status: 'end' });
  });
});

test('server Client.createStream: finished downloads leave neither a listener nor a streams entry', (t) => {
  const warnings = leakWarnings(t);
  const transport = new FakeTransport();
  const client = new Client(transport, { log: null });
  const baseline = transport.listenerCount('close');
  for (let i = 0; i < ROUNDS; i++) {
    const stream = client.createStream('down', 1);
    stream.write(new Uint8Array([i]));
    stream.end();
  }
  assert.strictEqual(transport.listenerCount('close'), baseline);
  assert.strictEqual(client.streams.size, 0);
  assert.deepStrictEqual(warnings(), []);
});

test('one connection, many streams both ways: listeners stay flat and uploads are never locked out', async (t) => {
  const warnings = leakWarnings(t);
  const router = defineRouter({
    files: {
      upload: procedure({
        access: 'public',
        handler: async (context, { id }) => {
          let size = 0;
          for await (const chunk of context.client.getStream(id)) size += chunk.length;
          return size;
        },
      }),
      download: procedure({
        access: 'public',
        handler: async (context, { size }) => {
          const stream = context.client.createStream('down', size);
          queueMicrotask(() => {
            stream.write(new Uint8Array(size).fill(7));
            stream.end();
          });
          return stream.id;
        },
      }),
      held: procedure({ access: 'public', handler: async (context) => context.client.streams.size }),
    },
  });
  // A cap far below ROUNDS: a download used to count against it for good.
  const { url } = await bootServer(t, { router, maxStreams: 2 });
  const client = await connectClient(t, url);
  await client.load('files');
  const baseline = client.listenerCount('close');
  const size = 1024;
  for (let round = 0; round < ROUNDS; round++) {
    const upload = client.createStream('up', size);
    const received = client.api.files.upload({ id: upload.id });
    upload.write(new Uint8Array(size).fill(1));
    upload.end();
    assert.strictEqual(await received, size, `upload ${round}`);
    const id = await client.api.files.download({ size });
    let downloaded = 0;
    for await (const chunk of client.getStream(id)) downloaded += chunk.length;
    assert.strictEqual(downloaded, size, `download ${round}`);
  }
  assert.strictEqual(client.listenerCount('close'), baseline);
  assert.strictEqual(await client.api.files.held({}), 0);
  assert.deepStrictEqual(warnings(), []);
});
