'use strict';

// RpcServer.attach / attachChannel end to end: a WrpcClient over a raw
// data channel (transport: 'webrtc', channel) against an ordinary
// RpcServer — the attachPort of WebRTC. Fake channels from
// tests/webrtc/rawChannel.js, negotiated by hand the way an application on
// this level does its own signaling; a factory-driven reconnect resumes a
// subscription across channels.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure } = require('../../src/rpc/router.js');
const { tracked, createEventLog } = require('../../src/rpc/subscriptions.js');
const { createEventStream, Emitter } = require('../../src/utils.js');
const { WrpcClient } = require('../../src/client/core.js');
const { ClientRtcTransport } = require('../../src/webrtc/transport.js');
const { chunkEncode } = require('../../src/chunks.js');
const { rawChannelPair } = require('../webrtc/rawChannel.js');
const { waitFor, within } = require('../webrtc/portContract.js');

const quiet = {
  log() {},
  info() {},
  warn() {},
  error() {},
  debug() {},
  child() {
    return this;
  },
};

const onceEvent = (emitter, name) => new Promise((resolve) => emitter.once(name, resolve));

const routerOf = ({ seen = [], events = [] } = {}) => {
  const feed = { log: createEventLog({ size: 16 }), streams: new Set() };
  const push = (data) => {
    const value = tracked(feed.log.push(data), data);
    for (const stream of feed.streams) stream.push(value);
  };
  const router = defineRouter(
    {
      calc: {
        add: procedure({ access: 'public', handler: async (_ctx, { a, b }) => a + b }),
        // The default access is 'session': a channel-attached client has none.
        secret: procedure({ handler: async () => 'never' }),
        meta: procedure({
          access: 'public',
          handler: async (ctx) => ({
            kind: ctx.client.transportKind,
            headers: { ...ctx.meta.headers },
            data: { ...ctx.meta.data },
            remoteAddress: ctx.meta.remoteAddress,
            session: ctx.session,
          }),
        }),
        note: procedure({
          access: 'public',
          handler: async (ctx, data) => void ctx.client.sendEvent('calc/note', { echo: data }),
        }),
        feed: procedure.subscription({
          access: 'public',
          handler: async function* (_ctx, _args, { lastEventId, signal }) {
            for (const value of feed.log.since(lastEventId) ?? []) yield value;
            const stream = createEventStream({ signal });
            feed.streams.add(stream);
            try {
              for await (const value of stream) yield value;
            } finally {
              feed.streams.delete(stream);
            }
          },
        }),
        readUpload: procedure({
          access: 'public',
          handler: async (ctx, { id }) => {
            const stream = ctx.client.getStream(id);
            let total = 0;
            let sum = 0;
            for await (const chunk of stream) {
              total += chunk.length;
              for (let i = 0; i < chunk.length; i++) sum = (sum + chunk[i]) % 251;
            }
            return { total, sum };
          },
        }),
        download: procedure({
          access: 'public',
          handler: async (ctx, { size }) => {
            const stream = ctx.client.createStream('blob', size);
            queueMicrotask(() => {
              stream.write(new Uint8Array(size).fill(7));
              stream.end();
            });
            return { id: stream.id };
          },
        }),
        on: {
          ping: procedure({
            access: 'public',
            handler: async (ctx, data) => void seen.push([ctx.client.source, data]),
          }),
        },
      },
    },
    {
      hooks: {
        onConnect: async (client) => {
          await timers.setTimeout(5);
          events.push(['connect', client.source]);
          client.join('hooked');
        },
        onDisconnect: async (client, { rooms }) => void events.push(['disconnect', client.source, [...rooms]]),
      },
    },
  );
  return { router, push, feed };
};

// A server with one raw channel attached, and a WrpcClient on the other
// end of it. `attach` are the attachChannel options.
const boot = async (t, { attach = {}, client: clientOptions = {}, logger = quiet, seen, events } = {}) => {
  const served = routerOf({ seen, events });
  const rpc = new RpcServer({ router: served.router, logger });
  t.after(() => rpc.close());
  const pair = await rawChannelPair(t);
  const attached = rpc.attachChannel(pair.b, attach);
  const client = await WrpcClient.connect('webrtc:server', {
    transport: 'webrtc',
    channel: pair.a,
    heartbeat: false,
    reconnect: false,
    ...clientOptions,
  });
  t.after(() => client.close());
  await client.load('calc');
  return { rpc, served, pair, attached, client };
};

test('attachChannel: a persistent webrtc client that answers calls and carries events both ways', async (t) => {
  const seen = [];
  const { rpc, pair, attached, client } = await boot(t, { attach: { peer: 'browser' }, seen });
  assert.strictEqual(attached.persistent, true);
  assert.strictEqual(attached.binary, true);
  assert.strictEqual(attached.source, 'browser');
  assert.strictEqual(attached.transportKind, 'webrtc');
  assert.strictEqual(rpc.getClient(attached.id), attached);
  assert.strictEqual(await client.api.calc.add({ a: 2, b: 3 }), 5);

  const notes = [];
  client.api.calc.on('note', (data) => notes.push(data));
  await client.api.calc.note('hello');
  client.sendEvent('calc/ping', { from: 'browser' });
  await waitFor(() => notes.length === 1 && seen.length === 1, 'events both ways');
  assert.deepStrictEqual(notes, [{ echo: 'hello' }]);
  assert.deepStrictEqual(seen, [['browser', { from: 'browser' }]]);
  assert.strictEqual(pair.b.binaryType, 'arraybuffer');
});

test('attachChannel: no session at attach — the default access refuses, public answers; observed meta lands', async (t) => {
  const { client } = await boot(t, {
    attach: { headers: { 'x-tenant': 't1' }, data: { user: 'u1' }, remoteAddress: '203.0.113.9' },
  });
  await assert.rejects(client.api.calc.secret(), (error) => error.code === 403);
  const meta = await client.api.calc.meta();
  assert.strictEqual(meta.kind, 'webrtc');
  assert.deepStrictEqual(meta.headers, { 'x-tenant': 't1' });
  assert.deepStrictEqual(meta.data, { user: 'u1' });
  assert.strictEqual(meta.remoteAddress, '203.0.113.9');
  assert.strictEqual(meta.session, null);
});

test('attachChannel: without observations the meta is empty and the source is the channel label', async (t) => {
  const { attached, client } = await boot(t);
  assert.strictEqual(attached.source, 'wrpc');
  const meta = await client.api.calc.meta();
  assert.deepStrictEqual(meta.headers, {});
  assert.deepStrictEqual(meta.data, {});
  assert.strictEqual(meta.remoteAddress, '');
});

test('attachChannel: connection hooks run, and closing the channel detaches the client', async (t) => {
  const events = [];
  const { rpc, pair, attached, client } = await boot(t, { attach: { peer: 'p1' }, events });
  assert.strictEqual(await client.api.calc.add({ a: 1, b: 1 }), 2, 'dispatch waited for the hook');
  assert.deepStrictEqual(events, [['connect', 'p1']]);
  assert.ok(attached.rooms.has('hooked'));
  assert.ok(rpc.clients.has(attached));
  const closed = onceEvent(client, 'close');
  pair.a.close();
  await within(closed, 'the client saw the close');
  await waitFor(() => events.length === 2, 'onDisconnect');
  assert.deepStrictEqual(events[1], ['disconnect', 'p1', ['hooked']]);
  assert.ok(!rpc.clients.has(attached));
  assert.strictEqual(rpc.getClient(attached.id), undefined);
});

test('attachChannel: uploads and downloads cross the fragmenting channel', async (t) => {
  const { client } = await boot(t, { attach: { maxMessageSize: 16 * 1024 }, client: { maxMessageSize: 16 * 1024 } });
  const size = 1024 * 1024;
  const payload = new Uint8Array(size);
  let sum = 0;
  for (let i = 0; i < size; i++) {
    payload[i] = i % 256;
    sum = (sum + payload[i]) % 251;
  }
  const stream = client.createStream('blob', size);
  const receiving = client.api.calc.readUpload({ id: stream.id });
  const drained = () => new Promise((resolve) => stream.once('drain', resolve));
  for (let offset = 0; offset < size; offset += 64 * 1024) {
    if (!stream.write(payload.subarray(offset, offset + 64 * 1024))) await drained();
  }
  stream.end();
  assert.deepStrictEqual(await within(receiving, 'upload read'), { total: size, sum });

  const { id } = await client.api.calc.download({ size: 100 * 1024 });
  const readable = client.getStream(id);
  let got = 0;
  for await (const chunk of readable) got += chunk.length;
  assert.strictEqual(got, 100 * 1024);
});

test('attachChannel: a factory reconnects the client on a fresh channel and the subscription resumes', async (t) => {
  const served = routerOf();
  const rpc = new RpcServer({ router: served.router, logger: quiet });
  t.after(() => rpc.close());
  const first = await rawChannelPair(t);
  const attached = [rpc.attachChannel(first.b, { peer: 'p' })];
  let calls = 0;
  const client = await WrpcClient.connect('webrtc:server', {
    transport: 'webrtc',
    // The application's own recovery: every re-open negotiates a new pair
    // and attaches its far end — what a real one would do over its signaling.
    channel: async () => {
      calls++;
      if (calls === 1) return first.a;
      const pair = await rawChannelPair(t, { world: first.world });
      attached.push(rpc.attachChannel(pair.b, { peer: 'p' }));
      return pair.a;
    },
    heartbeat: false,
    reconnect: { minDelay: 5, maxDelay: 20, jitter: false },
  });
  t.after(() => client.close());
  await client.load('calc');
  const got = [];
  const handle = client.api.calc.feed.subscribe({}, { onData: (value) => got.push(value) });
  await waitFor(() => served.feed.streams.size === 1, 'the handler is live');
  served.push('one');
  served.push('two');
  await waitFor(() => got.length === 2, 'two values');
  assert.match(handle.lastEventId, /\.1$/, 'the log labels every value');

  const reconnected = onceEvent(client, 'reconnect');
  first.b.close(); // the server side's channel dies
  await within(reconnected, 'reconnected');
  assert.strictEqual(calls, 2);
  assert.strictEqual(attached.length, 2);
  await waitFor(() => !rpc.clients.has(attached[0]) && rpc.clients.has(attached[1]), 'old client gone, new one in');
  await waitFor(() => served.feed.streams.size === 1, 'the handler is live again');
  served.push('three');
  await waitFor(() => got.length === 3, 'resumed value');
  assert.deepStrictEqual(got, ['one', 'two', 'three']);
  assert.strictEqual(await client.api.calc.add({ a: 1, b: 1 }), 2);
});

test('attachChannel: a static channel cannot come back — reconnect: false ends it cleanly', async (t) => {
  const { pair, client } = await boot(t);
  const failed = onceEvent(client, 'reconnect-failed');
  pair.b.close();
  await within(failed, 'reconnect-failed');
  assert.strictEqual(client.active, false);
});

test('attachChannel: a framing error from the client is logged and closes the channel', async (t) => {
  const warnings = [];
  const logger = { ...quiet, warn: (entry) => warnings.push(entry) };
  const { rpc, pair, attached } = await boot(t, { logger, attach: { peer: 'bad' } });
  pair.a.send(new Uint8Array([0b11111111, 1]));
  await waitFor(() => !rpc.clients.has(attached), 'detached');
  const warned = warnings.find((entry) => entry.event === 'channel.error');
  assert.ok(warned, `expected a channel.error warning: ${JSON.stringify(warnings)}`);
  assert.strictEqual(warned.peer, 'bad');
  assert.strictEqual(warned.err.name, 'FramingError');
  assert.strictEqual(pair.b.readyState, 'closed');
});

test('attach: any persistent transport announcing packet/chunk is a client; a non-persistent one is refused', async (t) => {
  const served = routerOf();
  const rpc = new RpcServer({ router: served.router, logger: quiet });
  t.after(() => rpc.close());
  class FakeTransport extends Emitter {
    kind = 'custom';
    source = 'custom-peer';
    connection = this;
    written = [];
    write(data) {
      this.written.push(data);
      return true;
    }
    send(obj) {
      return this.write(JSON.stringify(obj));
    }
    error(code, { id = '' } = {}) {
      return this.send({ type: 'callback', id, error: { code } });
    }
    close() {
      void this.emit('close');
    }
  }
  const transport = new FakeTransport();
  const client = rpc.attach(transport);
  assert.strictEqual(client.persistent, true);
  assert.strictEqual(client.transportKind, 'custom');
  assert.strictEqual(client.source, 'custom-peer');
  void transport.emit('packet', JSON.stringify({ type: 'call', id: 'c1', method: 'calc/add', args: { a: 1, b: 2 } }));
  await waitFor(() => transport.written.length === 1, 'the answer');
  assert.deepStrictEqual(JSON.parse(transport.written[0]), { type: 'callback', id: 'c1', result: 3 });
  // A chunk for a stream nobody opened is dropped, never a crash.
  void transport.emit('chunk', chunkEncode('nope', new Uint8Array([1])));
  await timers.setTimeout(5);
  transport.close();
  await waitFor(() => !rpc.clients.has(client), 'detached');
  assert.throws(() => rpc.attach({ write() {}, close() {}, on() {}, once() {} }), /persistent transport/);
  assert.throws(() => rpc.attach(null), TypeError);
});

test('attach: the transport registered under the webrtc name is the one the client picks', () => {
  assert.strictEqual(WrpcClient.transport.webrtc, ClientRtcTransport);
});
