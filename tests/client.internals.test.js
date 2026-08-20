'use strict';

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { Emitter } = require('../src/utils.js');
const { WrpcClient, WrpcError } = require('../src/client.js');
const { chunkEncode } = require('../src/chunks.js');

class FakeTransport extends Emitter {
  active = false;
  sent = [];

  constructor(url) {
    super();
    this.url = url;
  }

  async open() {
    this.active = true;
    this.emit('open');
  }

  close() {
    this.active = false;
    this.emit('close');
  }

  write(data) {
    this.sent.push(data);
  }

  send(obj) {
    this.write(JSON.stringify(obj));
  }

  online() {}
  offline() {}
}

const makeClient = () => {
  const transport = new FakeTransport('fake://x');
  const client = new WrpcClient('fake://x', transport);
  return { client, transport };
};

test('WrpcClient packet handling', async (t) => {
  await t.test('#handleStream rejects a duplicate stream id', async () => {
    const { client, transport } = makeClient();
    const id = 'dup-1';
    transport.emit('message', JSON.stringify({ type: 'stream', id, name: 'a', size: 3 }));
    await timers.setImmediate();
    const errorPromise = new Promise((resolve) => client.once('error', resolve));
    transport.emit('message', JSON.stringify({ type: 'stream', id, name: 'a', size: 3 }));
    const error = await errorPromise;
    assert.match(error.message, /already initialized/);
  });

  await t.test('#handleStream errors on a status update for an unknown stream', async () => {
    const { client, transport } = makeClient();
    const errorPromise = new Promise((resolve) => client.once('error', resolve));
    transport.emit('message', JSON.stringify({ type: 'stream', id: 'missing', status: 'end' }));
    const error = await errorPromise;
    assert.match(error.message, /is not initialized/);
  });

  await t.test('#handleBinary errors for a chunk addressed to an unknown stream (ArrayBuffer input)', async () => {
    const { client, transport } = makeClient();
    const chunk = chunkEncode('missing', new Uint8Array([1, 2, 3]));
    const arrayBuffer = chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength);
    const errorPromise = new Promise((resolve) => client.once('error', resolve));
    transport.emit('message', arrayBuffer);
    const error = await errorPromise;
    assert.match(error.message, /is not initialized/);
  });

  await t.test('#handlePacket rejects invalid JSON and structurally invalid packets', async () => {
    const { client, transport } = makeClient();
    const invalidJson = new Promise((resolve) => client.once('error', resolve));
    transport.emit('message', 'not json');
    assert.match((await invalidJson).message, /Invalid JSON packet/);

    const noId = new Promise((resolve) => client.once('error', resolve));
    transport.emit('message', JSON.stringify({ type: 'callback' }));
    assert.match((await noId).message, /Packet structure error/);
  });

  await t.test('#handlePacket resolves pending calls via introspection', async () => {
    const { client, transport } = makeClient();
    transport.active = true;
    const loadPromise = client.load('greeting');
    await timers.setImmediate();
    const sentPacket = JSON.parse(transport.sent[0]);
    assert.strictEqual(sentPacket.method, 'system/introspect');
    transport.emit(
      'message',
      JSON.stringify({ type: 'callback', id: sentPacket.id, result: { greeting: { hello: {} } } }),
    );
    await loadPromise;
    assert.strictEqual(typeof client.api.greeting.hello, 'function');
  });

  await t.test('#handlePacket rejects pending calls with a WrpcError', async () => {
    const { client, transport } = makeClient();
    transport.active = true;
    const loadPromise = client.load('greeting');
    await timers.setImmediate();
    const sentPacket = JSON.parse(transport.sent[0]);
    transport.emit(
      'message',
      JSON.stringify({ type: 'callback', id: sentPacket.id, error: { message: 'nope', code: 500 } }),
    );
    await assert.rejects(loadPromise, (error) => error instanceof WrpcError && error.code === 500);
  });

  await t.test('#handlePacket carries wire error details onto the WrpcError', async () => {
    const { client, transport } = makeClient();
    transport.active = true;
    const loadPromise = client.load('greeting');
    await timers.setImmediate();
    const sentPacket = JSON.parse(transport.sent[0]);
    const details = { issues: [{ message: 'name required', path: ['name'] }] };
    transport.emit(
      'message',
      JSON.stringify({ type: 'callback', id: sentPacket.id, error: { message: 'invalid', code: 400, details } }),
    );
    await assert.rejects(loadPromise, (error) => {
      assert.ok(error instanceof WrpcError);
      assert.strictEqual(error.code, 400);
      assert.deepStrictEqual(error.details, details);
      return true;
    });
  });

  await t.test('WrpcError leaves details absent when the wire carried none', () => {
    const error = new WrpcError({ message: 'nope', code: 404 });
    assert.strictEqual('details' in error, false);
  });
});

test('WrpcClient static online/offline/initialize', async (t) => {
  await t.test('online()/offline() notify tracked transports', async () => {
    const { client, transport } = makeClient();
    await client.open();
    let onlineCalls = 0;
    let offlineCalls = 0;
    transport.online = () => onlineCalls++;
    transport.offline = () => offlineCalls++;

    WrpcClient.online();
    assert.strictEqual(onlineCalls, 1);
    assert.strictEqual(WrpcClient.isOnline, true);

    WrpcClient.offline();
    assert.strictEqual(offlineCalls, 1);
    assert.strictEqual(WrpcClient.isOnline, false);

    client.close();
  });

  await t.test('online() reopens inactive connections and surfaces open() failures', async () => {
    const { client, transport } = makeClient();
    await client.open();
    transport.active = false;
    let reopenAttempted = false;
    client.open = async () => {
      reopenAttempted = true;
      throw new Error('reconnect failed');
    };
    const errorPromise = new Promise((resolve) => client.once('error', resolve));
    WrpcClient.online();
    const error = await errorPromise;
    assert.strictEqual(reopenAttempted, true);
    assert.match(error.message, /reconnect failed/);
    WrpcClient.connections.delete(client);
  });

  await t.test('initialize() wires window online/offline listeners when window exists', () => {
    const listeners = {};
    globalThis.window = { addEventListener: (type, fn) => (listeners[type] = fn) };
    try {
      WrpcClient.initialize();
      assert.strictEqual(listeners.online, WrpcClient.online);
      assert.strictEqual(listeners.offline, WrpcClient.offline);
    } finally {
      delete globalThis.window;
    }
  });

  await t.test('initialize() wires self online/offline listeners when only self exists', () => {
    const listeners = {};
    globalThis.self = { addEventListener: (type, fn) => (listeners[type] = fn) };
    try {
      WrpcClient.initialize();
      assert.strictEqual(listeners.online, WrpcClient.online);
      assert.strictEqual(listeners.offline, WrpcClient.offline);
    } finally {
      delete globalThis.self;
    }
  });
});

test('WrpcClient.connect with a Service Worker (event transport)', async (t) => {
  await t.test('getInstance returns a shared singleton', () => {
    const first = WrpcClient.transport.event.getInstance('worker://app');
    const second = WrpcClient.transport.event.getInstance('worker://app');
    assert.strictEqual(first, second);
  });

  await t.test('a fresh instance requires a worker to open', async () => {
    const EventTransport = WrpcClient.transport.event;
    const transport = new EventTransport('worker://fresh');
    await assert.rejects(transport.open({}), /Service Worker not provided/);
  });

  await t.test('connect() opens the singleton, exposes online/offline, and closes cleanly', async () => {
    const sent = [];
    const worker = { postMessage: (msg) => sent.push(msg) };

    const client = await WrpcClient.connect('worker://shared', { worker });
    assert.strictEqual(client.active, true);
    assert.ok(sent.some((m) => m.type === 'wrpc:connect'));

    const transport = WrpcClient.transport.event.getInstance('worker://shared');
    transport.online();
    transport.offline();
    assert.ok(sent.some((m) => m.type === 'wrpc:online'));
    assert.ok(sent.some((m) => m.type === 'wrpc:offline'));

    client.close();
    assert.strictEqual(client.active, false);
  });
});
