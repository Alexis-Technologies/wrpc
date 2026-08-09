'use strict';

const { MessageChannel } = require('node:worker_threads');
const { test } = require('node:test');
const assert = require('node:assert');

class MockWebSocket {
  static last = null;

  constructor() {
    MockWebSocket.last = this;
    this._listeners = new Map();
    queueMicrotask(() => this._emit('open'));
  }

  addEventListener(type, fn) {
    if (!this._listeners.has(type)) this._listeners.set(type, []);
    this._listeners.get(type).push(fn);
  }

  _emit(type, payload) {
    for (const fn of this._listeners.get(type) || []) {
      if (type === 'message') fn({ data: payload });
      else fn(payload);
    }
  }

  dispatchMessage(data) {
    this._emit('message', data);
  }

  send(data) {
    this.sentData ??= [];
    this.sentData.push(data);
    this._emit('send');
  }

  close() {
    this._emit('close');
  }
}

globalThis.WebSocket = MockWebSocket;

const { WrpcClient, WrpcClientProxy } = require('../src/client.js');

const { emitWarning } = process;
process.emitWarning = (warning, type, ...args) => {
  if (type === 'ExperimentalWarning') return;
  emitWarning(warning, type, ...args);
};

const createSwEnv = () => {
  const listeners = [];
  return {
    _listeners: listeners,
    addEventListener(type, fn) {
      listeners.push({ type, fn });
    },
    location: { protocol: 'http:', host: 'localhost:8020' },
    dispatch(event) {
      for (const { type, fn } of listeners) {
        if (type === 'message') fn(event);
      }
    },
  };
};

test('WrpcClientProxy', async (t) => {
  let savedSelf;

  t.after(() => {
    delete globalThis.self;
  });

  t.afterEach(() => {
    if (savedSelf !== undefined) {
      globalThis.self = savedSelf;
    } else {
      delete globalThis.self;
    }
  });

  await t.test('Throws when not in Service Worker context', () => {
    savedSelf = globalThis.self;
    delete globalThis.self;
    assert.throws(() => new WrpcClientProxy(), /WrpcClientProxy must run in ServiceWorker context/);
  });

  await t.test('Constructs when self is defined', () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    assert.doesNotThrow(() => new WrpcClientProxy());
  });

  await t.test('Handle event: wrpc:connect', () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    const { port1, port2 } = new MessageChannel();
    globalThis.self.dispatch({
      data: { type: 'wrpc:connect' },
      ports: [port2],
    });
    proxy.close();
    port1.close();
    port2.close();
  });

  await t.test('Broadcast to all ports', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();
    globalThis.self.dispatch({
      data: { type: 'wrpc:connect' },
      ports: [ch1.port2],
    });
    globalThis.self.dispatch({
      data: { type: 'wrpc:connect' },
      ports: [ch2.port2],
    });
    const received = [];
    ch1.port1.onmessage = (e) => received.push({ port: 1, data: e.data });
    ch2.port1.onmessage = (e) => received.push({ port: 2, data: e.data });
    ch1.port1.start();
    ch2.port1.start();
    await proxy.open();
    MockWebSocket.last.dispatchMessage('payload');
    await new Promise((resolve) => setImmediate(resolve));
    assert.strictEqual(received.length, 2);
    assert.strictEqual(received[0].data, 'payload');
    assert.strictEqual(received[1].data, 'payload');
    proxy.close();
    ch1.port1.close();
    ch1.port2.close();
    ch2.port1.close();
    ch2.port2.close();
  });

  await t.test('Handle event: wrpc:online', () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    let called = false;
    const original = WrpcClient.online;
    WrpcClient.online = () => {
      called = true;
    };
    globalThis.self.dispatch({ data: { type: 'wrpc:online' } });
    WrpcClient.online = original;
    assert(called);
    proxy.close();
  });

  await t.test('Handle event: wrpc:offline', () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    let called = false;
    const original = WrpcClient.offline;
    WrpcClient.offline = () => {
      called = true;
    };
    globalThis.self.dispatch({ data: { type: 'wrpc:offline' } });
    WrpcClient.offline = original;
    assert(called);
    proxy.close();
  });

  await t.test('Handle event: unknown wrpc event type throws', () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    assert.throws(() => globalThis.self.dispatch({ data: { type: 'wrpc:bogus' } }), /Unknown event: wrpc:bogus/);
    proxy.close();
  });

  await t.test('open() reuses the existing connection on subsequent calls', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    await proxy.open();
    await assert.doesNotReject(proxy.open());
    proxy.close();
  });

  await t.test('#handleMessage forwards port calls and #proxyPacket routes responses back', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    const { port1, port2 } = new MessageChannel();
    globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [port2] });
    await proxy.open();

    const received = [];
    port1.onmessage = (e) => received.push(e.data);
    port1.start();

    const callId = 'call-42';
    port1.postMessage(JSON.stringify({ type: 'call', id: callId, method: 'x/y', args: {} }));
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepStrictEqual(
      MockWebSocket.last.sentData.at(-1),
      JSON.stringify({ type: 'call', id: callId, method: 'x/y', args: {} }),
    );

    MockWebSocket.last.dispatchMessage(JSON.stringify({ type: 'callback', id: callId, result: 42 }));
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepStrictEqual(JSON.parse(received.at(-1)), { type: 'callback', id: callId, result: 42 });

    MockWebSocket.last.dispatchMessage(JSON.stringify({ type: 'event', name: 'unit/name', data: 1 }));
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepStrictEqual(JSON.parse(received.at(-1)), { type: 'event', name: 'unit/name', data: 1 });

    MockWebSocket.last.dispatchMessage(JSON.stringify({ type: 'stream', id: 'no-such-call', status: 'end' }));
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepStrictEqual(JSON.parse(received.at(-1)), { type: 'stream', id: 'no-such-call', status: 'end' });

    proxy.close();
    port1.close();
    port2.close();
  });
});
