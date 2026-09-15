'use strict';

const { MessageChannel } = require('node:worker_threads');
const { test } = require('node:test');
const assert = require('node:assert');

class MockWebSocket {
  static last = null;

  constructor(url) {
    MockWebSocket.last = this;
    this.url = url;
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
    // `message` is what a ServiceWorker's self fires; `connect` is a
    // SharedWorker's, carrying the per-page port.
    dispatch(event, eventType = 'message') {
      for (const { type, fn } of listeners) {
        if (type === eventType) fn(event);
      }
    },
  };
};

const tick = () => new Promise((resolve) => setImmediate(resolve));

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
    assert.throws(() => new WrpcClientProxy(), /WrpcClientProxy must run in a worker context/);
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

  await t.test('open() skips reopening while the connection is active', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    await proxy.open();
    const originalOpen = WrpcClient.prototype.open;
    let reopens = 0;
    WrpcClient.prototype.open = function (...args) {
      reopens++;
      return originalOpen.apply(this, args);
    };
    try {
      await proxy.open();
    } finally {
      WrpcClient.prototype.open = originalOpen;
    }
    assert.strictEqual(reopens, 0);
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

  await t.test('SharedWorker: wrpc:connect over the connection port, then a round trip', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    const conn = new MessageChannel();
    const page = new MessageChannel();
    globalThis.self.dispatch({ ports: [conn.port2] }, 'connect');
    conn.port1.postMessage({ type: 'wrpc:connect' }, [page.port2]);
    await tick();
    await proxy.open();

    const received = [];
    page.port1.onmessage = (e) => received.push(e.data);
    page.port1.start();

    page.port1.postMessage(JSON.stringify({ type: 'call', id: 'sw-1', method: 'x/y', args: {} }));
    await tick();
    assert.deepStrictEqual(JSON.parse(MockWebSocket.last.sentData.at(-1)), {
      type: 'call',
      id: 'sw-1',
      method: 'x/y',
      args: {},
    });
    MockWebSocket.last.dispatchMessage(JSON.stringify({ type: 'callback', id: 'sw-1', result: 1 }));
    await tick();
    assert.deepStrictEqual(JSON.parse(received.at(-1)), { type: 'callback', id: 'sw-1', result: 1 });

    proxy.close();
    page.port1.close();
    conn.port1.close();
  });

  await t.test('SharedWorker: online/offline over the connection port', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    const conn = new MessageChannel();
    globalThis.self.dispatch({ ports: [conn.port2] }, 'connect');
    const calls = [];
    const { online, offline } = WrpcClient;
    WrpcClient.online = () => calls.push('online');
    WrpcClient.offline = () => calls.push('offline');
    try {
      conn.port1.postMessage({ type: 'wrpc:online' });
      conn.port1.postMessage({ type: 'wrpc:offline' });
      await tick();
    } finally {
      WrpcClient.online = online;
      WrpcClient.offline = offline;
    }
    assert.deepStrictEqual(calls, ['online', 'offline']);
    proxy.close();
    conn.port1.close();
  });

  await t.test('SharedWorker: junk on the connection port is ignored', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    const conn = new MessageChannel();
    globalThis.self.dispatch({ ports: [conn.port2] }, 'connect');
    conn.port1.postMessage(null);
    conn.port1.postMessage('hello');
    conn.port1.postMessage({ type: 42 });
    await tick();
    // Still alive: a real connect after the junk registers as usual.
    const page = new MessageChannel();
    conn.port1.postMessage({ type: 'wrpc:connect' }, [page.port2]);
    await tick();
    const received = [];
    page.port1.onmessage = (e) => received.push(e.data);
    page.port1.start();
    await proxy.open();
    MockWebSocket.last.dispatchMessage('payload');
    await tick();
    assert.deepStrictEqual(received, ['payload']);
    proxy.close();
    page.port1.close();
    conn.port1.close();
  });

  await t.test('url option overrides the location-derived one', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const derived = new WrpcClientProxy();
    await derived.open();
    assert.strictEqual(MockWebSocket.last.url, 'ws://localhost:8020');
    derived.close();
    const explicit = new WrpcClientProxy({ url: 'wss://api.example.com/rpc' });
    await explicit.open();
    assert.strictEqual(MockWebSocket.last.url, 'wss://api.example.com/rpc');
    explicit.close();
  });

  await t.test('a page closing its port releases the port and its pending answers', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();
    globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [ch1.port2] });
    globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [ch2.port2] });
    await proxy.open();
    const received = [];
    ch1.port1.onmessage = (e) => received.push({ port: 1, data: e.data });
    ch2.port1.onmessage = (e) => received.push({ port: 2, data: e.data });
    ch1.port1.start();
    ch2.port1.start();

    // A call parked on port 1, then the page goes away. The proxy's own
    // `close` listener was registered at connect time, so it runs before
    // this one — and Node delivers `close` later than the next tick.
    ch1.port1.postMessage(JSON.stringify({ type: 'call', id: 'gone', method: 'x/y', args: {} }));
    await tick();
    const closed = new Promise((resolve) => ch1.port2.addEventListener('close', resolve, { once: true }));
    ch1.port1.close();
    await closed;

    MockWebSocket.last.dispatchMessage(JSON.stringify({ type: 'event', name: 'unit/name', data: 1 }));
    // The pending slot is gone with the port, so the answer falls back to a
    // broadcast — which no longer includes the closed port either.
    MockWebSocket.last.dispatchMessage(JSON.stringify({ type: 'callback', id: 'gone', result: 0 }));
    await tick();
    assert.deepStrictEqual(
      received.map((r) => r.port),
      [2, 2],
    );

    proxy.close();
    ch2.port1.close();
  });
});
