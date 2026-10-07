'use strict';

const { MessageChannel } = require('node:worker_threads');
const { test } = require('node:test');
const assert = require('node:assert');

class MockWebSocket {
  static last = null;
  // The subprotocol the mocked server selects: none unless a test says so —
  // 'wrpc.v2' is a 2.x server, 'wrpc.v1' a 1.0 one.
  static protocol = '';

  constructor(url) {
    MockWebSocket.last = this;
    this.url = url;
    this.protocol = MockWebSocket.protocol;
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
const { encodeAttachments, decodeAttachments } = require('../src/attachments.js');
const { chunkEncode, chunkDecode } = require('../src/chunks.js');

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
// A port hop is a macrotask or two: poll for what should arrive.
const until = async (predicate) => {
  for (let i = 0; i < 200 && !predicate(); i++) await tick();
  assert.ok(predicate(), 'never arrived');
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

    // The call the closed page was waiting for is cancelled upstream: it
    // used to run on with nobody to hear the answer.
    assert.deepStrictEqual(JSON.parse(MockWebSocket.last.sentData.at(-1)), { type: 'cancel', id: 'gone' });
    MockWebSocket.last.dispatchMessage(JSON.stringify({ type: 'event', name: 'unit/name', data: 1 }));
    // Its answer, arriving anyway, is nobody's: dropped, not broadcast to
    // every other page as it used to be. An event still reaches everyone.
    MockWebSocket.last.dispatchMessage(JSON.stringify({ type: 'callback', id: 'gone', result: 0 }));
    await tick();
    assert.deepStrictEqual(
      received.map((r) => r.port),
      [2],
    );

    proxy.close();
    ch2.port1.close();
  });

  await t.test(
    "a page's subscriptions are unsubscribed on its goodbye; an upstream close forgets every id",
    async () => {
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
      ch1.port1.postMessage(JSON.stringify({ type: 'subscribe', id: 'feed', method: 'x/ticks', args: {} }));
      ch1.port1.postMessage(JSON.stringify({ type: 'call', id: 'c1', method: 'x/y', args: {} }));
      await tick();
      // The page's transport says goodbye (`wrpc:close`) before closing its
      // port — what an engine that never fires MessagePort close still hears.
      ch1.port1.postMessage({ type: 'wrpc:close' });
      await tick();
      const sent = MockWebSocket.last.sentData.slice(-2).map((data) => JSON.parse(data));
      assert.deepStrictEqual(
        sent.sort((a, b) => a.type.localeCompare(b.type)),
        [
          { type: 'cancel', id: 'c1' },
          { type: 'unsubscribe', id: 'feed' },
        ],
      );
      // Data for the feed arriving late is nobody's.
      MockWebSocket.last.dispatchMessage(JSON.stringify({ type: 'data', id: 'feed', data: 1 }));
      await tick();
      assert.deepStrictEqual(received, []);
      // Page 2 subscribes; the upstream connection closes: nothing is
      // remembered for it, and its answer after the reconnect is dropped.
      ch2.port1.postMessage(JSON.stringify({ type: 'subscribe', id: 'feed2', method: 'x/ticks', args: {} }));
      await tick();
      MockWebSocket.last.close();
      await tick();
      await tick();
      MockWebSocket.last.dispatchMessage(JSON.stringify({ type: 'data', id: 'feed2', data: 1 }));
      await tick();
      assert.deepStrictEqual(
        received.filter((r) => r.port === 2 && JSON.parse(r.data).type === 'data'),
        [],
      );
      proxy.close();
      ch1.port1.close();
      ch2.port1.close();
      ch1.port2.close();
      ch2.port2.close();
    },
  );

  await t.test('an attachments frame crosses the port both ways, routed by the packet it carries', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    // Revision 2 end to end: the server selected `wrpc.v2`, and each page
    // named revision 2 on its first ping (the proxy answers with its own).
    MockWebSocket.protocol = 'wrpc.v2';
    const proxy = new WrpcClientProxy();
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();
    globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [ch1.port2] });
    globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [ch2.port2] });
    await proxy.open();
    MockWebSocket.protocol = '';
    const received = [];
    const pongs = [];
    const take = (port) => (e) =>
      typeof e.data === 'string' && e.data.includes('"pong"')
        ? pongs.push(e.data)
        : received.push({ port, data: e.data });
    ch1.port1.onmessage = take(1);
    ch2.port1.onmessage = take(2);
    ch1.port1.start();
    ch2.port1.start();
    ch1.port1.postMessage('{"type":"ping","v":2}');
    ch2.port1.postMessage('{"type":"ping","v":2}');
    await until(() => pongs.length === 2);
    assert.deepStrictEqual(pongs, ['{"type":"pong","v":2}', '{"type":"pong","v":2}']);

    // Up: the page's client sent a call with bytes as a frame; it goes on
    // the wire as it is.
    const call = { type: 'call', id: 'bin-1', method: 'files/put', args: { body: new Uint8Array([1, 2, 3]) } };
    ch1.port1.postMessage(encodeAttachments(call));
    await until(() => MockWebSocket.last.sentData?.length > 0);
    const sent = MockWebSocket.last.sentData.at(-1);
    assert.ok(sent instanceof Uint8Array, 'a frame, not JSON');
    assert.deepStrictEqual(decodeAttachments(sent), { ...call, args: { body: new Uint8Array([1, 2, 3]) } });

    // Down: the answer, a frame with bytes, reaches the caller's port only
    // (the socket hands over an ArrayBuffer, as a browser's does).
    const answer = encodeAttachments({ type: 'callback', id: 'bin-1', result: { thumb: new Uint8Array([9]) } });
    MockWebSocket.last.dispatchMessage(answer.buffer);
    await until(() => received.length === 1);
    await tick();
    assert.deepStrictEqual(
      received.map((r) => r.port),
      [1],
    );
    assert.deepStrictEqual(decodeAttachments(received[0].data).result.thumb, new Uint8Array([9]));

    // An event with bytes is for every page; a batch is broadcast as before.
    MockWebSocket.last.dispatchMessage(
      encodeAttachments({ type: 'event', name: 'files/ready', data: new Uint8Array([7]) }).buffer,
    );
    await until(() => received.length === 3);
    assert.deepStrictEqual(
      received.slice(1).map((r) => r.port),
      [1, 2],
    );

    // A stream-style frame keeps its slot until the stream ends, like text.
    ch2.port1.postMessage(JSON.stringify({ type: 'call', id: 'bin-2', method: 'x/y', args: {} }));
    await until(() => MockWebSocket.last.sentData.length === 2);
    MockWebSocket.last.dispatchMessage(
      encodeAttachments({ type: 'stream', id: 'bin-2', status: 'end', data: new Uint8Array([1]) }).buffer,
    );
    await until(() => received.length === 4);
    assert.strictEqual(received.at(-1).port, 2);

    proxy.close();
    ch1.port1.close();
    ch1.port2.close();
    ch2.port1.close();
    ch2.port2.close();
  });

  await t.test('a page that named no revision is a 1.0 page: a frame from upstream reaches it as JSON', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    MockWebSocket.protocol = 'wrpc.v2';
    const proxy = new WrpcClientProxy();
    const old = new MessageChannel();
    const modern = new MessageChannel();
    globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [old.port2] });
    globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [modern.port2] });
    await proxy.open();
    MockWebSocket.protocol = '';
    const got = { old: [], modern: [] };
    old.port1.onmessage = (e) => got.old.push(e.data);
    modern.port1.onmessage = (e) => got.modern.push(e.data);
    old.port1.start();
    modern.port1.start();
    // A 1.0 page's heartbeat names nothing, and is answered as 1.0 was.
    old.port1.postMessage('{"type":"ping"}');
    modern.port1.postMessage('{"type":"ping","v":2}');
    await until(() => got.old.length === 1 && got.modern.length === 1);
    assert.deepStrictEqual([got.old[0], got.modern[0]], ['{"type":"pong"}', '{"type":"pong","v":2}']);

    // One event with bytes, for every page: each in the form it reads.
    MockWebSocket.last.dispatchMessage(
      encodeAttachments({ type: 'event', name: 'files/ready', data: { blob: new Uint8Array([7, 8]) } }).buffer,
    );
    await until(() => got.old.length === 2 && got.modern.length === 2);
    assert.deepStrictEqual(JSON.parse(got.old[1]), {
      type: 'event',
      name: 'files/ready',
      data: { blob: { 0: 7, 1: 8 } },
    });
    assert.deepStrictEqual(decodeAttachments(got.modern[1]).data.blob, new Uint8Array([7, 8]));

    // A routed answer follows the same rule.
    old.port1.postMessage(JSON.stringify({ type: 'call', id: 'o-1', method: 'files/get', args: {} }));
    await until(() => MockWebSocket.last.sentData?.length === 1);
    MockWebSocket.last.dispatchMessage(
      encodeAttachments({ type: 'callback', id: 'o-1', result: { blob: new Uint8Array([1]) } }).buffer,
    );
    await until(() => got.old.length === 3);
    assert.deepStrictEqual(JSON.parse(got.old[2]), { type: 'callback', id: 'o-1', result: { blob: { 0: 1 } } });
    assert.strictEqual(got.modern.length, 2, 'the answer went to its caller only');

    proxy.close();
    for (const port of [old.port1, old.port2, modern.port1, modern.port2]) port.close();
  });

  await t.test('upstream of the proxy is a 1.0 server: a page’s frame leaves as the JSON of its packet', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    MockWebSocket.protocol = 'wrpc.v1';
    const proxy = new WrpcClientProxy();
    const page = new MessageChannel();
    globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [page.port2] });
    await proxy.open();
    MockWebSocket.protocol = '';
    page.port1.start();
    const call = { type: 'call', id: 'up-1', method: 'files/put', args: { body: new Uint8Array([1, 2, 3]) } };
    page.port1.postMessage(encodeAttachments(call));
    await until(() => MockWebSocket.last.sentData?.length > 0);
    const sent = MockWebSocket.last.sentData.at(-1);
    assert.strictEqual(typeof sent, 'string', 'no frame reaches a server that reads none');
    assert.deepStrictEqual(JSON.parse(sent), { ...call, args: { body: { 0: 1, 1: 2, 2: 3 } } });

    proxy.close();
    page.port1.close();
    page.port2.close();
  });

  await t.test(
    'a call the proxy cannot forward is answered with a coded error, never an unhandled rejection',
    async () => {
      savedSelf = globalThis.self;
      globalThis.self = createSwEnv();
      const unhandled = [];
      const onUnhandled = (error) => unhandled.push(error);
      process.on('unhandledRejection', onUnhandled);
      const originalConnect = WrpcClient.connect;
      WrpcClient.connect = async () => {
        throw Object.assign(new Error('upstream refused'), { code: 502 });
      };
      const proxy = new WrpcClientProxy();
      const { port1, port2 } = new MessageChannel();
      try {
        globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [port2] });
        const received = [];
        port1.onmessage = (e) => received.push(e.data);
        port1.start();
        port1.postMessage(JSON.stringify({ type: 'call', id: 'c-1', method: 'x/y', args: {} }));
        port1.postMessage(
          encodeAttachments({ type: 'call', id: 'c-2', method: 'x/y', args: { b: new Uint8Array(1) } }),
        );
        // Not packets at all: nothing to answer, nothing to blow up on.
        port1.postMessage('not json');
        port1.postMessage(new Uint8Array([1, 2, 3]));
        port1.postMessage(JSON.stringify({ type: 'event', name: 'x/y', data: 1 }));
        await until(() => received.length === 2);
        await tick();
        assert.deepStrictEqual(
          received.map((text) => JSON.parse(text)),
          [
            { type: 'callback', id: 'c-1', error: { message: 'upstream refused', code: 502 } },
            { type: 'callback', id: 'c-2', error: { message: 'upstream refused', code: 502 } },
          ],
        );
        // A heartbeat is answered without a connection at all.
        port1.postMessage(JSON.stringify({ type: 'ping' }));
        await until(() => received.length === 3);
        assert.deepStrictEqual(JSON.parse(received.at(-1)), { type: 'pong' });
      } finally {
        // Closed whatever happened above: an open port keeps the process alive.
        proxy.close();
        port1.close();
        port2.close();
        WrpcClient.connect = originalConnect;
        process.off('unhandledRejection', onUnhandled);
      }
      assert.deepStrictEqual(unhandled, []);
    },
  );
  await t.test('pages whose first calls arrive together share one upstream, and hear its loss at once', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const originalConnect = WrpcClient.connect;
    let connects = 0;
    WrpcClient.connect = (...args) => {
      connects++;
      return originalConnect.apply(WrpcClient, args);
    };
    const proxy = new WrpcClientProxy({ reconnect: false });
    const pages = [new MessageChannel(), new MessageChannel(), new MessageChannel()];
    const received = pages.map(() => []);
    try {
      pages.forEach(({ port1, port2 }, i) => {
        globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [port2] });
        port1.onmessage = (e) => received[i].push(JSON.parse(e.data));
        port1.start();
      });
      // Three tabs, three first calls in one turn: three upstream sockets before.
      pages.forEach(({ port1 }, i) => {
        port1.postMessage(JSON.stringify({ type: 'call', id: `c${i}`, method: 'x/y', args: {} }));
      });
      await until(() => MockWebSocket.last?.sentData?.length === 3);
      assert.strictEqual(connects, 1, 'one connect for all three');
      // The upstream goes while the three wait: each hears it now — a call
      // used to sit until its callTimeout.
      MockWebSocket.last.close();
      await until(() => received.every((messages) => messages.length === 1));
      received.forEach((messages, i) => {
        assert.deepStrictEqual(messages[0], {
          type: 'callback',
          id: `c${i}`,
          error: { message: 'The worker lost its connection to the server', code: 503 },
        });
      });
    } finally {
      proxy.close();
      for (const { port1, port2 } of pages) {
        port1.close();
        port2.close();
      }
      WrpcClient.connect = originalConnect;
    }
  });

  // A binary stream is packets AND raw chunks: the `stream` packet announces
  // it, the chunks carry its bytes, a `stream` packet with a status ends it.
  // Both halves have to cross the worker, or a page's upload reaches the
  // server empty and a server's download reaches the page empty.
  const bytesOf = (data) => new Uint8Array(data instanceof ArrayBuffer ? data : (data.buffer ?? data));

  await t.test('a server-opened stream reaches the pages: its chunks follow its announcement', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    const pages = [new MessageChannel(), new MessageChannel()];
    const got = pages.map(() => []);
    try {
      pages.forEach(({ port1, port2 }, i) => {
        globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [port2] });
        port1.onmessage = (e) => got[i].push(e.data);
        port1.start();
      });
      await proxy.open();
      const upstream = MockWebSocket.last;
      // The download's call, and its answer naming the stream.
      pages[0].port1.postMessage(JSON.stringify({ type: 'call', id: 'd-1', method: 'media/download', args: {} }));
      await until(() => upstream.sentData?.length === 1);
      upstream.dispatchMessage(JSON.stringify({ type: 'callback', id: 'd-1', result: { id: 's-1' } }));
      upstream.dispatchMessage(JSON.stringify({ type: 'stream', id: 's-1', name: 'report.csv', size: 3 }));
      upstream.dispatchMessage(chunkEncode('s-1', new Uint8Array([1, 2, 3])).buffer);
      upstream.dispatchMessage(JSON.stringify({ type: 'stream', id: 's-1', status: 'end' }));
      // The callback to its caller; the stream — announcement, chunk, end —
      // to every page, as the announcement always went.
      await until(() => got[0].length === 4 && got[1].length === 3);
      const chunk = got[0][2];
      assert.ok(typeof chunk !== 'string', 'the chunk arrives as bytes');
      const { id, payload } = chunkDecode(bytesOf(chunk));
      assert.strictEqual(id, 's-1');
      assert.deepStrictEqual([...payload], [1, 2, 3]);
      assert.deepStrictEqual([...bytesOf(got[1][1])], [...bytesOf(chunk)]);
      assert.deepStrictEqual(JSON.parse(got[0][3]), { type: 'stream', id: 's-1', status: 'end' });
      // Ended: a late chunk of it, and one of a stream nobody announced, go nowhere.
      upstream.dispatchMessage(chunkEncode('s-1', new Uint8Array([4])).buffer);
      upstream.dispatchMessage(chunkEncode('nobody', new Uint8Array([5])).buffer);
      await tick();
      await tick();
      assert.deepStrictEqual(
        got.map((messages) => messages.length),
        [4, 3],
      );
    } finally {
      proxy.close();
      for (const { port1, port2 } of pages) {
        port1.close();
        port2.close();
      }
    }
  });

  await t.test("a page's upload reaches the server: its chunks go up between its packets", async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    const page = new MessageChannel();
    const other = new MessageChannel();
    try {
      globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [page.port2] });
      globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [other.port2] });
      page.port1.start();
      other.port1.start();
      await proxy.open();
      const upstream = MockWebSocket.last;
      page.port1.postMessage(JSON.stringify({ type: 'stream', id: 'u-1', name: 'video.mp4', size: 4 }));
      page.port1.postMessage(chunkEncode('u-1', new Uint8Array([1, 2])));
      page.port1.postMessage(chunkEncode('u-1', new Uint8Array([3, 4])));
      // Another page cannot write into a stream it did not announce.
      other.port1.postMessage(chunkEncode('u-1', new Uint8Array([9])));
      page.port1.postMessage(JSON.stringify({ type: 'stream', id: 'u-1', status: 'end' }));
      await until(() => upstream.sentData?.length === 4);
      await tick();
      const sent = upstream.sentData;
      assert.strictEqual(sent.length, 4, 'nothing from the other page');
      assert.deepStrictEqual(JSON.parse(sent[0]), { type: 'stream', id: 'u-1', name: 'video.mp4', size: 4 });
      assert.deepStrictEqual(
        sent.slice(1, 3).map((bytes) => {
          const { id, payload } = chunkDecode(bytesOf(bytes));
          return [id, ...payload];
        }),
        [
          ['u-1', 1, 2],
          ['u-1', 3, 4],
        ],
      );
      assert.deepStrictEqual(JSON.parse(sent[3]), { type: 'stream', id: 'u-1', status: 'end' });
      // Ended: a chunk after it is dropped, not forwarded.
      page.port1.postMessage(chunkEncode('u-1', new Uint8Array([5])));
      await tick();
      await tick();
      assert.strictEqual(upstream.sentData.length, 4);
    } finally {
      proxy.close();
      for (const port of [page.port1, page.port2, other.port1, other.port2]) port.close();
    }
  });

  await t.test('a page that leaves mid-upload terminates its stream upstream', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy();
    const page = new MessageChannel();
    try {
      globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [page.port2] });
      page.port1.start();
      await proxy.open();
      const upstream = MockWebSocket.last;
      page.port1.postMessage(JSON.stringify({ type: 'stream', id: 'u-2', name: 'big.bin', size: 1024 }));
      page.port1.postMessage(chunkEncode('u-2', new Uint8Array([1])));
      await until(() => upstream.sentData?.length === 2);
      // The server would otherwise wait for the rest of the bytes forever.
      page.port1.postMessage({ type: 'wrpc:close' });
      await until(() => upstream.sentData.length === 3);
      assert.deepStrictEqual(JSON.parse(upstream.sentData[2]), { type: 'stream', id: 'u-2', status: 'terminate' });
    } finally {
      proxy.close();
      page.port1.close();
      page.port2.close();
    }
  });

  await t.test('a lost upstream ends the server streams the pages were reading', async () => {
    savedSelf = globalThis.self;
    globalThis.self = createSwEnv();
    const proxy = new WrpcClientProxy({ reconnect: false });
    const pages = [new MessageChannel(), new MessageChannel()];
    const got = pages.map(() => []);
    try {
      pages.forEach(({ port1, port2 }, i) => {
        globalThis.self.dispatch({ data: { type: 'wrpc:connect' }, ports: [port2] });
        port1.onmessage = (e) => got[i].push(e.data);
        port1.start();
      });
      await proxy.open();
      const upstream = MockWebSocket.last;
      upstream.dispatchMessage(JSON.stringify({ type: 'stream', id: 's-9', name: 'live.bin', size: 1024 }));
      upstream.dispatchMessage(chunkEncode('s-9', new Uint8Array([1])).buffer);
      await until(() => got.every((messages) => messages.length === 2));
      // No `end` will come: each reader hears the stream is over now.
      upstream.close();
      await until(() => got.every((messages) => messages.length === 3));
      for (const messages of got) {
        assert.deepStrictEqual(JSON.parse(messages[2]), { type: 'stream', id: 's-9', status: 'terminate' });
      }
    } finally {
      proxy.close();
      for (const { port1, port2 } of pages) {
        port1.close();
        port2.close();
      }
    }
  });
});
