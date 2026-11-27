'use strict';

const timers = require('node:timers/promises');
const { randomUUID } = require('node:crypto');
const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { Server } = require('../src/server.js');
const { Client } = require('../src/rpc/core.js');
const { SessionManager } = require('../src/rpc/sessions.js');
const { defineRouter, procedure } = require('../src/rpc/router.js');
const { handleRpc, handleMessage, handleBinary } = require('../src/rpc/dispatcher.js');
const { chunkEncode } = require('../src/chunks.js');
const { WrpcWritable } = require('../src/streams.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

const fakeHttpTransport = () => ({
  source: '127.0.0.1',
  connection: undefined,
  sent: [],
  errors: [],
  cookies: [],
  send(obj) {
    this.sent.push(obj);
  },
  error(code, { id = '', error = null } = {}) {
    this.errors.push({ code, id, error });
  },
  close: noop,
  sendSessionCookie(cookie) {
    this.cookies.push(cookie);
  },
});

const fakeWsTransport = () => ({
  source: '127.0.0.1',
  connection: {},
  sent: [],
  errors: [],
  send(obj) {
    this.sent.push(obj);
  },
  error(code, { id = '', error = null } = {}) {
    this.errors.push({ code, id, error });
  },
  close: noop,
  once: noop,
  sendSessionCookie: () => {
    throw new Error('sendSessionCookie should not be reachable from a ws transport');
  },
});

const createClient = (transport, sessions = new SessionManager({}, quiet), log = quiet) =>
  new Client(transport, { sessions, log });

const recordingStore = () => {
  const map = new Map();
  const calls = [];
  return {
    calls,
    async get(token) {
      return map.get(token) ?? null;
    },
    async set(token, data) {
      calls.push({ token, data: { ...data } });
      map.set(token, data);
    },
    async delete(token) {
      map.delete(token);
    },
  };
};

describe('Client over HTTP transport', () => {
  it('should throw from emit (non-close): HTTP has no persistent connection', () => {
    const client = createClient(fakeHttpTransport());
    assert.throws(() => client.sendEvent('ping', {}), /Can't send wrpc event to http transport/);
  });

  it('should not throw from emit("close")', () => {
    const client = createClient(fakeHttpTransport());
    assert.doesNotThrow(() => client.emit('close'));
  });

  it('should keep emit the LOCAL Emitter emit; sendEvent owns the wire', async () => {
    const closing = createClient(fakeHttpTransport()).emit('close');
    assert.ok(closing instanceof Promise);
    await closing;
    // `client.on(...)`/`client.emit(...)` behave like any Emitter — nothing
    // reaches the transport. That substitutability is the point of the
    // rename; the network send is spelled `sendEvent`, as it always was.
    const transport = fakeWsTransport();
    const client = createClient(transport);
    const seen = [];
    client.on('room/event', (data) => seen.push(data));
    await client.emit('room/event', { x: 1 });
    assert.deepStrictEqual(seen, [{ x: 1 }]);
    assert.deepStrictEqual(transport.sent, [], 'a local emit must not touch the wire');
    client.sendEvent('room/event', { x: 2 });
    assert.deepStrictEqual(transport.sent, [{ type: 'event', name: 'room/event', data: { x: 2 } }]);
  });

  it('should throw from getStream over HTTP', () => {
    const client = createClient(fakeHttpTransport());
    assert.throws(() => client.getStream('id'), /Can't receive stream from http transport/);
  });

  it('should throw from createStream over HTTP', () => {
    const client = createClient(fakeHttpTransport());
    assert.throws(() => client.createStream('name', 10), /Can't send wrpc streams to http transport/);
  });

  it('should send a session cookie from startSession via sendSessionCookie', async () => {
    const transport = fakeHttpTransport();
    const client = createClient(transport);
    const token = randomUUID();
    assert.strictEqual(client.startSession(token, {}), true);
    assert.strictEqual(transport.cookies.length, 1);
    assert.ok(transport.cookies[0].startsWith(`token=${token}; `));
    assert.match(transport.cookies[0], /HttpOnly/);
    await client.finalizeSession();
  });
});

describe('Client over WS transport', () => {
  it('should validate name and size in createStream', () => {
    const client = createClient(fakeWsTransport());
    assert.throws(() => client.createStream('', 10), /createStream: name must be a non-empty string/);
    assert.throws(() => client.createStream('name'), /createStream: size must be a non-negative integer/);
    // Zero is a size (tests/streams.hardening.test.js): what is refused is
    // anything that is not a count of bytes.
    assert.throws(() => client.createStream('name', -1), /createStream: size must be a non-negative integer/);
    assert.doesNotThrow(() => client.createStream('name', 0));
  });

  it('should send the init packet from createStream and keep the writable out of streams', () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    const stream = client.createStream('upload', 10);
    assert.ok(stream instanceof WrpcWritable);
    assert.deepStrictEqual(transport.sent, [{ type: 'stream', id: stream.id, name: 'upload', size: 10, cancel: true }]);
    // `streams` is what the peer announced: getStream() is never handed
    // this end's own writable, and `maxStreams` never counts it.
    assert.strictEqual(client.streams.size, 0);
    assert.throws(() => client.getStream(stream.id), /is not initialized/);
  });

  it('should throw from getStream for an unknown id', () => {
    const client = createClient(fakeWsTransport());
    assert.throws(() => client.getStream('missing'), /Stream missing is not initialized/);
  });

  it('should not send a cookie from startSession (persistent connection)', async () => {
    const client = createClient(fakeWsTransport());
    const token = randomUUID();
    assert.doesNotThrow(() => client.startSession(token, {}));
    await client.finalizeSession();
  });

  it('should run the session lifecycle: initialize, restore, finalize', async () => {
    const sessions = new SessionManager({}, quiet);
    const owner = createClient(fakeWsTransport(), sessions);
    const token = randomUUID();
    assert.strictEqual(owner.initializeSession(token, { role: 'admin' }), true);
    assert.strictEqual(owner.session.token, token);

    const restorer = createClient(fakeWsTransport(), sessions);
    assert.strictEqual(await restorer.restoreSession(token), true);
    assert.strictEqual(restorer.session.token, token);
    assert.strictEqual(restorer.session.state.role, 'admin');
    assert.strictEqual(await restorer.restoreSession('unknown-token'), false);

    assert.strictEqual(await owner.finalizeSession(), true);
    assert.strictEqual(owner.session, null);
    assert.strictEqual(await owner.finalizeSession(), false);
    // finalizeSession deletes the session from the store
    assert.strictEqual(await restorer.restoreSession(token), false);
  });

  it('should persist session state changes through the store', async () => {
    const store = recordingStore();
    const sessions = new SessionManager({ store }, quiet);
    const client = createClient(fakeWsTransport(), sessions);
    const token = randomUUID();
    client.initializeSession(token, {});
    client.session.state.role = 'admin';
    await timers.setImmediate();
    assert.strictEqual(store.calls.length, 2);
    assert.deepStrictEqual(store.calls[0], { token, data: {} });
    assert.deepStrictEqual(store.calls[1], { token, data: { role: 'admin' } });
    assert.deepStrictEqual(await store.get(token), { role: 'admin' });
  });

  it('should fail open streams on destroy but keep the stored session', async () => {
    const errors = [];
    const log = { ...quiet, error: (error) => errors.push(error) };
    const sessions = new SessionManager({}, quiet);
    const client = createClient(fakeWsTransport(), sessions, log);
    const token = randomUUID();
    client.initializeSession(token, {});

    // A reader in the middle of a stream is told that the rest is not
    // coming — a 503, as a call in flight is — and nobody is told for it.
    const failed = [];
    const stream = { onCancel: () => failed.push('told'), fail: (message, code) => failed.push([message, code]) };
    client.streams.set('a', stream);
    client.streams.set('c', { notAStream: true });

    client.destroy();
    await timers.setImmediate();
    await timers.setImmediate();

    assert.deepStrictEqual(failed, [['Connection closed', 503]]);
    assert.strictEqual(stream.onCancel, null);
    assert.strictEqual(client.streams.size, 0);
    assert.deepStrictEqual(errors, []);
    // NEW semantics: a dropped connection does not delete the session from
    // the store — a reconnecting client can restore it.
    const survivor = createClient(fakeWsTransport(), sessions);
    assert.strictEqual(await survivor.restoreSession(token), true);
    assert.strictEqual(survivor.session.token, token);
  });
});

const router = defineRouter({
  math: {
    add: procedure({ access: 'public', handler: async (_context, { a, b }) => Number(a) + Number(b) }),
  },
  'calc.v1': {
    double: procedure({ access: 'public', handler: async (_context, { x }) => x * 2 }),
  },
  secure: {
    whoami: procedure({ handler: async (context) => context.session.token }),
  },
  fail: {
    boom: procedure({
      access: 'public',
      handler: async () => {
        const error = new Error('domain failure');
        error.code = 422;
        return error;
      },
    }),
    crash: procedure({
      access: 'public',
      handler: async () => {
        throw new Error('kaput');
      },
    }),
    teapot: procedure({
      access: 'public',
      handler: async () => {
        const error = new Error('teapot');
        error.code = 418;
        throw error;
      },
    }),
    slow: procedure({
      access: 'public',
      handler: async () => {
        const error = new Error('late');
        error.code = 'ETIMEOUT';
        throw error;
      },
    }),
  },
});

describe('RPC dispatcher', () => {
  it('should dispatch a call in handleRpc and send a callback', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    await handleRpc(client, { type: 'call', id: '1', method: 'math/add', args: { a: 2, b: 3 } }, router);
    assert.deepStrictEqual(transport.sent, [{ type: 'callback', id: '1', result: 5 }]);
  });

  it('should resolve versioned units from the method name in handleRpc', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    await handleRpc(client, { type: 'call', id: '2', method: 'calc.v1/double', args: { x: 21 } }, router);
    assert.deepStrictEqual(transport.sent, [{ type: 'callback', id: '2', result: 42 }]);
    await handleRpc(client, { type: 'call', id: '3', method: 'calc/double', args: { x: 21 } }, router);
    assert.deepStrictEqual(transport.errors, [{ code: 404, id: '3', error: null }]);
  });

  it('should answer 404 in handleRpc for an unknown method', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    await handleRpc(client, { type: 'call', id: '4', method: 'nope/nothing', args: {} }, router);
    assert.deepStrictEqual(transport.errors, [{ code: 404, id: '4', error: null }]);
  });

  it('should miss in handleRpc on a multi-dot unit name instead of truncating the version', async () => {
    // 'calc.v1.2/double' must look up version 'v1.2' (a miss), never fall
    // back to the registered 'calc.v1'
    const transport = fakeWsTransport();
    const client = createClient(transport);
    await handleRpc(client, { type: 'call', id: '5', method: 'calc.v1.2/double', args: { x: 21 } }, router);
    assert.deepStrictEqual(transport.errors, [{ code: 404, id: '5', error: null }]);
    assert.deepStrictEqual(transport.sent, []);
  });

  it('should answer 403 in handleRpc for a session-access method without a session', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    await handleRpc(client, { type: 'call', id: '5', method: 'secure/whoami', args: {} }, router);
    assert.deepStrictEqual(transport.errors, [{ code: 403, id: '5', error: null }]);
  });

  it('should run a session-access method in handleRpc once a session exists', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    const token = randomUUID();
    client.initializeSession(token, {});
    await handleRpc(client, { type: 'call', id: '6', method: 'secure/whoami', args: {} }, router);
    assert.deepStrictEqual(transport.sent, [{ type: 'callback', id: '6', result: token }]);
  });

  it('should await sessionReady in handleRpc before the access check', async () => {
    const sessions = new SessionManager({}, quiet);
    const owner = createClient(fakeWsTransport(), sessions);
    const token = randomUUID();
    owner.initializeSession(token, {});

    const transport = fakeWsTransport();
    const client = createClient(transport, sessions);
    client.sessionReady = client.restoreSession(token);
    await handleRpc(client, { type: 'call', id: '7', method: 'secure/whoami', args: {} }, router);
    assert.deepStrictEqual(transport.sent, [{ type: 'callback', id: '7', result: token }]);
  });

  it('should surface Error-shaped results of handleRpc as protocol errors', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    await handleRpc(client, { type: 'call', id: '8', method: 'fail/boom', args: {} }, router);
    assert.strictEqual(transport.sent.length, 0);
    const [{ code, id, error }] = transport.errors;
    assert.strictEqual(code, 422);
    assert.strictEqual(id, '8');
    assert.match(error.message, /domain failure/);
  });

  it('should map errors thrown in handleRpc to 500 by default', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    await handleRpc(client, { type: 'call', id: '9', method: 'fail/crash', args: {} }, router);
    const [{ code, error }] = transport.errors;
    assert.strictEqual(code, 500);
    assert.match(error.message, /kaput/);
  });

  it('should pass numeric error codes through handleRpc and map ETIMEOUT to 408', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    await handleRpc(client, { type: 'call', id: '10', method: 'fail/teapot', args: {} }, router);
    await handleRpc(client, { type: 'call', id: '11', method: 'fail/slow', args: {} }, router);
    assert.strictEqual(transport.errors[0].code, 418);
    assert.strictEqual(transport.errors[1].code, 408);
  });

  it('should answer a structure error in handleMessage for an unrecognized packet shape', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    handleMessage(client, JSON.stringify({ nonsense: true }), router);
    handleMessage(client, 'not json at all', router);
    handleMessage(client, JSON.stringify({ type: 'call', method: 'math/add' }), router);
    assert.strictEqual(transport.errors.length, 3);
    for (const { code, error } of transport.errors) {
      assert.strictEqual(code, 500);
      assert.match(error.message, /Packet structure error/);
    }
  });

  it('should route a valid call packet from handleMessage to handleRpc', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    handleMessage(client, JSON.stringify({ type: 'call', id: '12', method: 'math/add', args: { a: 1, b: 1 } }), router);
    await timers.setImmediate();
    assert.deepStrictEqual(transport.sent, [{ type: 'callback', id: '12', result: 2 }]);
  });

  it('should reject an already-initialized stream id in handleStream', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    const id = randomUUID();
    const packet = JSON.stringify({ type: 'stream', id, name: 'dup', size: 3 });
    handleMessage(client, packet, router);
    await timers.setImmediate();
    assert.ok(client.streams.has(id));
    handleMessage(client, packet, router);
    await timers.setImmediate();
    const [{ code, error }] = transport.errors;
    assert.strictEqual(code, 400);
    assert.match(error.message, /already initialized/);
  });

  it('should answer a structure error in handleStream for a malformed stream packet', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    handleMessage(client, JSON.stringify({ type: 'stream', id: 'x', name: 'up', size: 'nope' }), router);
    await timers.setImmediate();
    const [{ code, error }] = transport.errors;
    assert.strictEqual(code, 400);
    assert.match(error.message, /Stream packet structure error/);
  });

  it('should answer an error in handleBinary for a chunk of an unknown stream id', async () => {
    const transport = fakeWsTransport();
    const client = createClient(transport);
    await handleBinary(client, chunkEncode('missing-stream', new Uint8Array([1, 2, 3])));
    const [{ code, id, error }] = transport.errors;
    assert.strictEqual(code, 400);
    assert.strictEqual(id, 'missing-stream');
    assert.match(error.message, /is not initialized/);
  });
});

const createServer = async (options = {}) => {
  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 50 },
    ...options,
  });
  await server.listen();
  const { port } = server.address();
  return { server, port };
};

describe('Server internals', () => {
  it('should serve REST-style RPC via /api/unit/method?params', async (t) => {
    const { server, port } = await createServer();
    t.after(() => server.close());

    const res = await fetch(`http://127.0.0.1:${port}/api/math/add?a=2&b=3`);
    const body = await res.json();
    assert.strictEqual(body.result, 5);
  });

  it('should retry listen on EADDRINUSE and reject once the retries are exhausted', async (t) => {
    const blocker = await createServer();
    t.after(() => blocker.server.close());

    const server = new Server({
      router,
      host: '127.0.0.1',
      port: blocker.port,
      protocol: 'http',
      logger: false,
      timeouts: { bind: 20 },
      retry: 2,
    });
    t.after(() => server.close());
    await assert.rejects(server.listen(), (error) => error.code === 'EADDRINUSE');
  });
  // The retry timer had no owner: a close() during the wait let the next
  // attempt bind a server that had been shut down, and listen() resolve.
  it('should cancel a listen retry that close() overtakes, and bind nothing', async (t) => {
    const blocker = await createServer();
    t.after(() => blocker.server.close());
    const server = new Server({
      router,
      host: '127.0.0.1',
      port: blocker.port,
      protocol: 'http',
      logger: false,
      timeouts: { bind: 40 },
      retry: 5,
    });
    const listening = server.listen();
    await timers.setTimeout(10);
    await server.close();
    await blocker.server.close();
    await assert.rejects(listening, (error) => error.cause?.code === 'EADDRINUSE');
    await timers.setTimeout(60);
    assert.strictEqual(server.httpServer.listening, false);
  });

  it('should resolve close() once its close listeners have settled, and survive one that throws', async (t) => {
    const { RpcServer } = require('../src/rpc/core.js');
    const { recorder } = require('./helpers/recorder.js');
    const log = recorder();
    const rpc = new RpcServer({ router, logger: log.writer, sse: false });
    let stopped = false;
    rpc.on('close', async () => {
      await timers.setTimeout(20);
      stopped = true;
    });
    rpc.on('close', () => {
      throw new Error('a listener of the application');
    });
    await rpc.close();
    assert.strictEqual(stopped, true, 'close() resolved before its listener finished');
    assert.strictEqual(log.find('listener.close')?.err?.message, 'a listener of the application');
  });
});
