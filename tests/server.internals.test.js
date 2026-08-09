'use strict';

const timers = require('node:timers/promises');
const { randomUUID } = require('node:crypto');
const { test } = require('node:test');
const assert = require('node:assert');

const { Server, Client } = require('../src/server.js');
const { WrpcClient } = require('../src/client.js');
const { chunkEncode } = require('../src/chunks.js');

const { emitWarning } = process;
process.emitWarning = (warning, type, ...args) => {
  if (type === 'ExperimentalWarning') return;
  emitWarning(warning, type, ...args);
};

const noop = () => {};

const fakeContext = () => ({
  console: { log: noop, info: noop, warn: noop, error: noop, debug: noop },
  auth: { saveSession: async () => {} },
});

const fakeHttpTransport = () => ({
  source: '127.0.0.1',
  connection: undefined,
  error: noop,
  send: noop,
  close: noop,
  sendSessionCookie: () => {
    throw new Error('sendSessionCookie should not be reachable from this transport in this test');
  },
  removeSessionCookie: noop,
});

const fakeWsTransport = () => ({
  source: '127.0.0.1',
  connection: {},
  error: noop,
  send: noop,
  close: noop,
  sendSessionCookie: noop,
  removeSessionCookie: noop,
});

test('Client over HTTP transport', async (t) => {
  await t.test('emit (non-close) throws: HTTP has no persistent connection', () => {
    const client = new Client(fakeHttpTransport(), fakeContext());
    assert.throws(() => client.emit('ping', {}), /Can't send wrpc event to http transport/);
  });

  await t.test('emit("close") does not throw', () => {
    const client = new Client(fakeHttpTransport(), fakeContext());
    assert.doesNotThrow(() => client.emit('close'));
  });

  await t.test('getStream throws over HTTP', () => {
    const client = new Client(fakeHttpTransport(), fakeContext());
    assert.throws(() => client.getStream('id'), /Can't receive stream from http transport/);
  });

  await t.test('createStream throws over HTTP', () => {
    const client = new Client(fakeHttpTransport(), fakeContext());
    assert.throws(() => client.createStream('name', 10), /Can't send wrpc streams to http transport/);
  });

  await t.test('startSession sends a session cookie for HTTP clients', () => {
    let cookieSent = null;
    const transport = fakeHttpTransport();
    transport.sendSessionCookie = (token) => (cookieSent = token);
    const client = new Client(transport, fakeContext());
    const token = randomUUID();
    client.startSession(token, {});
    assert.strictEqual(cookieSent, token);
    client.finalizeSession();
  });
});

test('Client over WS transport', async (t) => {
  await t.test('createStream validates name and size', () => {
    const client = new Client(fakeWsTransport(), fakeContext());
    assert.throws(() => client.createStream('', 10), /Stream name is not provided/);
    assert.throws(() => client.createStream('name', 0), /Stream size is not provided/);
  });

  await t.test('createStream registers a WrpcWritable', () => {
    const client = new Client(fakeWsTransport(), fakeContext());
    const stream = client.createStream('upload', 10);
    assert.strictEqual(client.getStream(stream.id), stream);
  });

  await t.test('getStream throws for an unknown id', () => {
    const client = new Client(fakeWsTransport(), fakeContext());
    assert.throws(() => client.getStream('missing'), /Stream missing is not initialized/);
  });

  await t.test('startSession does not send a cookie (persistent connection)', () => {
    const client = new Client(fakeWsTransport(), fakeContext());
    const token = randomUUID();
    assert.doesNotThrow(() => client.startSession(token, {}));
    client.finalizeSession();
  });

  await t.test('session lifecycle: start, restore, finalize', () => {
    const owner = new Client(fakeWsTransport(), fakeContext());
    const token = randomUUID();
    assert.strictEqual(owner.initializeSession(token, { role: 'admin' }), true);
    assert.strictEqual(owner.session.token, token);

    const restorer = new Client(fakeWsTransport(), fakeContext());
    assert.strictEqual(restorer.restoreSession(token), true);
    assert.strictEqual(restorer.session.token, token);
    assert.strictEqual(restorer.restoreSession('unknown-token'), false);

    assert.strictEqual(owner.finalizeSession(), true);
    assert.strictEqual(owner.session, null);
    assert.strictEqual(owner.finalizeSession(), false);
  });

  await t.test('session state changes are persisted through auth.saveSession', async () => {
    const saved = [];
    const context = fakeContext();
    context.auth.saveSession = async (token, data) => void saved.push({ token, data });
    const client = new Client(fakeWsTransport(), context);
    const token = randomUUID();
    client.initializeSession(token, {});
    client.session.state.role = 'admin';
    await timers.setImmediate();
    assert.strictEqual(saved.length, 1);
    assert.strictEqual(saved[0].token, token);
    assert.strictEqual(saved[0].data.role, 'admin');
    client.finalizeSession();
  });

  await t.test('destroy terminates open streams and clears the session', async () => {
    const errors = [];
    const context = fakeContext();
    context.console.error = (error) => errors.push(error);
    const client = new Client(fakeWsTransport(), context);
    const token = randomUUID();
    client.initializeSession(token, {});

    let terminated = false;
    client.streams.set('a', { terminate: async () => void (terminated = true) });
    client.streams.set('b', { terminate: async () => Promise.reject(new Error('boom')) });
    client.streams.set('c', { notAStream: true });

    client.destroy();
    await timers.setImmediate();
    await timers.setImmediate();

    assert.strictEqual(terminated, true);
    assert.strictEqual(client.streams.size, 0);
    assert.strictEqual(errors.length, 1);
    assert.match(errors[0].message, /boom/);
    assert.strictEqual(client.restoreSession(token), false);
  });
});

const createApplication = (api) => {
  const introspect = () => api;
  return {
    console: { log: noop, info: noop, warn: noop, error: noop, debug: noop },
    static: { constructor: { name: 'Static' } },
    auth: { saveSession: async () => {} },
    getMethod: (unit, _ver, method) => {
      if (unit === 'system' && method === 'introspect') {
        return { access: 'public', enter: async () => {}, leave: noop, invoke: async () => introspect() };
      }
      const def = api[unit]?.[method];
      if (!def) return null;
      return {
        access: def.access ?? 'public',
        enter: async () => {},
        leave: noop,
        invoke: (context, args) => def.handler(args, context),
      };
    },
  };
};

const createServer = async (api, options = {}) => {
  const server = new Server(createApplication(api), {
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    timeouts: { bind: 50 },
    ...options,
  });
  await server.listen();
  const { port } = server.httpServer.address();
  return { server, port };
};

test('Server internals', async (t) => {
  await t.test('#request: GET-style RPC via /api/unit/method?params', async (t) => {
    const api = { math: { add: { handler: async ({ a, b }) => Number(a) + Number(b) } } };
    const { server, port } = await createServer(api);
    t.after(() => server.close());

    const res = await fetch(`http://127.0.0.1:${port}/api/math/add?a=2&b=3`);
    const body = await res.json();
    assert.strictEqual(body.result, 5);
  });

  await t.test('handleRpc: Error-shaped results are surfaced as protocol errors', async (t) => {
    const api = {
      fail: {
        boom: {
          handler: async () => {
            const error = new Error('domain failure');
            error.code = 422;
            return error;
          },
        },
      },
    };
    const { server, port } = await createServer(api);
    t.after(() => server.close());

    const client = await WrpcClient.connect(`http://127.0.0.1:${port}/api`);
    t.after(() => client.close());
    await client.load('fail');
    await assert.rejects(client.api.fail.boom(), (error) => error.code === 422);
  });

  await t.test('#message: unrecognized packet shape yields a structure error', async (t) => {
    const { server, port } = await createServer({});
    t.after(() => server.close());

    const res = await fetch(`http://127.0.0.1:${port}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nonsense: true }),
    });
    const body = await res.json();
    assert.match(body.error.message, /Packet structure error/);
  });

  await t.test('handleStream: rejects an already-initialized stream id', async (t) => {
    const { server, port } = await createServer({});
    t.after(() => server.close());

    const socket = new WebSocket(`ws://127.0.0.1:${port}`);
    await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));

    const id = randomUUID();
    const initPacket = JSON.stringify({ type: 'stream', id, name: 'dup', size: 3 });
    socket.send(initPacket);
    await timers.setImmediate();
    socket.send(initPacket);

    const raw = await new Promise((resolve) => {
      socket.addEventListener('message', (event) => resolve(event.data), { once: true });
    });
    const response = JSON.parse(raw.toString());
    assert.match(response.error.message, /already initialized/);

    // Close the client side and wait for it to fully finish before this test
    // returns, so the server-side connection is never left dangling for the
    // `server.close()` cleanup registered above to race against.
    await new Promise((resolve) => {
      socket.addEventListener('close', resolve, { once: true });
      socket.close();
    });
  });

  await t.test('handleBinary: chunk for an unknown stream id yields an error', async (t) => {
    const { server, port } = await createServer({});
    t.after(() => server.close());

    const socket = new WebSocket(`ws://127.0.0.1:${port}`);
    await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));

    const chunk = chunkEncode('missing-stream', new Uint8Array([1, 2, 3]));
    socket.send(chunk);

    const raw = await new Promise((resolve) => {
      socket.addEventListener('message', (event) => resolve(event.data), { once: true });
    });
    const response = JSON.parse(raw.toString());
    assert.match(response.error.message, /is not initialized/);

    await new Promise((resolve) => {
      socket.addEventListener('close', resolve, { once: true });
      socket.close();
    });
  });

  await t.test('listen: retries on EADDRINUSE and eventually rejects when exhausted', async (t) => {
    const blocker = await createServer({});
    t.after(() => blocker.server.close());

    const application = createApplication({});
    const server = new Server(application, {
      host: '127.0.0.1',
      port: blocker.port,
      protocol: 'http',
      timeouts: { bind: 20 },
      retry: 2,
    });
    t.after(() => server.close());
    await assert.rejects(server.listen(), (error) => error.code === 'EADDRINUSE');
  });
});
