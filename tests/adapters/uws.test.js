'use strict';

const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');

const { createUwsEngine, UwsSocket, SUCCESS, DROPPED } = require('../../uws.js');
const { isEngine } = require('../../engine.js');
const { runEngineContract, standaloneHarness } = require('../engine/engineContract.js');
const { Server, defineRouter, procedure } = require('../../index.js');
const { ProtocolClient } = require('../websocket/protocolClient.js');
const { requireUws } = require('./boots.js');

const uws = requireUws();

test('createUwsEngine: injection is validated at the boundary', () => {
  assert.throws(() => createUwsEngine(), /pass the uWebSockets\.js module as options\.uws/);
  assert.throws(() => createUwsEngine({}), /pass the uWebSockets\.js module as options\.uws/);
  assert.throws(() => createUwsEngine({ uws: {} }), /pass the uWebSockets\.js module as options\.uws/);
  assert.throws(() => createUwsEngine({ app: {} }), /does not look like a uWebSockets\.js TemplatedApp/);
});

test('createUwsEngine: shape and capabilities', { skip: !uws && 'uWebSockets.js unavailable' }, () => {
  const engine = createUwsEngine({ uws });
  assert.strictEqual(engine.name, 'uws');
  assert.strictEqual(engine.standalone, true);
  assert.strictEqual(isEngine(engine), true);
  // uws owns liveness via idleTimeout, and exposes no socket-level pause
  assert.strictEqual(engine.capabilities.ping, false);
  assert.strictEqual(engine.capabilities.pause, false);
  assert.strictEqual(engine.capabilities.backpressure, true);
  assert.strictEqual(engine.capabilities.deflate, false);
  const compressed = createUwsEngine({ uws, compression: 1 });
  assert.strictEqual(compressed.capabilities.deflate, true);
  // uws.DISABLED is 0, so a falsy compressor must not read as deflate support
  assert.strictEqual(createUwsEngine({ uws, compression: 0 }).capabilities.deflate, false);
  engine.close();
  compressed.close();
});

test('createUwsEngine: attaching twice is refused', { skip: !uws && 'uWebSockets.js unavailable' }, () => {
  const engine = createUwsEngine({ uws });
  engine.attach({});
  assert.throws(() => engine.attach({}), /already attached/);
  engine.close();
});

test('uws engine satisfies the WrpcSocket engine contract', { skip: !uws && 'uWebSockets.js unavailable' }, (t) => {
  runEngineContract(
    standaloneHarness(() => createUwsEngine({ uws })),
    t,
  );
});

test('UwsSocket: a poisoned uws handle never escapes as a throw', () => {
  // uws invalidates the handle in its close callback; every method on it
  // throws afterwards. The WrpcSocket contract has to stay quiet instead.
  const poison = () => {
    throw new Error('Invalid access of closed uWS.WebSocket/SSLWebSocket.');
  };
  const socket = new UwsSocket(
    { send: poison, end: poison, close: poison, getBufferedAmount: poison },
    { remoteAddress: '10.0.0.1', protocol: 'wrpc' },
  );
  assert.strictEqual(socket.remoteAddress, '10.0.0.1');
  assert.strictEqual(socket.protocol, 'wrpc');
  assert.strictEqual(socket.bufferedAmount, 0);
  assert.strictEqual(socket.send('x'), false);
  assert.doesNotThrow(() => socket.close(1000, 'bye'));
  assert.doesNotThrow(() => socket.terminate());
});

test('UwsSocket: a dropped message fails loudly instead of silently', () => {
  // A hole in the frame stream would corrupt the RPC protocol, so a uws
  // DROPPED status has to surface as an error plus a terminate.
  let terminated = false;
  const socket = new UwsSocket(
    {
      send: () => DROPPED,
      close: () => {
        terminated = true;
      },
      end: () => {},
      getBufferedAmount: () => 0,
    },
    {},
  );
  const errors = [];
  socket.on('error', (error) => errors.push(error));
  assert.strictEqual(socket.send('payload'), false);
  assert.strictEqual(errors.length, 1);
  assert.match(errors[0].message, /backpressure limit exceeded/);
  assert.strictEqual(terminated, true);
});

test('UwsSocket: backpressure status maps onto the boolean contract', () => {
  let status = SUCCESS;
  const socket = new UwsSocket({ send: () => status, getBufferedAmount: () => 4096 }, {});
  assert.strictEqual(socket.send('a'), true);
  status = 0; // BACKPRESSURE: buffered, but not lost
  assert.strictEqual(socket.send('b'), false);
  assert.strictEqual(socket.bufferedAmount, 4096);
});

test('UwsSocket: markClosed emits close once and latches', () => {
  const socket = new UwsSocket({ getBufferedAmount: () => 0 }, {});
  const seen = [];
  socket.on('close', (code, reason) => seen.push([code, reason]));
  socket.markClosed(1001, 'going away');
  socket.markClosed(1006, 'again');
  assert.deepStrictEqual(seen, [[1001, 'going away']]);
  assert.strictEqual(socket.closed, true);
});

test(
  'listen: a busy port rejects as EADDRINUSE so the bind-retry works',
  { skip: !uws && 'uWebSockets.js unavailable' },
  async (t) => {
    // uws reports only success/failure, never a reason. Tagging the rejection
    // EADDRINUSE is what keeps Server.listen()'s retry loop working with a
    // standalone engine, so the tag is load-bearing, not cosmetic.
    const blocker = http.createServer();
    await new Promise((resolve) => blocker.listen(0, '127.0.0.1', resolve));
    t.after(() => new Promise((resolve) => blocker.close(resolve)));
    const { port } = blocker.address();

    const engine = createUwsEngine({ uws });
    t.after(() => engine.close());
    engine.attach({});
    await assert.rejects(engine.listen({ host: '127.0.0.1', port }), (error) => {
      assert.strictEqual(error.code, 'EADDRINUSE');
      assert.match(error.message, /Failed to bind 127\.0\.0\.1:/);
      return true;
    });
  },
);

test(
  'Server over uws: listen retries a busy port and then gives up',
  { skip: !uws && 'uWebSockets.js unavailable' },
  async (t) => {
    const blocker = http.createServer();
    await new Promise((resolve) => blocker.listen(0, '127.0.0.1', resolve));
    t.after(() => new Promise((resolve) => blocker.close(resolve)));
    const { port } = blocker.address();

    const server = new Server({
      router: defineRouter({ unit: { noop: procedure({ access: 'public', handler: async () => null }) } }),
      host: '127.0.0.1',
      port,
      logger: false,
      engine: createUwsEngine({ uws }),
      timeouts: { bind: 20 },
      retry: 2,
    });
    t.after(() => server.close());
    await assert.rejects(server.listen(), (error) => error.code === 'EADDRINUSE');
  },
);

test(
  'http: a body over the limit is refused instead of buffered',
  { skip: !uws && 'uWebSockets.js unavailable' },
  async (t) => {
    const server = new Server({
      router: defineRouter({ echo: { args: procedure({ access: 'public', handler: async (_c, args) => args }) } }),
      host: '127.0.0.1',
      port: 0,
      logger: false,
      engine: createUwsEngine({ uws, maxBodySize: 256 }),
    });
    t.after(() => server.close());
    await server.listen();
    const { port } = server.address();

    const small = await fetch(`http://127.0.0.1:${port}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Connection: 'close' },
      body: JSON.stringify({ type: 'call', id: '1', method: 'echo/args', args: { a: 'x' } }),
    });
    assert.strictEqual(small.status, 200);

    const huge = await fetch(`http://127.0.0.1:${port}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Connection: 'close' },
      body: JSON.stringify({ type: 'call', id: '2', method: 'echo/args', args: { a: 'x'.repeat(4096) } }),
    });
    assert.strictEqual(huge.status, 400);
    assert.deepStrictEqual(await huge.json(), {
      type: 'callback',
      id: '',
      error: { message: 'Body size limit exceeded', code: 400 },
    });
  },
);

test('control frames surface as ping and pong events', { skip: !uws && 'uWebSockets.js unavailable' }, async (t) => {
  const engine = createUwsEngine({ uws });
  t.after(() => engine.close());
  const source = engine.attach({});
  const { port } = await engine.listen({ host: '127.0.0.1', port: 0 });

  const connected = new Promise((resolve) => source.once('connection', resolve));
  const peer = new ProtocolClient(`ws://127.0.0.1:${port}`);
  await new Promise((resolve) => peer.on('open', resolve));
  const socket = await connected;

  const pinged = new Promise((resolve) => socket.on('ping', resolve));
  peer.ping(Buffer.from('are you there'));
  assert.strictEqual((await pinged).toString(), 'are you there');

  const ponged = new Promise((resolve) => socket.on('pong', resolve));
  peer.sendFrame(0x0a, Buffer.from('still here'));
  assert.strictEqual((await ponged).toString(), 'still here');

  peer.close();
});

test(
  'codec.rest: a binary REST body round-trips over uws',
  { skip: !uws && 'uWebSockets.js unavailable' },
  async (t) => {
    const codec = {
      rest: {
        contentType: 'application/x-wrpc-bin',
        encode: (value) => Buffer.concat([Buffer.from([0xab]), Buffer.from(JSON.stringify(value ?? null))]),
        decode: (body) => {
          const buffer = Buffer.isBuffer(body) ? body : Buffer.from(body);
          if (buffer[0] !== 0xab) throw new Error('bad frame');
          return JSON.parse(buffer.subarray(1).toString());
        },
      },
    };
    const server = new Server({
      router: defineRouter({
        projects: {
          create: procedure({
            access: 'public',
            http: { method: 'POST', path: '/projects/:orgId', status: 201 },
            handler: async (_ctx, { params, body }) => ({ orgId: params.orgId, name: body?.name }),
          }),
        },
      }),
      host: '127.0.0.1',
      port: 0,
      logger: false,
      codec,
      engine: createUwsEngine({ uws }),
    });
    t.after(() => server.close());
    await server.listen();
    const { port } = server.address();

    const res = await fetch(`http://127.0.0.1:${port}/api/projects/9`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-wrpc-bin', Connection: 'close' },
      body: codec.rest.encode({ name: 'Bin' }),
    });
    assert.strictEqual(res.status, 201);
    assert.strictEqual(res.headers.get('content-type'), 'application/x-wrpc-bin');
    assert.deepStrictEqual(codec.rest.decode(Buffer.from(await res.arrayBuffer())), { orgId: '9', name: 'Bin' });
  },
);
