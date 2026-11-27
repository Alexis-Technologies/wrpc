'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { createUwsEngine, UwsSocket, SUCCESS, DROPPED } = require('../../src/adapters/uws.js');
const { isEngine } = require('../../src/engine/index.js');
const { runEngineContract, standaloneHarness } = require('../engine/engine-contract.js');
const { Server, defineRouter, procedure } = require('../../index.js');
const { ProtocolClient } = require('../websocket/protocol-client.js');
const { requireUws } = require('./boots.js');

const uws = requireUws();

describe('createUwsEngine', () => {
  it('should validate the injection at the boundary', () => {
    assert.throws(() => createUwsEngine(), /pass the uWebSockets\.js module as options\.uws/);
    assert.throws(() => createUwsEngine({}), /pass the uWebSockets\.js module as options\.uws/);
    assert.throws(() => createUwsEngine({ uws: {} }), /pass the uWebSockets\.js module as options\.uws/);
    assert.throws(() => createUwsEngine({ app: {} }), /does not look like a uWebSockets\.js TemplatedApp/);
  });

  it('should have the expected shape and capabilities', { skip: !uws && 'uWebSockets.js unavailable' }, (t) => {
    // Each engine is closed by t.after the moment it exists: a closing line
    // at the end never ran when an assertion failed, and a native engine left
    // open wedges the whole run (tests/CLAUDE.md).
    const engine = createUwsEngine({ uws });
    t.after(() => engine.close());
    assert.strictEqual(engine.name, 'uws');
    assert.strictEqual(engine.standalone, true);
    assert.strictEqual(isEngine(engine), true);
    // uws owns liveness via idleTimeout, and exposes no socket-level pause
    assert.strictEqual(engine.capabilities.ping, false);
    assert.strictEqual(engine.capabilities.pause, false);
    assert.strictEqual(engine.capabilities.backpressure, true);
    assert.strictEqual(engine.capabilities.deflate, false);
    const compressed = createUwsEngine({ uws, compression: 1 });
    t.after(() => compressed.close());
    assert.strictEqual(compressed.capabilities.deflate, true);
    // uws.DISABLED is 0, so a falsy compressor must not read as deflate support
    const plain = createUwsEngine({ uws, compression: 0 });
    t.after(() => plain.close());
    assert.strictEqual(plain.capabilities.deflate, false);
  });

  it('should refuse attaching twice', { skip: !uws && 'uWebSockets.js unavailable' }, (t) => {
    const engine = createUwsEngine({ uws });
    t.after(() => engine.close());
    engine.attach({});
    assert.throws(
      () => engine.attach({}),
      (error) => error.code === 'ALX_ERR_CONFIG' && /already attached/.test(error.message),
    );
  });

  it(
    'should report through the logger attach() hands it when built without one, and stay silent with logger: false',
    { skip: !uws && 'uWebSockets.js unavailable' },
    async (t) => {
      const { recorder } = require('../helpers/recorder.js');
      const { WrpcClient } = require('../../index.js');
      const router = defineRouter({ unit: { noop: procedure({ access: 'public', handler: async () => null }) } });
      const probe = async (engine, expected) => {
        const log = recorder();
        const server = new Server({ router, host: '127.0.0.1', port: 0, logger: log.writer, engine });
        t.after(() => server.close());
        const sockets = [];
        server.wsServer.on('connection', (socket) => sockets.push(socket));
        await server.listen();
        const { port } = server.address();
        const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false, reconnect: false });
        t.after(() => client.close());
        assert.strictEqual(sockets.length, 1);
        // What a dropped frame would write, through the socket's own writer.
        sockets[0].log.error({ event: 'uws.probe' });
        assert.deepStrictEqual(
          log.all('uws.probe').map((e) => e.component),
          expected,
        );
      };
      await probe(createUwsEngine({ uws }), ['uws']);
      await probe(createUwsEngine({ uws, logger: false }), []);
    },
  );

  it(
    'should reject a busy port on listen as EADDRINUSE so the bind-retry works',
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
});

describe('uws engine', () => {
  describe('the WrpcSocket engine contract', () => {
    runEngineContract(
      standaloneHarness(() => createUwsEngine({ uws })),
      { skip: !uws && 'uWebSockets.js unavailable' },
    );
  });

  it(
    'should surface control frames as ping and pong events',
    { skip: !uws && 'uWebSockets.js unavailable' },
    async (t) => {
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
    },
  );

  it(
    'should actually compress outbound frames with a configured compressor',
    { skip: !uws && 'uWebSockets.js unavailable' },
    async (t) => {
      const { decompress } = require('../../src/websocket/permessage-deflate.js');
      const big = 'compress me '.repeat(512);
      const server = new Server({
        router: defineRouter({ echo: { big: procedure({ access: 'public', handler: async () => big }) } }),
        host: '127.0.0.1',
        port: 0,
        logger: false,
        engine: createUwsEngine({ uws, compression: uws.SHARED_COMPRESSOR }),
      });
      t.after(() => server.close());
      await server.listen();
      const { port } = server.address();
      const peer = new ProtocolClient(`ws://127.0.0.1:${port}`, {
        headers: { 'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits' },
      });
      t.after(() => peer.close());
      await new Promise((resolve, reject) => {
        peer.on('open', resolve);
        peer.on('close', () => reject(new Error('handshake rejected')));
      });
      assert.match(peer.handshakeHeaders['sec-websocket-extensions'] ?? '', /permessage-deflate/);
      const answer = new Promise((resolve) =>
        peer.once('frame', (opcode, payload, meta) => resolve({ payload, meta })),
      );
      peer.sendText(JSON.stringify({ type: 'call', id: '1', method: 'echo/big', args: {} }));
      const { payload, meta } = await answer;
      assert.strictEqual(meta.rsv & 0x40, 0x40, 'RSV1 set: the frame went out compressed');
      const packet = JSON.parse(decompress(payload, 1 << 20).toString());
      assert.strictEqual(packet.result, big);
    },
  );
});

describe('UwsSocket', () => {
  it('should never let a poisoned uws handle escape as a throw', () => {
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

  it('should fail a dropped message loudly instead of silently', () => {
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

  it('should map backpressure status onto the boolean contract', () => {
    let status = SUCCESS;
    const socket = new UwsSocket({ send: () => status, getBufferedAmount: () => 4096 }, {});
    assert.strictEqual(socket.send('a'), true);
    status = 0; // BACKPRESSURE: buffered, but not lost
    assert.strictEqual(socket.send('b'), false);
    assert.strictEqual(socket.bufferedAmount, 4096);
  });

  it('should emit close once on markClosed and latch', () => {
    const socket = new UwsSocket({ getBufferedAmount: () => 0 }, {});
    const seen = [];
    socket.on('close', (code, reason) => seen.push([code, reason]));
    socket.markClosed(1001, 'going away');
    socket.markClosed(1006, 'again');
    assert.deepStrictEqual(seen, [[1001, 'going away']]);
    assert.strictEqual(socket.closed, true);
  });
});

describe('Server over uws', () => {
  it(
    'should retry a busy port on listen and then give up',
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

  it(
    'should refuse a body over the limit instead of buffering it (http)',
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

  it(
    'should round-trip a binary REST body over uws (codec.rest)',
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
});
