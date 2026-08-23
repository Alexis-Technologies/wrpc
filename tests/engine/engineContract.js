'use strict';

// Shared contract suite for server-side engines (the Engine + WrpcSocket
// port). F2 ran it against the built-in node engine; F3 reuses it for the
// uWebSockets.js adapter. Not a *.test.js — imported by engine tests.
//
// Engines differ in who owns the listener, so the suite talks to a harness
// rather than to attach() directly:
//
//   harness.createEngine()                -> a fresh Engine
//   harness.boot(engine, attachOptions)   -> { source, port, teardown }
//
// Passing a bare createEngine function keeps the hosted (node http server)
// harness, which is what a hosted engine wants.

const assert = require('node:assert');
const http = require('node:http');

const { ProtocolClient } = require('../websocket/protocolClient.js');

// Hosted engines attach to a node http server the suite owns.
const hostedHarness = (createEngine) => ({
  createEngine,
  async boot(engine, attachOptions = {}) {
    const httpServer = http.createServer();
    const source = engine.attach({ server: httpServer, pingInterval: 5000, ...attachOptions });
    await new Promise((resolve) => httpServer.listen(0, resolve));
    const { port } = httpServer.address();
    const teardown = async () => {
      engine.close();
      await new Promise((resolve) => httpServer.close(resolve));
    };
    return { source, port, teardown };
  },
});

// Standalone engines own listening; the suite only asks for a port.
const standaloneHarness = (createEngine) => ({
  createEngine,
  async boot(engine, attachOptions = {}) {
    const source = engine.attach({ ...attachOptions });
    const { port } = await engine.listen({ host: '127.0.0.1', port: 0 });
    const teardown = async () => {
      engine.close();
    };
    return { source, port, teardown };
  },
});

const openPeer = (port) =>
  new Promise((resolve, reject) => {
    const peer = new ProtocolClient(`ws://127.0.0.1:${port}`);
    peer.on('open', () => resolve(peer));
    peer.on('close', () => reject(new Error('handshake rejected')));
  });

// `t` is a node:test context; `harness` is either a createEngine function
// (hosted) or a full harness object.
const runEngineContract = async (harness, t) => {
  const { createEngine, boot: bootEngine } = typeof harness === 'function' ? hostedHarness(harness) : harness;

  // `sub` is the subtest context: teardown is registered as an after-hook
  // so a failing assertion cannot leave a listening engine behind and wedge
  // the whole run (a standalone engine holds a native listen socket).
  const boot = async (sub, attachOptions) => {
    const engine = createEngine();
    const booted = await bootEngine(engine, attachOptions);
    sub.after(() => booted.teardown());
    return { engine, ...booted };
  };

  await t.test('engine shape: name, capabilities, attach, close', async () => {
    const engine = createEngine();
    assert.strictEqual(typeof engine.name, 'string');
    assert.strictEqual(typeof engine.attach, 'function');
    assert.strictEqual(typeof engine.close, 'function');
    // A standalone engine owns the network stack and must be able to listen.
    if (engine.standalone) assert.strictEqual(typeof engine.listen, 'function');
    const caps = engine.capabilities;
    for (const key of ['backpressure', 'ping', 'deflate', 'cork', 'pause']) {
      assert.strictEqual(typeof caps[key], 'boolean', `capability ${key}`);
    }
    engine.close(); // an engine that owns native resources must not be leaked
  });

  await t.test('connection event delivers a WrpcSocket and the upgrade request', async (sub) => {
    const { source, port } = await boot(sub);
    const connected = new Promise((resolve) => source.once('connection', (socket, req) => resolve({ socket, req })));
    const peer = await openPeer(port);
    const { socket, req } = await connected;

    assert.strictEqual(typeof socket.send, 'function');
    assert.strictEqual(typeof socket.close, 'function');
    assert.strictEqual(typeof socket.terminate, 'function');
    assert.strictEqual(typeof socket.bufferedAmount, 'number');
    assert.strictEqual(typeof socket.on, 'function');
    assert.ok(req.headers, 'upgrade request with headers');
    assert.strictEqual(typeof req.url, 'string', 'upgrade request with a url');

    peer.close();
  });

  await t.test('echo: text and binary round-trip, send returns a boolean', async (sub) => {
    const { source, port } = await boot(sub);
    source.on('connection', (socket) => {
      socket.on('message', (data, isBinary) => {
        const accepted = isBinary ? socket.send(Buffer.from(data)) : socket.send(data.toString());
        assert.strictEqual(typeof accepted, 'boolean');
      });
    });
    const peer = await openPeer(port);

    const textEcho = new Promise((resolve) => peer.once('message', resolve));
    peer.sendText('contract echo');
    assert.strictEqual((await textEcho).toString(), 'contract echo');

    // ProtocolClient surfaces binary data frames via 'frame', not 'message'
    const binaryEcho = new Promise((resolve) => {
      peer.once('frame', (opcode, payload) => resolve({ opcode, payload }));
    });
    peer.sendBinary(Buffer.from([1, 2, 3, 250]));
    const { opcode, payload } = await binaryEcho;
    assert.strictEqual(opcode, 0x02);
    assert.deepStrictEqual(Buffer.from(payload), Buffer.from([1, 2, 3, 250]));

    peer.close();
  });

  await t.test('message payloads survive the callback that delivered them', async (sub) => {
    // Engines that expose their receive buffer (or a neutered ArrayBuffer,
    // as uws does) must copy before handing the payload to the RPC core,
    // which consumes stream chunks asynchronously.
    const { source, port } = await boot(sub);
    const retained = new Promise((resolve) => {
      source.once('connection', (socket) => {
        socket.on('message', (data) => {
          const kept = data;
          setTimeout(() => resolve(Buffer.from(kept)), 20);
        });
      });
    });
    const peer = await openPeer(port);
    peer.sendBinary(Buffer.from([9, 8, 7, 6]));
    assert.deepStrictEqual(await retained, Buffer.from([9, 8, 7, 6]));
    peer.close();
  });

  await t.test('close(code, reason) reaches the peer', async (sub) => {
    const { source, port } = await boot(sub);
    source.on('connection', (socket) => socket.close(4001, 'contract bye'));
    // The close frame may arrive in the same TCP segment as the 101
    // response — the listener must be attached before the handshake
    const peer = new ProtocolClient(`ws://127.0.0.1:${port}`);
    const closed = await new Promise((resolve) => {
      peer.on('close', (code, reason) => resolve({ code, reason }));
    });
    assert.strictEqual(closed.code, 4001);
    assert.strictEqual(closed.reason.toString(), 'contract bye');
  });

  await t.test('terminate() drops the peer without a close frame', async (sub) => {
    const { source, port } = await boot(sub);
    source.on('connection', (socket) => socket.terminate());
    // Not openPeer: a fast engine can terminate before the peer finished
    // processing the 101, and waiting for 'open' would race that.
    const peer = new ProtocolClient(`ws://127.0.0.1:${port}`);
    await new Promise((resolve) => peer.socket.once('close', resolve));
  });

  await t.test('peer close surfaces as a close event with code and reason', async (sub) => {
    const { source, port } = await boot(sub);
    const socketClosed = new Promise((resolve) => {
      source.once('connection', (socket) => {
        socket.on('close', (code, reason) => resolve({ code, reason }));
      });
    });
    const peer = await openPeer(port);
    peer.close(4002, 'peer leaving');
    const { code, reason } = await socketClosed;
    assert.strictEqual(code, 4002);
    assert.strictEqual(String(reason), 'peer leaving');
  });

  await t.test('a closed socket reports no buffer and refuses sends', async (sub) => {
    const { source, port } = await boot(sub);
    const gone = new Promise((resolve) => {
      source.once('connection', (socket) => {
        socket.on('close', () => resolve(socket));
      });
    });
    const peer = await openPeer(port);
    peer.close(4003, 'done');
    const socket = await gone;
    // uws poisons its handle on close; the contract says stay quiet, not throw
    assert.strictEqual(socket.send('after close'), false);
    assert.strictEqual(typeof socket.bufferedAmount, 'number');
    socket.close(1000, 'again');
    socket.terminate();
  });

  await t.test('verifyClient rejection blocks the upgrade', async (sub) => {
    const { port } = await boot(sub, { verifyClient: () => false });
    const res = await ProtocolClient.attemptHandshake({
      host: '127.0.0.1',
      port,
      path: '/',
      headers: {
        Upgrade: 'websocket',
        Connection: 'Upgrade',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
      },
      timeoutMs: 600,
    });
    assert.strictEqual(parseInt(res.statusLine.split(' ')[1], 10), 403);
  });

  await t.test('subprotocol negotiation echoes the selected protocol', async (sub) => {
    const { source, port } = await boot(sub, { protocols: ['wrpc'] });
    const connected = new Promise((resolve) => source.once('connection', resolve));
    const res = await ProtocolClient.attemptHandshake({
      host: '127.0.0.1',
      port,
      path: '/',
      headers: {
        Upgrade: 'websocket',
        Connection: 'Upgrade',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
        'Sec-WebSocket-Protocol': 'nope, wrpc',
      },
      timeoutMs: 600,
    });
    assert.strictEqual(parseInt(res.statusLine.split(' ')[1], 10), 101);
    assert.strictEqual(res.headers['sec-websocket-protocol'], 'wrpc');
    const socket = await connected;
    assert.strictEqual(socket.protocol, 'wrpc');
  });

  await t.test('stopListening (when present) refuses new peers while accepted ones keep working', async (sub) => {
    const probe = createEngine();
    // Optional capability, standalone-shaped: hosted engines have no
    // listener of their own to stop. Skipped, never failed, elsewhere.
    if (typeof probe.stopListening !== 'function') {
      probe.close?.();
      return void sub.skip('engine has no stopListening');
    }
    probe.close?.();
    const { engine, source, port } = await boot(sub, {});
    const connected = new Promise((resolve) => source.once('connection', resolve));
    const peer = await openPeer(port);
    sub.after(() => peer.close());
    const socket = await connected;
    engine.stopListening();
    // The accepted socket still echoes...
    socket.on('message', (data) => socket.send(String(data)));
    const answered = new Promise((resolve) => peer.on('message', resolve));
    peer.sendText('still-alive');
    assert.strictEqual(String(await answered), 'still-alive');
    // ...while a NEW connection is refused at the listener.
    await assert.rejects(openPeer(port), () => true, 'a connect after stopListening must fail');
  });
};

module.exports = { runEngineContract, hostedHarness, standaloneHarness };
