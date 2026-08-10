'use strict';

// Shared contract suite for server-side engines (the Engine + WrpcSocket
// port). Ф2 runs it against the built-in node engine; Ф3 reuses it for
// the uWebSockets.js adapter. Not a *.test.js — imported by engine tests.

const assert = require('node:assert');
const http = require('node:http');

const { ProtocolClient } = require('../websocket/protocolClient.js');

const openPeer = (port) =>
  new Promise((resolve, reject) => {
    const peer = new ProtocolClient(`ws://127.0.0.1:${port}`);
    peer.on('open', () => resolve(peer));
    peer.on('close', () => reject(new Error('handshake rejected')));
  });

// `t` is a node:test context; `createEngine` returns a fresh Engine.
const runEngineContract = (createEngine, t) => {
  const boot = async () => {
    const engine = createEngine();
    const httpServer = http.createServer();
    const source = engine.attach({ server: httpServer, pingInterval: 5000 });
    await new Promise((resolve) => httpServer.listen(0, resolve));
    const { port } = httpServer.address();
    const teardown = async () => {
      engine.close();
      await new Promise((resolve) => httpServer.close(resolve));
    };
    return { engine, httpServer, source, port, teardown };
  };

  t.test('engine shape: name, capabilities, attach, close', async () => {
    const engine = createEngine();
    assert.strictEqual(typeof engine.name, 'string');
    assert.strictEqual(typeof engine.attach, 'function');
    assert.strictEqual(typeof engine.close, 'function');
    const caps = engine.capabilities;
    for (const key of ['backpressure', 'ping', 'deflate', 'cork', 'pause']) {
      assert.strictEqual(typeof caps[key], 'boolean', `capability ${key}`);
    }
  });

  t.test('connection event delivers a WrpcSocket and the upgrade request', async () => {
    const { source, port, teardown } = await boot();
    const connected = new Promise((resolve) => source.once('connection', (socket, req) => resolve({ socket, req })));
    const peer = await openPeer(port);
    const { socket, req } = await connected;

    assert.strictEqual(typeof socket.send, 'function');
    assert.strictEqual(typeof socket.close, 'function');
    assert.strictEqual(typeof socket.terminate, 'function');
    assert.strictEqual(typeof socket.bufferedAmount, 'number');
    assert.strictEqual(typeof socket.on, 'function');
    assert.ok(req.headers, 'upgrade request with headers');

    peer.close();
    await teardown();
  });

  t.test('echo: text and binary round-trip, send returns a boolean', async () => {
    const { source, port, teardown } = await boot();
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
    await teardown();
  });

  t.test('close(code, reason) reaches the peer', async () => {
    const { source, port, teardown } = await boot();
    source.on('connection', (socket) => socket.close(4001, 'contract bye'));
    // The close frame may arrive in the same TCP segment as the 101
    // response — the listener must be attached before the handshake
    const peer = new ProtocolClient(`ws://127.0.0.1:${port}`);
    const closed = await new Promise((resolve) => {
      peer.on('close', (code, reason) => resolve({ code, reason }));
    });
    assert.strictEqual(closed.code, 4001);
    assert.strictEqual(closed.reason.toString(), 'contract bye');
    await teardown();
  });

  t.test('terminate() drops the peer without a close frame', async () => {
    const { source, port, teardown } = await boot();
    source.on('connection', (socket) => socket.terminate());
    const peer = await openPeer(port);
    await new Promise((resolve) => peer.socket.once('close', resolve));
    await teardown();
  });

  t.test('peer close surfaces as a close event with code and reason', async () => {
    const { source, port, teardown } = await boot();
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
    await teardown();
  });

  t.test('verifyClient rejection blocks the upgrade', async () => {
    const engine = createEngine();
    const httpServer = http.createServer();
    engine.attach({ server: httpServer, verifyClient: () => false });
    await new Promise((resolve) => httpServer.listen(0, resolve));
    const { port } = httpServer.address();
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
    engine.close();
    await new Promise((resolve) => httpServer.close(resolve));
  });
};

module.exports = { runEngineContract };
