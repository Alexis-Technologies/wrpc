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

const { it } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { ProtocolClient } = require('../websocket/protocolClient.js');
const { waitFor } = require('../helpers/wait.js');
const { recorder } = require('../helpers/recorder.js');

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

// Registers the contract's cases in the caller's `describe`; `harness` is
// either a createEngine function (hosted) or a full harness object.
const runEngineContract = (harness, options = {}) => {
  const { createEngine, boot: bootEngine } = typeof harness === 'function' ? hostedHarness(harness) : harness;

  // `t` is the case's context: teardown is registered as an after-hook so a
  // failing assertion cannot leave a listening engine behind and wedge the
  // whole run (a standalone engine holds a native listen socket).
  const boot = async (t, attachOptions) => {
    const engine = createEngine();
    const booted = await bootEngine(engine, attachOptions);
    t.after(() => booted.teardown());
    return { engine, ...booted };
  };

  it('should have the engine shape: name, capabilities, attach, close', options, async () => {
    const engine = createEngine();
    assert.strictEqual(typeof engine.name, 'string');
    assert.strictEqual(typeof engine.attach, 'function');
    assert.strictEqual(typeof engine.close, 'function');
    // A standalone engine owns the network stack and must be able to listen.
    if (engine.standalone) assert.strictEqual(typeof engine.listen, 'function');
    const caps = engine.capabilities;
    for (const key of ['backpressure', 'ping', 'deflate', 'cork', 'pause', 'prepared']) {
      assert.strictEqual(typeof caps[key], 'boolean', `capability ${key}`);
    }
    engine.close(); // an engine that owns native resources must not be leaked
  });

  it('should deliver a WrpcSocket and the upgrade request on the connection event', options, async (t) => {
    const { source, port } = await boot(t);
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

  it('should round-trip text and binary on echo, send returning a boolean', options, async (t) => {
    const { source, port } = await boot(t);
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

  it('should keep message payloads past the callback that delivered them', options, async (t) => {
    // Engines that expose their receive buffer (or a neutered ArrayBuffer,
    // as uws does) must copy before handing the payload to the RPC core,
    // which consumes stream chunks asynchronously.
    const { source, port } = await boot(t);
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

  it('should bring close(code, reason) to the peer', options, async (t) => {
    const { source, port } = await boot(t);
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

  it('should drop the peer without a close frame on terminate()', options, async (t) => {
    const { source, port } = await boot(t);
    source.on('connection', (socket) => socket.terminate());
    // Not openPeer: a fast engine can terminate before the peer finished
    // processing the 101, and waiting for 'open' would race that.
    const peer = new ProtocolClient(`ws://127.0.0.1:${port}`);
    await new Promise((resolve) => peer.socket.once('close', resolve));
  });

  it('should surface a peer close as a close event with code and reason', options, async (t) => {
    const { source, port } = await boot(t);
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

  it('should report no buffer and refuse sends on a closed socket', options, async (t) => {
    const { source, port } = await boot(t);
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

  it('should block the upgrade on a verifyClient rejection', options, async (t) => {
    const { port } = await boot(t, { verifyClient: () => false });
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

  // A promise is truthy: an `async` gate used to admit every peer, whatever
  // it resolved to. Only `true` admits, and the engine says why once.
  it('should refuse the upgrade when verifyClient answers with a promise or a non-true value', options, async (t) => {
    const log = recorder();
    const answers = [async () => true, () => 'yes', () => 1];
    for (let i = 0; i < answers.length; i++) {
      const { port } = await boot(t, { verifyClient: answers[i], logger: log.writer });
      for (let attempt = 0; attempt < 2; attempt++) {
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
        assert.strictEqual(parseInt(res.statusLine.split(' ')[1], 10), 403, `answer ${i}, attempt ${attempt}`);
      }
    }
    assert.strictEqual(
      log.all('ws.gate.async').length,
      1,
      'one ws.gate.async line for the async gate, none for the others',
    );
  });

  it('should echo the selected protocol on subprotocol negotiation', options, async (t) => {
    const { source, port } = await boot(t, { protocols: ['wrpc'] });
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

  it('should hide carrier tokens from handleProtocols and never echo them', options, async (t) => {
    // wrpc.h./wrpc.m./wrpc.bearer. offers are DATA riding the header: every
    // engine drops them from the offer an application selects from, and
    // refuses to reflect one into the response.
    let seen = null;
    const handleProtocols = (offered, req) => {
      seen = offered;
      return req.headers['sec-websocket-protocol'].includes('reflect') ? 'wrpc.bearer.secret' : offered.at(-1);
    };
    const { port } = await boot(t, { handleProtocols });
    const handshake = (offer) =>
      ProtocolClient.attemptHandshake({
        host: '127.0.0.1',
        port,
        path: '/',
        headers: {
          Upgrade: 'websocket',
          Connection: 'Upgrade',
          'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
          'Sec-WebSocket-Protocol': offer,
        },
        timeoutMs: 600,
      });
    const res = await handshake('wrpc.h.eyJhIjoiMSJ9, wrpc.v1, wrpc.bearer.secret');
    assert.strictEqual(parseInt(res.statusLine.split(' ')[1], 10), 101);
    assert.deepStrictEqual(seen, ['wrpc.v1']);
    assert.strictEqual(res.headers['sec-websocket-protocol'], 'wrpc.v1');
    const reflected = await handshake('reflect, wrpc.bearer.secret');
    assert.strictEqual(parseInt(reflected.statusLine.split(' ')[1], 10), 101);
    assert.strictEqual(reflected.headers['sec-websocket-protocol'], undefined);
  });

  it('should select the newest wrpc revision offered with no app configuration', options, async (t) => {
    // A 2.x client offers both revisions and a 1.0 client `wrpc.v1` alone:
    // every engine answers the newest one it was offered, wherever it sits
    // in the list, and nothing when no revision was offered at all.
    const { source, port } = await boot(t);
    // The selection is load-bearing on the socket too: the core reads
    // `socket.protocol` for the revision (a missing one is revision 1).
    const sockets = [];
    source.on('connection', (socket) => sockets.push(socket));
    const selected = async (offer) => {
      const count = sockets.length;
      const res = await ProtocolClient.attemptHandshake({
        host: '127.0.0.1',
        port,
        path: '/',
        headers: {
          Upgrade: 'websocket',
          Connection: 'Upgrade',
          'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
          'Sec-WebSocket-Protocol': offer,
        },
        timeoutMs: 600,
      });
      assert.strictEqual(parseInt(res.statusLine.split(' ')[1], 10), 101, offer);
      await waitFor(() => sockets.length > count, 'the connection event');
      const answer = res.headers['sec-websocket-protocol'];
      assert.strictEqual(sockets.at(-1).protocol || undefined, answer, `${offer}: socket.protocol is the selection`);
      return answer;
    };
    assert.strictEqual(await selected('wrpc.v2, wrpc.v1'), 'wrpc.v2');
    assert.strictEqual(await selected('wrpc.v1, wrpc.v2'), 'wrpc.v2');
    assert.strictEqual(await selected('wrpc.v1'), 'wrpc.v1');
    assert.strictEqual(await selected('wrpc.h.eyJhIjoiMSJ9, wrpc.v2, wrpc.v1, wrpc.bearer.secret'), 'wrpc.v2');
    assert.strictEqual(await selected('chat'), undefined);
  });

  it(
    'should deliver one shared message to two peers like send(text) with sendPrepared (when present)',
    options,
    async (t) => {
      const { engine, source, port } = await boot(t);
      if (!engine.capabilities.prepared) return void t.skip('engine has no prepared-frame path');
      const sockets = [];
      source.on('connection', (socket) => sockets.push(socket));
      const first = await openPeer(port);
      const second = await openPeer(port);
      t.after(() => {
        first.close();
        second.close();
      });
      assert.strictEqual(sockets.length, 2);
      for (const socket of sockets) assert.strictEqual(typeof socket.sendPrepared, 'function');

      const message = { text: 'shared across peers', frames: null, compress: true };
      const received = Promise.all([
        new Promise((resolve) => first.once('message', resolve)),
        new Promise((resolve) => second.once('message', resolve)),
      ]);
      const accepted = sockets.map((socket) => socket.sendPrepared(message));
      for (const ok of accepted) assert.strictEqual(typeof ok, 'boolean');
      assert.notStrictEqual(message.frames, null, 'the engine claims the shared slot');
      const texts = (await received).map(String);
      assert.deepStrictEqual(texts, ['shared across peers', 'shared across peers']);
    },
  );

  it('should refuse new peers on stopListening (when present) while accepted ones keep working', options, async (t) => {
    const probe = createEngine();
    // Optional capability, standalone-shaped: hosted engines have no
    // listener of their own to stop. Skipped, never failed, elsewhere.
    if (typeof probe.stopListening !== 'function') {
      probe.close?.();
      return void t.skip('engine has no stopListening');
    }
    probe.close?.();
    const { engine, source, port } = await boot(t, {});
    const connected = new Promise((resolve) => source.once('connection', resolve));
    const peer = await openPeer(port);
    t.after(() => peer.close());
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
