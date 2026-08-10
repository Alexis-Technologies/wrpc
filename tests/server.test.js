'use strict';

const timers = require('node:timers/promises');
const { randomUUID } = require('node:crypto');
const { test } = require('node:test');
const assert = require('node:assert');

const { Server, defineRouter, procedure } = require('../index.js');
const { ProtocolClient } = require('./websocket/protocolClient.js');

const parseStatusCode = (statusLine) => {
  if (!statusLine) return null;
  const parts = statusLine.split(' ');
  const code = parseInt(parts[1], 10);
  return Number.isFinite(code) ? code : null;
};

const { emitWarning } = process;
process.emitWarning = (warning, type, ...args) => {
  if (type === 'ExperimentalWarning') return;
  emitWarning(warning, type, ...args);
};

const noop = () => {};
const quietConsole = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

const router = defineRouter({
  test: {
    hello: procedure({
      access: 'public',
      handler: async (_context, { name }) => {
        await timers.setTimeout(10);
        return `Hello, ${name}`;
      },
    }),
  },
});

test('Server / calls', async (t) => {
  const options = {
    router,
    host: 'localhost',
    port: 8003,
    protocol: 'http',
    console: quietConsole,
    timeouts: { bind: 100 },
  };

  let server;

  t.beforeEach(async () => {
    server = new Server(options);
    await server.listen();
  });

  t.afterEach(async () => {
    await server.close();
  });

  await t.test('handles HTTP RPC', async () => {
    const id = randomUUID();
    const args = { name: 'Max' };
    const packet = { type: 'call', id, method: 'test/hello', args };
    const res = await fetch(`http://${options.host}:${options.port}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(packet),
    });
    const response = await res.json();

    assert.strictEqual(response.id, id);
    assert.strictEqual(response.type, 'callback');
    assert.strictEqual(response.result, `Hello, ${args.name}`);
  });

  await t.test('WS RPC handles', async () => {
    const id = randomUUID();
    const args = { name: 'Max' };
    const packet = { type: 'call', id, method: 'test/hello', args };
    const socket = new ProtocolClient(`ws://${options.host}:${options.port}`);
    await new Promise((resolve) => socket.once('open', resolve));
    socket.send(JSON.stringify(packet));
    const resPacket = await new Promise((resolve) => socket.once('message', resolve));
    const response = JSON.parse(resPacket.toString());
    socket.close();
    assert.strictEqual(response.id, id);
    assert.strictEqual(response.type, 'callback');
    assert.strictEqual(response.result, `Hello, ${args.name}`);
  });

  await t.test('WS RPC handles on /api path', async () => {
    const id = randomUUID();
    const args = { name: 'Max' };
    const packet = { type: 'call', id, method: 'test/hello', args };
    const socket = new ProtocolClient(`ws://${options.host}:${options.port}/api`);
    await new Promise((resolve) => socket.once('open', resolve));
    socket.send(JSON.stringify(packet));
    const resPacket = await new Promise((resolve) => socket.once('message', resolve));
    const response = JSON.parse(resPacket.toString());
    socket.close();
    assert.strictEqual(response.id, id);
    assert.strictEqual(response.type, 'callback');
    assert.strictEqual(response.result, `Hello, ${args.name}`);
  });

  await t.test('responds 404 on non-/api HTTP path instead of hanging', async () => {
    const res = await fetch(`http://${options.host}:${options.port}/health`);
    assert.strictEqual(res.status, 404);
    const packet = await res.json();
    assert.strictEqual(packet.type, 'callback');
    assert.strictEqual(packet.error.code, 404);
  });

  await t.test('listen() works without the timeouts option', async () => {
    const extra = new Server({ router, host: 'localhost', port: 0, protocol: 'http', console: quietConsole });
    await extra.listen();
    await extra.close();
  });

  await t.test('listen() retry path works without the timeouts option', async () => {
    // The bind-retry handler used to dereference options.timeouts.bind, so a
    // server without `timeouts` crashed with a TypeError on EADDRINUSE.
    const blocker = new Server({ router, host: 'localhost', port: 0, protocol: 'http', console: quietConsole });
    await blocker.listen();
    const { port } = blocker.httpServer.address();
    const extra = new Server({ router, host: 'localhost', port, protocol: 'http', console: quietConsole });
    const listening = extra.listen();
    await timers.setTimeout(50); // first bind fails with EADDRINUSE, a retry is scheduled
    await blocker.close(); // free the port so the scheduled retry succeeds
    await listening;
    await extra.close();
  });

  await t.test('rejects websocket upgrade on invalid path', async () => {
    // The default verifyClient gate only accepts '/' and the RPC base paths,
    // so the engine aborts the upgrade with 403 Forbidden.
    const res = await ProtocolClient.attemptHandshake({
      host: options.host,
      port: options.port,
      path: '/invalid',
      headers: {
        Upgrade: 'websocket',
        Connection: 'Upgrade',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
      },
      timeoutMs: 600,
    });
    assert.strictEqual(parseStatusCode(res.statusLine), 403);
  });

  await t.test('ws.path relocates the websocket endpoint (regression: default gate 403ed it)', async () => {
    const custom = new Server({
      router,
      host: 'localhost',
      port: 0,
      protocol: 'http',
      console: quietConsole,
      ws: { path: '/socket' },
    });
    await custom.listen();
    const { port } = custom.httpServer.address();

    const socket = new ProtocolClient(`ws://localhost:${port}/socket`);
    const opened = await new Promise((resolve) => {
      socket.on('open', () => resolve(true));
      socket.on('close', () => resolve(false));
    });
    assert.strictEqual(opened, true);

    const packet = { type: 'call', id: 'ws-path', method: 'test/hello', args: { name: 'Path' } };
    socket.send(JSON.stringify(packet));
    const resPacket = await new Promise((resolve) => socket.once('message', resolve));
    assert.strictEqual(JSON.parse(resPacket.toString()).result, 'Hello, Path');
    socket.close();

    // the engine's own path filter still rejects everything else
    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      path: '/api',
      headers: {
        Upgrade: 'websocket',
        Connection: 'Upgrade',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
      },
      timeoutMs: 600,
    });
    assert.strictEqual(parseStatusCode(res.statusLine), 404);
    await custom.close();
  });
});
