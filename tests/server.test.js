'use strict';

const timers = require('node:timers/promises');
const { randomUUID } = require('node:crypto');
const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');

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

describe('Server / calls', () => {
  const options = {
    router,
    host: 'localhost',
    port: 8003,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 100 },
  };

  let server;

  beforeEach(async () => {
    server = new Server(options);
    await server.listen();
  });

  afterEach(async () => {
    await server.close();
  });

  it('should handle HTTP RPC', async () => {
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

  it('should handle WS RPC', async () => {
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

  it('should handle WS RPC on the /api path', async () => {
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

  it('should respond 404 on a non-/api HTTP path instead of hanging', async () => {
    const res = await fetch(`http://${options.host}:${options.port}/health`);
    assert.strictEqual(res.status, 404);
    const packet = await res.json();
    assert.strictEqual(packet.type, 'callback');
    assert.strictEqual(packet.error.code, 404);
  });

  it('should listen() without the timeouts option', async () => {
    const extra = new Server({ router, host: 'localhost', port: 0, protocol: 'http', logger: false });
    await extra.listen();
    await extra.close();
  });

  it('should take the listen() retry path without the timeouts option', async () => {
    // The bind-retry handler used to dereference options.timeouts.bind, so a
    // server without `timeouts` crashed with a TypeError on EADDRINUSE.
    const blocker = new Server({ router, host: 'localhost', port: 0, protocol: 'http', logger: false });
    await blocker.listen();
    const { port } = blocker.address();
    const extra = new Server({ router, host: 'localhost', port, protocol: 'http', logger: false });
    const listening = extra.listen();
    await timers.setTimeout(50); // first bind fails with EADDRINUSE, a retry is scheduled
    await blocker.close(); // free the port so the scheduled retry succeeds
    await listening;
    await extra.close();
  });

  it('should reject a websocket upgrade on an invalid path', async () => {
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

  it('should relocate the websocket endpoint with ws.path (regression: default gate 403ed it)', async () => {
    const custom = new Server({
      router,
      host: 'localhost',
      port: 0,
      protocol: 'http',
      logger: false,
      ws: { path: '/socket' },
    });
    await custom.listen();
    const { port } = custom.address();

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

describe('Server / the core, forwarded', () => {
  it("should forward rooms, clients, to/except/broadcast, cluster, getClient and sendTo to the core's own", async (t) => {
    const { bootServer, connectClient, waitFor } = require('./helpers/server.js');
    const { defineRouter: define, procedure: proc } = require('../index.js');
    const shellRouter = define({
      chat: {
        whoami: proc({ access: 'public', handler: async (context) => ({ id: context.client.id }) }),
        join: proc({ access: 'public', handler: async (context) => void context.client.join('lobby') }),
        emits: { note: { data: 'string' } },
      },
    });
    const { server, url } = await bootServer(t, { router: shellRouter });
    const first = await connectClient(t, url);
    const second = await connectClient(t, url);
    await first.load('chat');
    await second.load('chat');
    const { id } = await first.api.chat.whoami();
    await first.api.chat.join();
    const heard = { first: [], second: [] };
    first.api.chat.on('note', (data) => heard.first.push(data));
    second.api.chat.on('note', (data) => heard.second.push(data));

    assert.strictEqual(server.rooms, server.rpc.rooms);
    assert.strictEqual(server.cluster, server.rpc.cluster);
    // `clients` is a snapshot of the core's set each time it is read.
    assert.strictEqual(server.clients.size, 2);
    assert.deepStrictEqual(Array.from(server.clients), Array.from(server.rpc.clients));
    const local = server.getClient(id);
    assert.strictEqual(local.id, id);
    assert.strictEqual(server.getClient('nobody'), undefined);

    server.to('lobby').emit('chat/note', 'to the room');
    server.except(local).emit('chat/note', 'to everyone else');
    server.broadcast('chat/note', 'to all');
    assert.strictEqual(server.sendTo(id, 'chat/note', 'to one'), true);
    assert.strictEqual(server.sendTo(id, 'chat/note', 'not in that room', { room: 'elsewhere' }), false);
    await waitFor(() => heard.first.length === 3 && heard.second.length === 2, 'every event arrived');
    assert.deepStrictEqual(heard.first, ['to the room', 'to all', 'to one']);
    assert.deepStrictEqual(heard.second, ['to everyone else', 'to all']);
  });
});
