'use strict';

/**
 * One entry per compared stack: `label` for the report, `start()` returns
 * a `{ call, stop }` handle wired to a minimal "echo RPC" server — same
 * {id, method, args} envelope wrpc's own client scaffold uses (see
 * src/client.js #scaffold), so every stack pays the same per-call
 * JSON/UUID overhead and only the transport differs.
 *
 * Native modules (uWebSockets.js, and fastify-uws which bundles its own
 * copy of it) are required lazily inside their start() function: each
 * comparison stack runs in its own child process (see rpc-comparison.js),
 * and loading two different builds of the same native addon in one
 * process segfaults on exit.
 */
const http = require('node:http');
const { randomUUID } = require('node:crypto');

const WebSocket = require('ws');
const fastify = require('fastify');
const fastifyWebsocket = require('@fastify/websocket');

const { WrpcClient } = require('../../src/client.js');
const { createWrpcServer } = require('./wrpc-echo.js');

function createEchoClient(url) {
  const socket = new WebSocket(url);
  const pending = new Map();

  socket.on('message', (data) => {
    const { id, args } = JSON.parse(data);
    const resolve = pending.get(id);
    if (!resolve) return;
    pending.delete(id);
    resolve(args);
  });

  const opened = new Promise((resolve, reject) => {
    socket.once('open', resolve);
    socket.once('error', reject);
  });

  const call = (args) =>
    new Promise((resolve) => {
      const id = randomUUID();
      pending.set(id, resolve);
      socket.send(JSON.stringify({ type: 'call', id, method: 'bench/echo', args }));
    });

  return { opened, call, close: () => socket.close() };
}

async function startWrpc() {
  const api = { bench: { echo: { handler: async (args) => args } } };
  const { server, port } = await createWrpcServer(api);
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
  await client.load('bench');
  return {
    call: (args) => client.api.bench.echo(args),
    stop: async () => {
      client.close();
      await server.close();
    },
  };
}

async function startWs() {
  const httpServer = http.createServer();
  const wss = new WebSocket.Server({ server: httpServer });
  wss.on('connection', (socket) => {
    socket.on('message', (data) => {
      const { id, args } = JSON.parse(data);
      socket.send(JSON.stringify({ id, args }));
    });
  });
  await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
  const { port } = httpServer.address();
  const client = createEchoClient(`ws://127.0.0.1:${port}/`);
  await client.opened;
  return {
    call: client.call,
    stop: async () => {
      client.close();
      await new Promise((resolve) => httpServer.close(resolve));
    },
  };
}

async function startUws() {
  const uws = require('uWebSockets.js');
  const app = uws.App().ws('/*', {
    message: (socket, message) => {
      const { id, args } = JSON.parse(Buffer.from(message).toString('utf8'));
      socket.send(JSON.stringify({ id, args }));
    },
  });
  const token = await new Promise((resolve, reject) => {
    app.listen('127.0.0.1', 0, (listenToken) => {
      if (!listenToken) return reject(new Error('uWebSockets.js failed to listen'));
      resolve(listenToken);
    });
  });
  const port = uws.us_socket_local_port(token);
  const client = createEchoClient(`ws://127.0.0.1:${port}/`);
  await client.opened;
  return {
    call: client.call,
    stop: async () => {
      client.close();
      uws.us_listen_socket_close(token);
    },
  };
}

async function startFastifyWebsocket() {
  const app = fastify({ logger: false });
  await app.register(fastifyWebsocket);
  app.get('/', { websocket: true }, (socket) => {
    socket.on('message', (message) => {
      const { id, args } = JSON.parse(message.toString());
      socket.send(JSON.stringify({ id, args }));
    });
  });
  await app.listen({ host: '127.0.0.1', port: 0 });
  const { port } = app.server.address();
  const client = createEchoClient(`ws://127.0.0.1:${port}/`);
  await client.opened;
  return {
    call: client.call,
    stop: async () => {
      client.close();
      await app.close();
    },
  };
}

async function startFastifyUws() {
  const { serverFactory, websocket } = require('fastify-uws');
  const app = fastify({ logger: false, serverFactory });
  await app.register(websocket);
  app.get('/', { websocket: true }, (socket) => {
    socket.on('message', (message) => {
      const { id, args } = JSON.parse(message.toString());
      socket.send(JSON.stringify({ id, args }));
    });
  });
  await app.listen({ host: '127.0.0.1', port: 0 });
  const { port } = app.server.address();
  const client = createEchoClient(`ws://127.0.0.1:${port}/`);
  await client.opened;
  return {
    call: client.call,
    stop: async () => {
      client.close();
      await app.close();
    },
  };
}

module.exports = {
  wrpc: { label: 'wrpc (own WS + RPC dispatch)', start: startWrpc },
  ws: { label: 'ws (raw echo RPC)', start: startWs },
  uws: { label: 'uWebSockets.js (raw echo RPC)', start: startUws },
  'fastify-websocket': { label: '@fastify/websocket (raw echo RPC)', start: startFastifyWebsocket },
  'fastify-uws': { label: 'fastify-uws (raw echo RPC)', start: startFastifyUws },
};
