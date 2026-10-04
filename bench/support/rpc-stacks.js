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

// The same RPC path over the uWebSockets.js engine: what separates the cost
// of wrpc's dispatch from the cost of its JavaScript WebSocket engine.
async function startWrpcUws() {
  const { createUwsEngine } = require('../../src/adapters/uws.js');
  const uws = require('uWebSockets.js');
  const { Server } = require('../../src/server.js');
  const { defineRouter, procedure } = require('../../src/rpc/router.js');
  const server = new Server({
    router: defineRouter({ bench: { echo: procedure({ access: 'public', handler: async (_c, args) => args }) } }),
    host: '127.0.0.1',
    port: 0,
    logger: false,
    engine: createUwsEngine({ uws }),
  });
  await server.listen();
  const { port } = server.address();
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

// Client batching on: the 64 in-flight calls of the pipelined row share
// frames instead of each being one.
async function startWrpcBatch() {
  const api = { bench: { echo: { handler: async (args) => args } } };
  const { server, port } = await createWrpcServer(api);
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`, { batch: true });
  await client.load('bench');
  return {
    call: (args) => client.api.bench.echo(args),
    stop: async () => {
      client.close();
      await server.close();
    },
  };
}

// The same RPC path into the same Server, over the two transports with a
// native stack under them (bench/support/real-stacks.js): a WebTransport
// session (libquiche on both ends) and a WebRTC data channel
// (libdatachannel on both peers, a loopback pair negotiated in-process).
// Skipped, with the reason, unless WRPC_WT=fails / WRPC_RTC=node-datachannel.
async function startWrpcOver(kind) {
  const real = require('./real-stacks.js');
  const api = { bench: { echo: { handler: async (args) => args } } };
  const { server, port } = await createWrpcServer(api);
  const wt = kind === 'wt' ? await real.bootWt(server) : null;
  const connection = await real.connectOver(kind, server, { wt, port });
  await connection.client.load('bench');
  return {
    call: (args) => connection.client.api.bench.echo(args),
    stop: async () => {
      connection.close();
      await wt?.stop();
      await server.close();
      if (kind === 'webrtc') real.rtcCleanup();
    },
  };
}

const realSkip = (name) => () => require('./real-stacks.js')[name]();

// Raw echoes over the same two native stacks, no RPC layer — what separates
// wrpc's cost from the stack's own, as the raw ws/uws rows do for the
// WebSocket. The envelope and the correlation by id are the createEchoClient
// ones. A WebTransport stream is bytes, not messages, so the echo frames
// each message with a u32 length on one bidirectional stream; a data
// channel already carries messages.
const lengthFramed = (text) => {
  const body = Buffer.from(text);
  const out = Buffer.allocUnsafe(4 + body.length);
  out.writeUInt32BE(body.length, 0);
  body.copy(out, 4);
  return out;
};

const readFramed = async (readable, onMessage) => {
  const reader = readable.getReader();
  let held = Buffer.alloc(0);
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return;
    held = held.length === 0 ? Buffer.from(value) : Buffer.concat([held, value]);
    while (held.length >= 4 && held.length >= 4 + held.readUInt32BE(0)) {
      const length = held.readUInt32BE(0);
      onMessage(held.subarray(4, 4 + length).toString());
      held = held.subarray(4 + length);
    }
  }
};

const correlate = (send) => {
  const pending = new Map();
  const onMessage = (text) => {
    const { id, args } = JSON.parse(text);
    const resolve = pending.get(id);
    if (!resolve) return;
    pending.delete(id);
    resolve(args);
  };
  const call = (args) =>
    new Promise((resolve) => {
      const id = randomUUID();
      pending.set(id, resolve);
      send(JSON.stringify({ type: 'call', id, method: 'bench/echo', args }));
    });
  return { onMessage, call };
};

async function startWtRaw() {
  const { bootH3 } = require('./real-stacks.js');
  const { h3, port, WebTransport, serverCertificateHashes } = await bootH3();
  const serve = async (session) => {
    await session.ready;
    const streams = session.incomingBidirectionalStreams.getReader();
    for (;;) {
      const { value: stream, done } = await streams.read();
      if (done) return;
      const writer = stream.writable.getWriter();
      readFramed(stream.readable, (text) => {
        const { id, args } = JSON.parse(text);
        writer.write(lengthFramed(JSON.stringify({ id, args })));
      }).catch(() => {});
    }
  };
  const sessions = h3.sessionStream('/echo').getReader();
  (async () => {
    for (;;) {
      const { value: session, done } = await sessions.read();
      if (done) return;
      serve(session).catch(() => {});
    }
  })().catch(() => {});
  const transport = new WebTransport(`https://127.0.0.1:${port}/echo`, { serverCertificateHashes });
  await transport.ready;
  const stream = await transport.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  const { onMessage, call } = correlate((text) => writer.write(lengthFramed(text)));
  readFramed(stream.readable, onMessage).catch(() => {});
  return {
    call,
    stop: async () => {
      transport.close();
      await h3.stopServer();
    },
  };
}

async function startRtcRaw() {
  const real = require('./real-stacks.js');
  const pair = await real.rtcPair();
  pair.host.addEventListener('message', ({ data }) => {
    const { id, args } = JSON.parse(data);
    pair.host.send(JSON.stringify({ id, args }));
  });
  const { onMessage, call } = correlate((text) => pair.client.send(text));
  pair.client.addEventListener('message', ({ data }) => onMessage(data));
  return {
    call,
    stop: async () => {
      pair.close();
      real.rtcCleanup();
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

// Socket.io: a real RPC framework rather than a raw transport, so it pays for
// its own envelope, its acknowledgement bookkeeping and its engine.io layer —
// the same kinds of cost wrpc pays. `emitWithAck` is the request/response
// shape closest to a call.
async function startSocketIo() {
  const { Server } = require('socket.io');
  const { io } = require('socket.io-client');
  const httpServer = http.createServer();
  const server = new Server(httpServer, { serveClient: false });
  server.on('connection', (socket) => {
    socket.on('echo', (args, ack) => ack(args));
  });
  await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
  const { port } = httpServer.address();
  // websocket only: the default starts on HTTP long-polling and upgrades,
  // which would measure the upgrade rather than the steady state.
  const client = io(`http://127.0.0.1:${port}`, { transports: ['websocket'] });
  await new Promise((resolve, reject) => {
    client.once('connect', resolve);
    client.once('connect_error', reject);
  });
  return {
    call: (args) => client.emitWithAck('echo', args),
    stop: async () => {
      client.close();
      await server.close();
      await new Promise((resolve) => httpServer.close(resolve));
    },
  };
}

// tRPC over WebSocket (httpSubscriptionLink's sibling, wsLink) — the closest
// comparison to wrpc's own positioning. The function parser keeps the input
// validator free, so the number is transport plus tRPC's own envelope rather
// than someone's schema library.
async function startTrpcWs() {
  const { initTRPC } = require('@trpc/server');
  const { applyWSSHandler } = require('@trpc/server/adapters/ws');
  const { createTRPCClient, createWSClient, wsLink } = require('@trpc/client');

  const t = initTRPC.create();
  const appRouter = t.router({
    echo: t.procedure.input((value) => value).query(({ input }) => input),
  });

  const httpServer = http.createServer();
  const wss = new WebSocket.Server({ server: httpServer });
  const handler = applyWSSHandler({ wss, router: appRouter });
  await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
  const { port } = httpServer.address();

  const wsClient = createWSClient({ url: `ws://127.0.0.1:${port}` });
  const client = createTRPCClient({ links: [wsLink({ client: wsClient })] });
  return {
    call: (args) => client.echo.query(args),
    stop: async () => {
      wsClient.close();
      handler.broadcastReconnectNotification();
      wss.close();
      await new Promise((resolve) => httpServer.close(resolve));
    },
  };
}

// gRPC — a unary echo over HTTP/2 through @grpc/grpc-js, the service loaded
// from bench/support/echo.proto at runtime by @grpc/proto-loader (no
// codegen). Protobuf on the wire, so its envelope is cheaper to encode than
// the JSON every other row pays; that is part of what gRPC is.
async function startGrpc() {
  const path = require('node:path');
  const grpc = require('@grpc/grpc-js');
  const loader = require('@grpc/proto-loader');
  const definition = loader.loadSync(path.join(__dirname, 'echo.proto'), { keepCase: true });
  const { bench } = grpc.loadPackageDefinition(definition);
  const server = new grpc.Server();
  server.addService(bench.Bench.service, { Echo: (call, callback) => callback(null, call.request) });
  const port = await new Promise((resolve, reject) =>
    server.bindAsync('127.0.0.1:0', grpc.ServerCredentials.createInsecure(), (error, bound) =>
      error ? reject(error) : resolve(bound),
    ),
  );
  const client = new bench.Bench(`127.0.0.1:${port}`, grpc.credentials.createInsecure());
  await new Promise((resolve, reject) =>
    client.waitForReady(Date.now() + 5000, (error) => (error ? reject(error) : resolve())),
  );
  return {
    call: (args) =>
      new Promise((resolve, reject) => client.Echo(args, (error, reply) => (error ? reject(error) : resolve(reply)))),
    stop: async () => {
      client.close();
      await new Promise((resolve) => server.tryShutdown(resolve));
    },
  };
}

module.exports = {
  wrpc: { label: 'wrpc (own WS + RPC dispatch)', start: startWrpc },
  'wrpc-uws': { label: 'wrpc (uws engine + RPC dispatch)', start: startWrpcUws },
  'wrpc-batch': { label: 'wrpc (own WS, batch: true)', start: startWrpcBatch },
  'wrpc-wt': {
    label: 'wrpc (WebTransport, libquiche)',
    start: () => startWrpcOver('wt'),
    skip: realSkip('wtSkip'),
  },
  'wrpc-webrtc': {
    label: 'wrpc (WebRTC data channel, libdatachannel)',
    start: () => startWrpcOver('webrtc'),
    skip: realSkip('rtcSkip'),
  },
  ws: { label: 'ws (raw echo RPC)', start: startWs },
  uws: { label: 'uWebSockets.js (raw echo RPC)', start: startUws },
  'fastify-websocket': { label: '@fastify/websocket (raw echo RPC)', start: startFastifyWebsocket },
  'fastify-uws': { label: 'fastify-uws (raw echo RPC)', start: startFastifyUws },
  'wt-raw': { label: 'WebTransport (raw echo RPC, libquiche)', start: startWtRaw, skip: realSkip('wtSkip') },
  'webrtc-raw': {
    label: 'WebRTC data channel (raw echo RPC, libdatachannel)',
    start: startRtcRaw,
    skip: realSkip('rtcSkip'),
  },
  'socket.io': { label: 'socket.io (framework RPC via emitWithAck)', start: startSocketIo },
  'trpc-ws': { label: 'tRPC (framework RPC over wsLink)', start: startTrpcWs },
  grpc: { label: 'gRPC (@grpc/grpc-js unary over HTTP/2)', start: startGrpc },
};
