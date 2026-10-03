'use strict';

/**
 * The servers bench/http-comparison.js loads with autocannon: one JSON echo
 * over plain HTTP/1.1 per stack, each answering `POST` with the body it was
 * given — the shape of fastify/benchmarks' hello-world, with a body because
 * an RPC call has arguments. `start()` resolves with what autocannon needs:
 * the port, the path, and the exact request body (some stacks wrap the
 * arguments in an envelope of their own).
 *
 * Every framework here is a devDependency used only by the benchmarks;
 * wrpc itself stays zero-dependency (see CLAUDE.md).
 */

const http = require('node:http');

const ARGS = { name: 'Ada' };

const listen = (server) =>
  new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve(server.address().port)));

// wrpc's REST bridge: a procedure that declares `http: { method, path }` is
// a real endpoint — the body arrives as `body`, the answer is the plain
// result, no envelope.
async function startWrpcRest() {
  const { Server } = require('../../src/server.js');
  const { defineRouter, procedure } = require('../../src/rpc/router.js');
  const router = defineRouter({
    bench: {
      echo: procedure({
        access: 'public',
        http: { method: 'POST', path: '/echo' },
        handler: async (_context, { body }) => body,
      }),
    },
  });
  const server = new Server({ router, host: '127.0.0.1', port: 0, protocol: 'http', logger: false });
  await server.listen();
  return { port: server.address().port, path: '/api/echo', body: ARGS, stop: () => server.close() };
}

// wrpc's packet mode: the call packet a WebSocket would carry, as the body of
// one POST; the answer is its callback packet.
async function startWrpcPacket() {
  const { createWrpcServer } = require('./wrpc-echo.js');
  const { server, port } = await createWrpcServer({ bench: { echo: { handler: async (args) => args } } });
  const packet = { type: 'call', id: '1', method: 'bench/echo', args: ARGS };
  return { port, path: '/api', body: packet, stop: () => server.close() };
}

async function startFastify() {
  const fastify = require('fastify')({ logger: false });
  fastify.post('/echo', async (request) => request.body);
  await fastify.listen({ port: 0, host: '127.0.0.1' });
  return { port: fastify.server.address().port, path: '/echo', body: ARGS, stop: () => fastify.close() };
}

async function startExpress() {
  const express = require('express');
  const app = express();
  app.use(express.json());
  app.post('/echo', (request, response) => response.json(request.body));
  const server = http.createServer(app);
  const port = await listen(server);
  return { port, path: '/echo', body: ARGS, stop: () => new Promise((resolve) => server.close(resolve)) };
}

// tRPC v11's standalone HTTP adapter: a mutation is a POST to its path, the
// input is the body, the answer is `{ result: { data } }`.
async function startTrpc() {
  const { initTRPC } = require('@trpc/server');
  const { createHTTPServer } = require('@trpc/server/adapters/standalone');
  const t = initTRPC.create();
  const appRouter = t.router({ echo: t.procedure.input((value) => value).mutation(({ input }) => input) });
  const server = createHTTPServer({ router: appRouter });
  const port = await listen(server);
  return { port, path: '/echo', body: ARGS, stop: () => new Promise((resolve) => server.close(resolve)) };
}

// No framework: node:http reading the body, parsing it and answering it —
// the ceiling, as the raw echoes are in bench/rpc-comparison.js.
async function startNodeHttp() {
  const server = http.createServer((request, response) => {
    const chunks = [];
    request.on('data', (chunk) => chunks.push(chunk));
    request.on('end', () => {
      const body = JSON.stringify(JSON.parse(Buffer.concat(chunks).toString()));
      response.writeHead(200, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) });
      response.end(body);
    });
  });
  const port = await listen(server);
  return { port, path: '/echo', body: ARGS, stop: () => new Promise((resolve) => server.close(resolve)) };
}

module.exports = {
  'wrpc-rest': { label: 'wrpc (declared REST route)', start: startWrpcRest },
  'wrpc-packet': { label: 'wrpc (packet mode)', start: startWrpcPacket },
  fastify: { label: 'fastify', start: startFastify },
  express: { label: 'express', start: startExpress },
  trpc: { label: 'tRPC (standalone HTTP adapter)', start: startTrpc },
  'node-http': { label: 'node:http (no framework)', start: startNodeHttp },
};
