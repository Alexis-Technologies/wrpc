'use strict';

// Boot functions for the adapter swap-test: one behavioral spec
// (tests/adapters/spec.js) runs against every way of standing wrpc up.
// Not a *.test.js — imported by the adapter tests.

const http = require('node:http');
const path = require('node:path');
const Module = require('node:module');

const { Server } = require('../../index.js');
const { createNodeEngine } = require('../../engine.js');
const { createUwsEngine } = require('../../uws.js');
const { wrpcFastify } = require('../../fastify.js');
const { createWrpc } = require('../../express.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

// Optional peer dependencies: present in devDependencies, but a machine
// without a working uws binary should skip rather than fail.
const optional = (name) => {
  try {
    return require(name);
  } catch {
    return null;
  }
};

// pnpm gives fastify-uws its own uWebSockets.js build (2.0.0 pins 20.57.0,
// this package's devDependencies pin 20.69.0), so a process that touches both
// loads the SAME native addon twice. Two copies cannot coexist: creating and
// closing an App leaves the process to segfault in ~uWS::TemplatedApp during
// node's environment cleanup — after the last assertion has already passed,
// which turns a green run into an exit-code-139 failure.
//
// Seed the module cache so fastify-uws resolves uWebSockets.js to the copy we
// already loaded. One binary, no exit-time crash, and the fastify+uws boot
// exercises the same engine build as every other uws boot.
const pinNestedUws = (uws) => {
  let nested = null;
  try {
    nested = Module.createRequire(require.resolve('fastify-uws')).resolve('uWebSockets.js');
  } catch {
    return; // no fastify-uws, or it brings no uws of its own: nothing to pin
  }
  if (nested === require.resolve('uWebSockets.js')) return; // already deduped
  if (require.cache[nested]) return; // already loaded — too late to help
  const stub = new Module(nested, null);
  stub.filename = nested;
  stub.path = path.dirname(nested);
  stub.loaded = true;
  stub.exports = uws;
  require.cache[nested] = stub;
};

const requireUws = () => {
  const uws = optional('uWebSockets.js');
  if (uws) pinNestedUws(uws);
  return uws;
};

const rpcOptions = (options) => ({
  router: options.router,
  sessions: options.sessions,
  cors: options.cors ?? null,
  basePath: options.basePath,
  console: quiet,
});

// ---------------------------------------------------------------------------
// 1 + 2. The batteries-included Server, over each engine.

// `createEngine` is a factory, not an instance: every boot needs a fresh
// engine (a uws engine refuses a second attach, a node engine would drop
// its previous WebsocketServer).
const bootServer = (createEngine) => async (options) => {
  const server = new Server({
    ...rpcOptions(options),
    protocol: 'http',
    host: '127.0.0.1',
    port: 0,
    timeouts: { bind: 100 },
    engine: createEngine(),
  });
  await server.listen();
  const { port } = server.address();
  return { port, rpc: server.rpc, close: () => server.close() };
};

// ---------------------------------------------------------------------------
// 3 + 4. The fastify plugin, over each backend.

const bootFastify = (serverFactory) => async (options) => {
  const fastify = optional('fastify');
  const app = fastify({ ...(serverFactory ? { serverFactory } : {}), logger: false });
  await app.register(wrpcFastify, { ...rpcOptions(options), console: quiet });
  await app.listen({ host: '127.0.0.1', port: 0 });
  const address = app.server.address();
  return { port: address.port, rpc: app.wrpc, close: () => app.close() };
};

// ---------------------------------------------------------------------------
// 5. express middleware on a listener it does not own.

const bootExpress = async (options) => {
  const express = optional('express');
  const app = express();
  const wrpc = createWrpc({ ...rpcOptions(options) });
  app.use(wrpc.handler);
  const httpServer = http.createServer(app);
  httpServer.on('upgrade', wrpc.upgrade);
  await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
  const { port } = httpServer.address();
  return {
    port,
    rpc: wrpc.rpc,
    close: async () => {
      await wrpc.close();
      httpServer.closeAllConnections();
      await new Promise((resolve) => httpServer.close(resolve));
    },
  };
};

// `pathMiss` records who answers a request outside basePath:
//   'wrpc'      — the core's JSON 404 packet (it owns every request)
//   'framework' — the host framework's own 404 (fastify router, express)
// Both are 404s; only the first carries a wrpc error packet, and that
// difference is the point of composing as middleware.
const buildBoots = () => {
  const uws = requireUws();
  const fastify = optional('fastify');
  const fastifyUws = optional('fastify-uws');
  const express = optional('express');
  return [
    {
      name: 'Server + node engine',
      pathMiss: 'wrpc',
      streams: true,
      boot: bootServer(() => createNodeEngine()),
    },
    {
      name: 'Server + uws engine',
      pathMiss: 'wrpc',
      streams: true,
      skip: uws ? false : 'uWebSockets.js unavailable',
      boot: bootServer(() => createUwsEngine({ uws })),
    },
    {
      name: 'fastify + node engine',
      pathMiss: 'framework',
      streams: true,
      skip: fastify ? false : 'fastify unavailable',
      boot: bootFastify(null),
    },
    {
      name: 'fastify + uws engine',
      pathMiss: 'framework',
      streams: true,
      skip: fastify && fastifyUws && uws ? false : 'fastify-uws / uWebSockets.js unavailable',
      boot: (options) => bootFastify(optional('fastify-uws').serverFactory)(options),
    },
    {
      name: 'express + node engine',
      pathMiss: 'framework',
      streams: true,
      skip: express ? false : 'express unavailable',
      boot: bootExpress,
    },
  ];
};

module.exports = { buildBoots, bootServer, bootFastify, bootExpress, requireUws, optional, quiet };
