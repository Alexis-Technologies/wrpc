'use strict';

const { Server } = require('../../src/server.js');
const { defineRouter, procedure } = require('../../src/rpc/router.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

// `api` is the bench-local shorthand every stack under bench/ shares:
// { unit: { method: { access?, handler(args, context) } } }. Adapt it onto
// the real router — procedure() handlers take (context, args), the default
// access here is 'public' since these benchmarks measure an unauthenticated
// echo, and system/introspect no longer needs a hand-rolled stand-in:
// RpcServer registers it itself.
const toRouter = (api) => {
  const definition = {};
  for (const [unit, methods] of Object.entries(api)) {
    const unitDef = {};
    for (const [name, def] of Object.entries(methods)) {
      unitDef[name] = procedure({
        access: def.access ?? 'public',
        handler: (context, args) => def.handler(args, context),
      });
    }
    definition[unit] = unitDef;
  }
  return defineRouter(definition);
};

const createWrpcServer = async (api) => {
  const server = new Server({
    router: toRouter(api),
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    timeouts: { bind: 100 },
    console: quiet,
  });
  await server.listen();
  const { port } = server.address();
  return { server, port };
};

module.exports = { createWrpcServer };
