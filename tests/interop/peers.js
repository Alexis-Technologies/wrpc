'use strict';

// The two packages a mixed deployment runs side by side: this tree, and the
// PUBLISHED 1.0 (the `wrpc-v1` devDependency alias — `npm:@alexify/wrpc@1.0.0`,
// test-only like every other devDependency). Not a *.test.js: node --test
// must not run helpers. A machine without the alias installed skips the
// interop suite, the way the adapter suites skip a missing framework.

const next = require('../../index.js');
require('../../sse.js');

const optional = (name) => {
  try {
    return require(name);
  } catch {
    return null;
  }
};

const legacy = optional('wrpc-v1');
if (legacy) optional('wrpc-v1/sse');

// One router, defined with the library that serves it: `echo/say` answers
// its arguments, `echo/nudge` sends them back as an event first.
const routerOf = (lib) =>
  lib.defineRouter({
    echo: {
      say: lib.procedure({ access: 'public', handler: async (_context, args) => args }),
      nudge: lib.procedure({
        access: 'public',
        handler: async (context, args) => {
          context.client.sendEvent('echo/poke', args);
          return true;
        },
      }),
    },
  });

/** Boots `lib`'s Server on a free port; teardown is registered before anything can fail. */
const boot = async (t, lib, options = {}) => {
  const server = new lib.Server({
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 50 },
    router: routerOf(lib),
    ...options,
  });
  await server.listen();
  t.after(() => server.close());
  const { port } = server.address();
  const { basePath } = server.rpc;
  return { server, ws: `ws://127.0.0.1:${port}${basePath}`, http: `http://127.0.0.1:${port}${basePath}` };
};

/** Connects `lib`'s client (no heartbeat, no reconnect) with `echo` loaded. */
const connect = async (t, lib, url, options = {}) => {
  const client = await lib.WrpcClient.connect(url, { heartbeat: false, reconnect: false, ...options });
  t.after(() => void client.close());
  await client.load('echo');
  return client;
};

module.exports = { next, legacy, boot, connect, routerOf };
