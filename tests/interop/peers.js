'use strict';

// The two packages a mixed deployment runs side by side: this tree, and the
// PUBLISHED 1.0 (the `wrpc-v1` devDependency alias — `npm:@alexify/wrpc@1.0.0`,
// test-only like every other devDependency). Not a *.test.js: node --test
// must not run helpers. A machine without the alias installed skips the
// interop suite, the way the adapter suites skip a missing framework.

const path = require('node:path');
const vm = require('node:vm');

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
      // What the server observed of the connection: one declared header, the
      // declared data, and the connect URL (where a query carrier shows).
      seen: lib.procedure({
        access: 'public',
        handler: async (context) => {
          const { headers, data, url } = context.client.meta;
          return { tenant: headers['x-tenant'] ?? null, data: { ...data }, url };
        },
      }),
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
  // 1.0's worker transport is ONE instance per process (2.0 builds one per
  // connect): without this an earlier test's closed client still hears a
  // later one's answers, and says so on the console.
  if (options.worker && lib === legacy) lib.WrpcClient.transport.event.instance = null;
  const client = await lib.WrpcClient.connect(url, { heartbeat: false, reconnect: false, ...options });
  t.after(() => void client.close());
  await client.load('echo');
  return client;
};

// The BROWSER build of this tree's client, bundled the way an application's
// bundler does it (the `browser` field map swaps the platform halves in) and
// evaluated here: Node has the WebSocket, btoa and TextEncoder it needs. It
// is the only way to drive what a page does on a handshake — a Node client
// sends real request headers and never reaches the carrier tokens.
let bundled = null;
const browserBuild = () => {
  bundled ??= (async () => {
    const esbuild = require('esbuild');
    const result = await esbuild.build({
      entryPoints: [path.join(__dirname, '..', '..', 'browser.js')],
      bundle: true,
      platform: 'browser',
      format: 'cjs',
      write: false,
      logLevel: 'silent',
    });
    const module = { exports: {} };
    const wrapped = `(function (module, exports) {${result.outputFiles[0].text}\n})`;
    vm.runInThisContext(wrapped, { filename: 'wrpc.browser.bundle.js' })(module, module.exports);
    return module.exports;
  })();
  return bundled;
};

module.exports = { next, legacy, boot, connect, routerOf, browserBuild };
