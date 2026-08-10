'use strict';

const { createNodeEngine } = require('./node.js');

// Structural runtime check for user-provided engines (duck typing per the
// zero-dependency injection rule) — used by the Server shell to fail fast
// with a clear message instead of a deep TypeError.
//
// Two engine kinds satisfy the port:
//
//   hosted (default, `standalone` falsy) — attaches to a node http(s)
//     server's upgrade flow: attach({ server, ... }). The Server shell owns
//     the listener and feeds HTTP requests into RpcServer.handleHttpCall.
//     Such an engine MAY also support manual upgrades: when the returned
//     source exposes handleUpgrade(req, socket, head), a middleware adapter
//     can drive the handshake from its own 'upgrade' listener.
//
//   standalone (`standalone: true`) — owns the whole network stack, node
//     http included (uWebSockets.js). It is attached WITHOUT a server:
//     attach({ path, verifyClient, onHttpCall, ... }), where onHttpCall
//     receives the same abstract call description handleHttpCall consumes,
//     and it must implement listen({ host, port }) -> Promise<address>.
const isEngine = (engine) =>
  typeof engine === 'object' &&
  engine !== null &&
  typeof engine.name === 'string' &&
  typeof engine.attach === 'function' &&
  typeof engine.close === 'function' &&
  (!engine.standalone || typeof engine.listen === 'function');

module.exports = { createNodeEngine, isEngine };
