'use strict';

// What every broker binding needs from the server it binds to.

/** The RpcServer behind a `Server` (its `.rpc`) or an RpcServer itself. */
const rpcOf = (server, label) => {
  const rpc = server?.rpc ?? server;
  if (!rpc || typeof rpc.attach !== 'function' || !rpc.router || typeof rpc.router.getProcedure !== 'function') {
    throw new TypeError(`${label}: a Server or an RpcServer is required`);
  }
  return rpc;
};

module.exports = { rpcOf };
