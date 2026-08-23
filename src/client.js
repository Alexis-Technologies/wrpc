'use strict';

// The barrel the rest of the package (and the sse subpath) requires: the
// core, the built-in transports (their require registers them), and the
// Service Worker proxy. Splitting the modules changed no require path —
// this file kept the old one.

const { WrpcClient, WrpcError, ClientTransport, isClientTransport, metaHeaders } = require('./client/core.js');
require('./client/transports.js');
const { WrpcClientProxy } = require('./client/proxy.js');

WrpcClient.initialize();

// The contract-first entry point. `connect<Api>(url)` is the same call as
// `WrpcClient.connect(url)` under a name that reads as a function, because
// that is what carries the type argument in `connect<Api>('wss://host')` —
// the whole typed-client story is the ONE place a user names their contract.
// A wrapper rather than a re-export of the static: the name of the thing a
// user calls should not depend on how the class happens to expose it.
const connect = (url, options) => WrpcClient.connect(url, options);

// ClientTransport is exported for transports that live in their own subpath
// (see src/sse/client.js); it is deliberately NOT re-exported from the
// package barrel, where transports stay type-only.
module.exports = { WrpcClient, WrpcClientProxy, WrpcError, ClientTransport, isClientTransport, connect, metaHeaders };
