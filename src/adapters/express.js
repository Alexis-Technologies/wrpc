'use strict';

const { RpcServer, rpcOptions } = require('../rpc/core.js');
const { createNodeEngine, isEngine } = require('../engine/index.js');
const { isOriginAllowed } = require('../transport.js');
const { receiveBody, normalizeBody, nodeStream, MAX_BODY_SIZE } = require('./common.js');

// express (and bare node:http) adapter. Unlike the batteries-included
// Server, nothing here owns the listener — the app does:
//
//   const wrpc = createWrpc({ router });
//   app.use(wrpc.handler);
//   const httpServer = app.listen(8000);
//   httpServer.on('upgrade', wrpc.upgrade);
//
// Middleware semantics: a request outside basePath is passed to next()
// rather than answered with 404, so wrpc composes with the rest of the app
// instead of swallowing its routes.

const getPathname = (url) => (url ? url.split('?')[0] : '/');

const createWrpc = (options = {}) => {
  const { cors = null, console = globalThis.console, ws = {}, maxBodySize = MAX_BODY_SIZE } = options;
  const rpc = options.rpc ?? new RpcServer(rpcOptions({ ...options, cors, console }));
  const engine = options.engine ?? createNodeEngine(ws);
  if (!isEngine(engine)) {
    throw new TypeError('createWrpc: options.engine does not implement the Engine contract');
  }
  if (engine.standalone) {
    throw new TypeError(
      'createWrpc: a standalone engine (uWebSockets.js) owns its own network stack ' +
        'and cannot be driven from express middleware — use Server({ engine }) instead',
    );
  }

  const verifyUpgrade = ({ req }) => {
    if (ws.path === undefined) {
      const pathname = getPathname(req.url);
      if (pathname !== '/' && rpc.matchPath(pathname) === null) return false;
    }
    return isOriginAllowed(cors, req.headers.origin);
  };

  // No `server`: this engine is driven by hand from the app's own 'upgrade'
  // listener (see `upgrade` below).
  const source = engine.attach({ ...ws, verifyClient: ws.verifyClient ?? verifyUpgrade });
  source.on('connection', (socket, req) => {
    rpc.attachSocket(socket, {
      headers: req.headers,
      remoteAddress: req.socket?.remoteAddress ?? socket.remoteAddress,
    });
  });
  if (typeof source.handleUpgrade !== 'function') {
    engine.close(); // the attach above already claimed resources
    throw new TypeError('createWrpc: the engine does not support manual upgrades (no handleUpgrade on its source)');
  }

  const handler = (req, res, next) => {
    // originalUrl survives express' mount-path rewriting; basePath alone
    // decides which paths belong to wrpc.
    const url = req.originalUrl ?? req.url ?? '/';
    if (rpc.matchPath(getPathname(url)) === null) return void next();

    const respond = ({ status, headers, body }) => {
      if (res.writableEnded) return;
      res.writeHead(status, headers);
      res.end(body);
    };

    const dispatch = (body) =>
      rpc.handleHttpCall({
        method: req.method,
        url,
        headers: req.headers,
        body,
        remoteAddress: req.socket?.remoteAddress,
        respond,
        stream: nodeStream(res),
        onAbort: (listener) => void res.on('close', listener),
      });

    // A body parser upstream (express.json()) already drained the stream;
    // without one, read it here.
    if (req.body !== undefined) return void dispatch(normalizeBody(req.body));
    receiveBody(req, maxBodySize).then(dispatch, (error) => {
      const packet = { type: 'callback', id: '', error: { message: error.message, code: 400 } };
      respond({ status: 400, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(packet) });
    });
  };

  const upgrade = (req, socket, head) => {
    source.handleUpgrade(req, socket, head);
  };

  const close = async () => {
    await rpc.close();
    engine.close();
  };

  return { rpc, engine, wsServer: source, handler, upgrade, close };
};

module.exports = { createWrpc };
