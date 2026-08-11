'use strict';

const http = require('node:http');
const https = require('node:https');

const { Emitter } = require('./utils.js');
const { RpcServer, rpcOptions } = require('./rpc/core.js');
const { isOriginAllowed } = require('./transport.js');
const { createNodeEngine, isEngine } = require('./engine/index.js');
const { receiveBody, nodeStream } = require('./adapters/common.js');
const { createLoggerWriter } = require('./logging.js');

const DEFAULT_LISTEN_RETRY = 3;
const DEFAULT_BIND_TIMEOUT = 2000;

const getPathname = (url) => (url ? url.split('?')[0] : '/');

// Batteries-included shell over the engine-agnostic RpcServer core:
// creates the node http(s) server, attaches a WebSocket engine (the
// built-in one by default), and feeds HTTP requests into the core.
//
// With a standalone engine (uWebSockets.js) there is no node http server at
// all: `httpServer` stays null and the engine owns listening plus the HTTP
// request path. Read the bound address through `server.address()`, which
// covers both shapes.
class Server extends Emitter {
  httpServer = null;
  wsServer = null;
  rpc = null;
  #engine;
  #options;
  #log;
  #address = null;

  constructor(options = {}) {
    super();
    const { cors = null, logger = globalThis.console, engine = createNodeEngine(), ws = {} } = options;
    if (!isEngine(engine)) {
      throw new TypeError('Server: options.engine does not implement the Engine contract');
    }
    this.#options = options;
    this.#log = createLoggerWriter(logger);
    this.#engine = engine;
    this.rpc = new RpcServer(rpcOptions({ ...options, cors, logger }));
    if (engine.standalone) this.#initStandalone(ws, cors);
    else this.#init(ws, cors);
  }

  // The bound address, whichever side owns the listener.
  address() {
    if (this.httpServer) return this.httpServer.address();
    return this.#address;
  }

  // Rooms, forwarded to the core so `server.to('chat').emit(...)` reads the
  // same whether wrpc owns the listener or an adapter does.

  get rooms() {
    return this.rpc.rooms;
  }

  get clients() {
    return this.rpc.clients;
  }

  to(...rooms) {
    return this.rpc.to(...rooms);
  }

  except(...clients) {
    return this.rpc.except(...clients);
  }

  broadcast(name, data) {
    return this.rpc.broadcast(name, data);
  }

  #upgradeGate(wsOptions, cors) {
    // With an explicit ws.path the engine already gates the pathname, and
    // the default RPC-path gate would 403 every upgrade to a custom path —
    // keep only the origin check in that case.
    const checkPath = wsOptions.path === undefined;
    return wsOptions.verifyClient ?? (({ req }) => this.#verifyUpgrade(req, cors, checkPath));
  }

  #onConnection(socket, req) {
    this.rpc.attachSocket(socket, {
      headers: req.headers,
      remoteAddress: req.socket?.remoteAddress ?? socket.remoteAddress,
    });
  }

  // Standalone engines own node:http too, so the shell creates no server
  // and hands the core's HTTP entry point to the engine instead.
  #initStandalone(wsOptions, cors) {
    this.wsServer = this.#engine.attach({
      ...wsOptions,
      verifyClient: this.#upgradeGate(wsOptions, cors),
      onHttpCall: (call) => this.rpc.handleHttpCall(call),
    });
    this.wsServer.on('connection', (socket, req) => {
      this.#onConnection(socket, req);
    });
    this.on('port', (port) => {
      this.rpc.attachPort(port);
    });
  }

  #init(wsOptions, cors) {
    const { protocol, nagle = true, key, cert, SNICallback } = this.#options;
    const proto = protocol === 'http' ? http : https;
    const opt = { key, cert, noDelay: !nagle, SNICallback };
    this.httpServer = proto.createServer(opt);

    this.httpServer.on('request', (req, res) => {
      this.#handleHttpRequest(req, res);
    });

    const verifyClient = this.#upgradeGate(wsOptions, cors);
    this.wsServer = this.#engine.attach({ server: this.httpServer, ...wsOptions, verifyClient });
    this.wsServer.on('connection', (socket, req) => {
      this.#onConnection(socket, req);
    });

    this.on('port', (port) => {
      this.rpc.attachPort(port);
    });
  }

  // Default upgrade gate: the RPC paths (plus bare '/') and, when CORS
  // origins are configured, a matching Origin header.
  #verifyUpgrade(req, cors, checkPath = true) {
    if (checkPath) {
      const pathname = getPathname(req.url);
      const pathOk = pathname === '/' || this.rpc.matchPath(pathname) !== null;
      if (!pathOk) return false;
    }
    return isOriginAllowed(cors, req.headers.origin);
  }

  async #handleHttpRequest(req, res) {
    const respond = ({ status, headers, body }) => {
      if (res.writableEnded) return;
      res.writeHead(status, headers);
      res.end(body);
    };
    let body = null;
    try {
      body = await receiveBody(req);
    } catch (error) {
      const packet = { type: 'callback', id: '', error: { message: error.message, code: 400 } };
      return void respond({
        status: 400,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(packet),
      });
    }
    await this.rpc.handleHttpCall({
      method: req.method,
      url: req.url,
      headers: req.headers,
      body,
      remoteAddress: req.socket.remoteAddress,
      respond,
      // Keeps the response open and writes into it — how SSE is served.
      stream: nodeStream(res),
      // Lets the core evict clients for requests that never get a response
      onAbort: (listener) => void res.on('close', listener),
    });
  }

  // One bind attempt; rejects with the bind error (EADDRINUSE included) so
  // the retry loop above can decide, and detaches both listeners either way
  // so a retried listen() does not stack them.
  #bindOnce(host, port) {
    if (!this.httpServer) return this.#engine.listen({ host, port });
    return new Promise((resolve, reject) => {
      const onListening = () => {
        this.httpServer.off('error', onError);
        resolve(this.httpServer.address());
      };
      const onError = (error) => {
        this.httpServer.off('listening', onListening);
        reject(error);
      };
      this.httpServer.once('listening', onListening);
      this.httpServer.once('error', onError);
      this.httpServer.listen(port, host);
    });
  }

  listen() {
    const { host, port, timeouts = {}, retry } = this.#options;
    let count = retry || DEFAULT_LISTEN_RETRY;

    return new Promise((resolve, reject) => {
      const attempt = () => {
        this.#bindOnce(host, port).then(
          (address) => {
            this.#address = address;
            this.#log.info({ event: 'listen', port: address?.port ?? port }, `Listen port ${address?.port ?? port}`);
            resolve(this);
          },
          (error) => {
            if (error.code !== 'EADDRINUSE') return void reject(error);
            count--;
            if (count === 0) return void reject(error);
            this.#log.warn({ event: 'listen.retry', host, port }, `Address in use: ${host}:${port}, retry...`);
            setTimeout(attempt, timeouts.bind ?? DEFAULT_BIND_TIMEOUT);
          },
        );
      };
      attempt();
    });
  }

  async close() {
    if (!this.httpServer) {
      await this.rpc.close();
      return void this.#engine.close();
    }
    const closed = new Promise((resolve) => {
      this.httpServer.close((error) => {
        if (error) this.#log.error({ err: error, event: 'close' });
        resolve();
      });
    });
    await this.rpc.close();
    this.#engine.close();
    this.httpServer.closeAllConnections();
    await closed;
  }
}

module.exports = { Server };
