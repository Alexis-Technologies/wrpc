'use strict';

const http = require('node:http');
const https = require('node:https');

const { Emitter } = require('./utils.js');
const { RpcServer, rpcOptions } = require('./rpc/core.js');
const { createNodeEngine, isEngine } = require('./engine/index.js');
const { receiveBody, nodeStream, createUpgradeGate, respondBodyError, MAX_BODY_SIZE } = require('./adapters/common.js');
const { createLoggerWriter } = require('./logging.js');

const DEFAULT_LISTEN_RETRY = 3;
const DEFAULT_BIND_TIMEOUT = 2000;

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

  /** Cluster-wide presence, introspection and node-to-node messaging. */
  get cluster() {
    return this.rpc.cluster;
  }

  /** The local client with this id; undefined when not on this instance. */
  getClient(id) {
    return this.rpc.getClient(id);
  }

  #onConnection(socket, req) {
    this.rpc.attachSocket(socket, {
      headers: req.headers,
      url: req.url ?? '',
      remoteAddress: req.socket?.remoteAddress ?? socket.remoteAddress,
    });
  }

  // Standalone engines own node:http too, so the shell creates no server
  // and hands the core's HTTP entry point to the engine instead.
  #initStandalone(wsOptions, cors) {
    this.wsServer = this.#engine.attach({
      ...wsOptions,
      verifyClient: createUpgradeGate({ rpc: this.rpc, cors, ws: wsOptions }),
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

    const verifyClient = createUpgradeGate({ rpc: this.rpc, cors, ws: wsOptions });
    this.wsServer = this.#engine.attach({ server: this.httpServer, ...wsOptions, verifyClient });
    this.wsServer.on('connection', (socket, req) => {
      this.#onConnection(socket, req);
    });

    this.on('port', (port) => {
      this.rpc.attachPort(port);
    });
  }

  async #handleHttpRequest(req, res) {
    const respond = ({ status, headers, body }) => {
      if (res.writableEnded) return;
      res.writeHead(status, headers);
      res.end(body);
    };
    let body = null;
    try {
      body = await receiveBody(req, this.#options.maxBodySize ?? MAX_BODY_SIZE);
    } catch (error) {
      return void respondBodyError(respond, error);
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

  /**
   * Shuts the server down. With `drain` (ms) the shutdown is graceful:
   * intake stops first (new connections are refused, new calls answer 503),
   * in-flight calls get up to `drain` ms to settle, then every peer gets a
   * 1001 "going away" close frame, and only what remains is torn down hard.
   * Without it the same sequence runs with a zero-length drain window.
   */
  async close(options = {}) {
    const { drain = 0 } = options;
    if (!this.httpServer) {
      // Intake first, same ordering as the node boot below: a standalone
      // engine that can close its listen socket separately refuses new
      // connections while the in-flight work drains, instead of accepting
      // calls it will immediately 503.
      if (typeof this.#engine.stopListening === 'function') this.#engine.stopListening();
      await this.rpc.drain(drain);
      // Standalone engines own the whole stack: their close() both stops
      // the listener and says goodbye to the peers.
      this.#engine.close();
      return void (await this.rpc.close());
    }
    // Stop intake first, so a load balancer's next health check fails while
    // the in-flight work is still being finished.
    const closed = new Promise((resolve) => {
      this.httpServer.close((error) => {
        if (error) this.#log.error({ err: error, event: 'close' });
        resolve();
      });
    });
    await this.rpc.drain(drain);
    // The engine's close sends 1001 to every open socket BEFORE the core
    // evicts the clients — the reverse order used to terminate everyone and
    // then say goodbye to nobody.
    this.#engine.close();
    await this.rpc.close();
    this.httpServer.closeAllConnections();
    await closed;
  }
}

module.exports = { Server };
