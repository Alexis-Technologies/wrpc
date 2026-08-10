'use strict';

const http = require('node:http');
const https = require('node:https');

const { Emitter } = require('./utils.js');
const { RpcServer } = require('./rpc/core.js');
const { isOriginAllowed } = require('./transport.js');
const { createNodeEngine, isEngine } = require('./engine/index.js');

const DEFAULT_LISTEN_RETRY = 3;
const DEFAULT_BIND_TIMEOUT = 2000;
const MAX_BODY_SIZE = 10 * 1024 * 1024;

const receiveBody = async (stream, limit = MAX_BODY_SIZE) => {
  if (!Number.isSafeInteger(limit) || limit < 0) {
    throw new TypeError('Body size limit must be a non-negative safe integer');
  }
  const chunks = [];
  let size = 0;
  for await (const chunk of stream) {
    size += chunk.length;
    if (size > limit) throw new Error('Body size limit exceeded');
    chunks.push(chunk);
  }
  if (chunks.length === 1) return chunks[0];
  return Buffer.concat(chunks, size);
};

const getPathname = (url) => (url ? url.split('?')[0] : '/');

// Batteries-included shell over the engine-agnostic RpcServer core:
// creates the node http(s) server, attaches a WebSocket engine (the
// built-in one by default), and feeds HTTP requests into the core.
class Server extends Emitter {
  httpServer = null;
  wsServer = null;
  rpc = null;
  #engine;
  #options;
  #console;

  constructor(options = {}) {
    super();
    const {
      router,
      sessions,
      cors = null,
      basePath,
      console = globalThis.console,
      engine = createNodeEngine(),
      ws = {},
    } = options;
    if (!isEngine(engine)) {
      throw new TypeError('Server: options.engine does not implement the Engine contract');
    }
    this.#options = options;
    this.#console = console;
    this.#engine = engine;
    this.rpc = new RpcServer({ router, sessions, cors, basePath, console });
    this.#init(ws, cors);
  }

  #init(wsOptions, cors) {
    const { protocol, nagle = true, key, cert, SNICallback } = this.#options;
    const proto = protocol === 'http' ? http : https;
    const opt = { key, cert, noDelay: !nagle, SNICallback };
    this.httpServer = proto.createServer(opt);

    this.httpServer.on('request', (req, res) => {
      this.#handleHttpRequest(req, res);
    });

    // With an explicit ws.path the engine already gates the pathname, and
    // the default RPC-path gate would 403 every upgrade to a custom path —
    // keep only the origin check in that case.
    const checkPath = wsOptions.path === undefined;
    const verifyClient = wsOptions.verifyClient ?? (({ req }) => this.#verifyUpgrade(req, cors, checkPath));
    this.wsServer = this.#engine.attach({ server: this.httpServer, ...wsOptions, verifyClient });
    this.wsServer.on('connection', (socket, req) => {
      this.rpc.attachSocket(socket, {
        headers: req.headers,
        remoteAddress: req.socket?.remoteAddress,
      });
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
      // Lets the core evict clients for requests that never get a response
      onAbort: (listener) => void res.on('close', listener),
    });
  }

  listen() {
    const { host, port, timeouts = {}, retry } = this.#options;

    let count = retry || DEFAULT_LISTEN_RETRY;
    let listen = null;

    return new Promise((resolve, reject) => {
      const onListening = () => {
        this.#console.info(`Listen port ${port}`);
        resolve(this);
      };

      const onError = (error) => {
        if (error.code !== 'EADDRINUSE') return void reject(error);
        count--;
        if (count === 0) return void reject(error);
        this.#console.warn(`Address in use: ${host}:${port}, retry...`);
        setTimeout(listen, timeouts.bind ?? DEFAULT_BIND_TIMEOUT);
      };

      listen = () => {
        this.httpServer.once('listening', onListening);
        this.httpServer.once('error', onError);
        this.httpServer.listen(port, host);
      };

      listen();
    });
  }

  async close() {
    const closed = new Promise((resolve) => {
      this.httpServer.close((error) => {
        if (error) this.#console.error(error);
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
