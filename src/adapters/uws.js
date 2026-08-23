'use strict';

const { EventEmitter } = require('node:events');

const { MAX_BODY_SIZE, statusLine, eachHeader } = require('./common.js');

// uWebSockets.js engine adapter. The module itself is NEVER a dependency of
// @alexify/wrpc (zero-dependency rule) — the caller injects it:
//
//   const { createUwsEngine } = require('@alexify/wrpc/uws');
//   const engine = createUwsEngine({ uws: require('uWebSockets.js') });
//   const server = new Server({ router, engine, port: 8000 });
//
// This is a *standalone* engine: uws owns the whole network stack, node:http
// included, so the Server shell creates no http server and routes its HTTP
// calls through the `onHttpCall` attach option instead.

// uws send() status codes.
const BACKPRESSURE = 0;
const SUCCESS = 1;
const DROPPED = 2;

const DEFAULT_IDLE_TIMEOUT = 120; // seconds; uws' own liveness mechanism
const DEFAULT_MAX_PAYLOAD = 16 * 1024 * 1024;

// getRemoteAddressAsText hands back the formatted address as an ArrayBuffer;
// it is empty for a socket that is already gone.
const decodeAddress = (value) => Buffer.from(value).toString();

const copyBytes = (arrayBuffer) => Buffer.from(new Uint8Array(arrayBuffer));

const isTemplatedApp = (app) =>
  typeof app === 'object' &&
  app !== null &&
  typeof app.ws === 'function' &&
  typeof app.any === 'function' &&
  typeof app.listen === 'function';

// A node IncomingMessage look-alike: enough for verifyClient gates, the
// Server shell's path/origin checks, and RpcServer.attachSocket's meta.
const createUpgradeRequest = (path, query, headers, remoteAddress) => ({
  method: 'GET',
  url: query ? `${path}?${query}` : path,
  headers,
  socket: { remoteAddress },
});

// The protocol revision marker; must match the built-in engine's.
const WRPC_PROTOCOL = 'wrpc.v1';

// Mirrors WebsocketServer's negotiation: `false` from handleProtocols
// rejects the handshake, anything else selects (or declines) a subprotocol,
// and with no app configuration the wrpc revision is echoed when offered.
const negotiateProtocol = (header, { protocols, handleProtocols }, request) => {
  if (!header) return '';
  const offered = header
    .split(',')
    .map((token) => token.trim())
    .filter(Boolean);
  if (handleProtocols) {
    const selected = handleProtocols(offered, request);
    if (selected === false) return false;
    return selected || '';
  }
  if (protocols) return offered.find((name) => protocols.includes(name)) ?? '';
  if (offered.includes(WRPC_PROTOCOL)) return WRPC_PROTOCOL;
  return '';
};

/**
 * WrpcSocket over a uws WebSocket.
 *
 * The uws handle is invalid the moment its close handler runs — every method
 * on it throws "Invalid access of closed uWS.WebSocket" afterwards — so every
 * call here is gated on #closed rather than trusting the caller's timing.
 */
class UwsSocket extends EventEmitter {
  #ws;
  #closed = false;

  constructor(ws, { remoteAddress = '', protocol = '' } = {}) {
    super();
    this.#ws = ws;
    this.remoteAddress = remoteAddress;
    this.protocol = protocol;
  }

  get closed() {
    return this.#closed;
  }

  get bufferedAmount() {
    if (this.#closed) return 0;
    try {
      return this.#ws.getBufferedAmount();
    } catch {
      return 0;
    }
  }

  send(data) {
    if (this.#closed) return false;
    const isBinary = typeof data !== 'string';
    let status;
    try {
      status = this.#ws.send(data, isBinary);
    } catch {
      // Raced a close between the guard and the call.
      return false;
    }
    if (status === DROPPED) {
      // uws silently discarded the message (maxBackpressure hit with
      // closeOnBackpressureLimit off). A hole in the frame stream corrupts
      // the RPC protocol, so fail loudly instead of continuing.
      this.emit('error', new Error('uws dropped an outgoing message: backpressure limit exceeded'));
      this.terminate();
      return false;
    }
    return status === SUCCESS;
  }

  close(code = 1000, reason = '') {
    if (this.#closed) return;
    try {
      this.#ws.end(code, reason);
    } catch {
      // already gone
    }
  }

  terminate() {
    if (this.#closed) return;
    try {
      this.#ws.close();
    } catch {
      // already gone
    }
  }

  // Called from the uws close handler; after this the handle is poison.
  markClosed(code, reason) {
    if (this.#closed) return;
    this.#closed = true;
    this.emit('close', code, reason);
  }
}

const createUwsEngine = (engineOptions = {}) => {
  const {
    uws = null,
    app: providedApp = null,
    ssl = null,
    idleTimeout = DEFAULT_IDLE_TIMEOUT,
    maxPayloadLength = DEFAULT_MAX_PAYLOAD,
    // Finite by default, matching the built-in engine: with 0 uws buffers an
    // unresponsive peer without limit. One biggest-allowed message in
    // flight; past it the adapter terminates loudly (see the DROPPED path).
    maxBackpressure = maxPayloadLength,
    closeOnBackpressureLimit = false,
    maxLifetime = 0,
    compression = null,
    sendPingsAutomatically = true,
    maxBodySize = MAX_BODY_SIZE,
  } = engineOptions;

  if (!providedApp && (typeof uws !== 'object' || uws === null || typeof uws.App !== 'function')) {
    throw new TypeError(
      'createUwsEngine: pass the uWebSockets.js module as options.uws ' +
        '(it is injected, never a dependency of @alexify/wrpc), or an existing app as options.app',
    );
  }
  if (providedApp && !isTemplatedApp(providedApp)) {
    throw new TypeError('createUwsEngine: options.app does not look like a uWebSockets.js TemplatedApp');
  }

  // Only an app we created is ours to tear down; one handed in by fastify-uws
  // belongs to fastify.
  const ownsApp = !providedApp;
  const app = providedApp ?? (ssl ? uws.SSLApp(ssl) : uws.App());
  const source = new EventEmitter();
  const sockets = new Set();
  let listenSocket = null;
  let stoppedListening = false;
  let closed = false;
  let attached = false;

  const behavior = (attachOptions) => {
    const { verifyClient, protocols, handleProtocols } = attachOptions;
    return {
      idleTimeout,
      maxPayloadLength,
      maxBackpressure,
      closeOnBackpressureLimit,
      maxLifetime,
      sendPingsAutomatically,
      ...(compression === null ? {} : { compression }),

      upgrade(res, req, context) {
        // `req` is only valid synchronously, before the first await or the
        // upgrade call — read everything off it up front.
        const path = req.getUrl();
        const query = req.getQuery() ?? '';
        const headers = {};
        req.forEach((key, value) => {
          headers[key] = value;
        });
        const key = req.getHeader('sec-websocket-key');
        const offeredProtocols = req.getHeader('sec-websocket-protocol');
        const extensions = req.getHeader('sec-websocket-extensions');
        // The ws handle reports an empty address after upgrade; the response
        // still knows it here, so capture it while we can.
        const remoteAddress = decodeAddress(res.getRemoteAddressAsText());

        let aborted = false;
        res.onAborted(() => {
          aborted = true;
        });

        const request = createUpgradeRequest(path, query, headers, remoteAddress);
        const reject = (status) => {
          if (aborted) return;
          res.cork(() => {
            res.writeStatus(statusLine(status));
            res.end();
          });
        };
        if (closed) return void reject(503);
        if (verifyClient && !verifyClient({ req: request, socket: null, head: null })) {
          return void reject(403);
        }
        const protocol = negotiateProtocol(offeredProtocols, { protocols, handleProtocols }, request);
        if (protocol === false) return void reject(400);
        if (aborted) return;
        res.upgrade({ request, protocol, remoteAddress }, key, protocol, extensions, context);
      },

      open(ws) {
        const data = ws.getUserData();
        const socket = new UwsSocket(ws, data);
        data.socket = socket;
        sockets.add(socket);
        source.emit('connection', socket, data.request);
      },

      message(ws, message, isBinary) {
        // The ArrayBuffer is neutered when this callback returns, and the
        // RPC core hands payloads to async stream consumers — copy.
        ws.getUserData().socket?.emit('message', copyBytes(message), isBinary);
      },

      drain(ws) {
        ws.getUserData().socket?.emit('drain');
      },

      ping(ws, message) {
        ws.getUserData().socket?.emit('ping', copyBytes(message));
      },

      pong(ws, message) {
        ws.getUserData().socket?.emit('pong', copyBytes(message));
      },

      close(ws, code, message) {
        const { socket } = ws.getUserData();
        if (!socket) return;
        sockets.delete(socket);
        socket.markClosed(code, copyBytes(message).toString());
      },
    };
  };

  const httpHandler = (onHttpCall) => (res, req) => {
    const method = req.getCaseSensitiveMethod().toUpperCase();
    const path = req.getUrl();
    const query = req.getQuery() ?? '';
    const headers = {};
    req.forEach((key, value) => {
      headers[key] = value;
    });
    const remoteAddress = decodeAddress(res.getRemoteAddressAsText());
    // `req` is dead from here on.

    let aborted = false;
    let responded = false;
    const abortListeners = [];
    res.onAborted(() => {
      aborted = true;
      for (const listener of abortListeners) listener();
    });

    const respond = ({ status, headers: responseHeaders = {}, body }) => {
      if (aborted || responded) return;
      responded = true;
      res.cork(() => {
        res.writeStatus(statusLine(status));
        eachHeader(responseHeaders, (name, value) => {
          // uws derives Content-Length from end(); writing our own would
          // emit it twice.
          if (name.toLowerCase() === 'content-length') return;
          res.writeHeader(name, value);
        });
        res.end(body ?? '');
      });
    };

    const call = {
      method,
      url: query ? `${path}?${query}` : path,
      headers,
      body: null,
      remoteAddress,
      respond,
      onAbort: (listener) => void abortListeners.push(listener),
    };

    const chunks = [];
    let size = 0;
    let overflowed = false;
    res.onData((chunk, isLast) => {
      if (aborted || overflowed) return;
      if (chunk.byteLength > 0) {
        size += chunk.byteLength;
        if (size > maxBodySize) {
          overflowed = true;
          const packet = { type: 'callback', id: '', error: { message: 'Body size limit exceeded', code: 400 } };
          return void respond({
            status: 400,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(packet),
          });
        }
        chunks.push(copyBytes(chunk));
      }
      if (!isLast) return;
      if (chunks.length === 1) call.body = chunks[0];
      else if (chunks.length > 1) call.body = Buffer.concat(chunks, size);
      void onHttpCall(call);
    });
  };

  return {
    name: 'uws',
    standalone: true,
    capabilities: {
      backpressure: true,
      // uws owns liveness through idleTimeout + sendPingsAutomatically, so
      // the RPC layer must not run a ping loop of its own.
      ping: false,
      // uws.DISABLED is 0, so a falsy compressor means no deflate.
      deflate: Boolean(compression),
      cork: true,
      // No receive-side flow control: uws exposes no socket-level pause, so
      // a fast uploader is not throttled by a slow stream consumer.
      pause: false,
    },
    app,

    // attachOptions: { path, verifyClient, protocols, handleProtocols,
    //   onHttpCall } — no `server`, this engine owns the network stack.
    attach(attachOptions = {}) {
      if (attached) throw new Error('createUwsEngine: this engine is already attached');
      attached = true;
      const { path = '/*', onHttpCall } = attachOptions;
      app.ws(path, behavior(attachOptions));
      // Without onHttpCall the host framework (fastify) owns HTTP routing
      // and we only take over the upgrade path.
      if (onHttpCall) app.any('/*', httpHandler(onHttpCall));
      return source;
    },

    listen({ host = '0.0.0.0', port = 0 } = {}) {
      return new Promise((resolve, reject) => {
        app.listen(host, port, (token) => {
          if (!token) {
            // uws reports only success/failure, never a reason. In practice
            // a refused bind is a busy port, and tagging it EADDRINUSE keeps
            // the Server shell's bind-retry working.
            const error = new Error(`Failed to bind ${host}:${port}`);
            error.code = 'EADDRINUSE';
            return void reject(error);
          }
          listenSocket = token;
          const bound = typeof uws?.us_socket_local_port === 'function' ? uws.us_socket_local_port(token) : port;
          resolve({ address: host, family: host.includes(':') ? 'IPv6' : 'IPv4', port: bound });
        });
      });
    },

    // The listener-only phase of a graceful shutdown: new connections are
    // refused while the sockets already accepted keep working — the same
    // intake-first ordering the node boot gets from httpServer.close().
    // Optional in the engine port; feature-detected by the Server shell.
    stopListening() {
      if (closed || !listenSocket) return;
      if (typeof uws?.us_listen_socket_close === 'function') {
        uws.us_listen_socket_close(listenSocket);
        stoppedListening = true;
      }
      listenSocket = null;
    },

    close({ code = 1001, reason = 'Server is closing' } = {}) {
      if (closed) return;
      closed = true;
      for (const socket of sockets) socket.close(code, reason);
      sockets.clear();
      // app.close() closes the app's listen sockets itself, so closing the
      // token first would be a double free — a native SIGSEGV, not an error.
      // Only an app we do NOT own (fastify-uws) leaves the token to us, and
      // in that case we never opened one anyway. After stopListening() the
      // token is already closed, so app.close() must NOT run again.
      if (ownsApp && !stoppedListening) {
        app.close();
      } else if (listenSocket && typeof uws?.us_listen_socket_close === 'function') {
        uws.us_listen_socket_close(listenSocket);
      }
      listenSocket = null;
      source.emit('close');
    },
  };
};

module.exports = { createUwsEngine, UwsSocket, BACKPRESSURE, SUCCESS, DROPPED };
