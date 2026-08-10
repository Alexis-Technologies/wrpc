'use strict';

const crypto = require('node:crypto');
const { EventEmitter } = require('node:events');

const { Connection } = require('./connection.js');
const permessageDeflate = require('./permessageDeflate.js');

const MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const PING_INTERVAL = 10000;
const EOL = '\r\n';
const EOL2 = '\r\n\r\n';
const UPGRADE = [
  'HTTP/1.1 101 Switching Protocols',
  'Upgrade: websocket',
  'Connection: Upgrade',
  'Sec-WebSocket-Accept: ',
].join(EOL);

const hasToken = (value, token) => !!value && value.toLowerCase().includes(token);

const writeResponse = (socket, headerLines) => {
  socket.cork();
  socket.write(headerLines.join(EOL));
  socket.write(EOL2);
  socket.uncork();
};

const sendUpgrade = (socket, accept, extraHeaders = []) => {
  socket.cork();
  socket.write(UPGRADE);
  socket.write(accept);
  for (const line of extraHeaders) socket.write(EOL + line);
  socket.write(EOL2);
  socket.uncork();
};

const abort = (socket, code, message, { extraHeaders = [] } = {}) => {
  const lines = [`HTTP/1.1 ${code} ${message}`, 'Connection: close', ...extraHeaders];
  writeResponse(socket, lines);
  socket.destroy();
};

const isValidSecWebSocketKey = (key) =>
  typeof key === 'string' && key.length === 24 && Buffer.from(key, 'base64').length === 16;

const getPathname = (url) => (url ? url.split('?')[0] : '/');

class WebsocketServer extends EventEmitter {
  #options;
  #connections = new Set();
  #heartbeats = new Map(); // { awaiting: boolean }
  #pingTimer;
  #closed = false;

  // `server` is optional: without it nothing is bound and upgrades are
  // driven manually through handleUpgrade(req, socket, head) — that is how
  // middleware adapters (express) hook their own 'upgrade' listener.
  constructor({ server, ...opts } = {}) {
    super();
    if (server !== undefined && (!server || typeof server.on !== 'function')) {
      throw new TypeError('WebsocketServer: options.server must be an http.Server');
    }
    this.#options = {
      pingInterval: PING_INTERVAL,
      ...opts,
    };
    this.#startHeartbeat();
    if (server) this.#bind(server);
  }

  // Snapshot of the live connections (mutations do not affect the server)
  get connections() {
    return new Set(this.#connections);
  }

  // Drives one upgrade by hand. Same guarantees as the bound path: the raw
  // socket gets an error handler before parsing, and a throwing handshake
  // answers 500 instead of leaving the socket dangling.
  handleUpgrade(req, socket, head) {
    socket.on('error', () => {
      socket.destroy();
    });
    try {
      this.#handleUpgrade(req, socket, head);
    } catch (error) {
      this.emit('error', error);
      abort(socket, 500, 'Internal Server Error');
    }
  }

  #startHeartbeat() {
    const { pingInterval } = this.#options;
    this.#pingTimer = setInterval(() => {
      for (const ws of this.#connections) {
        // A paused connection cannot read pongs — skip it instead of
        // terminating a healthy peer that is merely applying backpressure.
        if (ws.isPaused) continue;
        const heartbeat = this.#heartbeats.get(ws);
        if (!heartbeat || heartbeat.awaiting) {
          ws.terminate();
          this.#connections.delete(ws);
          this.#heartbeats.delete(ws);
          continue;
        }
        heartbeat.awaiting = true;
        ws.sendPing();
      }
    }, pingInterval);
    this.#pingTimer.unref();
  }

  #bind(server) {
    server.on('upgrade', (req, socket, head) => {
      this.handleUpgrade(req, socket, head);
    });
    server.on('error', (error) => {
      // Forward error to WebsocketServer if:
      // 1) WebsocketServer has its own 'error' listeners; or
      // 2) The underlying server has no other 'error' listeners.
      const wsHasListeners = this.listenerCount('error') > 0;
      const httpHasOtherListeners = server.listenerCount('error') > 1;
      if (wsHasListeners || !httpHasOtherListeners) {
        this.emit('error', error);
      }
    });
    server.on('close', () => {
      this.close();
    });
  }

  close({ code = 1001, reason = 'Server is closing' } = {}) {
    if (this.#closed) return;
    this.#closed = true;
    clearInterval(this.#pingTimer);
    for (const ws of this.#connections) {
      ws.sendClose(code, reason);
    }
    this.#connections.clear();
    this.#heartbeats.clear();
    this.emit('close');
  }

  #negotiateProtocol(req, socket) {
    const header = req.headers['sec-websocket-protocol'];
    if (!header) return { protocol: '' };
    const offered = header
      .split(',')
      .map((token) => token.trim())
      .filter(Boolean);
    const { protocols, handleProtocols } = this.#options;
    if (handleProtocols) {
      const selected = handleProtocols(offered, req);
      if (selected === false) {
        abort(socket, 400, 'Subprotocol negotiation failed');
        return null;
      }
      return { protocol: selected || '' };
    }
    if (protocols) {
      const selected = offered.find((name) => protocols.includes(name));
      return { protocol: selected ?? '' };
    }
    return { protocol: '' };
  }

  #handleUpgrade(req, socket, head) {
    if (this.#closed) {
      return void abort(socket, 503, 'Service Unavailable');
    }
    const { path, verifyClient, perMessageDeflate } = this.#options;
    const pathname = getPathname(req.url);
    if (path !== undefined && pathname !== path) {
      return void abort(socket, 404, 'Not Found');
    }
    if (verifyClient) {
      const accepted = verifyClient({ req, socket, head });
      if (!accepted) return void abort(socket, 403, 'Forbidden');
    }
    // Enforce HTTP/1.1 per RFC 6455
    if (req.httpVersion !== '1.1') {
      return void abort(socket, 505, 'HTTP Version Not Supported');
    }
    // HTTP/1.1 requires Host header (RFC 7230 / RFC 9110)
    if (!req.headers.host) {
      return void abort(socket, 400, 'Missing Host header');
    }
    if (req.method !== 'GET') {
      return void abort(socket, 405, 'Method Not Allowed');
    }
    const upgrade = req.headers['upgrade'];
    if (!upgrade || upgrade.toLowerCase() !== 'websocket') {
      return void abort(socket, 400, 'Invalid Upgrade header');
    }
    if (!hasToken(req.headers['connection'], 'upgrade')) {
      return void abort(socket, 400, 'Invalid Connection header');
    }
    const version = req.headers['sec-websocket-version'];
    if (version !== '13') {
      const options = { extraHeaders: ['Sec-WebSocket-Version: 13'] };
      return void abort(socket, 426, 'Upgrade Required', options);
    }
    const key = req.headers['sec-websocket-key'];
    if (!key) return void abort(socket, 400, 'Missing Sec-WebSocket-Key');
    if (!isValidSecWebSocketKey(key)) {
      return void abort(socket, 400, 'Invalid Sec-WebSocket-Key');
    }

    const negotiated = this.#negotiateProtocol(req, socket);
    if (!negotiated) return;
    const { protocol } = negotiated;

    let deflate = null;
    if (perMessageDeflate) {
      const deflateOptions = perMessageDeflate === true ? {} : perMessageDeflate;
      deflate = permessageDeflate.negotiate(req.headers['sec-websocket-extensions'], deflateOptions);
      if (deflate && deflate.malformed) {
        return void abort(socket, 400, 'Invalid Sec-WebSocket-Extensions header');
      }
    }

    const extraHeaders = [];
    if (protocol) extraHeaders.push(`Sec-WebSocket-Protocol: ${protocol}`);
    if (deflate) extraHeaders.push(`Sec-WebSocket-Extensions: ${deflate.response}`);

    const accept = crypto.createHash('sha1').update(key).update(MAGIC).digest('base64');
    sendUpgrade(socket, accept, extraHeaders);

    const ws = new Connection(socket, head, {
      ...this.#options,
      isClient: false,
      protocol,
      deflate,
    });
    this.#setupHeartbeat(ws);
    this.emit('connection', ws, req);
  }

  #setupHeartbeat(ws) {
    this.#heartbeats.set(ws, { awaiting: false });
    this.#connections.add(ws);
    ws.on('pong', () => {
      const heartbeat = this.#heartbeats.get(ws);
      if (!heartbeat) return;
      heartbeat.awaiting = false;
    });
    ws.on('error', () => {
      this.#connections.delete(ws);
      this.#heartbeats.delete(ws);
    });
    ws.on('close', () => {
      this.#connections.delete(ws);
      this.#heartbeats.delete(ws);
    });
  }
}

module.exports = { WebsocketServer, MAGIC };
