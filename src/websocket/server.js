'use strict';

const crypto = require('node:crypto');
const { createLoggerWriter } = require('../logging.js');
const { EventEmitter } = require('node:events');

const { Connection, turnBudgetOf } = require('./connection.js');
const permessageDeflate = require('./permessageDeflate.js');

// The protocol revision marker (see docs/reference/protocol.md#versioning).
// A wrpc client OFFERS it; with no app-configured protocols the server
// echoes it back, which is what stamps the wire with a version both sides
// can rely on. A peer that offers nothing gets no subprotocol and speaks
// 1.0 — additive, nothing breaks. Re-exported below, as it always was.
const { getPathname, gateVerdict, negotiateProtocol } = require('./upgrade.js');
const { ConfigError } = require('../errors.js');

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
  for (let i = 0; i < extraHeaders.length; i++) socket.write(EOL + extraHeaders[i]);
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

class WebsocketServer extends EventEmitter {
  #options;
  #log;
  #connections = new Set();
  #heartbeats = new Map(); // { awaiting: boolean }
  #pingTimer;
  #closed = false;
  #saidAsyncGate = false;

  // `server` is optional: without it nothing is bound and upgrades are
  // driven manually through handleUpgrade(req, socket, head) — that is how
  // middleware adapters (express) hook their own 'upgrade' listener.
  constructor({ server, logger = globalThis.console, ...opts } = {}) {
    super();
    if (server !== undefined && (!server || typeof server.on !== 'function')) {
      throw new ConfigError('WebsocketServer: options.server must be an http.Server');
    }
    permessageDeflate.assertDeflateOptions(opts.perMessageDeflate);
    // Normalized once here, childed per connection below. The `Server` shell
    // passes its own writer down, and re-wrapping a writer is free, so the
    // common path allocates nothing extra.
    this.#log = createLoggerWriter(logger).child({ component: 'ws' });
    this.#options = {
      pingInterval: PING_INTERVAL,
      // Server connections coalesce the writes of one event-loop turn into
      // one flush (Connection's `coalesce`); `coalesce: false` opts out.
      coalesce: true,
      // And they share the event loop: what one of them hands over in one
      // turn is bounded (Connection's `turnBudget`); `turnBudget: false`
      // opts out, `{ messages, bytes }` sets the marks.
      turnBudget: true,
      ...opts,
    };
    // Refused here, not at the first connection it would be applied to.
    turnBudgetOf(this.#options.turnBudget);
    this.#startHeartbeat();
    if (server) this.#bind(server);
  }

  // Snapshot of the live connections (mutations do not affect the server)
  get connections() {
    return new Set(this.#connections);
  }

  // Drives one upgrade by hand. Same guarantees as the bound path: the raw
  // socket gets an error handler before parsing, and a throwing handshake
  // answers 500 instead of leaving the socket dangling. The error is said in
  // the log and emitted only to a listener: an 'error' with none throws, and
  // the caller here is an http server's 'upgrade' listener — none of the
  // shells puts a catch around it, so that throw ended the process.
  handleUpgrade(req, socket, head) {
    socket.on('error', () => {
      socket.destroy();
    });
    try {
      this.#handleUpgrade(req, socket, head);
    } catch (error) {
      this.#log.error({ err: error, event: 'ws.upgrade' });
      if (this.listenerCount('error') > 0) this.emit('error', error);
      abort(socket, 500, 'Internal Server Error');
    }
  }

  // An upgrade that was not accepted, said before it is answered. The
  // handshake is where "it just does not connect" starts, and a refusal used
  // to leave nothing behind: no line and no count, where the HTTP path says
  // `cors.refused` and `http.refused` for the same request. 403 is the
  // application's gate — `verifyClient`, which is the origin allowlist under
  // the shells — and reads like `cors.refused` does; the rest is a handshake
  // no WebSocket client sends, which a scanner produces by the thousand.
  #refuse(req, socket, status, reason, options) {
    const pathname = getPathname(req.url);
    const entry = {
      event: 'ws.refused',
      status,
      reason,
      path: pathname.length > 128 ? pathname.slice(0, 128) : pathname,
    };
    if (status === 403) this.#log.warn(entry);
    else this.#log.debug(entry);
    abort(socket, status, reason, options);
  }

  // Once per server: every upgrade is refused while the gate stays async, and
  // a line per refusal is already `ws.refused`.
  #sayAsyncGate() {
    if (this.#saidAsyncGate) return;
    this.#saidAsyncGate = true;
    this.#log.warn({ event: 'ws.gate.async' }, 'verifyClient returned a promise: it must answer true synchronously');
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
          // Debug: a peer that went away without a Close — a laptop lid, a
          // phone in a tunnel — is the ordinary end of a connection, and a
          // fleet of them is not a warning. But it is the one end the peer's
          // own Close never explains, so it is said somewhere.
          this.#log.debug({ event: 'ws.heartbeat', peer: ws.remoteAddress ?? '' });
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

  // The subprotocol this upgrade is answered with (negotiateProtocol in
  // ./upgrade.js, shared with the uWebSockets.js engine), or null once the
  // application's handleProtocols refused the handshake.
  #negotiateProtocol(req, socket) {
    const protocol = negotiateProtocol(req.headers['sec-websocket-protocol'], this.#options, req);
    if (protocol === false) {
      this.#refuse(req, socket, 400, 'Subprotocol negotiation failed');
      return null;
    }
    return { protocol };
  }

  #handleUpgrade(req, socket, head) {
    if (this.#closed) {
      return void this.#refuse(req, socket, 503, 'Service Unavailable');
    }
    const { path, verifyClient, perMessageDeflate } = this.#options;
    const pathname = getPathname(req.url);
    if (path !== undefined && pathname !== path) {
      return void this.#refuse(req, socket, 404, 'Not Found');
    }
    if (verifyClient) {
      const verdict = gateVerdict(verifyClient, { req, socket, head });
      if (verdict === 'async') this.#sayAsyncGate();
      if (verdict !== 'admit') return void this.#refuse(req, socket, 403, 'Forbidden');
    }
    // Enforce HTTP/1.1 per RFC 6455
    if (req.httpVersion !== '1.1') {
      return void this.#refuse(req, socket, 505, 'HTTP Version Not Supported');
    }
    // HTTP/1.1 requires Host header (RFC 7230 / RFC 9110)
    if (!req.headers.host) {
      return void this.#refuse(req, socket, 400, 'Missing Host header');
    }
    if (req.method !== 'GET') {
      return void this.#refuse(req, socket, 405, 'Method Not Allowed');
    }
    const upgrade = req.headers['upgrade'];
    if (!upgrade || upgrade.toLowerCase() !== 'websocket') {
      return void this.#refuse(req, socket, 400, 'Invalid Upgrade header');
    }
    if (!hasToken(req.headers['connection'], 'upgrade')) {
      return void this.#refuse(req, socket, 400, 'Invalid Connection header');
    }
    const version = req.headers['sec-websocket-version'];
    if (version !== '13') {
      const options = { extraHeaders: ['Sec-WebSocket-Version: 13'] };
      return void this.#refuse(req, socket, 426, 'Upgrade Required', options);
    }
    const key = req.headers['sec-websocket-key'];
    if (!key) return void this.#refuse(req, socket, 400, 'Missing Sec-WebSocket-Key');
    if (!isValidSecWebSocketKey(key)) {
      return void this.#refuse(req, socket, 400, 'Invalid Sec-WebSocket-Key');
    }

    const negotiated = this.#negotiateProtocol(req, socket);
    if (!negotiated) return;
    const { protocol } = negotiated;

    let deflate = null;
    if (perMessageDeflate) {
      const deflateOptions = perMessageDeflate === true ? {} : perMessageDeflate;
      deflate = permessageDeflate.negotiate(req.headers['sec-websocket-extensions'], deflateOptions);
      if (deflate && deflate.malformed) {
        return void this.#refuse(req, socket, 400, 'Invalid Sec-WebSocket-Extensions header');
      }
      // Per-connection selection: `filter(req)` decides whether THIS peer
      // gets compression at all — a browser on a slow link yes, a service
      // in the same datacenter no — by declining the offer, so the peer
      // learns it from the handshake rather than from a missing RSV1.
      // oxlint-disable-next-line no-restricted-properties -- the perMessageDeflate option, not Array#filter
      if (deflate && typeof deflateOptions.filter === 'function' && !deflateOptions.filter(req)) deflate = null;
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
      logger: this.#log.child({ peer: socket.remoteAddress ?? '' }),
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

module.exports = { MAGIC, WebsocketServer };
