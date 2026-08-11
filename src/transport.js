'use strict';

const http = require('node:http');

const { Emitter } = require('./utils.js');

// RFC 6265 permits '=' inside cookie values (base64, JWT) — split each
// pair on the FIRST '=' only, or the value gets silently truncated.
const parseCookies = (cookie) => {
  const values = [];
  const items = cookie.split(';');
  for (const item of items) {
    const eq = item.indexOf('=');
    const key = (eq < 0 ? item : item.slice(0, eq)).trim();
    const val = eq < 0 ? '' : item.slice(eq + 1).trim();
    values.push([key, val]);
  }
  return Object.fromEntries(values);
};

const SECURITY_HEADERS = {
  'X-Content-Type-Options': 'nosniff',
  'Strict-Transport-Security': 'max-age=31536000; includeSubdomains; preload',
  'Content-Type': 'application/json',
};

const DEFAULT_CORS_METHODS = 'POST, GET, OPTIONS';
// `x-wrpc-channel` (which event stream a POST belongs to) and `last-event-id`
// (the SSE resume header) are not CORS-safelisted, so a cross-origin peer's
// preflight fails before the request is ever sent unless they are named here.
const DEFAULT_CORS_HEADERS = 'Content-Type, x-wrpc-channel, last-event-id';

// CORS v2: `cors` is { origins: string[] | (origin) => boolean, credentials?,
// headers?, methods? }. Without a `cors` option every origin is allowed
// (wildcard, credentials-less) — the pre-F2 behavior. With `origins`, the
// request origin is echoed back only when allowed, plus `Vary: Origin`.
const buildHeaders = (cors, origin) => {
  const headers = {
    ...SECURITY_HEADERS,
    'Access-Control-Allow-Methods': cors?.methods ?? DEFAULT_CORS_METHODS,
    'Access-Control-Allow-Headers': cors?.headers ?? DEFAULT_CORS_HEADERS,
  };
  if (!cors || !cors.origins) {
    headers['Access-Control-Allow-Origin'] = '*';
    return headers;
  }
  headers['Vary'] = 'Origin';
  const allowed =
    typeof cors.origins === 'function' ? Boolean(origin && cors.origins(origin)) : cors.origins.includes(origin);
  if (allowed) {
    headers['Access-Control-Allow-Origin'] = origin;
    if (cors.credentials) headers['Access-Control-Allow-Credentials'] = 'true';
  }
  return headers;
};

const isOriginAllowed = (cors, origin) => {
  if (!cors || !cors.origins) return true;
  if (!origin) return true; // non-browser peers send no Origin header
  if (typeof cors.origins === 'function') return Boolean(cors.origins(origin));
  return cors.origins.includes(origin);
};

class ServerTransport extends Emitter {
  constructor(source) {
    // No listener cap: transports are fan-out points — every backpressured
    // outbound stream on the connection parks a once('drain'|'close')
    // listener here, and the default cap of 10 would throw on the 11th
    // concurrently stalled stream.
    super({ maxListeners: Number.MAX_SAFE_INTEGER });
    this.source = source;
  }

  error(code = 500, { id = '', error = null } = {}) {
    const status = http.STATUS_CODES[code] || 'Unknown error';
    const info = error ? error.message : status;
    const packet = { type: 'callback', id, error: { message: info, code } };
    return this.send(packet, code);
  }

  // Returns the transport's backpressure signal (false = above the
  // high-water mark) so a producer — a subscription pump, a stream — can
  // wait for 'drain' instead of buffering without limit.
  send(obj, code = 200) {
    return this.write(JSON.stringify(obj), code);
  }
}

// Net-free HTTP transport over an abstract call description:
// { method, url, headers, body?, remoteAddress?, respond({ status, headers, body }) }.
// The node Server shell and the framework adapters both speak this shape.
class ServerHttpTransport extends ServerTransport {
  #respond;
  #responded = false;
  #setCookies = [];
  #batch = null; // requested ids, in order — null outside batch mode
  #collected = [];

  constructor(call, options = {}) {
    super(call.remoteAddress ?? '');
    this.call = call;
    this.headers = options.headers ?? { ...SECURITY_HEADERS };
    this.#respond = call.respond;
    if (Array.isArray(options.batch)) this.#batch = options.batch;
  }

  get responded() {
    return this.#responded;
  }

  get batched() {
    return this.#batch !== null;
  }

  // A batch frame gets ONE response carrying every answer. They are held
  // until the last one arrives and then emitted in the order the packets
  // were sent, so a caller can zip requests to responses positionally
  // without depending on how fast each handler happened to be.
  send(obj, code = 200) {
    if (!this.#batch) return super.send(obj, code);
    if (this.#responded) return true;
    this.#collected.push(obj);
    if (this.#collected.length < this.#batch.length) return true;
    return this.write(JSON.stringify(this.#ordered()), 200);
  }

  #ordered() {
    const pending = this.#collected.slice();
    const answers = [];
    for (const id of this.#batch) {
      const index = pending.findIndex((packet) => packet.id === id);
      if (index < 0) continue;
      answers.push(...pending.splice(index, 1));
    }
    // Anything with no matching id (a structure error carries an empty one)
    // still has to be reported, so it goes at the end rather than nowhere.
    answers.push(...pending);
    return answers;
  }

  write(data, httpCode = 200) {
    if (this.#responded) return true;
    this.#responded = true;
    const body = Buffer.isBuffer(data) ? data : Buffer.from(data);
    const headers = { ...this.headers, 'Content-Length': body.length };
    if (this.#setCookies.length > 0) headers['Set-Cookie'] = this.#setCookies;
    this.#respond({ status: httpCode, headers, body });
    this.emit('close');
    return true;
  }

  getCookies() {
    const { cookie } = this.call.headers;
    if (!cookie) return {};
    return parseCookies(cookie);
  }

  sendSessionCookie(cookieHeader) {
    this.#setCookies.push(cookieHeader);
  }

  // Closed before every answer arrived — a shutdown, an evicted client. In
  // batch mode `error()` would only collect ONE more answer and then keep
  // waiting for the rest, which are never coming, so the request hangs. Fill
  // every still-unanswered slot with its own error packet instead: an
  // id-less packet is one the caller cannot route back to a pending call.
  close() {
    if (this.#responded) return;
    if (!this.#batch) return void this.error(503);
    const message = http.STATUS_CODES[503];
    const answered = new Set(this.#collected.map((packet) => packet.id));
    for (const id of this.#batch) {
      if (answered.has(id)) continue;
      answered.add(id);
      this.#collected.push({ type: 'callback', id: typeof id === 'string' ? id : '', error: { message, code: 503 } });
    }
    this.write(JSON.stringify(this.#ordered()), 503);
  }
}

class ServerWsTransport extends ServerTransport {
  constructor(connection, meta = {}) {
    super(meta.remoteAddress ?? connection.remoteAddress ?? '');
    this.connection = connection;
    connection.on('close', () => void this.emit('close'));
    connection.on('drain', () => void this.emit('drain'));
  }

  write(data) {
    if (typeof data !== 'string' && !Buffer.isBuffer(data)) {
      data = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
    }
    return this.connection.send(data);
  }

  close() {
    this.connection.terminate();
  }
}

class ServerEventTransport extends ServerTransport {
  constructor(port) {
    super('event transport');
    this.port = port;
    port.on('close', () => void this.emit('close'));
  }

  write(data) {
    this.port.postMessage(data);
    return true; // a MessagePort has no backpressure to report
  }

  close() {
    this.port.close();
  }
}

ServerTransport.transport = {
  http: ServerHttpTransport,
  ws: ServerWsTransport,
  event: ServerEventTransport,
};

module.exports = {
  ServerTransport,
  buildHeaders,
  isOriginAllowed,
  parseCookies,
  SECURITY_HEADERS,
};
