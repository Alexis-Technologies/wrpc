'use strict';

const http = require('node:http');

const { Emitter, toKebab } = require('./utils.js');

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
// `x-wrpc-channel` (which event stream a POST belongs to), `last-event-id`
// (the SSE resume header) and `x-wrpc-meta` (per-request client metadata)
// are not CORS-safelisted, so a cross-origin peer's preflight fails before
// the request is ever sent unless they are named here. An application whose
// clients declare CUSTOM connection headers over http/sse must widen the
// list via `cors.headers` the same way.
const DEFAULT_CORS_HEADERS = 'Content-Type, x-wrpc-channel, last-event-id, x-wrpc-meta';

const CORS_META_PREFIX = 'x-wrpc-meta-';
// `headers` accepts an array purely so composing a list stays readable; the
// string form is what the header value has always been and is untouched.
const corsHeaderList = (value) => (Array.isArray(value) ? value.join(', ') : value);
// The per-key meta spelling (metaFormat: 'prefixed') sends one real request
// header per key, and CORS has no wildcard for header names — so every key a
// cross-origin client will send has to be named. Run through toKebab so
// `metaHeaders: ['userId']` grants `x-wrpc-meta-user-id`: the name the client
// actually sends, not the one the config happened to spell.
const allowedHeaders = (cors) => {
  const declared = corsHeaderList(cors?.headers) ?? DEFAULT_CORS_HEADERS;
  const meta = cors?.metaHeaders;
  if (!meta || meta.length === 0) return declared;
  let allow = declared;
  for (let i = 0; i < meta.length; i++) allow += `, ${CORS_META_PREFIX}${toKebab(meta[i])}`;
  return allow;
};

// CORS v2: `cors` is { origins: string[] | (origin) => boolean, credentials?,
// headers?, metaHeaders?, methods? }. Without a `cors` option every origin is allowed
// (wildcard, credentials-less) — the pre-F2 behavior. With `origins`, the
// request origin is echoed back only when allowed, plus `Vary: Origin`.
const buildHeaders = (cors, origin) => {
  const headers = {
    ...SECURITY_HEADERS,
    'Access-Control-Allow-Methods': cors?.methods ?? DEFAULT_CORS_METHODS,
    'Access-Control-Allow-Headers': allowedHeaders(cors),
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

// What the peer is told. 4xx messages are written for the caller
// (validation, quotas, refusals) and travel as-is; a 5xx message is a server
// internal — an uncaught exception's text can carry paths, queries or stack
// fragments — so the peer gets the status line and the details stay in the
// server log, unless the error opts in with `expose = true` (which the
// router's own coded errors do: their messages are part of the protocol).
// The packet id is the correlation: the same id is on the server log line.
const publicErrorMessage = (code, error) => {
  const status = http.STATUS_CODES[code] || 'Unknown error';
  if (!error) return status;
  if (code < 500 || error.expose === true) return error.message;
  return status;
};

// `details` follows the exact same rule as the message: structured issue
// lists (validation paths, quota numbers) are part of the 4xx conversation,
// while a 5xx's internals stay in the log unless the error opts in.
const publicErrorDetails = (code, error) => {
  if (!error || error.details === undefined) return undefined;
  if (code < 500 || error.expose === true) return error.details;
  return undefined;
};

// The one builder for the wire error object, so every packet that carries
// an error ({type:'callback'} and {type:'end'} alike) redacts identically.
// The `details` key is omitted entirely when there is nothing to say —
// an optional field, absent rather than null, per the protocol's
// additive-fields rule.
const wireError = (code, error) => {
  const wire = { message: publicErrorMessage(code, error), code };
  const details = publicErrorDetails(code, error);
  if (details !== undefined) wire.details = details;
  return wire;
};

class ServerTransport extends Emitter {
  // Which wire this is, for log entries and metric attributes. Subclasses
  // override it; the base value covers a transport nobody labelled.
  kind = 'unknown';

  constructor(source) {
    // No listener cap: transports are fan-out points — every backpressured
    // outbound stream on the connection parks a once('drain'|'close')
    // listener here, and the default cap of 10 would throw on the 11th
    // concurrently stalled stream.
    super({ maxListeners: Number.MAX_SAFE_INTEGER });
    this.source = source;
  }

  error(code = 500, { id = '', error = null } = {}) {
    const packet = { type: 'callback', id, error: wireError(code, error) };
    return this.send(packet, code);
  }

  // Returns the transport's backpressure signal (false = above the
  // high-water mark) so a producer — a subscription pump, a stream — can
  // wait for 'drain' instead of buffering without limit.
  //
  // `text` is the already-serialized form of `obj` when the dispatcher's
  // compiled-serializer fast path built one (see handleRpc); passing both
  // keeps the object available to the overrides that need it (batch
  // collection, REST unwrapping) while the plain path skips a stringify.
  send(obj, code = 200, text = null) {
    // An injected codec (RpcServer options.codec, assigned per transport)
    // re-frames every packet; it wins over precompiled `text` by
    // construction — the server refuses codec + serializers up front.
    if (this.codec) return this.write(this.codec.encode(obj), code);
    return this.write(text ?? JSON.stringify(obj), code);
  }
}

// Net-free HTTP transport over an abstract call description:
// { method, url, headers, body?, remoteAddress?, respond({ status, headers, body }) }.
// The node Server shell and the framework adapters both speak this shape.
class ServerHttpTransport extends ServerTransport {
  kind = 'http';

  #respond;
  #responded = false;
  #setCookies = [];
  #batch = null; // requested ids, in order — null outside batch mode
  #collected = [];
  // Declarative-route mode: the response body is the PLAIN result (or the
  // wire error object), not a callback envelope — external REST semantics.
  // `{ status }` carries the route's success status; null everywhere else.
  #rest = null;

  constructor(call, options = {}) {
    super(call.remoteAddress ?? '');
    this.call = call;
    this.headers = options.headers ?? { ...SECURITY_HEADERS };
    this.#respond = call.respond;
    if (Array.isArray(options.batch)) this.#batch = options.batch;
    if (options.rest) this.#rest = options.rest;
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
  // Both special modes ignore `text` deliberately: batch mode collects the
  // OBJECTS to re-order and re-serialize as one frame, and REST mode writes
  // the bare result rather than the callback envelope the text carries.
  send(obj, code = 200, text = null) {
    if (this.#rest && obj.type === 'callback') {
      // The REST body codec (codec.rest) encodes VALUES — the plain result
      // and the wire error object — never envelopes. Absent, JSON as ever.
      const codec = this.#rest.codec ?? null;
      if (obj.error) {
        return this.write(codec ? codec.encode(obj.error) : JSON.stringify(obj.error), obj.error.code ?? code);
      }
      const status = this.#rest.status ?? 200;
      // 204 promises "no content": the result (if any) is discarded on the
      // wire by contract, not by accident.
      if (status === 204) return this.write('', 204);
      // An undefined result travels as an encoded `null` — one documented
      // behaviour with and without a codec.
      if (obj.result === undefined) return this.write(codec ? codec.encode(null) : 'null', status);
      return this.write(codec ? codec.encode(obj.result) : JSON.stringify(obj.result), status);
    }
    if (!this.#batch) return super.send(obj, code, text);
    if (this.#responded) return true;
    this.#collected.push(obj);
    if (this.#collected.length < this.#batch.length) return true;
    const ordered = this.#ordered();
    return this.write(this.codec ? this.codec.encode(ordered) : JSON.stringify(ordered), 200);
  }

  // Building an id index makes this O(n), but the Map costs more than the
  // quadratic it removes until the batch gets big: the linear scan wins 1.31x
  // at 4 answers and 1.08x at 8, and loses 1.20x at 16 and 3.92x at 128.
  // Re-run bench/batch-ordering.js before moving this.
  static #INDEX_THRESHOLD = 12;

  #ordered() {
    const collected = this.#collected;
    if (collected.length >= ServerHttpTransport.#INDEX_THRESHOLD) return this.#orderedIndexed();
    const pending = collected.slice();
    const answers = [];
    for (const id of this.#batch) {
      const index = pending.findIndex((packet) => packet.id === id);
      if (index < 0) continue;
      answers.push(pending[index]);
      pending.splice(index, 1);
    }
    // Anything with no matching id (a structure error carries an empty one)
    // still has to be reported, so it goes at the end rather than nowhere.
    for (let i = 0; i < pending.length; i++) answers.push(pending[i]);
    return answers;
  }

  // Same output as the scan above, including the two behaviours that are easy
  // to lose: ids repeated within one batch consume one collected answer each,
  // and unmatched answers trail in COLLECTION order, not index order.
  #orderedIndexed() {
    const collected = this.#collected;
    const byId = new Map();
    for (let i = 0; i < collected.length; i++) {
      const { id } = collected[i];
      const bucket = byId.get(id);
      if (bucket === undefined) byId.set(id, [i]);
      else bucket.push(i);
    }
    const answers = [];
    const taken = new Uint8Array(collected.length);
    for (const id of this.#batch) {
      const bucket = byId.get(id);
      if (bucket === undefined || bucket.length === 0) continue;
      const index = bucket.shift();
      taken[index] = 1;
      answers.push(collected[index]);
    }
    for (let i = 0; i < collected.length; i++) if (taken[i] === 0) answers.push(collected[i]);
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

  // Cookies a handler queued (startSession) that no write() will flush —
  // a host-delegated route copies these onto its own reply.
  get pendingCookies() {
    return this.#setCookies;
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
    const answered = new Set();
    for (let i = 0; i < this.#collected.length; i++) answered.add(this.#collected[i].id);
    for (const id of this.#batch) {
      if (answered.has(id)) continue;
      answered.add(id);
      this.#collected.push({ type: 'callback', id: typeof id === 'string' ? id : '', error: { message, code: 503 } });
    }
    const ordered = this.#ordered();
    this.write(this.codec ? this.codec.encode(ordered) : JSON.stringify(ordered), 503);
  }
}

class ServerWsTransport extends ServerTransport {
  kind = 'ws';

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

  // A graceful goodbye: the peer gets a close frame (1001 "going away") and
  // the chance to finish the handshake; the engine's closeTimeout destroys
  // one that never answers. Hard teardown stays spelled terminate().
  close() {
    if (typeof this.connection.close === 'function') {
      this.connection.close(1001, 'Server is closing');
    } else {
      this.connection.terminate();
    }
  }
}

class ServerEventTransport extends ServerTransport {
  kind = 'event';

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
  publicErrorMessage,
  publicErrorDetails,
  wireError,
  SECURITY_HEADERS,
};
