'use strict';

const crypto = require('node:crypto');

const { toKebab } = require('./utils.js');
const { cacheHeadersFor, RESERVED_HEADERS, checkHeader } = require('./rpc/rest.js');
const { STATUS_CODES } = require('./status.js');
const { publicErrorMessage, publicErrorDetails, wireError } = require('./rpc/errors.js');
const { ServerTransport } = require('./rpc/server-transport.js');
const { META_HEADER, META_PREFIX, CHANNEL_HEADER } = require('./wire.js');
const { chooseEncoding, markEncoded } = require('./content-encoding.js');
const { isPromise } = require('./compression/ids.js');
const { hasBytes, encodeAttachments, isAttachmentsFrame } = require('./attachments.js');
const { ArgumentError } = require('./errors.js');

// HSTS for the host that answered, and no more than that: this used to say
// `includeSubdomains; preload` on every response — a commitment for the whole
// domain tree, and consent to a browser preload list that takes months to
// leave, made by a library on behalf of whoever owns the domain. That
// decision is theirs: `http.hsts` takes the header they want, or `false`.
const HSTS = 'Strict-Transport-Security';
const SECURITY_HEADERS = {
  'X-Content-Type-Options': 'nosniff',
  [HSTS]: 'max-age=31536000',
  'Content-Type': 'application/json',
  // The HTTP side's version marker, on every response — the ws subprotocol
  // ladder's counterpart (protocol.md#versioning): the newest revision the
  // server speaks. 1 here, which is what 1.0 answers; a server that reads
  // framed messages answers 2 through buildHeaders, and that is how an HTTP
  // client learns it may send one.
  'wrpc-version': '1',
};

// What an HTTP client that reads framed messages asks for, and what a
// server looks for before it answers with one. `Accept` rather than a
// header of wrpc's own: it is CORS-safelisted, so it costs no preflight a
// 1.0 server — or an application's own `cors.headers` list, which REPLACES
// the default — would refuse.
// Building an id index makes a batch's ordering O(n), but the Map costs
// more than the quadratic it removes until the batch gets big: the linear
// scan wins 1.23x at 4 answers and 1.05x at 8, and loses 1.18x at 16 and
// 3.91x at 128 (docs/guide/performance.md) — so the switch sits between 8
// and 16; the client's default batch.maxSize of 16 is the first measured
// size where the index wins. Re-run bench/batch-ordering.js before moving
// either.
const INDEX_THRESHOLD = 12;
const FRAME_TYPE = 'application/octet-stream';

const DEFAULT_CORS_METHODS = 'POST, GET, OPTIONS';
// `x-wrpc-channel` (which event stream a POST belongs to), `last-event-id`
// (the SSE resume header) and `x-wrpc-meta` (per-request client metadata)
// are not CORS-safelisted, so a cross-origin peer's preflight fails before
// the request is ever sent unless they are named here. An application whose
// clients declare CUSTOM connection headers over http/sse must widen the
// list via `cors.headers` the same way.
const DEFAULT_CORS_HEADERS = `Content-Type, ${CHANNEL_HEADER}, last-event-id, ${META_HEADER}`;

const CORS_META_PREFIX = META_PREFIX;
// The per-key meta spelling (metaFormat: 'prefixed') sends one real request
// header per key, and CORS has no wildcard for header names — so every key a
// cross-origin client will send has to be named. Run through toKebab so
// `metaHeaders: ['userId']` grants `x-wrpc-meta-user-id`: the name the client
// actually sends, not the one the config happened to spell.
//
// Memoized on the cors OBJECT: the string derives only from the immutable
// option (assigned once in RpcServer's constructor), yet buildHeaders runs
// on every HTTP call — preflight, packet POST, REST, SSE — and the toKebab
// regex passes made it cost ~1.9 µs/request with six camelCase metaHeaders,
// against ~20 ns for the whole buildHeaders once memoized
// (bench/cors-headers.js). Only the STRING is cached;
// the headers object stays per-request (it carries per-origin fields and is
// mutated by callers).
const ALLOW_CACHE = new WeakMap();

// RFC 6265 permits '=' inside cookie values (base64, JWT) — split each
// pair on the FIRST '=' only, or the value gets silently truncated.
//
// A name that comes twice keeps its FIRST value. A browser sends the cookies
// of a name most specific path first (RFC 6265 5.4), so the first one is the
// site's own; the one behind it is what a sibling subdomain set for the
// parent domain — `token=theirs; Domain=.example.com` — to have its own
// session token read in place of the user's (cookie tossing). The last one
// used to win. Read back to front, so that fromEntries — kept for the own
// `__proto__` it creates where an assignment would not — leaves the first.
const parseCookies = (cookie) => {
  const values = [];
  const items = cookie.split(';');
  for (let i = items.length - 1; i >= 0; i--) {
    const item = items[i];
    const eq = item.indexOf('=');
    const key = (eq < 0 ? item : item.slice(0, eq)).trim();
    const val = eq < 0 ? '' : item.slice(eq + 1).trim();
    values.push([key, val]);
  }
  return Object.fromEntries(values);
};

const withoutHeader = (headers, name) => {
  const result = {};
  const keys = Object.keys(headers);
  for (let i = 0; i < keys.length; i++) if (keys[i] !== name) result[keys[i]] = headers[keys[i]];
  return result;
};
const readsFrames = (headers) => {
  const accept = headers?.accept;
  return typeof accept === 'string' && accept.includes(FRAME_TYPE);
};
// `headers` accepts an array purely so composing a list stays readable; the
// string form is what the header value has always been and is untouched.
const corsHeaderList = (value) => (Array.isArray(value) ? value.join(', ') : value);
const allowedHeaders = (cors) => {
  if (!cors) return DEFAULT_CORS_HEADERS;
  const cached = ALLOW_CACHE.get(cors);
  if (cached !== undefined) return cached;
  const declared = corsHeaderList(cors.headers) ?? DEFAULT_CORS_HEADERS;
  const meta = cors.metaHeaders;
  let allow = declared;
  if (meta && meta.length > 0) {
    for (let i = 0; i < meta.length; i++) allow += `, ${CORS_META_PREFIX}${toKebab(meta[i])}`;
  }
  ALLOW_CACHE.set(cors, allow);
  return allow;
};

// CORS v2: `cors` is { origins: string[] | (origin) => boolean, credentials?,
// headers?, metaHeaders?, methods? }. Without a `cors` option every origin is allowed
// (wildcard, credentials-less) — the pre-F2 behavior. With `origins`, the
// request origin is echoed back only when allowed, plus `Vary: Origin`.
//
// `revision` is the newest protocol revision the server speaks (RpcServer's
// `revision`); the marker is exposed because script on another origin reads
// only the response headers named so.
// Whether `origin` is on the allowlist, when the allowlist is not a function.
// A string is ONE origin, compared whole. It used to reach `.includes()` as
// it was, and on a string that is a substring test: `origins:
// 'https://app.example.com'` admitted `https://app.example.co`, a domain
// anybody can register.
const listed = (origins, origin) => (typeof origins === 'string' ? origins === origin : origins.includes(origin));

// `hsts`: the `http.hsts` option — a header value of the application's, or
// false for none; left out, the default above.
// What node's own res.setHeader() throws in the same place, code included
// (ERR_HTTP_HEADERS_SENT): a handler that wrote a header too late is told
// what a host's response would have told it.
const headersSent = (what) => {
  const error = new Error(`${what}: the response was already sent`);
  error.code = 'ERR_HTTP_HEADERS_SENT';
  return error;
};

// A `cors.origins` function answers synchronously. A promise is truthy, and
// `Boolean(promise)` used to allow every origin to an `async` check: it is
// refused instead, its rejection kept from going unhandled.
const allowedBy = (verdict) => {
  const isPending = verdict !== null && typeof verdict === 'object' && typeof verdict.then === 'function';
  if (!isPending) return Boolean(verdict);
  verdict.then(undefined, () => {});
  return false;
};

const buildHeaders = (cors, origin, revision = 1, hsts = undefined) => {
  const headers = {
    ...(hsts === false ? PLAIN_HEADERS : SECURITY_HEADERS),
    'Access-Control-Allow-Methods': cors?.methods ?? DEFAULT_CORS_METHODS,
    'Access-Control-Allow-Headers': allowedHeaders(cors),
    'Access-Control-Expose-Headers': 'wrpc-version',
  };
  if (typeof hsts === 'string') headers[HSTS] = hsts;
  if (revision === 2) headers['wrpc-version'] = '2';
  if (!cors || !cors.origins) {
    headers['Access-Control-Allow-Origin'] = '*';
    return headers;
  }
  headers['Vary'] = 'Origin';
  const isFunction = typeof cors.origins === 'function';
  const allowed = isFunction ? Boolean(origin) && allowedBy(cors.origins(origin)) : listed(cors.origins, origin);
  if (allowed) {
    headers['Access-Control-Allow-Origin'] = origin;
    if (cors.credentials) headers['Access-Control-Allow-Credentials'] = 'true';
  }
  return headers;
};

const isOriginAllowed = (cors, origin) => {
  if (!cors || !cors.origins) return true;
  if (!origin) return true; // non-browser peers send no Origin header
  if (typeof cors.origins === 'function') return allowedBy(cors.origins(origin));
  return listed(cors.origins, origin);
};

// If-None-Match: a list of entity tags, weak or strong, or `*`.
const matchesEtag = (header, etag) => {
  if (typeof header !== 'string' || header.length === 0) return false;
  if (header.trim() === '*') return true;
  const bare = etag.slice(2); // past the W/
  const parts = header.split(',');
  for (let i = 0; i < parts.length; i++) {
    let tag = parts[i].trim();
    if (tag.startsWith('W/')) tag = tag.slice(2);
    if (tag === bare) return true;
  }
  return false;
};

// A REST result's body. An undefined result travels as an encoded `null` —
// one documented behaviour with and without a codec.
const restBody = (codec, result) => {
  if (codec) return codec.encode(result === undefined ? null : result);
  return result === undefined ? 'null' : JSON.stringify(result);
};

// The bytes of an answer as the host writes them.
const bodyBytes = (data) => {
  if (Buffer.isBuffer(data)) return data;
  if (typeof data === 'string') return Buffer.from(data);
  return Buffer.from(data.buffer, data.byteOffset, data.byteLength);
};

// The same without HSTS, for `http: { hsts: false }` — built once, so a
// response under that option is a spread like any other, never a `delete`.
const PLAIN_HEADERS = withoutHeader(SECURITY_HEADERS, HSTS);

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

  #status = null;
  // The normalized `http.compression` option (content-encoding.js), or null:
  // the core hands it to the transports it builds for real answers.
  #compression = null;
  #failed = null;

  constructor(call, options = {}) {
    super(call.remoteAddress ?? '');
    this.call = call;
    this.headers = options.headers ?? { ...SECURITY_HEADERS };
    this.#respond = call.respond;
    if (options.compression) this.#compression = options.compression;
    // `failed(coding, error)`: the core's reporter for an encoder that threw.
    if (options.failed) this.#failed = options.failed;
    if (Array.isArray(options.batch)) this.#batch = options.batch;
    if (options.rest) {
      this.#rest = options.rest;
      // A declared route's static response headers, applied once here so
      // every answer — result or error — carries them.
      if (options.rest.headers) Object.assign(this.headers, options.rest.headers);
    }
  }

  // The `context.http.setHeader` seam: one header onto this response,
  // refused once the answer is written or for a transport-owned name.
  setHeader(name, value) {
    if (typeof name !== 'string' || name.length === 0) throw new ArgumentError('setHeader: name must be a string');
    if (RESERVED_HEADERS.has(name.toLowerCase())) {
      throw new ArgumentError(`setHeader: '${name}' is owned by the transport`);
    }
    if (this.#responded) throw headersSent('setHeader');
    const text = String(value);
    checkHeader(name, text, 'setHeader', ArgumentError);
    // One header per name, whatever the spelling: a second call under
    // another case replaces the first rather than sending both.
    const lower = name.toLowerCase();
    for (const key in this.headers) {
      if (key !== name && key.toLowerCase() === lower) delete this.headers[key];
    }
    this.headers[name] = text;
  }

  // The `context.http.status` seam: the success status of THIS response
  // (a REST route's declared status is the default). Errors keep their code.
  setStatus(code) {
    if (!(Number.isInteger(code) && code >= 200 && code <= 599)) throw new ArgumentError('status: an integer 200-599');
    if (this.#responded) throw headersSent('status');
    this.#status = code;
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
  send(packet, code = 200, text = null) {
    if (this.#rest && packet.type === 'callback') {
      // The REST body codec (codec.rest) encodes VALUES — the plain result
      // and the wire error object — never envelopes. Absent, JSON as ever.
      const codec = this.#rest.codec ?? null;
      if (packet.error) {
        return this.write(codec ? codec.encode(packet.error) : JSON.stringify(packet.error), packet.error.code ?? code);
      }
      // A REST body is a plain value with no frame to carry bytes in: a
      // result holding them needs codec.rest, and says so instead of
      // shipping the objects JSON makes of typed arrays.
      if (codec === null && this.attachments !== false && hasBytes(packet.result)) {
        const error = { message: 'Binary results on REST need codec.rest', code: 501 };
        return this.write(JSON.stringify(error), 501);
      }
      const status = this.#status ?? this.#rest.status ?? 200;
      // 204 promises "no content": the result (if any) is discarded on the
      // wire by contract, not by accident.
      if (status === 204) return this.write('', 204);
      return this.#writeCacheable(restBody(codec, packet.result), status);
    }
    if (!this.#batch) return super.send(packet, code, text);
    if (this.#responded) return true;
    this.#collected.push(packet);
    if (this.#collected.length < this.#batch.length) return true;
    const ordered = this.#ordered();
    return this.write(this.#encodeBatch(ordered), 200);
  }

  // One batch answer: the codec's framing, an attachments frame when any
  // answer holds bytes, JSON otherwise.
  #encodeBatch(ordered) {
    if (this.codec) return this.codec.encode(ordered);
    if (this.attachments !== false && hasBytes(ordered)) return encodeAttachments(ordered);
    return JSON.stringify(ordered);
  }

  #ordered() {
    const collected = this.#collected;
    if (collected.length >= INDEX_THRESHOLD) return this.#orderedIndexed();
    const pending = collected.slice();
    const answers = [];
    const batch = this.#batch;
    for (let i = 0; i < batch.length; i++) {
      let index = 0;
      while (index < pending.length && pending[index].id !== batch[i]) index++;
      if (index === pending.length) continue;
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

  // A successful REST result under the route's cache policy: Cache-Control
  // as the policy decides once the session is known, a weak ETag over the
  // body, and 304 for a matching If-None-Match — HEAD and GET alike, since
  // the host strips a HEAD body itself.
  #writeCacheable(body, status) {
    const rest = this.#rest;
    const policy =
      rest.cache && status >= 200 && status < 300
        ? cacheHeadersFor(rest.cache, {
            access: rest.access,
            session: rest.hasSession?.() === true,
            cookies: this.#setCookies.length > 0,
          })
        : null;
    if (policy === null) return this.write(body, status);
    this.headers['Cache-Control'] = policy.control;
    if (!policy.etag) return this.write(body, status);
    const bytes = Buffer.isBuffer(body) ? body : Buffer.from(body);
    const etag = `W/"${crypto.createHash('sha1').update(bytes).digest('base64url')}"`;
    this.headers['ETag'] = etag;
    if (matchesEtag(this.call.headers?.['if-none-match'], etag)) return this.write('', 304);
    return this.write(bytes, status);
  }

  // The one funnel every HTTP answer leaves through — packet, batch, REST,
  // error — which is what makes `Content-Encoding` a single decision:
  // enabled, past the threshold, accepted by the peer, not already encoded
  // upstream, admitted by the filter. Below the async threshold the encode is
  // synchronous, like permessage-deflate's default; at or over it the body
  // goes to zlib's threadpool and the response is written from the
  // callback — `responded` is already true, so a second write and a close
  // racing it are no-ops either way.
  write(data, httpCode = 200) {
    if (this.#responded) return true;
    this.#responded = true;
    const body = bodyBytes(data);
    const headers = { ...this.headers };
    // An attachments frame is bytes, and says so — a client reads the body
    // as bytes only under this type.
    if (typeof data !== 'string' && isAttachmentsFrame(body)) headers['Content-Type'] = 'application/octet-stream';
    if (this.#setCookies.length > 0) headers['Set-Cookie'] = this.#setCookies;
    const compression = this.#compression;
    const encoder =
      compression !== null && body.length >= compression.threshold
        ? chooseEncoding(compression, this.call, headers)
        : null;
    if (encoder === null) return this.#answer(headers, body, httpCode);
    markEncoded(headers, encoder.token);
    // The encoder refused the body (out of memory is the realistic case):
    // the plain bytes still answer, and honestly labelled — and the failure
    // is said, since nothing else about the answer shows it.
    const plain = (error) => {
      this.#failed?.(encoder.token, error);
      delete headers['Content-Encoding'];
      return this.#answer(headers, body, httpCode);
    };
    if (encoder.encodeAsync !== null && compression.async !== null && body.length >= compression.async.threshold) {
      encoder.encodeAsync(
        body,
        (error, encoded) => void (error ? plain(error) : this.#answer(headers, encoded, httpCode)),
      );
      return true;
    }
    let encoded = null;
    try {
      encoded = encoder.encode(body);
    } catch (error) {
      return plain(error);
    }
    // An application's own coding may answer a promise.
    if (isPromise(encoded)) {
      encoded.then((out) => this.#answer(headers, out, httpCode), plain);
      return true;
    }
    return this.#answer(headers, encoded, httpCode);
  }

  #answer(headers, body, httpCode) {
    headers['Content-Length'] = body.length;
    try {
      this.#respond({ status: httpCode, headers, body });
    } catch (error) {
      // The host refused to write the response (node: a header value it
      // will not send — checked at every seam a handler reaches, but a
      // `headers` option built by hand is not). Without this the request
      // hung until the peer gave up and the client evicted nothing: a bare
      // 500 in its place, and the close every answer promises.
      try {
        this.#respond({ status: 500, headers: { ...SECURITY_HEADERS, 'Content-Length': 0 }, body: Buffer.alloc(0) });
      } catch {
        // The peer is gone, or the host is: nothing more to say.
      }
      if (this.listenerCount('error') > 0) this.emit('error', error);
    }
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
    const message = STATUS_CODES[503];
    const answered = new Set();
    for (let i = 0; i < this.#collected.length; i++) answered.add(this.#collected[i].id);
    for (const id of this.#batch) {
      if (answered.has(id)) continue;
      answered.add(id);
      this.#collected.push({ type: 'callback', id: typeof id === 'string' ? id : '', error: { message, code: 503 } });
    }
    const ordered = this.#ordered();
    this.write(this.#encodeBatch(ordered), 503);
  }
}

class ServerWsTransport extends ServerTransport {
  kind = 'ws';

  // `meta.kind` names the wire when a WrpcSocket-shaped connection is not a
  // WebSocket — attachSession in @alexify/wrpc/wt passes 'wt' — so logs and
  // metrics say which; the transport itself is the same either way.
  constructor(connection, meta = {}) {
    super(meta.remoteAddress ?? connection.remoteAddress ?? '');
    if (typeof meta.kind === 'string' && meta.kind) this.kind = meta.kind;
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

  // A write with per-message options (`compress: false`); the socket
  // contract's second argument, ignored by sockets that predate it.
  writeWith(text, options) {
    return this.connection.send(text, options);
  }

  // The fan-out seam: a shared `{ text, frames, compress }` message goes to
  // the socket's prepared-frame path when it has one (the built-in engine),
  // which encodes and deflates it once for every recipient; a socket
  // without one (uws, WebTransport) gets the text as an ordinary send.
  writeShared(message) {
    const socket = this.connection;
    // An attachments frame arrives as a Uint8Array; the engine speaks
    // Buffers. Converted ONCE on the shared message, for every recipient.
    const { text } = message;
    if (typeof text !== 'string' && !Buffer.isBuffer(text)) {
      message.text = Buffer.from(text.buffer, text.byteOffset, text.byteLength);
    }
    if (typeof socket.sendPrepared === 'function') return socket.sendPrepared(message);
    return socket.send(message.text, message.compress === false ? message : null);
  }

  // An event as a datagram where the socket has them (a WebTransport
  // session): true when it went out, false when it could not — a WebSocket
  // has no unreliable path, and Client.sendRaw then writes it reliably.
  writeUnreliable(text) {
    const socket = this.connection;
    return typeof socket.sendUnreliable === 'function' && socket.sendUnreliable(text) === true;
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
    // A port stays open like a socket does — what Client.persistent
    // checks (rpc/client.js), and so what admits events, subscriptions
    // and streams. Without it a port-attached client was silently
    // request/response only.
    this.connection = this;
    // A port has no handshake: until its page said — on its first ping, or
    // in the connect message handed to attachPort — that it reads framed
    // messages, it is sent none (protocol.md#versioning). A 1.0 page never
    // says so.
    this.revision = 1;
    this.attachments = false;
    // The newest revision the server behind this port speaks; attachPort
    // lowers it for a server that sends no frames.
    this.max = 2;
    port.on('close', () => void this.emit('close'));
  }

  // The page named revision `v`: this connection speaks the older of the
  // two, and the answer is this end's own — which is how the page learns it.
  negotiate(v) {
    this.setRevision(v === 2 && this.max === 2 ? 2 : 1);
    return this.max;
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
  publicErrorMessage,
  publicErrorDetails,
  wireError,
  ServerTransport,
  SECURITY_HEADERS,
  parseCookies,
  readsFrames,
  buildHeaders,
  isOriginAllowed,
};
