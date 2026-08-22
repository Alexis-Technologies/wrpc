'use strict';

const http = require('node:http');

const { Emitter, jsonParse, isCodec } = require('../utils.js');
const { generateUUID } = require('../runtime/node.js');
const { ServerTransport, buildHeaders, isOriginAllowed } = require('../transport.js');
const { WrpcWritable } = require('../streams.js');
const { SessionManager } = require('./sessions.js');
const { defineRouter, procedure, runHooksSafe } = require('./router.js');
const { RoomRegistry, Broadcast, RoomsBackplane } = require('./rooms.js');
const { Cluster } = require('./cluster.js');
const { SseChannels, CHANNEL_HEADER } = require('../sse/server.js');
const { isBackplane } = require('../scaling/index.js');
const {
  handleMessage,
  handleBinary,
  handleRpc,
  split,
  parseParams,
  sanitizeMeta,
  DEFAULT_MAX_BATCH,
} = require('./dispatcher.js');
const { createLoggerWriter } = require('../logging.js');
const { createServerTelemetry } = require('../telemetry/server.js');

// One peer holding thousands of open generators is a denial of service the
// application never opted into; the cap is generous but present.
const DEFAULT_MAX_SUBSCRIPTIONS = 256;
// Same reasoning for in-flight calls: each holds a controller, a context and
// possibly a queue slot until it settles.
const DEFAULT_MAX_CALLS = 1000;
// How long client.ask() waits for the peer's answer. Mirrors the client's
// default call timeout: an ask is a call travelling the other way.
const DEFAULT_ASK_TIMEOUT = 7_000;

// After this long an unsettled onConnect chain logs a warning: a hook that
// never resolves holds the client's dispatch (see #addClient), and the warn
// is the only trace that hang would leave.
const ONCONNECT_STALL_MS = 5_000;

const ServerHttpTransport = ServerTransport.transport.http;
const ServerWsTransport = ServerTransport.transport.ws;
const ServerEventTransport = ServerTransport.transport.event;

// The empty halves of a ClientMeta. Null-prototyped: header names and
// declared metadata keys are peer-controlled, and a plain literal would
// answer meta.headers['toString'] with a real function.
const FROZEN_EMPTY = Object.freeze({ __proto__: null });
const EMPTY_META = Object.freeze({
  data: FROZEN_EMPTY,
  headers: FROZEN_EMPTY,
  url: '',
  remoteAddress: '',
  protocol: '',
});

// The frozen snapshot of what the peer presented when the connection was
// made. `headers` is copied, not adopted: the source object belongs to the
// host request and other code may still be reading (or mutating) it.
const buildMeta = ({ headers, url, remoteAddress, protocol, data } = {}) =>
  Object.freeze({
    data: data ?? FROZEN_EMPTY,
    headers: headers ? Object.freeze({ __proto__: null, ...headers }) : FROZEN_EMPTY,
    url: url ?? '',
    remoteAddress: remoteAddress ?? '',
    protocol: protocol ?? '',
  });

// Connection-phase headers on the ws leg: the WHATWG WebSocket constructor
// cannot set real upgrade headers, so the client carries declared ones as
// ONE query parameter on the connect URL. PEER-CONTROLLED, so every step
// below is a refusal rather than a throw — an oversize or malformed label
// leaves the connection with no label, never without a connection. Observed
// upgrade headers always win the merge: the query can only ADD names the
// request did not carry, and the reserved names it could spoof are dropped
// outright (on http/sse, fetch itself refuses to send them, so the deny
// list exists exactly for this query path).
const HEADERS_PARAM = 'wrpc_h';
const DEFAULT_META_MAX = 2048;
const RESERVED_DECLARED = /^(?:cookie|host|origin)$|^(?:sec-|content-|proxy-|x-wrpc-)/;

const declaredHeaders = (url, limit, log) => {
  const query = split(url ?? '', '?')[1];
  if (!query) return null;
  // Capped on the ENCODED length, before any decoding work — that is the
  // string the peer actually controls.
  if (query.length > limit) {
    log.warn({ event: 'meta.oversize', bytes: query.length });
    return null;
  }
  const raw = new URLSearchParams(query).get(HEADERS_PARAM);
  if (!raw) return null;
  const value = jsonParse(raw);
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  let declared = null;
  for (const key of Object.keys(value)) {
    if (typeof value[key] !== 'string') continue; // a flat string map only
    // Lowercased: node lowercases observed header names, and schema.headers
    // validation must see one casing convention, not two.
    const name = key.toLowerCase();
    if (name === '__proto__' || RESERVED_DECLARED.test(name)) continue;
    (declared ??= { __proto__: null })[name] = value[key];
  }
  return declared;
};

// The connection-phase half of `meta` — the x-wrpc-meta request header
// (http/sse; percent-encoded JSON, since header values must stay latin-1)
// or the wrpc_meta connect-URL parameter (ws). Same sanitizer as the
// per-packet field, same refusal-not-throw discipline. Unlike headers this
// bag is deliberately outside schema validation: it is a label for
// cross-cutting hooks, not procedure input.
const META_HEADER = 'x-wrpc-meta';
const META_PARAM = 'wrpc_meta';

const declaredData = (headers, url, limit, log) => {
  let raw = null;
  const header = headers?.[META_HEADER];
  if (typeof header === 'string' && header.length > 0) {
    if (header.length > limit) {
      log.warn({ event: 'meta.oversize', bytes: header.length });
      return null;
    }
    try {
      raw = decodeURIComponent(header);
    } catch {
      return null;
    }
  } else {
    const query = split(url ?? '', '?')[1];
    if (!query || query.length > limit) return null;
    raw = new URLSearchParams(query).get(META_PARAM);
  }
  if (!raw) return null;
  return sanitizeMeta(jsonParse(raw), limit);
};

// A capability refusal ("this transport cannot carry that") is part of the
// protocol conversation, not a server internal: 400-coded and exposed so
// the peer reads the actual reason instead of a masked 500.
const refusal = (message) => {
  const error = new Error(message);
  error.code = 400;
  error.expose = true;
  return error;
};

class Context {
  #log = null;

  constructor(client, signal = null, target = null) {
    this.client = client;
    this.uuid = client.generateId();
    this.state = {};
    // Aborted when the caller cancels, unsubscribes, or disconnects. A
    // handler that awaits anything long-lived should pass it along; one
    // that ignores it simply runs to completion and has its result dropped.
    this.signal = signal;
    // Call identity, so cross-cutting hooks need not re-derive it from the
    // packet: the wire target ('unit.ver/name'; an inbound event's name
    // verbatim) and the resolved Procedure handling it.
    this.method = target?.method ?? null;
    this.procedure = target?.procedure ?? null;
    // The caller's per-invocation metadata (the packet's optional `meta`
    // field, sanitized) — a frozen empty object rather than null, so a
    // cross-cutting hook reads context.callMeta.idem without `?.`.
    this.callMeta = target?.callMeta ?? FROZEN_EMPTY;
  }

  get session() {
    return this.client.session;
  }

  // Bound lazily: a Context is allocated for every call, subscribe and
  // inbound event, and most handlers never log. `uuid` is already there, so
  // the correlation id exists whether or not anyone asks for the child.
  get log() {
    return (this.#log ??= this.client.log.child({ callId: this.uuid }));
  }

  // Rooms are reached from a handler through here — `ctx.server.to(room)` —
  // rather than by closing over a server the router had to exist before.
  get server() {
    return this.client.server;
  }

  // The connection's presented metadata, mirrored the way `session` is: a
  // getter, so the per-call Context allocates nothing for it.
  get meta() {
    return this.client.meta;
  }
}

class Client extends Emitter {
  #transport = null;
  #sessions = null;
  #rooms = null;
  #server = null;
  #log = null;
  #otel = null;
  #codec = null;
  // id -> { resolve, reject, timer }: answers this client owes to asks the
  // server sent it. See ask()/expectAnswer()/settleAnswer().
  #asks = new Map();
  #ready = null;

  constructor(transport, options = {}) {
    super();
    const {
      sessions,
      rooms,
      server,
      log,
      otel,
      maxSubscriptions = DEFAULT_MAX_SUBSCRIPTIONS,
      maxCalls = DEFAULT_MAX_CALLS,
      generateId,
      codec = null,
      meta = null,
      metaMax = DEFAULT_META_MAX,
    } = options;
    // The dispatcher's cap on the per-packet `meta` field (sanitizeMeta).
    this.metaMax = metaMax;
    // What the peer presented when the connection was made (headers, url,
    // negotiated subprotocol) — frozen, EMPTY_META when nothing was. The
    // peer-controlled parts are labels, never authorization inputs.
    this.meta = meta ?? EMPTY_META;
    this.#transport = transport;
    this.#sessions = sessions;
    this.#codec = codec;
    // A Client built outside an RpcServer still has working rooms; it just
    // has nobody to share them with.
    this.#rooms = rooms ?? new RoomRegistry();
    this.#server = server ?? null;
    // Connection-scoped, bound once: a connection lives for minutes, and the
    // binding hoists the peer id out of every line logged for it.
    this.#log = createLoggerWriter(log ?? globalThis.console).child({ peer: transport.source });
    this.#otel = otel ?? createServerTelemetry(null);
    this.source = transport.source;
    this.session = null;
    this.sessionReady = Promise.resolve();
    this.streams = new Map();
    // id -> AbortController, for the two things a peer can take back.
    this.calls = new Map();
    this.subscriptions = new Map();
    this.maxSubscriptions = maxSubscriptions;
    this.maxCalls = maxCalls;
    // Context uuids and server-side stream ids; uuid v4 unless the app
    // brings its own (cuid/ulid/a test counter) — see RpcServerOptions.
    this.generateId = typeof generateId === 'function' ? generateId : generateUUID;
    // Instance-prefixed, so the id IS the address: a cluster command for
    // this client goes straight to this instance's channel, no broadcast.
    // A standalone Client (no server) has no instance and no prefix.
    this.id = this.#server ? `${this.#server.instanceId}.${this.generateId()}` : this.generateId();
    // The application's bag, carried by cluster descriptors — what
    // socket.data is in socket.io. wrpc itself never reads it.
    this.data = {};
  }

  error(code, { id = '', error = null } = {}) {
    const httpCode = code <= 599 ? code : 500;
    const status = http.STATUS_CODES[httpCode];
    const info = error ? error.stack : status || 'Unknown error';
    this.#transport.error(code, { id, error });
    this.#log.error({ event: 'rpc.error', code, id, err: error }, `${this.source}\t${code}\t${info}`);
  }

  // What dispatch actually gates on: the session restore PLUS the settled
  // onConnect hooks (#addClient assigns the combined promise). Two promises
  // on purpose — a hook may `await client.sessionReady`, so folding the
  // hooks into that same promise would make such a hook wait for itself.
  // Follows `sessionReady` until #addClient diverges them, so a standalone
  // Client that assigns sessionReady by hand keeps the gate it expects.
  get ready() {
    return this.#ready ?? this.sessionReady;
  }

  set ready(value) {
    this.#ready = value;
  }

  /** The connection-scoped writer, reached by the dispatcher and handlers. */
  get log() {
    return this.#log;
  }

  /** The server's telemetry writer; disabled-shaped when unconfigured. */
  get otel() {
    return this.#otel;
  }

  /** 'ws' | 'http' | 'sse' | 'event' — a metric attribute and a log field. */
  get transportKind() {
    return this.#transport.kind;
  }

  // Diagnostics for packets that carry no id to answer on (inbound events):
  // the log is the only channel left.
  warn(message, entry = {}) {
    this.#log.warn({ event: 'rpc.warn', ...entry }, `${this.source}\t${message}`);
  }

  /** Returns false when the transport is above its high-water mark. */
  send(obj, options = {}) {
    const { code, method, text } = options;
    const flushed = this.#transport.send(obj, code, text);
    // Debug on purpose: one line per successful call is a firehose. A
    // console logger drops debug outright; a structured logger's own level
    // decides. Failures still log at error, unconditionally.
    const isSuccessCallback = obj.type === 'callback' && !obj.error;
    if (isSuccessCallback) {
      this.#log.debug({ event: 'call.ok', method, id: obj.id }, `${this.source}\tCALL\t${method}\tOK`);
    }
    return flushed;
  }

  /**
   * Resolves when the transport has drained — or when it closes, so a
   * producer waiting on a peer that never reads is released by the
   * disconnect rather than parked forever.
   */
  drain() {
    return new Promise((resolve) => {
      const done = () => {
        this.#transport.off('drain', done);
        this.#transport.off('close', done);
        resolve();
      };
      this.#transport.on('drain', done);
      this.#transport.on('close', done);
    });
  }

  // True for transports that stay open (WebSocket, worker port): only
  // those can carry events and streams.
  get persistent() {
    return Boolean(this.#transport.connection);
  }

  /** The RpcServer this client belongs to; null for a standalone Client. */
  get server() {
    return this.#server;
  }

  /** False on a text-only transport (SSE), where binary streams cannot go. */
  get binary() {
    return this.#transport.binary !== false;
  }

  createContext(signal = null, target = null) {
    return new Context(this, signal, target);
  }

  // The inbound half of the wire codec: what handleMessage parses frames
  // with. Malformed input answers null, same contract as jsonParse.
  decodePacket(text) {
    if (!this.#codec) return jsonParse(text);
    try {
      return this.#codec.decode(typeof text === 'string' ? text : String(text));
    } catch {
      return null;
    }
  }

  // NOTE: `emit` is the inherited LOCAL Emitter emit — `client.on('x', fn)`
  // works, and Client is substitutable for the Emitter it extends. The wire
  // send has its own name and always had it: `sendEvent`.

  sendEvent(name, data) {
    const packet = { type: 'event', name, data };
    if (!this.#transport.connection) {
      throw refusal(`Can't send wrpc event to http transport`);
    }
    this.send(packet);
  }

  /**
   * Writes an ALREADY-serialized packet. The fan-out seam: a broadcast to N
   * clients stringifies once and hands every recipient the same text,
   * instead of paying JSON.stringify per client. Returns the transport's
   * backpressure signal, like send().
   */
  sendRaw(text) {
    if (!this.#transport.connection) {
      throw refusal(`Can't send wrpc event to http transport`);
    }
    return this.#transport.write(text);
  }

  /**
   * A call in the other direction: sends `{type:'event', name, data, id}`
   * and resolves with the answer the peer's responder returns (registered
   * client-side with `client.respond(name, fn)`). Rejects with 408 on
   * timeout, 503 when the connection drops first.
   */
  ask(name, data, options = {}) {
    if (!this.#transport.connection) {
      throw refusal(`Can't send wrpc event to http transport`);
    }
    const id = this.generateId();
    const packet = { type: 'event', name, data, id };
    this.send(packet);
    return this.expectAnswer(id, options.timeout);
  }

  /**
   * Registers a pending answer slot for `id`. The seam Broadcast.ask()
   * uses: the broadcast writes its own pre-serialized packet through
   * sendRaw and only needs the bookkeeping half of ask().
   */
  expectAnswer(id, timeout) {
    return new Promise((resolve, reject) => {
      const wait = timeout > 0 ? timeout : DEFAULT_ASK_TIMEOUT;
      const timer = setTimeout(() => {
        this.#asks.delete(id);
        const error = new Error('Ask timeout');
        error.code = 408;
        reject(error);
      }, wait);
      if (typeof timer.unref === 'function') timer.unref();
      this.#asks.set(id, { resolve, reject, timer });
    });
  }

  /** Routes an inbound `callback` to its pending ask; false when none. */
  settleAnswer(packet) {
    const pending = this.#asks.get(packet.id);
    if (!pending) return false;
    this.#asks.delete(packet.id);
    clearTimeout(pending.timer);
    if (packet.error) {
      const error = new Error(packet.error.message ?? 'Ask failed');
      error.code = packet.error.code ?? 500;
      if (packet.error.details !== undefined) error.details = packet.error.details;
      pending.reject(error);
    } else {
      pending.resolve(packet.result);
    }
    return true;
  }

  // ---------------------------------------------------------------------
  // Rooms. The registry owns both directions, so this client keeps no room
  // state of its own that a missed leave could leave stale.

  join(room) {
    return this.#rooms.join(this, room);
  }

  leave(room) {
    return this.#rooms.leave(this, room);
  }

  /** The rooms this client is in — a copy, safe to iterate while leaving. */
  get rooms() {
    return new Set(this.#rooms.roomsOf(this));
  }

  in(room) {
    return this.#rooms.members(room).has(this);
  }

  getStream(id) {
    if (!this.#transport.connection) {
      throw refusal(`Can't receive stream from http transport`);
    }
    if (!this.binary) throw refusal(`Can't receive stream over a text-only transport`);
    const stream = this.streams.get(id);
    if (stream) return stream;
    throw new Error(`Stream ${id} is not initialized`);
  }

  createStream(name, size) {
    if (!this.#transport.connection) {
      throw refusal(`Can't send wrpc streams to http transport`);
    }
    if (!this.binary) throw refusal(`Can't send wrpc streams over a text-only transport`);
    if (!name) throw new Error('Stream name is not provided');
    if (!size) throw new Error('Stream size is not provided');
    const id = this.generateId();
    if (typeof id !== 'string' || id.length === 0 || id.length > 255) {
      throw new TypeError('createStream: generateId must return a string of at most 255 characters');
    }
    const stream = new WrpcWritable(id, name, size, this.#transport);
    this.streams.set(id, stream);
    return stream;
  }

  initializeSession(token, data = {}) {
    // Re-initializing the SAME token must not finalize first: with an async
    // store the fire-and-forget delete(token) could land after the new
    // set(token) and silently wipe the fresh session
    if (this.session && this.session.token !== token) void this.finalizeSession();
    this.session = this.#sessions.create(token, data);
    return true;
  }

  async finalizeSession() {
    if (!this.session) return false;
    const { token } = this.session;
    this.session = null;
    await this.#sessions.destroy(token);
    return true;
  }

  startSession(token, data = {}) {
    this.initializeSession(token, data);
    if (!this.#transport.connection) {
      // The carrier stamps the response when it can (a cookie); a bearer
      // carrier answers null and the handler returns tokens in its result.
      const header = this.#sessions.transport.write(this.session.token);
      if (header) this.#transport.sendSessionCookie(header);
    }
    return true;
  }

  async restoreSession(token) {
    const session = await this.#sessions.restore(token);
    if (!session) return false;
    this.session = session;
    return true;
  }

  close() {
    this.#transport.close();
  }

  // A dropped connection does NOT delete the session from the store —
  // that is what makes restoreSession after a reconnect possible.
  // Sessions end via finalizeSession or store-side expiry only.
  destroy() {
    const log = this.#log;
    this.#rooms.leaveAll(this);
    // A gone peer cannot receive an answer, so everything still running on
    // its behalf is told to stop — this is what runs a subscription
    // handler's `finally`, releasing whatever it had open.
    const disconnected = new Error('Client disconnected');
    for (const controller of this.calls.values()) controller.abort(disconnected);
    for (const controller of this.subscriptions.values()) controller.abort(disconnected);
    this.calls.clear();
    this.subscriptions.clear();
    // An answer can no longer arrive: whoever asked is settled NOW instead
    // of waiting out the ask timeout on a peer that is gone.
    if (this.#asks.size > 0) {
      const gone = new Error('Client disconnected');
      gone.code = 503;
      for (const pending of this.#asks.values()) {
        clearTimeout(pending.timer);
        pending.reject(gone);
      }
      this.#asks.clear();
    }
    // Contained: destroy() runs from the transport's close handler, where a
    // throwing app listener would otherwise be an unhandled rejection.
    Promise.resolve(this.emit('close')).catch((error) => {
      log.error({ err: error, event: 'listener.close' });
    });
    for (const stream of this.streams.values()) {
      if (typeof stream.terminate !== 'function') continue;
      Promise.resolve(stream.terminate()).catch((error) => {
        log.error({ err: error, event: 'stream.terminate', stream: stream.id });
      });
    }
    this.streams.clear();
  }
}

const DEFAULT_BASE_PATH = '/api';

// The options the core owns. Every shell and adapter funnels its own option
// bag through here, so adding a core option cannot be silently dropped by
// one of the four places that construct an RpcServer.
const RPC_OPTION_KEYS = [
  'router',
  'sessions',
  'cors',
  'basePath',
  'logger',
  'telemetry',
  'backplane',
  'instanceId',
  'generateId',
  'introspection',
  'maxBatch',
  'maxSubscriptions',
  'maxCalls',
  'sse',
  'cluster',
  'querystring',
  'codec',
  'metaMaxBytes',
];

const rpcOptions = (options = {}) => {
  const picked = {};
  for (const key of RPC_OPTION_KEYS) {
    if (options[key] !== undefined) picked[key] = options[key];
  }
  return picked;
};

const normalizeBasePath = (basePath) => {
  if (!basePath) return '';
  let path = basePath.startsWith('/') ? basePath : `/${basePath}`;
  if (path.endsWith('/')) path = path.slice(0, -1);
  return path;
};

// Engine-agnostic RPC core: no node:http imports on the request path.
// Sockets come in through attachSocket (any WrpcSocket-shaped engine
// connection), HTTP calls through handleHttpCall (an abstract call
// description), worker ports through attachPort.
class RpcServer extends Emitter {
  #router;
  #sessions;
  #rooms;
  #backplane = null;
  #cluster = null;
  #instance;
  #cors;
  #basePath;
  #log;
  #roomsLog;
  #sseLog;
  #otel;
  #limits;
  #generateId;
  #sse = null;
  #querystring = null;
  #metaMax = DEFAULT_META_MAX;
  #codec = null;
  #codecOption = null;
  #restCodec = null;
  #draining = false;
  #clients = new Set();
  #byId = new Map();

  constructor(options = {}) {
    super();
    const {
      router,
      sessions,
      cors = null,
      basePath = DEFAULT_BASE_PATH,
      logger = globalThis.console,
      telemetry = null,
      backplane = null,
      instanceId = generateUUID(),
      generateId = null,
      introspection = true,
      maxBatch = DEFAULT_MAX_BATCH,
      maxSubscriptions = DEFAULT_MAX_SUBSCRIPTIONS,
      maxCalls = DEFAULT_MAX_CALLS,
      sse = {},
      cluster = {},
      querystring = null,
      codec = null,
      metaMaxBytes = DEFAULT_META_MAX,
    } = options;
    // The cap on peer-declared metadata (the ws wrpc_h query parameter and
    // the per-packet meta field), measured on the encoded input.
    this.#metaMax = Number.isInteger(metaMaxBytes) && metaMaxBytes > 0 ? metaMaxBytes : DEFAULT_META_MAX;
    if (!router || typeof router.getProcedure !== 'function') {
      throw new TypeError('RpcServer: options.router (a Router from defineRouter) is required');
    }
    if (backplane && !isBackplane(backplane)) {
      throw new TypeError('RpcServer: options.backplane does not implement the backplane contract');
    }
    // The dot separates the instance prefix from the rest of a client id
    // (`<instanceId>.<generateId()>`), so an instance name carrying one
    // would make every one of its client ids parse to the wrong address.
    if (String(instanceId).includes('.')) {
      throw new TypeError('RpcServer: options.instanceId must not contain "."');
    }
    // Pluggable query-string codec (qs and friends). Structural: anything
    // with parse(str) -> object. The default is the prototype-safe
    // URLSearchParams path in parseParams; an injected parser takes over
    // prototype-pollution responsibility (documented).
    if (querystring !== null && typeof querystring.parse !== 'function') {
      throw new TypeError('RpcServer: options.querystring must provide a parse(text) function');
    }
    if (codec !== null && !isCodec(codec)) {
      throw new TypeError('RpcServer: options.codec must provide encode(packet)/decode(text), a rest section, or both');
    }
    this.#log = createLoggerWriter(logger);
    this.#otel = createServerTelemetry(telemetry);
    // Built once here rather than per broadcast or per channel: Broadcast is
    // constructed on every to()/except()/broadcast().
    this.#roomsLog = this.#log.child({ component: 'rooms' });
    this.#sseLog = this.#log.child({ component: 'sse' });
    this.#sessions = new SessionManager(sessions, this.#log.child({ component: 'sessions' }));
    this.#cors = cors;
    this.#basePath = normalizeBasePath(basePath);
    this.#instance = instanceId;
    this.#generateId = typeof generateId === 'function' ? generateId : generateUUID;
    this.#querystring = querystring;
    // Two halves, two fields: #codec is the PACKET codec (ws/http/sse/worker
    // frames — a rest-only codec leaves packet mode JSON), #restCodec the
    // REST body codec. The raw option survives for the public getter.
    this.#codecOption = codec;
    this.#codec = codec && typeof codec.encode === 'function' && typeof codec.decode === 'function' ? codec : null;
    this.#restCodec = codec?.rest ?? null;
    this.#limits = { maxBatch, maxSubscriptions, maxCalls };
    this.#router = this.#withIntrospection(router, introspection);
    // A compiled fjs serializer emits JSON; a packet codec re-frames the
    // whole wire. Both at once would mean the serializer's output is thrown
    // away (or worse, double-encoded) — refusal beats a silent precedence.
    // (codec.rest is compatible: the fast path serializes envelopes, and
    // REST bodies never carry envelopes.)
    if (this.#codec && this.#router.hasSerializers) {
      throw new TypeError('RpcServer: options.codec and compiled response serializers are mutually exclusive');
    }
    this.#initRooms(backplane, cluster);
    // A channel's client is built from the GET that opened the stream, so it
    // restores the session from that request's cookie the way attachSocket
    // does — otherwise a browser holding a valid cookie starts the channel
    // anonymous and every `access: 'session'` procedure on it answers 403.
    const addClient = (transport, call) =>
      this.#addClient(
        transport,
        (client) => this.#restoreToken(client, { headers: call.headers, url: call.url }),
        buildMeta({
          headers: call.headers,
          data: declaredData(call.headers, call.url, this.#metaMax, this.#sseLog),
          url: call.url,
          remoteAddress: call.remoteAddress,
        }),
      );
    // The identity a request presents, bound to a channel at creation and
    // required again on every re-attach and channel POST — the id alone
    // must never be enough to act as the channel's session.
    const channelKey = (headers) => this.#requestKey(headers);
    this.#sse =
      sse === false ? null : new SseChannels({ ...sse, log: this.#sseLog, otel: this.#otel, addClient, channelKey });
  }

  #initRooms(backplane, clusterOptions) {
    // The cluster exists with or without a backplane — without one every
    // operation degrades to its local half, so application code written
    // against `server.cluster` never branches on the deployment.
    const cluster = new Cluster({
      backplane,
      instance: this.#instance,
      local: this.#clusterOps(),
      log: this.#log.child({ component: 'cluster' }),
      generateId: this.#generateId,
      options: clusterOptions,
    });
    this.#cluster = cluster;
    if (!backplane) {
      this.#rooms = new RoomRegistry();
      return;
    }
    const binder = new RoomsBackplane({
      backplane,
      instance: this.#instance,
      log: this.#roomsLog,
      // A replayed event is delivered LOCALLY: publishing it again would
      // bounce it between instances forever.
      deliver: (rooms, name, data) => {
        const target = rooms ? this.#target().to(...rooms) : this.#target();
        target.local().emit(name, data);
      },
    });
    this.#backplane = binder;
    this.#rooms = new RoomRegistry({
      onSubscribe: (room) => binder.joinRoom(room),
      onUnsubscribe: (room) => binder.leaveRoom(room),
      // Every membership change is a presence delta; the periodic snapshot
      // corrects whatever the broker drops.
      onJoin: (room) => cluster.delta(room, 1),
      onLeave: (room) => cluster.delta(room, -1),
    });
    binder.start();
    cluster.start();
  }

  // The seam the cluster reaches this node's clients through: selectors and
  // descriptors here, correlation and channels there.
  #clusterOps() {
    const select = (sel = {}) => {
      if (typeof sel.id === 'string') {
        const client = this.#byId.get(sel.id);
        return client ? [client] : [];
      }
      if (typeof sel.room === 'string') return Array.from(this.#rooms.members(sel.room));
      // Persistent connections only: a per-request HTTP client is not a
      // peer anyone means to enumerate, join or disconnect. Filtered while
      // collecting — Array.from().filter() built the whole client list first
      // and then threw most of it away.
      const persistent = [];
      for (const client of this.#clients) if (client.persistent) persistent.push(client);
      return persistent;
    };
    return {
      count: (room) => this.#rooms.count(room),
      snapshot: () => {
        const rooms = {};
        for (const room of this.#rooms.list()) rooms[room] = this.#rooms.count(room);
        let clients = 0;
        for (const client of this.#clients) if (client.persistent) clients++;
        return { rooms, clients };
      },
      // One pass: the filter+map chain allocated an intermediate array on top
      // of the one select() already built.
      descriptors: (sel) => {
        const selected = select(sel);
        const out = [];
        for (let i = 0; i < selected.length; i++) {
          const client = selected[i];
          if (!client.persistent) continue;
          const rooms = [];
          for (const room of client.rooms) rooms.push(room);
          out.push({
            id: client.id,
            instance: this.#instance,
            rooms,
            data: client.data,
            transport: client.transportKind,
            session: Boolean(client.session),
          });
        }
        return out;
      },
      join: (sel, rooms) => {
        if (!Array.isArray(rooms)) return;
        for (const client of select(sel)) {
          for (const room of rooms) client.join(room);
        }
      },
      leave: (sel, rooms) => {
        if (!Array.isArray(rooms)) return;
        for (const client of select(sel)) {
          for (const room of rooms) client.leave(room);
        }
      },
      disconnect: (sel) => {
        for (const client of select(sel)) client.close();
      },
      // The remote leg of a broadcast ask: LOCAL delivery only — the
      // question already reached every other node as its own request. An
      // ARRAY of rooms is a narrowing even when empty (to() with no rooms
      // reaches nobody); only null means "everyone" — collapsing [] into
      // the all-clients target would resurrect the WHERE-id-IN-() mistake
      // for any envelope arriving with rooms: [] on the wire.
      ask: (rooms, name, data, timeout, onCount) => {
        const target = Array.isArray(rooms) ? this.#target().to(...rooms) : this.#target();
        return target.local().ask(name, data, { timeout, onCount });
      },
    };
  }

  /** Cluster-wide presence, introspection and node-to-node messaging. */
  get cluster() {
    return this.#cluster;
  }

  /** The local client with this id; undefined when not on this instance. */
  getClient(id) {
    return this.#byId.get(id);
  }

  get router() {
    return this.#router;
  }

  get sessions() {
    return this.#sessions;
  }

  get rooms() {
    return this.#rooms;
  }

  /** Identifies this instance on the backplane (echo suppression). */
  get instanceId() {
    return this.#instance;
  }

  get basePath() {
    return this.#basePath;
  }

  /** The injected codec option, verbatim — how an adapter inspects codec.rest. */
  get codec() {
    return this.#codecOption;
  }

  get clients() {
    return new Set(this.#clients);
  }

  /**
   * A host-delegated REST route (the fastify adapter's native routes) runs
   * its procedure outside handleHttpCall: the host owns routing, validation
   * and serialization; wrpc still owns the session, the rooms and the
   * client lifecycle. Returns the per-request client + context; call
   * `release()` when the response is done (wired to its close event) so the
   * client is evicted. The safe-method CSRF rule is the same one
   * #handleRest applies.
   */
  async delegatedContext({ method = 'GET', headers = {}, remoteAddress = '', url = '' } = {}, target = null) {
    const transport = new ServerHttpTransport({ headers, remoteAddress, respond: () => {} }, { headers: {} });
    const verb = String(method).toUpperCase();
    const safeMethod = verb === 'GET' || verb === 'HEAD';
    // The safe-method CSRF rule guards AMBIENT authority (a browser cookie
    // attached without script). A non-ambient carrier — a bearer header the
    // page's own code must set — has nothing to guard, so it restores on
    // safe methods too.
    const ambient = this.#sessions.transport.ambient === true;
    const restore =
      !safeMethod || !ambient || this.#isSameOriginFetch(headers)
        ? (c) => this.#restoreToken(c, { headers, url })
        : null;
    const client = this.#addClient(
      transport,
      restore,
      buildMeta({ headers, data: declaredData(headers, url, this.#metaMax, this.#log), url, remoteAddress }),
    );
    await client.ready;
    const context = client.createContext(null, target);
    return { client, context, transport, release: () => transport.emit('close') };
  }

  #target() {
    return new Broadcast({
      registry: this.#rooms,
      clients: () => this.#clients,
      publish: this.#backplane ? (envelope) => this.#backplane.publish(envelope) : null,
      cluster: this.#backplane ? this.#cluster : null,
      log: this.#roomsLog,
      otel: this.#otel,
      codec: this.#codec,
    });
  }

  /** Everyone in any of `rooms`, each client once. */
  to(...rooms) {
    return this.#target().to(...rooms);
  }

  /** Everyone connected, minus `clients`. */
  except(...clients) {
    return this.#target().except(...clients);
  }

  /** Everyone connected; returns the number of LOCAL recipients. */
  broadcast(name, data) {
    return this.#target().emit(name, data);
  }

  // `mode`: true mounts system/introspect as public (the default the typed
  // client and the codegen CLI rely on), 'session' gates it behind a
  // session, false leaves the API surface unadvertised entirely. A router
  // that already defines its own introspect always wins.
  // `introspection` is a boolean, 'session', or the object form
  // { access?: true | 'session' | false, schemas?: boolean } — the latter
  // controls whether input schema parts travel to clients.
  #withIntrospection(router, mode) {
    const config = mode !== null && typeof mode === 'object' ? mode : { access: mode };
    const { access = true, schemas = true } = config;
    if (access === false || mode === false) return router;
    if (router.getProcedure('system', '*', 'introspect')) return router;
    const system = defineRouter({
      system: {
        introspect: procedure({
          access: access === 'session' ? 'session' : 'public',
          handler: async (_context, units) => this.#router.introspect(units, { schemas }),
        }),
      },
    });
    return router.merge(system);
  }

  #addClient(transport, restore = null, meta = null) {
    // The transport encodes outbound packets; the Client decodes inbound
    // ones. One server-wide codec — which is what keeps the broadcast
    // fan-out single-encode.
    if (this.#codec) transport.codec = this.#codec;
    const options = {
      codec: this.#codec,
      sessions: this.#sessions,
      rooms: this.#rooms,
      server: this,
      log: this.#log,
      otel: this.#otel,
      maxSubscriptions: this.#limits.maxSubscriptions,
      maxCalls: this.#limits.maxCalls,
      generateId: this.#generateId,
      meta,
      metaMax: this.#metaMax,
    };
    const client = new Client(transport, options);
    this.#clients.add(client);
    this.#byId.set(client.id, client);
    this.#otel.recordConnection(1, transport.kind);
    // Assigned BEFORE the hooks run: the documented recipe is
    // `onConnect: async (client) => { await client.sessionReady; ... }`, and
    // a hook that ran ahead of this assignment awaited the constructor's
    // resolved default and saw session === null. A thunk, not a promise —
    // the restore needs the client this method is what creates.
    if (restore) client.sessionReady = restore(client);
    // Router-level connection lifecycle hooks. Contained (a throwing hook is
    // logged, never fatal — refusing a connection is verifyClient's job) but
    // ORDERED for dispatch: `client.ready` is session restore plus settled
    // hooks, and the dispatcher awaits it before the access check, so a
    // subscribe racing the hooks can no longer miss a room broadcast the
    // hook's re-join was about to earn it. Fires for every attached client —
    // the per-request HTTP ones included.
    const { onConnect, onDisconnect } = this.#router.connectionHooks;
    if (onConnect.length > 0) {
      const hooks = runHooksSafe(onConnect, client, null, this.#log, 'onConnect');
      client.ready = client.sessionReady.then(() => hooks);
      // A hook that never settles now holds this client's dispatch (before,
      // it silently ran late and lost broadcasts) — a stall leaves a trace.
      const stall = setTimeout(() => {
        this.#log.warn({ event: 'onConnect.stalled', peer: client.source, ms: ONCONNECT_STALL_MS });
      }, ONCONNECT_STALL_MS);
      if (typeof stall.unref === 'function') stall.unref();
      void client.ready.then(() => clearTimeout(stall));
    }
    transport.once('close', () => {
      // Snapshotted BEFORE destroy(): its first act is rooms.leaveAll(), so
      // by hook time the registry is empty — the payload is the only way a
      // disconnect hook learns which rooms the client was in. (client.rooms
      // already returns a fresh Set copy.) Not reordered: the hooks run
      // fire-and-forget, so ordering would not guarantee visibility anyway.
      const payload = onDisconnect.length > 0 ? { rooms: client.rooms } : null;
      client.destroy();
      this.#clients.delete(client);
      this.#byId.delete(client.id);
      this.#otel.recordConnection(-1, transport.kind);
      if (onDisconnect.length > 0) void runHooksSafe(onDisconnect, client, payload, this.#log, 'onDisconnect');
    });
    return client;
  }

  // The injected token carrier decides what "this request presents a
  // session" means: a cookie by default, an Authorization header or a
  // payload field when the app swapped the strategy (sessions.transport).
  #restoreToken(client, request) {
    const token = this.#sessions.transport.read(request);
    if (!token) return Promise.resolve(false);
    return client.restoreSession(token).then(
      (restored) => {
        this.#otel.recordSession('restore', restored ? 'hit' : 'miss');
        return restored;
      },
      (error) => {
        this.#log.error({ err: error, event: 'session.restore' });
        this.#otel.recordSession('restore', 'error');
        return false;
      },
    );
  }

  attachSocket(socket, meta = {}) {
    const transport = new ServerWsTransport(socket, meta);
    // Declared-then-observed: the wrpc_h query can only add names the
    // upgrade request did not carry (see declaredHeaders).
    const declared = declaredHeaders(meta.url, this.#metaMax, this.#log);
    const client = this.#addClient(
      transport,
      (c) => this.#restoreToken(c, { headers: meta.headers, url: meta.url }),
      buildMeta({
        headers: declared ? { ...declared, ...meta.headers } : meta.headers,
        data: declaredData(meta.headers, meta.url, this.#metaMax, this.#log),
        url: meta.url,
        remoteAddress: meta.remoteAddress ?? socket.remoteAddress,
        protocol: socket.protocol,
      }),
    );

    // Receive-side flow control: while binary chunks are being consumed
    // (WrpcReadable.push applies its high-water mark), stop reading from
    // the socket so the pressure reaches the peer through TCP.
    let inflight = 0;
    const done = () => {
      inflight--;
      if (inflight === 0 && typeof socket.resume === 'function') socket.resume();
    };
    socket.on('message', (data, isBinary) => {
      if (!isBinary) return void handleMessage(client, data, this.#router, this.#limits);
      inflight++;
      if (inflight === 1 && typeof socket.pause === 'function') socket.pause();
      handleBinary(client, new Uint8Array(data)).then(done, done);
    });
    socket.on('error', () => transport.emit('close'));
    return client;
  }

  attachPort(port, meta = null) {
    const transport = new ServerEventTransport(port);
    // A MessagePort carries no request, so there is nothing to observe; a
    // consumer that received declared headers in the 'wrpc:connect' message
    // may hand them over here.
    const client = this.#addClient(transport, null, meta?.headers ? buildMeta({ headers: meta.headers }) : null);
    port.on('message', (data) => {
      if (typeof data === 'string' || Buffer.isBuffer(data)) {
        handleMessage(client, data, this.#router, this.#limits);
      } else if (data instanceof Uint8Array) {
        handleBinary(client, data);
      }
    });
    return client;
  }

  // Path contract under basePath (default '/api'):
  //   POST <basePath>            — a JSON call packet in the body
  //   ANY  <basePath>/unit/method?args — REST mode, args from query + body
  matchPath(pathname) {
    const base = this.#basePath || '';
    if (pathname === (base || '/')) return { mode: 'packet' };
    const prefix = `${base}/`;
    if (base && pathname.startsWith(prefix)) {
      return { mode: 'rest', rest: pathname.slice(prefix.length) };
    }
    if (!base && pathname.length > 1) return { mode: 'rest', rest: pathname.slice(1) };
    return null;
  }

  // Packet-mode bodies speak the codec when one is configured; REST-mode
  // bodies stay JSON on purpose (curl and browsers are that mode's
  // audience). Malformed input answers null either way.
  #decodeBody(body) {
    if (!this.#codec) return jsonParse(body);
    try {
      return this.#codec.decode(typeof body === 'string' ? body : String(body));
    } catch {
      return null;
    }
  }

  // A batch frame needs to be recognized BEFORE the transport exists: the
  // transport has to know how many answers to collect and in which order to
  // emit them, which only the request's own id list can tell it.
  #batchIds(body) {
    const packet = this.#decodeBody(body);
    if (!Array.isArray(packet)) return null;
    if (packet.length === 0 || packet.length > this.#limits.maxBatch) return null;
    const ids = new Array(packet.length);
    for (let i = 0; i < packet.length; i++) {
      const item = packet[i];
      ids[i] = item && typeof item === 'object' ? item.id : undefined;
    }
    return ids;
  }

  get sse() {
    return this.#sse;
  }

  // The SSE endpoint sits just under basePath so it moves with it, and it is
  // the one route whose response is a stream rather than a body.
  get eventsPath() {
    return `${this.#basePath}/events`;
  }

  // The identity an HTTP request presents: its cookie's session token, or
  // '' when it carries none. What SSE channels are keyed by.
  #requestKey(headers = {}) {
    // The identity a request presents, whatever the injected carrier is —
    // the cookie token by default, the Authorization header under a bearer
    // strategy. An SSE channel is keyed on it at creation and must present
    // the same one on every re-attach and POST.
    return this.#sessions.transport.read({ headers, url: '' }) ?? '';
  }

  // A POST carrying a live channel id belongs to that channel's client, not
  // to a fresh request/response one: that is what lets a subscription opened
  // by a POST deliver its values down the peer's event stream. The POST
  // itself answers 202 — every reply travels on the stream.
  //
  // The channel id alone is NOT enough: the POST must also present the
  // cookie identity the channel was created under, or knowing an id (they
  // ride in URLs and logs) would be a bearer token for someone else's
  // session-carrying client.
  //
  // `headers` are the same CORS-bearing response headers every other HTTP
  // answer carries: without them a browser on another origin cannot read
  // this response at all, which makes cross-origin SSE impossible.
  #handleChannelPost(call, channelId, headers) {
    // Channel POSTs answer packet-mode bodies, so the packet codec's type.
    if (this.#codec?.contentType) headers = { ...headers, 'Content-Type': this.#codec.contentType };
    const channel = this.#sse.get(channelId);
    const respond = (status, packet) => {
      const body = Buffer.from(this.#codec ? this.#codec.encode(packet) : JSON.stringify(packet));
      call.respond({ status, headers: { ...headers, 'Content-Length': body.length }, body });
    };
    if (!channel) {
      // 409, matching the events endpoint: "this channel is gone" is the
      // signal the client recovers from by starting a fresh channel.
      return void respond(409, { type: 'callback', id: '', error: { message: 'Unknown channel', code: 409 } });
    }
    if (!this.#sse.authorized(channel, call.headers)) {
      const error = { message: 'Channel belongs to another session', code: 403 };
      return void respond(403, { type: 'callback', id: '', error });
    }
    handleMessage(channel.client, call.body, this.#router, this.#limits);
    call.respond({ status: 202, headers: { ...headers, 'Content-Length': 0 } });
  }

  async handleHttpCall(call) {
    const headers = buildHeaders(this.#cors, call.headers?.origin);
    if (call.method === 'OPTIONS') {
      return void call.respond({ status: 200, headers });
    }
    // With cors.origins configured, a browser request from a disallowed
    // origin is refused outright, not merely denied the response headers:
    // the page could not read the answer either way, but the call itself
    // would still have RUN — with the cookie session restored — which is
    // exactly the cross-site request an origin allowlist exists to stop.
    if (!isOriginAllowed(this.#cors, call.headers?.origin)) {
      return void new ServerHttpTransport(call, { headers }).error(403);
    }
    // No Content-Type override here: which codec's type applies depends on
    // the MODE (packet vs REST), decided below — a blanket header would
    // advertise the packet framing on REST bodies it never framed.
    const [pathname, params] = split(call.url ?? '/', '?');
    const match = this.matchPath(pathname);
    if (this.#sse) {
      if (pathname === this.eventsPath && (call.method ?? 'GET').toUpperCase() === 'GET') {
        return void this.#handleSseOpen(call, params, headers);
      }
      const channelId = call.headers?.[CHANNEL_HEADER];
      if (channelId && call.method === 'POST' && match?.mode === 'packet') {
        return void this.#handleChannelPost(call, channelId, headers);
      }
    }
    if (!match) {
      return void new ServerHttpTransport(call, { headers }).error(404);
    }
    if (match.mode === 'packet') return this.#handlePacketPost(call, headers);
    return this.#handleRest(call, match.rest, params, headers);
  }

  // The one query-string seam: the injected parser (qs and friends) when
  // configured, the prototype-safe URLSearchParams default otherwise.
  #parseQuery(params) {
    return this.#querystring ? this.#querystring.parse(params ?? '') : parseParams(params);
  }

  // GET {basePath}/events — opens (or re-attaches) an SSE channel. The id
  // may arrive by header (preferred — URLs end up in logs) or query param.
  #handleSseOpen(call, params, headers) {
    const query = this.#parseQuery(params);
    const channelId = call.headers?.[CHANNEL_HEADER] || query.channel || null;
    const lastEventId = call.headers?.['last-event-id'] ?? query.lastEventId ?? null;
    this.#sse.open(call, { channelId, lastEventId, headers });
  }

  // POST {basePath} — a JSON call packet (or a batch array) in the body.
  async #handlePacketPost(call, headers) {
    // Mode-aware: only packet-mode responses carry the packet codec's type.
    if (this.#codec?.contentType) headers['Content-Type'] = this.#codec.contentType;
    const batch = call.method === 'POST' ? this.#batchIds(call.body) : null;
    const transport = new ServerHttpTransport(call, { headers, batch });
    if (call.method !== 'POST') return void transport.error(403);
    const client = this.#addClient(
      transport,
      (c) => this.#restoreToken(c, { headers: call.headers, url: call.url }),
      buildMeta({
        headers: call.headers,
        data: declaredData(call.headers, call.url, this.#metaMax, this.#log),
        url: call.url,
        remoteAddress: call.remoteAddress,
      }),
    );
    // An aborted or never-answered request must still evict the client:
    // the transport only self-closes when it writes a response.
    if (typeof call.onAbort === 'function') call.onAbort(() => transport.emit('close'));
    await client.ready;
    return void handleMessage(client, call.body, this.#router, this.#limits);
  }

  // ANY {basePath}/unit/method?args — REST mode, args from query + body.
  //
  // A cross-site GET/HEAD carries the SameSite=Lax session cookie on
  // top-level navigation, so ambient-authority dispatch of session
  // procedures would be a CSRF hole. Safe methods therefore run WITHOUT the
  // cookie-restored session (public procedures only) unless the request
  // proves intent with a same-origin fetch header.
  async #handleRest(call, rest, params, headers) {
    const method = (call.method ?? 'GET').toUpperCase();
    // The REST body codec (codec.rest), when configured, re-frames every
    // REST body — declared and conventional, results, errors and requests.
    // The PACKET codec never applies here: REST's default audience is curl
    // and browsers, and its bodies are values, not packet frames.
    const restCodec = this.#restCodec;
    if (restCodec?.contentType) headers['Content-Type'] = restCodec.contentType;
    // Declarative routes first: a procedure that mapped itself onto a verb
    // and path owns that path. The conventional /:unit/:method mode stays
    // as the fallback, so introspection-driven callers keep working.
    const route = this.#router.hasRestRoutes ? this.#matchDeclaredRoute(method, rest) : null;
    // Declarative-route refusals answer in REST shape too (a plain wire
    // error object), not as callback envelopes — same contract as a hit.
    if (route?.malformed) {
      return void new ServerHttpTransport(call, { headers, rest: { codec: restCodec } }).error(400);
    }
    if (route?.allowed) {
      const headersWithAllow = { ...headers, Allow: route.allowed.join(', ') };
      return void new ServerHttpTransport(call, { headers: headersWithAllow, rest: { codec: restCodec } }).error(405);
    }
    const transport = new ServerHttpTransport(call, {
      headers,
      rest: route ? { status: route.http.status, codec: restCodec } : null,
    });
    const safeMethod = method === 'GET' || method === 'HEAD';
    // Same ambient-only CSRF reasoning as delegatedContext above.
    const restore =
      !safeMethod || this.#sessions.transport.ambient !== true || this.#isSameOriginFetch(call.headers)
        ? (c) => this.#restoreToken(c, { headers: call.headers, url: call.url })
        : null;
    // For a per-request client the connection IS the call, so the declared
    // data doubles as this call's meta: a curl caller passes x-wrpc-meta and
    // a hook reads context.callMeta, same as on ws.
    const data = declaredData(call.headers, call.url, this.#metaMax, this.#log);
    const client = this.#addClient(
      transport,
      restore,
      buildMeta({ headers: call.headers, data, url: call.url, remoteAddress: call.remoteAddress }),
    );
    // #addClient assigned the packet codec; REST bodies are not packet
    // frames, so it comes back off. The conventional mode's callback
    // envelope IS the body value, so the rest codec (when configured)
    // takes the packet codec's slot on the transport.
    transport.codec = restCodec;
    if (typeof call.onAbort === 'function') call.onAbort(() => transport.emit('close'));
    await client.ready;
    // A request body under a rest codec that fails to decode is the
    // caller's malformed input: 400 in REST shape, never a throw upward.
    const decodeBody = (fallback) => {
      const raw = call.body;
      if (raw === undefined || raw === null || raw.length === 0) return fallback;
      if (!restCodec) return jsonParse(raw) ?? fallback;
      return restCodec.decode(raw);
    };
    const id = this.#generateId();
    if (route) {
      // REST semantics: the request arrives structured, and the SAME shape
      // is what a ws caller passes by hand — the mapping only defines how
      // an HTTP request is unpacked into args.
      let body;
      try {
        body = decodeBody(undefined);
      } catch {
        return void transport.error(400);
      }
      const args = { params: route.params, query: this.#parseQuery(params), body };
      const packet = { type: 'call', id, method: `${route.unitKey}/${route.methodName}`, args };
      if (data) packet.meta = data;
      return void handleRpc(client, packet, this.#router);
    }
    const parameters = this.#parseQuery(params);
    const [unit, name] = split(rest, '/');
    let body;
    try {
      body = decodeBody(null) ?? {};
    } catch {
      return void transport.error(400);
    }
    const args = { ...parameters, ...body };
    const packet = { type: 'call', id, method: `${unit}/${name}`, args };
    if (data) packet.meta = data;
    return void handleRpc(client, packet, this.#router);
  }

  // Splits and percent-decodes the path, then consults the router's trie.
  // A malformed escape answers 400 rather than throwing into the host.
  #matchDeclaredRoute(method, rest) {
    const raw = rest.split('/');
    const segments = new Array(raw.length);
    for (let i = 0; i < raw.length; i++) {
      try {
        segments[i] = decodeURIComponent(raw[i]);
      } catch {
        return { malformed: true };
      }
    }
    return this.#router.matchRest(method, segments);
  }

  // Fetch metadata (sent by every modern browser, absent for non-browser
  // peers): a cross-site top-level navigation is exactly what CSRF uses.
  #isSameOriginFetch(headers = {}) {
    const site = headers['sec-fetch-site'];
    if (!site) return true; // curl, server-to-server, older clients
    return site === 'same-origin' || site === 'none';
  }

  /** True while drain() runs: new calls are refused with 503. */
  get draining() {
    return this.#draining;
  }

  /**
   * The graceful half of a shutdown: stop taking new calls (they answer
   * 503) and wait up to `timeout` ms for the in-flight ones to settle.
   * Subscriptions are deliberately NOT waited for — a live feed has no
   * natural end; it is ended by the close that follows. Resolves early the
   * moment nothing is in flight; a 0/absent timeout is a no-op.
   */
  async drain(timeout = 0) {
    if (!(timeout > 0)) return;
    this.#draining = true;
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      let busy = 0;
      for (const client of this.#clients) busy += client.calls.size;
      if (busy === 0) return;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }

  async close() {
    if (this.#sse) this.#sse.close();
    for (const client of this.#clients) client.close();
    this.#clients.clear();
    this.#byId.clear();
    // The goodbye goes out first, while the backplane binder still works:
    // other nodes evict this instance immediately instead of waiting out
    // the presence timeout.
    this.#cluster.close();
    // Unsubscribe before dropping the rooms, so the registry's last-member
    // callbacks have nothing left to do. The injected backplane itself is
    // never closed here: its lifetime belongs to whoever created it.
    if (this.#backplane) this.#backplane.close();
    this.#rooms.clear();
  }
}

module.exports = { RpcServer, Client, Context, rpcOptions, DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_CALLS };
