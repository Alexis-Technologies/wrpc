'use strict';

// The per-connection halves of the engine-agnostic core: `Context` (one
// invocation's view) and `Client` (one peer). Split out of rpc/core.js —
// they reach the RpcServer only through the options bag #addClient builds,
// so the dependency is one-way: core requires this file, never the reverse.

const http = require('node:http');

const { Emitter, jsonParse } = require('../utils.js');
const { generateUUID } = require('../runtime/node.js');
const { WrpcWritable } = require('../streams.js');
const { RoomRegistry } = require('./rooms.js');
const { DEFAULT_META_MAX } = require('./meta.js');
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

module.exports = {
  Context,
  Client,
  DEFAULT_MAX_SUBSCRIPTIONS,
  DEFAULT_MAX_CALLS,
  FROZEN_EMPTY,
  EMPTY_META,
  buildMeta,
};
