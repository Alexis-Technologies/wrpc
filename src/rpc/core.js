'use strict';

const http = require('node:http');

const { Emitter, jsonParse } = require('../utils.js');
const { generateUUID } = require('../runtime/node.js');
const { ServerTransport, buildHeaders, parseCookies } = require('../transport.js');
const { WrpcWritable } = require('../streams.js');
const { SessionManager } = require('./sessions.js');
const { defineRouter, procedure } = require('./router.js');
const { RoomRegistry, Broadcast, RoomsBackplane } = require('./rooms.js');
const { SseChannels, CHANNEL_HEADER } = require('../sse/server.js');
const { isBackplane } = require('../scaling/index.js');
const { handleMessage, handleBinary, handleRpc, split, parseParams, DEFAULT_MAX_BATCH } = require('./dispatcher.js');
const { createLoggerWriter } = require('../logging.js');

// One peer holding thousands of open generators is a denial of service the
// application never opted into; the cap is generous but present.
const DEFAULT_MAX_SUBSCRIPTIONS = 256;

const ServerHttpTransport = ServerTransport.transport.http;
const ServerWsTransport = ServerTransport.transport.ws;
const ServerEventTransport = ServerTransport.transport.event;

class Context {
  #log = null;

  constructor(client, signal = null) {
    this.client = client;
    this.uuid = generateUUID();
    this.state = {};
    // Aborted when the caller cancels, unsubscribes, or disconnects. A
    // handler that awaits anything long-lived should pass it along; one
    // that ignores it simply runs to completion and has its result dropped.
    this.signal = signal;
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
}

class Client extends Emitter {
  #transport = null;
  #sessions = null;
  #rooms = null;
  #server = null;
  #log = null;

  constructor(transport, { sessions, rooms, server, log, maxSubscriptions = DEFAULT_MAX_SUBSCRIPTIONS } = {}) {
    super();
    this.#transport = transport;
    this.#sessions = sessions;
    // A Client built outside an RpcServer still has working rooms; it just
    // has nobody to share them with.
    this.#rooms = rooms ?? new RoomRegistry();
    this.#server = server ?? null;
    // Connection-scoped, bound once: a connection lives for minutes, and the
    // binding hoists the peer id out of every line logged for it.
    this.#log = createLoggerWriter(log ?? globalThis.console).child({ peer: transport.source });
    this.source = transport.source;
    this.session = null;
    this.sessionReady = Promise.resolve();
    this.streams = new Map();
    // id -> AbortController, for the two things a peer can take back.
    this.calls = new Map();
    this.subscriptions = new Map();
    this.maxSubscriptions = maxSubscriptions;
  }

  error(code, { id = '', error = null } = {}) {
    const httpCode = code <= 599 ? code : 500;
    const status = http.STATUS_CODES[httpCode];
    const info = error ? error.stack : status || 'Unknown error';
    this.#transport.error(code, { id, error });
    this.#log.error({ event: 'rpc.error', code, id, err: error }, `${this.source}\t${code}\t${info}`);
  }

  /** The connection-scoped writer, reached by the dispatcher and handlers. */
  get log() {
    return this.#log;
  }

  // Diagnostics for packets that carry no id to answer on (inbound events):
  // the log is the only channel left.
  warn(message, entry = {}) {
    this.#log.warn({ event: 'rpc.warn', ...entry }, `${this.source}\t${message}`);
  }

  /** Returns false when the transport is above its high-water mark. */
  send(obj, options = {}) {
    const { code, method } = options;
    const flushed = this.#transport.send(obj, code);
    const isSuccessCallback = obj.type === 'callback' && !obj.error;
    if (isSuccessCallback) {
      this.#log.log({ event: 'call.ok', method, id: obj.id }, `${this.source}\tCALL\t${method}\tOK`);
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

  createContext(signal = null) {
    return new Context(this, signal);
  }

  emit(name, data) {
    if (name === 'close') return super.emit(name, data);
    this.sendEvent(name, data);
    return Promise.resolve();
  }

  sendEvent(name, data) {
    const packet = { type: 'event', name, data };
    if (!this.#transport.connection) {
      throw new Error(`Can't send wrpc event to http transport`);
    }
    this.send(packet);
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
      throw new Error(`Can't receive stream from http transport`);
    }
    if (!this.binary) throw new Error(`Can't receive stream over a text-only transport`);
    const stream = this.streams.get(id);
    if (stream) return stream;
    throw new Error(`Stream ${id} is not initialized`);
  }

  createStream(name, size) {
    if (!this.#transport.connection) {
      throw new Error(`Can't send wrpc streams to http transport`);
    }
    if (!this.binary) throw new Error(`Can't send wrpc streams over a text-only transport`);
    if (!name) throw new Error('Stream name is not provided');
    if (!size) throw new Error('Stream size is not provided');
    const id = generateUUID();
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
      this.#transport.sendSessionCookie(this.#sessions.cookieHeader(this.session.token));
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
    this.emit('close');
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
  'backplane',
  'instanceId',
  'maxBatch',
  'maxSubscriptions',
  'sse',
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
  #instance;
  #cors;
  #basePath;
  #log;
  #roomsLog;
  #sseLog;
  #limits;
  #sse = null;
  #clients = new Set();

  constructor(options = {}) {
    super();
    const {
      router,
      sessions,
      cors = null,
      basePath = DEFAULT_BASE_PATH,
      logger = globalThis.console,
      backplane = null,
      instanceId = generateUUID(),
      maxBatch = DEFAULT_MAX_BATCH,
      maxSubscriptions = DEFAULT_MAX_SUBSCRIPTIONS,
      sse = {},
    } = options;
    if (!router || typeof router.getProcedure !== 'function') {
      throw new TypeError('RpcServer: options.router (a Router from defineRouter) is required');
    }
    if (backplane && !isBackplane(backplane)) {
      throw new TypeError('RpcServer: options.backplane does not implement the backplane contract');
    }
    this.#log = createLoggerWriter(logger);
    // Built once here rather than per broadcast or per channel: Broadcast is
    // constructed on every to()/except()/broadcast().
    this.#roomsLog = this.#log.child({ component: 'rooms' });
    this.#sseLog = this.#log.child({ component: 'sse' });
    this.#sessions = new SessionManager(sessions, this.#log.child({ component: 'sessions' }));
    this.#cors = cors;
    this.#basePath = normalizeBasePath(basePath);
    this.#instance = instanceId;
    this.#limits = { maxBatch, maxSubscriptions };
    this.#router = this.#withIntrospection(router);
    this.#initRooms(backplane);
    // A channel's client is built from the GET that opened the stream, so it
    // restores the session from that request's cookie the way attachSocket
    // does — otherwise a browser holding a valid cookie starts the channel
    // anonymous and every `access: 'session'` procedure on it answers 403.
    const addClient = (transport, headers) => {
      const client = this.#addClient(transport);
      client.sessionReady = this.#restoreFromCookie(client, headers);
      return client;
    };
    this.#sse = sse === false ? null : new SseChannels({ ...sse, log: this.#sseLog, addClient });
  }

  #initRooms(backplane) {
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
    });
    binder.start();
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

  get clients() {
    return new Set(this.#clients);
  }

  #target() {
    return new Broadcast({
      registry: this.#rooms,
      clients: () => this.#clients,
      publish: this.#backplane ? (envelope) => this.#backplane.publish(envelope) : null,
      log: this.#roomsLog,
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

  #withIntrospection(router) {
    if (router.getProcedure('system', '*', 'introspect')) return router;
    const system = defineRouter({
      system: {
        introspect: procedure({
          access: 'public',
          handler: async (_context, units) => this.#router.introspect(units),
        }),
      },
    });
    return router.merge(system);
  }

  #addClient(transport) {
    const options = {
      sessions: this.#sessions,
      rooms: this.#rooms,
      server: this,
      log: this.#log,
      maxSubscriptions: this.#limits.maxSubscriptions,
    };
    const client = new Client(transport, options);
    this.#clients.add(client);
    transport.once('close', () => {
      client.destroy();
      this.#clients.delete(client);
    });
    return client;
  }

  #restoreFromCookie(client, headers) {
    const cookie = headers?.cookie;
    if (!cookie) return Promise.resolve(false);
    const token = this.#sessions.readToken(parseCookies(cookie));
    if (!token) return Promise.resolve(false);
    return client.restoreSession(token).catch((error) => {
      this.#log.error({ err: error, event: 'session.restore' });
      return false;
    });
  }

  attachSocket(socket, meta = {}) {
    const transport = new ServerWsTransport(socket, meta);
    const client = this.#addClient(transport);
    client.sessionReady = this.#restoreFromCookie(client, meta.headers);

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

  attachPort(port) {
    const transport = new ServerEventTransport(port);
    const client = this.#addClient(transport);
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

  // A batch frame needs to be recognized BEFORE the transport exists: the
  // transport has to know how many answers to collect and in which order to
  // emit them, which only the request's own id list can tell it.
  #batchIds(body) {
    const packet = jsonParse(body);
    if (!Array.isArray(packet)) return null;
    if (packet.length === 0 || packet.length > this.#limits.maxBatch) return null;
    return packet.map((item) => (item && typeof item === 'object' ? item.id : undefined));
  }

  get sse() {
    return this.#sse;
  }

  // The SSE endpoint sits just under basePath so it moves with it, and it is
  // the one route whose response is a stream rather than a body.
  get eventsPath() {
    return `${this.#basePath}/events`;
  }

  // A POST carrying a live channel id belongs to that channel's client, not
  // to a fresh request/response one: that is what lets a subscription opened
  // by a POST deliver its values down the peer's event stream. The POST
  // itself answers 202 — every reply travels on the stream.
  //
  // `headers` are the same CORS-bearing response headers every other HTTP
  // answer carries: without them a browser on another origin cannot read
  // this response at all, which makes cross-origin SSE impossible.
  #handleChannelPost(call, channelId, headers) {
    const channel = this.#sse.get(channelId);
    const respond = (status, packet) => {
      const body = Buffer.from(JSON.stringify(packet));
      call.respond({ status, headers: { ...headers, 'Content-Length': body.length }, body });
    };
    if (!channel) {
      return void respond(404, { type: 'callback', id: '', error: { message: 'Unknown channel', code: 404 } });
    }
    handleMessage(channel.client, call.body, this.#router, this.#limits);
    call.respond({ status: 202, headers: { ...headers, 'Content-Length': 0 } });
  }

  async handleHttpCall(call) {
    const headers = buildHeaders(this.#cors, call.headers?.origin);
    if (call.method === 'OPTIONS') {
      return void call.respond({ status: 200, headers });
    }
    const [pathname, params] = split(call.url ?? '/', '?');
    if (this.#sse) {
      if (pathname === this.eventsPath && (call.method ?? 'GET').toUpperCase() === 'GET') {
        const query = parseParams(params);
        const channelId = query.channel || call.headers?.[CHANNEL_HEADER] || undefined;
        const lastEventId = call.headers?.['last-event-id'] ?? query.lastEventId ?? null;
        return void this.#sse.open(call, { channelId, lastEventId, headers });
      }
      const channelId = call.headers?.[CHANNEL_HEADER];
      if (channelId && call.method === 'POST' && this.matchPath(pathname)?.mode === 'packet') {
        return void this.#handleChannelPost(call, channelId, headers);
      }
    }
    const isPacketPost = call.method === 'POST' && this.matchPath(pathname)?.mode === 'packet';
    const batch = isPacketPost ? this.#batchIds(call.body) : null;
    const transport = new ServerHttpTransport(call, { headers, batch });
    const match = this.matchPath(pathname);
    if (!match) return void transport.error(404);

    const client = this.#addClient(transport);
    // An aborted or never-answered request must still evict the client:
    // the transport only self-closes when it writes a response.
    if (typeof call.onAbort === 'function') call.onAbort(() => transport.emit('close'));

    if (match.mode === 'packet') {
      if (call.method !== 'POST') return void transport.error(403);
      await this.#restoreFromCookie(client, call.headers);
      return void handleMessage(client, call.body, this.#router, this.#limits);
    }
    // REST mode: a cross-site GET/HEAD carries the SameSite=Lax session
    // cookie on top-level navigation, so ambient-authority dispatch of
    // session procedures would be a CSRF hole. Safe methods therefore
    // run WITHOUT the cookie-restored session (public procedures only)
    // unless the request proves intent with a same-origin fetch header.
    const method = (call.method ?? 'GET').toUpperCase();
    const safeMethod = method === 'GET' || method === 'HEAD';
    if (!safeMethod || this.#isSameOriginFetch(call.headers)) {
      await this.#restoreFromCookie(client, call.headers);
    }
    const parameters = parseParams(params);
    const [unit, name] = split(match.rest, '/');
    const body = jsonParse(call.body) || {};
    const args = { ...parameters, ...body };
    const id = generateUUID();
    const packet = { type: 'call', id, method: `${unit}/${name}`, args };
    return void handleRpc(client, packet, this.#router);
  }

  // Fetch metadata (sent by every modern browser, absent for non-browser
  // peers): a cross-site top-level navigation is exactly what CSRF uses.
  #isSameOriginFetch(headers = {}) {
    const site = headers['sec-fetch-site'];
    if (!site) return true; // curl, server-to-server, older clients
    return site === 'same-origin' || site === 'none';
  }

  async close() {
    if (this.#sse) this.#sse.close();
    for (const client of this.#clients) client.close();
    this.#clients.clear();
    // Unsubscribe before dropping the rooms, so the registry's last-member
    // callbacks have nothing left to do. The injected backplane itself is
    // never closed here: its lifetime belongs to whoever created it.
    if (this.#backplane) this.#backplane.close();
    this.#rooms.clear();
  }
}

module.exports = { RpcServer, Client, Context, rpcOptions, DEFAULT_MAX_SUBSCRIPTIONS };
