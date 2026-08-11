'use strict';

const http = require('node:http');

const { Emitter, jsonParse } = require('../utils.js');
const { generateUUID } = require('../runtime/node.js');
const { ServerTransport, buildHeaders, parseCookies } = require('../transport.js');
const { WrpcWritable } = require('../streams.js');
const { SessionManager } = require('./sessions.js');
const { defineRouter, procedure } = require('./router.js');
const { RoomRegistry, Broadcast, RoomsBackplane } = require('./rooms.js');
const { isBackplane } = require('../scaling/index.js');
const { handleMessage, handleBinary, handleRpc, split, parseParams } = require('./dispatcher.js');

const ServerHttpTransport = ServerTransport.transport.http;
const ServerWsTransport = ServerTransport.transport.ws;
const ServerEventTransport = ServerTransport.transport.event;

class Context {
  constructor(client) {
    this.client = client;
    this.uuid = generateUUID();
    this.state = {};
  }

  get session() {
    return this.client.session;
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
  #console = null;

  constructor(transport, { sessions, rooms, server, console } = {}) {
    super();
    this.#transport = transport;
    this.#sessions = sessions;
    // A Client built outside an RpcServer still has working rooms; it just
    // has nobody to share them with.
    this.#rooms = rooms ?? new RoomRegistry();
    this.#server = server ?? null;
    this.#console = console;
    this.source = transport.source;
    this.session = null;
    this.sessionReady = Promise.resolve();
    this.streams = new Map();
  }

  error(code, { id = '', error = null } = {}) {
    const httpCode = code <= 599 ? code : 500;
    const status = http.STATUS_CODES[httpCode];
    const info = error ? error.stack : status || 'Unknown error';
    this.#transport.error(code, { id, error });
    this.#console.error(`${this.source}\t${code}\t${info}`);
  }

  // Diagnostics for packets that carry no id to answer on (inbound events):
  // the log is the only channel left.
  warn(message) {
    this.#console.warn(`${this.source}\t${message}`);
  }

  send(obj, options = {}) {
    const { code, method } = options;
    this.#transport.send(obj, code);
    const isSuccessCallback = obj.type === 'callback' && !obj.error;
    if (!isSuccessCallback) return;
    this.#console.log(`${this.source}\tCALL\t${method}\tOK`);
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

  createContext() {
    return new Context(this);
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
    const stream = this.streams.get(id);
    if (stream) return stream;
    throw new Error(`Stream ${id} is not initialized`);
  }

  createStream(name, size) {
    if (!this.#transport.connection) {
      throw new Error(`Can't send wrpc streams to http transport`);
    }
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
    const console = this.#console;
    this.#rooms.leaveAll(this);
    this.emit('close');
    for (const stream of this.streams.values()) {
      if (typeof stream.terminate !== 'function') continue;
      Promise.resolve(stream.terminate()).catch((error) => {
        console.error(error);
      });
    }
    this.streams.clear();
  }
}

const DEFAULT_BASE_PATH = '/api';

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
  #console;
  #clients = new Set();

  constructor(options = {}) {
    super();
    const {
      router,
      sessions,
      cors = null,
      basePath = DEFAULT_BASE_PATH,
      console = globalThis.console,
      backplane = null,
      instanceId = generateUUID(),
    } = options;
    if (!router || typeof router.getProcedure !== 'function') {
      throw new TypeError('RpcServer: options.router (a Router from defineRouter) is required');
    }
    if (backplane && !isBackplane(backplane)) {
      throw new TypeError('RpcServer: options.backplane does not implement the backplane contract');
    }
    this.#sessions = new SessionManager(sessions, console);
    this.#cors = cors;
    this.#basePath = normalizeBasePath(basePath);
    this.#console = console;
    this.#instance = instanceId;
    this.#router = this.#withIntrospection(router);
    this.#initRooms(backplane);
  }

  #initRooms(backplane) {
    if (!backplane) {
      this.#rooms = new RoomRegistry();
      return;
    }
    const binder = new RoomsBackplane({
      backplane,
      instance: this.#instance,
      console: this.#console,
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
      console: this.#console,
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
    const options = { sessions: this.#sessions, rooms: this.#rooms, server: this, console: this.#console };
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
      this.#console.error(error);
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
      if (!isBinary) return void handleMessage(client, data, this.#router);
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
        handleMessage(client, data, this.#router);
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

  async handleHttpCall(call) {
    const headers = buildHeaders(this.#cors, call.headers?.origin);
    if (call.method === 'OPTIONS') {
      return void call.respond({ status: 200, headers });
    }
    const transport = new ServerHttpTransport(call, { headers });
    const [pathname, params] = split(call.url ?? '/', '?');
    const match = this.matchPath(pathname);
    if (!match) return void transport.error(404);

    const client = this.#addClient(transport);
    // An aborted or never-answered request must still evict the client:
    // the transport only self-closes when it writes a response.
    if (typeof call.onAbort === 'function') call.onAbort(() => transport.emit('close'));

    if (match.mode === 'packet') {
      if (call.method !== 'POST') return void transport.error(403);
      await this.#restoreFromCookie(client, call.headers);
      return void handleMessage(client, call.body, this.#router);
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
    for (const client of this.#clients) client.close();
    this.#clients.clear();
    // Unsubscribe before dropping the rooms, so the registry's last-member
    // callbacks have nothing left to do. The injected backplane itself is
    // never closed here: its lifetime belongs to whoever created it.
    if (this.#backplane) this.#backplane.close();
    this.#rooms.clear();
  }
}

module.exports = { RpcServer, Client, Context };
