'use strict';

const { Emitter, jsonParse, isCodec } = require('../utils.js');
const { generateUUID } = require('../runtime/node.js');
const { ServerTransport, buildHeaders, isOriginAllowed } = require('../transport.js');
const { SessionManager } = require('./sessions.js');
const { defineRouter, procedure, runHooksSafe } = require('./router.js');
const { RoomRegistry, Broadcast, RoomsBackplane } = require('./rooms.js');
const { Cluster } = require('./cluster.js');
const { SseChannels } = require('../sse/server.js');
// The channel header from the import-free constants module, NOT from
// sse/server.js: the string is shared, the implementation is not.
const { CHANNEL_HEADER } = require('../wire.js');
const { isBackplane } = require('../scaling/index.js');
const {
  handleMessage,
  handleBinary,
  handleRpc,
  split,
  parseParams,
  DEFAULT_MAX_BATCH,
  UNKNOWN_TARGET,
} = require('./dispatcher.js');
const { createLoggerWriter } = require('../logging.js');
const { createServerTelemetry } = require('../telemetry/server.js');
const { TRACEPARENT, TRACESTATE } = require('../telemetry/shared.js');
const { Context, Client, DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_CALLS, buildMeta } = require('./client.js');
const { DEFAULT_META_MAX, declaredHeaders, declaredData } = require('./meta.js');

// After this long an unsettled onConnect chain logs a warning: a hook that
// never resolves holds the client's dispatch (see #addClient), and the warn
// is the only trace that hang would leave.
const ONCONNECT_STALL_MS = 5_000;

const ServerHttpTransport = ServerTransport.transport.http;
const ServerWsTransport = ServerTransport.transport.ws;
const ServerEventTransport = ServerTransport.transport.event;

// call joins the caller's trace exactly like a packet call does.
const copyTraceHeaders = (headers, packet) => {
  const parent = headers?.traceparent;
  if (typeof parent !== 'string' || parent.length === 0) return;
  packet[TRACEPARENT] = parent;
  const state = headers.tracestate;
  if (typeof state === 'string' && state.length > 0) packet[TRACESTATE] = state;
};

// A capability refusal ("this transport cannot carry that") is part of the
// protocol conversation, not a server internal: 400-coded and exposed so
// the peer reads the actual reason instead of a masked 500.

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
  'rooms',
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
      rooms = {},
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
    this.#initRooms(backplane, cluster, rooms);
    // A channel's client is built from the GET that opened the stream, so it
    // restores the session from that request's cookie the way attachSocket
    // does — otherwise a browser holding a valid cookie starts the channel
    // anonymous and every `access: 'session'` procedure on it answers 403.
    const addClient = (transport, call) => {
      const data = declaredData(call.headers, split(call.url ?? '', '?')[1], this.#metaMax, this.#sseLog);
      return this.#addClient(
        transport,
        (client) =>
          this.#restoreToken(client, { headers: call.headers, url: call.url, declared: call.headers, meta: data }),
        buildMeta({ headers: call.headers, data, url: call.url, remoteAddress: call.remoteAddress }),
      );
    };
    // The identity a request presents, bound to a channel at creation and
    // required again on every re-attach and channel POST — the id alone
    // must never be enough to act as the channel's session.
    const channelKey = (headers) => this.#requestKey(headers);
    this.#sse =
      sse === false ? null : new SseChannels({ ...sse, log: this.#sseLog, otel: this.#otel, addClient, channelKey });
  }

  #initRooms(backplane, clusterOptions, roomsOptions = {}) {
    // The cluster exists with or without a backplane — without one every
    // operation degrades to its local half, so application code written
    // against `server.cluster` never branches on the deployment.
    // `cluster: false` opts out of the cluster layer HONESTLY (it used to
    // be silently ignored): the Cluster is built without the backplane, so
    // presence replication, commands and asks degrade to their local
    // halves, while the rooms backplane below is untouched.
    const enabled = clusterOptions !== false;
    const cluster = new Cluster({
      backplane: enabled ? backplane : null,
      instance: this.#instance,
      local: this.#clusterOps(),
      log: this.#log.child({ component: 'cluster' }),
      otel: this.#otel,
      generateId: this.#generateId,
      options: enabled ? clusterOptions : {},
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
      linger: roomsOptions?.linger,
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

  // The telemetry writer, for hosts that run procedures OUTSIDE the
  // dispatcher (the fastify adapter's delegated routes): they bracket
  // invokeBare with the same spans/metrics the packet path gets, without
  // reaching into private state. The writer's shape is @experimental, like
  // the telemetry option it reflects.
  get otel() {
    return this.#otel;
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
    const data = declaredData(headers, split(url ?? '', '?')[1], this.#metaMax, this.#log);
    const restore =
      !safeMethod || !ambient || this.#isSameOriginFetch(headers)
        ? (c) => this.#restoreToken(c, { headers, url, declared: headers, meta: data })
        : null;
    const client = this.#addClient(transport, restore, buildMeta({ headers, data, url, remoteAddress }));
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
  // Besides the raw { headers, url }, read() receives what the core already
  // parsed — `declared` (the merged declared+observed header bag, wrpc_h
  // included and capped on the configurable metaMaxBytes) and `meta` (the
  // sanitized connection-metadata bag, both spellings merged) — so a
  // strategy never re-implements the wire parsing and cannot drift from it.
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
    const merged = declared ? { ...declared, ...meta.headers } : meta.headers;
    const data = declaredData(meta.headers, split(meta.url ?? '', '?')[1], this.#metaMax, this.#log);
    const client = this.#addClient(
      transport,
      (c) => this.#restoreToken(c, { headers: meta.headers, url: meta.url, declared: merged, meta: data }),
      buildMeta({
        headers: merged,
        data,
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
      // A configuration error, not routine traffic: the offending origin is
      // the one fact the operator needs. Counted on the calls series with
      // UNKNOWN_TARGET so refused traffic shows up next to answered traffic
      // without new cardinality — the rule every early refusal below shares
      // (these paths run before any Client exists, so nothing else records).
      this.#log.warn({ event: 'cors.refused', origin: call.headers?.origin });
      this.#otel.recordCall(UNKNOWN_TARGET, 'error', 403);
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
      this.#log.warn({ event: 'http.refused', code: 404, path: pathname });
      this.#otel.recordCall(UNKNOWN_TARGET, 'error', 404);
      return void new ServerHttpTransport(call, { headers }).error(404);
    }
    if (match.mode === 'packet') return this.#handlePacketPost(call, headers, params);
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
  async #handlePacketPost(call, headers, params) {
    // Mode-aware: only packet-mode responses carry the packet codec's type.
    if (this.#codec?.contentType) headers['Content-Type'] = this.#codec.contentType;
    const batch = call.method === 'POST' ? this.#batchIds(call.body) : null;
    const transport = new ServerHttpTransport(call, { headers, batch });
    if (call.method !== 'POST') {
      this.#log.warn({ event: 'http.refused', code: 403, method: call.method });
      this.#otel.recordCall(UNKNOWN_TARGET, 'error', 403);
      return void transport.error(403);
    }
    const data = declaredData(call.headers, params, this.#metaMax, this.#log);
    const client = this.#addClient(
      transport,
      (c) => this.#restoreToken(c, { headers: call.headers, url: call.url, declared: call.headers, meta: data }),
      buildMeta({ headers: call.headers, data, url: call.url, remoteAddress: call.remoteAddress }),
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
      this.#log.warn({ event: 'http.refused', code: 400, path: rest });
      this.#otel.recordCall(UNKNOWN_TARGET, 'error', 400);
      return void new ServerHttpTransport(call, { headers, rest: { codec: restCodec } }).error(400);
    }
    if (route?.allowed) {
      this.#log.warn({ event: 'http.refused', code: 405, method, path: rest });
      this.#otel.recordCall(UNKNOWN_TARGET, 'error', 405);
      const headersWithAllow = { ...headers, Allow: route.allowed.join(', ') };
      return void new ServerHttpTransport(call, { headers: headersWithAllow, rest: { codec: restCodec } }).error(405);
    }
    const transport = new ServerHttpTransport(call, {
      headers,
      rest: route ? { status: route.http.status, codec: restCodec } : null,
    });
    const safeMethod = method === 'GET' || method === 'HEAD';
    // For a per-request client the connection IS the call, so the declared
    // data doubles as this call's meta: a curl caller passes x-wrpc-meta and
    // a hook reads context.callMeta, same as on ws.
    const data = declaredData(call.headers, params, this.#metaMax, this.#log);
    // Same ambient-only CSRF reasoning as delegatedContext above.
    const restore =
      !safeMethod || this.#sessions.transport.ambient !== true || this.#isSameOriginFetch(call.headers)
        ? (c) => this.#restoreToken(c, { headers: call.headers, url: call.url, declared: call.headers, meta: data })
        : null;
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
      copyTraceHeaders(call.headers, packet);
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
    copyTraceHeaders(call.headers, packet);
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
   * False while a backplane channel subscribe is failing and being retried
   * (rooms or cluster): the node can publish but cannot HEAR — the
   * half-connected state a readiness probe should drain it on. Always true
   * without a backplane.
   */
  get healthy() {
    return (this.#backplane === null || this.#backplane.healthy) && this.#cluster.healthy;
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
