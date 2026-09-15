'use strict';

// The server half of a wrpc peer: a router, a dispatcher and one Client
// per attached peer transport — the browser-safe subset of RpcServer, and
// deliberately not RpcServer itself. Sessions, the HTTP/REST/SSE surfaces
// and the cluster are dead weight in a page and drag node:crypto along;
// what a peer needs is exactly what is composed here from the same
// modules the server uses: Client (rpc/client.js), handleMessage /
// handleBinary (rpc/dispatcher.js), RoomRegistry + Broadcast
// (rpc/rooms.js) and the router's introspection. The ~60 lines that
// mirror RpcServer's #addClient / #withIntrospection / #target are a
// conscious duplication (see the plan in docs/guide/webrtc.md).
//
// Trust: a peer's procedures default to access 'session' like every wrpc
// procedure, and there is no session manager here. With `trust: 'link'`
// (the default) every attached client carries a frozen pseudo-session
// whose token is the peer id — a link only exists after signaling through
// a server that admitted both peers, plus the local accept() hook, so the
// link IS the authentication and handlers see `context.session.data.peer`.
// `trust: 'none'` leaves session null: only access 'public' procedures
// answer, and authorization is the application's, in hooks, from
// `context.meta.data.peer`.

const { Emitter, isCodec } = require('../utils.js');
const { createLoggerWriter } = require('../logging.js');
const { createServerTelemetry } = require('../telemetry/server.js');
const { generateUUID } = require('../runtime/node.js');
const { defineRouter, procedure, runHooksSafe } = require('../rpc/router.js');
const { RoomRegistry, Broadcast } = require('../rpc/rooms.js');
const { Client, DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_CALLS, buildMeta } = require('../rpc/client.js');
const { handleMessage, handleBinary } = require('../rpc/dispatcher.js');
const { DEFAULT_META_MAX } = require('../rpc/meta.js');

const TRUST = ['link', 'none'];
const ONCONNECT_STALL_MS = 5000;

const isInboundTransport = (transport) =>
  typeof transport === 'object' &&
  transport !== null &&
  typeof transport.write === 'function' &&
  typeof transport.close === 'function' &&
  typeof transport.on === 'function' &&
  typeof transport.once === 'function' &&
  Boolean(transport.connection);

class PeerHost extends Emitter {
  #otel;
  #router;
  #codec;
  #log;
  #roomsLog;
  #rooms = new RoomRegistry();
  #clients = new Set();
  #byId = new Map();
  #limits;
  #generateId;
  #metaMax;
  #trust;
  #instance;

  constructor({
    router,
    codec = null,
    logger = false,
    generateId = null,
    introspection = true,
    maxBatch = undefined,
    maxSubscriptions = DEFAULT_MAX_SUBSCRIPTIONS,
    maxCalls = DEFAULT_MAX_CALLS,
    metaMaxBytes = DEFAULT_META_MAX,
    trust = 'link',
    instanceId = null,
    telemetry = null,
  } = {}) {
    super();
    if (!router || typeof router.getProcedure !== 'function') {
      throw new TypeError('PeerHost: options.router (a Router from defineRouter) is required');
    }
    if (codec !== null && !isCodec(codec)) {
      throw new TypeError('PeerHost: options.codec must provide encode(packet)/decode(text)');
    }
    if (!TRUST.includes(trust)) throw new TypeError(`PeerHost: options.trust must be one of ${TRUST.join(', ')}`);
    if (instanceId !== null && (typeof instanceId !== 'string' || instanceId.includes('.') || instanceId === '')) {
      throw new TypeError('PeerHost: options.instanceId must be a non-empty string without "."');
    }
    this.#log = createLoggerWriter(logger);
    // The same server-side writer RpcServer uses: spans for the calls this
    // peer answers, the connection gauge, and the rtc instruments.
    this.#otel = createServerTelemetry(telemetry);
    this.#roomsLog = this.#log.child({ component: 'rooms' });
    this.#codec = codec && typeof codec.encode === 'function' && typeof codec.decode === 'function' ? codec : null;
    this.#generateId = typeof generateId === 'function' ? generateId : generateUUID;
    this.#metaMax = Number.isInteger(metaMaxBytes) && metaMaxBytes > 0 ? metaMaxBytes : DEFAULT_META_MAX;
    this.#limits = { maxBatch, maxSubscriptions, maxCalls };
    this.#trust = trust;
    // The prefix of every client id here; a uuid has no '.', so the id
    // parses like a server's would (instanceOfClientId).
    this.#instance = instanceId ?? generateUUID();
    this.#router = this.#withIntrospection(router, introspection);
    if (this.#codec && this.#router.hasSerializers) {
      throw new TypeError('PeerHost: options.codec and compiled response serializers are mutually exclusive');
    }
  }

  get router() {
    return this.#router;
  }

  get rooms() {
    return this.#rooms;
  }

  /** A snapshot: safe to iterate while peers come and go. */
  get clients() {
    return new Set(this.#clients);
  }

  get instanceId() {
    return this.#instance;
  }

  get trust() {
    return this.#trust;
  }

  /** The telemetry writer (disabled-shaped when none was injected). */
  get otel() {
    return this.#otel;
  }

  getClient(id) {
    return this.#byId.get(id);
  }

  /**
   * Attaches one peer: a persistent transport that announces inbound
   * traffic as 'packet' (text) and 'chunk' (bytes) events —
   * RtcPeerTransport, or anything shaped like it. `peer` is the remote id;
   * `room` and `data` are what the peer said about itself at signaling
   * time, frozen into the client's meta (and, under trust 'link', its
   * session data).
   */
  attach(transport, { peer, room = null, data = null } = {}) {
    if (!isInboundTransport(transport)) {
      throw new TypeError('PeerHost.attach: a persistent transport with write/close/on/once is required');
    }
    if (typeof peer !== 'string' || peer.length === 0) {
      throw new TypeError('PeerHost.attach: peer must be a non-empty string');
    }
    if (this.#codec) transport.codec = this.#codec;
    const about = Object.freeze({ __proto__: null, peer, room, ...(data && typeof data === 'object' ? data : {}) });
    const client = new Client(transport, {
      codec: this.#codec,
      sessions: null,
      rooms: this.#rooms,
      server: this,
      log: this.#log,
      otel: this.#otel,
      maxSubscriptions: this.#limits.maxSubscriptions,
      maxCalls: this.#limits.maxCalls,
      generateId: this.#generateId,
      meta: buildMeta({ data: about, remoteAddress: peer }),
      metaMax: this.#metaMax,
    });
    if (this.#trust === 'link') client.session = Object.freeze({ token: peer, data: about });
    this.#clients.add(client);
    this.#byId.set(client.id, client);
    this.#otel.recordConnection(1, transport.kind);
    // Router-level connection hooks, ordered before dispatch exactly as
    // RpcServer does it: client.ready gates the access check.
    const { onConnect, onDisconnect } = this.#router.connectionHooks;
    if (onConnect.length > 0) {
      const hooks = runHooksSafe(onConnect, client, null, this.#log, 'onConnect');
      client.ready = client.sessionReady.then(() => hooks);
      const stall = setTimeout(() => {
        this.#log.warn({ event: 'onConnect.stalled', peer, ms: ONCONNECT_STALL_MS });
      }, ONCONNECT_STALL_MS);
      if (typeof stall.unref === 'function') stall.unref();
      void client.ready.then(() => clearTimeout(stall));
    }
    transport.on('packet', (text) => handleMessage(client, text, this.#router, this.#limits));
    transport.on('chunk', (bytes) => handleBinary(client, bytes));
    transport.once('close', () => {
      const payload = onDisconnect.length > 0 ? { rooms: client.rooms } : null;
      client.destroy();
      this.#clients.delete(client);
      this.#byId.delete(client.id);
      this.#otel.recordConnection(-1, transport.kind);
      if (onDisconnect.length > 0) void runHooksSafe(onDisconnect, client, payload, this.#log, 'onDisconnect');
      void this.emit('detach', client).catch((error) => this.#log.error({ err: error, event: 'listener.detach' }));
    });
    void this.emit('attach', client).catch((error) => this.#log.error({ err: error, event: 'listener.attach' }));
    return client;
  }

  #target() {
    return new Broadcast({
      registry: this.#rooms,
      clients: () => this.#clients,
      publish: null,
      cluster: null,
      log: this.#roomsLog,
      otel: this.#otel,
      codec: this.#codec,
    });
  }

  /** Everyone in any of `rooms`, each client once. */
  to(...rooms) {
    return this.#target().to(...rooms);
  }

  /** Every attached peer, minus `clients`. */
  except(...clients) {
    return this.#target().except(...clients);
  }

  /** Every attached peer; returns the recipient count. */
  broadcast(name, data) {
    return this.#target().emit(name, data);
  }

  /** Closes every attached peer's link. */
  close() {
    for (const client of [...this.#clients]) client.close();
  }

  // Mirrors RpcServer: system/introspect mounted public unless the router
  // brings its own; `introspection` is true | 'session' | false | { access, schemas }.
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
}

module.exports = { PeerHost, isInboundTransport };
