'use strict';

const { Emitter, jsonParse, isCodec, resolveGenerateId } = require('../utils.js');
const { ServerTransport, buildHeaders, isOriginAllowed, readsFrames } = require('../transport.js');
const { hasTransportShape, isInboundTransport } = require('./serverTransport.js');
const { SessionManager } = require('./sessions.js');
const { defineRouter, procedure, runHooksSafe } = require('./router.js');
const { RoomRegistry, Broadcast, RoomsBackplane, BROADCAST_CHANNEL } = require('./rooms.js');
const { Cluster, instanceOfClientId } = require('./cluster.js');
const { SseChannels } = require('../sse/server.js');
const { normalizeCompression, addVary } = require('../contentEncoding.js');
const {
  maxMessageOf,
  normalizeSyncCompression,
  decodeOrNull,
  createFailureReporter,
} = require('../compression/sync.js');
const { createEnvelope } = require('./envelope.js');
const { normalizeServerEncryption, wantsEncryption, SealedSocket } = require('../encryption/server.js');
const {
  FRAME_MARK,
  FRAME_ATTACHMENTS,
  FRAME_PACKET_COMPRESSED,
  FRAME_CHUNK_COMPRESSED,
  WRPC_V2,
} = require('../wire.js');
const { isAttachmentsFrame, decodeAttachments } = require('../attachments.js');
// The channel header from the import-free constants module, NOT from
// sse/server.js: the string is shared, the implementation is not.
const { CHANNEL_HEADER } = require('../wire.js');
const { splitChannelRef } = require('../sse/constants.js');
const { isBackplane } = require('../scaling/index.js');
const {
  handleMessage,
  handleBinary,
  dispatchMessage,
  dispatchBinary,
  handleRpc,
  split,
  parseParams,
  DEFAULT_MAX_BATCH,
  UNKNOWN_TARGET,
} = require('./dispatcher.js');
const { createLoggerWriter } = require('../logging.js');
const { createServerTelemetry } = require('../telemetry/server.js');
const { TRACEPARENT, TRACESTATE } = require('../telemetry/shared.js');
const {
  Context,
  Client,
  DEFAULT_MAX_SUBSCRIPTIONS,
  DEFAULT_MAX_CALLS,
  DEFAULT_MAX_STREAMS,
  buildMeta,
} = require('./client.js');
const { DEFAULT_META_MAX, declaredData } = require('./meta.js');
const { readDeclared, normalizeDeclaredHeaders } = require('./handshake.js');
const { AMBIENT_HEADERS } = require('./reserved.js');

// After this long an unsettled onConnect chain logs a warning: a hook that
// never resolves holds the client's dispatch (see #addClient), and the warn
// is the only trace that hang would leave.
const ONCONNECT_STALL_MS = 5_000;

const ServerHttpTransport = ServerTransport.transport.http;
const ServerWsTransport = ServerTransport.transport.ws;
const ServerEventTransport = ServerTransport.transport.event;

// The transport a socket attached with `meta.kind` gets: a registered
// ServerWsTransport subclass under that name ('wt' from @alexify/wrpc/wt,
// whose require registers it), the WebSocket one otherwise. hasOwn, and
// only a ws-shaped class: the table also holds http and event.
const socketTransportFor = (kind) => {
  if (typeof kind === 'string' && kind && Object.hasOwn(ServerTransport.transport, kind)) {
    const Transport = ServerTransport.transport[kind];
    if (Transport === ServerWsTransport || Transport.prototype instanceof ServerWsTransport) return Transport;
  }
  return ServerWsTransport;
};

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
// The keys `options.rooms` knows; anything else is a warning (see #initRooms).
const ROOMS_OPTION_KEYS = ['epoch', 'linger', 'maxTracked', 'compression', 'encryption', 'maxMessage'];
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
  'maxStreams',
  'sse',
  'http',
  'compression',
  'maxMessage',
  'attachments',
  'cluster',
  'rooms',
  'querystring',
  'codec',
  'metaMaxBytes',
  'declaredHeaders',
  'encryption',
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
  // The normalized `http.compression` option, or null (off, the default).
  #compression = null;
  #compressionFailed;
  #httpFailed;
  // The largest inflated client frame accepted on a socket (the Node ws
  // client's per-message compression, negotiated on ping/pong).
  #maxMessage;
  #querystring = null;
  #metaMax = DEFAULT_META_MAX;
  #declared = null;
  // Session encryption (@alexify/wrpc/encryption), normalized; null when off.
  #encryption = null;
  // Its per-request half: sealed HTTP calls unwrapped before routing.
  #sealing = null;
  #codec = null;
  #codecOption = null;
  #restCodec = null;
  // Binary attachments (src/attachments.js): on by default, `attachments:
  // false` sends every packet as JSON as revision 1 did.
  #attachments = true;
  // `revision.mismatch` is a configuration error, the same on every
  // connection: said once at warn, then at debug.
  #mismatchSaid = false;
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
      instanceId = null,
      generateId = null,
      introspection = true,
      maxBatch = DEFAULT_MAX_BATCH,
      maxSubscriptions = DEFAULT_MAX_SUBSCRIPTIONS,
      maxCalls = DEFAULT_MAX_CALLS,
      maxStreams = DEFAULT_MAX_STREAMS,
      sse = {},
      http = {},
      compression = null,
      maxMessage,
      attachments = true,
      cluster = {},
      rooms = {},
      querystring = null,
      codec = null,
      metaMaxBytes = DEFAULT_META_MAX,
      declaredHeaders = null,
      encryption,
    } = options;
    // The cap on peer-declared metadata (the ws wrpc_h query parameter and
    // the per-packet meta field), measured on the encoded input.
    this.#metaMax = Number.isInteger(metaMaxBytes) && metaMaxBytes > 0 ? metaMaxBytes : DEFAULT_META_MAX;
    // Opt-in allowlist of the names a ws handshake may DECLARE (beside the
    // deny list, which always applies): `['authorization']` for a deployment
    // whose handlers read nothing else from a declaration.
    this.#declared = normalizeDeclaredHeaders(declaredHeaders, 'RpcServer: options');
    this.#encryption = normalizeServerEncryption(encryption, 'RpcServer: options');
    if (!router || typeof router.getProcedure !== 'function') {
      throw new TypeError('RpcServer: options.router (a Router from defineRouter) is required');
    }
    if (backplane && !isBackplane(backplane)) {
      throw new TypeError('RpcServer: options.backplane does not implement the backplane contract');
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
    if (this.#encryption !== null) {
      const log = this.#log.child({ component: 'encryption' });
      this.#sealing = this.#encryption.http({
        log,
        // What the connection says about the sender is not the sender's to
        // declare inside a sealed request (reserved.js; handed over because
        // src/encryption/ never requires src/rpc/).
        reserved: AMBIENT_HEADERS,
        // One bare status for every refusal; WHICH check it was is for the log.
        // `quiet`: the sealing already said it, rate-limited (a replay cache
        // full of live entries refuses a flood, and a line per request is
        // the flood's second victim).
        refuse: (call, headers, status, reason, quiet = false) => {
          if (!quiet) log.warn({ event: 'encryption.refused', reason, kind: 'http' });
          this.#otel.recordEncryption(reason, 'http');
          this.#otel.recordCall(UNKNOWN_TARGET, 'error', status);
          new ServerHttpTransport(call, { headers }).error(status);
        },
      });
    }
    // Built once here rather than per broadcast or per channel: Broadcast is
    // constructed on every to()/except()/broadcast().
    this.#roomsLog = this.#log.child({ component: 'rooms' });
    this.#sseLog = this.#log.child({ component: 'sse' });
    // One reporter for every carrier this core compresses on: a codec that
    // threw is counted each time and said once (see createFailureReporter).
    this.#compressionFailed = createFailureReporter(this.#log.child({ component: 'compression' }), this.#otel);
    this.#httpFailed = (coding, error) => this.#compressionFailed('http', 'encode', coding, error);
    this.#sessions = new SessionManager(sessions, this.#log.child({ component: 'sessions' }), this.#otel);
    // Session encryption is for where TLS ends before the data does — and
    // there a cookie is the wrong credential: it stays on the OUTER request
    // by construction (script cannot set HttpOnly), on every transport, read
    // by the very terminator the encryption exists to keep out. Said once,
    // at construction; bearerTransport()/payloadTransport() carry the
    // credential inside the channel.
    if (this.#encryption !== null && this.#sessions.transport.ambient === true) {
      this.#log.warn({ event: 'encryption.ambient-session' });
    }
    this.#cors = cors;
    this.#basePath = normalizeBasePath(basePath);
    // Resolved before #instance, because an omitted instanceId is minted BY
    // the generator: a user who injected one gets it used for every id the
    // server mints, the routing prefix included, not for all of them but one.
    const ids = resolveGenerateId(generateId, 'RpcServer');
    this.#generateId = ids.generate;
    this.#instance = instanceId ?? ids.first;
    // The dot separates the instance prefix from the rest of a client id
    // (`<instanceId>.<generateId()>`), so an instance name carrying one
    // would make every one of its client ids parse to the wrong address.
    // Checked after the mint, so a generator that answers a dotted id is
    // refused by the same rule as a hand-passed one — and named as the
    // option that actually produced it.
    if (String(this.#instance).includes('.')) {
      const source = instanceId === null ? 'generateId must not return' : 'options.instanceId must not contain';
      throw new TypeError(`RpcServer: ${source} "."`);
    }
    this.#querystring = querystring;
    if (typeof http !== 'object' || http === null) throw new TypeError('RpcServer: options.http must be an object');
    // gzip on packet-mode and REST answers for peers that ask for it — off
    // unless the app turns it on; the SSE half has its own under `sse`.
    this.#compression = normalizeCompression(http.compression, 'RpcServer: options.http');
    // Two halves, two fields: #codec is the PACKET codec (ws/http/sse/worker
    // frames — a rest-only codec leaves packet mode JSON), #restCodec the
    // REST body codec. The raw option survives for the public getter.
    this.#codecOption = codec;
    this.#codec = codec && typeof codec.encode === 'function' && typeof codec.decode === 'function' ? codec : null;
    this.#restCodec = codec?.rest ?? null;
    // `compression` here is the SOCKET side: accepting per-message
    // compressed frames from a Node ws client that negotiated them. Off by
    // default, like the http/sse/rooms/cluster halves; rides in the limits
    // bag so the dispatcher's ping handler sees it.
    this.#attachments = attachments !== false;
    // A packet codec owns the wire: attachments are its business, not ours.
    if (this.#codec) this.#attachments = false;
    this.#limits = {
      maxBatch,
      maxSubscriptions,
      maxCalls,
      maxStreams,
      compression: normalizeSyncCompression(compression, 'RpcServer: options'),
      attachments: this.#attachments,
    };
    this.#maxMessage = maxMessageOf(maxMessage, 'RpcServer');
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
      const { restore, meta } = this.#identify(call, false, this.#sseLog);
      return this.#addClient(transport, restore, meta);
    };
    // The identity a request presents, bound to a channel at creation and
    // required again on every re-attach and channel POST — the id alone
    // must never be enough to act as the channel's session.
    const channelKey = (headers) => this.#requestKey(headers);
    this.#sse =
      sse === false
        ? null
        : new SseChannels({
            generateId: this.#generateId,
            ...sse,
            log: this.#sseLog,
            otel: this.#otel,
            addClient,
            channelKey,
          });
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
    // Envelope compression on the backplane, per layer and off by default:
    // a string carrier, so a compressed envelope rides as base64 under a
    // marker — and unlike the socket carriers there is no negotiation, so
    // every instance must run it (documented as a two-step rollout).
    const clusterLog = this.#log.child({ component: 'cluster' });
    // `encryption` seals what compression left, per layer as well, under a
    // shared keyring (./envelope.js): Redis and whoever operates it then
    // carry ciphertext. Off by default; the rollout is three deploys.
    const clusterEnvelope = enabled
      ? createEnvelope({
          compression: clusterOptions?.compression,
          encryption: clusterOptions?.encryption,
          maxMessage: maxMessageOf(clusterOptions?.maxMessage, 'RpcServer: options.cluster'),
          name: 'RpcServer: options.cluster',
          layer: 'cluster',
          event: 'cluster',
          log: clusterLog,
          failed: (direction, codec, error) => this.#compressionFailed('cluster', direction, codec, error),
        })
      : null;
    const cluster = new Cluster({
      backplane: enabled ? backplane : null,
      instance: this.#instance,
      local: this.#clusterOps(),
      log: clusterLog,
      otel: this.#otel,
      generateId: this.#generateId,
      options: enabled ? { ...clusterOptions, envelope: clusterEnvelope, attachments: this.#attachments } : {},
    });
    this.#cluster = cluster;
    // `onSubscribe`/`onUnsubscribe` fire on a room's FIRST member and its
    // last, which is exactly where a live-room gauge moves — so the
    // instrument rides the callbacks that already exist rather than costing
    // the hot join/leave path anything.
    const countRoom = (delta) => this.#otel.recordRooms(delta);
    // `rooms` is a 1.0 option, so an unknown key is a warning, not a
    // TypeError — except `rooms.backplane`, the one spelling that reads as
    // if it worked (the backplane is a top-level option) and did nothing.
    if (roomsOptions !== null && typeof roomsOptions === 'object') {
      if (roomsOptions.backplane !== undefined) {
        throw new TypeError(
          'RpcServer: options.rooms.backplane is not an option — pass the backplane as options.backplane',
        );
      }
      for (const key in roomsOptions) {
        if (!ROOMS_OPTION_KEYS.includes(key)) this.#roomsLog.warn({ event: 'rooms.option', key });
      }
    }
    // Built before the no-backplane return, so `rooms.compression` and
    // `rooms.encryption` are validated where they are written even on an
    // instance that has no backplane to use them on (a codec name the
    // platform lacks, a keyring that is not one) — the cluster options were
    // already checked that way.
    const envelope = createEnvelope({
      compression: roomsOptions?.compression,
      encryption: roomsOptions?.encryption,
      maxMessage: maxMessageOf(roomsOptions?.maxMessage, 'RpcServer: options.rooms'),
      name: 'RpcServer: options.rooms',
      layer: 'rooms',
      event: 'backplane',
      log: this.#roomsLog,
      failed: (direction, codec, error) => this.#compressionFailed('rooms', direction, codec, error),
    });
    if (!backplane) {
      this.#rooms = new RoomRegistry({
        onSubscribe: () => countRoom(1),
        onUnsubscribe: () => countRoom(-1),
      });
      return;
    }
    const binder = new RoomsBackplane({
      backplane,
      instance: this.#instance,
      log: this.#roomsLog,
      linger: roomsOptions?.linger,
      maxTracked: roomsOptions?.maxTracked,
      envelope,
      // The producer-restart marker a resume cursor is validated against.
      // Accepted by RoomsBackplane all along; forwarded (and declared) only
      // now, so a deployment that pins it across restarts finally can.
      epoch: roomsOptions?.epoch,
      // Loss made visible: a jump in a publisher's sequence is logged and
      // counted, so a broker that drops envelopes shows up in dashboards
      // instead of in a bug report about a message nobody received.
      // The log line names the channel; the metric carries only its KIND
      // — a room name is peer-chosen and unbounded, and a label with an
      // unbounded set of values is a cardinality bomb in any metrics
      // backend. Computed here, in the Node-only core: the writer is
      // bundled into the webrtc browser entry.
      onGap: ({ channel, instance, missed }) => {
        this.#roomsLog.warn({ event: 'backplane.gap', channel, instance, missed });
        this.#otel.recordBackplaneGap(channel === BROADCAST_CHANNEL ? 'broadcast' : 'room', missed);
      },
      // A replayed event is delivered LOCALLY: publishing it again would
      // bounce it between instances forever.
      deliver: (rooms, name, data, unreliable = false) => {
        const target = rooms ? this.#target().to(...rooms) : this.#target();
        target.local().emit(name, data, unreliable ? { unreliable: true } : null);
      },
    });
    this.#backplane = binder;
    this.#rooms = new RoomRegistry({
      onSubscribe: (room) => {
        countRoom(1);
        binder.joinRoom(room);
      },
      onUnsubscribe: (room) => {
        countRoom(-1);
        binder.leaveRoom(room);
      },
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
        if (!client) return [];
        // id AND room: the room is a condition on that one client, not a
        // second population — an addressed relay bounded by a membership.
        if (typeof sel.room === 'string' && !client.in(sel.room)) return [];
        return [client];
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
      // The local leg of Cluster.send / RpcServer.sendTo: a per-request HTTP
      // client cannot receive one, and is skipped the way Broadcast skips it.
      event: (sel, name, data) => {
        for (const client of select(sel)) if (client.persistent) client.sendEvent(name, data);
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

  /**
   * One event to one client by id, on this instance or — through the
   * cluster's addressed command — on the instance its id names. `room`
   * narrows delivery to a client still in that room. Returns true when
   * the event was delivered locally or handed to the backplane, false when
   * it is known not to be deliverable (unknown local id, non-persistent
   * client, not in `room`, or a foreign id with no backplane). Bytes in
   * `data` cross to the other instance as bytes — a binary envelope, as a
   * room event's do.
   */
  sendTo(clientId, name, data, options = {}) {
    if (typeof clientId !== 'string' || clientId.length === 0) {
      throw new TypeError('sendTo: clientId must be a non-empty string');
    }
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('Event name must be a non-empty string');
    }
    const room = typeof options.room === 'string' ? options.room : undefined;
    const client = this.#byId.get(clientId);
    if (client) {
      if (!client.persistent || (room !== undefined && !client.in(room))) return false;
      client.sendEvent(name, data);
      return true;
    }
    const instance = instanceOfClientId(clientId);
    if (instance === null || instance === this.#instance || !this.#backplane) return false;
    return this.#cluster.send(clientId, name, data, { room });
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

  /** The per-connection caps every attached client gets — frozen. */
  get limits() {
    return Object.freeze({ ...this.#limits });
  }

  /**
   * Whether `encryption.required` is on: what a binding built on `attach`
   * (the broker consumers, a raw data channel) reads to vouch for its
   * transport — or refuse to attach at all — before a delivery arrives.
   */
  get encryptionRequired() {
    return this.#encryption?.required === true;
  }

  /** The injected codec option, verbatim — how an adapter inspects codec.rest. */
  get codec() {
    return this.#codecOption;
  }

  /**
   * The newest protocol revision this server speaks (protocol.md#versioning):
   * 2, or 1 when it sends and reads no framed messages (`attachments: false`,
   * a packet codec). What a shell composing an engine reads to narrow the
   * WebSocket negotiation, and what `wrpc-version` answers on HTTP.
   */
  get revision() {
    return this.#attachments ? 2 : 1;
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

  // The same seam for a codec that failed on a carrier attached from
  // outside (a WebTransport session, a broker binding):
  // `(carrier, direction, codec id, error)` — counted every time, said once
  // per carrier, direction and codec.
  get compressionFailed() {
    return this.#compressionFailed;
  }

  // The logging half of the same seam, and for the same reason: a framework
  // adapter or an external attacher (a WebTransport session, an express
  // handler) has somewhere to report a failure that never reaches a Client,
  // rather than emitting an 'error' nobody listens for. This is the
  // NORMALIZED writer, not the `logger` that was passed — re-wrapping one is
  // free, so handing it straight back into another component's `logger`
  // option is the intended use.
  get log() {
    return this.#log;
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
    // A host-delegated route is a plaintext request by construction — a
    // sealed one is unwrapped by handleHttpCall and answered by the core's
    // own REST — so under `required` it is refused here, BEFORE a client is
    // added, the same 426 the core answers on its own surface.
    if (this.#encryption?.required) {
      this.#refusePlaintext('http');
      const error = new Error('Upgrade Required: this server answers sealed requests only');
      error.code = 426;
      error.statusCode = 426;
      error.expose = true;
      throw error;
    }
    const transport = new ServerHttpTransport({ headers, remoteAddress, respond: () => {} }, { headers: {} });
    const verb = String(method).toUpperCase();
    const safeMethod = verb === 'GET' || verb === 'HEAD';
    const { restore, meta } = this.#identify({ headers, url, remoteAddress }, safeMethod);
    const client = this.#addClient(transport, restore, meta);
    await client.ready;
    const context = client.createContext(null, target);
    return { client, context, transport, release: () => transport.emit('close') };
  }

  // One line and one metric for every plaintext request refused under
  // `encryption.required`, whichever surface it arrived on.
  #refusePlaintext(kind) {
    this.#log.warn({ event: 'encryption.refused', reason: 'plaintext', kind });
    this.#otel.recordCall(UNKNOWN_TARGET, 'error', 426);
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
      attachments: this.#attachments,
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

  // A connection or a request whose peer said nothing of revision 2: no
  // framed message is sent to it. The per-transport flag is the one
  // `attachments: false` sets server-wide, so the send paths check nothing
  // new.
  #speakRevision1(transport) {
    transport.setRevision(1);
  }

  #addClient(transport, restore = null, meta = null) {
    // The transport encodes outbound packets; the Client decodes inbound
    // ones. One server-wide codec — which is what keeps the broadcast
    // fan-out single-encode.
    if (this.#codec) transport.codec = this.#codec;
    if (!this.#attachments) transport.attachments = false;
    const options = {
      codec: this.#codec,
      sessions: this.#sessions,
      rooms: this.#rooms,
      server: this,
      log: this.#log,
      otel: this.#otel,
      maxSubscriptions: this.#limits.maxSubscriptions,
      maxCalls: this.#limits.maxCalls,
      maxStreams: this.#limits.maxStreams,
      generateId: this.#generateId,
      meta,
      metaMax: this.#metaMax,
    };
    const client = new Client(transport, options);
    this.#clients.add(client);
    this.#byId.set(client.id, client);
    this.#otel.recordConnection(1, transport.kind, transport.revision);
    // A revision settled after attach (a port's ping, a WebTransport peer's
    // capabilities) moves the connection between the two series.
    transport.on('revision', (from) => {
      this.#otel.recordConnection(-1, transport.kind, from);
      this.#otel.recordConnection(1, transport.kind, transport.revision);
    });
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
      this.#otel.recordConnection(-1, transport.kind, transport.revision);
      if (onDisconnect.length > 0) void runHooksSafe(onDisconnect, client, payload, this.#log, 'onDisconnect');
    });
    return client;
  }

  // Who a request says it is — the three steps every request-shaped entry
  // point takes (an SSE channel's GET, a packet POST, a REST call, a
  // host-delegated route, `attach({ request })`), in ONE place: the declared
  // connection data, the thunk that restores a session from the request's
  // token, and the client's meta. They were five copies, and a field added
  // to what a token carrier reads in four of them is a session restored
  // differently on the fifth. (attachSocket is not one of them: a WebSocket
  // handshake brings its declarations on other carriers — readDeclared.)
  //
  // `safeMethod`: the CSRF rule for a GET/HEAD. It guards AMBIENT authority
  // (a browser cookie attached without script) and lives only here: such a
  // request restores no session unless the browser says it is same-origin
  // (#isSameOriginFetch). A non-ambient carrier — a bearer header the
  // page's own code must set — has nothing to guard, so it restores on safe
  // methods too.
  #identify({ headers, url, remoteAddress }, safeMethod = false, log = this.#log) {
    const data = declaredData(headers, split(url ?? '', '?')[1], this.#metaMax, log);
    const guarded = safeMethod && this.#sessions.transport.ambient === true && !this.#isSameOriginFetch(headers);
    return {
      data,
      restore: guarded ? null : (client) => this.#restoreToken(client, { headers, url, declared: headers, meta: data }),
      meta: buildMeta({ headers, data, url, remoteAddress }),
    };
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
        // Not "no session": the store could not be asked. A session
        // procedure is then refused 503 — what a broker consumer retries —
        // where a 403 sent the delivery straight to its dead-letter queue.
        client.sessionUnavailable = true;
        return false;
      },
    );
  }

  #isKeyPath(url) {
    const [pathname] = split(url ?? '/', '?');
    return pathname === `${this.#basePath}/encryption-key`;
  }

  // The socket a connection is attached through: a SealedSocket when the
  // peer announced encryption, one that refuses when `required` and it did
  // not, null for a plaintext connection that is allowed to be one.
  #sealSocket(socket, meta) {
    const encryption = this.#encryption;
    if (encryption === null) return null;
    // The peer rides the child bindings: a refused handshake names WHO, on
    // a socket transport (an http refusal has the proxy's address instead,
    // and carries none). The counter is the core's, handed in as a callback
    // so src/encryption/ imports no telemetry.
    const kind = typeof meta.kind === 'string' && meta.kind ? meta.kind : 'ws';
    const peer = meta.remoteAddress ?? socket.remoteAddress;
    const log = this.#log.child(
      typeof peer === 'string' && peer ? { component: 'encryption', peer } : { component: 'encryption' },
    );
    const record = (outcome) => this.#otel.recordEncryption(outcome, kind);
    if (wantsEncryption(meta.url)) return new SealedSocket(socket, { encryption, kind, log, record });
    return encryption.required
      ? new SealedSocket(socket, { encryption, kind, log, record, refuse: 'plaintext' })
      : null;
  }

  /**
   * The public key bundle of the current encryption key — what a client
   * pins as `serverKey`. Safe to publish; null when encryption is off.
   */
  encryptionKey() {
    return this.#encryption === null ? Promise.resolve(null) : this.#encryption.bundle();
  }

  attachSocket(socket, meta = {}) {
    // Session encryption: a client announces it in the connect URL, because
    // the server may be the first to send (an onConnect hook, a broadcast)
    // and has to know the mode before any frame. The socket is then a
    // SealedSocket — the handshake, and every message after it decrypted and
    // re-announced as an engine socket would — and nothing below this line
    // changes. The flag is not a secret and stripping it buys an attacker a
    // refusal: a client configured to encrypt never accepts plaintext, and
    // under `required` neither does this server.
    const raw = socket;
    const sealed = this.#sealSocket(socket, meta);
    if (sealed !== null) socket = sealed;
    const transport = new (socketTransportFor(meta.kind))(socket, meta);
    // The revision this connection speaks (protocol.md#versioning). A
    // WebSocket says it in the subprotocol the engine selected: a 1.0 client
    // offers `wrpc.v1` alone, and is then sent no framed message — its bytes
    // travel as the JSON 1.0 made of them, through the same per-transport
    // flag `attachments: false` sets for everyone. WebTransport says it in
    // the capabilities each end sends first (`f`): revision 1 until the
    // client's arrive and say it reads frames — two 2.x ends whose
    // `attachments` disagree used to send each other frames the other
    // refused.
    if (meta.kind === 'wt') {
      this.#speakRevision1(transport);
      const raise = (reads) => {
        if (!reads) return void this.#log.debug({ event: 'revision.peer', transport: 'wt', peer: meta.remoteAddress });
        if (this.#attachments) transport.setRevision(2);
      };
      if (typeof raw.peerFrames === 'boolean') raise(raw.peerFrames);
      else raw.once?.('frames', raise);
    } else if (socket.protocol !== WRPC_V2) {
      this.#speakRevision1(transport);
      // Who is still on 1.0 during an upgrade: per connection, at debug.
      this.#log.debug({
        event: 'revision.peer',
        transport: meta.kind ?? 'ws',
        protocol: socket.protocol || null,
        peer: meta.remoteAddress,
      });
    } else if (!this.#attachments) {
      // An engine composed by hand selected `wrpc.v2` for a server that
      // sends no frames and reads none: the client will send one. The
      // built-in shells narrow the engine to `wrpc.v1` (see `revision`).
      const level = this.#mismatchSaid ? 'debug' : 'warn';
      this.#mismatchSaid = true;
      this.#log[level]({ event: 'revision.mismatch', protocol: socket.protocol });
    }
    // Declared-then-observed: whichever carrier brought the bags (subprotocol
    // offers, the query, real headers), a declaration can only add names the
    // upgrade request did not carry — see rpc/handshake.js, which a
    // verifyClient gate reads through as well (readHandshake).
    const {
      headers: merged,
      meta: data,
      declared,
    } = readDeclared(meta.headers, meta.url, this.#metaMax, this.#log, this.#declared);
    const client = this.#addClient(
      transport,
      // The session is restored once the handshake is done: the dispatcher
      // awaits `client.ready`, so no call runs on a channel still in the
      // clear, and an onConnect hook that awaits `client.sessionReady` reads
      // `client.encryption` — the peer's static key under XX, the handshake
      // hash to bind a credential to.
      (c) => {
        const restore = () =>
          this.#restoreToken(c, { headers: meta.headers, url: meta.url, declared: merged, meta: data });
        if (sealed === null) return restore();
        return sealed.ready.then((info) => {
          if (info === null) return false;
          c.encryption = info;
          return restore();
        });
      },
      buildMeta({
        headers: merged,
        declared,
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
      let bytes = new Uint8Array(data);
      // A framed message (wire.js): a compressed packet is dispatched from
      // here, a compressed chunk falls through to the chunk path inflated;
      // an attachments frame is the chunk path's to dispatch (handleBinary).
      if (bytes[0] === FRAME_MARK && bytes[1] !== FRAME_ATTACHMENTS) {
        bytes = this.#inflateFrame(client, bytes);
        if (bytes === null) return;
      }
      inflight++;
      if (inflight === 1 && typeof socket.pause === 'function') socket.pause();
      handleBinary(client, bytes, this.#router, this.#limits).then(done, done);
    });
    // Debug, not warn: the built-in Connection already wrote its own line
    // for what it saw; this is the client's side of the same event.
    socket.on('error', (error) => {
      client.log.debug({ event: 'socket.error', err: error });
      transport.emit('close');
    });
    return client;
  }

  // A 0x00-marked binary frame from a socket peer: inflated with the codec
  // the peer negotiated on ping/pong. Anything else — a frame before the
  // negotiation, an unknown kind, a body that does not inflate under
  // `maxMessage` — is answered with an id-less 400 and logged, never a
  // hang-up: the connection itself is fine. Answers the inflated chunk for
  // kind 4, null otherwise (a packet was dispatched, or the frame refused).
  #inflateFrame(client, bytes) {
    const kind = bytes[1];
    const active = client.compression;
    if (active === null || (kind !== FRAME_PACKET_COMPRESSED && kind !== FRAME_CHUNK_COMPRESSED)) {
      client.log.warn({ event: 'frame.refused', kind, negotiated: active !== null });
      client.error(400, { error: new Error('Unexpected framed message'), level: 'debug' });
      return null;
    }
    // Why it did not inflate goes on the line: ERR_BUFFER_TOO_LARGE is the
    // `maxMessage` cap, anything else the bytes — garbage, or a dictionary
    // the two ends do not share.
    let cause = null;
    const out = decodeOrNull(active, bytes.subarray(2), this.#maxMessage, (error) => void (cause = error));
    if (out === null) {
      this.#otel.recordCompressionFailure(client.transportKind, 'decode');
      client.log.warn({ event: 'frame.refused', kind, reason: 'inflate', codec: active.id, code: cause?.code });
      client.error(400, { error: new Error('Framed message does not inflate'), level: 'debug' });
      return null;
    }
    if (kind === FRAME_CHUNK_COMPRESSED) return new Uint8Array(out.buffer, out.byteOffset, out.byteLength);
    handleMessage(client, out, this.#router, this.#limits);
    return null;
  }

  /**
   * Any persistent inbound transport announcing its traffic as 'packet'
   * (text) and 'chunk' (bytes) events — the seam a wire this core never
   * heard of plugs into (a WebRTC data channel does, through
   * attachChannel in @alexify/wrpc/webrtc: the core knows no framing).
   * `meta` is a buildMeta() result (rpc/client.js) or null: a transport
   * carries no request, so whatever the application observed about the
   * connection is handed over.
   *
   * Two ways to give the client an identity, mutually exclusive:
   * - `session`: a pseudo-session object the host vouches for (a broker
   *   binding's service identity, the PeerHost `trust` pattern). Assigned
   *   before the onConnect hooks run, so a hook already sees it.
   * - `request: { headers, url, remoteAddress }`: what the peer presented,
   *   restored through the configured token carrier exactly as
   *   attachSocket does — a bearer token riding a broker header restores a
   *   real session.
   *
   * `persistent: false` attaches a request/response carrier instead — a
   * broker consumer binding, whose client runs calls but carries no events,
   * streams or subscriptions and is not one of the "connected clients"
   * broadcasts, presence and fetchClients count. Its transport's
   * `connection` is cleared, which is exactly what Client.persistent reads.
   */
  attach(transport, { meta: given = null, session = null, request = null, persistent = true, encrypted = false } = {}) {
    // Under `encryption.required` a wire this core cannot see into has to
    // be vouched for: a WebRTC data channel is (DTLS, end to end), a broker
    // binding is when it seals its own frames.
    if (this.#encryption?.required && encrypted !== true) {
      throw new Error('RpcServer.attach: encryption is required, and this transport was not declared encrypted');
    }
    if (persistent === false) {
      if (!hasTransportShape(transport)) {
        throw new TypeError('RpcServer.attach: a transport with write/send/error/close/on/once/off is required');
      }
      transport.connection = null;
    } else if (!isInboundTransport(transport)) {
      throw new TypeError(
        'RpcServer.attach: a persistent transport (connection set) with write/send/error/close/on/once/off is required',
      );
    }
    if (session !== null && (typeof session !== 'object' || Array.isArray(session))) {
      throw new TypeError('RpcServer.attach: options.session must be an object');
    }
    if (session !== null && request !== null) {
      throw new TypeError('RpcServer.attach: options.session and options.request are mutually exclusive');
    }
    if (given !== null && (typeof given !== 'object' || Array.isArray(given))) {
      throw new TypeError('RpcServer.attach: options.meta must be an object');
    }
    // What the application observed is normalized like every other entry
    // point's meta — frozen, its headers a null-prototype bag — whether it
    // was built with buildMeta() or written by hand.
    let meta = given === null ? null : buildMeta(given);
    let restore = null;
    if (session !== null) {
      restore = (client) => {
        client.session = session;
        return Promise.resolve(true);
      };
    } else if (request !== null) {
      if (typeof request !== 'object') throw new TypeError('RpcServer.attach: options.request must be an object');
      const identity = this.#identify({
        headers: request.headers ?? {},
        url: request.url ?? '',
        remoteAddress: request.remoteAddress,
      });
      restore = identity.restore;
      meta ??= identity.meta;
    }
    const client = this.#addClient(transport, restore, meta);
    // Contained: a listener that threw would reject the transport's emit()
    // — an unhandled rejection for whoever announced the packet.
    transport.on('packet', (text) => dispatchMessage(client, text, this.#router, this.#limits));
    transport.on('chunk', (bytes) => dispatchBinary(client, bytes, this.#router, this.#limits));
    return client;
  }

  attachPort(port, meta = null) {
    const transport = new ServerEventTransport(port);
    // A MessagePort carries no request, so there is nothing to observe; a
    // consumer that received declared headers in the 'wrpc:connect' message
    // may hand them over here.
    const client = this.#addClient(transport, null, meta?.headers ? buildMeta({ headers: meta.headers }) : null);
    // The revision is the page's to name: on its first ping, or — for a
    // consumer that hands the connect message over — in `meta.v`. Until
    // then the port is sent no framed message, and a server that sends
    // none itself never raises it.
    if (!this.#attachments) transport.max = 1;
    if (meta?.v !== undefined) transport.negotiate(meta.v);
    port.on('message', (data) => {
      // Same rule as the socket path: text is a packet, bytes are a stream
      // chunk. A Buffer IS a Uint8Array, and checking Buffer.isBuffer first
      // used to hand a binary chunk to the JSON parser. Anything else
      // (a structured-clone of an object) is not on the wire and is dropped.
      if (typeof data === 'string') {
        dispatchMessage(client, data, this.#router, this.#limits);
      } else if (ArrayBuffer.isView(data)) {
        dispatchBinary(
          client,
          new Uint8Array(data.buffer, data.byteOffset, data.byteLength),
          this.#router,
          this.#limits,
        );
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
    if (!this.#codec) {
      // A POST body that is an attachments frame parses to its packet.
      if (typeof body !== 'string' && body !== null && isAttachmentsFrame(body)) {
        if (!this.#attachments) return null;
        try {
          return decodeAttachments(body);
        } catch {
          return null;
        }
      }
      return jsonParse(body);
    }
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
  // The channel id alone is NOT enough: the POST must present the channel's
  // secret (the `ready` frame's, after the id in the header) — the id is
  // whatever the application's generator makes of it, a counter included —
  // and, on top, the cookie identity the channel was created under, or
  // knowing an id would be a bearer token for someone else's
  // session-carrying client.
  //
  // `headers` are the same CORS-bearing response headers every other HTTP
  // answer carries: without them a browser on another origin cannot read
  // this response at all, which makes cross-origin SSE impossible.
  #handleChannelPost(call, channelRef, headers) {
    // Channel POSTs answer packet-mode bodies, so the packet codec's type.
    if (this.#codec?.contentType) headers = { ...headers, 'Content-Type': this.#codec.contentType };
    const ref = splitChannelRef(channelRef);
    const channel = ref === null ? null : this.#sse.get(ref.id);
    const respond = (status, packet) => {
      const body = Buffer.from(this.#codec ? this.#codec.encode(packet) : JSON.stringify(packet));
      call.respond({ status, headers: { ...headers, 'Content-Length': body.length }, body });
    };
    if (!channel || !this.#sse.holds(channel, ref.secret)) {
      // 409, matching the events endpoint: "this channel is gone" is the
      // signal the client recovers from by starting a fresh channel — and
      // the same answer for a live channel whose secret was not presented,
      // so a guessed id learns nothing.
      return void respond(409, { type: 'callback', id: '', error: { message: 'Unknown channel', code: 409 } });
    }
    if (!this.#sse.authorized(channel, call.headers)) {
      const error = { message: 'Channel belongs to another session', code: 403 };
      return void respond(403, { type: 'callback', id: '', error });
    }
    // An attachments frame on a channel POST: SSE is text-only in both
    // directions, and the refusal is explicit rather than a corrupt value.
    if (typeof call.body !== 'string' && call.body !== null && isAttachmentsFrame(call.body)) {
      const error = { message: 'Binary attachments need a WebSocket', code: 415 };
      return void respond(415, { type: 'callback', id: '', error });
    }
    handleMessage(channel.client, call.body, this.#router, this.#limits);
    call.respond({ status: 202, headers: { ...headers, 'Content-Length': 0 } });
  }

  // Every host hands a request here without a catch of its own (`void
  // rpc.handleHttpCall(call)`), so whatever escapes the routing below — a
  // peer's input no refusal anticipated — was an unhandled rejection and a
  // request nobody answered. Caught once, here: logged, counted, answered 500
  // unless something already answered or started a stream. The call is
  // copied so the answer can be known; one object per request, against an
  // HTTP parse.
  async handleHttpCall(call) {
    let answered = false;
    const { respond, stream } = call;
    const guarded = {
      ...call,
      respond: (response) => {
        answered = true;
        return respond(response);
      },
      stream:
        typeof stream === 'function'
          ? (options) => {
              answered = true;
              return stream(options);
            }
          : stream,
    };
    try {
      return await this.#routeHttpCall(guarded);
    } catch (error) {
      this.#log.error({ err: error, event: 'http.failed' });
      this.#otel.recordCall(UNKNOWN_TARGET, 'error', 500);
      if (answered) return;
      try {
        new ServerHttpTransport(guarded, { headers: buildHeaders(this.#cors, call.headers?.origin) }).error(500);
      } catch {
        // The host cannot write either: nothing more to say.
      }
    }
  }

  async #routeHttpCall(call) {
    const headers = buildHeaders(this.#cors, call.headers?.origin, this.revision);
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
    if (this.#encryption !== null) {
      // The public key bundle a client pins, for whoever has no other way
      // to learn it. Trust on first use: what travels here is only as
      // trustworthy as the connection it travelled over.
      if (this.#encryption.discovery && call.method === 'GET' && this.#isKeyPath(call.url)) {
        const body = Buffer.from(JSON.stringify({ key: await this.#encryption.bundle() }));
        const type = { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'Content-Length': body.length };
        return void call.respond({ status: 200, headers: { ...headers, ...type }, body });
      }
      // A sealed request: the real one is inside. Unwrapped here, before
      // anything is routed — from this line on it is an ordinary call,
      // marked `encrypted`, whose respond() seals the answer.
      if (this.#sealing.isSealed(call)) {
        // unwrap refuses everything it recognizes itself; what it does not
        // (a primitive that threw) is one request answered 500, not a
        // rejection the transport never hears of.
        try {
          call = await this.#sealing.unwrap(call, headers);
        } catch (error) {
          this.#log.error({ err: error, event: 'encryption.unwrap' });
          this.#otel.recordCall(UNKNOWN_TARGET, 'error', 500);
          return void new ServerHttpTransport(call, { headers }).error(500);
        }
        if (!call) return;
      }
    }
    // Under `encryption.required` nothing plaintext is answered.
    if (this.#encryption?.required && call.encrypted !== true) {
      this.#refusePlaintext('http');
      return void new ServerHttpTransport(call, { headers }).error(426);
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
    // `<id>.<secret>` — the header, else the query (URLs end up in logs).
    const ref = splitChannelRef(call.headers?.[CHANNEL_HEADER] || query.channel || '');
    const lastEventId = call.headers?.['last-event-id'] ?? query.lastEventId ?? null;
    this.#sse.open(call, { channelId: ref?.id ?? null, secret: ref?.secret ?? '', lastEventId, headers });
  }

  // POST {basePath} — a JSON call packet (or a batch array) in the body.
  async #handlePacketPost(call, headers) {
    // Mode-aware: only packet-mode responses carry the packet codec's type.
    if (this.#codec?.contentType) headers['Content-Type'] = this.#codec.contentType;
    const batch = call.method === 'POST' ? this.#batchIds(call.body) : null;
    const transport = new ServerHttpTransport(call, {
      headers,
      batch,
      compression: this.#compression,
      failed: this.#httpFailed,
    });
    if (call.method !== 'POST') {
      this.#log.warn({ event: 'http.refused', code: 403, method: call.method });
      this.#otel.recordCall(UNKNOWN_TARGET, 'error', 403);
      return void transport.error(403);
    }
    // Revision 1 unless the caller asked for a framed answer: a 1.0 client
    // reads the body as JSON whatever its type says (protocol.md#versioning).
    // The same call is a frame or JSON by its `Accept`, which a shared cache
    // must therefore key on. Settled before the client exists, which counts
    // the connection under its revision.
    if (this.#attachments) addVary(headers, 'Accept');
    if (!readsFrames(call.headers)) this.#speakRevision1(transport);
    const { restore, meta } = this.#identify(call);
    const client = this.#addClient(transport, restore, meta);
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
      compression: this.#compression,
      failed: this.#httpFailed,
      rest: route
        ? {
            status: route.http.status,
            codec: restCodec,
            headers: route.http.headers,
            cache: route.http.cache,
            access: route.proc.access,
            hasSession: () => Boolean(client?.session),
          }
        : null,
    });
    const safeMethod = method === 'GET' || method === 'HEAD';
    // The conventional mode answers a callback envelope, as packet mode
    // does: a framed one only for a caller that asked for it — to curl, a
    // 1.0 client or a browser's fetch it stays the JSON 1.0 answered. A
    // declared route's body is a plain value and has no frame either way.
    // Settled before the client exists, which counts it under its revision.
    if (route === null && this.#attachments) addVary(headers, 'Accept');
    if (route === null && !readsFrames(call.headers)) this.#speakRevision1(transport);
    // For a per-request client the connection IS the call, so the declared
    // data doubles as this call's meta: a curl caller passes x-wrpc-meta and
    // a hook reads context.callMeta, same as on ws.
    const { data, restore, meta } = this.#identify(call, safeMethod);
    const client = this.#addClient(transport, restore, meta);
    // #addClient assigned the packet codec; REST bodies are not packet
    // frames, so it comes back off. The conventional mode's callback
    // envelope IS the body value, so the rest codec (when configured)
    // takes the packet codec's slot on the transport.
    transport.codec = restCodec;
    // The response seam a REST handler reaches as `context.http`: the
    // request line, and setHeader()/status() onto this very response.
    // Null on every other transport, and on packet-mode HTTP, where one
    // response answers a whole batch.
    client.http = httpReply(call, method, transport);
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
  //
  // (httpReply lives below the class — see the module tail.)
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
    // Announced once: a binding that pulls work on its own (a broker
    // consumer) stops fetching here instead of taking messages only to
    // answer them 503.
    if (!this.#draining) {
      this.#draining = true;
      this.emit('draining');
    }
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      let busy = 0;
      for (const client of this.#clients) busy += client.calls.size;
      if (busy === 0) return;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }

  async close() {
    // Announced first, while everything still works: a binding that pulls
    // work on its own (a broker consumer) stops fetching before its client
    // is torn down under it.
    this.emit('close');
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

// `context.http` on a REST call: what the handler may read about the
// request and set on the response before it is written.
const httpReply = (call, method, transport) => ({
  method,
  url: call.url ?? '/',
  headers: call.headers ?? {},
  setHeader: (name, value) => transport.setHeader(name, value),
  status: (code) => transport.setStatus(code),
});

module.exports = { RpcServer, Client, Context, rpcOptions, DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_CALLS };
