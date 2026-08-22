'use strict';

// The client core: WrpcError, the ClientTransport base and WrpcClient
// itself. The three built-in transports live in ./transports.js (registered
// the same way src/sse/client.js registers the SSE one) and the Service
// Worker proxy in ./proxy.js; ../client.js is the barrel that assembles
// them, so require paths and the browser field are unchanged.

const { Emitter, jsonParse, isCodec, toKebab, backoffDelay, createEventStream } = require('../utils.js');
const { generateUUID } = require('../runtime/node.js');
const { chunkDecode } = require('../chunks.js');
const { WrpcReadable, WrpcWritable } = require('../streams.js');
const { createLoggerWriter } = require('../logging.js');
const { createClientTelemetry } = require('../telemetry/client.js');

const CALL_TIMEOUT = 7 * 1000;

// The wire revision this client speaks, offered as a WebSocket subprotocol.
const WRPC_PROTOCOL = 'wrpc.v1';

// Monotonic where available; Date.now is the fallback for a host without it.
const now = () => (typeof performance === 'object' ? performance.now() : Date.now());
const RECONNECT_TIMEOUT = 2 * 1000;

// 499, nginx's "client closed request": the caller took the call back, so
// it is neither a server fault nor a success.
const CANCELLED_ERROR = { message: 'Cancelled by the caller', code: 499 };

// What every in-flight call is rejected with the moment the connection is
// gone: an answer can no longer arrive, and waiting out callTimeout would
// just park the caller on a corpse.
const CONNECTION_CLOSED_ERROR = { message: 'Connection closed', code: 503 };

const RECONNECT = {
  minDelay: RECONNECT_TIMEOUT,
  maxDelay: 30 * 1000,
  factor: 2,
  jitter: true,
  retries: Infinity,
};

// App-level heartbeat. A browser WebSocket exposes no protocol-level ping,
// so a connection that died without a close frame (dropped NAT mapping,
// suspended laptop, a proxy that stopped forwarding) looks perfectly open
// from JavaScript until the first call times out. Sending `{type:'ping'}`
// and expecting `{type:'pong'}` is the only liveness signal available.
const HEARTBEAT = { interval: 30 * 1000, timeout: 10 * 1000 };

// Heartbeat timers are unref'd so the beat itself never keeps a process
// alive — the live socket is already a ref'd handle, so an idle-but-connected
// client still holds the loop open. The RECONNECT timer is deliberately NOT
// unref'd: during an outage there is no socket left to hold the loop, and a
// process whose only work is a wrpc client must not exit mid-reconnect.
// (In a browser setTimeout returns a number and there is nothing to unref.)
const unref = (timer) => {
  if (timer && typeof timer.unref === 'function') timer.unref();
  return timer;
};

const normalizeReconnect = (options) => {
  const { reconnect, reconnectTimeout } = options;
  if (reconnect === false) return { ...RECONNECT, retries: 0 };
  const base = reconnectTimeout ? { ...RECONNECT, minDelay: reconnectTimeout } : RECONNECT;
  // Both paths share the invariants: a `reconnectTimeout` above the default
  // cap would otherwise silently reconnect FASTER than asked, since the cap
  // wins inside backoffDelay.
  const merged = reconnect ? { ...base, ...reconnect } : { ...base };
  if (!(merged.minDelay > 0)) merged.minDelay = RECONNECT.minDelay;
  if (!(merged.maxDelay >= merged.minDelay)) merged.maxDelay = merged.minDelay;
  return merged;
};

// Call batching. Several calls issued in the same tick travel as ONE frame
// (a JSON array), which on HTTP is one request instead of N and on a
// WebSocket is one frame instead of N. Only `call` packets batch: a ping, a
// cancel or an unsubscribe is a control packet whose whole point is to
// arrive now.
const BATCH = { flush: 'microtask', maxSize: 16, maxBytes: 64 * 1024 };

const normalizeBatch = (options) => {
  const { batch } = options;
  if (!batch) return null;
  const merged = { ...BATCH, ...(batch === true ? {} : batch) };
  const timed = typeof merged.flush === 'number' && merged.flush >= 0;
  if (!timed && merged.flush !== 'microtask') merged.flush = BATCH.flush;
  if (!(merged.maxSize > 1)) merged.maxSize = BATCH.maxSize;
  if (!(merged.maxBytes > 0)) merged.maxBytes = BATCH.maxBytes;
  return merged;
};

const normalizeHeartbeat = (options) => {
  const { heartbeat } = options;
  if (heartbeat === false || heartbeat === 0) return null;
  if (!heartbeat) return HEARTBEAT;
  const merged = { ...HEARTBEAT, ...heartbeat };
  if (!(merged.interval > 0) || !(merged.timeout > 0)) return null;
  return merged;
};

const toByteView = async (input) => {
  if (typeof input.arrayBuffer === 'function') {
    const buffer = await input.arrayBuffer();
    return new Uint8Array(buffer);
  }
  return new Uint8Array(input);
};

class WrpcError extends Error {
  constructor({ message, code, details }) {
    super(message);
    this.code = code;
    // Structured issue lists the server attached (validation paths and the
    // like) — an optional wire field, absent on most errors.
    if (details !== undefined) this.details = details;
  }
}

// Client-side pre-validation: the introspected input schema parts compiled
// by an app-injected ajv (`validation: { ajv }`), so a doomed call rejects
// locally with the SAME 400 + details it would earn from the server —
// without the round trip. wrpc imports no schema library; without the
// injection this costs nothing.
const PREVALIDATE_PARTS = [
  ['params', 'params'],
  ['querystring', 'query'],
  ['body', 'body'],
];

const compilePrevalidate = (ajv, schema) => {
  const parts = [];
  for (const [part, argsKey] of PREVALIDATE_PARTS) {
    if (schema[part] === undefined) continue;
    parts.push([part, argsKey, ajv.compile(schema[part])]);
  }
  if (parts.length === 0) return null;
  return (args) => {
    const value = args && typeof args === 'object' ? args : {};
    let issues = null;
    for (const [part, argsKey, validate] of parts) {
      if (validate(value[argsKey] ?? {})) continue;
      issues ??= [];
      for (const item of validate.errors ?? []) {
        issues.push({ message: item.message, path: `/${part}${item.instancePath ?? ''}` });
      }
    }
    if (issues) {
      const messages = [];
      for (const issue of issues) messages.push(`${issue.path} ${issue.message}`);
      throw new WrpcError({ message: messages.join('; '), code: 400, details: { issues } });
    }
    return value;
  };
};

// A declared value has to survive being a header value on http/sse, so one
// rule produces it everywhere — including the ws query, which could carry
// richer JSON but must not, or the two transports would disagree on shape.
// null/undefined DROP the key rather than sending the string 'null': a
// header cannot say "absent", and a lie the server cannot undo is worse
// than an omission it can.
const toHeaderValue = (value) => {
  if (typeof value === 'string') return value;
  if (value === null || value === undefined) return null;
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
};

// A header value must stay latin-1 or fetch throws a TypeError deep inside
// the transport, long after the mistake was made.
const HEADER_SAFE = /^[ -~]*$/;

// Normalizes one declared bag: keys to kebab, and — when the values will
// become header values — through toHeaderValue. Refusal-style like the
// server's half: a value that cannot ride drops with a warning rather than
// failing the connection it was only labelling.
const normalizeDeclared = (value, stringify, log) => {
  if (!value) return null;
  let out = null;
  for (const key in value) {
    if (key === '__proto__') continue;
    const name = toKebab(key);
    if (name.length === 0) continue;
    let entry = value[key];
    if (stringify) {
      entry = toHeaderValue(entry);
      if (entry === null) continue;
      if (!HEADER_SAFE.test(entry)) {
        log.warn({ event: 'declared.unsendable', key: name });
        continue;
      }
    }
    (out ??= {})[name] = entry;
  }
  return out;
};

// The declared-meta header block for one request, built once per open (or
// per request, when a call brings its own). Two spellings of the same bag:
// the canonical single header is type-faithful and needs one CORS entry;
// the prefixed one is what a gateway can route on, strip or inject, at the
// cost of string-only values and a CORS entry per key.
const METAS = 'x-wrpc-meta';
const metaHeaders = (meta, prefixed) => {
  if (!prefixed) return { [METAS]: encodeURIComponent(JSON.stringify(meta)) };
  const out = {};
  for (const key in meta) out[`${METAS}-${key}`] = meta[key];
  return out;
};

// ws and http spell the same endpoint with different schemes; a fallback
// list crosses that line, so the URL is re-spelled per candidate.
const mapScheme = (url, name) => {
  if (name === 'ws') return url.replace(/^https:/, 'wss:').replace(/^http:/, 'ws:');
  return url.replace(/^wss:/, 'https:').replace(/^ws:/, 'http:');
};

class ClientTransport extends Emitter {
  active = false;
  // Whether the connection stays open: only a persistent one can carry a
  // cancel, an unsubscribe or a subscription's values.
  persistent = true;
  // Opt-in: only a transport that can silently die needs an app-level
  // heartbeat. A request/response transport has nothing to keep alive, and
  // a MessagePort to a Service Worker cannot half-close.
  heartbeat = false;

  constructor(url) {
    super();
    this.url = url;
  }

  send(obj) {
    // The codec is handed to the transport at bind time (client core).
    // `obj.meta` rides along so a transport with a header block can mirror
    // it; a control packet has none, and ws/worker ignore the argument.
    this.write(this.codec ? this.codec.encode(obj) : JSON.stringify(obj), obj?.meta);
  }

  // Drop the connection without waiting for a close handshake. The default
  // is a graceful close; transports that can hang on an unresponsive peer
  // override it.
  terminate() {
    this.close();
  }

  // eslint-disable-next-line class-methods-use-this
  online() {}

  // eslint-disable-next-line class-methods-use-this
  offline() {}
}

class WrpcClient extends Emitter {
  static connections = new Set();
  static isOnline = true;

  static online() {
    WrpcClient.isOnline = true;
    for (const connection of WrpcClient.connections) {
      connection.#transport.online();
      if (!connection.active) {
        // #escalate, not emit('error'): Emitter throws for an unlistened
        // 'error', and a throw inside this loop would abandon the re-open
        // of every connection after this one.
        connection.open().catch((error) => connection.#escalate(error, 'online.open'));
      }
    }
  }

  static offline() {
    WrpcClient.isOnline = false;
    for (const connection of WrpcClient.connections) {
      connection.#transport.offline();
    }
  }

  static initialize() {
    if (typeof window !== 'undefined') {
      window.addEventListener('online', WrpcClient.online);
      window.addEventListener('offline', WrpcClient.offline);
      return;
    }
    if (typeof self !== 'undefined') {
      self.addEventListener('online', WrpcClient.online);
      self.addEventListener('offline', WrpcClient.offline);
    }
  }

  api = {};
  #log = null;
  #otel = null;
  #transport = null;
  #calls = new Map();
  #cancelled = new Set();
  #subscriptions = new Map();
  #batch = null;
  #pending = [];
  #pendingBytes = 0;
  #flushTimer = null;
  #streams = new Map();
  #callTimeout = CALL_TIMEOUT;
  #querystring = null;
  #validation = null;
  // The ordered fallback list (null without one) and the live candidate.
  #transportNames = null;
  #transportIndex = 0;
  #boundHandlers = null;
  #codec = null;
  #codecRest = null;
  #reconnect = RECONNECT;
  #reconnectTimer = null;
  #attempt = 0;
  #connected = false;
  // The authenticate hook and the settling of the first connect's afterOpen
  // run — open() awaits it so connect() returns an authenticated client.
  #authenticate = null;
  #authenticating = false;
  #opened = null;
  // The refresh hook, the codes that trigger it, and the in-flight run
  // shared by every concurrent refusal (single-flight).
  #refresh = null;
  #refreshCodes = [401];
  #refreshing = null;
  // Connection-phase headers and metadata: objects, or functions
  // re-evaluated on every open so a reconnect presents fresh values.
  #headers = null;
  #meta = null;
  #metaFormat = 'json';
  #random = Math.random;
  #generateId = generateUUID;
  #heartbeat = null;
  #pingTimer = null;
  #pongTimer = null;
  #loaded = new Set();
  // Units scaffolded from a static introspection artifact via use(). Kept
  // apart from #loaded: they are never re-introspected on reconnect — the
  // artifact is the contract, exactly as a generated .d.ts would be.
  #static = new Set();
  #unitMethods = new Map();
  #responders = new Map();
  #proxyPacket = null;
  #options = {};

  get active() {
    return this.#transport.active;
  }

  /** How many reconnect attempts have been made since the last open. */
  get attempt() {
    return this.#attempt;
  }

  constructor(url, transport, options = {}) {
    super();
    const { callTimeout, proxy, random, generateId, logger, telemetry, querystring, validation, codec } = options;
    const { authenticate } = options;
    // Presents this connection's credential: awaited inside open() on the
    // first connect, and again on every reconnect BEFORE the subscriptions
    // are re-opened and the units re-loaded — the window an 'open' listener
    // structurally cannot reach (see #afterOpen).
    if (typeof authenticate === 'function') this.#authenticate = authenticate;
    const { refresh } = options;
    // Single-flight credential refresh: on a refusal whose code is listed
    // (401 by default) the handler runs ONCE for all concurrent refusals,
    // and each refused call is re-issued exactly once. `authenticate` heals
    // a NEW connection; `refresh` heals a LIVE one whose credential expired.
    if (refresh !== undefined && refresh !== null) {
      if (typeof refresh === 'function') {
        this.#refresh = refresh;
      } else if (typeof refresh.handler === 'function') {
        this.#refresh = refresh.handler;
        if (Array.isArray(refresh.on) && refresh.on.length > 0) this.#refreshCodes = [...refresh.on];
      } else {
        throw new TypeError('WrpcClient: options.refresh must be a function or { on?, handler }');
      }
    }
    const { headers } = options;
    // Connection-phase headers, distinct from per-call `meta`: they ride as
    // REAL request headers on http/sse (and any transport that can send
    // them), and as ONE query parameter on the browser ws connect URL —
    // the WHATWG WebSocket constructor takes no headers. Validated when the
    // server's procedure declares `schema.headers`.
    if (headers !== undefined && headers !== null) {
      const valid = typeof headers === 'function' || (typeof headers === 'object' && !Array.isArray(headers));
      if (!valid) throw new TypeError('WrpcClient: options.headers must be an object or a function returning one');
      this.#headers = headers;
    }
    const { meta } = options;
    // Connection-phase metadata — headers' unvalidated sibling: never runs
    // through schema validation, lands on client.meta.data server-side, and
    // has a per-call twin (the packet `meta` field / context.callMeta).
    if (meta !== undefined && meta !== null) {
      const valid = typeof meta === 'function' || (typeof meta === 'object' && !Array.isArray(meta));
      if (!valid) throw new TypeError('WrpcClient: options.meta must be an object or a function returning one');
      this.#meta = meta;
    }
    const { metaFormat } = options;
    // A sibling option rather than a descriptor on `meta`: that option
    // already means "an object OR a function returning one", so making the
    // object form ALSO mean a descriptor would be genuinely ambiguous —
    // { value, carrier } is a legitimate meta bag today. And not inferred
    // from the data either: "prefixed when every value is a string" would
    // make the wire shape depend on the payload, so one numeric field would
    // silently flip the connection and change what CORS has to allow.
    if (metaFormat !== undefined) {
      if (metaFormat !== 'json' && metaFormat !== 'prefixed') {
        throw new TypeError("WrpcClient: options.metaFormat must be 'json' or 'prefixed'");
      }
      this.#metaFormat = metaFormat;
    }
    // Off by default, unlike the server: a client that printed on every
    // reconnect would be noise in a browser console nobody asked for.
    this.#log = createLoggerWriter(logger);
    this.#otel = createClientTelemetry(telemetry);
    if (callTimeout) this.#callTimeout = callTimeout;
    // Pluggable query-string serializer (qs and friends) for mapped REST
    // requests — the mirror of the server's `querystring` option, so array
    // encodings agree end to end.
    if (querystring && typeof querystring.stringify === 'function') this.#querystring = querystring;
    // Structural, like every injection: anything with compile(schema) -> fn
    // (ajv-shaped: boolean answer, `.errors` on failure).
    if (validation?.ajv && typeof validation.ajv.compile === 'function') this.#validation = validation.ajv;
    if (Array.isArray(options.transport)) this.#transportNames = [...options.transport];
    // The wire codec — the client half of the server's `codec` option. The
    // packet half must produce single-line text (SSE frames by line); the
    // optional `rest` section frames REST-leg bodies and may be binary.
    // Structural check shared with the server (isCodec).
    if (codec !== undefined && codec !== null) {
      if (!isCodec(codec)) {
        throw new TypeError(
          'WrpcClient: options.codec must provide encode(packet)/decode(text), a rest section, or both',
        );
      }
      this.#codec = typeof codec.encode === 'function' && typeof codec.decode === 'function' ? codec : null;
      this.#codecRest = codec.rest ?? null;
    }
    if (proxy) this.#proxyPacket = proxy;
    if (random) this.#random = random; // deterministic jitter in tests
    // Packet, subscription and stream ids; uuid v4 unless the app brings
    // its own (cuid/ulid/a test counter). Correlation ids, not secrets.
    if (typeof generateId === 'function') this.#generateId = generateId;
    this.#reconnect = normalizeReconnect(options);
    this.#heartbeat = normalizeHeartbeat(options);
    this.#batch = normalizeBatch(options);
    this.url = url;
    this.#transport = transport;
    this.#options = options;
    this.#bindTransport();
  }

  // A rejected open (an authenticate hook that failed, a refused socket)
  // must not leave a half-born client behind: open() registered it in
  // WrpcClient.connections before the transport was reached, and a failing
  // authenticate arms the reconnect timer — close() undoes both, so the
  // caller who never received the client has nothing running for it.
  static async #openOrClose(client) {
    try {
      await client.open();
    } catch (error) {
      client.close();
      throw error;
    }
    return client;
  }

  static async connect(url, options = {}) {
    if (options.worker) {
      const transport = WrpcClient.transport.event.getInstance(url);
      const client = new WrpcClient(url, transport, options);
      return WrpcClient.#openOrClose(client);
    }
    // The scheme picks the transport unless one is named. 'sse' only exists
    // once '@alexify/wrpc/sse' has been required, which is what registers it.
    // A list is an ordered fallback: candidates are tried in the given
    // order, each with its own reconnect budget — there is deliberately no
    // default order, the application names its own. All names are checked
    // UP FRONT: a fallback that fails at fall-back time is a fallback
    // nobody tested.
    if (Array.isArray(options.transport)) {
      if (options.transport.length === 0) throw new Error('transport list must not be empty');
      for (const candidate of options.transport) {
        if (candidate === 'event') throw new Error("transport list cannot contain 'event' — pass options.worker");
        if (typeof WrpcClient.transport[candidate] !== 'function') {
          throw new Error(`Unknown transport '${candidate}'`);
        }
      }
    }
    const name = Array.isArray(options.transport)
      ? options.transport[0]
      : (options.transport ?? (url.startsWith('http') ? 'http' : 'ws'));
    const Transport = WrpcClient.transport[name];
    if (typeof Transport !== 'function') {
      throw new Error(`Unknown transport '${name}'`);
    }
    const transport = new Transport(mapScheme(url, name));
    const client = new WrpcClient(url, transport, options);
    return WrpcClient.#openOrClose(client);
  }

  // An 'error' with no listener throws by design (see Emitter), which is
  // right for a synchronous mistake and wrong for a background failure: a
  // throw out of a timer or a promise chain would take the process down for
  // a reconnect that is about to be retried anyway.
  #escalate(error, event = 'client.error') {
    // A logger observes; a listener handles. Both run — the log line is not
    // a fallback for a missing listener.
    this.#log.error({ err: error, event });
    if (this.listenerCount('error') > 0) return void this.emit('error', error);
    globalThis.console?.error?.(error);
  }

  // The connection died: every pending call is settled NOW with a coded
  // rejection (503) instead of waiting out callTimeout on a corpse, its
  // timer cleared and its abort listener released.
  #failCalls() {
    if (this.#calls.size === 0 && this.#pending.length === 0) return;
    const entries = Array.from(this.#calls.values());
    this.#calls.clear();
    this.#pending.length = 0;
    this.#pendingBytes = 0;
    this.#cancelled.clear();
    for (const entry of entries) {
      clearTimeout(entry.timeout);
      entry.release();
      entry.reject(new WrpcError(CONNECTION_CLOSED_ERROR));
    }
  }

  // Mirror of the server-side Client.destroy(): an inbound stream mid-
  // transfer is terminated so its consumer's `for await` ends instead of
  // parking forever on a connection that no longer exists.
  #failStreams() {
    if (this.#streams.size === 0) return;
    const streams = Array.from(this.#streams.values());
    this.#streams.clear();
    for (const stream of streams) {
      Promise.resolve(stream.terminate()).catch(() => {});
    }
  }

  // The handlers are kept so #unbindTransport can take them OFF a
  // transport being abandoned by a fallback swap — a late 'close' from the
  // old socket would otherwise double-schedule reconnects on the new one.
  #unbindTransport() {
    if (!this.#boundHandlers) return;
    for (const [event, handler] of this.#boundHandlers) this.#transport.off(event, handler);
    this.#boundHandlers = null;
  }

  #bindTransport() {
    // The transport owns the outbound write, so it carries the codec; a
    // fallback swap re-binds and re-hands it here. The logger rides the same
    // seam: a transport that refuses something (an oversize meta block) has
    // to be able to say so where the application can see it.
    if (this.#codec) this.#transport.codec = this.#codec;
    this.#transport.log = this.#log;
    const bind = (event, handler) => {
      this.#boundHandlers ??= [];
      this.#boundHandlers.push([event, handler]);
      this.#transport.on(event, handler);
    };
    bind('open', () => {
      clearTimeout(this.#reconnectTimer);
      this.#reconnectTimer = null;
      const attempts = this.#attempt;
      this.#attempt = 0;
      this.#startHeartbeat();
      const reconnected = this.#connected;
      this.#connected = true;
      this.#log.info({ event: reconnected ? 'reconnected' : 'open', url: this.url, attempts });
      this.#otel.recordConnection(1);
      if (reconnected) this.#otel.recordReconnect('recovered', attempts);
      // Without an authenticate hook this is the historical path, spelled
      // out rather than routed through the async #afterOpen: an async hop
      // would push both the 'open' emit and #restore one microtask later,
      // and the ordering of a lifecycle emit against the subscribe packets
      // that follow it is exactly what applications end up depending on.
      //
      // Lifecycle emits are async (listeners may be): a throwing listener
      // must surface through #escalate, not as an unhandled rejection.
      if (!this.#authenticate) {
        this.emit('open').catch((error) => this.#escalate(error, 'listener.open'));
        if (reconnected) this.#restore(attempts).catch((error) => this.#escalate(error, 'reconnect.restore'));
        return;
      }
      const settled = this.#afterOpen(reconnected, attempts);
      // The first connect's rejection is open()'s to re-raise (see open());
      // every later one has nowhere to go but #escalate. Both branches
      // attach a handler in this turn, so neither is an unhandled rejection.
      if (reconnected) settled.catch((error) => this.#escalate(error, 'reconnect.afterOpen'));
      else this.#opened = settled;
    });

    bind('close', () => {
      this.#stopHeartbeat();
      // Settled before 'close' is announced: a listener reacting to the
      // close must find the calls already rejected and the streams ended,
      // not racing them.
      this.#failCalls();
      this.#failStreams();
      this.#log.info({ event: 'close', url: this.url });
      this.#otel.recordConnection(-1);
      this.emit('close').catch((error) => this.#escalate(error, 'listener.close'));
      this.#scheduleReconnect();
    });

    bind('error', (error) => {
      this.#escalate(error, 'transport.error');
    });

    bind('message', (data) => {
      const escalate = (error) => this.#escalate(error, 'message');
      if (typeof data === 'string') this.#handlePacket(data).catch(escalate);
      else this.#handleBinary(data).catch(escalate);
    });
  }

  // A reconnected socket is a NEW server-side client: whatever `load()` set
  // up (the introspected method list) belongs to the connection that just
  // died, so it is rebuilt before 'reconnect' is announced. The api unit
  // objects themselves are reused, so event listeners registered on them
  // survive — that is the whole point of reloading rather than reconnecting
  // and leaving `api` quietly stale.
  //
  // The two halves are DECOUPLED on purpose: subscriptions are re-opened
  // first and unconditionally, because a failing load() used to sit in
  // front of the resume loop in a fire-and-forget call — one introspect
  // hiccup silently killed every subscription forever. And a restore that
  // failed is a connection that is not usable as reconnected: the transport
  // is terminated so the normal reconnect path (backoff, retries,
  // 'reconnect-failed') takes over, instead of leaving a half-restored
  // client that looks open.
  // The seam the transport's 'open' event could not provide. #restore sends
  // every subscribe packet BEFORE its first await (see below), so an async
  // 'open' listener can never win that race: its body past the first await
  // resumes with the packets already on the wire. This runs the credential
  // first, awaited, and only then announces 'open' and restores.
  async #afterOpen(reconnected, attempts) {
    try {
      // The guard keeps a refresh from running inside the hook that would
      // itself be re-run by it (see #maybeRefresh).
      this.#authenticating = true;
      await this.#authenticate(this, { reconnected, attempts });
    } catch (error) {
      this.#log.warn({ event: 'authenticate.failed', err: error, attempts, url: this.url });
      void this.emit('authenticate-failed', { error, attempts, reconnected }).catch((e) =>
        this.#escalate(e, 'listener.authenticate-failed'),
      );
      // Same reasoning, same ordering, as the restore path below: put the
      // pre-open attempt count back BEFORE terminate() synchronously hands
      // it to the reconnect scheduler.
      if (this.#attempt === 0) this.#attempt = attempts;
      if (this.active) this.#transport.terminate();
      throw error;
    } finally {
      this.#authenticating = false;
    }
    // 'open' now means USABLE: a listener that fires a session-gated call
    // finds the credential already presented. Still not awaited — a listener
    // is an observer, and one that throws must not take the restore with it.
    this.emit('open').catch((error) => this.#escalate(error, 'listener.open'));
    if (reconnected) await this.#restore(attempts);
  }

  async #restore(attempts) {
    // Each subscription is re-opened from the last eventId it saw, so the
    // server can replay what was missed instead of starting over. A feed
    // that yields untracked values has no eventId and simply resumes live.
    const subscriptions = Array.from(this.#subscriptions.values());
    for (const record of subscriptions) this.#openSubscription(record);
    const units = Array.from(this.#loaded);
    try {
      if (units.length > 0) await this.load(...units);
    } catch (error) {
      this.#log.warn({ event: 'restore.failed', err: error, attempts, url: this.url });
      void this.emit('restore-failed', { error, attempts }).catch((e) => this.#escalate(e, 'listener.restore-failed'));
      // The attempt counter was reset to 0 the moment the socket opened, so
      // a restore failing afterwards used to hand #scheduleReconnect a zero:
      // retry at minDelay forever, the window never growing, `retries` never
      // exhausting, the next fallback transport never reached. Put the
      // pre-open count back FIRST — terminate() emits 'close' synchronously,
      // and that is where the scheduler reads it. The guard covers the other
      // ordering: a socket that died on its own already advanced the
      // counter, and that number is the fresher one.
      if (this.#attempt === 0) this.#attempt = attempts;
      // Still "open" from the transport's point of view — force the cycle.
      if (this.active) this.#transport.terminate();
      throw error;
    }
    await this.emit('reconnect', { units, attempts, subscriptions: subscriptions.length });
  }

  // Ends every live subscription NOW, loudly — the path a fallback onto a
  // request/response transport takes: re-sending `subscribe` packets the
  // server would refuse one by one (400 each) would be the quiet version
  // of the same failure.
  #failSubscriptions(message) {
    if (this.#subscriptions.size === 0) return;
    const records = Array.from(this.#subscriptions.values());
    this.#subscriptions.clear();
    for (const record of records) {
      record.onRelease?.();
      const error = new WrpcError({ message, code: 400 });
      record.stream?.fail(error);
      if (record.onError) record.onError(error);
      else if (!record.stream) this.#escalate(error, 'subscription.error');
    }
  }

  // The next candidate on the fallback list takes over: the dead
  // transport's listeners come off (a late 'close' from its socket must
  // not double-drive the new one), the URL is re-spelled for the new
  // scheme, and capability loss is loud rather than silent.
  #advanceTransport() {
    if (!this.#transportNames || this.#transportIndex >= this.#transportNames.length - 1) return false;
    const from = this.#transportNames[this.#transportIndex];
    this.#transportIndex++;
    const to = this.#transportNames[this.#transportIndex];
    this.#unbindTransport();
    try {
      this.#transport.terminate();
    } catch {
      // Already dead — that is why we are here.
    }
    const Transport = WrpcClient.transport[to];
    this.#transport = new Transport(mapScheme(this.url, to));
    this.#bindTransport();
    if (this.#transport.persistent === false) {
      this.#failSubscriptions(`Transport fell back to '${to}', which cannot carry subscriptions`);
    }
    this.#log.warn({ event: 'transport.fallback', from, to, url: this.url });
    this.emit('transport-fallback', { from, to }).catch((error) =>
      this.#escalate(error, 'listener.transport-fallback'),
    );
    return true;
  }

  #scheduleReconnect() {
    if (this.active) return;
    if (!WrpcClient.connections.has(this)) return;
    if (this.#reconnectTimer) return;
    const { retries } = this.#reconnect;
    if (this.#attempt >= retries) {
      // Exhaustion falls through the candidate list before it is final:
      // retries are PER CANDIDATE (the counter resets), and the fresh
      // candidate gets an immediate first try — backoff was guarding the
      // old transport's endpoint, not this one.
      if (this.#advanceTransport()) {
        this.#attempt = 0;
        return void this.open().catch((error) => {
          this.#escalate(error, 'fallback.open');
          this.#scheduleReconnect();
        });
      }
      this.#log.warn({ event: 'reconnect.failed', attempts: this.#attempt, url: this.url });
      this.#otel.recordReconnect('exhausted', this.#attempt);
      return void this.emit('reconnect-failed', { attempts: this.#attempt }).catch((error) =>
        this.#escalate(error, 'listener.reconnect-failed'),
      );
    }
    const delay = backoffDelay({ ...this.#reconnect, attempt: this.#attempt, random: this.#random });
    this.#attempt++;
    this.#log.debug({ event: 'reconnecting', attempt: this.#attempt, delay, url: this.url });
    this.emit('reconnecting', { attempt: this.#attempt, delay }).catch((error) =>
      this.#escalate(error, 'listener.reconnecting'),
    );
    // Not unref'd: see the note on `unref` above.
    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = null;
      this.open().catch((error) => {
        // A rejected open never emitted 'close', so nothing else would
        // schedule the next attempt — the loop has to continue here.
        this.#escalate(error, 'reconnect.open');
        this.#scheduleReconnect();
      });
    }, delay);
  }

  // ---------------------------------------------------------------------
  // Heartbeat: ping -> await pong -> ping. One timer is armed at a time, so
  // a stalled connection cannot pile pings up behind the missing pong.

  #startHeartbeat() {
    this.#stopHeartbeat();
    if (!this.#heartbeat || !this.#transport.heartbeat) return;
    this.#armPing();
  }

  #stopHeartbeat() {
    if (this.#pingTimer) clearTimeout(this.#pingTimer);
    if (this.#pongTimer) clearTimeout(this.#pongTimer);
    this.#pingTimer = null;
    this.#pongTimer = null;
  }

  #armPing() {
    this.#pingTimer = unref(
      setTimeout(() => {
        this.#pingTimer = null;
        this.#sendPing();
      }, this.#heartbeat.interval),
    );
  }

  #sendPing() {
    if (!this.active) return;
    try {
      this.send({ type: 'ping' });
    } catch (error) {
      return void this.#escalate(error, 'heartbeat.ping');
    }
    this.#pongTimer = unref(
      setTimeout(() => {
        this.#pongTimer = null;
        this.#onHeartbeatTimeout();
      }, this.#heartbeat.timeout),
    );
  }

  #onPong() {
    if (!this.#pongTimer) return; // unsolicited pong: nothing was waiting
    clearTimeout(this.#pongTimer);
    this.#pongTimer = null;
    if (this.#heartbeat && this.active) this.#armPing();
  }

  // The peer stopped answering: close so the transport reports 'close' and
  // the normal reconnect path takes over.
  #onHeartbeatTimeout() {
    this.#log.warn({ event: 'heartbeat.timeout', url: this.url });
    this.emit('heartbeat-timeout');
    try {
      this.#transport.terminate();
    } catch (error) {
      this.#escalate(error, 'heartbeat.terminate');
    }
  }

  // Re-evaluated on EVERY open, the function form included: a version bump,
  // a rotated device id or a fresh bearer token is picked up by the
  // reconnect that follows, not frozen at construction.
  async #resolveDeclared(source) {
    const value = typeof source === 'function' ? await source() : source;
    if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
    return value;
  }

  async open() {
    WrpcClient.connections.add(this);
    // A fresh options object per open, never a mutation: #options belongs
    // to the application, and #advanceTransport keeps rebuilding candidates
    // from the pristine url.
    let options = this.#options;
    if (this.#headers || this.#meta) {
      options = { ...options };
      // Headers are ALWAYS stringified: `{ v: 2 }` used to arrive as '2' over
      // http (fetch coerces) and vanish over ws (the server keeps a flat
      // string map), so the two transports quietly disagreed. Meta keeps its
      // JSON types here — only the prefixed carrier flattens them.
      if (this.#headers) {
        options.headers = normalizeDeclared(await this.#resolveDeclared(this.#headers), true, this.#log);
      }
      if (this.#meta) {
        // The prefixed carrier makes every value a header value, so it
        // stringifies — on ws and the worker too, where the JSON parameter
        // could have carried richer types. The guarantee this option makes
        // is about the bag the SERVER observes, not about the wire, and a
        // bag whose shape changed with the transport would break it.
        const prefixed = this.#metaFormat === 'prefixed';
        options.meta = normalizeDeclared(await this.#resolveDeclared(this.#meta), prefixed, this.#log);
        if (prefixed) options.metaPrefixed = true;
      }
    }
    await this.#transport.open(options);
    // Assigned synchronously by the 'open' handler above (every transport
    // emits 'open' before its open() resolves), so a first connect with an
    // authenticate hook is awaited here: connect() resolves authenticated.
    const opened = this.#opened;
    if (!opened) return;
    this.#opened = null;
    await opened;
  }

  close() {
    clearTimeout(this.#reconnectTimer);
    this.#reconnectTimer = null;
    this.#opened = null;
    this.#stopHeartbeat();
    // Anything still queued leaves before the socket does; a call whose
    // packet never shipped would otherwise wait out its whole timeout.
    this.flush();
    // Snapshotted and cleared BEFORE anyone is told: a listener that reacts by
    // unsubscribing, or by closing again, then finds nothing left to do
    // instead of mutating the map being walked — and `handle.closed` already
    // reads true by the time it is asked.
    const ended = Array.from(this.#subscriptions.values());
    this.#subscriptions.clear();
    // An explicit close ends the session: a later open() is a fresh start,
    // not a reconnect, so it must not replay 'reconnect'.
    this.#connected = false;
    this.#attempt = 0;
    WrpcClient.connections.delete(this);
    // An explicit close settles everything in flight the same way a dropped
    // connection does — flushed packets included, since their answers can
    // no longer arrive.
    this.#failCalls();
    this.#failStreams();
    this.#transport.close();
    // Last, with nothing left to tear down. Closing the client ends every
    // subscription it carried, and a subscription that ended has to say so:
    // `unsubscribe()` stays silent because the caller named that one feed,
    // whereas close() is usually called by something else entirely (a page
    // teardown, a shutdown hook), so the code owning the feed never asked.
    for (const record of ended) this.#endSubscription(record);
  }

  /**
   * The terminal signal for one subscription, in the order the `end` packet
   * delivers it. A listener that throws is contained rather than allowed to
   * rob the rest of theirs — or, inside close(), to abandon the teardown.
   */
  #endSubscription(record) {
    try {
      record.onRelease?.();
      record.onEnd?.();
    } catch (error) {
      this.#escalate(error, 'subscription.listener');
    }
    // Always, even if a listener threw: an `iterate()` consumer is parked in
    // next() and would otherwise wait there forever.
    record.stream?.end();
  }

  write(data) {
    this.#transport.write(data);
  }

  send(data) {
    // Only calls batch: everything else is a control packet whose value is
    // that it leaves now.
    if (this.#batch && data?.type === 'call') return void this.#enqueue(data);
    this.#transport.send(data);
  }

  #enqueue(packet) {
    // Serialized ONCE: the same text is the size accounting and the wire
    // bytes — the old path stringified for the length, dropped the string,
    // and paid stringify again at flush. Sizes are UTF-16 code units
    // (byte-exact for ASCII payloads; a bound either way).
    const text = this.#codec ? this.#codec.encode(packet) : JSON.stringify(packet);
    const size = text.length;
    this.#pending.push({ packet, text, size });
    this.#pendingBytes += size;
    const { maxSize, maxBytes } = this.#batch;
    if (this.#pending.length >= maxSize || this.#pendingBytes >= maxBytes) return void this.flush();
    this.#schedule();
  }

  #schedule() {
    if (this.#flushTimer !== null) return;
    const { flush } = this.#batch;
    if (flush === 'microtask') {
      this.#flushTimer = 'microtask';
      queueMicrotask(() => this.flush());
      return;
    }
    this.#flushTimer = unref(setTimeout(() => this.flush(), flush));
  }

  /** Sends whatever calls are waiting to be batched. Safe to call anytime. */
  flush() {
    if (this.#flushTimer !== null && this.#flushTimer !== 'microtask') clearTimeout(this.#flushTimer);
    this.#flushTimer = null;
    if (this.#pending.length === 0) return;
    const pending = this.#pending;
    this.#pending = [];
    this.#pendingBytes = 0;
    // A batch of one is that packet: no reason to make the peer unwrap it.
    // The frame is assembled from the texts serialized at enqueue — flush
    // never runs JSON.stringify again.
    let frame = pending[0].text;
    // The per-call meta of everything in this frame, merged in order so the
    // LAST writer wins. One POST has one header block, so this is a lossy
    // SUMMARY for the infrastructure between the two ends — a gateway, a
    // WAF, an access log — and deliberately not a data channel: each call's
    // exact meta is already in its own packet's `meta` field, which the
    // batch never touches and which is what becomes context.callMeta.
    // Allocated only when something actually carries meta.
    let meta = null;
    for (let i = 0; i < pending.length; i++) {
      const carried = pending[i].packet.meta;
      if (carried) meta = Object.assign(meta ?? {}, carried);
    }
    if (pending.length > 1) {
      if (this.#codec) {
        // Only the codec knows its framing, so the batch array is encoded
        // whole — per-item texts served the size accounting (a documented
        // double encode on this deliberate slow path).
        const packets = new Array(pending.length);
        for (let i = 0; i < pending.length; i++) packets[i] = pending[i].packet;
        frame = this.#codec.encode(packets);
      } else {
        // Concatenated in place: map() built a throwaway array of the very
        // texts already sitting in `pending`, just to hand them to join().
        frame = `[${pending[0].text}`;
        for (let i = 1; i < pending.length; i++) frame += `,${pending[i].text}`;
        frame += ']';
      }
    }
    try {
      this.#transport.write(frame, meta);
    } catch (error) {
      this.#escalate(error, 'batch.flush');
    }
  }

  // A call cancelled before its batch left never has to be cancelled on the
  // wire — dropping it here is both cheaper and safer than racing a `cancel`
  // packet ahead of the `call` it refers to.
  #unqueue(id) {
    const index = this.#pending.findIndex((entry) => entry.packet.id === id);
    if (index < 0) return false;
    const [entry] = this.#pending.splice(index, 1);
    this.#pendingBytes -= entry.size;
    return true;
  }

  getStream(id) {
    const stream = this.#streams.get(id);
    if (stream) return stream;
    throw new Error(`Stream ${id} is not initialized`);
  }

  createStream(name, size) {
    const id = this.#generateId();
    // The binary chunk header stores the id length in one byte, so a custom
    // generator emitting more than 255 UTF-8 bytes would only fail later,
    // deep inside chunkEncode — refuse it here, where the cause is visible.
    if (typeof id !== 'string' || id.length === 0 || id.length > 255) {
      throw new TypeError('createStream: generateId must return a string of at most 255 characters');
    }
    return new WrpcWritable(id, name, size, this);
  }

  createBlobUploader(blob) {
    const { name = 'blob', size } = blob;
    const consumer = this.createStream(name, size);
    const { id } = consumer;
    const upload = async () => {
      for await (const chunk of blob.stream()) {
        consumer.write(chunk);
      }
      consumer.end();
    };
    return { id, upload };
  }

  // Inbound half of the wire codec; null for malformed, like jsonParse.
  #decodePacket(text) {
    if (!this.#codec) return jsonParse(text);
    try {
      return this.#codec.decode(text);
    } catch {
      return null;
    }
  }

  async #handlePacket(data) {
    const packet = this.#decodePacket(data);
    if (!packet) {
      if (this.#proxyPacket) return void this.#proxyPacket(data, null);
      throw new Error('Invalid JSON packet');
    }
    // A batch frame answers several packets at once; each one is dispatched
    // exactly as it would have been on its own.
    if (Array.isArray(packet)) {
      if (this.#proxyPacket) return void this.#proxyPacket(data, packet);
      // Contained per item: one answer this client cannot route must not
      // strand the rest of the batch, which is the guarantee the protocol
      // makes about a failure inside a batch.
      for (const item of packet) {
        try {
          await this.#dispatch(item ?? {});
        } catch (error) {
          this.#escalate(error, 'batch.dispatch');
        }
      }
      return;
    }
    const { type } = packet;
    // Heartbeat first, and before the proxy hand-off: the pong answers a
    // ping this client sent, so it is never anyone else's packet.
    if (type === 'pong') return void this.#onPong();
    if (type === 'ping') return void this.send({ type: 'pong' });
    if (this.#proxyPacket) return void this.#proxyPacket(data, packet);
    await this.#dispatch(packet);
  }

  async #dispatch(packet) {
    const { type, id, name } = packet;
    if (type === 'event') {
      // An event carrying an id is an ask: the server wants an answer, and
      // that is a different contract from fan-out delivery to listeners.
      if (typeof id === 'string' && id) return void (await this.#answerAsk(name, packet.data, id));
      return void (await this.#handleEvent(name, packet.data));
    }
    if (!id) throw new Error('Packet structure error');
    if (type === 'callback') return void this.#settle(packet);
    if (type === 'data' || type === 'end') return void this.#handleSubscriptionPacket(packet);
    if (type === 'stream') await this.#handleStream(packet);
  }

  #settle(packet) {
    const { id } = packet;
    const call = this.#calls.get(id);
    // The 499 acknowledging a cancel this client sent: the caller was
    // rejected the moment it aborted, so the ack is expected, not an error.
    if (!call && this.#cancelled.delete(id)) return;
    if (!call) throw new Error(`Callback ${id} not found`);
    this.#calls.delete(id);
    clearTimeout(call.timeout);
    call.release?.();
    if (packet.error) return void call.reject(new WrpcError(packet.error));
    call.resolve(packet.result);
  }

  // `data` carries one value of a subscription; `end` closes it, with an
  // `error` when the server-side generator threw. A packet for a
  // subscription this client already dropped is ignored: the unsubscribe and
  // the last values in flight cross on the wire, and that is normal.
  #handleSubscriptionPacket(packet) {
    const { type, id } = packet;
    const record = this.#subscriptions.get(id);
    if (!record) return;
    if (type === 'data') {
      // Remembered for the resume after a reconnect. Untracked values leave
      // it alone: a feed with no ids simply has no resume point.
      if (packet.eventId !== undefined) record.lastEventId = packet.eventId;
      record.onData?.(packet.data);
      record.stream?.push(packet.data);
      return;
    }
    this.#subscriptions.delete(id);
    if (!packet.error) return void this.#endSubscription(record);
    record.onRelease?.();
    const error = new WrpcError(packet.error);
    record.stream?.fail(error);
    if (record.onError) return void record.onError(error);
    // Nobody asked to hear about it, but a subscription that died must not
    // die quietly.
    if (!record.stream) this.#escalate(error, 'subscription.error');
  }

  /**
   * Registers the answer this client gives when the server asks `name`
   * ('unit/event'). One responder per name — two answers to one question
   * are ambiguous, so a duplicate registration throws, like Emitter's
   * duplicate-listener guard. Client-level rather than per-unit on purpose:
   * unit objects carry server-named methods, where a method called
   * 'respond' would collide.
   */
  respond(name, handler) {
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('respond: name must be a non-empty string');
    }
    if (typeof handler !== 'function') {
      throw new TypeError('respond: handler must be a function');
    }
    if (this.#responders.has(name)) {
      throw new Error(`Duplicate responder for '${name}'`);
    }
    this.#responders.set(name, handler);
  }

  unrespond(name) {
    return this.#responders.delete(name);
  }

  // The server asked: run the responder and answer with the callback packet
  // the id makes addressable. No responder is answered too — a 501 now beats
  // the server discovering nothing at its timeout.
  async #answerAsk(name, data, id) {
    const responder = typeof name === 'string' ? this.#responders.get(name) : undefined;
    if (!responder) {
      this.send({ type: 'callback', id, error: { message: `No responder for '${name}'`, code: 501 } });
      return void (await this.emit('unhandled-event', { name, data, id }));
    }
    try {
      const result = await responder(data);
      this.send({ type: 'callback', id, result });
    } catch (error) {
      const code = typeof error?.code === 'number' ? error.code : 500;
      const wire = { message: error?.message ?? 'Responder failed', code };
      // Same exposure rule the server applies: 4xx details are part of the
      // conversation, a 5xx's internals stay here unless the error opts in.
      if (error?.details !== undefined && (code < 500 || error.expose === true)) wire.details = error.details;
      this.send({ type: 'callback', id, error: wire });
    }
  }

  // Events are addressed 'unit/event'. One that reaches no listener — an
  // unloaded unit, or a loaded one nobody subscribed to — surfaces as
  // 'unhandled-event' rather than vanishing: a silently dropped broadcast is
  // indistinguishable from a broken server.
  async #handleEvent(name, data) {
    if (typeof name === 'string') {
      const slash = name.indexOf('/');
      if (slash > 0) {
        const unit = name.slice(0, slash);
        const eventName = name.slice(slash + 1);
        // `api` is a plain object, so a wire-supplied unit like 'constructor'
        // or 'toString' resolves up the prototype chain. Only an Emitter this
        // client put there itself is a real unit — anything else is an event
        // nobody is listening for.
        const apiUnit = this.api[unit];
        if (eventName && apiUnit instanceof Emitter && apiUnit.listenerCount(eventName) > 0) {
          return void (await apiUnit.emit(eventName, data));
        }
      }
    }
    await this.emit('unhandled-event', { name, data });
  }

  async #handleStream(packet) {
    const { id, name, size, status } = packet;
    const stream = this.#streams.get(id);
    if (status === undefined) {
      if (stream) {
        throw new Error(`Stream ${name} is already initialized`);
      }
      const readableStream = new WrpcReadable(id, name, size);
      this.#streams.set(id, readableStream);
      return;
    }
    if (!stream) throw new Error(`Stream ${id} is not initialized`);
    if (status === 'end') {
      await stream.close();
      this.#streams.delete(id);
    } else if (status === 'terminate') {
      await stream.terminate();
      this.#streams.delete(id);
    }
  }

  async #handleBinary(input) {
    const byteView = await toByteView(input);
    const { id, payload } = chunkDecode(byteView);
    const stream = this.#streams.get(id);
    if (!stream) {
      throw new Error(`Stream ${id} is not initialized`);
    }
    await stream.push(payload);
  }

  async load(...units) {
    if (!this.active) throw new Error('Not connected');
    const introspection = await this.#call('system/introspect', units);
    for (const unit of units) {
      const instance = introspection[unit];
      if (!instance) continue;
      this.#loaded.add(unit);
      // Dynamic wins: a live introspection supersedes a static artifact for
      // the same unit, and from here on the unit reloads on reconnect.
      this.#static.delete(unit);
      this.#scaffoldUnit(unit, instance);
    }
  }

  /**
   * Static introspection: scaffolds units from an introspection artifact
   * (the raw `system/introspect` shape, e.g. generated by
   * `wrpc types --schema`) with no wire traffic. Synchronous and usable
   * before open(). A unit already load()ed is skipped (dynamic wins);
   * static units are never re-introspected on reconnect.
   */
  use(introspection) {
    if (typeof introspection !== 'object' || introspection === null || Array.isArray(introspection)) {
      throw new TypeError('use() takes an introspection object: { unit: { method: info } }');
    }
    for (const unit of Object.keys(introspection)) {
      const instance = introspection[unit];
      if (typeof instance !== 'object' || instance === null || Array.isArray(instance)) {
        throw new TypeError(`use(): unit '${unit}' must be an object of method infos`);
      }
      if (this.#loaded.has(unit)) continue;
      this.#static.add(unit);
      this.#scaffoldUnit(unit, instance);
    }
    return this;
  }

  #scaffoldUnit(unit, instance) {
    // Reuse the unit's emitter when it already exists: a reconnect reloads
    // every unit, and replacing the object would silently drop every event
    // listener the caller registered on it.
    let methods = this.api[unit];
    if (!(methods instanceof Emitter)) {
      methods = new Emitter();
      // defineProperty, not assignment: a unit named '__proto__' would go
      // through Object.prototype's setter and mutate the prototype instead
      // of becoming a unit.
      Object.defineProperty(this.api, unit, {
        value: methods,
        enumerable: true,
        writable: true,
        configurable: true,
      });
    }
    const request = this.#scaffold(unit);
    const methodNames = Object.keys(instance);
    const previous = this.#unitMethods.get(unit);
    if (previous) {
      // A method the server no longer exposes must stop being callable.
      for (const methodName of previous) {
        if (!methodNames.includes(methodName)) delete methods[methodName];
      }
    }
    for (const methodName of methodNames) {
      methods[methodName] = request(methodName, instance[methodName]);
    }
    this.#unitMethods.set(unit, new Set(methodNames));
  }

  /** Sends a fire-and-forget event to the server; `name` is 'unit/event'. */
  sendEvent(name, data) {
    const packet = { type: 'event', name, data };
    // Fire-and-forget still deserves a parent: the server opens a CONSUMER
    // span for the event, and this is what links it to the caller's trace.
    this.#otel.inject(packet);
    this.send(packet);
  }

  // `unit` is the introspection unit key — 'auth' or the pinned 'auth.v1' —
  // so the version already rides inside it; the wire target is 'unit/name'.
  #target(unit, methodName) {
    return `${unit}/${methodName}`;
  }

  /**
   * One call addressed by wire target ('unit/name', 'unit.vN/name') with no
   * scaffolding: the escape hatch for a method whose unit has not been
   * load()ed — which is every method inside an `authenticate` hook on a
   * first connect, since `api` is built by load() and load() runs after
   * auth. No client-side pre-validation and no REST leg: the packet path.
   */
  call(method, args = {}, options = {}) {
    return this.#call(method, args, options);
  }

  // One-shot retry through the injected refresh hook: on a refusal whose
  // code the hook covers, run the (single-flight) refresh and re-issue the
  // call ONCE — with a fresh packet id, since the once-path builds a new
  // packet. The retry calls the once-path directly, so a second refusal
  // surfaces as-is: no loop by construction. Never fires inside the
  // authenticate hook (#authenticating) — the hook heals connections, and a
  // refresh triggered by its own calls would recurse.
  #withRefresh(issue) {
    return issue().catch(async (error) => {
      if (!this.active || this.#authenticating || !this.#refreshCodes.includes(error?.code)) throw error;
      try {
        await this.#runRefresh(error);
      } catch {
        // The refresh's own failure is the hook's private business — the
        // caller gets the refusal it actually received.
        throw error;
      }
      return issue();
    });
  }

  #runRefresh(error) {
    // Ten concurrent refusals produce ONE handler run; everyone awaits it.
    // The first refusal's error is the one the handler sees.
    return (this.#refreshing ??= Promise.resolve(this.#refresh(this, error)).finally(() => {
      this.#refreshing = null;
    }));
  }

  #call(target, args, options = {}) {
    if (!this.#refresh) return this.#callOnce(target, args, options);
    return this.#withRefresh(() => this.#callOnce(target, args, options));
  }

  #callOnce(target, args, options = {}) {
    const id = this.#generateId();
    const { signal } = options;
    const packet = { type: 'call', id, method: target, args };
    if (options.meta !== undefined) packet.meta = options.meta;
    if (!this.#otel.enabled) return this.#dispatchCall(target, packet, id, signal);
    const started = now();
    return this.#otel.withSpan({ packet, target }, (handle) => {
      // Injected INSIDE the span so the traceparent names this call's span,
      // which is what the server will pick up as its parent.
      this.#otel.inject(packet);
      return this.#dispatchCall(target, packet, id, signal).then(
        (result) => {
          this.#otel.endSpan(handle, { 'wrpc.status': 'ok' });
          this.#otel.recordCall(target, 'ok', now() - started);
          return result;
        },
        (error) => {
          this.#otel.recordError(handle, error);
          this.#otel.endSpan(handle, { 'wrpc.status': 'error', 'rpc.wrpc.status_code': error?.code });
          this.#otel.recordCall(target, 'error', now() - started);
          throw error;
        },
      );
    });
  }

  // The REST leg of a mapped procedure: args arrive in the same
  // { params, query, body } shape the procedure sees on every transport;
  // here they become the path, the query string and the JSON body. The
  // response is the plain result (or the wire error object) — external
  // REST semantics, not a callback envelope.
  async #restCall(http, args = {}, options = {}) {
    const { signal, meta } = options;
    if (signal?.aborted) throw new WrpcError(CANCELLED_ERROR);
    const method = http.method;
    const safeMethod = method === 'GET' || method === 'HEAD';
    // codec.rest frames the REST leg's bodies (values, not packets; binary
    // allowed) in both directions; without it the leg speaks JSON as ever.
    const rest = this.#codecRest;
    const encodeBody = () => (rest ? rest.encode(args.body) : JSON.stringify(args.body));
    const body = safeMethod || args?.body === undefined ? undefined : encodeBody();
    const url = this.#restUrl(http, args);
    let res;
    try {
      // On REST there is no packet, so per-call meta and connection meta are
      // the SAME channel — the server unpacks the request's whole declared
      // bag into packet.meta. Merging is the existing semantics rather than
      // a workaround, and the per-call half wins. Safe from the batching
      // problem by construction: this leg is one call per request.
      res = await this.#transport.request(method, url, body, signal, { rest, meta });
    } catch (error) {
      if (signal?.aborted) throw new WrpcError(CANCELLED_ERROR);
      throw new WrpcError({ message: `HTTP request failed: ${error?.message ?? error}`, code: 503 });
    }
    if (res.status === 204) return undefined;
    let parsed;
    if (rest) {
      // Error statuses decode through the same codec — the wire error
      // object arrives codec-framed exactly like a result.
      try {
        parsed = res.body.length === 0 ? undefined : rest.decode(res.body);
      } catch {
        parsed = null;
      }
    } else {
      parsed = jsonParse(res.text);
    }
    if (res.status >= 200 && res.status < 300) return parsed;
    const wire = parsed && typeof parsed === 'object' && typeof parsed.message === 'string' ? parsed : null;
    throw new WrpcError({
      message: wire?.message ?? `HTTP request failed (${res.status})`,
      code: wire?.code ?? res.status,
      details: wire?.details,
    });
  }

  #restUrl(http, args) {
    const params = args?.params ?? {};
    const segments = http.path === '/' ? [] : http.path.slice(1).split('/');
    const parts = new Array(segments.length);
    for (let i = 0; i < segments.length; i++) {
      const segment = segments[i];
      if (segment.startsWith(':')) {
        const value = params[segment.slice(1)];
        if (value === undefined) {
          throw new WrpcError({
            message: `Missing path param '${segment.slice(1)}' for ${http.method} ${http.path}`,
            code: 400,
          });
        }
        parts[i] = encodeURIComponent(String(value));
      } else {
        parts[i] = segment;
      }
    }
    const baseUrl = this.#transport.url ?? this.url;
    let url = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
    if (parts.length > 0) url += `/${parts.join('/')}`;
    const query = args?.query;
    if (query && Object.keys(query).length > 0) {
      const text = this.#querystring ? this.#querystring.stringify(query) : String(new URLSearchParams(query));
      if (text) url += `?${text}`;
    }
    return url;
  }

  #dispatchCall(target, packet, id, signal) {
    return new Promise((resolve, reject) => {
      if (signal?.aborted) return void reject(new WrpcError(CANCELLED_ERROR));
      const timeout = setTimeout(() => {
        if (!this.#calls.has(id)) return;
        this.#calls.delete(id);
        this.#unqueue(id);
        release();
        reject(new Error('Request timeout'));
      }, this.#callTimeout);
      const onAbort = () => {
        if (!this.#calls.has(id)) return;
        this.#calls.delete(id);
        clearTimeout(timeout);
        // Still queued: drop it instead of racing a cancel ahead of the
        // call. And a request/response transport cannot carry a cancel at
        // all — sending one there only earns a 400 nobody can route.
        if (!this.#unqueue(id) && this.active && this.#transport.persistent !== false) {
          this.#cancelled.add(id);
          this.send({ type: 'cancel', id });
        }
        reject(new WrpcError(CANCELLED_ERROR));
      };
      const release = () => signal?.removeEventListener('abort', onAbort);
      signal?.addEventListener('abort', onAbort, { once: true });
      this.#calls.set(id, { resolve, reject, timeout, release });
      this.send(packet);
    });
  }

  /**
   * Opens a subscription. `onData` receives every value; the returned handle
   * carries the last seen eventId and can stop it. The subscription is
   * re-opened from that eventId automatically after a reconnect.
   */
  #subscribe(target, args, options = {}) {
    const id = this.#generateId();
    const record = {
      id,
      target,
      args,
      lastEventId: options.lastEventId,
      onData: options.onData ?? null,
      onError: options.onError ?? null,
      onEnd: options.onEnd ?? null,
      stream: options.stream ?? null,
      // Lets iterate() drop its abort listener however the subscription ends.
      onRelease: options.onRelease ?? null,
    };
    const live = this.#subscriptions;
    live.set(id, record);
    this.#openSubscription(record);
    return {
      id,
      unsubscribe: () => this.#unsubscribe(id),
      get lastEventId() {
        return record.lastEventId;
      },
      get closed() {
        return !live.has(id);
      },
    };
  }

  #openSubscription(record) {
    const packet = { type: 'subscribe', id: record.id, method: record.target, args: record.args };
    if (record.lastEventId !== undefined && record.lastEventId !== null) {
      packet.lastEventId = record.lastEventId;
    }
    // The server-side subscription span becomes a child of whatever trace
    // was active when the subscription was opened (or re-opened).
    this.#otel.inject(packet);
    this.send(packet);
  }

  #unsubscribe(id) {
    const record = this.#subscriptions.get(id);
    if (!record) return false;
    this.#subscriptions.delete(id);
    record.onRelease?.();
    record.stream?.end();
    // The server answers with `end`, which lands on a record that is gone —
    // ignored on purpose, the caller already knows.
    if (this.active) this.send({ type: 'unsubscribe', id });
    return true;
  }

  #scaffold(unit) {
    const createMethod = (methodName, info = {}) => {
      const target = this.#target(unit, methodName);
      if (info.kind !== 'subscription') {
        // Compiled once per scaffold — load() re-runs on every reconnect,
        // so a schema change on the server lands with the reload.
        const prevalidate = this.#validation && info.schema ? compilePrevalidate(this.#validation, info.schema) : null;
        const guard = (args) => {
          if (!prevalidate) return null;
          try {
            prevalidate(args);
          } catch (error) {
            return Promise.reject(error);
          }
          return null;
        };
        // A procedure with an `http` mapping, on a transport that can carry
        // it, goes out as the SAME REST request an external consumer would
        // send — one endpoint, two audiences. Every other transport speaks
        // packets as always.
        let fn;
        if (info.http && this.#transport.rest === true) {
          fn = (args = {}, options = {}) =>
            guard(args) ??
            (this.#refresh
              ? this.#withRefresh(() => this.#restCall(info.http, args, options))
              : this.#restCall(info.http, args, options));
        } else {
          fn = (args = {}, options = {}) => guard(args) ?? this.#call(target, args, options);
        }
        // Per-call metadata, bound once: api.unit.m.withMeta({ idem })(args).
        // Works on both legs — the packet path carries it as the packet's
        // `meta` field, the mapped REST leg as request headers.
        // Keys are normalized HERE, at bind time rather than per invocation,
        // so a bound method costs no more than it did before. Values keep
        // their JSON types — the packet field is JSON, and only a header
        // carrier has to flatten. The raw client.call(target, args, { meta })
        // escape hatch is deliberately NOT normalized: it is the seam the
        // auth hooks write against, and it hands the wire exactly what it was
        // given.
        fn.withMeta = (meta) => {
          const bound = normalizeDeclared(meta, false, this.#log);
          return (args = {}, options = {}) => fn(args, { ...options, meta: bound });
        };
        return fn;
      }
      // A subscription is not callable: it answers with a stream, so it
      // exposes the two ways to consume one instead of pretending to be a
      // function that resolves once.
      return {
        kind: 'subscription',
        subscribe: (args = {}, options = {}) => this.#subscribe(target, args, options),
        iterate: (args = {}, options = {}) => this.#iterate(target, args, options),
      };
    };
    return createMethod;
  }

  #iterate(target, args, options = {}) {
    const { signal } = options;
    const stream = createEventStream({ signal, highWaterMark: options.highWaterMark });
    // An already-aborted signal means the caller is gone before it started:
    // opening a subscription nobody will consume would leave a generator
    // running on the server with no handle to stop it.
    if (signal?.aborted) {
      const iterator = stream[Symbol.asyncIterator]();
      iterator.subscription = { id: null, lastEventId: undefined, closed: true, unsubscribe: () => false };
      return iterator;
    }
    // Breaking out of `for await` (or aborting) has to reach the server, and
    // whichever ends it first has to release the listener the other used.
    let stop = null;
    const release = () => {
      if (!stop) return;
      signal?.removeEventListener('abort', stop);
      stop = null;
    };
    const handle = this.#subscribe(target, args, {
      ...options,
      stream,
      onData: options.onData ?? null,
      onRelease: release,
    });
    stop = () => {
      stop = null;
      handle.unsubscribe();
    };
    signal?.addEventListener('abort', stop, { once: true });
    const iterator = stream[Symbol.asyncIterator]();
    const originalReturn = iterator.return.bind(iterator);
    iterator.return = () => {
      release();
      handle.unsubscribe();
      return originalReturn();
    };
    iterator.subscription = handle;
    return iterator;
  }
}

module.exports = {
  WrpcClient,
  WrpcError,
  ClientTransport,
  WRPC_PROTOCOL,
  CALL_TIMEOUT,
  normalizeReconnect,
  metaHeaders,
  unref,
  toByteView,
};
