'use strict';

// WrpcPeer: one wrpc peer — a router others can call, a signaler to find
// them through, an RTC adapter to reach them with — and PeerLink: one
// connected peer, both directions.
//
//   peer A                                     peer B
//   link.remote (WrpcClient) ──channel 0──▶ PeerHost.attach → Client
//   PeerHost.attach → Client ◀──channel 1── link.remote (WrpcClient)
//
// Roles are decided by id order alone: the peer with the LOWER id is the
// RtcLink initiator (it offers, it restarts ICE, it redials), the other is
// the polite responder. connect() works from either side — a responder
// that wants a link sends the initiator a 'connect' knock over signaling,
// and the initiator dials. So there is never an offer glare by
// construction, and a responder recovering from a failed link asks the
// same way.
//
// Failure: the RtcLink reports 'failed' (ICE did not come back inside the
// restart window); the client half's transport went 'close' and its
// WrpcClient runs the ordinary backoff/reconnect cycle, whose open() waits
// for the link to be connected again; the host half's Client is destroyed
// and re-attached on the next 'open'. The PeerLink drives the link: the
// initiator redials with backoff, the responder knocks with backoff, both
// give up after `redial.retries` and close.

const { Emitter, backoffDelay } = require('../utils.js');
const { WrpcClient } = require('../client/core.js');
const { createLoggerWriter } = require('../logging.js');
const { createServerTelemetry } = require('../telemetry/server.js');
const { isRtcAdapter, createW3cAdapter } = require('./port.js');
const { RtcLink, DEFAULT_CHANNELS, normalizeChannels } = require('./link.js');
const { ClientRtcTransport, RtcPeerTransport } = require('./transport.js');
const { PeerHost } = require('./host.js');
const { isSignaler, isSignalMessage } = require('./signaler.js');

const REDIAL = { retries: 5, minDelay: 500, maxDelay: 10_000, factor: 2, jitter: true };
// The remote WrpcClient's reconnect: quick, since the link itself carries
// the real backoff, and unbounded, since the PeerLink decides when a link
// is over.
const CLIENT_RECONNECT = { minDelay: 100, maxDelay: 2_000, jitter: true, retries: Infinity };

const normalizeRedial = (redial) => {
  if (redial === false) return { ...REDIAL, retries: 0 };
  const merged = { ...REDIAL, ...(redial && typeof redial === 'object' ? redial : {}) };
  if (!(merged.retries >= 0)) merged.retries = REDIAL.retries;
  if (!(merged.minDelay > 0)) merged.minDelay = REDIAL.minDelay;
  if (!(merged.maxDelay >= merged.minDelay)) merged.maxDelay = merged.minDelay;
  return merged;
};

const isPeerId = (value) => typeof value === 'string' && value.length > 0;

/** One peer, both directions. Constructed by WrpcPeer; never directly. */
class PeerLink extends Emitter {
  #peer;
  #id;
  #instance;
  #room;
  #data;
  #link;
  #remote;
  #host;
  #hostOptions;
  #client = null;
  #rooms = new Set();
  #state = 'connecting';
  #everOpen = false;
  #redial;
  #redialAttempt = 0;
  #redialTimer = null;
  #opened;
  #log;
  #otel;
  #counted = false;
  #unbind = [];

  constructor(peer, { id, instance, room, data, link, host, hostOptions, client, framing, redial, log, otel }) {
    super();
    this.#peer = peer;
    this.#id = id;
    this.#instance = instance;
    this.#room = room;
    this.#data = data;
    this.#link = link;
    this.#host = host;
    this.#hostOptions = hostOptions;
    this.#redial = redial;
    this.#log = log;
    this.#otel = otel;
    this.#opened = deferred();
    const transport = new ClientRtcTransport(`webrtc:${id}`, { link, framing, ...hostOptions.water });
    this.#remote = new WrpcClient(`webrtc:${id}`, transport, {
      logger: false,
      ...client,
      reconnect: client.reconnect === false ? false : { ...CLIENT_RECONNECT, ...(client.reconnect ?? {}) },
    });
    this.#bindAll();
  }

  /** The remote peer's id. */
  get id() {
    return this.#id;
  }

  /**
   * The remote peer's incarnation — the `instance` its signaler carries —
   * once known, else null. A signal from the same id under another instance
   * is another endpoint, and this link is stale.
   */
  get instance() {
    return this.#instance;
  }

  /** The signaling room this link was made in, or null. */
  get room() {
    return this.#room;
  }

  /** The remote peer's roster data, when known. */
  get data() {
    return this.#data;
  }

  /** True when this side dialled (lower id). */
  get initiator() {
    return this.#link.initiator;
  }

  /** 'connecting' | 'open' | 'reconnecting' | 'closed' */
  get state() {
    return this.#state;
  }

  /** The RtcLink underneath. */
  get link() {
    return this.#link;
  }

  /** The WrpcClient that calls the remote peer's router. */
  get remote() {
    return this.#remote;
  }

  /** The remote's scaffolded api (after load()). */
  get api() {
    return this.#remote.api;
  }

  /** The host-side Client the remote peer is to this router; null while down. */
  get client() {
    return this.#client;
  }

  /** The host-side rooms this link is kept in across redials (a copy). */
  get rooms() {
    return new Set(this.#rooms);
  }

  /** True once both directions are up. */
  get open() {
    return this.#state === 'open';
  }

  /** Resolves when both directions are up; rejects when the link closes first. */
  ready() {
    return this.#opened.promise;
  }

  // ---- the remote's api, delegated

  load(...units) {
    return this.#remote.load(...units);
  }

  call(method, args, options) {
    return this.#remote.call(method, args, options);
  }

  /** Answers the remote peer's asks; replaces an earlier responder of that name. */
  respond(name, handler) {
    this.#remote.unrespond(name);
    this.#remote.respond(name, handler);
  }

  unrespond(name) {
    return this.#remote.unrespond(name);
  }

  // ---- the host side, addressed

  /** An event to the remote peer, through this router's Client for it. */
  send(name, data) {
    this.#hostClient('send').sendEvent(name, data);
  }

  /** Asks the remote peer; answered by its remote.respond(). */
  ask(name, data, options) {
    return this.#hostClient('ask').ask(name, data, options);
  }

  /** A binary stream to the remote peer. */
  createStream(name, size) {
    return this.#hostClient('createStream').createStream(name, size);
  }

  /** Keeps this link's host Client in `room`, now and after every redial. */
  join(room) {
    this.#rooms.add(room);
    if (this.#client) this.#client.join(room);
  }

  leave(room) {
    this.#rooms.delete(room);
    if (this.#client) this.#client.leave(room);
  }

  /** Goodbye: the peer is told, both directions end, no redial. */
  close() {
    if (this.#state === 'closed') return;
    this.#link.close();
  }

  // ---- lifecycle (WrpcPeer drives these)

  /**
   * @internal Dials. The initiator offers; a responder that asked for the
   * link itself (connect() from the higher id) knocks so the initiator
   * dials — one answering an offer already arriving does not.
   */
  start({ knock = false } = {}) {
    this.#link.start();
    if (knock && !this.initiator) this.#knock();
    // The first open: the client half waits for the link; a failure before
    // the first open is the redial cycle's to retry, and the client is only
    // opened once the link is.
    void this.#link.waitOpen().then(
      () => this.#openClient(),
      () => {},
    );
  }

  /** @internal A knock from the responder: dial again if this side is the initiator and the link failed. */
  knocked() {
    if (this.initiator && this.#link.state === 'failed') this.#redialNow();
  }

  /** @internal A signal from the remote peer. */
  receive(message) {
    return this.#link.receive(message);
  }

  /** @internal The remote's instance, learned from its first signal or roster entry. */
  adopt(instance) {
    if (this.#instance === null && typeof instance === 'string') this.#instance = instance;
  }

  /**
   * @internal Closes without a goodbye: after a signaling reset this side's
   * id changed, or the remote came back as another incarnation, so a
   * 'close' sent now would reach the peer from a stranger — or worse, land
   * on the fresh link already being made. The peer learns through the
   * roster (leave/join) or through ICE.
   */
  abandon() {
    void this.#link.receive({ type: 'close' });
  }

  #hostClient(what) {
    if (!this.#client) throw new Error(`PeerLink.${what}: the link to '${this.#id}' is not open`);
    return this.#client;
  }

  #bindAll() {
    const link = this.#link;
    const remote = this.#remote;
    const on = (emitter, name, fn) => {
      emitter.on(name, fn);
      this.#unbind.push(() => emitter.off(name, fn));
    };
    on(link, 'open', () => this.#attachHost());
    on(link, 'state', (state) => {
      if (state === 'failed') this.#onFailed();
      else if (state === 'closed') this.#onClosed();
    });
    on(link, 'error', (error) => this.#error(error));
    on(link, 'restart', ({ outcome }) => this.#otel.recordRtcRestart(outcome));
    on(remote, 'open', () => this.#onUp());
    on(remote, 'reconnect-failed', () => this.close());
    // Bound for the client's whole life, not unbound on close: an open()
    // still in flight when the link closes settles afterwards, and an
    // unheard client error goes to the console.
    remote.on('error', (error) => {
      // While the link is down its reconnect attempts fail by design; only
      // an error on a live link is anyone's business.
      if (this.#state === 'open') this.#error(error);
      else this.#log.debug({ err: error, event: 'rtc.peer.client', state: this.#state });
    });
  }

  #attachHost() {
    if (!this.#host || this.#client) return;
    const transport = new RtcPeerTransport(this.#link, {
      peer: this.#id,
      framing: this.#hostOptions.framing,
      ...this.#hostOptions.water,
      onError: (error) => this.#error(error),
    });
    const client = this.#host.attach(transport, { peer: this.#id, room: this.#room, data: this.#data });
    this.#client = client;
    for (const room of this.#rooms) client.join(room);
    transport.once('close', () => {
      if (this.#client === client) this.#client = null;
    });
    void this.emit('attach', client).catch((error) => this.#error(error));
  }

  async #openClient() {
    if (this.#state === 'closed') return;
    try {
      await this.#remote.open();
    } catch (error) {
      // The link went down between its 'open' and the client's: the redial
      // cycle owns the retry, and only a failure on a LIVE link is news.
      if (this.#state !== 'closed' && this.#link.state === 'connected') this.#error(error);
    }
  }

  #onUp() {
    if (this.#state === 'closed') return;
    this.#redialAttempt = 0;
    const first = !this.#everOpen;
    this.#everOpen = true;
    this.#setState('open');
    this.#count(1);
    if (first) {
      this.#opened.resolve(this);
      void this.emit('open').catch((error) => this.#error(error));
    } else {
      void this.emit('reconnect').catch((error) => this.#error(error));
    }
  }

  #onFailed() {
    if (this.#state === 'closed') return;
    this.#setState('reconnecting');
    this.#count(-1);
    if (this.#redialAttempt >= this.#redial.retries) {
      this.#log.warn({ event: 'rtc.peer.gave-up', attempts: this.#redialAttempt });
      return void this.close();
    }
    const delay = backoffDelay({ ...this.#redial, attempt: this.#redialAttempt });
    this.#redialAttempt++;
    this.#log.info({ event: 'rtc.peer.redial', attempt: this.#redialAttempt, delay, initiator: this.initiator });
    clearTimeout(this.#redialTimer);
    this.#redialTimer = setTimeout(() => {
      this.#redialTimer = null;
      if (this.#state === 'closed') return;
      this.#otel.recordRtcRedial(this.#role);
      if (this.initiator) this.#redialNow();
      else if (this.#link.state === 'failed') this.#knock();
      // A responder whose initiator never answers fails again on the
      // connect timeout of nothing — so it re-arms itself here.
      if (!this.initiator && this.#link.state === 'failed') this.#onFailed();
    }, delay);
  }

  #redialNow() {
    if (this.#link.redial()) {
      void this.#link.waitOpen().then(
        () => this.#openClient(),
        () => {},
      );
    }
  }

  #knock() {
    this.#peer.signal(this.#id, { type: 'connect' }, this.#room);
  }

  #onClosed() {
    if (this.#state === 'closed') return;
    this.#setState('closed');
    this.#count(-1);
    clearTimeout(this.#redialTimer);
    this.#redialTimer = null;
    for (const unbind of this.#unbind) unbind();
    this.#unbind = [];
    // The client's reconnect cycle stops here; its transport's close() on a
    // closed link is a no-op.
    this.#remote.close();
    this.#opened.reject(new Error(`PeerLink to '${this.#id}' closed`));
    this.#peer.released(this);
    void this.emit('close').catch((error) => this.#error(error));
  }

  get #role() {
    return this.initiator ? 'initiator' : 'responder';
  }

  // The open-links gauge: +1 when both directions come up, -1 once when
  // they go down, whatever the order of failure and close.
  #count(delta) {
    if (delta > 0 && this.#counted) return;
    if (delta < 0 && !this.#counted) return;
    this.#counted = delta > 0;
    this.#otel.recordRtcLink(delta, this.#role);
  }

  #setState(state) {
    if (this.#state === state) return;
    this.#state = state;
    void this.emit('state', state).catch((error) => this.#error(error));
  }

  #error(error) {
    this.#log.error({ err: error, event: 'rtc.peer.error', peer: this.#id });
    if (this.listenerCount('error') > 0) return void this.emit('error', error).catch(() => {});
    this.#peer.escalate(error, this);
  }
}

const deferred = () => {
  const record = {};
  record.promise = new Promise((resolve, reject) => {
    record.resolve = resolve;
    record.reject = reject;
  });
  record.promise.catch(() => {});
  return record;
};

class WrpcPeer extends Emitter {
  #signaler;
  #adapter;
  #configuration;
  #channels;
  #host = null;
  #router;
  #clientOptions;
  #hostOptions;
  #framing;
  #connectTimeout;
  #restartTimeout;
  #redial;
  #accept;
  #log;
  #otel;
  #links = new Map();
  // Signals for a peer whose accept() is still pending.
  #pending = new Map();
  #meshes = new Map();
  #started = null;
  #closed = false;
  #onSignal = (event) => void this.#receive(event);
  #onReset = (event) => void this.#reset(event);
  #onReplaced = (event) => void this.#replaced(event);

  constructor(options = {}) {
    super();
    const {
      router = null,
      signaler,
      rtc = null,
      configuration = {},
      iceServers,
      channels = {},
      client = {},
      host = {},
      framing = {},
      connectTimeout,
      restartTimeout,
      redial = {},
      accept = null,
      logger = false,
      telemetry = null,
    } = options;
    if (!isSignaler(signaler)) throw new TypeError('WrpcPeer: options.signaler must satisfy the Signaler contract');
    const adapter = rtc ?? createW3cAdapter();
    if (!isRtcAdapter(adapter)) throw new TypeError('WrpcPeer: options.rtc must satisfy the RtcAdapter contract');
    if (accept !== null && typeof accept !== 'function') {
      throw new TypeError('WrpcPeer: options.accept must be a function');
    }
    if (typeof client !== 'object' || client === null) {
      throw new TypeError('WrpcPeer: options.client must be an object');
    }
    if (typeof host !== 'object' || host === null) throw new TypeError('WrpcPeer: options.host must be an object');
    this.#signaler = signaler;
    this.#adapter = adapter;
    this.#configuration = iceServers ? { ...configuration, iceServers } : configuration;
    this.#channels = normalizeChannels({ ...DEFAULT_CHANNELS, ...channels });
    this.#router = router;
    this.#log = createLoggerWriter(logger).child({ component: 'peer' });
    this.#clientOptions = client;
    const { highWaterMark, lowWaterMark, ...hostRest } = host;
    const water = {};
    if (highWaterMark !== undefined) water.highWaterMark = highWaterMark;
    if (lowWaterMark !== undefined) water.lowWaterMark = lowWaterMark;
    this.#hostOptions = { water, framing };
    this.#framing = framing;
    this.#connectTimeout = connectTimeout;
    this.#restartTimeout = restartTimeout;
    this.#redial = normalizeRedial(redial);
    this.#accept = accept;
    if (router) this.#host = new PeerHost({ logger, telemetry, ...hostRest, router });
    // One writer for the peer: the host's when there is one (so its spans,
    // connection gauge and the link instruments share instruments), its
    // own otherwise — a client-only peer still has links to count.
    this.#otel = this.#host ? this.#host.otel : createServerTelemetry(telemetry);
    // Listening from the start: a peer that never called start() itself
    // still answers a knock — start() runs on the first signal.
    signaler.on('signal', this.#onSignal);
    signaler.on('reset', this.#onReset);
    signaler.on('replaced', this.#onReplaced);
  }

  /** This peer's id: the signaler's, once start() resolved. */
  get id() {
    return this.#signaler.id;
  }

  get signaler() {
    return this.#signaler;
  }

  get host() {
    return this.#host;
  }

  get router() {
    return this.#host ? this.#host.router : this.#router;
  }

  get channels() {
    return this.#channels;
  }

  /** Every link, keyed by remote id (a copy). */
  get links() {
    return new Map(this.#links);
  }

  /** The link to `id`, or undefined. */
  link(id) {
    return this.#links.get(id);
  }

  /** Identifies through the signaler and starts listening for peers. */
  start() {
    if (this.#closed) return Promise.reject(new Error('WrpcPeer is closed'));
    return (this.#started ??= this.#signaler.ready().catch((error) => {
      this.#started = null;
      throw error;
    }));
  }

  /**
   * A link to `remoteId`, dialled from either side; idempotent while one
   * exists — unless `options.instance` names another incarnation of the
   * id than the one linked, which abandons the stale link and dials the new
   * endpoint. Resolves with the PeerLink once both directions are up.
   */
  async connect(remoteId, options = {}) {
    if (!isPeerId(remoteId)) throw new TypeError('WrpcPeer.connect: remoteId must be a non-empty string');
    await this.start();
    if (remoteId === this.id) throw new Error('WrpcPeer.connect: cannot connect to self');
    const instance = isPeerId(options.instance) ? options.instance : null;
    const existing = this.#links.get(remoteId);
    if (existing && this.#current(existing, instance)) return existing.ready();
    const link = this.#create(remoteId, options.room ?? null, options.data ?? null, instance);
    link.start({ knock: true });
    return link.ready();
  }

  /** Joins a signaling room and links with everyone in it: a Mesh. */
  join(room, options = {}) {
    const { Mesh } = require('./mesh.js');
    if (typeof room !== 'string' || room.length === 0) {
      throw new TypeError('WrpcPeer.join: room must be a non-empty string');
    }
    const existing = this.#meshes.get(room);
    if (existing) return existing;
    const mesh = new Mesh(this, room, options);
    this.#meshes.set(room, mesh);
    mesh.once('left', () => this.#meshes.delete(room));
    return mesh;
  }

  /** The Mesh for `room`, if joined. */
  mesh(room) {
    return this.#meshes.get(room);
  }

  /** Closes every link and mesh; the signaler is the owner's to close. */
  close() {
    if (this.#closed) return;
    this.#closed = true;
    this.#signaler.off('signal', this.#onSignal);
    this.#signaler.off('reset', this.#onReset);
    this.#signaler.off('replaced', this.#onReplaced);
    for (const mesh of [...this.#meshes.values()]) mesh.detach();
    this.#meshes.clear();
    for (const link of [...this.#links.values()]) link.close();
    this.#pending.clear();
    void this.emit('close').catch((error) => this.escalate(error));
  }

  // ---- internal (PeerLink / Mesh call these)

  /** @internal */
  signal(to, message, room) {
    try {
      const result = this.#signaler.send(to, message, room === null ? undefined : { room });
      if (result && typeof result.then === 'function') result.catch((error) => this.escalate(error));
    } catch (error) {
      this.escalate(error);
    }
  }

  /** @internal A link closed: forget it. */
  released(link) {
    if (this.#links.get(link.id) === link) this.#links.delete(link.id);
  }

  /** @internal A link (or mesh) with nobody listening for its error. */
  escalate(error, source = null) {
    if (this.listenerCount('error') > 0) return void this.emit('error', error, source).catch(() => {});
    this.#log.error({ err: error, event: 'rtc.peer.unhandled' });
  }

  /** @internal Whether any mesh other than `except` holds `id`. */
  held(id, except) {
    for (const mesh of this.#meshes.values()) if (mesh !== except && mesh.has(id)) return true;
    return false;
  }

  // Whether `existing` is still the link to its peer: it is, unless
  // `instance` says the id now lives at another endpoint — then the stale
  // link is abandoned (no goodbye: it would land on the new one) and the
  // answer is false. A link that never learned an instance adopts the
  // first it sees.
  #current(existing, instance) {
    if (instance === null) return true;
    if (existing.instance === null) {
      existing.adopt(instance);
      return true;
    }
    if (existing.instance === instance) return true;
    this.#log.info({ event: 'rtc.peer.incarnation', peer: existing.id, previous: existing.instance, instance });
    existing.abandon();
    return false;
  }

  #create(remoteId, room, data, instance = null) {
    const localId = this.id;
    const link = new RtcLink({
      localId,
      remoteId,
      adapter: this.#adapter,
      configuration: this.#configuration,
      channels: this.#channels,
      connectTimeout: this.#connectTimeout,
      restartTimeout: this.#restartTimeout,
      log: this.#log,
      signal: (message) => this.signal(remoteId, message, room),
    });
    const peerLink = new PeerLink(this, {
      id: remoteId,
      instance,
      room,
      data,
      link,
      host: this.#host,
      hostOptions: this.#hostOptions,
      client: this.#clientOptions,
      framing: this.#framing,
      redial: this.#redial,
      log: this.#log.child({ peer: remoteId }),
      otel: this.#otel,
    });
    this.#links.set(remoteId, peerLink);
    void this.emit('link', peerLink).catch((error) => this.escalate(error));
    return peerLink;
  }

  async #receive({ from, instance, room, message }) {
    if (this.#closed || !isPeerId(from) || !isSignalMessage(message)) return;
    try {
      await this.start();
    } catch (error) {
      return void this.escalate(error);
    }
    if (this.#closed || this.id === null || from === this.id) return;
    const incarnation = isPeerId(instance) ? instance : null;
    const existing = this.#links.get(from);
    const stale = existing !== undefined && !this.#current(existing, incarnation);
    if (existing && !stale) {
      if (message.type === 'connect') return void existing.knocked();
      return void (await existing.receive(message));
    }
    // Unknown peer: a knock or an offer opens a link, once accept() agrees;
    // a candidate or close for a link we do not have is noise. Except after
    // a stale link went: its redial may already have reached the new
    // incarnation and this is that side's answer, so the initiator (lower
    // id) dials afresh on anything the newcomer says.
    const pending = this.#pending.get(from);
    if (pending) return void pending.push(message);
    const opening =
      message.type === 'connect' ||
      (message.type === 'description' && message.description?.type === 'offer') ||
      (stale && message.type !== 'close' && this.id < from);
    if (!opening) return;
    const queue = [message];
    this.#pending.set(from, queue);
    let accepted = true;
    try {
      accepted = this.#accept === null ? true : await this.#accept(from, room ?? null);
    } catch (error) {
      accepted = false;
      this.escalate(error);
    }
    this.#pending.delete(from);
    if (this.#closed) return;
    if (accepted !== true) {
      this.#log.info({ event: 'rtc.peer.refused', peer: from, room });
      if (message.type !== 'close') this.signal(from, { type: 'close' }, room ?? null);
      return;
    }
    const link = this.#create(from, room ?? null, null, incarnation);
    link.start();
    for (const queued of queue) {
      if (queued.type === 'connect') link.knocked();
      // A stale-incarnation answer or candidate belongs to the link that
      // went; the fresh dial above is what reaches the newcomer.
      else if (!stale || queued.type !== 'description' || queued.description?.type === 'offer') {
        await link.receive(queued);
      }
    }
  }

  // The signaler re-identified. Under a stable identity the id is the same
  // and the links, which never needed signaling to keep working, stay; a
  // Mesh re-adopts them from the rosters the reset carries. When the id
  // changed, every link was made under the old one and the remote side
  // keys it by that: close them all, without a goodbye.
  #reset(event) {
    const changed = event?.id !== event?.previous;
    this.#log.warn({ event: 'rtc.peer.reset', id: event?.id, previous: event?.previous, changed });
    if (changed) for (const link of [...this.#links.values()]) link.abandon();
    void this.emit('reset', event).catch((error) => this.escalate(error));
  }

  // A newer connection took this peer id: this incarnation is over. The
  // remote peers drop their links to it as the roster says so; here the
  // links are abandoned (a goodbye would come from a stranger) and the peer
  // closes. The owner decides whether a page starts a new one.
  #replaced(event) {
    if (this.#closed) return;
    this.#log.warn({ event: 'rtc.peer.replaced', id: event?.id });
    for (const link of [...this.#links.values()]) link.abandon();
    void this.emit('replaced', event).catch((error) => this.escalate(error));
    this.close();
  }
}

module.exports = { WrpcPeer, PeerLink, normalizeRedial, REDIAL };
