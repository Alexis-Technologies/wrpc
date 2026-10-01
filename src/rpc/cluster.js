'use strict';

const crypto = require('node:crypto');

const { Emitter, jsonParse } = require('../utils.js');
const { generateUUID } = require('../runtime/node.js');
const { createLoggerWriter } = require('../logging.js');
const { DISABLED, SPAN_KIND_CONSUMER } = require('../telemetry/shared.js');
const { hasBytes, encodeAttachments } = require('../attachments.js');
const { ReplayWindow, DEFAULT_REPLAY_WINDOW } = require('../encryption/envelope.js');

// Cluster: presence, introspection and node-to-node messaging across every
// wrpc instance sharing a backplane. Built ON TOP of the pub/sub contract
// (publish/subscribe/close) rather than into it, so every adapter — memory,
// redis, anything isBackplane-shaped — gets cluster operations without
// implementing correlation, timeouts or aggregation itself. The same wire
// mechanics socket.io's cluster adapter arrives at (a shared channel plus a
// per-node response channel), expressed over the contract instead of a class
// hierarchy.
//
// Two channels, both held for the life of the process:
//
//   cluster            — every node; presence, wide requests and commands
//   inst:<instanceId>  — one node; its answers and addressed commands
//
// Presence is REPLICATED, not requested: join/leave publishes a ±1 delta,
// a periodic snapshot corrects whatever an at-most-once broker dropped, and
// count()/presence() read a local map with no network at all. Liveness
// counts every message a node sends — the snapshot only fills silence — and
// a graceful close() says goodbye so eviction is immediate.
//
// Requests (fetchClients, ask) know their respondent set from presence and
// complete the moment the last live node answers; the timeout is a backstop
// that resolves with `incomplete: true`, never silently.

const ENVELOPE_VERSION = 1;
const CLUSTER_CHANNEL = 'cluster';
const INSTANCE_CHANNEL_PREFIX = 'inst:';

const DEFAULT_PRESENCE_INTERVAL = 5_000;
const DEFAULT_REQUEST_TIMEOUT = 2_000;
// The per-node ceiling on one fetchClients reply: each descriptor embeds
// client.data, and an unbounded reply is ONE pub/sub message — big enough
// fan-ins used to be able to kill the requester's broker connection
// (redis's client-output-buffer-limit) and take every room channel with it.
const DEFAULT_MAX_FETCH = 1_000;
// How far a signed envelope's clock may sit from this node's — both ways. It
// is the one bound on what a node that was NOT listening can be replayed: a
// counter window only remembers what this process heard.
const DEFAULT_MAX_SKEW = 30_000;

// The rooms an app opts into replicating: an array, a predicate or a
// RegExp. Null replicates everything — fine for topic rooms, expensive for
// the per-user `user:<id>` pattern, where presence pays O(nodes x rooms)
// heap for rooms nobody ever queries with presence()/count().
const normalizeRoomsFilter = (rooms) => {
  if (typeof rooms === 'function') return rooms;
  if (Array.isArray(rooms)) {
    const set = new Set(rooms);
    return (room) => set.has(room);
  }
  if (rooms instanceof RegExp) return (room) => rooms.test(room);
  return null;
};

// Order-independent 32-bit digest of a room->count presence table: FNV-1a
// per entry, summed mod 2^32. Receivers hold the same entries in arrival
// order, so an order-sensitive digest would false-mismatch forever.
const entryHash = (room, count) => {
  let h = 0x811c9dc5;
  for (let i = 0; i < room.length; i++) {
    h ^= room.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  h ^= count;
  return Math.imul(h, 0x01000193) >>> 0;
};

const instanceChannel = (instance) => INSTANCE_CHANNEL_PREFIX + instance;

// The instance a prefixed client id belongs to: everything before the first
// dot. A standalone Client's id has no dot and no instance — not addressable.
const instanceOfClientId = (id) => {
  if (typeof id !== 'string') return null;
  const dot = id.indexOf('.');
  return dot > 0 ? id.slice(0, dot) : null;
};

class Cluster extends Emitter {
  #backplane;
  #otel;
  #instance;
  #epoch;
  #log;
  #local;
  #presenceInterval;
  #presenceTimeout;
  #requestTimeout;
  #maxFetch;
  #roomsFilter;
  #secret;
  // Replay protection under `secret` (see #admit): this node's own counter,
  // and per sender name the life it follows — { epoch, window, newest }.
  #strict = true;
  #maxSkew = DEFAULT_MAX_SKEW;
  #seq = 0;
  #guards = new Map();
  // `${from}\0${reason}` -> when it was last warned about: a replayed or
  // unsequenced envelope repeats, and one warn a presence timeout says it.
  #refused = new Map();
  #envelope = null;
  // False under `attachments: false`: bytes then stay the JSON 1.0 made of
  // them on every leg, this one included.
  #bytes = true;
  #generateId;
  // instance -> { epoch, lastSeen, clients, rooms: Map<room, count> }
  #nodes = new Map();
  // requestId -> { missing: Set<instance>, answers, errors, expected, timer, resolve }
  #requests = new Map();
  #responders = new Map();
  #unsubscribes = [];
  #timer = null;
  #closed = false;
  // Channels whose subscribe failed and is being retried (see #subscribeTo).
  #pendingSubs = 0;

  /**
   * `local` is the seam to the owning RpcServer: how remote requests and
   * commands reach this node's clients without Cluster knowing the server.
   *   count(room), snapshot() -> { clients, rooms }
   *   descriptors(sel) -> Array
   *   join(sel, rooms) / leave(sel, rooms) / disconnect(sel)
   *   event(sel, name, data)   — deliver one event to the selected client(s)
   *   ask(rooms, name, data, timeout, onCount) -> Promise<{answers, errors}>
   */
  constructor({
    backplane = null,
    instance,
    local,
    log = globalThis.console,
    otel = null,
    generateId = null,
    options = {},
  }) {
    super();
    this.#backplane = backplane;
    this.#otel = otel ?? DISABLED;
    this.#instance = instance;
    this.#local = local;
    this.#log = createLoggerWriter(log);
    // RpcServer hands down a generator it already resolved and probed, so
    // this is a trust, not a second validation — an id-per-connection server
    // must not pay a probe per Cluster either.
    this.#generateId = typeof generateId === 'function' ? generateId : generateUUID;
    // The boot marker. instanceId may be STABLE across restarts ('node-1');
    // the epoch never is, which is how a receiver tells "restarted, replace
    // its counters" from "same process, merge". It comes from the same
    // generator as every other id so one injection covers the whole server.
    this.#epoch = this.#generateId();
    const interval = options.presenceInterval > 0 ? options.presenceInterval : DEFAULT_PRESENCE_INTERVAL;
    this.#presenceInterval = interval;
    this.#presenceTimeout = options.presenceTimeout > 0 ? options.presenceTimeout : interval * 3;
    this.#requestTimeout = options.requestTimeout > 0 ? options.requestTimeout : DEFAULT_REQUEST_TIMEOUT;
    this.#maxFetch = options.maxFetch === 0 ? 0 : options.maxFetch > 0 ? options.maxFetch : DEFAULT_MAX_FETCH;
    this.#roomsFilter = normalizeRoomsFilter(options.rooms);
    // Opt-in envelope authentication (`cluster: { secret }`). The backplane
    // is a TRUST PEER of every node: without a secret, anything that can
    // publish on the `cluster` channel can disconnect every client or join
    // anyone to any room. The HMAC turns "can publish" into "can publish
    // AND holds the shared secret". Node-only by construction (node:crypto)
    // — this file never ships to a browser.
    this.#secret = typeof options.secret === 'string' && options.secret.length > 0 ? options.secret : null;
    // What a signature alone does not say: WHEN and WHERE. `replay` is new
    // in 2.0, so a value that is not one of its two is a TypeError rather
    // than the lenient fallback the 1.0 options above keep.
    const { replay = 'strict', maxSkew = DEFAULT_MAX_SKEW } = options;
    if (replay !== 'strict' && replay !== 'accept') {
      throw new TypeError("RpcServer: options.cluster.replay must be 'strict' or 'accept'");
    }
    if (!(Number.isFinite(maxSkew) && maxSkew > 0)) {
      throw new TypeError('RpcServer: options.cluster.maxSkew must be a positive number of milliseconds');
    }
    this.#strict = replay === 'strict';
    this.#maxSkew = maxSkew;
    // The envelope codec the core built from `cluster.compression`, or null —
    // applied AFTER signing, so the signature is over the JSON text as ever.
    this.#envelope = options.envelope ?? null;
    this.#bytes = options.attachments !== false;
  }

  get instanceId() {
    return this.#instance;
  }

  get epoch() {
    return this.#epoch;
  }

  /** Without a backplane every operation degrades to its local half. */
  get connected() {
    return this.#backplane !== null;
  }

  /**
   * False while a channel subscribe is failing (and being retried): the
   * node is half-connected — it can publish but cannot hear — which is
   * exactly what a readiness probe should drain it on. 'degraded' and
   * 'recovered' fire on the transitions.
   */
  get healthy() {
    return this.#pendingSubs === 0;
  }

  // -----------------------------------------------------------------------
  // Lifecycle

  start() {
    if (!this.#backplane || this.#closed) return;
    const ready = [this.#subscribeTo(CLUSTER_CHANNEL), this.#subscribeTo(instanceChannel(this.#instance))];
    // The newcomer announce goes out only once this node's own inbox is
    // live: existing nodes answer the hello with ADDRESSED state, and an
    // answer racing our SUBSCRIBE would be lost — the newcomer would stay
    // cold until the first periodic snapshot instead of warming instantly.
    Promise.all(ready).then(() => {
      if (this.#closed) return;
      this.#post(CLUSTER_CHANNEL, { t: 'hello', ...this.#snapshot() });
    });
    const timer = setInterval(() => this.#tick(), this.#presenceInterval);
    if (typeof timer.unref === 'function') timer.unref();
    this.#timer = timer;
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = null;
    // Goodbye first, while the publish path still works: receivers evict
    // immediately instead of waiting out presenceTimeout.
    if (this.#backplane) this.#post(CLUSTER_CHANNEL, { t: 'bye' });
    for (const off of this.#unsubscribes) this.#safeOff(off);
    this.#unsubscribes.length = 0;
    // Whatever was waiting on the cluster resolves with what it has.
    for (const [requestId, request] of Array.from(this.#requests)) {
      this.#requests.delete(requestId);
      clearTimeout(request.timer);
      request.settle(true);
    }
    this.#nodes.clear();
    this.#guards.clear();
    this.#refused.clear();
  }

  // Subscribes with capped-backoff RETRY on rejection: a rejected subscribe
  // used to be terminal and silent — the node kept publishing but could
  // never hear again, a half-connected state nothing surfaced.
  #subscribeTo(channel, attempt = 0) {
    const handler = (message) => this.#receive(message, channel);
    return Promise.resolve()
      .then(() => this.#backplane.subscribe(channel, handler))
      .then(
        (off) => {
          if (attempt > 0) {
            this.#pendingSubs--;
            this.#log.warn({ event: 'cluster.recovered', channel, attempt });
            if (this.#pendingSubs === 0) {
              void Promise.resolve(this.emit('recovered', { channel })).catch((error) =>
                this.#log.error({ err: error, event: 'cluster.listener', name: 'recovered' }),
              );
            }
          }
          if (typeof off !== 'function') return;
          if (this.#closed) return void this.#safeOff(off);
          this.#unsubscribes.push(off);
        },
        (error) => {
          this.#log.error({ err: error, event: 'cluster.subscribe', channel, attempt });
          if (this.#closed) return;
          if (attempt === 0) {
            this.#pendingSubs++;
            void Promise.resolve(this.emit('degraded', { channel, error })).catch((e) =>
              this.#log.error({ err: e, event: 'cluster.listener', name: 'degraded' }),
            );
          }
          const delay = Math.min(30_000, 500 * 2 ** attempt);
          const timer = setTimeout(() => {
            if (!this.#closed) this.#subscribeTo(channel, attempt + 1);
          }, delay);
          if (typeof timer.unref === 'function') timer.unref();
        },
      );
  }

  // An adapter's unsubscribe may throw or reject; either way it is the
  // adapter's problem to report, never this layer's crash.
  #safeOff(off) {
    try {
      const result = off();
      if (result && typeof result.catch === 'function') {
        result.catch((error) => this.#log.error({ err: error, event: 'cluster.unsubscribe' }));
      }
    } catch (error) {
      this.#log.error({ err: error, event: 'cluster.unsubscribe' });
    }
  }

  // The snapshot this node REPLICATES: the local one, through the rooms
  // filter when the app configured one.
  #snapshot() {
    const snapshot = this.#local.snapshot();
    if (!this.#roomsFilter) return snapshot;
    const rooms = {};
    for (const room in snapshot.rooms) {
      if (this.#roomsFilter(room)) rooms[room] = snapshot.rooms[room];
    }
    return { rooms, clients: snapshot.clients };
  }

  // One periodic tick does both presence jobs: publish the corrective
  // signal (an at-most-once broker WILL drop a delta eventually) and sweep
  // for nodes that went silent. The corrective signal is a DIGEST — a hash
  // over the room table, not the table itself: a full snapshot every tick
  // cost O(nodes^2 x rooms) backplane bytes at steady state, all of it
  // usually confirming nothing changed. A receiver whose view hashes
  // differently asks THAT node for a full state with an addressed 'sync'
  // (see #receive), so the table travels only when it is actually wrong.
  #tick() {
    const { rooms, clients } = this.#snapshot();
    let hash = 0;
    let count = 0;
    for (const room in rooms) {
      hash = (hash + entryHash(room, rooms[room])) >>> 0;
      count++;
    }
    this.#post(CLUSTER_CHANNEL, { t: 'digest', clients, n: count, h: hash });
    const now = Date.now();
    const deadline = now - this.#presenceTimeout;
    for (const [instance, node] of this.#nodes) {
      if (node.lastSeen < deadline) this.#evict(instance, 'timeout');
    }
    // A sender's replay guard outlives its presence record on purpose — an
    // evicted node's envelopes are exactly what a replay would bring back —
    // and is dropped once the clock alone refuses everything it remembers.
    for (const [instance, guard] of this.#guards) {
      if (now - guard.newest > this.#maxSkew) this.#guards.delete(instance);
    }
    for (const [key, warned] of this.#refused) {
      if (warned < deadline) this.#refused.delete(key);
    }
  }

  // True when the message left synchronously intact; false on a serialize
  // failure or a synchronous publish throw. Fire-and-forget callers ignore
  // it; #request settles immediately on false instead of waiting out a
  // timeout for a question that never went anywhere. (An async publish
  // rejection still only logs — by then at-most-once already owns it.)
  //
  // `payload` is the part of the body an APPLICATION wrote — an event's
  // data, a question's, an answer — and so the only part that can hold
  // bytes. Presence and commands never do and are never walked; with bytes
  // in it the envelope leaves as a binary one (see #serialize).
  #post(channel, body, payload = undefined) {
    const envelope = { v: ENVELOPE_VERSION, from: this.#instance, epoch: this.#epoch, ...body };
    if (this.#secret !== null) {
      // What the signature is about to cover, set AFTER the body so nothing
      // in it can spell them: this process' counter, the channel the
      // envelope is published on, and the sender's clock. A 1.x node verifies
      // the HMAC over the whole re-serialized envelope, so the three are
      // transparent to it. (`ts` is the trace state below, hence `at`.)
      envelope.seq = ++this.#seq;
      envelope.ch = channel;
      envelope.at = Date.now();
    }
    // The active trace context rides the envelope as tp/ts (ignored by
    // receivers that predate it — the additive-fields rule): the cross-node
    // hop is where a trace is most valuable and used to be exactly where
    // context was dropped.
    this.#otel.inject(envelope);
    const binary = this.#bytes && payload !== undefined && hasBytes(payload);
    if (binary && typeof this.#envelope?.encodeFrame !== 'function') {
      // A Cluster wired by hand, without the core's envelope: JSON would
      // deliver the {"0":…} object it makes of bytes — a silently wrong
      // delivery. Refused as undeliverable, and said.
      this.#log.warn({ event: 'cluster.bytes', type: body.t, name: body.name ?? body.args?.name });
      return false;
    }
    let message = null;
    try {
      message = binary ? this.#frame(envelope) : this.#text(envelope);
    } catch (error) {
      this.#log.error({ err: error, event: 'cluster.serialize', type: body.t });
      return false;
    }
    // A sealer that cannot seal (a keyring without its current key) is
    // named as such, and nothing leaves: never plaintext across the wire.
    try {
      if (binary) message = this.#envelope.encodeFrame(message, channel);
      else if (this.#envelope !== null) message = this.#envelope.encode(message, channel);
    } catch (error) {
      this.#log.error({ err: error, event: 'cluster.seal', type: body.t });
      return false;
    }
    try {
      const result = this.#backplane.publish(channel, message);
      if (result && typeof result.catch === 'function') {
        result.catch((error) => this.#log.error({ err: error, event: 'cluster.publish', channel }));
      }
    } catch (error) {
      // A broken backplane must never break the caller's own request path.
      this.#log.error({ err: error, event: 'cluster.publish', channel });
      return false;
    }
    return true;
  }

  // The envelope as JSON text. Signed over the serialized body, sig appended
  // LAST: the receiver deletes `sig` from the parsed object and re-serializes
  // — key order survives a JSON round trip, so the bytes match.
  #text(envelope) {
    const message = JSON.stringify(envelope);
    if (this.#secret === null) return message;
    envelope.sig = crypto.createHmac('sha256', this.#secret).update(message).digest('hex');
    return JSON.stringify(envelope);
  }

  // The envelope as a binary attachments frame — what an envelope holding
  // bytes travels as, the frame a socket carries such a packet in. The
  // signature is the same construction over the FRAME: the bytes of the
  // envelope without `sig`, which the receiver gets back by deleting `sig`
  // from what it decoded and encoding again (the encoder walks keys in
  // order, as JSON does). A frame never equals a JSON text, so a signature
  // made for one form cannot be presented under the other.
  #frame(envelope) {
    const frame = encodeAttachments(envelope);
    if (this.#secret === null) return frame;
    envelope.sig = crypto.createHmac('sha256', this.#secret).update(frame).digest('hex');
    return encodeAttachments(envelope);
  }

  // -----------------------------------------------------------------------
  // Presence: replicated counters, local reads

  /** Fired by the registry on every successful local join/leave. */
  delta(room, d) {
    if (!this.#backplane || this.#closed) return;
    if (this.#roomsFilter && !this.#roomsFilter(room)) return;
    this.#post(CLUSTER_CHANNEL, { t: 'delta', room, d });
  }

  /** Cluster-wide membership of `room`: a local sum, no network. */
  count(room) {
    let total = this.#local.count(room);
    for (const node of this.#nodes.values()) total += node.rooms.get(room) ?? 0;
    return total;
  }

  /** Per-instance breakdown of `room`; zero-count instances are omitted. */
  presence(room) {
    const instances = {};
    let total = 0;
    const local = this.#local.count(room);
    if (local > 0) {
      instances[this.#instance] = local;
      total += local;
    }
    for (const [instance, node] of this.#nodes) {
      const count = node.rooms.get(room) ?? 0;
      if (count === 0) continue;
      instances[instance] = count;
      total += count;
    }
    return { total, instances };
  }

  /** Ids of the live instances, this one first. */
  instances() {
    return [this.#instance, ...this.#nodes.keys()];
  }

  // Envelope authentication, the receiving half: a message without a valid
  // signature is dropped and logged. Constant-time compare — the signature
  // is the credential here.
  #verify(envelope, from, binary) {
    const sig = envelope.sig;
    if (typeof sig !== 'string' || sig.length === 0) {
      this.#log.warn({ event: 'cluster.unsigned', from });
      this.#otel.recordClusterVerification('unsigned');
      return false;
    }
    delete envelope.sig;
    let expected = null;
    try {
      const signed = binary ? encodeAttachments(envelope) : JSON.stringify(envelope);
      expected = crypto.createHmac('sha256', this.#secret).update(signed).digest('hex');
    } catch (error) {
      // The third way verification fails, and the only one that was silent:
      // an envelope that will not serialize again (a cycle a replicated
      // payload picked up), or a secret the crypto layer refuses. Its
      // siblings above and below both log, so an operator watching
      // `cluster.*` saw two of three reasons a node went quiet.
      this.#log.error({ err: error, event: 'cluster.verify', from });
      this.#otel.recordClusterVerification('error');
      return false;
    }
    const a = Buffer.from(sig);
    const b = Buffer.from(expected);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
      this.#log.warn({ event: 'cluster.badsig', from });
      this.#otel.recordClusterVerification('badsig');
      return false;
    }
    return true;
  }

  // Replay protection, the receiving half. It runs AFTER #verify, so every
  // field read here was written by a node holding the secret; what the
  // signature cannot say is that the envelope is being heard where and when
  // it was published, and for the first time:
  //
  //   ch     the channel it was published on — a copy moved to another
  //          instance's inbox, or from an inbox to `cluster`, is refused;
  //   at     the sender's clock, within `maxSkew` of this one — the bound on
  //          what a node that was not listening (a fresh boot) can be fed;
  //   seq    the sender's counter, under a sliding window per life of that
  //          sender (a receiver hears two of its channels, so gaps are
  //          normal) — a repeat, or one older than the window, is refused;
  //   epoch  another life of the same name is followed only when it is
  //          NEWER than everything accepted from the one it replaces, so an
  //          envelope of a dead process cannot bring that process back.
  //
  // An envelope with no counter is what a 1.x node sends: refused, unless
  // `replay: 'accept'` says a rolling upgrade is under way.
  #admit(envelope, from, channel) {
    const { seq, ch, at, epoch } = envelope;
    if (seq === undefined) return this.#strict ? this.#replayed(from, channel, 'unsequenced') : true;
    if (!Number.isSafeInteger(seq) || seq < 0 || typeof at !== 'number') return this.#replayed(from, channel, 'seq');
    if (ch !== channel) return this.#replayed(from, channel, 'channel');
    if (Math.abs(Date.now() - at) > this.#maxSkew) return this.#replayed(from, channel, 'stale');
    let guard = this.#guards.get(from);
    if (guard === undefined || guard.epoch !== epoch) {
      if (guard !== undefined && at <= guard.newest) return this.#replayed(from, channel, 'stale');
      guard = { epoch, window: new ReplayWindow(DEFAULT_REPLAY_WINDOW), newest: at };
      this.#guards.set(from, guard);
    }
    if (!guard.window.accept(seq)) return this.#replayed(from, channel, 'seq');
    if (at > guard.newest) guard.newest = at;
    return true;
  }

  // Always counted; warned once per sender and reason a presence timeout,
  // debug in between — whoever replays one envelope can replay it in a loop.
  #replayed(from, channel, reason) {
    this.#otel.recordClusterVerification('replay');
    const key = `${from}\0${reason}`;
    const now = Date.now();
    const loud = now - (this.#refused.get(key) ?? 0) > this.#presenceTimeout;
    if (loud) this.#refused.set(key, now);
    this.#log[loud ? 'warn' : 'debug']({ event: 'cluster.replay', from, channel, reason });
    return false;
  }

  #seen(from, epoch) {
    let node = this.#nodes.get(from);
    if (node && node.epoch !== epoch) {
      // A restart: REPLACE the counters, never merge — the old process'
      // rooms are gone with it, only its name survived. And like any other
      // death proof, open requests stop waiting for the OLD process now:
      // it missed their question, and the reborn one never will answer it.
      this.#nodes.delete(from);
      node = null;
      this.#stopWaitingFor(from);
    }
    if (!node) {
      node = { epoch, lastSeen: 0, clients: 0, rooms: new Map() };
      this.#nodes.set(from, node);
      // Symmetric with eviction: membership changes are the cluster events
      // an operator reasons about, and joins used to be silent.
      this.#log.info({ event: 'cluster.join', instance: from });
      this.#otel.recordClusterInstances(1);
    }
    node.lastSeen = Date.now();
    return node;
  }

  #applySnapshot(node, envelope) {
    const { rooms, clients } = envelope;
    node.clients = typeof clients === 'number' ? clients : 0;
    node.rooms = new Map();
    if (rooms && typeof rooms === 'object') {
      // for...in, not Object.entries: this runs per presence snapshot per peer,
      // and entries allocated an array plus a pair object for every room.
      for (const room in rooms) {
        const count = rooms[room];
        if (typeof count === 'number' && count > 0) node.rooms.set(room, count);
      }
    }
  }

  #evict(instance, reason) {
    const node = this.#nodes.get(instance);
    if (!node) return;
    this.#nodes.delete(instance);
    // A timeout eviction is a node that went SILENT — likely partitioned or
    // dead without a goodbye — which deserves warn; a bye is routine.
    if (reason === 'timeout') this.#log.warn({ event: 'cluster.evict', instance, reason });
    else this.#log.debug({ event: 'cluster.evict', instance, reason });
    this.#otel.recordClusterInstances(-1);
    this.#stopWaitingFor(instance);
  }

  // A dead node's answers are never coming: every open request stops
  // waiting for it right now, not at its timeout. `incomplete` stays
  // honest: a node that promised answers (bask's count phase) but never
  // delivered them makes the settled result incomplete even though nobody
  // is left to wait for.
  #stopWaitingFor(instance) {
    for (const [requestId, request] of Array.from(this.#requests)) {
      if (!request.missing.delete(instance)) continue;
      if (request.missing.size > 0) continue;
      this.#requests.delete(requestId);
      clearTimeout(request.timer);
      request.settle(this.#hasUndelivered(request));
    }
  }

  #hasUndelivered(request) {
    for (const instance of request.counted) {
      if (!request.finals.has(instance)) return true;
    }
    return false;
  }

  // -----------------------------------------------------------------------
  // Node-to-node messaging (the serverSideEmit pair)

  // NOTE: as everywhere in wrpc, `emit` is the LOCAL Emitter emit — it is
  // what delivers a remote node's message to `cluster.on(name, fn)`
  // listeners here. The wire send has its own name, same as Client's.

  /** Fire-and-forget to every OTHER node's `cluster.on(name, ...)`. */
  sendEvent(name, data) {
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('Event name must be a non-empty string');
    }
    if (!this.#backplane || this.#closed) return;
    this.#post(CLUSTER_CHANNEL, { t: 'e', name, data }, data);
  }

  /**
   * Asks every other node and collects their answers. Each node answers
   * through its registered responder — `cluster.respond(name, fn)` — or
   * contributes an error when it has none. Resolves as soon as every live
   * node answered; the timeout resolves `incomplete: true`.
   */
  ask(name, data, options = {}) {
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('Event name must be a non-empty string');
    }
    return this.#request('emit', { name, data }, options.timeout ?? this.#requestTimeout).then(
      ({ payloads, incomplete }) => {
        const answers = [];
        const errors = [];
        for (const payload of payloads) {
          if (payload && typeof payload === 'object' && 'error' in payload) errors.push(payload.error);
          else answers.push(payload?.value);
        }
        return { answers, errors, incomplete };
      },
    );
  }

  /**
   * Registers this node's answer to `cluster.ask(name)` from other nodes.
   * One responder per name: two answers to one question are ambiguous.
   */
  respond(name, handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('Cluster.respond: handler must be a function');
    }
    if (this.#responders.has(name)) {
      throw new Error(`Duplicate responder for '${name}'`);
    }
    this.#responders.set(name, handler);
  }

  unrespond(name) {
    return this.#responders.delete(name);
  }

  // -----------------------------------------------------------------------
  // Introspection and commands

  /**
   * Descriptors of matching clients across the cluster. `sel` narrows by
   * room (`{ room }`) or matches everyone (`{}`). Resolves with an array;
   * when a node never answered inside `timeout`, the partial array carries
   * a non-enumerable `incomplete: true` and the miss is logged.
   */
  fetchClients(sel = {}, options = {}) {
    const local = this.#local.descriptors(sel);
    return this.#request('fetch', { sel }, options.timeout ?? this.#requestTimeout).then(({ payloads, incomplete }) => {
      const clients = local;
      let truncated = false;
      for (const payload of payloads) {
        // push(...payload) is Function.prototype.apply in disguise and throws
        // RangeError once a fan-in exceeds the engine's argument limit — a hard
        // failure on exactly the large deployments this path exists to serve.
        const list = Array.isArray(payload) ? payload : Array.isArray(payload?.list) ? payload.list : null;
        if (payload?.truncated === true) truncated = true;
        if (list) for (let i = 0; i < list.length; i++) clients.push(list[i]);
      }
      if (incomplete) {
        this.#log.warn({ event: 'cluster.fetch.incomplete', received: clients.length });
        Object.defineProperty(clients, 'incomplete', { value: true, enumerable: false, configurable: true });
      }
      // A node over its maxFetch cap answered with its first `cap` entries
      // and said so — surfaced the same non-enumerable way as `incomplete`.
      if (truncated) {
        this.#log.warn({ event: 'cluster.fetch.truncated', received: clients.length });
        Object.defineProperty(clients, 'truncated', { value: true, enumerable: false, configurable: true });
      }
      return clients;
    });
  }

  /**
   * `target` is a client id (addressed: one instance hears it) or a
   * selector object (`{ room }` / `{}`: applied everywhere). Commands are
   * fire-and-forget with the backplane's at-most-once delivery.
   */
  join(target, ...rooms) {
    this.#command('join', target, { rooms });
  }

  leave(target, ...rooms) {
    this.#command('leave', target, { rooms });
  }

  disconnect(target) {
    this.#command('disconnect', target, {});
  }

  /**
   * One event to ONE client, wherever it is connected: the id names the
   * instance, so this is an addressed command — one publish, one receiver
   * — never a broadcast-and-filter. `room` narrows delivery to a client
   * still in that room (a relay that must not outlive a membership).
   * Fire-and-forget with the backplane's at-most-once delivery; a local
   * id is applied directly.
   */
  send(clientId, name, data, options = {}) {
    if (typeof clientId !== 'string' || clientId.length === 0) {
      throw new TypeError('Cluster.send: clientId must be a non-empty string');
    }
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('Event name must be a non-empty string');
    }
    const room = typeof options.room === 'string' ? options.room : undefined;
    return this.#command('event', clientId, { name, data }, room);
  }

  // The command ops, in one place. Both call sites below used to spell this
  // chain out themselves and disagreed on the unknown-op case — one fell
  // through to disconnect, the other ignored it — so the default is now the
  // caller's to state: this returns false rather than picking one.
  // `args` is the envelope's op-specific part: { rooms } for join/leave,
  // { name, data } for event — the same keys on the wire, so a node that
  // predates an op ignores it and one that knows it reads it (additive).
  #applyOp(op, sel, args) {
    if (op === 'join') this.#local.join(sel, args.rooms);
    else if (op === 'leave') this.#local.leave(sel, args.rooms);
    else if (op === 'disconnect') this.#local.disconnect(sel);
    else if (op === 'event') this.#local.event(sel, args.name, args.data);
    else return false;
    return true;
  }

  // `room`, when given, rides inside the selector: a client that left the
  // room between send and delivery is not selected — a relay bounded by a
  // membership must not outlive it.
  #command(op, target, args, room = undefined) {
    const apply = (sel) => {
      // `op` here is a literal from join()/leave()/disconnect()/send(), never
      // user input, so an unknown one is a bug in this file and should be loud.
      if (!this.#applyOp(op, sel, args)) throw new Error(`Unknown cluster command op '${op}'`);
    };
    // An addressed command rides the target instance's own channel — one
    // publish, one receiver — instead of asking every node to filter.
    // Answers whether the command was applied here or handed to the
    // backplane: false is "known not to be delivered".
    if (typeof target === 'string') {
      const sel = room === undefined ? { id: target } : { id: target, room };
      const instance = instanceOfClientId(target);
      if (instance === this.#instance || instance === null) {
        apply(sel);
        return true;
      }
      if (!this.#backplane || this.#closed) return false;
      // Only an event carries what an application wrote (`data`).
      return this.#post(instanceChannel(instance), { t: 'cmd', op, sel, ...args }, args.data);
    }
    const sel = target && typeof target === 'object' ? target : {};
    apply(sel);
    if (!this.#backplane || this.#closed) return true;
    this.#post(CLUSTER_CHANNEL, { t: 'cmd', op, sel, ...args });
    return true;
  }

  /**
   * The remote leg of Broadcast.ask(): every node asks its own members and
   * answers twice — first how many it asked (so the caller knows how many
   * client answers exist at all), then the answers themselves.
   */
  broadcastAsk(rooms, name, data, timeout) {
    // The backstop covers the slowest possible answer: the remote node's own
    // per-client timeout, plus the request round-trip margin.
    const budget = timeout + this.#requestTimeout;
    return this.#request('bask', { rooms, name, data, timeout }, budget).then(({ payloads, meta, incomplete }) => {
      const answers = [];
      const errors = [];
      for (const payload of payloads) {
        if (!payload || typeof payload !== 'object') continue;
        // Appended by index, not spread: see the note on fetchClients above.
        const { answers: mine, errors: theirs } = payload;
        if (Array.isArray(mine)) for (let i = 0; i < mine.length; i++) answers.push(mine[i]);
        if (Array.isArray(theirs)) for (let i = 0; i < theirs.length; i++) errors.push(theirs[i]);
      }
      return { answers, errors, expected: meta.expected, incomplete };
    });
  }

  // -----------------------------------------------------------------------
  // The request/reply engine

  #request(op, args, timeout) {
    const missing = new Set(this.#nodes.keys());
    const result = { payloads: [], meta: { expected: 0 }, incomplete: false };
    if (!this.#backplane || this.#closed || missing.size === 0) return Promise.resolve(result);
    const requestId = this.#generateId();
    return new Promise((resolve) => {
      const request = {
        missing,
        // Who promised more (bask's count phase) vs who delivered a final:
        // what keeps `incomplete` honest when eviction empties the missing
        // set with promised answers still undelivered.
        counted: new Set(),
        finals: new Set(),
        result,
        timer: null,
        settle: (incomplete) => {
          result.incomplete = incomplete;
          this.#otel.recordClusterRequest(op, !incomplete);
          resolve(result);
        },
      };
      request.timer = setTimeout(() => {
        this.#requests.delete(requestId);
        request.settle(true);
      }, timeout);
      if (typeof request.timer.unref === 'function') request.timer.unref();
      this.#requests.set(requestId, request);
      // A question that never left — unserializable args, a synchronously
      // broken backplane — settles NOW: nobody will ever answer it, and
      // waiting out the full timeout would just park the caller.
      if (!this.#post(CLUSTER_CHANNEL, { t: 'q', q: requestId, op, args }, args.data)) {
        this.#requests.delete(requestId);
        clearTimeout(request.timer);
        request.settle(true);
      }
    });
  }

  #answer(envelope) {
    const request = this.#requests.get(envelope.a);
    if (!request) return; // late answer after timeout/eviction: already settled
    if (envelope.fin === false) {
      // An intermediate reply carries accounting, not an answer — the count
      // phase of a broadcast ask.
      const count = envelope.payload?.count;
      if (typeof count === 'number') {
        request.result.meta.expected += count;
        request.counted.add(envelope.from);
      }
      return;
    }
    request.result.payloads.push(envelope.payload);
    request.finals.add(envelope.from);
    if (!request.missing.delete(envelope.from) || request.missing.size > 0) return;
    this.#requests.delete(envelope.a);
    clearTimeout(request.timer);
    request.settle(this.#hasUndelivered(request));
  }

  // Answers a request from another node. `reply(payload, fin)` publishes to
  // the requester's own channel; the op's resolved value is the final reply.
  #serve(envelope) {
    const { q: requestId, op, args = {}, from } = envelope;
    const reply = (payload, fin) => {
      // A node that already said goodbye must not speak again: the bye told
      // the requester to stop waiting, and a post-bye answer would arrive
      // from an instance the receiver just evicted.
      if (this.#closed) return;
      // A fetch reply is descriptors — this node's own JSON, a thousand of
      // them — and is not walked; an answer is the application's.
      this.#post(instanceChannel(from), { t: 'a', a: requestId, fin, payload }, op === 'fetch' ? undefined : payload);
    };
    const run = () => {
      if (op === 'fetch') {
        const list = this.#local.descriptors(args.sel ?? {});
        // A truncated reply says so — silent truncation is the one thing
        // this layer never does. The cap exists because the reply is ONE
        // pub/sub message; see DEFAULT_MAX_FETCH.
        if (this.#maxFetch > 0 && list.length > this.#maxFetch) {
          this.#log.warn({ event: 'cluster.fetch.truncated', count: list.length, cap: this.#maxFetch });
          list.length = this.#maxFetch;
          return { list, truncated: true };
        }
        return { list };
      }
      if (op === 'emit') return this.#respondTo(args, from);
      if (op === 'bask') {
        const { rooms, name, data, timeout } = args;
        return this.#local.ask(rooms ?? null, name, data, timeout, (count) => reply({ count }, false));
      }
      throw new Error(`Unknown cluster op '${op}'`);
    };
    // A CONSUMER span parented on the envelope's tp/ts (see #post): the
    // requester's client span in one process becomes the parent of the
    // serving span in another — the same linkage a call packet gets. The
    // client stub carries the two attributes buildCallAttributes reads.
    const stub = { transportKind: 'backplane', persistent: true };
    this.#otel.withSpan({ client: stub, packet: envelope, target: `cluster/${op}`, kind: SPAN_KIND_CONSUMER }, (h) =>
      Promise.resolve()
        .then(run)
        .then(
          (payload) => {
            this.#otel.endSpan(h, { 'wrpc.status': 'ok' });
            reply(payload, true);
          },
          (error) => {
            this.#otel.recordError(h, error);
            this.#otel.endSpan(h, { 'wrpc.status': 'error' });
            this.#log.error({ err: error, event: 'cluster.serve', op });
            reply({ error: error?.message ?? 'Cluster op failed' }, true);
          },
        ),
    );
  }

  async #respondTo(args, from) {
    const responder = this.#responders.get(args.name);
    if (!responder) return { error: `No responder for '${args.name}'` };
    try {
      return { value: await responder(args.data, from) };
    } catch (error) {
      return { error: error?.message ?? 'Responder failed' };
    }
  }

  #receive(message, channel) {
    if (this.#closed) return;
    let text = message;
    if (typeof message === 'string') {
      if (this.#envelope !== null) text = this.#envelope.decode(message, channel);
      else if (message.charCodeAt(0) === 119 && message.startsWith('wrpc-enc:')) text = null;
      if (text === null) return void this.#log.warn({ event: 'cluster.encoded' });
      // Refused by a sealing envelope, which reported why — or our own echo.
      if (text === undefined) return;
      // Still sealed: this node holds no keys, and says so.
      if (typeof text === 'string' && text.charCodeAt(0) === 119 && text.startsWith('wrpc-sealed:')) {
        return void this.#log.warn({ event: 'cluster.sealed', channel });
      }
    }
    // A binary envelope (bytes in an event's data, a question or an answer)
    // was decoded to its object by the envelope seam already.
    const binary = typeof message === 'string' && typeof text !== 'string';
    const envelope = typeof text === 'string' ? jsonParse(text) : text;
    if (!envelope || typeof envelope !== 'object') return;
    const { from, epoch, t } = envelope;
    if (from === this.#instance || typeof from !== 'string' || from.length === 0) return;
    if (this.#secret && !(this.#verify(envelope, from, binary) && this.#admit(envelope, from, channel))) return;
    this.#otel.recordClusterMessage(typeof t === 'string' ? t : '<unknown>');
    if (t === 'bye') {
      // Only the life we actually track may say goodbye: a bye from a
      // PREVIOUS epoch, arriving late while the restarted process is
      // already up, must not evict the fresh entry.
      const node = this.#nodes.get(from);
      if (node && node.epoch === epoch) this.#evict(from, 'bye');
      return;
    }
    if (t === 'a') {
      // Settled BEFORE the liveness/epoch bookkeeping: were #seen to run
      // first, a reborn node answering the very question in this envelope
      // would trigger the epoch sweep and settle the request without the
      // answer we are holding in our hands.
      this.#answer(envelope);
      this.#seen(from, epoch);
      return;
    }
    // EVERY message is a liveness proof — the snapshot only fills silence.
    const node = this.#seen(from, epoch);
    switch (t) {
      case 'hello':
        this.#applySnapshot(node, envelope);
        // Addressed, not broadcast: N answers reach one newcomer instead of
        // N answers reaching N nodes.
        this.#post(instanceChannel(from), { t: 'state', ...this.#snapshot() });
        return;
      case 'state':
        node.syncing = 0;
        return void this.#applySnapshot(node, envelope);
      case 'digest': {
        const { clients, n, h } = envelope;
        if (typeof clients === 'number') node.clients = clients;
        if (typeof n !== 'number' || typeof h !== 'number') return;
        let hash = 0;
        let count = 0;
        for (const [room, roomCount] of node.rooms) {
          hash = (hash + entryHash(room, roomCount)) >>> 0;
          count++;
        }
        if (count === n && hash === h) return; // the view is correct
        // One outstanding sync per node: the answering 'state' clears it,
        // so a slow answer cannot stack requests every tick. The wait is
        // BOUNDED: the answer travels at-most-once too (our own inbox
        // channel may not even be subscribed yet on the first digest), and
        // a lost `state` used to leave `syncing` set forever — every later
        // digest ignored, the view wrong until the node restarted. After
        // two presence intervals without an answer the sync is asked again.
        const now = Date.now();
        if (node.syncing && now - node.syncing < this.#presenceInterval * 2) return;
        node.syncing = now;
        this.#post(instanceChannel(from), { t: 'sync' });
        return;
      }
      case 'sync':
        // Addressed: one node's view of us drifted; hand it the table.
        return void this.#post(instanceChannel(from), { t: 'state', ...this.#snapshot() });
      case 'delta': {
        const { room, d } = envelope;
        if (typeof room !== 'string' || typeof d !== 'number') return;
        const next = (node.rooms.get(room) ?? 0) + d;
        if (next > 0) node.rooms.set(room, next);
        else node.rooms.delete(room);
        return;
      }
      case 'e': {
        const { name, data } = envelope;
        if (typeof name !== 'string' || name.length === 0) return;
        return void Promise.resolve(this.emit(name, data)).catch((error) => {
          this.#log.error({ err: error, event: 'cluster.listener', name });
        });
      }
      case 'q':
        return void this.#serve(envelope);
      case 'cmd': {
        const { op, sel, rooms, name, data } = envelope;
        // Wire-shape sanity on the most powerful envelope type: op a
        // string, sel a plain object, rooms (when present) an array of
        // strings, an event's name a non-empty string — a malformed
        // command is dropped, never partially run.
        if (typeof op !== 'string') return;
        if (sel !== undefined && (typeof sel !== 'object' || sel === null || Array.isArray(sel))) return;
        if (rooms !== undefined && (!Array.isArray(rooms) || rooms.some((room) => typeof room !== 'string'))) {
          return;
        }
        if (op === 'event' && (typeof name !== 'string' || name.length === 0)) return;
        try {
          // `op` arrived from a peer: an unrecognised one is ignored, the same
          // way the switch's own default ignores an unrecognised envelope type.
          this.#applyOp(op, sel ?? {}, { rooms, name, data });
        } catch (error) {
          this.#log.error({ err: error, event: 'cluster.command', op });
        }
        break;
      }
      default:
        break;
    }
  }
}

module.exports = { Cluster, instanceOfClientId, CLUSTER_CHANNEL, INSTANCE_CHANNEL_PREFIX };
