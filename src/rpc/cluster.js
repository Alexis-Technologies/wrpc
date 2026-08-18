'use strict';

const { Emitter, jsonParse } = require('../utils.js');
const { generateUUID } = require('../runtime/node.js');
const { createLoggerWriter } = require('../logging.js');

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
  #instance;
  #epoch;
  #log;
  #local;
  #presenceInterval;
  #presenceTimeout;
  #requestTimeout;
  #generateId;
  // instance -> { epoch, lastSeen, clients, rooms: Map<room, count> }
  #nodes = new Map();
  // requestId -> { missing: Set<instance>, answers, errors, expected, timer, resolve }
  #requests = new Map();
  #responders = new Map();
  #unsubscribes = [];
  #timer = null;
  #closed = false;

  /**
   * `local` is the seam to the owning RpcServer: how remote requests and
   * commands reach this node's clients without Cluster knowing the server.
   *   count(room), snapshot() -> { clients, rooms }
   *   descriptors(sel) -> Array
   *   join(sel, rooms) / leave(sel, rooms) / disconnect(sel)
   *   ask(rooms, name, data, timeout, onCount) -> Promise<{answers, errors}>
   */
  constructor({ backplane = null, instance, local, log = globalThis.console, generateId = null, options = {} }) {
    super();
    this.#backplane = backplane;
    this.#instance = instance;
    // The boot marker. instanceId may be STABLE across restarts ('node-1');
    // the epoch never is, which is how a receiver tells "restarted, replace
    // its counters" from "same process, merge".
    this.#epoch = generateUUID();
    this.#local = local;
    this.#log = createLoggerWriter(log);
    this.#generateId = typeof generateId === 'function' ? generateId : generateUUID;
    const interval = options.presenceInterval > 0 ? options.presenceInterval : DEFAULT_PRESENCE_INTERVAL;
    this.#presenceInterval = interval;
    this.#presenceTimeout = options.presenceTimeout > 0 ? options.presenceTimeout : interval * 3;
    this.#requestTimeout = options.requestTimeout > 0 ? options.requestTimeout : DEFAULT_REQUEST_TIMEOUT;
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
      this.#post(CLUSTER_CHANNEL, { t: 'hello', ...this.#local.snapshot() });
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
  }

  #subscribeTo(channel) {
    const handler = (message) => this.#receive(message);
    return Promise.resolve()
      .then(() => this.#backplane.subscribe(channel, handler))
      .then(
        (off) => {
          if (typeof off !== 'function') return;
          if (this.#closed) return void this.#safeOff(off);
          this.#unsubscribes.push(off);
        },
        (error) => this.#log.error({ err: error, event: 'cluster.subscribe', channel }),
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

  // One periodic tick does both presence jobs: publish the corrective
  // snapshot (an at-most-once broker WILL drop a delta eventually; the next
  // snapshot heals it) and sweep for nodes that went silent.
  #tick() {
    this.#post(CLUSTER_CHANNEL, { t: 'state', ...this.#local.snapshot() });
    const deadline = Date.now() - this.#presenceTimeout;
    for (const [instance, node] of this.#nodes) {
      if (node.lastSeen < deadline) this.#evict(instance, 'timeout');
    }
  }

  // True when the message left synchronously intact; false on a serialize
  // failure or a synchronous publish throw. Fire-and-forget callers ignore
  // it; #request settles immediately on false instead of waiting out a
  // timeout for a question that never went anywhere. (An async publish
  // rejection still only logs — by then at-most-once already owns it.)
  #post(channel, body) {
    const envelope = { v: ENVELOPE_VERSION, from: this.#instance, epoch: this.#epoch, ...body };
    let message = null;
    try {
      message = JSON.stringify(envelope);
    } catch (error) {
      this.#log.error({ err: error, event: 'cluster.serialize', type: body.t });
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

  // -----------------------------------------------------------------------
  // Presence: replicated counters, local reads

  /** Fired by the registry on every successful local join/leave. */
  delta(room, d) {
    if (!this.#backplane || this.#closed) return;
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
    this.#log.debug({ event: 'cluster.evict', instance, reason });
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
    this.#post(CLUSTER_CHANNEL, { t: 'e', name, data });
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
      for (const payload of payloads) {
        // push(...payload) is Function.prototype.apply in disguise and throws
        // RangeError once a fan-in exceeds the engine's argument limit — a hard
        // failure on exactly the large deployments this path exists to serve.
        if (Array.isArray(payload)) for (let i = 0; i < payload.length; i++) clients.push(payload[i]);
      }
      if (incomplete) {
        this.#log.warn({ event: 'cluster.fetch.incomplete', received: clients.length });
        Object.defineProperty(clients, 'incomplete', { value: true, enumerable: false, configurable: true });
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
    this.#command('join', target, rooms);
  }

  leave(target, ...rooms) {
    this.#command('leave', target, rooms);
  }

  disconnect(target) {
    this.#command('disconnect', target, undefined);
  }

  // The three room ops, in one place. Both call sites below used to spell this
  // chain out themselves and disagreed on the unknown-op case — one fell
  // through to disconnect, the other ignored it — so the default is now the
  // caller's to state: this returns false rather than picking one.
  #applyOp(op, sel, rooms) {
    if (op === 'join') this.#local.join(sel, rooms);
    else if (op === 'leave') this.#local.leave(sel, rooms);
    else if (op === 'disconnect') this.#local.disconnect(sel);
    else return false;
    return true;
  }

  #command(op, target, rooms) {
    const apply = (sel) => {
      // `op` here is a literal from join()/leave()/disconnect(), never user
      // input, so an unknown one is a bug in this file and should be loud.
      if (!this.#applyOp(op, sel, rooms)) throw new Error(`Unknown cluster command op '${op}'`);
    };
    // An addressed command rides the target instance's own channel — one
    // publish, one receiver — instead of asking every node to filter.
    if (typeof target === 'string') {
      const sel = { id: target };
      const instance = instanceOfClientId(target);
      if (instance === this.#instance || instance === null) return void apply(sel);
      if (!this.#backplane || this.#closed) return;
      return void this.#post(instanceChannel(instance), { t: 'cmd', op, sel, rooms });
    }
    const sel = target && typeof target === 'object' ? target : {};
    apply(sel);
    if (!this.#backplane || this.#closed) return;
    this.#post(CLUSTER_CHANNEL, { t: 'cmd', op, sel, rooms });
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
      if (!this.#post(CLUSTER_CHANNEL, { t: 'q', q: requestId, op, args })) {
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
      this.#post(instanceChannel(from), { t: 'a', a: requestId, fin, payload });
    };
    const run = () => {
      if (op === 'fetch') return this.#local.descriptors(args.sel ?? {});
      if (op === 'emit') return this.#respondTo(args, from);
      if (op === 'bask') {
        const { rooms, name, data, timeout } = args;
        return this.#local.ask(rooms ?? null, name, data, timeout, (count) => reply({ count }, false));
      }
      throw new Error(`Unknown cluster op '${op}'`);
    };
    Promise.resolve()
      .then(run)
      .then(
        (payload) => reply(payload, true),
        (error) => {
          this.#log.error({ err: error, event: 'cluster.serve', op });
          reply({ error: error?.message ?? 'Cluster op failed' }, true);
        },
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

  #receive(message) {
    if (this.#closed) return;
    const envelope = typeof message === 'string' ? jsonParse(message) : message;
    if (!envelope || typeof envelope !== 'object') return;
    const { from, epoch, t } = envelope;
    if (from === this.#instance || typeof from !== 'string' || from.length === 0) return;
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
        this.#post(instanceChannel(from), { t: 'state', ...this.#local.snapshot() });
        return;
      case 'state':
        return void this.#applySnapshot(node, envelope);
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
        const { op, sel, rooms } = envelope;
        try {
          // `op` arrived from a peer: an unrecognised one is ignored, the same
          // way the switch's own default ignores an unrecognised envelope type.
          this.#applyOp(op, sel ?? {}, rooms);
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
