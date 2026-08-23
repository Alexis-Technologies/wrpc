'use strict';

const { jsonParse } = require('../utils.js');
const { createLoggerWriter } = require('../logging.js');

// Rooms: named groups of clients, layered on top of the existing
// `{ type: 'event' }` packets — no new wire type is needed, a room broadcast
// is just the same event packet sent to several connections.
//
// The registry is the single source of truth for both directions (which
// clients a room holds, which rooms a client joined) so a disconnect only has
// to call leaveAll(client): Client itself keeps no room state that could drift.
//
// Cross-instance fan-out is optional and lives behind the backplane contract
// (see src/scaling/): RoomsBackplane publishes every non-local emit and
// replays what other instances published. Delivery is at-most-once — see
// docs/reference/protocol.md.

const BROADCAST_CHANNEL = 'broadcast';
const ROOM_CHANNEL_PREFIX = 'room:';
const ENVELOPE_VERSION = 1;

// How long a broadcast question waits for each client's answer. Matches the
// client's default call timeout: an ask is a call in the other direction.
const DEFAULT_ASK_TIMEOUT = 7_000;
const EMPTY_ASK = { answers: [], errors: [], expected: 0, incomplete: false };

const roomChannel = (room) => ROOM_CHANNEL_PREFIX + room;

const EMPTY = new Set();

const checkRoom = (room) => {
  if (typeof room !== 'string' || room.length === 0) {
    throw new TypeError('Room name must be a non-empty string');
  }
  return room;
};

class RoomRegistry {
  #rooms = new Map(); // room -> Set<Client>
  #joined = new Map(); // Client -> Set<room>
  #onSubscribe;
  #onUnsubscribe;
  #onJoin;
  #onLeave;

  // onSubscribe/onUnsubscribe fire when a room gains its first member and
  // loses its last one: that is exactly when a backplane has to (un)subscribe
  // the room's channel. onJoin/onLeave fire on EVERY membership change: the
  // cluster's presence deltas. Plain callbacks rather than events —
  // join/leave is a hot path and Emitter.emit allocates a promise per call.
  constructor({ onSubscribe = null, onUnsubscribe = null, onJoin = null, onLeave = null } = {}) {
    this.#onSubscribe = onSubscribe;
    this.#onUnsubscribe = onUnsubscribe;
    this.#onJoin = onJoin;
    this.#onLeave = onLeave;
  }

  get size() {
    return this.#rooms.size;
  }

  list() {
    return Array.from(this.#rooms.keys());
  }

  members(room) {
    return this.#rooms.get(room) ?? EMPTY;
  }

  count(room) {
    return this.#rooms.get(room)?.size ?? 0;
  }

  has(room) {
    return this.#rooms.has(room);
  }

  roomsOf(client) {
    return this.#joined.get(client) ?? EMPTY;
  }

  join(client, room) {
    checkRoom(room);
    let members = this.#rooms.get(room);
    const created = !members;
    if (created) {
      members = new Set();
      this.#rooms.set(room, members);
    }
    if (members.has(client)) return false;
    members.add(client);
    let rooms = this.#joined.get(client);
    if (!rooms) {
      rooms = new Set();
      this.#joined.set(client, rooms);
    }
    rooms.add(room);
    if (created && this.#onSubscribe) this.#onSubscribe(room);
    if (this.#onJoin) this.#onJoin(room, client);
    return true;
  }

  leave(client, room) {
    const members = this.#rooms.get(room);
    if (!members || !members.delete(client)) return false;
    const rooms = this.#joined.get(client);
    if (rooms) {
      rooms.delete(room);
      if (rooms.size === 0) this.#joined.delete(client);
    }
    if (this.#onLeave) this.#onLeave(room, client);
    if (members.size > 0) return true;
    this.#rooms.delete(room);
    if (this.#onUnsubscribe) this.#onUnsubscribe(room);
    return true;
  }

  leaveAll(client) {
    const rooms = this.#joined.get(client);
    if (!rooms) return;
    // Copy: leave() mutates the very set being iterated.
    for (const room of Array.from(rooms)) this.leave(client, room);
    this.#joined.delete(client);
  }

  clear() {
    const rooms = this.list();
    this.#rooms.clear();
    this.#joined.clear();
    if (!this.#onUnsubscribe) return;
    for (const room of rooms) this.#onUnsubscribe(room);
  }
}

// An immutable, chainable delivery target. Every modifier returns a NEW
// Broadcast, so `const room = server.to('chat')` can be kept and reused
// without later `.except()` calls leaking into it.
class Broadcast {
  #registry;
  #clients;
  #publish;
  #cluster;
  #targets;
  #excluded;
  #localOnly;
  #log;
  #otel;
  #codec;

  constructor({
    registry,
    clients,
    publish = null,
    cluster = null,
    log = globalThis.console,
    otel = null,
    codec = null,
    targets = null,
    excluded = null,
    localOnly = false,
  }) {
    this.#registry = registry;
    this.#clients = clients;
    this.#publish = publish;
    this.#cluster = cluster;
    this.#log = createLoggerWriter(log);
    this.#otel = otel;
    this.#codec = codec;
    this.#targets = targets;
    this.#excluded = excluded;
    this.#localOnly = localOnly;
  }

  #derive(changes) {
    return new Broadcast({
      registry: this.#registry,
      clients: this.#clients,
      publish: this.#publish,
      cluster: this.#cluster,
      log: this.#log,
      otel: this.#otel,
      codec: this.#codec,
      targets: this.#targets,
      excluded: this.#excluded,
      localOnly: this.#localOnly,
      ...changes,
    });
  }

  // Narrowing to rooms is a union, not an intersection: to('a').to('b')
  // reaches everyone in either room, each client exactly once.
  //
  // to() with no rooms narrows to NOBODY, not to everybody. `to(...list)`
  // with an empty list is the dangerous case — the reading where it falls
  // back to every connected client is the `WHERE id IN ()` mistake.
  to(...rooms) {
    for (const room of rooms) checkRoom(room);
    const targets = this.#targets ? [...this.#targets, ...rooms] : [...rooms];
    return this.#derive({ targets });
  }

  except(...clients) {
    if (clients.length === 0) return this;
    const excluded = new Set(this.#excluded);
    for (const client of clients) excluded.add(client);
    return this.#derive({ excluded });
  }

  // Suppresses the backplane publish: the event stays on this instance.
  // Also how replayed remote events are delivered, so they are not echoed
  // back onto the backplane.
  local() {
    return this.#derive({ localOnly: true });
  }

  get rooms() {
    return this.#targets ? [...this.#targets] : null;
  }

  #recipients() {
    if (!this.#targets) return this.#clients();
    if (this.#targets.length === 1) return this.#registry.members(this.#targets[0]);
    const recipients = new Set();
    for (const room of this.#targets) {
      for (const client of this.#registry.members(room)) recipients.add(client);
    }
    return recipients;
  }

  /**
   * Sends `{ type: 'event', name, data }` to every matching client and
   * returns how many received it LOCALLY — remote instances are reached
   * through the backplane, whose delivery this number says nothing about.
   */
  emit(name, data) {
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('Event name must be a non-empty string');
    }
    // Narrowed to no rooms at all: nobody here, nobody anywhere — so there
    // is nothing to publish either.
    if (this.#targets && this.#targets.length === 0) return 0;
    // Serialized ONCE for the whole fan-out: a room of N used to pay
    // JSON.stringify (and utf8 encoding downstream) N times for one emit.
    // A payload that cannot serialize (circular) is reported, not thrown:
    // the broadcaster is usually far from whoever built the value.
    let text;
    try {
      // A configured codec is server-wide, so this stays a SINGLE encode
      // for the whole fan-out — same property the JSON path has.
      const packet = { type: 'event', name, data };
      text = this.#codec ? this.#codec.encode(packet) : JSON.stringify(packet);
    } catch (error) {
      this.#log.error({ err: error, event: 'broadcast.serialize', name });
      return 0;
    }
    let sent = 0;
    for (const client of this.#recipients()) {
      if (this.#excluded?.has(client)) continue;
      // HTTP clients cannot carry events; skipping beats throwing mid-fan-out.
      if (!client.persistent) continue;
      try {
        const flushed = client.sendRaw(text);
        sent++;
        // Not silently discarded any more: a recipient above its high-water
        // mark is visible in the metrics, and the engine's maxBackpressure
        // cap is what disconnects one that never drains.
        if (flushed === false) this.#otel?.recordBackpressure(client.transportKind);
      } catch (error) {
        // One dead socket must not truncate the fan-out.
        this.#log.error({ err: error, event: 'broadcast.send', name });
      }
    }
    const published = Boolean(!this.#localOnly && this.#publish);
    if (published) {
      this.#publish({ rooms: this.#targets, name, data });
    }
    this.#otel?.recordBroadcast(name, sent, published);
    return sent;
  }

  /**
   * Emits `name` to every matching client AND waits for each one's answer
   * (the peer registers one with `client.respond(name, fn)`). Resolves
   * `{ answers, errors, expected, incomplete }` — never rejects: a broadcast
   * question has many answerers, so per-client failures are data, not an
   * exception that discards the answers that DID arrive.
   *
   * With a cluster attached the question reaches every instance's members
   * too; `local()` keeps it on this one.
   */
  ask(name, data, options = {}) {
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('Event name must be a non-empty string');
    }
    // Narrowed to no rooms at all: nobody here, nobody anywhere — the same
    // short-circuit emit() has, extended to the cluster leg. Without it the
    // "WHERE id IN ()" mistake comes back through the wire: an empty rooms
    // array used to reach every client of every OTHER instance.
    if (this.#targets && this.#targets.length === 0) {
      if (options.onCount) options.onCount(0);
      return Promise.resolve({ answers: [], errors: [], expected: 0, incomplete: false });
    }
    const timeout = options.timeout > 0 ? options.timeout : DEFAULT_ASK_TIMEOUT;
    const local = this.#askLocal(name, data, timeout);
    if (options.onCount) options.onCount(local.expected);
    const remote =
      !this.#localOnly && this.#cluster
        ? this.#cluster.broadcastAsk(this.#targets, name, data, timeout)
        : Promise.resolve(EMPTY_ASK);
    return Promise.all([local.done, remote]).then(([mine, theirs]) => ({
      answers: [...mine.answers, ...theirs.answers],
      errors: [...mine.errors, ...theirs.errors],
      expected: local.expected + theirs.expected,
      incomplete: Boolean(theirs.incomplete),
    }));
  }

  // The local leg. The payload is serialized ONCE for the whole fan-out —
  // an ack needs a per-recipient id, but that is a suffix concatenation on
  // the shared prefix, not a second JSON.stringify of the data.
  #askLocal(name, data, timeout) {
    let prefix;
    try {
      // The suffix surgery below splices a per-recipient id into JSON text;
      // under a codec the whole packet is encoded per recipient instead —
      // a deliberate slow path, since only the codec knows its framing.
      prefix = this.#codec ? null : JSON.stringify({ type: 'event', name, data }).slice(0, -1);
      if (this.#codec) this.#codec.encode({ type: 'event', name, data }); // surface a circular payload NOW
    } catch (error) {
      this.#log.error({ err: error, event: 'broadcast.serialize', name });
      return { expected: 0, done: Promise.resolve({ answers: [], errors: [] }) };
    }
    const pending = [];
    if (!this.#targets || this.#targets.length > 0) {
      for (const client of this.#recipients()) {
        if (this.#excluded?.has(client)) continue;
        if (!client.persistent) continue;
        const id = client.generateId();
        try {
          const frame = this.#codec
            ? this.#codec.encode({ type: 'event', name, data, id })
            : `${prefix},"id":${JSON.stringify(id)}}`;
          client.sendRaw(frame);
        } catch (error) {
          this.#log.error({ err: error, event: 'broadcast.send', name });
          continue;
        }
        // Registered AFTER the write went through: a send that threw never
        // parks an answer slot that nothing will settle.
        pending.push(client.expectAnswer(id, timeout));
      }
    }
    const done = Promise.allSettled(pending).then((settled) => {
      const answers = [];
      const errors = [];
      for (const outcome of settled) {
        if (outcome.status === 'fulfilled') {
          answers.push(outcome.value);
          continue;
        }
        const failure = { message: outcome.reason?.message ?? 'Ask failed', code: outcome.reason?.code ?? 500 };
        if (outcome.reason?.details !== undefined) failure.details = outcome.reason.details;
        errors.push(failure);
      }
      return { answers, errors };
    });
    return { expected: pending.length, done };
  }
}

// Binds a room registry to an injected backplane.
//
// Channel scheme (the socket.io Redis-adapter shape): a broadcast, or an emit
// targeting more than one room, goes to the single `broadcast` channel every
// instance subscribes to; a single-room emit goes to that room's own channel,
// which only instances holding members of it subscribe to. Either way an
// envelope reaches a given instance through exactly ONE channel, so no
// receiver-side deduplication is needed.
// How long an emptied room's channel stays subscribed (ms): the grace
// window that absorbs reconnect churn. 0 disables (`rooms: { linger: 0 }`).
const DEFAULT_LINGER = 5_000;

class RoomsBackplane {
  #backplane;
  #instance;
  #deliver;
  #log;
  #linger;
  #channels = new Map(); // channel -> { count, off, stale, timer, lingerTimer }
  #closed = false;
  // Channels whose subscribe FAILED and is being retried: while any are
  // pending, cross-instance delivery on them is dark and `healthy` is
  // false — what a readiness probe should drain the node on.
  #pending = 0;

  constructor({ backplane, instance, deliver, log = globalThis.console, linger = DEFAULT_LINGER }) {
    this.#backplane = backplane;
    this.#instance = instance;
    this.#deliver = deliver;
    this.#log = createLoggerWriter(log);
    this.#linger = linger > 0 ? linger : 0;
  }

  get healthy() {
    return this.#pending === 0;
  }

  // The broadcast channel is retained for the process' whole life: an
  // instance with no rooms at all still has to hear server.broadcast().
  start() {
    this.retain(BROADCAST_CHANNEL);
  }

  retain(channel) {
    if (this.#closed) return;
    const entry = this.#channels.get(channel);
    if (entry) {
      entry.count++;
      entry.stale = false;
      // Re-populated inside the linger window: cancel the disposal, no
      // broker round trip happened at all.
      if (entry.lingerTimer) {
        clearTimeout(entry.lingerTimer);
        entry.lingerTimer = null;
      }
      return;
    }
    const record = { count: 1, off: null, stale: false, timer: null, lingerTimer: null };
    this.#channels.set(channel, record);
    this.#subscribe(channel, record, 0);
  }

  // Subscribes with capped-backoff RETRY on rejection. A rejected subscribe
  // used to delete the record outright, and nothing ever re-attempted:
  // while the room stayed populated the instance was permanently deaf on
  // its channel — and on BROADCAST_CHANNEL, permanently deaf to every
  // server.broadcast() — while looking perfectly healthy.
  #subscribe(channel, record, attempt) {
    const handler = (message) => this.#receive(message);
    Promise.resolve()
      .then(() => this.#backplane.subscribe(channel, handler))
      .then(
        (off) => {
          if (attempt > 0) {
            this.#pending--;
            this.#log.warn({ event: 'backplane.recovered', channel, attempt });
          }
          record.off = typeof off === 'function' ? off : null;
          // The room emptied (or the server closed) while subscribe was in
          // flight — unsubscribe now that there is something to unsubscribe.
          if (record.stale || this.#closed) this.#dispose(channel, record);
        },
        (error) => {
          this.#log.error({ err: error, event: 'backplane.subscribe', channel, attempt });
          if (record.stale || this.#closed) {
            if (attempt > 0) this.#pending--;
            return void this.#channels.delete(channel);
          }
          if (attempt === 0) this.#pending++;
          const delay = Math.min(30_000, 500 * 2 ** attempt);
          record.timer = setTimeout(() => {
            record.timer = null;
            this.#subscribe(channel, record, attempt + 1);
          }, delay);
          record.timer.unref?.();
        },
      );
  }

  release(channel) {
    const record = this.#channels.get(channel);
    if (!record) return;
    record.count--;
    if (record.count > 0) return;
    if (!record.off) {
      record.stale = true; // subscribe still in flight; dispose on arrival
      return;
    }
    // Linger before unsubscribing: for a single-member room (the per-user
    // pattern) every connect/disconnect used to be a SUBSCRIBE/UNSUBSCRIBE
    // pair on the broker, and every reconnect re-opened the documented
    // "published while between subscriptions" loss window. Holding the
    // emptied channel for a grace period collapses that churn to nothing.
    if (this.#linger > 0) {
      record.lingerTimer = setTimeout(() => {
        record.lingerTimer = null;
        if (record.count === 0 && !this.#closed) this.#dispose(channel, record);
      }, this.#linger);
      if (typeof record.lingerTimer.unref === 'function') record.lingerTimer.unref();
      return;
    }
    this.#dispose(channel, record);
  }

  #dispose(channel, record) {
    this.#channels.delete(channel);
    if (record.timer) {
      clearTimeout(record.timer);
      record.timer = null;
    }
    if (record.lingerTimer) {
      clearTimeout(record.lingerTimer);
      record.lingerTimer = null;
    }
    if (!record.off) return;
    try {
      const result = record.off();
      if (result && typeof result.catch === 'function') {
        result.catch((error) => this.#log.error({ err: error, event: 'backplane.unsubscribe', channel }));
      }
    } catch (error) {
      this.#log.error({ err: error, event: 'backplane.unsubscribe', channel });
    }
  }

  joinRoom(room) {
    this.retain(roomChannel(room));
  }

  leaveRoom(room) {
    this.release(roomChannel(room));
  }

  publish({ rooms, name, data }) {
    if (this.#closed) return;
    const envelope = { v: ENVELOPE_VERSION, instance: this.#instance, rooms: rooms ?? null, name, data };
    let message = null;
    try {
      message = JSON.stringify(envelope);
    } catch (error) {
      // Non-serializable payload: local delivery already happened, so this
      // is a cross-instance loss, not a lost event.
      return void this.#log.error({ err: error, event: 'backplane.serialize', name });
    }
    const single = rooms && rooms.length === 1;
    const channel = single ? roomChannel(rooms[0]) : BROADCAST_CHANNEL;
    try {
      const result = this.#backplane.publish(channel, message);
      if (result && typeof result.catch === 'function') {
        result.catch((error) => this.#log.error({ err: error, event: 'backplane.publish', channel }));
      }
    } catch (error) {
      // A broken backplane must never break local delivery.
      this.#log.error({ err: error, event: 'backplane.publish', channel });
    }
  }

  #receive(message) {
    if (this.#closed) return;
    const envelope = typeof message === 'string' ? jsonParse(message) : message;
    if (!envelope || typeof envelope !== 'object') return;
    // Echo suppression: every instance sees its own publishes.
    if (envelope.instance === this.#instance) return;
    const { rooms, name, data } = envelope;
    if (typeof name !== 'string' || name.length === 0) return;
    if (rooms !== null && rooms !== undefined && !Array.isArray(rooms)) return;
    try {
      this.#deliver(rooms ?? null, name, data);
    } catch (error) {
      this.#log.error({ err: error, event: 'backplane.deliver', name });
    }
  }

  // Unsubscribes every channel this server holds. The backplane itself is
  // NOT closed: it was injected, so its lifetime belongs to the caller.
  close() {
    if (this.#closed) return;
    this.#closed = true;
    for (const [channel, record] of Array.from(this.#channels)) {
      if (!record.off) {
        record.stale = true;
        continue;
      }
      this.#dispose(channel, record);
    }
  }
}

module.exports = {
  RoomRegistry,
  Broadcast,
  RoomsBackplane,
  roomChannel,
  BROADCAST_CHANNEL,
  ENVELOPE_VERSION,
};
