'use strict';

const { jsonParse } = require('../utils.js');

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

  // onSubscribe/onUnsubscribe fire when a room gains its first member and
  // loses its last one: that is exactly when a backplane has to (un)subscribe
  // the room's channel. Plain callbacks rather than events — join/leave is a
  // hot path and Emitter.emit allocates a promise per call.
  constructor({ onSubscribe = null, onUnsubscribe = null } = {}) {
    this.#onSubscribe = onSubscribe;
    this.#onUnsubscribe = onUnsubscribe;
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
  #targets;
  #excluded;
  #localOnly;
  #console;

  constructor({
    registry,
    clients,
    publish = null,
    console = globalThis.console,
    targets = null,
    excluded = null,
    localOnly = false,
  }) {
    this.#registry = registry;
    this.#clients = clients;
    this.#publish = publish;
    this.#console = console;
    this.#targets = targets;
    this.#excluded = excluded;
    this.#localOnly = localOnly;
  }

  #derive(changes) {
    return new Broadcast({
      registry: this.#registry,
      clients: this.#clients,
      publish: this.#publish,
      console: this.#console,
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
    let sent = 0;
    for (const client of this.#recipients()) {
      if (this.#excluded?.has(client)) continue;
      // HTTP clients cannot carry events; skipping beats throwing mid-fan-out.
      if (!client.persistent) continue;
      try {
        client.sendEvent(name, data);
        sent++;
      } catch (error) {
        // One dead socket must not truncate the fan-out.
        this.#console.error(error);
      }
    }
    if (!this.#localOnly && this.#publish) {
      this.#publish({ rooms: this.#targets, name, data });
    }
    return sent;
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
class RoomsBackplane {
  #backplane;
  #instance;
  #deliver;
  #console;
  #channels = new Map(); // channel -> { count, off, stale }
  #closed = false;

  constructor({ backplane, instance, deliver, console = globalThis.console }) {
    this.#backplane = backplane;
    this.#instance = instance;
    this.#deliver = deliver;
    this.#console = console;
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
      return;
    }
    const record = { count: 1, off: null, stale: false };
    this.#channels.set(channel, record);
    const handler = (message) => this.#receive(message);
    Promise.resolve()
      .then(() => this.#backplane.subscribe(channel, handler))
      .then(
        (off) => {
          record.off = typeof off === 'function' ? off : null;
          // The room emptied (or the server closed) while subscribe was in
          // flight — unsubscribe now that there is something to unsubscribe.
          if (record.stale || this.#closed) this.#dispose(channel, record);
        },
        (error) => {
          this.#channels.delete(channel);
          this.#console.error(error);
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
    this.#dispose(channel, record);
  }

  #dispose(channel, record) {
    this.#channels.delete(channel);
    if (!record.off) return;
    try {
      const result = record.off();
      if (result && typeof result.catch === 'function') {
        result.catch((error) => this.#console.error(error));
      }
    } catch (error) {
      this.#console.error(error);
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
      return void this.#console.error(error);
    }
    const single = rooms && rooms.length === 1;
    const channel = single ? roomChannel(rooms[0]) : BROADCAST_CHANNEL;
    try {
      const result = this.#backplane.publish(channel, message);
      if (result && typeof result.catch === 'function') {
        result.catch((error) => this.#console.error(error));
      }
    } catch (error) {
      // A broken backplane must never break local delivery.
      this.#console.error(error);
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
      this.#console.error(error);
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
