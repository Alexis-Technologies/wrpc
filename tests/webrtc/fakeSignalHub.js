'use strict';

// An in-memory signaling hub: N RosterSignalers over one roster, delivering
// through setImmediate the way createSignalingUnit + wrpcSignaler would
// over a socket — without a server. Test hooks: mute(id) drops every
// signal addressed to `id` (an unreachable peer), drop(id) is that peer's
// signaling connection dying (leaves announced), reset(id, newId) is its
// wrpcSignaler reconnecting under a new server-issued id. Not a *.test.js.

const { Emitter } = require('../../src/utils.js');

class HubSignaler extends Emitter {
  #hub;
  #id;
  #rooms = new Map(); // room -> data

  constructor(hub, id) {
    super();
    this.#hub = hub;
    this.#id = id;
  }

  get id() {
    return this.#id;
  }

  /** @internal */
  set id(value) {
    this.#id = value;
  }

  get rooms() {
    return this.#rooms;
  }

  ready() {
    return Promise.resolve(this.#id);
  }

  send(to, message, options = {}) {
    this.#hub.relay(this.#id, to, options.room ?? null, message);
  }

  async join(room, data = null) {
    await this.#hub.tick();
    this.#rooms.set(room, data);
    return this.#hub.join(this.#id, room, data);
  }

  async leave(room) {
    await this.#hub.tick();
    this.#rooms.delete(room);
    this.#hub.leave(this.#id, room);
  }

  close() {}
}

class FakeSignalHub {
  #peers = new Map(); // id -> HubSignaler
  #rooms = new Map(); // room -> Map<id, data>
  #muted = new Set();
  sent = [];

  signaler(id) {
    const signaler = new HubSignaler(this, id);
    this.#peers.set(id, signaler);
    return signaler;
  }

  tick() {
    return new Promise((resolve) => setImmediate(resolve));
  }

  #deliver(id, name, payload) {
    const target = this.#peers.get(id);
    if (!target) return;
    setImmediate(() => void target.emit(name, payload).catch(() => {}));
  }

  relay(from, to, room, message) {
    this.sent.push({ from, to, room, type: message.type });
    if (this.#muted.has(to) || !this.#peers.has(to)) return;
    this.#deliver(to, 'signal', { from, room, message });
  }

  join(id, room, data) {
    let members = this.#rooms.get(room);
    if (!members) this.#rooms.set(room, (members = new Map()));
    const fresh = !members.has(id);
    members.set(id, data);
    const roster = [];
    for (const [other, otherData] of members) {
      if (other === id) continue;
      roster.push({ id: other, data: otherData });
      if (fresh) this.#deliver(other, 'join', { room, id, data });
    }
    return roster;
  }

  leave(id, room) {
    const members = this.#rooms.get(room);
    if (!members || !members.delete(id)) return;
    for (const other of members.keys()) this.#deliver(other, 'leave', { room, id });
  }

  members(room) {
    return new Set(this.#rooms.get(room)?.keys() ?? []);
  }

  mute(id) {
    this.#muted.add(id);
  }

  unmute(id) {
    this.#muted.delete(id);
  }

  /** The peer's signaling connection died: it leaves every room. */
  drop(id) {
    for (const room of [...this.#rooms.keys()]) this.leave(id, room);
    this.#peers.delete(id);
  }

  /** The peer's signaler reconnected as `newId`: rooms re-joined, 'reset' announced. */
  async reset(id, newId) {
    const signaler = this.#peers.get(id);
    for (const room of [...this.#rooms.keys()]) this.leave(id, room);
    this.#peers.delete(id);
    signaler.id = newId;
    this.#peers.set(newId, signaler);
    await this.tick();
    const rooms = [];
    for (const [room, data] of signaler.rooms) rooms.push({ room, members: this.join(newId, room, data) });
    await signaler.emit('reset', { id: newId, previous: id, rooms });
  }
}

module.exports = { FakeSignalHub, HubSignaler };
