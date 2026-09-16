'use strict';

// An in-memory signaling hub: N RosterSignalers over one roster, delivering
// through setImmediate the way createSignalingUnit + wrpcSignaler would
// over a socket — without a server. Payloads carry what the real unit's
// do: `instance` (the signaler's incarnation), `address` (here the id
// itself) and a `reason` on leave. Test hooks: mute(id) drops every signal
// addressed to `id` (an unreachable peer), drop(id) is that peer's
// signaling connection dying (leaves announced as 'disconnect'),
// reconnect(id) is its wrpcSignaler coming back under the SAME id (rooms
// re-joined, 'reset' with id === previous), reset(id, newId) the same under
// a new server-issued id, and replace(id, instance) a newer connection
// taking the id over ('replaced' to the old signaler, 'replaced' leaves to
// the rooms unless the incarnation is the same). Not a *.test.js.

const { Emitter } = require('../../src/utils.js');

let sequence = 0;

class HubSignaler extends Emitter {
  #hub;
  #id;
  #instance;
  #rooms = new Map(); // room -> data
  #replaced = false;

  constructor(hub, id, instance) {
    super();
    this.#hub = hub;
    this.#id = id;
    this.#instance = instance;
  }

  get id() {
    return this.#id;
  }

  /** @internal */
  set id(value) {
    this.#id = value;
  }

  get instance() {
    return this.#instance;
  }

  get rooms() {
    return this.#rooms;
  }

  get replaced() {
    return this.#replaced;
  }

  /** @internal */
  set replaced(value) {
    this.#replaced = value;
  }

  ready() {
    if (this.#replaced) return Promise.reject(new Error('Signaler was replaced'));
    return Promise.resolve(this.#id);
  }

  send(to, message, options = {}) {
    if (this.#replaced) throw new Error('Signaler was replaced');
    this.#hub.relay(this, to, options.room ?? null, message);
  }

  async join(room, data = null) {
    await this.#hub.tick();
    if (this.#replaced) throw new Error('Signaler was replaced');
    this.#rooms.set(room, data);
    return this.#hub.join(this, room, data);
  }

  async leave(room) {
    await this.#hub.tick();
    this.#rooms.delete(room);
    this.#hub.leave(this, room, 'left');
  }

  close() {}
}

class FakeSignalHub {
  #peers = new Map(); // id -> HubSignaler
  #rooms = new Map(); // room -> Map<id, data>
  #muted = new Set();
  sent = [];

  signaler(id, { instance = `inst-${++sequence}` } = {}) {
    const signaler = new HubSignaler(this, id, instance);
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

  /** `from` is a HubSignaler, or a bare id for a signal from nobody in particular. */
  relay(from, to, room, message) {
    const fromId = typeof from === 'string' ? from : from.id;
    const instance = typeof from === 'string' ? null : from.instance;
    this.sent.push({ from: fromId, to, room, type: message.type });
    if (this.#muted.has(to) || !this.#peers.has(to)) return;
    const target = this.#peers.get(to);
    this.#deliver(to, 'signal', {
      from: fromId,
      instance,
      address: fromId,
      to,
      toInstance: target.instance,
      room,
      message,
    });
  }

  join(signaler, room, data) {
    const { id } = signaler;
    let members = this.#rooms.get(room);
    if (!members) this.#rooms.set(room, (members = new Map()));
    const fresh = !members.has(id);
    members.set(id, data);
    const roster = [];
    for (const [other, otherData] of members) {
      if (other === id) continue;
      const peer = this.#peers.get(other);
      roster.push({ id: other, instance: peer ? peer.instance : null, address: other, data: otherData });
      if (fresh) this.#deliver(other, 'join', { room, id, instance: signaler.instance, address: id, data });
    }
    return roster;
  }

  leave(signaler, room, reason) {
    const members = this.#rooms.get(room);
    if (!members || !members.delete(signaler.id)) return;
    for (const other of members.keys()) {
      this.#deliver(other, 'leave', {
        room,
        id: signaler.id,
        instance: signaler.instance,
        address: signaler.id,
        reason,
      });
    }
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

  /** The peer's signaling connection died: it leaves every room ('disconnect'). */
  drop(id) {
    const signaler = this.#peers.get(id);
    if (!signaler) return;
    for (const room of [...this.#rooms.keys()]) this.leave(signaler, room, 'disconnect');
    this.#peers.delete(id);
  }

  /**
   * The peer's signaler reconnected: its rooms were lost with the old
   * connection ('disconnect' leaves), it re-joins them under `newId` (the
   * same id by default — a stable identity) and announces 'reset'.
   */
  async reset(id, newId = id) {
    const signaler = this.#peers.get(id);
    for (const room of [...this.#rooms.keys()]) this.leave(signaler, room, 'disconnect');
    this.#peers.delete(id);
    signaler.id = newId;
    this.#peers.set(newId, signaler);
    await this.tick();
    const rooms = [];
    for (const [room, data] of signaler.rooms) rooms.push({ room, members: this.join(signaler, room, data) });
    await signaler.emit('reset', { id: newId, previous: id, rooms });
  }

  /** The same as reset(id): the signaling connection came back under the same id. */
  reconnect(id) {
    return this.reset(id, id);
  }

  /**
   * A newer connection identified as `id`: the old signaler is told
   * 'replaced' and is over; the rooms hear 'replaced' leaves unless the new
   * incarnation is the same one (a takeover by the same tab). Returns the
   * new signaler.
   */
  async replace(id, { instance = `inst-${++sequence}` } = {}) {
    const old = this.#peers.get(id);
    const same = old.instance === instance;
    for (const room of [...this.#rooms.keys()]) {
      const members = this.#rooms.get(room);
      if (!members.has(id)) continue;
      if (same) members.delete(id);
      else this.leave(old, room, 'replaced');
    }
    old.replaced = true;
    const fresh = new HubSignaler(this, id, instance);
    this.#peers.set(id, fresh);
    setImmediate(() => void old.emit('replaced', { id }).catch(() => {}));
    await this.tick();
    return fresh;
  }
}

module.exports = { FakeSignalHub, HubSignaler };
