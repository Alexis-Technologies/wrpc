'use strict';

// The Signaler contract and the built-in implementation over a WrpcClient.
//
//   interface Signaler {
//     readonly id: string | null            this peer's id once ready()
//     ready(): Promise<string>              resolves the id (single-flight)
//     send(to, message, { room? })          relay one SignalMessage to a peer
//     on('signal', ({ from, room, message }) => void)   an inbound one
//     off(event, handler); close?()
//   }
//   interface RosterSignaler extends Signaler {
//     join(room, data?): Promise<Array<{ id, data }>>   the other members
//     leave(room): Promise<void>
//     on('join' | 'leave', ({ room, id, data? }) => void)
//     on('reset', ({ id, previous, rooms }) => void)    id changed: rebuild
//   }
//   SignalMessage = { type: 'description', description }
//                 | { type: 'candidate', candidate }
//                 | { type: 'close' }
//                 | { type: 'connect' }   a knock: "dial me" (WrpcPeer, from the non-initiator)
//
// WrpcPeer needs only a Signaler; Mesh needs a RosterSignaler. Anything
// with the shape qualifies (isSignaler / hasRoster are structural, like
// isEngine and isBackplane) — a hand-rolled one over socket.io, a hosted
// signaling service, a MessagePort between two tabs. wrpcSignaler() is the
// one wrpc ships: the client half of createSignalingUnit, over any
// WrpcClient transport (ws, sse, ...).

const { Emitter } = require('../utils.js');

const SIGNAL_MESSAGE_TYPES = Object.freeze(['description', 'candidate', 'close', 'connect']);

const isSignalMessage = (message) =>
  typeof message === 'object' && message !== null && SIGNAL_MESSAGE_TYPES.includes(message.type);

const isFunction = (value) => typeof value === 'function';

const isSignaler = (value) =>
  typeof value === 'object' &&
  value !== null &&
  'id' in value &&
  isFunction(value.ready) &&
  isFunction(value.send) &&
  isFunction(value.on) &&
  isFunction(value.off);

const hasRoster = (value) => isSignaler(value) && isFunction(value.join) && isFunction(value.leave);

const isWrpcClientLike = (client) =>
  typeof client === 'object' &&
  client !== null &&
  isFunction(client.call) &&
  isFunction(client.sendEvent) &&
  isFunction(client.use) &&
  isFunction(client.on) &&
  isFunction(client.off);

const checkRoom = (room) => {
  if (typeof room !== 'string' || room.length === 0) throw new TypeError('room must be a non-empty string');
  return room;
};

class WrpcSignaler extends Emitter {
  #client;
  #unit;
  #id = null;
  #ready = null;
  // room -> join data, so a reconnect can re-join what this peer was in.
  #rooms = new Map();
  #closed = false;
  #listeners;

  constructor(client, options = {}) {
    super();
    if (!isWrpcClientLike(client)) {
      throw new TypeError('wrpcSignaler: client must be a WrpcClient (call, sendEvent, use, on, off)');
    }
    const { unit = 'signaling' } = options;
    if (typeof unit !== 'string' || unit.length === 0) throw new TypeError('wrpcSignaler: unit must be a unit name');
    this.#client = client;
    this.#unit = unit;
    // A static unit with no methods: enough for client.api[unit] to exist as
    // the Emitter inbound events are delivered on, with no wire traffic and
    // no load() — usable before open() and untouched by reconnect's reload.
    client.use({ [unit]: {} });
    const api = client.api[unit];
    const relay = (name) => (payload) => {
      if (typeof payload !== 'object' || payload === null) return;
      return this.emit(name, payload);
    };
    this.#listeners = {
      signal: (payload) => {
        if (typeof payload !== 'object' || payload === null || !isSignalMessage(payload.message)) return;
        if (typeof payload.from !== 'string') return;
        return this.emit('signal', { from: payload.from, room: payload.room ?? null, message: payload.message });
      },
      join: relay('join'),
      leave: relay('leave'),
      reconnect: () => void this.#reset(),
    };
    api.on('signal', this.#listeners.signal);
    api.on('join', this.#listeners.join);
    api.on('leave', this.#listeners.leave);
    client.on('reconnect', this.#listeners.reconnect);
  }

  /** The peer id the server issued, or null before ready(). */
  get id() {
    return this.#id;
  }

  /** The WrpcClient this signaler rides. */
  get client() {
    return this.#client;
  }

  /** The unit name on the server. */
  get unit() {
    return this.#unit;
  }

  /** The rooms joined through this signaler (a copy). */
  get rooms() {
    return new Set(this.#rooms.keys());
  }

  ready() {
    if (this.#closed) return Promise.reject(new Error('Signaler is closed'));
    if (this.#ready === null) {
      // Any failure — a refused call, an answer without an id — forgets the
      // attempt, so the next ready() asks again instead of caching it.
      this.#ready = this.#client
        .call(`${this.#unit}/whoami`)
        .then((result) => {
          if (this.#closed) throw new Error('Signaler is closed');
          if (typeof result?.id !== 'string' || result.id.length === 0) {
            throw new TypeError(`${this.#unit}/whoami answered without an id`);
          }
          this.#id = result.id;
          return result.id;
        })
        .catch((error) => {
          this.#ready = null;
          throw error;
        });
    }
    return this.#ready;
  }

  send(to, message, options = {}) {
    if (typeof to !== 'string' || to.length === 0) throw new TypeError('send: to must be a peer id');
    if (!isSignalMessage(message)) {
      throw new TypeError('send: message.type must be description, candidate, close or connect');
    }
    const room = options.room === undefined ? null : checkRoom(options.room);
    this.#client.sendEvent(`${this.#unit}/signal`, { to, room, message });
  }

  async join(room, data = null) {
    checkRoom(room);
    await this.ready();
    const result = await this.#client.call(`${this.#unit}/join`, { room, data });
    // A close() that landed while the call was in flight must not
    // resurrect the room it just forgot.
    if (!this.#closed) this.#rooms.set(room, data);
    return Array.isArray(result?.members) ? result.members : [];
  }

  async leave(room) {
    checkRoom(room);
    this.#rooms.delete(room);
    await this.#client.call(`${this.#unit}/leave`, { room });
  }

  async members(room) {
    checkRoom(room);
    const result = await this.#client.call(`${this.#unit}/members`, { room });
    return Array.isArray(result) ? result : [];
  }

  // The signaling connection came back as a NEW server-side client: new id,
  // rooms gone. Re-join every room this peer was in, then announce the
  // reset with the fresh rosters so a Mesh can rebuild in one pass.
  async #reset() {
    const previous = this.#id;
    this.#ready = null;
    this.#id = null;
    const rooms = [];
    try {
      await this.ready();
      for (const [room, data] of this.#rooms) {
        const members = await this.join(room, data);
        rooms.push({ room, members });
      }
    } catch (error) {
      if (!this.#closed && this.listenerCount('error') > 0) await this.emit('error', error);
      return;
    }
    // close() may have run while the calls above were in flight.
    if (this.#closed) return;
    await this.emit('reset', { id: this.#id, previous, rooms });
  }

  /** Detaches from the client; the client itself stays open. */
  close() {
    if (this.#closed) return;
    this.#closed = true;
    const api = this.#client.api[this.#unit];
    api.off('signal', this.#listeners.signal);
    api.off('join', this.#listeners.join);
    api.off('leave', this.#listeners.leave);
    this.#client.off('reconnect', this.#listeners.reconnect);
    this.#rooms.clear();
    this.#ready = null;
    this.#id = null;
  }
}

/** The client half of createSignalingUnit, over an open or opening WrpcClient. */
const wrpcSignaler = (client, options) => new WrpcSignaler(client, options);

module.exports = { WrpcSignaler, wrpcSignaler, isSignaler, hasRoster, isSignalMessage, SIGNAL_MESSAGE_TYPES };
