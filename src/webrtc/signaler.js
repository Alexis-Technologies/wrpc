'use strict';

// The Signaler contract and the built-in implementation over a WrpcClient.
//
//   interface Signaler {
//     readonly id: string | null            this peer's id once ready()
//     readonly instance?: string | null     this incarnation of the id (optional)
//     ready(): Promise<string>              resolves the id (single-flight)
//     send(to, message, { room?, address? })   relay one SignalMessage to a peer
//     on('signal', ({ from, instance?, address?, room, message }) => void)   an inbound one
//     off(event, handler); close?()
//   }
//   interface RosterSignaler extends Signaler {
//     join(room, data?): Promise<Array<{ id, instance?, address?, data }>>   the other members
//     leave(room): Promise<void>
//     on('join', ({ room, id, instance?, address?, data? }) => void)
//     on('leave', ({ room, id, instance?, reason? }) => void)
//                                           reason: 'left' | 'disconnect' | 'replaced'
//     on('reset', ({ id, previous, rooms }) => void)    re-identified: rebuild
//     on('replaced', ({ id }) => void)      a newer connection took this id
//   }
//   interface AssertingSignaler extends Signaler {   trust assertions (hasAssertions)
//     assert({ fingerprint }): Promise<{ assertion, iat?, exp? }>   a token for one of MY certificates
//     keys?(): Promise<Array<JsonWebKey>>            the server's public keys
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
//
// Identity: the id is whatever the server's identity strategy answers to
// whoami — the connection's client id by default, or a stable
// application id — with the `identity` option as this side's proposal.
// `instance` is generated once per signaler and tells incarnations of the
// same id apart across reconnects; the server only carries it. Peer
// addresses (the routable client ids the roster and signals carry) are
// remembered here and attached to send(), so a relay never has to resolve
// a peer id on the hot path.

const { Emitter } = require('../utils.js');
const { generateUUID } = require('../runtime/node.js');

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

const hasAssertions = (value) => isSignaler(value) && isFunction(value.assert);

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

const isId = (value) => typeof value === 'string' && value.length > 0;

const optionalId = (value) => (isId(value) ? value : null);

class WrpcSignaler extends Emitter {
  #client;
  #unit;
  #identity;
  #instance;
  #id = null;
  #ready = null;
  // room -> join data, so a reconnect can re-join what this peer was in.
  #rooms = new Map();
  // peer id -> the routable address the server last told us for it.
  #addresses = new Map();
  #closed = false;
  #replaced = false;
  #listeners;

  constructor(client, options = {}) {
    super();
    if (!isWrpcClientLike(client)) {
      throw new TypeError('wrpcSignaler: client must be a WrpcClient (call, sendEvent, use, on, off)');
    }
    const { unit = 'signaling', identity = null, generateId = generateUUID } = options;
    if (typeof unit !== 'string' || unit.length === 0) throw new TypeError('wrpcSignaler: unit must be a unit name');
    if (identity !== null && !isId(identity) && !isFunction(identity)) {
      throw new TypeError('wrpcSignaler: identity must be a non-empty string or a function');
    }
    if (!isFunction(generateId)) throw new TypeError('wrpcSignaler: generateId must be a function');
    const instance = generateId();
    if (!isId(instance)) throw new TypeError('wrpcSignaler: generateId must return a non-empty string');
    this.#client = client;
    this.#unit = unit;
    this.#identity = identity;
    this.#instance = instance;
    // A static unit with no methods: enough for client.api[unit] to exist as
    // the Emitter inbound events are delivered on, with no wire traffic and
    // no load() — usable before open() and untouched by reconnect's reload.
    client.use({ [unit]: {} });
    const api = client.api[unit];
    this.#listeners = {
      signal: (payload) => {
        if (typeof payload !== 'object' || payload === null || !isSignalMessage(payload.message)) return;
        if (!isId(payload.from)) return;
        // Addressed to a peer id or an incarnation this is not: a stale
        // address hint landed on this connection. Not ours.
        if (isId(payload.to) && payload.to !== this.#id) return;
        if (isId(payload.toInstance) && payload.toInstance !== this.#instance) return;
        const address = optionalId(payload.address);
        if (address) this.#addresses.set(payload.from, address);
        return this.emit('signal', {
          from: payload.from,
          instance: optionalId(payload.instance),
          address,
          room: payload.room ?? null,
          message: payload.message,
        });
      },
      join: (payload) => {
        if (typeof payload !== 'object' || payload === null) return;
        if (isId(payload.id) && isId(payload.address)) this.#addresses.set(payload.id, payload.address);
        return this.emit('join', payload);
      },
      leave: (payload) => {
        if (typeof payload !== 'object' || payload === null) return;
        if (isId(payload.id)) this.#addresses.delete(payload.id);
        return this.emit('leave', payload);
      },
      replaced: (payload) => {
        if (typeof payload !== 'object' || payload === null) return;
        return this.#onReplaced(payload);
      },
      reconnect: () => void this.#reset(),
    };
    api.on('signal', this.#listeners.signal);
    api.on('join', this.#listeners.join);
    api.on('leave', this.#listeners.leave);
    api.on('replaced', this.#listeners.replaced);
    client.on('reconnect', this.#listeners.reconnect);
  }

  /** The peer id the server agreed to, or null before ready(). */
  get id() {
    return this.#id;
  }

  /** This incarnation of the id: generated once, sent with every whoami. */
  get instance() {
    return this.#instance;
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

  /** True once a newer connection took this peer id; the signaler is over. */
  get replaced() {
    return this.#replaced;
  }

  /** The routable address last learned for a peer, or null. */
  addressOf(id) {
    return this.#addresses.get(id) ?? null;
  }

  ready() {
    if (this.#closed) return Promise.reject(new Error('Signaler is closed'));
    if (this.#replaced) return Promise.reject(new Error('Signaler was replaced'));
    if (this.#ready === null) {
      // Any failure — a refused call, an answer without an id — forgets the
      // attempt, so the next ready() asks again instead of caching it.
      this.#ready = this.#whoami().catch((error) => {
        this.#ready = null;
        throw error;
      });
    }
    return this.#ready;
  }

  async #whoami() {
    const identity = this.#identity;
    const proposed = isFunction(identity) ? await identity() : identity;
    if (proposed !== null && proposed !== undefined && !isId(proposed)) {
      throw new TypeError('wrpcSignaler: identity must produce a non-empty string');
    }
    const args = { instance: this.#instance };
    if (isId(proposed)) args.id = proposed;
    const result = await this.#client.call(`${this.#unit}/whoami`, args);
    if (this.#closed) throw new Error('Signaler is closed');
    if (this.#replaced) throw new Error('Signaler was replaced');
    if (typeof result?.id !== 'string' || result.id.length === 0) {
      throw new TypeError(`${this.#unit}/whoami answered without an id`);
    }
    this.#id = result.id;
    return result.id;
  }

  send(to, message, options = {}) {
    if (!isId(to)) throw new TypeError('send: to must be a peer id');
    if (!isSignalMessage(message)) {
      throw new TypeError('send: message.type must be description, candidate, close or connect');
    }
    if (this.#replaced) throw new Error('Signaler was replaced');
    const room = options.room === undefined ? null : checkRoom(options.room);
    const address = optionalId(options.address) ?? this.#addresses.get(to) ?? null;
    const payload = { to, room, message };
    if (address !== null) payload.address = address;
    this.#client.sendEvent(`${this.#unit}/signal`, payload);
  }

  async join(room, data = null) {
    checkRoom(room);
    await this.ready();
    const result = await this.#client.call(`${this.#unit}/join`, { room, data });
    // A close() that landed while the call was in flight must not
    // resurrect the room it just forgot.
    if (!this.#closed && !this.#replaced) this.#rooms.set(room, data);
    const members = Array.isArray(result?.members) ? result.members : [];
    for (let i = 0; i < members.length; i++) {
      const member = members[i];
      if (isId(member?.id) && isId(member.address)) this.#addresses.set(member.id, member.address);
    }
    return members;
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

  /** A trust assertion binding this peer's id to one of its certificates: `<unit>/assert`. */
  async assert({ fingerprint } = {}) {
    if (typeof fingerprint !== 'string' || fingerprint.length === 0) {
      throw new TypeError('assert: fingerprint must be a non-empty string');
    }
    await this.ready();
    const result = await this.#client.call(`${this.#unit}/assert`, { fingerprint });
    if (typeof result?.assertion !== 'string' || result.assertion.length === 0) {
      throw new TypeError(`${this.#unit}/assert answered without an assertion`);
    }
    return result;
  }

  /** The server's public assertion keys (JWKs): `<unit>/keys`. */
  async keys() {
    const result = await this.#client.call(`${this.#unit}/keys`);
    if (!Array.isArray(result?.keys)) throw new TypeError(`${this.#unit}/keys answered without keys`);
    return result.keys;
  }

  // The signaling connection came back as a NEW server-side client: rooms
  // gone, the id to be agreed again (the same one, under a stable identity
  // strategy — so it is kept until the answer lands, and a knock arriving
  // meanwhile still has a local id to dial with). Re-join every room this
  // peer was in, then announce the reset with the fresh rosters so a Mesh
  // can rebuild in one pass.
  async #reset() {
    if (this.#replaced) return;
    const previous = this.#id;
    this.#ready = null;
    this.#addresses.clear();
    const rooms = [];
    try {
      await this.ready();
      for (const [room, data] of this.#rooms) {
        const members = await this.join(room, data);
        rooms.push({ room, members });
      }
    } catch (error) {
      if (!this.#closed && !this.#replaced && this.listenerCount('error') > 0) await this.emit('error', error);
      return;
    }
    // close() may have run while the calls above were in flight.
    if (this.#closed || this.#replaced) return;
    await this.emit('reset', { id: this.#id, previous, rooms });
  }

  // A newer connection identified as this peer id: this incarnation is
  // over. Nothing is re-identified on reconnect and nothing is sent; the
  // owner decides what the page does next.
  async #onReplaced(payload) {
    if (this.#closed || this.#replaced) return;
    const id = this.#id;
    this.#replaced = true;
    this.#id = null;
    this.#ready = null;
    this.#rooms.clear();
    this.#addresses.clear();
    await this.emit('replaced', { id: isId(payload.id) ? payload.id : id });
  }

  /** Detaches from the client; the client itself stays open. */
  close() {
    if (this.#closed) return;
    this.#closed = true;
    const api = this.#client.api[this.#unit];
    api.off('signal', this.#listeners.signal);
    api.off('join', this.#listeners.join);
    api.off('leave', this.#listeners.leave);
    api.off('replaced', this.#listeners.replaced);
    this.#client.off('reconnect', this.#listeners.reconnect);
    this.#rooms.clear();
    this.#addresses.clear();
    this.#ready = null;
    this.#id = null;
  }
}

/** The client half of createSignalingUnit, over an open or opening WrpcClient. */
const wrpcSignaler = (client, options) => new WrpcSignaler(client, options);

module.exports = {
  WrpcSignaler,
  wrpcSignaler,
  isSignaler,
  hasRoster,
  hasAssertions,
  isSignalMessage,
  SIGNAL_MESSAGE_TYPES,
};
