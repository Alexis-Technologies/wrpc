'use strict';

// Mesh: everyone in a signaling room, linked to everyone. Joining hands
// the roster to the peer, which links with each member — from either side,
// since connect() knocks when this side is not the initiator — and a
// member arriving later is linked as it joins. Each open link's host-side
// Client is kept in the room `mesh:<room>` on this peer's PeerHost, which
// is what makes broadcast() and ask() a single-encode Broadcast fan-out
// rather than a loop over links.

const { Emitter } = require('../utils.js');
const { hasRoster } = require('./signaler.js');

class Mesh extends Emitter {
  #peer;
  #room;
  #data;
  #signaler;
  #members = new Map(); // id -> PeerLink
  #responders = new Map();
  #joined = null;
  #left = false;
  #unbind = [];

  constructor(peer, room, { data = null } = {}) {
    super();
    if (!hasRoster(peer.signaler)) throw new TypeError('Mesh: the signaler must carry a roster (join/leave)');
    this.#peer = peer;
    this.#room = room;
    this.#data = data;
    this.#signaler = peer.signaler;
    const on = (emitter, name, fn) => {
      emitter.on(name, fn);
      this.#unbind.push(() => emitter.off(name, fn));
    };
    on(this.#signaler, 'join', (event) => {
      if (event.room === room) this.#ensure(event.id, event.data ?? null);
    });
    on(this.#signaler, 'leave', (event) => {
      if (event.room === room) this.#drop(event.id, true);
    });
    on(peer, 'reset', (event) => {
      const entry = Array.isArray(event?.rooms) ? event.rooms.find((item) => item.room === room) : null;
      if (entry) for (const member of entry.members) this.#ensure(member.id, member.data ?? null);
    });
    on(peer, 'link', (link) => this.#adopt(link));
    this.#joined = this.#join();
  }

  get room() {
    return this.#room;
  }

  /** Ids of the members this peer has an open link with (a copy). */
  get peers() {
    const ids = new Set();
    for (const [id, link] of this.#members) if (link.open) ids.add(id);
    return ids;
  }

  /** Every member link, open or still connecting (a copy). */
  get links() {
    return new Map(this.#members);
  }

  link(id) {
    return this.#members.get(id);
  }

  /** @internal */
  has(id) {
    return this.#members.has(id);
  }

  /** The host-side room every member link's Client is kept in. */
  get hostRoom() {
    return `mesh:${this.#room}`;
  }

  /** Resolves once the roster was fetched and every member link is dialling. */
  ready() {
    return this.#joined;
  }

  /** One event to every open member; how many received it. */
  broadcast(name, data) {
    return this.#host('broadcast').to(this.hostRoom).emit(name, data);
  }

  /** A question to every open member: { answers, errors, expected, incomplete }. */
  ask(name, data, options) {
    return this.#host('ask').to(this.hostRoom).ask(name, data, options);
  }

  /** Answers asks from every member, current and future. */
  respond(name, handler) {
    if (typeof handler !== 'function') throw new TypeError('Mesh.respond: handler must be a function');
    this.#responders.set(name, handler);
    for (const link of this.#members.values()) link.respond(name, handler);
  }

  unrespond(name) {
    const had = this.#responders.delete(name);
    for (const link of this.#members.values()) link.unrespond(name);
    return had;
  }

  /** Leaves the room; links no other mesh holds are closed. */
  async leave() {
    if (this.#left) return;
    this.detach();
    try {
      await this.#signaler.leave(this.#room);
    } catch (error) {
      this.#peer.escalate(error, this);
    }
  }

  /** @internal Drops every member without touching the signaler. */
  detach() {
    if (this.#left) return;
    this.#left = true;
    for (const unbind of this.#unbind) unbind();
    this.#unbind = [];
    for (const id of [...this.#members.keys()]) this.#drop(id, false);
    void this.emit('left').catch((error) => this.#peer.escalate(error, this));
  }

  #host(what) {
    const host = this.#peer.host;
    if (!host) throw new Error(`Mesh.${what}: this peer has no router, so no host to fan out from`);
    return host;
  }

  async #join() {
    const members = await this.#signaler.join(this.#room, this.#data);
    if (this.#left) return;
    for (const member of members) this.#ensure(member.id, member.data ?? null);
  }

  // A member: link if not yet linked (from either side), and keep its host
  // Client in the mesh room.
  #ensure(id, data) {
    if (this.#left || id === this.#peer.id) return;
    let link = this.#peer.link(id);
    if (!link) {
      // connect() resolves on open and rejects on close; both are
      // announced through the link's events below, so the promise itself
      // is only kept from being an unhandled rejection.
      this.#peer.connect(id, { room: this.#room, data }).catch(() => {});
      link = this.#peer.link(id);
    }
    if (link) this.#adopt(link, data);
  }

  // A link this peer made or accepted: a member of this mesh when it was
  // made in this room, or when the roster names it.
  #adopt(link, data = undefined) {
    if (this.#left || this.#members.has(link.id)) return;
    if (data === undefined && link.room !== this.#room) return;
    this.#members.set(link.id, link);
    link.join(this.hostRoom);
    for (const [name, handler] of this.#responders) link.respond(name, handler);
    const onOpen = () =>
      void this.emit('join', { id: link.id, data: data ?? link.data }).catch((e) => this.#peer.escalate(e, this));
    if (link.open) onOpen();
    else link.once('open', onOpen);
    link.once('close', () => {
      link.off('open', onOpen);
      this.#drop(link.id, true);
    });
    void this.emit('link', link).catch((error) => this.#peer.escalate(error, this));
  }

  #drop(id, announce) {
    const link = this.#members.get(id);
    if (!link) return;
    this.#members.delete(id);
    link.leave(this.hostRoom);
    for (const name of this.#responders.keys()) link.unrespond(name);
    if (!this.#peer.held(id, this)) link.close();
    if (announce) void this.emit('leave', { id }).catch((error) => this.#peer.escalate(error, this));
  }
}

module.exports = { Mesh };
