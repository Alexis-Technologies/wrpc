'use strict';

// Mesh: everyone in a signaling room, linked to everyone. Joining hands
// the roster to the peer, which links with each member — from either side,
// since connect() knocks when this side is not the initiator — and a
// member arriving later is linked as it joins. Each open link's host-side
// Client is kept in the room `mesh:<room>` on this peer's PeerHost, which
// is what makes broadcast() and ask() a single-encode Broadcast fan-out
// rather than a loop over links.
//
// A member whose signaling connection dropped (`leave` with reason
// 'disconnect') is only `away`: its link never needed signaling to keep
// working, and under a stable identity the member re-joins as the same
// incarnation moments later. The link is kept until the member leaves for
// real, comes back as another incarnation (the peer relinks), or the link
// itself fails through the ordinary redial cycle.

const { Emitter } = require('../utils.js');
const { hasRoster } = require('./signaler.js');

class Mesh extends Emitter {
  #peer;
  #room;
  #data;
  #signaler;
  #members = new Map(); // id -> PeerLink
  #away = new Set(); // ids whose signaling dropped while their link stayed up
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
      if (event.room === room) this.#ensure(event.id, event.data ?? null, event.instance ?? null);
    });
    on(this.#signaler, 'leave', (event) => {
      if (event.room !== room) return;
      if (event.reason === 'disconnect' && this.#members.get(event.id)?.open) this.#away.add(event.id);
      // A replaced member's link is abandoned, not closed: a goodbye sent
      // to its id now would reach the NEW incarnation — and land on the
      // fresh link this mesh is about to make with it.
      else this.#dropId(event.id, true, event.reason === 'replaced');
    });
    on(peer, 'reset', (event) => {
      const entry = Array.isArray(event?.rooms) ? event.rooms.find((item) => item.room === room) : null;
      if (!entry) return;
      for (const member of entry.members) this.#ensure(member.id, member.data ?? null, member.instance ?? null);
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

  /** Members whose signaling connection dropped while their link stayed up (a copy). */
  get away() {
    return new Set(this.#away);
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
    for (const link of [...this.#members.values()]) this.#drop(link, false);
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
    for (const member of members) this.#ensure(member.id, member.data ?? null, member.instance ?? null);
  }

  // A member: link if not yet linked (from either side), and keep its host
  // Client in the mesh room. The same incarnation coming back from `away`
  // is already linked and only cleared; another incarnation makes the peer
  // abandon the stale link (its close drops the member) and dial the fresh
  // one, adopted through the peer's 'link' event.
  #ensure(id, data, instance) {
    if (this.#left || id === this.#peer.id) return;
    this.#away.delete(id);
    // connect() resolves on open and rejects on close; both are announced
    // through the link's events below, so the promise itself is only kept
    // from being an unhandled rejection.
    this.#peer.connect(id, { room: this.#room, data, instance }).catch(() => {});
    const link = this.#peer.link(id);
    if (link) this.#adopt(link, data);
  }

  // A link this peer made or accepted: a member of this mesh when it was
  // made in this room, or when the roster names it. A link that replaced a
  // member's earlier one takes its place; the old one leaves on its close.
  #adopt(link, data = undefined) {
    if (this.#left) return;
    const known = this.#members.get(link.id);
    if (known === link) return;
    if (data === undefined && link.room !== this.#room) return;
    if (known) this.#release(known);
    this.#members.set(link.id, link);
    link.join(this.hostRoom);
    for (const [name, handler] of this.#responders) link.respond(name, handler);
    const onOpen = () =>
      void this.emit('join', { id: link.id, data: data ?? link.data }).catch((e) => this.#peer.escalate(e, this));
    if (link.open) onOpen();
    else link.once('open', onOpen);
    link.once('close', () => {
      link.off('open', onOpen);
      if (this.#members.get(link.id) === link) this.#drop(link, true);
    });
    void this.emit('link', link).catch((error) => this.#peer.escalate(error, this));
  }

  #dropId(id, announce, abandon = false) {
    const link = this.#members.get(id);
    if (link) this.#drop(link, announce, abandon);
  }

  #drop(link, announce, abandon = false) {
    this.#members.delete(link.id);
    this.#away.delete(link.id);
    this.#release(link);
    if (!this.#peer.held(link.id, this)) {
      if (abandon) link.abandon();
      else link.close();
    }
    if (announce) void this.emit('leave', { id: link.id }).catch((error) => this.#peer.escalate(error, this));
  }

  // Out of the mesh room and off the responders, whether or not it closes.
  #release(link) {
    link.leave(this.hostRoom);
    for (const name of this.#responders.keys()) link.unrespond(name);
  }
}

module.exports = { Mesh };
