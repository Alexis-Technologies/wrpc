'use strict';

// Mesh: everyone in a signaling room, linked to everyone. Joining hands
// the roster to the peer, which links with each member — from either side,
// since connect() knocks when this side is not the initiator — and a
// member arriving later is linked as it joins. Each open link's host-side
// Client is kept in the room `mesh:<room>` on this peer's PeerHost, which
// is what makes broadcast() and ask() one Broadcast fan-out rather than a
// loop over links: the payload is serialized once, encoded to UTF-8 once
// and, under compression, deflated once per codec; only the fragmenting to
// each link's message size is per link (ChannelCodec.sendShared).
//
// A member whose signaling connection dropped (`leave` with reason
// 'disconnect') is only `away`: its link never needed signaling to keep
// working, and under a stable identity the member re-joins as the same
// incarnation moments later. The link is kept until the member leaves for
// real, comes back as another incarnation (the peer relinks), or the link
// itself fails through the ordinary redial cycle.
//
// A link that gives up — its redial budget spent, a peer that said goodbye
// to the link but not to the room, an accept() that refused — used to be
// the end of that edge: #ensure only ran on a join, so two peers both still
// in the room stayed unlinked for good, and broadcast()/ask() quietly went
// around the member. The mesh keeps the ROSTER now, and an edge to a member
// still on it is dialled again, at a pace that backs off to `maxDelay` and
// stays there for as long as the member is in the room.

const { Emitter, backoffDelay } = require('../utils.js');
const { hasRoster } = require('./signaler.js');
const { isPeerId } = require('./ids.js');

// How an edge to a member still in the room is dialled again once its link
// is gone: full-jitter backoff from a second to a minute, and then every
// minute — a refusal comes back as a close like any other, so the pace has
// a floor rather than a count. `false` never dials again.
const RELINK = { minDelay: 1000, maxDelay: 60_000, jitter: true };

const normalizeRelink = (value) => {
  if (value === false) return null;
  if (value === undefined || value === null || value === true) return RELINK;
  const relink = { ...RELINK, ...value };
  const { minDelay, maxDelay } = relink;
  if (!(Number.isFinite(minDelay) && minDelay >= 0 && Number.isFinite(maxDelay) && maxDelay >= minDelay)) {
    throw new TypeError('Mesh: relink must be false or { minDelay, maxDelay, jitter } with 0 <= minDelay <= maxDelay');
  }
  return relink;
};

class Mesh extends Emitter {
  #peer;
  #room;
  #data;
  #signaler;
  // Who the room holds, as the signaler told it: id -> { data, instance }.
  #roster = new Map();
  #relink;
  // id -> { timer, attempt, said }: an edge waiting to be dialled again.
  #relinks = new Map();
  #members = new Map(); // id -> PeerLink
  #away = new Set(); // ids whose signaling dropped while their link stayed up
  #responders = new Map();
  #joined = null;
  #left = false;
  #unbind = [];

  constructor(peer, room, { data = null, relink } = {}) {
    super();
    if (!hasRoster(peer.signaler)) throw new TypeError('Mesh: the signaler must carry a roster (join/leave)');
    this.#peer = peer;
    this.#room = room;
    this.#data = data;
    this.#relink = normalizeRelink(relink);
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
      const link = this.#members.get(event.id);
      // A leave about another incarnation of the member than the one
      // linked — the old tab's goodbye, landing after the new tab was
      // linked — is not about this link; acting on it would drop the fresh one.
      if (link && isPeerId(event.instance) && link.instance !== null && event.instance !== link.instance) {
        peer.log.debug({ event: 'mesh.leave.stale', room, peer: event.id, instance: event.instance });
        return;
      }
      // The same guard for a member not linked right now: a stale goodbye
      // must not take the incarnation the roster holds off it.
      const listed = this.#roster.get(event.id);
      if (
        !link &&
        listed &&
        isPeerId(event.instance) &&
        listed.instance !== null &&
        event.instance !== listed.instance
      ) {
        return;
      }
      if (event.reason === 'disconnect' && link?.open) return void this.#away.add(event.id);
      // Gone from the room: off the roster, and nothing left to dial again.
      this.#forget(event.id);
      // A replaced member's link is abandoned, not closed: a goodbye sent
      // to its id now would reach the NEW incarnation — and land on the
      // fresh link this mesh is about to make with it.
      this.#dropId(event.id, true, event.reason === 'replaced');
    });
    on(peer, 'reset', (event) => {
      const entry = Array.isArray(event?.rooms) ? event.rooms.find((item) => item.room === room) : null;
      if (!entry) return;
      for (const member of entry.members) this.#ensure(member.id, member.data ?? null, member.instance ?? null);
    });
    on(peer, 'link', (link) => this.#adopt(link));
    this.#joined = this.#join();
    // A join nobody awaits (peer.join() fires and forgets) whose roster
    // fetch fails used to be an unhandled rejection: it is the peer's
    // error, and the mesh detaches — it never joined.
    this.#joined.catch((error) => {
      if (this.#left) return;
      this.#peer.escalate(error, this);
      this.detach();
    });
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
    for (const id of [...this.#roster.keys()]) this.#forget(id);
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
    this.#roster.set(id, { data, instance });
    // connect() resolves on open and rejects on close; both are announced
    // through the link's events below, so the promise itself is only kept
    // from being an unhandled rejection. It is still worth a line: a mesh
    // that never forms a particular edge is otherwise indistinguishable
    // from one whose member simply has nothing to say. Debug, because a
    // roster churning through unreachable members would repeat it.
    this.#peer.connect(id, { room: this.#room, data, instance }).catch((error) => {
      this.#peer.log.debug({ err: error, event: 'mesh.dial', room: this.#room, peer: id });
    });
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
    const onOpen = () => {
      // The edge is back: whatever was waiting to dial it again is over.
      this.#settle(link.id);
      void this.emit('join', { id: link.id, data: data ?? link.data }).catch((e) => this.#peer.escalate(e, this));
    };
    if (link.open) onOpen();
    else link.once('open', onOpen);
    link.once('close', (closure) => {
      link.off('open', onOpen);
      // Dropped by this mesh (a leave, a detach): not its member any more.
      if (this.#members.get(link.id) !== link) return;
      // A member that was only `away` — its signaling gone, the link all
      // that was left of it — is out of the room once the link is too.
      if (this.#away.has(link.id)) this.#forget(link.id);
      this.#drop(link, true);
      // Ended by an application — `link.close()` on either side, a goodbye:
      // that was a decision, and it used to be undone by a redial within
      // half a second, the member back in `peers` with a new `join` (the side
      // that heard the goodbye redialled too). A link that ended by itself —
      // a failure, a redial budget run out, a refusal — is dialled again.
      if (closure?.reason === 'goodbye') return;
      // The link ended by itself and the member is still in the room: the
      // edge is dialled again.
      this.#again(link.id);
    });
    void this.emit('link', link).catch((error) => this.#peer.escalate(error, this));
  }

  // Off the roster: out of the room, so nothing is dialled for it again.
  #forget(id) {
    this.#roster.delete(id);
    this.#settle(id);
  }

  #settle(id) {
    const pending = this.#relinks.get(id);
    if (pending === undefined) return;
    clearTimeout(pending.timer);
    this.#relinks.delete(id);
  }

  // The edge to a member still on the roster, dialled again after a pause
  // that grows to `maxDelay` and stays there. No count: a member that
  // refuses, or cannot be reached, is asked again once a minute for as long
  // as it is in the room — and the application is told, once per outage,
  // when the pace has reached that floor ('unreachable').
  #again(id) {
    if (this.#left || this.#relink === null || !this.#roster.has(id)) return;
    const pending = this.#relinks.get(id) ?? { timer: null, attempt: 0, said: false };
    const relink = this.#relink;
    const delay = backoffDelay({ ...relink, attempt: pending.attempt });
    if (!pending.said && relink.minDelay * 2 ** pending.attempt >= relink.maxDelay) {
      pending.said = true;
      this.#peer.log.warn({ event: 'mesh.unreachable', room: this.#room, peer: id, attempts: pending.attempt });
      void this.emit('unreachable', { id, attempts: pending.attempt }).catch((e) => this.#peer.escalate(e, this));
    }
    pending.attempt++;
    clearTimeout(pending.timer);
    pending.timer = setTimeout(() => {
      pending.timer = null;
      const listed = this.#roster.get(id);
      if (this.#left || listed === undefined) return void this.#relinks.delete(id);
      this.#peer
        .connect(id, { room: this.#room, data: listed.data, instance: listed.instance })
        // Never linked: connect() rejected before there was a link to hear
        // a close from (a link that was made says so through its 'close').
        .catch((error) => {
          this.#peer.log.debug({ err: error, event: 'mesh.dial', room: this.#room, peer: id });
          const waiting = this.#relinks.get(id);
          if (waiting !== undefined && waiting.timer === null && !this.#members.has(id)) this.#again(id);
        });
      const link = this.#peer.link(id);
      if (link) this.#adopt(link, listed.data);
    }, delay);
    pending.timer.unref?.();
    this.#relinks.set(id, pending);
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
