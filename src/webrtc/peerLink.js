'use strict';

// PeerLink: one connected peer, both directions — the RtcLink underneath,
// the WrpcClient that calls the remote's router over it, and the host-side
// Client the remote is to this peer's own router.
//
//   peer A                                     peer B
//   link.remote (WrpcClient) ──channel 0──▶ PeerHost.attach → Client
//   PeerHost.attach → Client ◀──channel 1── link.remote (WrpcClient)
//
// Failure: the RtcLink reports 'failed' (ICE did not come back inside the
// restart window); the client half's transport went 'close' and its
// WrpcClient runs the ordinary backoff/reconnect cycle, whose open() waits
// for the link to be connected again; the host half's Client is destroyed
// and re-attached on the next 'open'. The PeerLink drives the link: the
// initiator redials with backoff, the responder knocks with backoff, both
// give up after `redial.retries` and close.
//
// A PeerLink never holds its WrpcPeer. What it needs of it is a PORT, built
// by the peer over its own private state and handed in at construction:
//
//   assertions                        whether inbound descriptions are verified
//   signal(to, message, room)         a signal to a peer, failures contained
//   verify(from, message, pinned, claims) -> { fingerprint, claims }
//   released(link)                    this link closed: forget it
//   escalate(error, source)           an error nobody on the link listens for
//
// Those used to be public methods of WrpcPeer marked @internal — absent
// from the types, callable all the same, and able to desynchronize it.

const { Emitter, backoffDelay } = require('../utils.js');
const { WrpcClient } = require('../client/core.js');
const { ClientRtcTransport, RtcPeerTransport } = require('./transport.js');
const { deferred } = require('./ids.js');

const REDIAL = { retries: 5, minDelay: 500, maxDelay: 10_000, factor: 2, jitter: true };
// The remote WrpcClient's reconnect: quick, since the link itself carries
// the real backoff, and unbounded, since the PeerLink decides when a link
// is over.
const CLIENT_RECONNECT = { minDelay: 100, maxDelay: 2_000, jitter: true, retries: Infinity };

const normalizeRedial = (redial) => {
  if (redial === false) return { ...REDIAL, retries: 0 };
  const merged = { ...REDIAL, ...(redial && typeof redial === 'object' ? redial : {}) };
  if (!(merged.retries >= 0)) merged.retries = REDIAL.retries;
  if (!(merged.minDelay > 0)) merged.minDelay = REDIAL.minDelay;
  if (!(merged.maxDelay >= merged.minDelay)) merged.maxDelay = merged.minDelay;
  return merged;
};

/** One peer, both directions. Constructed by WrpcPeer; never directly. */
class PeerLink extends Emitter {
  #port;
  #id;
  #instance;
  #room;
  #data;
  #link;
  #remote;
  #host;
  #hostOptions;
  #client = null;
  #rooms = new Set();
  #claims;
  #fingerprint;
  #inbound = Promise.resolve();
  #state = 'connecting';
  #everOpen = false;
  #redial;
  #redialAttempt = 0;
  #redialTimer = null;
  #connectTimeout;
  #opened;
  #log;
  #otel;
  #counted = false;
  #unbind = [];

  constructor(port, options) {
    super();
    const { id, instance, room, data, link, host, hostOptions, client, framing, redial, log, otel } = options;
    const { compression } = hostOptions;
    this.#connectTimeout = options.connectTimeout ?? 0;
    this.#port = port;
    this.#id = id;
    this.#instance = instance;
    this.#claims = options.claims ?? null;
    this.#fingerprint = options.fingerprint ?? null;
    this.#room = room;
    this.#data = data;
    this.#link = link;
    this.#host = host;
    this.#hostOptions = hostOptions;
    this.#redial = redial;
    this.#log = log;
    this.#otel = otel;
    this.#opened = deferred();
    const transport = new ClientRtcTransport(`webrtc:${id}`, { link, framing, compression, ...hostOptions.water });
    this.#remote = new WrpcClient(`webrtc:${id}`, transport, {
      logger: false,
      ...client,
      reconnect: client.reconnect === false ? false : { ...CLIENT_RECONNECT, ...(client.reconnect ?? {}) },
    });
    this.#bindAll();
  }

  /** The remote peer's id. */
  get id() {
    return this.#id;
  }

  /**
   * The remote peer's incarnation — the `instance` its signaler carries —
   * once known, else null. A signal from the same id under another instance
   * is another endpoint, and this link is stale.
   */
  get instance() {
    return this.#instance;
  }

  /**
   * The remote peer's verified assertion claims (`sub`, `exp`, `fp`, and
   * whatever the signaling server added), or null: no assertions
   * configured, or — on the dialling side — none seen yet.
   */
  get claims() {
    return this.#claims;
  }

  /** The signaling room this link was made in, or null. */
  get room() {
    return this.#room;
  }

  /** The remote peer's roster data, when known. */
  get data() {
    return this.#data;
  }

  /** True when this side dialled (lower id). */
  get initiator() {
    return this.#link.initiator;
  }

  /** 'connecting' | 'open' | 'reconnecting' | 'closed' */
  get state() {
    return this.#state;
  }

  /** The RtcLink underneath. */
  get link() {
    return this.#link;
  }

  /** The WrpcClient that calls the remote peer's router. */
  get remote() {
    return this.#remote;
  }

  /** The remote's scaffolded api (after load()). */
  get api() {
    return this.#remote.api;
  }

  /** The host-side Client the remote peer is to this router; null while down. */
  get client() {
    return this.#client;
  }

  /** The host-side rooms this link is kept in across redials (a copy). */
  get rooms() {
    return new Set(this.#rooms);
  }

  /** True once both directions are up. */
  get open() {
    return this.#state === 'open';
  }

  /** Resolves when both directions are up; rejects when the link closes first. */
  ready() {
    return this.#opened.promise;
  }

  // ---- the remote's api, delegated

  load(...units) {
    return this.#remote.load(...units);
  }

  call(method, args, options) {
    return this.#remote.call(method, args, options);
  }

  /** Answers the remote peer's asks; replaces an earlier responder of that name. */
  respond(name, handler) {
    this.#remote.unrespond(name);
    this.#remote.respond(name, handler);
  }

  unrespond(name) {
    return this.#remote.unrespond(name);
  }

  // ---- the host side, addressed

  /** An event to the remote peer, through this router's Client for it. */
  send(name, data) {
    this.#hostClient('send').sendEvent(name, data);
  }

  /** Asks the remote peer; answered by its remote.respond(). */
  ask(name, data, options) {
    return this.#hostClient('ask').ask(name, data, options);
  }

  /** A binary stream to the remote peer. */
  createStream(name, size) {
    return this.#hostClient('createStream').createStream(name, size);
  }

  /** Keeps this link's host Client in `room`, now and after every redial. */
  join(room) {
    this.#rooms.add(room);
    if (this.#client) this.#client.join(room);
  }

  leave(room) {
    this.#rooms.delete(room);
    if (this.#client) this.#client.leave(room);
  }

  /** Goodbye: the peer is told, both directions end, no redial. */
  close() {
    if (this.#state === 'closed') return;
    this.#link.close();
  }

  // ---- lifecycle (WrpcPeer drives these)

  /**
   * @internal Dials. The initiator offers; a responder that asked for the
   * link itself (connect() from the higher id) knocks so the initiator
   * dials — one answering an offer already arriving does not.
   */
  start({ knock = false } = {}) {
    this.#link.start();
    if (knock && !this.initiator) this.#knock();
    // The first open: the client half waits for the link; a failure before
    // the first open is the redial cycle's to retry, and the client is only
    // opened once the link is.
    void this.#link.waitOpen().then(
      () => this.#openClient(),
      () => {},
    );
  }

  /**
   * @internal A knock from the responder: it wants the link and has none.
   * The initiator dials again when its own link failed — and when its link
   * looks CONNECTED but once opened: the responder's half is gone and this
   * side's pc never noticed (an asymmetric failure), so the link is failed
   * here for the redial cycle to rebuild. A dial under way is left to
   * finish; a responder never dials.
   */
  knocked() {
    if (!this.initiator) return;
    const state = this.#link.state;
    if (state === 'failed') return void this.#redialNow();
    if (state === 'connected' && this.#everOpen) this.#link.fail(new Error('the peer knocked on a link it lost'));
  }

  /**
   * @internal A signal from the remote peer. Serialized: a description is
   * verified (asynchronously) before the link applies it, and the next
   * signal must not overtake it.
   */
  receive(message) {
    const run = () => this.#receive(message);
    this.#inbound = this.#inbound.then(run, run);
    return this.#inbound;
  }

  async #receive(message) {
    if (this.#state === 'closed') return;
    if (message?.type === 'description' && this.#port.assertions) {
      try {
        const verified = await this.#port.verify(this.#id, message, this.#fingerprint, this.#claims);
        this.#fingerprint = verified.fingerprint;
        this.#claims = verified.claims;
      } catch (error) {
        // Not who the signaling said, or not provably so: the link ends
        // here with a goodbye, before the description touches the pc.
        this.#log.warn({ event: 'rtc.peer.refused', peer: this.#id, reason: error.code ?? 'assertion', err: error });
        this.#link.close('refused');
        return;
      }
    }
    return this.#link.receive(message);
  }

  /** @internal The remote's instance, learned from its first signal or roster entry. */
  adopt(instance) {
    if (this.#instance === null && typeof instance === 'string') this.#instance = instance;
  }

  /**
   * @internal Closes without a goodbye: after a signaling reset this side's
   * id changed, or the remote came back as another incarnation, so a
   * 'close' sent now would reach the peer from a stranger — or worse, land
   * on the fresh link already being made. The peer learns through the
   * roster (leave/join) or through ICE.
   */
  abandon() {
    this.#link.abandon();
  }

  #hostClient(what) {
    if (!this.#client) throw new Error(`PeerLink.${what}: the link to '${this.#id}' is not open`);
    return this.#client;
  }

  #bindAll() {
    const link = this.#link;
    const remote = this.#remote;
    const on = (emitter, name, fn) => {
      emitter.on(name, fn);
      this.#unbind.push(() => emitter.off(name, fn));
    };
    on(link, 'open', () => this.#attachHost());
    on(link, 'state', (state) => {
      if (state === 'failed') this.#onFailed();
      else if (state === 'closed') this.#onClosed();
    });
    on(link, 'error', (error) => this.#error(error));
    on(link, 'restart', ({ outcome }) => this.#otel.recordRtcRestart(outcome));
    on(remote, 'open', () => this.#onUp());
    on(remote, 'reconnect-failed', () => this.#link.close('gave-up'));
    // Bound for the client's whole life, not unbound on close: an open()
    // still in flight when the link closes settles afterwards, and an
    // unheard client error goes to the console.
    remote.on('error', (error) => {
      // While the link is down its reconnect attempts fail by design; only
      // an error on a live link is anyone's business.
      if (this.#state === 'open') this.#error(error);
      else this.#log.debug({ err: error, event: 'rtc.peer.client', state: this.#state });
    });
  }

  #attachHost() {
    if (!this.#host || this.#client) return;
    const transport = new RtcPeerTransport(this.#link, {
      peer: this.#id,
      framing: this.#hostOptions.framing,
      compression: this.#hostOptions.compression,
      ...this.#hostOptions.water,
      onError: (error) => this.#error(error),
    });
    const client = this.#host.attach(transport, {
      peer: this.#id,
      room: this.#room,
      data: this.#data,
      claims: this.#claims,
    });
    this.#client = client;
    for (const room of this.#rooms) client.join(room);
    transport.once('close', () => {
      if (this.#client === client) this.#client = null;
    });
    void this.emit('attach', client).catch((error) => this.#error(error));
  }

  async #openClient() {
    if (this.#state === 'closed') return;
    try {
      await this.#remote.open();
    } catch (error) {
      // The link went down between its 'open' and the client's: the redial
      // cycle owns the retry, and only a failure on a LIVE link is news.
      if (this.#state !== 'closed' && this.#link.state === 'connected') this.#error(error);
    }
  }

  #onUp() {
    if (this.#state === 'closed') return;
    this.#redialAttempt = 0;
    // A redial or knock still scheduled is for a failure that is over.
    clearTimeout(this.#redialTimer);
    this.#redialTimer = null;
    const first = !this.#everOpen;
    this.#everOpen = true;
    this.#setState('open');
    this.#count(1);
    if (first) {
      this.#opened.resolve(this);
      void this.emit('open').catch((error) => this.#error(error));
    } else {
      void this.emit('reconnect').catch((error) => this.#error(error));
    }
  }

  #onFailed() {
    if (this.#state === 'closed') return;
    this.#setState('reconnecting');
    this.#count(-1);
    if (this.#redialAttempt >= this.#redial.retries) {
      this.#log.warn({ event: 'rtc.peer.gave-up', attempts: this.#redialAttempt });
      // The initiator's goodbye ends a link nobody else can rebuild. A
      // responder gives up quietly: the initiator may still be redialling
      // on a longer backoff, and a 'close' from here used to end the link
      // it was about to rebuild.
      return void (this.initiator ? this.#link.close('gave-up') : this.#link.abandon('gave-up'));
    }
    const delay = backoffDelay({ ...this.#redial, attempt: this.#redialAttempt });
    this.#redialAttempt++;
    this.#log.info({ event: 'rtc.peer.redial', attempt: this.#redialAttempt, delay, initiator: this.initiator });
    clearTimeout(this.#redialTimer);
    this.#redialTimer = setTimeout(() => {
      this.#redialTimer = null;
      if (this.#state === 'closed') return;
      // Counted where the action happened, not where it was scheduled.
      if (this.initiator) {
        if (this.#redialNow()) this.#otel.recordRtcRedial(this.#role);
        return;
      }
      if (this.#link.state !== 'failed') return;
      this.#knock();
      this.#otel.recordRtcRedial(this.#role);
      // A responder whose initiator never answers fails again on the
      // connect timeout of nothing: re-armed once that window has passed,
      // not at once — each knock used to count an attempt within one
      // backoff step, the whole budget gone in milliseconds while the
      // initiator's dial had not even timed out.
      this.#redialTimer = setTimeout(() => {
        this.#redialTimer = null;
        if (this.#state !== 'closed' && this.#link.state === 'failed') this.#onFailed();
      }, this.#connectTimeout);
      this.#redialTimer.unref?.();
    }, delay);
  }

  #redialNow() {
    if (!this.#link.redial()) return false;
    void this.#link.waitOpen().then(
      () => this.#openClient(),
      () => {},
    );
    return true;
  }

  #knock() {
    this.#port.signal(this.#id, { type: 'connect' }, this.#room);
  }

  #onClosed() {
    if (this.#state === 'closed') return;
    this.#setState('closed');
    this.#count(-1);
    clearTimeout(this.#redialTimer);
    this.#redialTimer = null;
    for (const unbind of this.#unbind) unbind();
    this.#unbind = [];
    // The client's reconnect cycle stops here; its transport's close() on a
    // closed link is a no-op.
    this.#remote.close();
    this.#opened.reject(new Error(`PeerLink to '${this.#id}' closed`));
    this.#port.released(this);
    // Why the link ended — the goodbye's reason, or this side's own — is
    // what tells a refusal from a give-up from a goodbye on a dashboard.
    const closure = this.#link.closure;
    this.#otel.recordRtcClose(closure.reason, closure.remote);
    void this.emit('close', closure).catch((error) => this.#error(error));
  }

  get #role() {
    return this.initiator ? 'initiator' : 'responder';
  }

  // The open-links gauge: +1 when both directions come up, -1 once when
  // they go down, whatever the order of failure and close.
  #count(delta) {
    if (delta > 0 && this.#counted) return;
    if (delta < 0 && !this.#counted) return;
    this.#counted = delta > 0;
    this.#otel.recordRtcLink(delta, this.#role);
  }

  #setState(state) {
    if (this.#state === state) return;
    this.#state = state;
    void this.emit('state', state).catch((error) => this.#error(error));
  }

  #error(error) {
    this.#log.error({ err: error, event: 'rtc.peer.error', peer: this.#id });
    if (this.listenerCount('error') > 0) return void this.emit('error', error).catch(() => {});
    this.#port.escalate(error, this);
  }
}

module.exports = { PeerLink, normalizeRedial, REDIAL };
