'use strict';

// WrpcPeer: one wrpc peer — a router others can call, a signaler to find
// them through, an RTC adapter to reach them with. It keeps the links (one
// PeerLink per remote peer — peerLink.js), reads the signals, holds a peer
// whose accept() has not answered, tracks incarnations, and owns the
// assertion layer (stamping outbound descriptions, verifying inbound ones).
//
// Roles are decided by id order alone: the peer with the LOWER id is the
// RtcLink initiator (it offers, it restarts ICE, it redials), the other is
// the polite responder. connect() works from either side — a responder
// that wants a link sends the initiator a 'connect' knock over signaling,
// and the initiator dials. So there is never an offer glare by
// construction, and a responder recovering from a failed link asks the
// same way.
//
// What a PeerLink and a Mesh need of the peer they belong to is handed to
// them as a PORT — closures over this class's private state — rather than
// the peer itself: `signal`, `stamp`, `verifyDescription`, `released` and
// `held` used to be public methods marked @internal, absent from the types
// and callable all the same.

const { Emitter } = require('../utils.js');
const { createLoggerWriter } = require('../logging.js');
const { createServerTelemetry } = require('../telemetry/server.js');
const { isRtcAdapter, createW3cAdapter } = require('./port.js');
const { RtcLink, DEFAULT_CHANNELS, normalizeChannels } = require('./link.js');
const { normalizeWaterMarks, normalizeBackpressure } = require('./transport.js');
const { PeerLink, normalizeRedial, REDIAL } = require('./peerLink.js');

// Signals held for a peer whose accept() has not answered yet; past it
// they are dropped, said once.
const MAX_PENDING_SIGNALS = 64;
// How long a peer that said goodbye ignores a knock or an offer from the
// incarnation it said it to: the other side's channels close before the
// goodbye — which goes through the signaling server — reaches it, and the
// redial that starts meanwhile would re-open what the application ended.
// A goodbye is one signaling hop; this is many.
const GOODBYE_HOLD = 5000;
const { PeerHost } = require('./host.js');
const { isSignaler, isSignalMessage } = require('./signaler.js');
const { isPeerId } = require('./ids.js');
const { createAssertionVerifier, sdpFingerprint, isAssertion, AssertionError } = require('./assertions.js');
const { normalizeCompression } = require('../compression/index.js');

class WrpcPeer extends Emitter {
  #signaler;
  #adapter;
  #configuration;
  #channels;
  #host = null;
  #router;
  #clientOptions;
  #hostOptions;
  #framing;
  #compression = null;
  #connectTimeout;
  #restartTimeout;
  #redial;
  #accept;
  #verifier = null;
  // The signaling server's clock minus ours, from our own assertions' iat:
  // the common reference both peers check `exp` against.
  #clock = 0;
  #log;
  #otel;
  #links = new Map();
  // Peers this side said goodbye to, for GOODBYE_HOLD: id -> { instance, until }.
  #goodbyes = new Map();
  // Signals for a peer whose accept() is still pending.
  #pending = new Map();
  #meshes = new Map();
  #started = null;
  #closed = false;
  // What a PeerLink is given instead of this peer (peerLink.js), and a Mesh.
  #linkPort;
  #meshPort;
  // Contained: a signal whose handling rejects (a link refusing a
  // description) used to be an unhandled rejection.
  #onSignal = (event) => void this.#receive(event).catch((error) => this.escalate(error));
  #onReset = (event) => void this.#reset(event);
  #onReplaced = (event) => void this.#replaced(event);

  constructor(options = {}) {
    super();
    const {
      router = null,
      signaler,
      rtc = null,
      configuration = {},
      iceServers,
      channels = {},
      client = {},
      host = {},
      framing = {},
      compression = null,
      connectTimeout,
      restartTimeout,
      redial = {},
      accept = null,
      assertions = null,
      logger = false,
      telemetry = null,
    } = options;
    if (!isSignaler(signaler)) throw new TypeError('WrpcPeer: options.signaler must satisfy the Signaler contract');
    if (assertions !== null && (typeof assertions !== 'object' || Array.isArray(assertions))) {
      throw new TypeError('WrpcPeer: options.assertions must be an object');
    }
    const adapter = rtc ?? createW3cAdapter();
    if (!isRtcAdapter(adapter)) throw new TypeError('WrpcPeer: options.rtc must satisfy the RtcAdapter contract');
    if (accept !== null && typeof accept !== 'function') {
      throw new TypeError('WrpcPeer: options.accept must be a function');
    }
    if (typeof client !== 'object' || client === null) {
      throw new TypeError('WrpcPeer: options.client must be an object');
    }
    if (typeof host !== 'object' || host === null) throw new TypeError('WrpcPeer: options.host must be an object');
    if (assertions !== null) {
      if (typeof signaler.assert !== 'function') {
        throw new TypeError('WrpcPeer: options.assertions needs a signaler with assert() — the server must issue them');
      }
      const keys = assertions.keys ?? (typeof signaler.keys === 'function' ? () => signaler.keys() : null);
      if (keys === null) {
        throw new TypeError('WrpcPeer: options.assertions.keys is required when the signaler has no keys()');
      }
      this.#verifier = createAssertionVerifier({
        keys,
        issuer: assertions.issuer ?? null,
        ...(assertions.refreshInterval === undefined ? {} : { refreshInterval: assertions.refreshInterval }),
        ...(assertions.maxAge === undefined ? {} : { maxAge: assertions.maxAge }),
      });
    }
    if (host.trust === 'assertion' && this.#verifier === null) {
      throw new TypeError("WrpcPeer: host.trust 'assertion' needs options.assertions");
    }
    this.#signaler = signaler;
    this.#adapter = adapter;
    this.#configuration = iceServers ? { ...configuration, iceServers } : configuration;
    this.#channels = normalizeChannels({ ...DEFAULT_CHANNELS, ...channels });
    this.#router = router;
    this.#log = createLoggerWriter(logger).child({ component: 'peer' });
    this.#clientOptions = client;
    const { highWaterMark, lowWaterMark, maxBackpressure, ...hostRest } = host;
    // Checked here, at construction — not at the first link, where a bad
    // mark used to surface.
    const marks = normalizeWaterMarks(highWaterMark, lowWaterMark, 'WrpcPeer: options.host');
    const water = {
      highWaterMark: marks.high,
      lowWaterMark: marks.low,
      maxBackpressure: normalizeBackpressure(maxBackpressure, 'WrpcPeer: options.host'),
    };
    // Per-message compression on every link, both directions: announced in
    // each description this peer sends, applied on a link whose peer named
    // the same codec — a peer without it is served plain.
    this.#compression = normalizeCompression(compression, 'WrpcPeer: options');
    this.#hostOptions = { water, framing, compression: this.#compression };
    this.#framing = framing;
    this.#connectTimeout = connectTimeout;
    this.#restartTimeout = restartTimeout;
    this.#redial = normalizeRedial(redial);
    this.#accept = accept;
    if (router) this.#host = new PeerHost({ logger, telemetry, ...hostRest, router });
    // One writer for the peer: the host's when there is one (so its spans,
    // connection gauge and the link instruments share instruments), its
    // own otherwise — a client-only peer still has links to count.
    this.#otel = this.#host ? this.#host.otel : createServerTelemetry(telemetry);
    const escalate = (error, source) => this.escalate(error, source);
    this.#linkPort = {
      assertions: this.#verifier !== null,
      signal: (to, message, room) => this.#signal(to, message, room),
      verify: (from, message, pinned, claims) => this.#verifyDescription(from, message, pinned, claims),
      released: (link) => this.#released(link),
      escalate,
    };
    const peer = this;
    this.#meshPort = {
      get id() {
        return peer.id;
      },
      signaler,
      log: this.#log,
      host: this.#host,
      connect: (id, options) => this.connect(id, options),
      link: (id) => this.#links.get(id),
      held: (id, except) => this.#held(id, except),
      escalate,
      on: (name, listener) => this.on(name, listener),
      off: (name, listener) => this.off(name, listener),
    };
    // Listening from the start: a peer that never called start() itself
    // still answers a knock — start() runs on the first signal.
    signaler.on('signal', this.#onSignal);
    signaler.on('reset', this.#onReset);
    signaler.on('replaced', this.#onReplaced);
  }

  /** This peer's id: the signaler's, once start() resolved. */
  get id() {
    return this.#signaler.id;
  }

  get signaler() {
    return this.#signaler;
  }

  // The writer this peer was built with, childed with `component: 'peer'`.
  // A Mesh built on top reports through it rather than carrying a logger
  // option of its own — same seam as `Client.log` and `RpcServer.log`.
  get log() {
    return this.#log;
  }

  get host() {
    return this.#host;
  }

  get router() {
    return this.#host ? this.#host.router : this.#router;
  }

  get channels() {
    return this.#channels;
  }

  /** True when this peer issues and verifies trust assertions. */
  get assertions() {
    return this.#verifier !== null;
  }

  /** Every link, keyed by remote id (a copy). */
  get links() {
    return new Map(this.#links);
  }

  /** The link to `id`, or undefined. */
  link(id) {
    return this.#links.get(id);
  }

  /** Identifies through the signaler and starts listening for peers. */
  start() {
    if (this.#closed) return Promise.reject(new Error('WrpcPeer is closed'));
    return (this.#started ??= this.#signaler.ready().catch((error) => {
      this.#started = null;
      throw error;
    }));
  }

  // What every description this peer sends announces: its codecs, and
  // which of its halves read framed messages (`f`: 1 the host, 2 the
  // client) — each end of a link sends a frame only where the other said so.
  #caps() {
    const client = this.#clientOptions;
    const f = (this.#host?.revision === 2 ? 1 : 0) | (client.attachments !== false && !client.codec ? 2 : 0);
    return this.#compression === null ? { f } : { enc: this.#compression.ids, f };
  }

  /**
   * A link to `remoteId`, dialled from either side; idempotent while one
   * exists — unless `options.instance` names another incarnation of the
   * id than the one linked, which abandons the stale link and dials the new
   * endpoint. Resolves with the PeerLink once both directions are up.
   */
  async connect(remoteId, options = {}) {
    if (!isPeerId(remoteId)) {
      throw new TypeError('WrpcPeer.connect: remoteId must be a non-empty string of at most 256 characters');
    }
    await this.start();
    if (remoteId === this.id) throw new Error('WrpcPeer.connect: cannot connect to self');
    const instance = isPeerId(options.instance) ? options.instance : null;
    // Dialling it is this side changing its mind about a goodbye.
    this.#goodbyes.delete(remoteId);
    const existing = this.#links.get(remoteId);
    if (existing && this.#current(existing, instance)) return existing.ready();
    const link = this.#create(remoteId, options.room ?? null, options.data ?? null, instance);
    if (link !== existing) {
      try {
        link.start({ knock: true });
      } catch (error) {
        // A dial that could not be made: no link to keep.
        link.abandon();
        throw error;
      }
      // An inbound open for the same peer still in its accept(): this link
      // is the one. What it queued — the peer's knock, its offer, the
      // candidates — is this link's now, and the accept, when it answers,
      // finds nothing left to open. A second PeerLink used to be made for
      // it, over this one, which leaked.
      const pending = this.#pending.get(remoteId);
      if (pending) {
        this.#pending.delete(remoteId);
        for (const queued of pending) {
          if (queued.type === 'connect') link.knocked();
          else if (queued.type === 'candidate' || queued.description?.type === 'offer') {
            link.receive(queued).catch((error) => this.escalate(error, link));
          }
        }
      }
    }
    return link.ready();
  }

  /** Joins a signaling room and links with everyone in it: a Mesh. */
  join(room, options = {}) {
    const { Mesh } = require('./mesh.js');
    if (typeof room !== 'string' || room.length === 0) {
      throw new TypeError('WrpcPeer.join: room must be a non-empty string');
    }
    const existing = this.#meshes.get(room);
    if (existing) return existing;
    const mesh = new Mesh(this.#meshPort, room, options);
    this.#meshes.set(room, mesh);
    mesh.once('left', () => this.#meshes.delete(room));
    return mesh;
  }

  /** The Mesh for `room`, if joined. */
  mesh(room) {
    return this.#meshes.get(room);
  }

  /** Closes every link and mesh; the signaler is the owner's to close. */
  close() {
    if (this.#closed) return;
    this.#closed = true;
    this.#signaler.off('signal', this.#onSignal);
    this.#signaler.off('reset', this.#onReset);
    this.#signaler.off('replaced', this.#onReplaced);
    for (const mesh of [...this.#meshes.values()]) mesh.detach();
    this.#meshes.clear();
    for (const link of [...this.#links.values()]) link.close();
    this.#pending.clear();
    void this.emit('close').catch((error) => this.escalate(error));
  }

  // ---- what the ports are made of

  // A signal to a peer; a signaler that throws or rejects is this peer's
  // error, never the caller's.
  #signal(to, message, room) {
    try {
      const result = this.#signaler.send(to, message, room === null ? undefined : { room });
      if (result && typeof result.then === 'function') result.catch((error) => this.escalate(error));
    } catch (error) {
      this.escalate(error);
    }
  }

  // An outbound description gets this peer's assertion for the certificate
  // it declares — one call to the signaling server per dial.
  async #stamp(message) {
    if (message.type !== 'description') return message;
    const fingerprint = sdpFingerprint(message.description?.sdp);
    if (fingerprint === null) {
      throw new Error('WrpcPeer: the local description declares no single certificate fingerprint');
    }
    const result = await this.#signaler.assert({ fingerprint });
    if (!isAssertion(result?.assertion)) throw new TypeError('signaler.assert() answered without an assertion');
    if (typeof result.iat === 'number') this.#clock = result.iat * 1000 - Date.now();
    return { ...message, assertion: result.assertion };
  }

  // The claims behind an inbound description from `from`: the signature is
  // checked when the description's certificate differs from the one pinned
  // on the link (a new pc — dial or redial); an ICE restart on the same pc
  // is a string compare.
  async #verifyDescription(from, message, pinned = null, claims = null) {
    const sdp = message.description?.sdp;
    const fingerprint = sdpFingerprint(sdp);
    // A description on a pc whose fingerprint is already pinned needs no
    // second verification, and is not counted: it is not an assertion check.
    if (fingerprint !== null && fingerprint === pinned) return { fingerprint, claims };
    // A failure here is a SECURITY signal — a peer presenting a token that
    // does not bind to the DTLS fingerprint of the connection it sent — and
    // it had no metric at all, so a campaign of them was invisible.
    if (!isAssertion(message.assertion)) {
      this.#otel.recordRtcAssertion('missing');
      throw new AssertionError('assertion: missing', 'missing');
    }
    let verified;
    try {
      verified = await this.#verifier.verify(message.assertion, { from, sdp, now: Date.now() + this.#clock });
    } catch (error) {
      // The verifier's refusal code is the outcome — signature, fingerprint,
      // subject (a substitution), expired, kid, issuer, malformed (an
      // operational fault). It used to read a `reason` no AssertionError
      // ever had, so every refusal counted as 'invalid'.
      this.#otel.recordRtcAssertion(error instanceof AssertionError ? error.code : 'invalid');
      throw error;
    }
    this.#otel.recordRtcAssertion('ok');
    return { fingerprint, claims: verified };
  }

  // A link closed: forget it — and, when this side said goodbye, remember
  // whom to for GOODBYE_HOLD (see #receive). Swept as it grows: every entry
  // is younger than the hold.
  #released(link) {
    if (this.#links.get(link.id) === link) this.#links.delete(link.id);
    const closure = link.link.closure;
    if (closure?.reason !== 'goodbye' || closure.remote) return;
    const now = Date.now();
    for (const [id, said] of this.#goodbyes) if (said.until <= now) this.#goodbyes.delete(id);
    this.#goodbyes.set(link.id, { instance: link.instance, until: now + GOODBYE_HOLD });
  }

  // Whether this side said goodbye to that incarnation of `id` a moment ago.
  // A signaler that names no instances matches by id alone.
  #saidGoodbye(id, instance) {
    const said = this.#goodbyes.get(id);
    if (said === undefined) return false;
    if (said.until <= Date.now()) {
      this.#goodbyes.delete(id);
      return false;
    }
    return said.instance === null || instance === null || said.instance === instance;
  }

  /** A link (or mesh) with nobody listening for its error: the peer's 'error', or a log line. */
  escalate(error, source = null) {
    if (this.listenerCount('error') > 0) return void this.emit('error', error, source).catch(() => {});
    this.#log.error({ err: error, event: 'rtc.peer.unhandled' });
  }

  // Whether any mesh other than `except` holds `id`.
  #held(id, except) {
    for (const mesh of this.#meshes.values()) if (mesh !== except && mesh.has(id)) return true;
    return false;
  }

  // Whether `existing` is still the link to its peer: it is, unless
  // `instance` says the id now lives at another endpoint — then the stale
  // link is abandoned (no goodbye: it would land on the new one) and the
  // answer is false. A link that never learned an instance adopts the
  // first it sees.
  #current(existing, instance) {
    if (instance === null) return true;
    if (existing.instance === null) {
      existing.adopt(instance);
      return true;
    }
    if (existing.instance === instance) return true;
    this.#log.info({ event: 'rtc.peer.incarnation', peer: existing.id, previous: existing.instance, instance });
    existing.abandon();
    return false;
  }

  #create(remoteId, room, data, instance = null, verified = null) {
    // A link that is not closed is THE link: a second one over it would
    // leak the first, and take its peer's signals.
    const current = this.#links.get(remoteId);
    if (current !== undefined && current.state !== 'closed') {
      this.#log.error({ event: 'rtc.peer.duplicate', peer: remoteId, state: current.state });
      return current;
    }
    const localId = this.id;
    const relay = (message) => this.#signal(remoteId, message, room);
    // With assertions, a description waits for its token — one round trip
    // to the server — and the candidates that follow it wait their turn,
    // so none overtakes the description they belong to.
    let chain = Promise.resolve();
    const stamped = (message) => {
      const next = chain.then(() => this.#stamp(message)).then(relay);
      chain = next.catch(() => {});
      return next;
    };
    const link = new RtcLink({
      localId,
      remoteId,
      adapter: this.#adapter,
      configuration: this.#configuration,
      channels: this.#channels,
      connectTimeout: this.#connectTimeout,
      restartTimeout: this.#restartTimeout,
      log: this.#log,
      caps: this.#caps(),
      signal: this.#verifier === null ? relay : stamped,
    });
    const peerLink = new PeerLink(this.#linkPort, {
      id: remoteId,
      instance,
      room,
      data,
      claims: verified ? verified.claims : null,
      fingerprint: verified ? verified.fingerprint : null,
      link,
      host: this.#host,
      hostOptions: this.#hostOptions,
      client: this.#clientOptions,
      framing: this.#framing,
      redial: this.#redial,
      connectTimeout: this.#connectTimeout,
      log: this.#log.child({ peer: remoteId }),
      otel: this.#otel,
    });
    this.#links.set(remoteId, peerLink);
    void this.emit('link', peerLink).catch((error) => this.escalate(error));
    return peerLink;
  }

  async #receive({ from, instance, room, message }) {
    if (this.#closed || !isPeerId(from) || !isSignalMessage(message)) return;
    try {
      await this.start();
    } catch (error) {
      return void this.escalate(error);
    }
    if (this.#closed || this.id === null || from === this.id) return;
    const incarnation = isPeerId(instance) ? instance : null;
    const existing = this.#links.get(from);
    const stale = existing !== undefined && !this.#current(existing, incarnation);
    if (existing && !stale) {
      if (message.type === 'connect') return void existing.knocked();
      return void (await existing.receive(message));
    }
    // Unknown peer: a knock or an offer opens a link, once accept() agrees;
    // a candidate or close for a link we do not have is noise. Except after
    // a stale link went: its redial may already have reached the new
    // incarnation and this is that side's answer, so the initiator (lower
    // id) dials afresh on anything the newcomer says.
    const pending = this.#pending.get(from);
    if (pending) {
      // Bounded: a peer whose accept() is thinking is not a peer who may
      // fill memory with candidates meanwhile. Said once per queue.
      if (pending.length < MAX_PENDING_SIGNALS) pending.push(message);
      else if (!pending.overflowed) {
        pending.overflowed = true;
        this.#log.warn({ event: 'rtc.signal.overflow', peer: from, what: 'signals', max: MAX_PENDING_SIGNALS });
      }
      return;
    }
    const opening =
      message.type === 'connect' ||
      (message.type === 'description' && message.description?.type === 'offer') ||
      (stale && message.type !== 'close' && this.id < from);
    if (!opening) return;
    // The other side's redial, begun before our goodbye reached it — its
    // channels closed first — would undo an application's close: a link it
    // ended came back within half a second. The goodbye is on its way and
    // ends that redial; the knock or offer is dropped.
    if (this.#saidGoodbye(from, incarnation)) {
      return void this.#log.debug({ event: 'rtc.signal.goodbye', peer: from, type: message.type });
    }
    const queue = [message];
    this.#pending.set(from, queue);
    // Verification is protocol, accept() is policy: an offer's assertion is
    // checked first, and the hook sees the verified claims. A knock carries
    // no description; its claims arrive with the answer, before the host
    // half attaches.
    let verified = null;
    if (this.#verifier !== null && message.type === 'description') {
      try {
        verified = await this.#verifyDescription(from, message);
      } catch (error) {
        if (this.#pending.get(from) !== queue) return;
        this.#pending.delete(from);
        if (this.#closed) return;
        this.#log.warn({ event: 'rtc.peer.refused', peer: from, room, reason: error.code ?? 'assertion', err: error });
        this.#signal(from, { type: 'close', reason: 'refused' }, room ?? null);
        return;
      }
    }
    let accepted = true;
    try {
      const about = { instance: incarnation, claims: verified ? verified.claims : null };
      accepted = this.#accept === null ? true : await this.#accept(from, room ?? null, about);
    } catch (error) {
      accepted = false;
      this.escalate(error);
    }
    // connect() took this queue over while accept() was pending, or close()
    // cleared it: nothing is left here to open.
    if (this.#pending.get(from) !== queue) return;
    this.#pending.delete(from);
    if (this.#closed) return;
    if (accepted !== true) {
      this.#log.info({ event: 'rtc.peer.refused', peer: from, room, reason: 'accept' });
      if (message.type !== 'close') this.#signal(from, { type: 'close', reason: 'refused' }, room ?? null);
      return;
    }
    const link = this.#create(from, room ?? null, null, incarnation, verified);
    try {
      link.start();
    } catch (error) {
      link.abandon();
      return void this.escalate(error, link);
    }
    for (const queued of queue) {
      if (queued.type === 'connect') link.knocked();
      // A stale-incarnation answer or candidate belongs to the link that
      // went; the fresh dial above is what reaches the newcomer.
      else if (!stale || queued.type !== 'description' || queued.description?.type === 'offer') {
        await link.receive(queued);
      }
    }
  }

  // The signaler re-identified. Under a stable identity the id is the same
  // and the links, which never needed signaling to keep working, stay; a
  // Mesh re-adopts them from the rosters the reset carries. When the id
  // changed, every link was made under the old one and the remote side
  // keys it by that: close them all, without a goodbye.
  #reset(event) {
    const changed = event?.id !== event?.previous;
    this.#log.warn({ event: 'rtc.peer.reset', id: event?.id, previous: event?.previous, changed });
    if (changed) for (const link of [...this.#links.values()]) link.abandon();
    void this.emit('reset', event).catch((error) => this.escalate(error));
  }

  // A newer connection took this peer id: this incarnation is over. The
  // remote peers drop their links to it as the roster says so; here the
  // links are abandoned (a goodbye would come from a stranger) and the peer
  // closes. The owner decides whether a page starts a new one.
  #replaced(event) {
    if (this.#closed) return;
    this.#log.warn({ event: 'rtc.peer.replaced', id: event?.id });
    for (const link of [...this.#links.values()]) link.abandon();
    void this.emit('replaced', event).catch((error) => this.escalate(error));
    this.close();
  }
}

module.exports = { WrpcPeer, PeerLink, normalizeRedial, REDIAL };
