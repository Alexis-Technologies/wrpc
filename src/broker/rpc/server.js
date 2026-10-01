'use strict';

// RPC over a broker, server side: `attachBrokerRpc(server, broker, { service })`.
//
// One service address, consumed as a competing group by every instance:
//
// - a `request` is answered exactly like a packet-mode HTTP POST — it IS
//   one: handed to RpcServer.handleHttpCall with its peer headers, so
//   batches, per-request clients, token-carrier sessions and meta behave
//   identically to HTTP, with no affinity between instances;
// - a `hello` opens a SESSION on whichever instance took it: the full
//   protocol (events, subscriptions, streams, cancellation) over this
//   instance's own inbox, one client attached through RpcServer.attach with
//   the hello's headers as its request.
//
// Sessions end on a `bye`, a frame-sequence gap, a send the broker refuses,
// or silence past `idleTimeout` (a client whose process vanished never says
// goodbye; its heartbeat is what keeps a live one inside the window).

const { ServerTransport } = require('../../rpc/serverTransport.js');
const { createLoggerWriter } = require('../../logging.js');
const { capabilityOf, brokerName } = require('../port.js');
const { toBytes, fingerprint } = require('../ids.js');
const { rpcOf } = require('../host.js');
const {
  HEADER_KIND,
  HEADER_INBOX,
  HEADER_REASON,
  HEADER_ENC,
  HEADER_TIME,
  KIND,
  serviceAddress,
  peerHeaders,
  seqOf,
  packetBody,
  frameBody,
  sessionFrame,
  sealFrame,
  openFrame,
} = require('./frames.js');
const { createBrokerSealing, HEADER_SEALED } = require('../sealing.js');
const { HEADER_LENGTH } = require('../../encryption/envelope.js');
const { DEFAULT_MAX_SKEW } = require('../../encryption/httpServer.js');
const {
  normalizeSyncCompression,
  headerNegotiator,
  maxMessageOf,
  encodeIfSmaller,
} = require('../../compression/sync.js');

const DEFAULT_IDLE_TIMEOUT = 90_000; // 3x the client's 30 s heartbeat
const DEFAULT_HIGH_WATER_MARK = 1024;
// Sessions one instance holds at once: every `hello` that reaches the
// service address costs a Client, a transport and a table entry until
// `idleTimeout`, so without a ceiling one sender could grow an instance
// without bound. Past it a hello is answered `bye` — the sender may try
// another instance, the group spreads them. 0 lifts the cap.
const DEFAULT_MAX_SESSIONS = 10_000;

// The server half of one session: what the core's Client writes to. The
// base supplies send()/error() over write().
class BrokerSessionTransport extends ServerTransport {
  kind = 'broker';
  connection = true;
  #direct;
  #peer;
  #session;
  #seq = 0;
  #unconfirmed = 0;
  #highWaterMark;
  #closed = false;
  #onFailure;
  // What both ends agreed to on hello/welcome — `{ encode, decode }` — or null.
  #compression;
  // The binding's sealing (../sealing.js) or null: compress, then seal.
  #sealing;
  // A codec that threw on the way out — the frame left plain; said by the
  // server's reporter.
  #failed;

  constructor({ direct, peer, session, highWaterMark, onFailure, compression = null, sealing = null, failed = null }) {
    super(`broker:${fingerprint(session)}`);
    this.#direct = direct;
    this.#peer = peer;
    this.#session = session;
    this.#highWaterMark = highWaterMark;
    this.#onFailure = onFailure;
    this.#compression = compression;
    this.#sealing = sealing;
    this.#failed = failed;
  }

  /** The codec ids in effect on this session — `{ encode, decode }` — or null. */
  get compression() {
    const active = this.#compression;
    return active === null ? null : { encode: active.encode.id, decode: active.decode.id };
  }

  // Frames go out in call order (the direct contract keeps one sender's
  // order); the broker's confirmation is what backpressure counts.
  write(data) {
    return this.#send(data, null);
  }

  // A write with per-message options (`compress: false`) — what
  // Client.sendRaw and a Broadcast use, as on a WebSocket.
  writeWith(text, options) {
    return this.#send(text, options);
  }

  #send(data, options) {
    if (this.#closed) return false;
    const seq = ++this.#seq;
    this.#unconfirmed++;
    const frame = sessionFrame(
      this.#sealing,
      this.#peer,
      this.#session,
      seq,
      data,
      this.#compression,
      options,
      this.#failed,
    );
    this.#direct.send(this.#peer, frame.body, { headers: frame.headers, correlationId: this.#session }).then(
      () => this.#confirmed(),
      (error) => {
        this.#confirmed();
        this.#onFailure(error);
      },
    );
    return this.#unconfirmed < this.#highWaterMark;
  }

  // One frame left the broker's hands, delivered or refused: crossing back
  // under the high-water mark is what a parked producer waits for.
  #confirmed() {
    this.#unconfirmed--;
    if (this.#unconfirmed === this.#highWaterMark - 1) this.emit('drain');
  }

  bye(reason) {
    if (this.#closed) return;
    // Through the sealer like every other frame: a sealed client drops a
    // plaintext goodbye as `unsealed` and would go on waiting for a session
    // that is over.
    const headers = { [HEADER_KIND]: KIND.BYE, [HEADER_REASON]: reason };
    const frame = sealFrame(this.#sealing, this.#peer, this.#session, headers, '');
    this.#direct.send(this.#peer, frame.body, { headers: frame.headers, correlationId: this.#session }).catch(() => {});
  }

  get closed() {
    return this.#closed;
  }

  close() {
    if (this.#closed) return;
    this.bye('closed');
    this.#closed = true;
    this.emit('close');
  }

  // Ended by the peer or the broker: no goodbye to send.
  drop() {
    if (this.#closed) return;
    this.#closed = true;
    this.emit('close');
  }
}

const attachBrokerRpc = async (server, broker, options = {}) => {
  const rpc = rpcOf(server, 'attachBrokerRpc');
  const direct = capabilityOf(broker, 'direct', 'attachBrokerRpc');
  const {
    service,
    address: addressOption,
    idleTimeout = DEFAULT_IDLE_TIMEOUT,
    highWaterMark = DEFAULT_HIGH_WATER_MARK,
    maxSessions = DEFAULT_MAX_SESSIONS,
    sessions: allowSessions = true,
    logger = null,
    compression = null,
    maxMessage,
    encryption = null,
  } = options;
  const address = serviceAddress(service, addressOption, 'attachBrokerRpc');
  // Off by default. On, this end's list is announced back on `welcome` to a
  // client whose `hello` list shares a codec with it, and a stateless answer
  // is compressed with the first codec here that the request named — a
  // client without the option is served plain.
  const codec = normalizeSyncCompression(compression, 'attachBrokerRpc: options');
  const agree = headerNegotiator(codec);
  const announce = codec === null ? '' : codec.ids.join(',');
  const cap = maxMessageOf(maxMessage, 'attachBrokerRpc');
  // Off by default. On, every frame is sealed under the shared keyring —
  // body AND headers, so the bearer token a client presents no longer rests
  // in the broker — and a frame that is not is refused (`acceptPlaintext`
  // for the rollout). A sealed frame is also what `encryption.required` on
  // the server accepts from this binding.
  const sealing = createBrokerSealing(encryption, 'attachBrokerRpc: options', { layer: 'broker-rpc', replay: true });
  // The group address is consumed by many instances, and the envelope's
  // replay window lives in one process: a sealed `request` or `hello`
  // carries the sender's clock inside the seal, refused past `maxSkew`, and
  // `replay` — a shared "seen once" memory, `seen(id, ttl)` like the HTTP
  // one — closes the window that leaves open. Both belong to the RPC
  // binding only; the log and queue layers are re-read by design.
  const maxSkew = encryption?.maxSkew ?? DEFAULT_MAX_SKEW;
  const replay = encryption?.replay ?? null;
  if (sealing !== null && !(Number.isFinite(maxSkew) && maxSkew > 0)) {
    throw new TypeError('attachBrokerRpc: encryption.maxSkew must be a positive number of milliseconds');
  }
  if (replay !== null && typeof replay?.seen !== 'function') {
    throw new TypeError('attachBrokerRpc: encryption.replay must be { seen(id, ttl) } or null');
  }
  if (!(Number.isFinite(idleTimeout) && idleTimeout > 0)) {
    throw new TypeError('attachBrokerRpc: idleTimeout must be a positive number of milliseconds');
  }
  if (!Number.isInteger(highWaterMark) || highWaterMark <= 0) {
    throw new TypeError('attachBrokerRpc: highWaterMark must be a positive integer');
  }
  if (!Number.isInteger(maxSessions) || maxSessions < 0) {
    throw new TypeError('attachBrokerRpc: maxSessions must be a non-negative integer (0 for no limit)');
  }
  const system = brokerName(broker) === 'custom' ? brokerName(direct) : brokerName(broker);
  // The server's writer, not the console: a pino-configured server used to
  // see this binding's lines on the raw console, without `event` or
  // `session`. `logger: false` still silences it.
  const log = createLoggerWriter(logger ?? rpc.log).child({ component: 'broker.rpc', broker: system });
  // A codec that threw: the frame leaves plain, or one that came in is
  // refused — counted and said once by the server's reporter. One receiver
  // per direction for the binding; the codec is the entry it is handed.
  const encodeFailed = (error, entry) => rpc.compressionFailed('broker', 'encode', entry.id, error);
  const decodeFailed = (error, entry) => rpc.compressionFailed('broker', 'decode', entry.id, error);
  const inbox = direct.inbox();
  const sessions = new Map(); // session id -> { transport, client, expectSeq, lastSeen, peer, compression }
  // Hellos refused at the cap since the last sweep: one line per sweep for
  // however many, not one per hello — a sender at the cap is a flood.
  let refused = 0;
  const basePath = rpc.basePath || '/';

  const reply = (message, headers, body) => {
    const frame = sealFrame(sealing, message.replyTo, message.correlationId, headers, body);
    return direct
      .send(message.replyTo, frame.body, { headers: frame.headers, correlationId: message.correlationId })
      .catch((error) => {
        log.warn({ event: 'broker.rpc.reply', err: error, to: message.replyTo });
      });
  };

  // One inbound frame, opened — or null for one that was refused: logged
  // with its reason, never answered (an answer would be an oracle, and the
  // sender of a frame that does not open is not the peer anyway).
  const opened = (at, message) => {
    const frame = openFrame(sealing, at, message);
    if (frame.refused === undefined) return frame;
    log.warn({ event: 'broker.rpc.refused', reason: frame.refused, kind: message.headers?.[HEADER_KIND] });
    return null;
  };

  // Stateless: a packet-mode POST in all but carrier. The request itself
  // travels plain (the client cannot know yet what this instance speaks);
  // its `wrpc-enc` lists the codecs the answer may come back in.
  const onRequest = (message) => {
    if (typeof message.replyTo !== 'string' || message.replyTo.length === 0) return;
    const headers = peerHeaders(message.headers);
    const active = agree(message.headers?.[HEADER_ENC]);
    void rpc.handleHttpCall({
      method: 'POST',
      url: basePath,
      headers,
      body: packetBody(message.body),
      remoteAddress: 'broker',
      encrypted: message.sealed === true,
      respond: ({ body }) => {
        const text = typeof body === 'string' ? body : packetBody(body);
        const encoded = active === null ? null : encodeIfSmaller(active.encode, text, encodeFailed);
        if (encoded === null) return void reply(message, { [HEADER_KIND]: KIND.RESPONSE }, text);
        reply(message, { [HEADER_KIND]: KIND.RESPONSE, [HEADER_ENC]: active.encode.id }, encoded);
      },
    });
  };

  // `level` is the reason's: the broker lost or mangled a frame (a
  // sequence gap, an undecodable frame) is warn — something on the path is
  // wrong; a client that went quiet is info; a goodbye, a replacement and
  // this server's own closing are debug — the routine ends of a session.
  // `reason` is one of a closed set — it is the label of
  // `wrpc.broker.rpc.session.ends`: gap | undecodable | idle | send_failed |
  // replaced | bye | peer_gap | closing. `detail` is the text of it (the
  // sequence numbers of a gap), for the log line and the peer's `wrpc-reason`.
  const endSession = (id, reason, { notify = true, level = 'debug', detail = reason } = {}) => {
    const session = sessions.get(id);
    if (!session) return;
    sessions.delete(id);
    if (notify) session.transport.bye(detail);
    session.transport.drop();
    rpc.otel.recordBrokerSessionEnd(reason);
    log[level]({ event: 'broker.rpc.session.end', session: fingerprint(id), reason: detail });
  };

  const onHello = (message) => {
    const id = message.correlationId;
    if (typeof id !== 'string' || id.length === 0 || typeof message.replyTo !== 'string') return;
    if (!allowSessions) {
      return void reply(message, { [HEADER_KIND]: KIND.BYE, [HEADER_REASON]: 'sessions disabled' }, '');
    }
    const existing = sessions.get(id);
    if (existing === undefined && maxSessions > 0 && sessions.size >= maxSessions) {
      refused++;
      return void reply(message, { [HEADER_KIND]: KIND.BYE, [HEADER_REASON]: 'too many sessions' }, '');
    }
    // A client re-saying hello with the same id (its welcome was lost) gets
    // a fresh session — provided the old one has seen no frame yet and the
    // hello comes from the same inbox. A LIVE session is never replaced by
    // a hello: neither by its own client's stray one, nor by a sender who
    // guessed its id, which used to end the session for the client that
    // held it and hand its id to the guesser.
    if (existing !== undefined) {
      if (existing.expectSeq !== 1 || existing.peer !== message.replyTo) {
        return void log.debug({
          event: 'broker.rpc.hello.duplicate',
          session: fingerprint(id),
          live: existing.expectSeq !== 1,
        });
      }
      endSession(id, 'replaced', { notify: false });
    }
    const active = agree(message.headers?.[HEADER_ENC]);
    const transport = new BrokerSessionTransport({
      direct,
      peer: message.replyTo,
      session: id,
      highWaterMark,
      compression: active,
      sealing,
      failed: encodeFailed,
      onFailure: (error) => {
        log.warn({ event: 'broker.rpc.send', err: error, session: fingerprint(id) });
        endSession(id, 'send_failed', { notify: false, detail: 'send failed' });
      },
    });
    const session = {
      transport,
      client: null,
      expectSeq: 1,
      lastSeen: Date.now(),
      compression: active,
      peer: message.replyTo,
      // Opened by a sealed hello: sealed frames only from here on.
      sealed: message.sealed === true,
    };
    sessions.set(id, session);
    // The core's own close path (RpcServer.close, client.close()) removes it.
    transport.once('close', () => {
      if (sessions.get(id) === session) sessions.delete(id);
    });
    try {
      session.client = rpc.attach(transport, {
        request: { headers: peerHeaders(message.headers), url: '', remoteAddress: 'broker' },
        encrypted: message.sealed === true,
      });
    } catch (error) {
      // `encryption.required` on the server, and a hello that was not sealed.
      sessions.delete(id);
      log.warn({ event: 'broker.rpc.refused', reason: 'plaintext', err: error });
      return void reply(message, { [HEADER_KIND]: KIND.BYE, [HEADER_REASON]: 'encryption required' }, '');
    }
    const welcome = { [HEADER_KIND]: KIND.WELCOME, [HEADER_INBOX]: inbox };
    if (active !== null) welcome[HEADER_ENC] = announce;
    void reply(message, welcome, '');
  };

  const onFrame = (raw) => {
    const message = opened(inbox, raw);
    if (message === null) return;
    const id = message.correlationId;
    const session = typeof id === 'string' ? sessions.get(id) : undefined;
    if (!session) {
      // A frame for a session this instance does not hold (it restarted, or
      // the session idled out): tell the client, which reconnects.
      if (typeof id === 'string' && message.headers?.[HEADER_KIND] !== KIND.BYE && message.replyTo) {
        // Debug, deliberately: any participant of the broker can send these
        // in a loop, and the client's own reconnect is the recovery.
        log.debug({ event: 'broker.rpc.session.unknown', session: fingerprint(id) });
        void reply(message, { [HEADER_KIND]: KIND.BYE, [HEADER_REASON]: 'unknown session' }, '');
      }
      return;
    }
    const kind = message.headers?.[HEADER_KIND];
    // Under `acceptPlaintext` — the rollout — a plaintext frame with the
    // right session id and sequence number used to walk into a session the
    // sealed hello had opened: a downgrade, and a bye that way ended it.
    // Dropped and named; the sequence is not consumed, so the real frame
    // is still served.
    if (session.sealed && message.sealed !== true) {
      return void log.warn({ event: 'broker.rpc.refused', reason: 'downgrade', kind });
    }
    if (kind === KIND.BYE) {
      // The client left because IT saw a gap in what this side sent — the
      // broker lost a frame on the way out, which nothing here can see. Its
      // text is reduced to that one word, never logged as it came.
      if (message.headers?.[HEADER_REASON] === 'gap') {
        return void endSession(id, 'peer_gap', { notify: false, level: 'warn', detail: 'the client saw a gap' });
      }
      return void endSession(id, 'bye', { notify: false });
    }
    const seq = seqOf(message.headers);
    if (seq !== session.expectSeq) {
      const detail = `sequence gap: expected ${session.expectSeq}, got ${seq}`;
      return void endSession(id, 'gap', { level: 'warn', detail });
    }
    session.expectSeq++;
    session.lastSeen = Date.now();
    // A frame marked compressed on a session that agreed to nothing, with
    // another codec, or that does not inflate under the cap: the peer's
    // protocol violation, and the session ends as on a sequence gap.
    const body = frameBody(message, session.compression === null ? null : codec, cap, decodeFailed);
    if (body === null) return void endSession(id, 'undecodable', { level: 'warn', detail: 'undecodable frame' });
    if (kind === KIND.CHUNK) session.transport.emit('chunk', toBytes(body));
    else session.transport.emit('packet', packetBody(body));
  };

  const dispatch = (message) => {
    const kind = message.headers?.[HEADER_KIND];
    if (kind === KIND.REQUEST) return void onRequest(message);
    if (kind === KIND.HELLO) return void onHello(message);
  };

  // The envelope's own header — kid, sender salt, counter — names a sealed
  // frame uniquely across the fleet: what the shared replay memory is keyed
  // by, for 2·maxSkew (past that the clock refuses the frame anyway).
  const replayId = (raw) => {
    const bytes = toBytes(raw.body);
    return `${raw.headers?.[HEADER_SEALED]}:${Buffer.from(bytes.subarray(2, HEADER_LENGTH)).toString('base64url')}`;
  };

  const onService = (raw) => {
    const message = opened(address, raw);
    if (message === null) return;
    if (message.sealed !== true) return void dispatch(message);
    const kind = message.headers?.[HEADER_KIND];
    const sent = Number(message.headers?.[HEADER_TIME]);
    if (!(Number.isFinite(sent) && Math.abs(Date.now() - sent) <= maxSkew)) {
      return void log.warn({ event: 'broker.rpc.refused', reason: 'stale', kind });
    }
    if (replay === null) return void dispatch(message);
    Promise.resolve()
      .then(() => replay.seen(replayId(raw), 2 * maxSkew))
      .then(
        (seen) => {
          if (seen) log.warn({ event: 'broker.rpc.refused', reason: 'replay', kind });
          else dispatch(message);
        },
        (error) => {
          // Fail closed, like the HTTP replay store: a memory that cannot be
          // asked serves nothing — never "probably not a replay".
          log.error({ event: 'broker.rpc.replay', err: error, kind });
        },
      );
  };

  let stopService = await direct.listen(address, onService, { group: address });
  let stopInbox;
  try {
    stopInbox = await direct.listen(inbox, onFrame);
  } catch (error) {
    await stopService();
    throw error;
  }

  const sweep = setInterval(
    () => {
      const cutoff = Date.now() - idleTimeout;
      for (const [id, session] of sessions) if (session.lastSeen < cutoff) endSession(id, 'idle', { level: 'info' });
      if (refused > 0) {
        log.warn({ event: 'broker.rpc.capacity', refused, sessions: sessions.size, max: maxSessions });
        refused = 0;
      }
    },
    Math.max(50, Math.min(idleTimeout / 3, 10_000)),
  );
  if (typeof sweep.unref === 'function') sweep.unref();

  // Draining: take no new work from the shared address (the other instances
  // do), keep serving the sessions already here until close.
  const onDraining = () => {
    const stop = stopService;
    stopService = null;
    if (stop) void stop();
  };
  let stopped = false;
  const stop = async () => {
    if (stopped) return;
    stopped = true;
    rpc.off('draining', onDraining);
    rpc.off('close', onClose);
    clearInterval(sweep);
    if (stopService) await stopService();
    stopService = null;
    for (const id of Array.from(sessions.keys())) endSession(id, 'closing', { detail: 'server closing' });
    await stopInbox();
  };
  const onClose = () => void stop();
  rpc.on('draining', onDraining);
  rpc.on('close', onClose);

  return {
    address,
    inbox,
    get sessions() {
      return sessions.size;
    },
    // Not stopped — and neither listener known to be deaf: a read loop
    // retrying after an error, a channel or a connection that is gone. An
    // adapter that cannot tell reads as healthy (`stop.healthy` is optional
    // in the direct contract), as does the address once draining gave it up.
    get healthy() {
      return !stopped && stopService?.healthy !== false && stopInbox.healthy !== false;
    },
    stop,
  };
};

module.exports = { attachBrokerRpc, BrokerSessionTransport };
