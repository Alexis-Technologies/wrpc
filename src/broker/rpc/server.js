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
const { toBytes } = require('../ids.js');
const { rpcOf } = require('../host.js');
const {
  HEADER_KIND,
  HEADER_SEQ,
  HEADER_INBOX,
  HEADER_REASON,
  HEADER_ENC,
  KIND,
  serviceAddress,
  peerHeaders,
  seqOf,
  packetBody,
} = require('./frames.js');
const {
  normalizeSyncCompression,
  codecById,
  headerNegotiator,
  maxMessageOf,
  encodeIfSmaller,
  decodeOrNull,
} = require('../../compression/sync.js');

// A frame's body as sent, inflated when its `wrpc-enc` header names the
// codec it was compressed with — any codec of `local`, the list this end
// holds, passed only once the session agreed on something — or null:
// marked but nothing agreed, a codec not held, a body that does not
// inflate under the cap.
const frameBody = (message, local, maxMessage) => {
  const encoding = message.headers?.[HEADER_ENC];
  if (encoding === undefined || encoding === null || encoding === '') return message.body;
  const entry = local === null || typeof encoding !== 'string' ? null : codecById(local, encoding);
  return entry === null ? null : decodeOrNull(entry, toBytes(message.body), maxMessage);
};

const DEFAULT_IDLE_TIMEOUT = 90_000; // 3x the client's 30 s heartbeat
const DEFAULT_HIGH_WATER_MARK = 1024;

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

  constructor({ direct, peer, session, highWaterMark, onFailure, compression = null }) {
    super(`broker:${session}`);
    this.#direct = direct;
    this.#peer = peer;
    this.#session = session;
    this.#highWaterMark = highWaterMark;
    this.#onFailure = onFailure;
    this.#compression = compression;
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
    const binary = typeof data !== 'string';
    const headers = { [HEADER_KIND]: binary ? KIND.CHUNK : KIND.PACKET, [HEADER_SEQ]: String(++this.#seq) };
    let body = binary ? toBytes(data) : data;
    const active = this.#compression;
    if (active !== null && (options === null || options.compress !== false)) {
      const encoded = encodeIfSmaller(active.encode, body);
      if (encoded !== null) {
        body = encoded;
        headers[HEADER_ENC] = active.encode.id;
      }
    }
    this.#unconfirmed++;
    this.#direct.send(this.#peer, body, { headers, correlationId: this.#session }).then(
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
    const headers = { [HEADER_KIND]: KIND.BYE, [HEADER_REASON]: reason };
    this.#direct.send(this.#peer, '', { headers, correlationId: this.#session }).catch(() => {});
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
    sessions: allowSessions = true,
    logger = null,
    compression = null,
    maxMessage,
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
  if (!(Number.isFinite(idleTimeout) && idleTimeout > 0)) {
    throw new TypeError('attachBrokerRpc: idleTimeout must be a positive number of milliseconds');
  }
  if (!Number.isInteger(highWaterMark) || highWaterMark <= 0) {
    throw new TypeError('attachBrokerRpc: highWaterMark must be a positive integer');
  }
  const system = brokerName(broker) === 'custom' ? brokerName(direct) : brokerName(broker);
  const log = createLoggerWriter(logger ?? globalThis.console).child({ component: 'broker.rpc', broker: system });
  const inbox = direct.inbox();
  const sessions = new Map(); // session id -> { transport, client, expectSeq, lastSeen }
  const basePath = rpc.basePath || '/';

  const reply = (message, headers, body) =>
    direct.send(message.replyTo, body, { headers, correlationId: message.correlationId }).catch((error) => {
      log.warn({ event: 'broker.rpc.reply', err: error, to: message.replyTo });
    });

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
      respond: ({ body }) => {
        const text = typeof body === 'string' ? body : packetBody(body);
        const encoded = active === null ? null : encodeIfSmaller(active.encode, text);
        if (encoded === null) return void reply(message, { [HEADER_KIND]: KIND.RESPONSE }, text);
        reply(message, { [HEADER_KIND]: KIND.RESPONSE, [HEADER_ENC]: active.encode.id }, encoded);
      },
    });
  };

  const endSession = (id, reason, { notify = true } = {}) => {
    const session = sessions.get(id);
    if (!session) return;
    sessions.delete(id);
    if (notify) session.transport.bye(reason);
    session.transport.drop();
    log.debug({ event: 'broker.rpc.session.end', session: id, reason });
  };

  const onHello = (message) => {
    const id = message.correlationId;
    if (typeof id !== 'string' || id.length === 0 || typeof message.replyTo !== 'string') return;
    if (!allowSessions) {
      return void reply(message, { [HEADER_KIND]: KIND.BYE, [HEADER_REASON]: 'sessions disabled' }, '');
    }
    // A client re-saying hello with the same id (its welcome was lost) gets
    // a fresh session: the old one could not have seen a frame yet.
    if (sessions.has(id)) endSession(id, 'replaced', { notify: false });
    const active = agree(message.headers?.[HEADER_ENC]);
    const transport = new BrokerSessionTransport({
      direct,
      peer: message.replyTo,
      session: id,
      highWaterMark,
      compression: active,
      onFailure: (error) => {
        log.warn({ event: 'broker.rpc.send', err: error, session: id });
        endSession(id, 'send failed', { notify: false });
      },
    });
    const session = { transport, client: null, expectSeq: 1, lastSeen: Date.now(), compression: active };
    sessions.set(id, session);
    // The core's own close path (RpcServer.close, client.close()) removes it.
    transport.once('close', () => {
      if (sessions.get(id) === session) sessions.delete(id);
    });
    session.client = rpc.attach(transport, {
      request: { headers: peerHeaders(message.headers), url: '', remoteAddress: 'broker' },
    });
    const welcome = { [HEADER_KIND]: KIND.WELCOME, [HEADER_INBOX]: inbox };
    if (active !== null) welcome[HEADER_ENC] = announce;
    void reply(message, welcome, '');
  };

  const onFrame = (message) => {
    const id = message.correlationId;
    const session = typeof id === 'string' ? sessions.get(id) : undefined;
    if (!session) {
      // A frame for a session this instance does not hold (it restarted, or
      // the session idled out): tell the client, which reconnects.
      if (typeof id === 'string' && message.headers?.[HEADER_KIND] !== KIND.BYE && message.replyTo) {
        void reply(message, { [HEADER_KIND]: KIND.BYE, [HEADER_REASON]: 'unknown session' }, '');
      }
      return;
    }
    const kind = message.headers?.[HEADER_KIND];
    if (kind === KIND.BYE) return void endSession(id, 'bye', { notify: false });
    const seq = seqOf(message.headers);
    if (seq !== session.expectSeq) {
      return void endSession(id, `sequence gap: expected ${session.expectSeq}, got ${seq}`);
    }
    session.expectSeq++;
    session.lastSeen = Date.now();
    // A frame marked compressed on a session that agreed to nothing, with
    // another codec, or that does not inflate under the cap: the peer's
    // protocol violation, and the session ends as on a sequence gap.
    const body = frameBody(message, session.compression === null ? null : codec, cap);
    if (body === null) return void endSession(id, 'undecodable frame');
    if (kind === KIND.CHUNK) session.transport.emit('chunk', toBytes(body));
    else session.transport.emit('packet', packetBody(body));
  };

  const onService = (message) => {
    const kind = message.headers?.[HEADER_KIND];
    if (kind === KIND.REQUEST) return void onRequest(message);
    if (kind === KIND.HELLO) return void onHello(message);
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
      for (const [id, session] of sessions) if (session.lastSeen < cutoff) endSession(id, 'idle');
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
    for (const id of Array.from(sessions.keys())) endSession(id, 'server closing');
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
    get healthy() {
      return !stopped;
    },
    stop,
  };
};

module.exports = { attachBrokerRpc, BrokerSessionTransport };
