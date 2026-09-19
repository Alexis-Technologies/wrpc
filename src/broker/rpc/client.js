'use strict';

// RPC over a broker, client side: `WrpcClient.transport.broker`, registered
// when @alexify/wrpc/broker is required (the sse/webrtc idiom).
//
//   const client = await connect('broker://billing', {
//     transport: 'broker',
//     broker,                  // a broker with the `direct` capability
//     mode: 'session',         // or 'stateless' (the default)
//   });
//
// 'stateless' is request/response, like the http transport: any instance
// answers any call, batches included; no events, subscriptions or streams.
// 'session' is the full protocol with one instance, which the client
// reaches through the hello/welcome handshake; its heartbeat is what keeps
// the session inside the server's idle window, and a frame-sequence gap or
// a `bye` is a lost connection the ordinary reconnect cycle recovers from.

const { WrpcClient, ClientTransport } = require('../../client/core.js');
const { generateUUID } = require('../../runtime/node.js');
const { capabilityOf } = require('../port.js');
const { toBytes } = require('../ids.js');
const {
  HEADER_KIND,
  HEADER_SEQ,
  HEADER_INBOX,
  HEADER_ENC,
  KIND,
  serviceAddress,
  peerHeaders,
  seqOf,
  packetBody,
} = require('./frames.js');
const {
  normalizeSyncCompression,
  negotiate,
  maxMessageOf,
  encodeIfSmaller,
  decodeOrNull,
} = require('../../compression/sync.js');

// A frame's body, inflated when marked — see the server's frameBody.
const frameBody = (message, active, maxMessage) => {
  const encoding = message.headers?.[HEADER_ENC];
  if (encoding === undefined || encoding === null || encoding === '') return message.body;
  if (active === null || encoding !== active.id) return null;
  return decodeOrNull(active, toBytes(message.body), maxMessage);
};

const DEFAULT_REQUEST_TIMEOUT = 30_000;
const DEFAULT_HIGH_WATER_MARK = 1024;

// broker://billing -> 'billing'
const serviceOf = (url) => {
  const match = /^broker:\/\/([^/?#]+)/.exec(String(url));
  return match ? decodeURIComponent(match[1]) : null;
};

class ClientBrokerTransport extends ClientTransport {
  // Decided per open() from `mode`; the defaults describe stateless.
  persistent = false;
  heartbeat = false;
  mode = 'stateless';

  #direct = null;
  #address = null;
  #inbox = null;
  #stopInbox = null;
  #headers = {};
  #requestTimeout = DEFAULT_REQUEST_TIMEOUT;
  #highWaterMark = DEFAULT_HIGH_WATER_MARK;
  // session state
  #session = null;
  #remote = null;
  #seq = 0;
  #expectSeq = 1;
  #unconfirmed = 0;
  #welcome = null;
  #generation = 0;
  // Per-message compression: the option, and what the server agreed to on
  // welcome (a session) — stateless answers are decided per request.
  #compression = null;
  #active = null;
  #maxMessage;
  // Correlation ids AND the session id. The session id is the key the server
  // holds this connection's frame state under, so guessing one lets a sender
  // inject frames into somebody else's session — which is why it is worth
  // being able to replace the default with a generator of known strength.
  // `generateId` is assigned by the owning WrpcClient (already resolved from
  // its own option) the way `codec` and `log` are; a transport driven
  // standalone falls back.
  generateId = generateUUID;

  async open(options = {}) {
    if (this.active) return;
    const mode = options.mode ?? 'stateless';
    if (mode !== 'stateless' && mode !== 'session') {
      throw new TypeError("broker transport: mode must be 'stateless' or 'session'");
    }
    this.#direct = capabilityOf(options.broker, 'direct', 'broker transport');
    this.#address = serviceAddress(serviceOf(this.url), options.address, 'broker transport');
    this.#headers = peerHeaders(options.headers);
    if (options.meta) this.#headers['x-wrpc-meta'] = JSON.stringify(options.meta);
    this.#requestTimeout = options.requestTimeout ?? DEFAULT_REQUEST_TIMEOUT;
    this.#compression = normalizeSyncCompression(options.compression, 'broker transport: options');
    this.#maxMessage = maxMessageOf(options.maxMessage, 'broker transport');
    this.#active = null;
    // What every stateless request and the hello announce: the codec an
    // answer may come back in.
    if (this.#compression !== null) this.#headers[HEADER_ENC] = this.#compression.id;
    this.mode = mode;
    this.persistent = mode === 'session';
    this.heartbeat = mode === 'session';
    const generation = ++this.#generation;
    this.#inbox = this.#direct.inbox();
    const stop = await this.#direct.listen(this.#inbox, (message) => this.#onMessage(message, generation));
    if (generation !== this.#generation) return void (await stop());
    this.#stopInbox = stop;
    if (mode === 'session') {
      try {
        await this.#handshake(generation);
      } catch (error) {
        await this.#release();
        throw error;
      }
    }
    this.active = true;
    this.emit('open');
  }

  async #handshake(generation) {
    this.#session = this.generateId();
    this.#seq = 0;
    this.#expectSeq = 1;
    this.#unconfirmed = 0;
    const welcomed = new Promise((resolve, reject) => {
      this.#welcome = { resolve, reject };
    });
    // Settled by a close() that may come after a failed hello already
    // abandoned the await below: never an unhandled rejection.
    welcomed.catch(() => {});
    const headers = { ...this.#headers, [HEADER_KIND]: KIND.HELLO };
    await this.#direct.send(this.#address, '', {
      headers,
      correlationId: this.#session,
      replyTo: this.#inbox,
      timeout: this.#requestTimeout,
    });
    // The client's connectTimeout races the whole open(); a terminate()
    // meanwhile settles this through #welcome.
    this.#remote = await welcomed;
    if (generation !== this.#generation) throw new Error('Connection closed');
  }

  #onMessage(message, generation) {
    if (generation !== this.#generation) return;
    const kind = message.headers?.[HEADER_KIND];
    if (this.mode === 'stateless') {
      if (kind !== KIND.RESPONSE) return;
      const body = frameBody(message, this.#compression, this.#maxMessage);
      if (body === null) return void this.#escalate(new Error('broker transport: an answer that cannot be inflated'));
      this.emit('message', packetBody(body));
      return;
    }
    if (message.correlationId !== this.#session) return;
    if (kind === KIND.WELCOME) {
      const remote = message.headers?.[HEADER_INBOX];
      const pending = this.#welcome;
      this.#welcome = null;
      this.#active = negotiate(this.#compression, message.headers?.[HEADER_ENC]);
      if (pending && typeof remote === 'string' && remote.length > 0) pending.resolve(remote);
      return;
    }
    if (kind === KIND.BYE) {
      return void this.#lost(new Error(`Session ended: ${message.headers?.['wrpc-reason'] ?? 'bye'}`));
    }
    if (kind !== KIND.PACKET && kind !== KIND.CHUNK) return;
    const seq = seqOf(message.headers);
    if (seq !== this.#expectSeq) {
      return void this.#lost(new Error(`Session frame gap: expected ${this.#expectSeq}, got ${seq}`), true);
    }
    this.#expectSeq++;
    const body = frameBody(message, this.#active, this.#maxMessage);
    if (body === null) return void this.#lost(new Error('Session frame cannot be inflated'), true);
    if (kind === KIND.CHUNK) this.emit('message', toBytes(body));
    else this.emit('message', packetBody(body));
  }

  /** The compression codec id in effect on the session — both ends named it — or null. */
  get compression() {
    return this.#active === null ? null : this.#active.id;
  }

  #escalate(error) {
    if (this.listenerCount('error') === 0) return;
    void this.emit('error', error).catch(() => {});
  }

  // The connection is gone: announced as a close the reconnect cycle
  // handles. A gap also tells the server, whose session is now useless.
  #lost(error, notify = false) {
    if (this.#welcome) {
      const pending = this.#welcome;
      this.#welcome = null;
      return void pending.reject(error);
    }
    if (!this.active) return;
    if (notify) this.#bye();
    this.active = false;
    this.#generation++;
    void this.#release();
    this.emit('close', error);
  }

  #bye() {
    if (!this.#remote || !this.#session) return;
    this.#direct
      .send(this.#remote, '', { headers: { [HEADER_KIND]: KIND.BYE }, correlationId: this.#session })
      .catch(() => {});
  }

  async #release() {
    const stop = this.#stopInbox;
    this.#stopInbox = null;
    this.#remote = null;
    if (stop) await stop().catch(() => {});
  }

  write(data, options = null) {
    if (!this.active) throw new Error('Not connected');
    if (this.mode === 'stateless') return this.#request(data);
    const binary = typeof data !== 'string';
    const headers = { [HEADER_KIND]: binary ? KIND.CHUNK : KIND.PACKET, [HEADER_SEQ]: String(++this.#seq) };
    let body = binary ? toBytes(data) : data;
    const active = this.#active;
    if (active !== null && (options === null || options.compress !== false)) {
      const encoded = encodeIfSmaller(active, body);
      if (encoded !== null) {
        body = encoded;
        headers[HEADER_ENC] = active.id;
      }
    }
    this.#unconfirmed++;
    const generation = this.#generation;
    this.#direct
      .send(this.#remote, body, {
        headers,
        correlationId: this.#session,
        replyTo: this.#inbox,
      })
      .then(
        () => this.#confirmed(),
        (error) => {
          this.#confirmed();
          if (generation === this.#generation) this.#lost(error);
        },
      );
    return this.#unconfirmed < this.#highWaterMark;
  }

  #confirmed() {
    this.#unconfirmed--;
    if (this.#unconfirmed === this.#highWaterMark - 1) this.emit('drain');
  }

  #request(data) {
    const headers = { ...this.#headers, [HEADER_KIND]: KIND.REQUEST };
    this.#direct
      .send(this.#address, data, {
        headers,
        correlationId: this.generateId(),
        replyTo: this.#inbox,
        timeout: this.#requestTimeout,
      })
      // Nobody serves the address (a 503 the broker knows about) or the
      // send failed outright: the calls this frame carried settle now.
      .catch((error) => this.failPackets(data, typeof error?.code === 'number' ? error.code : 503));
    return true;
  }

  close() {
    this.#generation++;
    if (this.#welcome) {
      const pending = this.#welcome;
      this.#welcome = null;
      pending.reject(new Error('Connection closed'));
    }
    if (!this.active) return void this.#release();
    if (this.mode === 'session') this.#bye();
    this.active = false;
    void this.#release();
    this.emit('close');
  }
}

// Assigned INTO the registry, never replacing it (see src/client/core.js).
WrpcClient.transport.broker = ClientBrokerTransport;

module.exports = { ClientBrokerTransport };
