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
  HEADER_INBOX,
  HEADER_ENC,
  HEADER_VERSION,
  KIND,
  HEADER_REASON,
  serviceAddress,
  peerHeaders,
  seqOf,
  packetBody,
  frameBody,
  sessionFrame,
  sealFrame,
  openFrame,
} = require('./frames.js');
const { createBrokerSealing } = require('../sealing.js');
const { normalizeSyncCompression, headerNegotiator, maxMessageOf } = require('../../compression/sync.js');

const DEFAULT_REQUEST_TIMEOUT = 30_000;
const DEFAULT_HIGH_WATER_MARK = 1024;

// broker://billing -> 'billing'
const serviceOf = (url) => {
  const match = /^broker:\/\/([^/?#]+)/.exec(String(url));
  return match ? decodeURIComponent(match[1]) : null;
};

class ClientBrokerTransport extends ClientTransport {
  // `options.encryption` here is the KEYRING form — `{ keys, … }`, shared
  // with the service — not the session object of createEncryption().
  static encrypts = 'keys';

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
  // The binding's sealing under a shared keyring (../sealing.js), or null.
  #sealing = null;
  // Welcomed by a sealed frame: the session takes sealed frames only, so a
  // plaintext bye or packet naming it (acceptPlaintext lets one through the
  // opener) is a downgrade, dropped.
  #sealedSession = false;
  #active = null;
  // This end reads framed messages: what its hello and requests say.
  #frames = true;
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
    // `encryption: { keys }` — the binding's own sealing under a keyring the
    // service shares (NOT the session object of createEncryption: there is
    // no connection here to hold a handshake). Headers move inside with the
    // body, so `authorization` no longer rests in the broker.
    this.#sealing = createBrokerSealing(options.encryption, 'broker transport: options', {
      layer: 'broker-rpc',
      replay: true,
    });
    this.#active = null;
    // What every stateless request and the hello announce: the codecs an
    // answer may come back in, in this end's order of preference.
    if (this.#compression !== null) this.#headers[HEADER_ENC] = this.#compression.ids.join(',');
    // Revision 1 until the service's welcome said 2, and only when this end
    // reads frames itself; a stateless request stays at 1 (frames.js).
    this.#frames = this.attachments !== false && !this.codec;
    this.revision = 1;
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
    this.#sealedSession = false;
    const welcomed = new Promise((resolve, reject) => {
      this.#welcome = { resolve, reject };
    });
    // Settled by a close() that may come after a failed hello already
    // abandoned the await below: never an unhandled rejection.
    welcomed.catch(() => {});
    const hello = sealFrame(
      this.#sealing,
      this.#address,
      this.#session,
      this.#frames
        ? { ...this.#headers, [HEADER_KIND]: KIND.HELLO, [HEADER_VERSION]: '2' }
        : { ...this.#headers, [HEADER_KIND]: KIND.HELLO },
      '',
    );
    await this.#direct.send(this.#address, hello.body, {
      headers: hello.headers,
      correlationId: this.#session,
      replyTo: this.#inbox,
      timeout: this.#requestTimeout,
    });
    // The client's connectTimeout races the whole open(); a terminate()
    // meanwhile settles this through #welcome.
    this.#remote = await welcomed;
    if (generation !== this.#generation) throw new Error('Connection closed');
  }

  #onMessage(raw, generation) {
    if (generation !== this.#generation) return;
    const message = openFrame(this.#sealing, this.#inbox, raw);
    // A frame that does not open is not the service's: dropped, and said so.
    if (message.refused !== undefined) {
      return void this.log?.warn({ event: 'broker.rpc.refused', reason: message.refused });
    }
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
      this.#active = headerNegotiator(this.#compression)(message.headers?.[HEADER_ENC]);
      this.#sealedSession = message.sealed === true;
      this.revision = this.#frames && message.headers?.[HEADER_VERSION] === '2' ? 2 : 1;
      if (pending && typeof remote === 'string' && remote.length > 0) pending.resolve(remote);
      return;
    }
    if (this.#sealedSession && message.sealed !== true) {
      return void this.log?.warn({ event: 'broker.rpc.refused', reason: 'downgrade', kind });
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
    const body = frameBody(message, this.#active === null ? null : this.#compression, this.#maxMessage);
    if (body === null) return void this.#lost(new Error('Session frame cannot be inflated'), true);
    if (kind === KIND.CHUNK) this.emit('message', toBytes(body));
    else this.emit('message', packetBody(body));
  }

  /** The codec ids in effect on the session — `{ encode, decode }` — or null. */
  get compression() {
    const active = this.#active;
    return active === null ? null : { encode: active.encode.id, decode: active.decode.id };
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
    // Told why: a gap seen HERE is a frame the broker lost on the server's
    // way out, which the server cannot see for itself.
    if (notify) this.#bye('gap');
    this.active = false;
    this.#generation++;
    void this.#release();
    this.emit('close', error);
  }

  #bye(reason = null) {
    if (!this.#remote || !this.#session) return;
    const headers =
      reason === null ? { [HEADER_KIND]: KIND.BYE } : { [HEADER_KIND]: KIND.BYE, [HEADER_REASON]: reason };
    const bye = sealFrame(this.#sealing, this.#remote, this.#session, headers, '');
    this.#direct.send(this.#remote, bye.body, { headers: bye.headers, correlationId: this.#session }).catch(() => {});
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
    const seq = ++this.#seq;
    this.#unconfirmed++;
    const generation = this.#generation;
    const frame = sessionFrame(this.#sealing, this.#remote, this.#session, seq, data, this.#active, options);
    this.#direct
      .send(this.#remote, frame.body, {
        headers: frame.headers,
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
    const correlationId = this.generateId();
    const request = sealFrame(
      this.#sealing,
      this.#address,
      correlationId,
      { ...this.#headers, [HEADER_KIND]: KIND.REQUEST },
      data,
    );
    this.#direct
      .send(this.#address, request.body, {
        headers: request.headers,
        correlationId,
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
