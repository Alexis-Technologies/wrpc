'use strict';

// The three built-in client transports, registered on WrpcClient.transport
// exactly the way the SSE subpath registers its own — the registry is the
// one seam every transport, built-in or not, goes through.

const { WrpcClient, ClientTransport, WRPC_V1, WRPC_V2, META_MAX, metaHeaders, refusedStatus } = require('./core.js');
const { jsonParse } = require('../utils.js');
const { decodeAttachments } = require('../attachments.js');
const { createWsCompression } = require('./wsCompression.js');
const { openSocket } = require('./wsHandshake.js');

const { WebSocket } = globalThis;

class ClientWsTransport extends ClientTransport {
  // Carries `options.encryption` (@alexify/wrpc/encryption): the handshake
  // runs inside open(), and every frame after it is sealed.
  static encrypts = true;

  // The one transport that can die without saying so.
  heartbeat = true;
  // Until the server selected `wrpc.v2`: a 1.0 server selects `wrpc.v1`.
  revision = 1;

  #socket = null;
  #opening = null;
  // Per-message compression of what THIS side sends (wsCompression.js —
  // a stub in a browser): the offer goes out in a ping on open, and frames
  // change only once the server's pong agreed. Re-negotiated per open.
  #compression = null;
  #negotiating = false;
  // The encrypted session of this connection, from `options.encryption`:
  // `{ ready, send, receive }`. Re-made per open — a reconnect is a new
  // handshake and new keys.
  #secure = null;
  // Set once a server answered `wrpc.v1` to a handshake that carried the
  // declared bags as subprotocol tokens: a 1.0 server reads them from the
  // connect URL only, so every later open of this transport uses the query.
  #requery = false;

  /** The session's facts once established (see WrpcClient#encryption), or null. */
  encryption = null;

  /** The compression codec id in effect on this side's frames, or null. */
  get compression() {
    return this.#compression === null ? null : this.#compression.id;
  }

  async open(options = {}) {
    if (this.active) return Promise.resolve();
    if (this.#opening) return this.#opening;
    // Under a wire codec a packet is not JSON, and the negotiation ping
    // would not be either: the option is left off there.
    this.#compression = this.codec ? null : createWsCompression(options.compression);
    this.#negotiating = this.#compression !== null;
    const opening = new Promise((resolve, reject) => {
      // The client OFFERS the protocol revisions it speaks, newest first, and
      // the server selects one (see protocol.md#versioning): a 1.0 server
      // picks `wrpc.v1`, and this connection then speaks what 1.0 spoke. A
      // client that sends no frames (`attachments: false`, a packet codec)
      // has nothing of revision 2 to offer. `protocols` overrides the offer,
      // and an empty array offers nothing — an escape hatch for a proxy that
      // mangles the header. The selected protocol lands on `this.protocol`.
      // The declared headers/meta leave with the handshake: real request
      // headers from Node, subprotocol carrier tokens from a browser, the
      // connect-URL query only by `carrier: 'query'` — see wsHandshake.js
      // and its browser half.
      const { encryption = null } = options;
      // Announced in the URL: the server may be the first to send, so it
      // has to know the mode before any frame. Not a secret — stripping it
      // gets a refusal, never plaintext.
      const url =
        encryption === null ? this.url : `${this.url}${this.url.includes('?') ? '&' : '?'}${encryption.param}=1`;
      const offer = options.protocols ?? (this.attachments === false ? [WRPC_V1] : [WRPC_V2, WRPC_V1]);
      // A copy the handshake may write to (`carried`), and where a transport
      // that already met a revision-1 server asks for the query carrier.
      const handshake = this.#requery ? { ...options, carrier: 'query' } : { ...options };
      const socket = openSocket(WebSocket, url, offer, handshake, this.log);
      // Bytes arrive as ArrayBuffers, never Blobs: an attachments frame is
      // classified synchronously on the way in, in order with the packets.
      socket.binaryType = 'arraybuffer';
      this.#socket = socket;
      const onClose = (error) => {
        // Scoped to the socket it was registered for. Both 'close' and
        // 'error' route here, and terminate() abandons a socket while it is
        // still alive — its later close must not clear the #socket of the
        // replacement a reconnect has already installed.
        if (this.#socket !== socket) return;
        this.#socket = null;
        // A handshake still running is over with the socket: its ready()
        // rejects now, not at the handshake timeout.
        this.#secure?.cancel(error);
        this.#secure = null;
        this.encryption = null;
        if (this.#opening) {
          this.#opening = null;
          this.emit('error', error);
          return void reject(new Error('Connection closed'));
        }
        if (!this.active) return;
        this.active = false;
        this.emit('close', error);
      };
      const established = () => {
        // First on the wire, before the core's own open handler sends
        // anything: the pong that answers it is what turns compression on.
        if (this.#compression !== null) this.#send(this.#compression.offer);
        this.active = true;
        this.emit('open');
        this.#opening = null;
        resolve();
      };
      const onOpen = () => {
        this.protocol = socket.protocol || '';
        this.revision = this.protocol === WRPC_V2 ? 2 : 1;
        // Offered `wrpc.v2`, answered `wrpc.v1`: possibly a 1.0 server, and
        // 1.0 reads the declared bags from the connect URL only — the
        // tokens this handshake carried went unread. Dialled again, once,
        // with the query 1.0 itself used; an application that chose its
        // carrier, or its offer, is left with its choice.
        if (
          handshake.carried &&
          this.protocol === WRPC_V1 &&
          offer.includes(WRPC_V2) &&
          (options.carrier ?? 'auto') === 'auto'
        ) {
          this.#requery = true;
          this.log?.warn({ event: 'handshake.requery' });
          // Abandoned, not closed-and-reported: its close is nobody's now.
          this.#socket = null;
          socket.close();
          this.#opening = null;
          return void this.open(options).then(resolve, reject);
        }
        if (encryption === null) return void established();
        // The handshake first: nothing of this connection leaves in the
        // clear, the compression offer included. `connectTimeout` is racing
        // open(), so it covers this too; a failure closes the socket, which
        // is what rejects the pending open.
        this.#secure = encryption.secure({
          kind: 'ws',
          write: (bytes) => socket.send(bytes),
          deliver: (data) => this.#deliver(data),
          fail: (error) => {
            this.log?.warn({ event: 'encryption.failed', err: error });
            socket.close();
          },
        });
        this.#secure.ready.then(
          (info) => {
            if (this.#socket !== socket) return;
            this.encryption = info;
            established();
          },
          () => {},
        );
      };
      socket.addEventListener('open', onOpen, { once: true });
      socket.addEventListener('close', onClose, { once: true });
      socket.addEventListener('error', onClose, { once: true });
      socket.addEventListener('message', ({ data }) => {
        if (this.#socket !== socket) return;
        if (this.#secure !== null) return void this.#secure.receive(data);
        this.#deliver(data);
      });
    });
    this.#opening = opening;
    return opening;
  }

  close() {
    if (!this.active) return;
    this.#socket.close();
  }

  // A peer that stopped answering will never complete a close handshake, so
  // waiting for one would gate the reconnect on the socket's own timeout.
  // Report the close now; the socket's later 'close' is then a no-op.
  terminate() {
    // A connect still in flight (the connectTimeout race): closing a
    // CONNECTING socket aborts the handshake and fires 'close', which is
    // where onClose rejects the pending open().
    if (this.#opening) return void this.#socket?.close();
    if (!this.active) return;
    const socket = this.#socket;
    this.active = false;
    this.#socket = null;
    this.#secure?.cancel();
    this.#secure = null;
    this.encryption = null;
    this.emit('close');
    socket?.close();
  }

  // One inbound message, decrypted already when the connection is sealed.
  #deliver(data) {
    // Only until the first pong answered the offer — a plain pong is a
    // no, and either way nothing is inspected after that.
    if (this.#negotiating && this.#compression.accept(data) !== null) this.#negotiating = false;
    this.emit('message', data);
  }

  // Compress, then seal: ciphertext does not compress.
  #send(data) {
    if (this.#secure !== null) return void this.#secure.send(data);
    this.#socket.send(data);
  }

  write(data) {
    if (!this.active) throw new Error('Not connected');
    if (this.#compression !== null) {
      const frame = this.#compression.encode(data);
      if (frame !== null) return void this.#send(frame);
    }
    this.#send(data);
  }
}

// What a client that reads framed messages asks an HTTP server for: a 2.x
// server answers a result holding bytes as a frame only to a request that
// names it, and a 1.0 server ignores the header. CORS-safelisted, so it adds
// no preflight.
const ACCEPT_FRAMES = 'application/octet-stream, application/json';

class ClientHttpTransport extends ClientTransport {
  // Carries `options.encryption`: no session here, so every request is
  // sealed to the pinned server key on its own (HPKE) — by wrapping fetch.
  // Per request, not per session: checked up front against the option's
  // `fetch` (see WrpcClient's #checkEncryption).
  static encrypts = 'request';

  // One request, one response: nothing to cancel or subscribe on.
  persistent = false;
  // No handshake to negotiate on: a server says the revision it speaks in
  // the `wrpc-version` of every response, so this side sends a frame only
  // after an answer said 2 — and a 1.0 server, which says 1, never gets one
  // unless it shares an address with 2.x ones; see write().
  revision = 1;
  // Can carry procedure-mapped REST requests (client/core #restCall): a
  // call whose procedure declares `http` goes out as the same REST request
  // an external consumer would send, not as a packet POST.
  rest = true;
  // Consumes write()'s meta argument as request headers — the flag the
  // batch flush checks before building its per-frame aggregate.
  metaHeaders = true;
  // Connection-phase headers and metadata, resolved per open — here they
  // ride as REAL request headers on every packet POST and REST leg.
  headers = null;
  meta = null;
  metaBag = null;
  prefixed = false;
  // Injectable fetch, defaulted here (not just in open()) so a caller that
  // uses this transport directly — request() without an open() — still
  // gets the runtime's own fetch. Re-resolved on open() like headers/meta:
  // lets a caller hand in undici's fetch bound to a tuned Agent/Pool
  // (keep-alive, proxy, an undici-cache-interceptor store) without wrpc
  // ever depending on undici.
  fetch = globalThis.fetch;

  async open(options = {}) {
    this.headers = options.headers ?? null;
    // Stored and always CALLED unbound (never as this.fetch(...)): native
    // fetch throws "Illegal invocation" in browsers when invoked with a
    // receiver other than the global object, which a plain method call
    // would hand it. An injected fetch is called the same free-function way.
    this.fetch = options.fetch ?? globalThis.fetch;
    const { encryption = null } = options;
    if (encryption !== null) {
      if (typeof encryption.fetch !== 'function') {
        throw new TypeError('options.encryption has no serverKey to seal a request to — the http transport needs one');
      }
      this.fetch = encryption.fetch(this.fetch, this.url);
    }
    // Built once per open, as a header BLOCK rather than a single encoded
    // value: which spelling it is (one canonical JSON header, or one header
    // per key) is the client's metaFormat choice, and every leg below just
    // spreads whatever came out.
    this.prefixed = options.metaPrefixed === true;
    // Both the raw bag and the prebuilt block: a call that brings its own
    // meta has to merge with the BAG, not with the block. In json mode the
    // two halves are the same header name, so layering blocks would replace
    // the connection's meta rather than extend it.
    this.metaBag = options.meta ?? null;
    this.meta = this.metaBag ? metaHeaders(this.metaBag, this.prefixed) : null;
    if (this.active) return;
    this.active = true;
    this.emit('open');
  }

  close() {
    if (!this.active) return;
    this.active = false;
    this.emit('close');
  }

  // The REST leg: one mapped call, one plain-bodied response. The caller
  // (client/core #restCall) interprets status and body; aborting `signal`
  // aborts the fetch. `rest` is the codec.rest section when configured —
  // it owns this leg's Content-Type and switches the read path to bytes.
  // The PACKET codec's contentType belongs to write() below, never here:
  // a JSON REST body must say JSON.
  async request(method, url, body, signal, options = {}) {
    const { rest = null, meta = null, trace = null } = options;
    // Declared first, wire headers after: the protocol's own always win.
    // A call's own meta merges over the connection's bag — per-call wins a
    // key collision — and only then becomes headers. `trace` is the REST
    // leg's traceparent/tracestate pair, injected by the telemetry writer.
    const block = meta ? this.#requestMeta(meta) : this.meta;
    const headers = {
      ...this.headers,
      ...block,
      ...trace,
      'Content-Type': rest?.contentType ?? 'application/json',
    };
    const init = body === undefined ? { method, headers, signal } : { method, headers, body, signal };
    const doFetch = this.fetch;
    const res = await doFetch(url, init);
    if (rest) return { status: res.status, body: new Uint8Array(await res.arrayBuffer()) };
    return { status: res.status, text: await res.text() };
  }

  // One request's meta block: the connection bag with this request's own
  // layered over it. Capped here rather than left to the server, whose
  // limit drops the WHOLE bag — a silent loss on the side that cannot fix
  // it. Refusing here keeps the connection's own label intact and says so.
  #requestMeta(meta) {
    const merged = { ...this.metaBag, ...meta };
    const block = metaHeaders(merged, this.prefixed);
    let bytes = 0;
    // String() defensively: a non-string slipping through would make bytes
    // NaN, and `NaN <= META_MAX` refuses the whole block with no real cause.
    for (const key in block) bytes += key.length + String(block[key]).length;
    if (bytes <= META_MAX) return block;
    this.log.warn({ event: 'meta.oversize', bytes });
    return this.meta;
  }

  // Malformed answers null either way — the codec's parse is the probe's.
  #decode(text) {
    if (!this.codec) return jsonParse(text);
    try {
      return this.codec.decode(text);
    } catch {
      return null;
    }
  }

  write(data, meta) {
    // A batch's aggregated per-call meta (see client core flush) merges over
    // the connection bag for THIS request only. It is a summary: one POST
    // has one header block, so a key carried by several calls shows the last
    // value. Nothing is lost — each call's exact meta rides its own packet.
    const block = meta ? this.#requestMeta(meta) : this.meta;
    // An attachments frame is bytes and says so; a text body is the packet
    // codec's type, JSON by default.
    const contentType =
      typeof data === 'string' ? (this.codec?.contentType ?? 'application/json') : 'application/octet-stream';
    const headers = { ...this.headers, ...block, 'Content-Type': contentType };
    // A client that reads frames says so; one that opted out, or speaks a
    // packet codec, is answered in text as before.
    if (this.attachments !== false && !this.codec) headers.Accept = ACCEPT_FRAMES;
    const options = { method: 'POST', headers, body: data };
    const doFetch = this.fetch;
    const send = async () => {
      try {
        const res = await doFetch(this.url, options);
        // The server reads framed messages: from here on, bytes this side
        // sends travel as bytes.
        const modern = res.headers.get('wrpc-version') === '2';
        if (modern) this.revision = 2;
        // A frame answer (a result holding bytes) is read as bytes and
        // handed on as one; the core classifies it.
        if (res.ok && res.headers.get('content-type') === 'application/octet-stream') {
          return void this.emit('message', new Uint8Array(await res.arrayBuffer()));
        }
        const text = await res.text();
        // A frame reached a server that does not say 2 — a 1.0 instance
        // behind the same address as 2.x ones, a rollback — and wrpc
        // answered it (a proxy's page has no packet in it, and says nothing
        // about the server behind). That server refused the frame before
        // reading a call out of it: this side speaks revision 1 again, and
        // the same packets go once more as the JSON 1.0 reads.
        if (!modern && typeof data !== 'string' && this.#decode(text) !== null) {
          this.revision = 1;
          return void this.write(JSON.stringify(decodeAttachments(data)), meta);
        }
        // Error statuses normally still carry wrpc callback packets (the
        // server answers errors as JSON with the same code). Only when the
        // body is NOT wrpc's — a proxy's HTML 502, an empty body — are
        // answers synthesized, so the exact calls this request carried
        // settle now instead of waiting out callTimeout.
        if (res.ok || this.#decode(text) !== null) return void this.emit('message', text);
        // The synthesized per-id answers live on the base class now — the
        // SSE transport's outbound half fails the same way (see failPackets).
        this.failPackets(data, res.status);
      } catch (error) {
        this.emit('error', error);
        // A sealed request the server refused in plaintext names its status
        // only for the closed set the sealing layer vouches for (400, 409,
        // 426 — a retired key, a clock, a required mode); the outer status
        // of anything else is unauthenticated and stays a 503.
        this.failPackets(data, refusedStatus(error));
      }
    };
    send();
  }
}

const isPongText = (data) => typeof data === 'string' && data.startsWith('{"type":"pong"');

// What the other end wrote, through the codec — null for anything it does
// not decode (an injected codec may throw on a message it did not make).
const decodeQuietly = (codec, data) => {
  try {
    return codec.decode(data);
  } catch {
    return null;
  }
};

class ClientEventTransport extends ClientTransport {
  static instance = null;

  // A port has no handshake, so the revision rides the first ping and its
  // pong (protocol.md#versioning): until the other end — the worker proxy,
  // or a server the port is attached to — said it reads framed messages,
  // this side sends none. A 1.0 worker answers a plain pong.
  revision = 1;

  #port = null;
  #worker = null;
  // Set by a pong that names a revision at all: a 2.x end, which also
  // understands the goodbye a 1.0 proxy would throw on.
  #modern = false;

  // @deprecated Kept as the class-level singleton it always was, for code
  // that still calls it — but connect() no longer does: each connect({
  // worker }) builds its own transport, so the one returned here belongs to
  // no client. Use `new WrpcClient.transport.event(url)`.
  static getInstance(url) {
    if (ClientEventTransport.instance) {
      return ClientEventTransport.instance;
    }
    const transport = new ClientEventTransport(url);
    ClientEventTransport.instance = transport;
    return transport;
  }

  async open(options = {}) {
    if (this.active) return;
    const worker = options.worker ?? this.#worker;
    if (!worker) throw new Error('Worker not provided');
    // A SharedWorker is reached through its `port`; a ServiceWorker, a
    // dedicated Worker or a raw MessagePort posts directly. Resolved once,
    // so online()/offline() and every reopen address the same target.
    this.#worker = worker.port ?? worker;
    const { port1, port2 } = new MessageChannel();
    this.#port = port1;
    this.revision = 1;
    this.#modern = false;
    // What this side reads: 2, or 1 under `attachments: false` / a codec.
    const { codec } = this;
    const mine = this.attachments !== false && !codec ? 2 : 1;
    let negotiating = true;
    port1.addEventListener('message', ({ data }) => {
      if (data === undefined) return;
      // Only until the first pong: it answers the ping below, and is the
      // transport's — the core never asked for it. Under a packet codec it
      // is the codec's, like everything the other end writes.
      if (negotiating) {
        const pong = codec ? decodeQuietly(codec, data) : isPongText(data) ? jsonParse(data) : null;
        if (pong?.type === 'pong') {
          negotiating = false;
          this.#modern = pong.v !== undefined;
          this.revision = pong.v === 2 && mine === 2 ? 2 : 1;
          return;
        }
      }
      this.emit('message', data);
    });
    port1.start();
    // Declared headers travel in the connect message for the port's
    // consumer. The built-in proxy (client/proxy.js) ignores them — its own
    // WrpcClient carries its own options — but a custom consumer that
    // attaches ports to an RpcServer can hand them to attachPort.
    const connect = { type: 'wrpc:connect' };
    if (options.headers) connect.headers = options.headers;
    if (options.meta) connect.meta = options.meta;
    this.#worker.postMessage(connect, [port2]);
    // First on the port, before anything the core sends: by the time a
    // `load()` is answered the revision is known. Not waited for — a call
    // sent before the pong leaves as revision 1, which every end reads. A
    // server under a packet codec decodes every message with it: a JSON
    // ping was a malformed packet there, answered with an id-less 500.
    const ping = { type: 'ping', v: mine };
    port1.postMessage(codec ? codec.encode(ping) : JSON.stringify(ping));
    this.active = true;
    this.emit('open');
  }

  close() {
    // A second close (terminate() after close(), or #openOrClose cleaning up
    // an open() that threw before a port existed) has nothing to close —
    // and must not replace the original error with a TypeError.
    if (!this.active) return;
    this.active = false;
    // A goodbye first: the worker releases what this page was waiting for
    // on it, where a MessagePort's own close event may never fire. Only to
    // an end that named a revision: a 1.0 proxy reads every message as a
    // packet and throws on this one.
    try {
      if (this.#modern) this.#port.postMessage({ type: 'wrpc:close' });
    } catch {
      // Already closed.
    }
    this.#port.close();
    this.#port = null;
    this.emit('close');
  }

  online() {
    if (this.#worker) this.#worker.postMessage({ type: 'wrpc:online' });
  }

  offline() {
    if (this.#worker) this.#worker.postMessage({ type: 'wrpc:offline' });
  }

  write(data) {
    if (!this.#port) throw new Error('Not connected');
    this.#port.postMessage(data);
  }
}

// Assigned INTO the class's null-proto registry, never replacing it: the
// sse subpath registers the same way, and require order stops mattering.
Object.assign(WrpcClient.transport, {
  ws: ClientWsTransport,
  http: ClientHttpTransport,
  event: ClientEventTransport,
});

module.exports = { ClientWsTransport, ClientHttpTransport, ClientEventTransport };
