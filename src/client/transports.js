'use strict';

// The three built-in client transports, registered on WrpcClient.transport
// exactly the way the SSE subpath registers its own — the registry is the
// one seam every transport, built-in or not, goes through.

const { WrpcClient, ClientTransport, WRPC_PROTOCOL, metaHeaders, connectUrl } = require('./core.js');
const { jsonParse } = require('../utils.js');
const { createWsCompression } = require('./wsCompression.js');

// Mirrors the server's metaMaxBytes default — see connectUrl in core.js for
// the query carrier; the http leg applies the same cap to its header block.
const META_MAX = 2048;
const { WebSocket } = globalThis;

class ClientWsTransport extends ClientTransport {
  // The one transport that can die without saying so.
  heartbeat = true;

  #socket = null;
  #opening = null;
  // Per-message compression of what THIS side sends (wsCompression.js —
  // a stub in a browser): the offer goes out in a ping on open, and frames
  // change only once the server's pong agreed. Re-negotiated per open.
  #compression = null;
  #negotiating = false;

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
      // The client OFFERS the protocol revision; the server echoes it (see
      // protocol.md#versioning). `protocols` overrides the offer, and an
      // empty array offers nothing — an escape hatch for a proxy that
      // mangles the header. The selected protocol lands on `this.protocol`.
      let protocols = options.protocols ?? [WRPC_PROTOCOL];
      // The Authorization header is the ONE declared name that is a secret,
      // and the connect URL lands in proxy access logs. RFC 6455 gives ws a
      // header that survives the WHATWG constructor — the subprotocol offer
      // — so a Bearer credential rides as `wrpc.bearer.<token>` and is
      // stripped from the wrpc_h bag (the server's bearer transport reads
      // sec-websocket-protocol first). A token outside the RFC 7230 token
      // charset cannot be a subprotocol name and falls back to the query,
      // with the loud caveat below.
      let bag = options.headers;
      const auth = bag?.authorization;
      const bearer = typeof auth === 'string' && auth.startsWith('Bearer ') ? auth.slice(7) : null;
      if (bearer && /^[!#$%&'*+.^_`|~A-Za-z0-9-]+$/.test(bearer)) {
        protocols = [...protocols, `wrpc.bearer.${bearer}`];
        bag = { ...bag };
        delete bag.authorization;
        if (Object.keys(bag).length === 0) bag = null;
      }
      // Connection-phase headers ride as ONE query parameter each: the
      // WHATWG WebSocket constructor cannot set real headers, in the browser
      // by spec and in Node because the client uses the same globalThis
      // implementation (connectUrl, shared with the WebTransport transport).
      const url = connectUrl(this.url, bag, options.meta, this.log);
      const socket = protocols.length > 0 ? new WebSocket(url, protocols) : new WebSocket(url);
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
        if (this.#opening) {
          this.#opening = null;
          this.emit('error', error);
          return void reject(new Error('Connection closed'));
        }
        if (!this.active) return;
        this.active = false;
        this.emit('close', error);
      };
      const onOpen = () => {
        this.protocol = socket.protocol || '';
        // First on the wire, before the core's own open handler sends
        // anything: the pong that answers it is what turns compression on.
        if (this.#compression !== null) socket.send(this.#compression.offer);
        this.active = true;
        this.emit('open');
        this.#opening = null;
        resolve();
      };
      socket.addEventListener('open', onOpen, { once: true });
      socket.addEventListener('close', onClose, { once: true });
      socket.addEventListener('error', onClose, { once: true });
      socket.addEventListener('message', ({ data }) => {
        // Only until the first pong answered the offer — a plain pong is a
        // no, and either way nothing is inspected after that.
        if (this.#negotiating && this.#compression.accept(data) !== null) this.#negotiating = false;
        this.emit('message', data);
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
    this.emit('close');
    socket?.close();
  }

  write(data) {
    if (!this.active) throw new Error('Not connected');
    if (this.#compression !== null) {
      const frame = this.#compression.encode(data);
      if (frame !== null) return void this.#socket.send(frame);
    }
    this.#socket.send(data);
  }
}

class ClientHttpTransport extends ClientTransport {
  // One request, one response: nothing to cancel or subscribe on.
  persistent = false;
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
    const options = { method: 'POST', headers, body: data };
    const doFetch = this.fetch;
    const send = async () => {
      try {
        const res = await doFetch(this.url, options);
        // A frame answer (a result holding bytes) is read as bytes and
        // handed on as one; the core classifies it.
        if (res.ok && res.headers.get('content-type') === 'application/octet-stream') {
          return void this.emit('message', new Uint8Array(await res.arrayBuffer()));
        }
        const text = await res.text();
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
        this.failPackets(data, 503);
      }
    };
    send();
  }
}

class ClientEventTransport extends ClientTransport {
  static instance = null;

  #port = null;
  #worker = null;

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
    port1.addEventListener('message', ({ data }) => {
      if (data === undefined) return;
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
    this.active = true;
    this.emit('open');
  }

  close() {
    // A second close (terminate() after close(), or #openOrClose cleaning up
    // an open() that threw before a port existed) has nothing to close —
    // and must not replace the original error with a TypeError.
    if (!this.active) return;
    this.active = false;
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
