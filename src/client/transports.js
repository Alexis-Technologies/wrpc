'use strict';

// The three built-in client transports, registered on WrpcClient.transport
// exactly the way the SSE subpath registers its own — the registry is the
// one seam every transport, built-in or not, goes through.

const { WrpcClient, ClientTransport, WRPC_PROTOCOL, metaHeaders } = require('./core.js');
const { HEADERS_PARAM, META_PARAM } = require('../wire.js');

// Mirrors the server's metaMaxBytes default: past it the server drops the
// entire declared bag, so refusing here is the difference between a visible
// warning and a label that silently stopped arriving.
const META_MAX = 2048;
const { jsonParse } = require('../utils.js');
const { WebSocket } = globalThis;

class ClientWsTransport extends ClientTransport {
  // The one transport that can die without saying so.
  heartbeat = true;

  #socket = null;
  #opening = null;

  async open(options = {}) {
    if (this.active) return Promise.resolve();
    if (this.#opening) return this.#opening;
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
      // Connection-phase headers ride as ONE query parameter: the WHATWG
      // WebSocket constructor cannot set real headers, in the browser by
      // spec and in Node because the client uses the same globalThis
      // implementation. The server reads observed upgrade headers first and
      // this parameter only for names they do not carry, so a transport
      // that CAN send real headers needs no query at all. Loud caveat: the
      // connect URL lands in proxy access logs — a device id belongs here,
      // a secret does not.
      const params = [];
      // Capped like the http leg: past metaMaxBytes the server drops the
      // ENTIRE bag (measured over the whole query), so sending it anyway
      // would be a silent loss on the side that cannot see it. The refusal
      // keeps the connection working, un-labelled, and says so.
      const declare = (param, bag) => {
        const value = encodeURIComponent(JSON.stringify(bag));
        const bytes = param.length + 1 + value.length;
        if (bytes > META_MAX) return void this.log?.warn({ event: 'meta.oversize', param, bytes });
        params.push(`${param}=${value}`);
      };
      if (bag) declare(HEADERS_PARAM, bag);
      if (options.meta) declare(META_PARAM, options.meta);
      const url = params.length > 0 ? `${this.url}${this.url.includes('?') ? '&' : '?'}${params.join('&')}` : this.url;
      const socket = protocols.length > 0 ? new WebSocket(url, protocols) : new WebSocket(url);
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
        this.active = true;
        this.emit('open');
        this.#opening = null;
        resolve();
      };
      socket.addEventListener('open', onOpen, { once: true });
      socket.addEventListener('close', onClose, { once: true });
      socket.addEventListener('error', onClose, { once: true });
      socket.addEventListener('message', ({ data }) => {
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

  async open(options = {}) {
    this.headers = options.headers ?? null;
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
    const res = await fetch(url, init);
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
    const headers = { ...this.headers, ...block, 'Content-Type': this.codec?.contentType ?? 'application/json' };
    const options = { method: 'POST', headers, body: data };
    const send = async () => {
      try {
        const res = await fetch(this.url, options);
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
    if (!worker) throw new Error('Service Worker not provided');
    this.#worker = worker;
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
