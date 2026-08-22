'use strict';

// The three built-in client transports, registered on WrpcClient.transport
// exactly the way the SSE subpath registers its own — the registry is the
// one seam every transport, built-in or not, goes through.

const { WrpcClient, ClientTransport, WRPC_PROTOCOL } = require('./core.js');
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
      const protocols = options.protocols ?? [WRPC_PROTOCOL];
      // Connection-phase headers ride as ONE query parameter: the WHATWG
      // WebSocket constructor cannot set real headers, in the browser by
      // spec and in Node because the client uses the same globalThis
      // implementation. The server reads observed upgrade headers first and
      // this parameter only for names they do not carry, so a transport
      // that CAN send real headers needs no query at all. Loud caveat: the
      // connect URL lands in proxy access logs — a device id belongs here,
      // a secret does not.
      const params = [];
      if (options.headers) params.push(`wrpc_h=${encodeURIComponent(JSON.stringify(options.headers))}`);
      if (options.meta) params.push(`wrpc_meta=${encodeURIComponent(JSON.stringify(options.meta))}`);
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
  // Connection-phase headers and metadata, resolved per open — here they
  // ride as REAL request headers on every packet POST and REST leg.
  headers = null;
  meta = null;

  async open(options = {}) {
    this.headers = options.headers ?? null;
    // Encoded once per open: a header value must stay latin-1, so the meta
    // object travels as percent-encoded JSON.
    this.meta = options.meta ? encodeURIComponent(JSON.stringify(options.meta)) : null;
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
  async request(method, url, body, signal, rest = null) {
    // Declared first, wire headers after: the protocol's own always win.
    const headers = { ...this.headers, 'Content-Type': rest?.contentType ?? 'application/json' };
    if (this.meta) headers['x-wrpc-meta'] = this.meta;
    const options = body === undefined ? { method, headers, signal } : { method, headers, body, signal };
    const res = await fetch(url, options);
    if (rest) return { status: res.status, body: new Uint8Array(await res.arrayBuffer()) };
    return { status: res.status, text: await res.text() };
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

  write(data) {
    const headers = { ...this.headers, 'Content-Type': this.codec?.contentType ?? 'application/json' };
    if (this.meta) headers['x-wrpc-meta'] = this.meta;
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
        this.#fail(data, res.status);
      } catch (error) {
        this.emit('error', error);
        this.#fail(data, 503);
      }
    };
    send();
  }

  // Synthesizes an error callback for every call packet the failed request
  // carried — the transport is the only party that knows which ids just
  // died with it. Both directions speak the codec when one is configured.
  #fail(data, status) {
    const parsed = this.#decode(data);
    if (!parsed) return;
    const packets = Array.isArray(parsed) ? parsed : [parsed];
    const answers = [];
    for (const packet of packets) {
      if (!packet || typeof packet !== 'object' || typeof packet.id !== 'string') continue;
      answers.push({
        type: 'callback',
        id: packet.id,
        error: { message: `HTTP request failed (${status})`, code: status },
      });
    }
    if (answers.length === 0) return;
    const frame = Array.isArray(parsed) ? answers : answers[0];
    this.emit('message', this.codec ? this.codec.encode(frame) : JSON.stringify(frame));
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

WrpcClient.transport = {
  ws: ClientWsTransport,
  http: ClientHttpTransport,
  event: ClientEventTransport,
};

module.exports = { ClientWsTransport, ClientHttpTransport, ClientEventTransport };
