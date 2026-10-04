'use strict';

// The worker-side proxy (Service Worker, SharedWorker or dedicated Worker):
// forwards packets between a page's WrpcClient and a worker-held
// connection. Shares nothing with WrpcClient beyond calling connect() —
// which is why it lives in its own module.

const { Emitter, jsonParse } = require('../utils.js');
const { WrpcClient, CALL_TIMEOUT, normalizeReconnect } = require('./core.js');
const { isAttachmentsFrame, decodeAttachments } = require('../attachments.js');

// The page's packet: JSON text, or an attachments frame carrying one (a
// call with bytes in its args). Anything else — bytes that are no frame,
// text that is no JSON — is null: not a packet.
const parsePacket = (data) => {
  if (typeof data === 'string') return jsonParse(data);
  const view = data instanceof Uint8Array ? data : data instanceof ArrayBuffer ? new Uint8Array(data) : null;
  if (view === null || !isAttachmentsFrame(view)) return null;
  try {
    return decodeAttachments(view);
  } catch {
    return null;
  }
};

class WrpcClientProxy extends Emitter {
  #ports = new Set();
  // The ports whose page reads framed messages — it said revision 2 on its
  // first ping (protocol.md#versioning). A page that did not is a 1.0 page,
  // or one that opted out: a frame from upstream reaches it as JSON.
  #modern = new WeakSet();
  #pending = new Map();
  // The ids of the pages' open subscriptions: what a port's release
  // unsubscribes upstream, where a call is cancelled.
  #feeds = new Set();
  // The ids of the pages' calls in flight: what a lost upstream answers.
  #calls = new Set();
  // The connect in progress: pages whose first packets arrive together
  // share it, where each used to open its own upstream socket.
  #opening = null;
  #connection = null;
  #callTimeout = CALL_TIMEOUT;
  #reconnect = null;
  #heartbeat = undefined;
  #logger = undefined;
  #telemetry = undefined;
  #encryption = undefined;
  #url = undefined;

  // The control bus: `wrpc:*` objects, a transferred port riding on
  // `wrpc:connect`. On a ServiceWorker (and a dedicated Worker) it is `self`;
  // on a SharedWorker it is the per-page port the `connect` event hands
  // over. Optional chaining because a page can post anything on that port.
  #onControl = (event) => {
    const type = event.data?.type;
    if (typeof type === 'string' && type.startsWith('wrpc')) this.#handleEvent(event);
  };

  constructor(options = {}) {
    super();
    const { callTimeout, heartbeat, logger, telemetry, url, encryption } = options;
    if (callTimeout) this.#callTimeout = callTimeout;
    this.#reconnect = normalizeReconnect(options);
    this.#heartbeat = heartbeat;
    // The proxy rebuilds its own options bag, so anything not forwarded here
    // is silently lost on the connection it owns.
    this.#logger = logger;
    this.#telemetry = telemetry;
    // The hop that leaves the machine is this worker's, so session
    // encryption (createEncryption) is configured here, not on the page.
    this.#encryption = encryption;
    this.#url = url;
    if (typeof self === 'undefined') {
      throw new Error('WrpcClientProxy must run in a worker context');
    }
    // Both listeners, no context sniffing: a ServiceWorker never fires
    // `connect`, a SharedWorker never fires `message` on self.
    self.addEventListener('message', this.#onControl);
    self.addEventListener('connect', (event) => {
      const port = event.ports[0];
      port.addEventListener('message', this.#onControl);
      port.start();
    });
  }

  open() {
    this.#opening ??= this.#open().finally(() => {
      this.#opening = null;
    });
    return this.#opening;
  }

  async #open() {
    if (this.#connection) {
      if (this.#connection.active) return;
      await this.#connection.open();
      return;
    }
    const protocol = self.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = this.#url ?? `${protocol}//${self.location.host}`;
    const options = {
      callTimeout: this.#callTimeout,
      reconnect: this.#reconnect,
      heartbeat: this.#heartbeat,
      logger: this.#logger,
      telemetry: this.#telemetry,
      encryption: this.#encryption,
      proxy: (data, packet) => this.#proxyPacket(data, packet),
    };
    this.#connection = await WrpcClient.connect(url, options);
    // What the pages were waiting for died with the connection, and the
    // client above answers only its OWN calls — a page's went up as raw
    // packets. Each page hears it now, where it used to wait out its
    // callTimeout: a call its error, a subscription its end. A reconnect
    // starts with none.
    this.#connection.on('close', () => {
      const error = { message: 'The worker lost its connection to the server', code: 503 };
      for (const [id, port] of this.#pending) {
        if (this.#feeds.has(id)) port.postMessage(JSON.stringify({ type: 'end', id, error }));
        else if (this.#calls.has(id)) port.postMessage(JSON.stringify({ type: 'callback', id, error }));
      }
      this.#pending.clear();
      this.#feeds.clear();
      this.#calls.clear();
    });
  }

  // A page is gone: its port, and everything it was waiting for. What is
  // still running upstream for it is stopped — its subscriptions
  // unsubscribed, its calls cancelled — instead of running for the life of
  // the connection with nobody to hear the answer.
  #release(port) {
    if (!this.#ports.delete(port)) return;
    const connection = this.#connection;
    const live = connection !== null && connection.active;
    for (const [id, pending] of this.#pending) {
      if (pending !== port) continue;
      this.#pending.delete(id);
      this.#calls.delete(id);
      const feed = this.#feeds.delete(id);
      if (live) connection.send(feed ? { type: 'unsubscribe', id } : { type: 'cancel', id });
    }
  }

  close() {
    if (!this.#connection) return;
    this.#connection.close();
    this.#connection = null;
  }

  #handleEvent(event) {
    const { type } = event.data;
    if (type === 'wrpc:connect') {
      const port = event.ports[0];
      if (!port) throw new Error('MessagePort not provided');
      this.#ports.add(port);
      port.addEventListener('message', (messageEvent) => {
        // The page's transport says goodbye before it closes its port: a
        // MessagePort's own `close` is what older engines never fire.
        if (messageEvent.data?.type === 'wrpc:close') return void this.#release(port);
        // A failure to forward — nothing to connect to, a packet that is
        // none — answers the caller when there is one, instead of leaving
        // it to its callTimeout, and is never an unhandled rejection.
        this.#handleMessage(messageEvent, port).catch((error) => this.#refuse(messageEvent.data, port, error));
      });
      // Best effort: the page half closing fires `close` here in current
      // engines (and in Node), so a closed tab does not pin its port — or
      // the answers it was still waiting for — for the life of the worker.
      port.addEventListener('close', () => this.#release(port));
      port.start();
      return;
    }
    if (type === 'wrpc:online') WrpcClient.online();
    else if (type === 'wrpc:offline') WrpcClient.offline();
    else throw new Error(`Unknown event: ${type}`);
  }

  async #handleMessage(event, port) {
    const { data } = event;
    if (data === undefined) throw new Error('Message data is undefined');
    const packet = parsePacket(data);
    if (!packet) throw new Error('Invalid JSON packet');
    // The worker is the page's peer: it answers the page's heartbeat itself
    // rather than paying for a round trip to the server for every port. A
    // ping that names a revision is the page's first, and is answered with
    // this end's: the proxy reads frames, whatever is upstream of it.
    if (packet.type === 'ping') {
      if (packet.v === undefined) return void port.postMessage('{"type":"pong"}');
      if (packet.v === 2) this.#modern.add(port);
      return void port.postMessage('{"type":"pong","v":2}');
    }
    if (packet.type === 'pong') return;
    if (!packet.id) throw new Error('Invalid JSON packet');
    await this.open();
    if (!this.#connection || !this.#connection.active) {
      throw new Error('Not connected to server');
    }
    this.#pending.set(packet.id, port);
    if (packet.type === 'subscribe') this.#feeds.add(packet.id);
    else if (packet.type === 'call') this.#calls.add(packet.id);
    // What the page wrote goes upstream as it is: a frame stays a frame —
    // unless the server speaks revision 1 and reads none, where it leaves
    // as the JSON of the packet it carries.
    if (typeof data === 'string') return void this.#connection.write(data);
    if (this.#connection.revision !== 2) return void this.#connection.write(JSON.stringify(packet));
    this.#connection.write(data instanceof ArrayBuffer ? new Uint8Array(data) : data);
  }

  // The page's call could not be forwarded: it hears a coded error now
  // rather than its callTimeout later. Anything without a caller is dropped.
  #refuse(data, port, error) {
    const packet = parsePacket(data);
    if (!packet || packet.type !== 'call' || typeof packet.id !== 'string') return;
    const code = Number.isInteger(error?.code) ? error.code : 503;
    const message = typeof error?.message === 'string' ? error.message : 'Proxy could not forward the call';
    port.postMessage(JSON.stringify({ type: 'callback', id: packet.id, error: { message, code } }));
  }

  // `packet` is the already-parsed form of `data` (the client parses once —
  // for an attachments frame `data` is the frame and `packet` the packet it
  // carries, routed exactly like its JSON twin); it is null when the frame
  // was no packet at all, and a batch array is broadcast as it always was.
  #proxyPacket(data, packet) {
    const parsed = packet ?? (typeof data === 'string' ? jsonParse(data) : null);
    if (!parsed || Array.isArray(parsed)) return void this.#broadcast(data, parsed);
    const { type, id, status } = parsed;
    if (type === 'event') return void this.#broadcast(data, parsed);
    const port = this.#pending.get(id);
    if (!port) {
      // A server-opened stream's packets are for whichever page picks
      // them up (its callback carried the id). Anything else with an id
      // nobody waits for — the callback of a page that left, the data of
      // a feed it unsubscribed — is nobody's, and used to go to every page.
      if (type === 'stream') this.#broadcast(data, parsed);
      return;
    }
    this.#post(port, data, parsed);
    // `end` is a subscription's terminal packet, so it releases its slot the
    // same way a callback does — otherwise every subscription a page opens
    // pins a port reference in the worker for the life of the connection.
    if (type === 'callback' || type === 'end') {
      this.#pending.delete(id);
      this.#feeds.delete(id);
      this.#calls.delete(id);
      return;
    }
    if (type !== 'stream') return;
    const streamDone = status === 'end' || status === 'terminate';
    if (streamDone) this.#pending.delete(id);
  }

  // One message to one page. `parsed` is the packet `data` carries when it
  // is a frame: a page that reads no frames is posted the packet's JSON —
  // what a revision-1 server would have sent it.
  #post(port, data, parsed) {
    const framed = typeof data !== 'string' && parsed !== null && !this.#modern.has(port);
    port.postMessage(framed ? JSON.stringify(parsed) : data);
  }

  #broadcast(data, parsed = null) {
    for (const port of this.#ports) this.#post(port, data, parsed);
  }
}

module.exports = { WrpcClientProxy };
