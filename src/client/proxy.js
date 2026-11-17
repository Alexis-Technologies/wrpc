'use strict';

// The worker-side proxy (Service Worker, SharedWorker or dedicated Worker):
// forwards packets between a page's WrpcClient and a worker-held
// connection. Shares nothing with WrpcClient beyond calling connect() —
// which is why it lives in its own module.

const { Emitter, jsonParse } = require('../utils.js');
const { WrpcClient, CALL_TIMEOUT, normalizeReconnect } = require('./core.js');
const { isAttachmentsFrame, decodeAttachments } = require('../attachments.js');
const { chunkDecode } = require('../chunks.js');
const { MAX_STREAMS } = require('../streams.js');

// What a port posted, as bytes; null for anything that is not.
const viewOf = (data) =>
  data instanceof Uint8Array ? data : data instanceof ArrayBuffer ? new Uint8Array(data) : null;

// The page's packet: JSON text, or an attachments frame carrying one (a
// call with bytes in its args). Anything else — bytes that are no frame,
// text that is no JSON — is null: not a packet.
const parsePacket = (data) => {
  if (typeof data === 'string') return jsonParse(data);
  const view = viewOf(data);
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
  // Binary streams cross as packets AND raw chunks, and a chunk carries
  // nothing but its stream id — so the id is routed by its announcement.
  // An upload's chunks go up only from the page that announced it; a
  // server stream's go down where its announcement went: to the page that
  // waited on the id, or to every page there was — the pages that HOLD it,
  // a Set that a leaving page is taken out of. A page that connects later
  // was never told of the stream, and is sent none of it.
  #uploads = new Map();
  #downloads = new Map();
  // The server streams whose sender asked to hear when nobody reads them
  // (`cancel` on the announcement): what #drop may answer.
  #askers = new Set();
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
      proxy: (data, packet, chunk) => (chunk ? this.#proxyChunk(data) : this.#proxyPacket(data, packet)),
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
      // A server stream will send no more: its readable ends now instead of
      // waiting for bytes forever.
      for (const [id, holders] of this.#downloads) {
        this.#to(holders, JSON.stringify({ type: 'stream', id, status: 'terminate' }));
      }
      this.#pending.clear();
      this.#feeds.clear();
      this.#calls.clear();
      this.#uploads.clear();
      this.#downloads.clear();
      this.#askers.clear();
    });
  }

  // A page is gone: its port, and everything it was waiting for. What is
  // still running upstream for it is stopped — its subscriptions
  // unsubscribed, its calls cancelled, its uploads terminated (the server
  // would wait for their bytes forever) — instead of running for the life
  // of the connection with nobody to hear the answer.
  #release(port) {
    if (!this.#ports.delete(port)) return;
    const connection = this.#connection;
    const live = connection !== null && connection.active;
    for (const [id, pending] of this.#pending) {
      if (pending !== port) continue;
      this.#pending.delete(id);
      this.#calls.delete(id);
      const feed = this.#feeds.delete(id);
      const upload = this.#uploads.delete(id);
      if (live) {
        connection.send(
          upload
            ? { type: 'stream', id, status: 'terminate' }
            : feed
              ? { type: 'unsubscribe', id }
              : { type: 'cancel', id },
        );
      }
    }
    for (const [id, owner] of this.#uploads) if (owner === port) this.#uploads.delete(id);
    // A server stream this page held: with its last holder gone nobody is
    // left to read it, and the server — which went on sending it to a
    // worker that dropped every chunk — is told to stop.
    for (const [id, holders] of this.#downloads) {
      if (holders.delete(port) && holders.size === 0) this.#drop(id);
    }
  }

  // A server stream nobody holds any more is forgotten, and its sender told
  // — once, and only one that asked: the answer a page gives for a stream
  // its reader let go of, from the worker, for the pages that are gone.
  #drop(id) {
    this.#downloads.delete(id);
    if (this.#askers.delete(id)) this.#answer(id, 'Stream is no longer read', 499);
  }

  // The worker's own word on a server stream, said as a page says it: a
  // `callback` error on the stream's id.
  #answer(id, message, code) {
    const connection = this.#connection;
    if (connection === null || !connection.active) return;
    connection.send({ type: 'callback', id, error: { message, code } });
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
    if (!packet) return this.#upload(data, port);
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
    // A batch — the page's `batch: true` — is its calls in one frame: each
    // is this page's like a call sent alone, and the frame goes up whole.
    // And an event carries no id, by protocol: it is forwarded, answered by
    // nothing. Both used to stop at the id check below and vanish: a page's
    // sendEvent() never reached the server, and every batched call waited
    // out its callTimeout.
    if (Array.isArray(packet)) {
      await this.#upstream();
      for (const element of packet) if (element?.id) this.#track(element, port);
      return void this.#forward(data, packet);
    }
    if (packet.type === 'event') {
      await this.#upstream();
      return void this.#forward(data, packet);
    }
    if (!packet.id) throw new Error('Invalid JSON packet');
    // An upload is this page's from its announcement to its end — said
    // before the await, so a chunk posted right behind it finds it.
    if (packet.type === 'stream') {
      if (!packet.status) this.#uploads.set(packet.id, port);
      else if (this.#uploads.get(packet.id) === port) this.#uploads.delete(packet.id);
    }
    await this.#upstream();
    this.#track(packet, port);
    this.#forward(data, packet);
  }

  // Whose answer the packet's id is from now on.
  #track(packet, port) {
    const { id, type } = packet;
    // A stream's end is answered by nothing: the slot its announcement took
    // goes with it. Neither is a callback — the page's answer to an ask, or
    // its word that it no longer reads a server stream: a slot taken for
    // that id would send the stream's terminate to this page alone, when
    // the stream went to every page.
    if (type === 'stream' && packet.status) this.#pending.delete(id);
    else if (type !== 'callback') this.#pending.set(id, port);
    if (type === 'subscribe') this.#feeds.add(id);
    else if (type === 'call') this.#calls.add(id);
  }

  // What the page wrote goes upstream as it is: a frame stays a frame —
  // unless the server speaks revision 1 and reads none, where it leaves as
  // the JSON of the packet it carries.
  #forward(data, packet) {
    if (typeof data === 'string') return void this.#connection.write(data);
    if (this.#connection.revision !== 2) return void this.#connection.write(JSON.stringify(packet));
    this.#connection.write(data instanceof ArrayBuffer ? new Uint8Array(data) : data);
  }

  // The connection the pages' traffic goes up on, opened if it is not.
  async #upstream() {
    await this.open();
    if (!this.#connection || !this.#connection.active) {
      throw new Error('Not connected to server');
    }
  }

  // Not a packet: a chunk of a stream this page announced goes up, behind
  // the same await as the packets around it; anything else is nobody's.
  async #upload(data, port) {
    const view = viewOf(data);
    if (view === null || this.#uploads.get(chunkDecode(view).id) !== port) throw new Error('Invalid JSON packet');
    await this.#upstream();
    this.#connection.write(view);
  }

  // The page's call could not be forwarded: it hears a coded error now
  // rather than its callTimeout later. Anything without a caller is dropped.
  #refuse(data, port, error) {
    const parsed = parsePacket(data);
    const code = Number.isInteger(error?.code) ? error.code : 503;
    const message = typeof error?.message === 'string' ? error.message : 'Proxy could not forward the call';
    // Every call of a batch is a call that could not be forwarded.
    for (const packet of Array.isArray(parsed) ? parsed : [parsed]) {
      if (!packet || packet.type !== 'call' || typeof packet.id !== 'string') continue;
      port.postMessage(JSON.stringify({ type: 'callback', id: packet.id, error: { message, code } }));
    }
  }

  // `packet` is the already-parsed form of `data` (the client parses once —
  // for an attachments frame `data` is the frame and `packet` the packet it
  // carries, routed exactly like its JSON twin); it is null when the frame
  // was no packet at all.
  #proxyPacket(data, packet) {
    const parsed = packet ?? (typeof data === 'string' ? jsonParse(data) : null);
    // An array is the answers to a batch, which only a request/response
    // uplink sends back in one frame: each is its own page's, like an answer
    // that came alone — broadcast, every page got every other page's.
    if (typeof data === 'string' && Array.isArray(parsed)) {
      for (const element of parsed) if (element) this.#proxyPacket(JSON.stringify(element), element);
      return;
    }
    if (!parsed || Array.isArray(parsed)) return void this.#broadcast(data, parsed);
    const { type, id } = parsed;
    if (type === 'event') return void this.#broadcast(data, parsed);
    const port = this.#pending.get(id);
    if (type === 'stream') return void this.#proxyStream(data, parsed, port);
    // Anything with an id nobody waits for — the callback of a page that
    // left, the data of a feed it unsubscribed — is nobody's, and used to go
    // to every page.
    if (!port) return;
    this.#post(port, data, parsed);
    // `end` is a subscription's terminal packet, so it releases its slot the
    // same way a callback does — otherwise every subscription a page opens
    // pins a port reference in the worker for the life of the connection.
    if (type === 'callback' || type === 'end') {
      this.#pending.delete(id);
      this.#feeds.delete(id);
      this.#calls.delete(id);
    }
  }

  // A server stream's packet. Its announcement is where its chunks will
  // follow: the page that waited on the id, or — a server-opened stream is
  // for whichever page picks it up, its callback carried the id — every
  // page there is. Its end goes where the stream went.
  #proxyStream(data, parsed, port) {
    const { id, status } = parsed;
    if (status) {
      // Where the stream went — or, for one this worker saw no announcement
      // of, to the page that waits on the id. One nobody holds and nobody
      // waits for is nobody's: it used to go to every page, each of which
      // had never heard of the stream.
      const holders = this.#downloads.get(id) ?? (port ? [port] : null);
      this.#downloads.delete(id);
      this.#askers.delete(id);
      if (holders !== null) for (const each of holders) this.#post(each, data, parsed);
      if (port && (status === 'end' || status === 'terminate')) this.#pending.delete(id);
      return;
    }
    // Capped, as a page caps what its server opens on it: every stream the
    // server announces and does not end is an entry here, and the server
    // alone decides how many — two hundred thousand of them were 48 MiB in
    // the worker, each posted to every page. Past the cap an announcement
    // is handed to nobody, and a sender that asked to hear is answered as a
    // page would answer it.
    if (this.#downloads.size >= MAX_STREAMS) {
      if (parsed.cancel === true) this.#answer(id, 'Too many open streams', 429);
      return;
    }
    const holders = new Set(port ? [port] : this.#ports);
    this.#downloads.set(id, holders);
    if (parsed.cancel === true) this.#askers.add(id);
    // No page at all: nobody will read it, and the server is told at once.
    if (holders.size === 0) return void this.#drop(id);
    for (const each of holders) this.#post(each, data, parsed);
  }

  // A chunk of a server stream: where its announcement went, or nowhere
  // for a stream that was never announced, or already ended.
  #proxyChunk(view) {
    const holders = this.#downloads.get(chunkDecode(view).id);
    if (holders !== undefined) this.#to(holders, view);
  }

  // As it is, to each of the pages that hold a stream.
  #to(holders, message) {
    for (const port of holders) port.postMessage(message);
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
