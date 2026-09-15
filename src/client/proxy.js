'use strict';

// The worker-side proxy (Service Worker, SharedWorker or dedicated Worker):
// forwards packets between a page's WrpcClient and a worker-held
// connection. Shares nothing with WrpcClient beyond calling connect() —
// which is why it lives in its own module.

const { Emitter, jsonParse } = require('../utils.js');
const { WrpcClient, CALL_TIMEOUT, normalizeReconnect } = require('./core.js');

class WrpcClientProxy extends Emitter {
  #ports = new Set();
  #pending = new Map();
  #connection = null;
  #callTimeout = CALL_TIMEOUT;
  #reconnect = null;
  #heartbeat = undefined;
  #logger = undefined;
  #telemetry = undefined;
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
    const { callTimeout, heartbeat, logger, telemetry, url } = options;
    if (callTimeout) this.#callTimeout = callTimeout;
    this.#reconnect = normalizeReconnect(options);
    this.#heartbeat = heartbeat;
    // The proxy rebuilds its own options bag, so anything not forwarded here
    // is silently lost on the connection it owns.
    this.#logger = logger;
    this.#telemetry = telemetry;
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

  async open() {
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
      proxy: (data, packet) => this.#proxyPacket(data, packet),
    };
    this.#connection = await WrpcClient.connect(url, options);
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
        this.#handleMessage(messageEvent, port);
      });
      // Best effort: the page half closing fires `close` here in current
      // engines (and in Node), so a closed tab does not pin its port — or
      // the answers it was still waiting for — for the life of the worker.
      port.addEventListener('close', () => {
        this.#ports.delete(port);
        for (const [id, pending] of this.#pending) {
          if (pending === port) this.#pending.delete(id);
        }
      });
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
    await this.open();
    if (!this.#connection || !this.#connection.active) {
      throw new Error('Not connected to server');
    }
    const packet = jsonParse(data);
    if (!packet) throw new Error('Invalid JSON packet');
    // The worker is the page's peer: it answers the page's heartbeat itself
    // rather than paying for a round trip to the server for every port.
    if (packet.type === 'ping') return void port.postMessage(JSON.stringify({ type: 'pong' }));
    if (packet.type === 'pong') return;
    if (!packet.id) throw new Error('Invalid JSON packet');
    this.#pending.set(packet.id, port);
    this.#connection.write(data);
  }

  // `packet` is the already-parsed form of `data` (the client parses once);
  // it is null when the frame was not JSON at all.
  #proxyPacket(data, packet) {
    if (typeof data !== 'string') return void this.#broadcast(data);
    const parsed = packet ?? jsonParse(data);
    if (!parsed) return void this.#broadcast(data);
    const { type, id, status } = parsed;
    if (type === 'event') return void this.#broadcast(data);
    const port = this.#pending.get(id);
    if (!port) return void this.#broadcast(data);
    port.postMessage(data);
    // `end` is a subscription's terminal packet, so it releases its slot the
    // same way a callback does — otherwise every subscription a page opens
    // pins a port reference in the worker for the life of the connection.
    if (type === 'callback' || type === 'end') return void this.#pending.delete(id);
    if (type !== 'stream') return;
    const streamDone = status === 'end' || status === 'terminate';
    if (streamDone) this.#pending.delete(id);
  }

  #broadcast(data, excludePort = null) {
    for (const port of this.#ports) {
      if (port === excludePort) continue;
      port.postMessage(data);
    }
  }
}

module.exports = { WrpcClientProxy };
