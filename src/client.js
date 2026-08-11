'use strict';

const { Emitter, jsonParse, backoffDelay } = require('./utils.js');
const { generateUUID } = require('./runtime/node.js');
const { WebSocket } = globalThis;
const { chunkDecode } = require('./chunks.js');
const { WrpcReadable, WrpcWritable } = require('./streams.js');

const CALL_TIMEOUT = 7 * 1000;
const RECONNECT_TIMEOUT = 2 * 1000;

const RECONNECT = {
  minDelay: RECONNECT_TIMEOUT,
  maxDelay: 30 * 1000,
  factor: 2,
  jitter: true,
  retries: Infinity,
};

// App-level heartbeat. A browser WebSocket exposes no protocol-level ping,
// so a connection that died without a close frame (dropped NAT mapping,
// suspended laptop, a proxy that stopped forwarding) looks perfectly open
// from JavaScript until the first call times out. Sending `{type:'ping'}`
// and expecting `{type:'pong'}` is the only liveness signal available.
const HEARTBEAT = { interval: 30 * 1000, timeout: 10 * 1000 };

// Heartbeat timers are unref'd so the beat itself never keeps a process
// alive — the live socket is already a ref'd handle, so an idle-but-connected
// client still holds the loop open. The RECONNECT timer is deliberately NOT
// unref'd: during an outage there is no socket left to hold the loop, and a
// process whose only work is a wrpc client must not exit mid-reconnect.
// (In a browser setTimeout returns a number and there is nothing to unref.)
const unref = (timer) => {
  if (timer && typeof timer.unref === 'function') timer.unref();
  return timer;
};

const normalizeReconnect = (options) => {
  const { reconnect, reconnectTimeout } = options;
  if (reconnect === false) return { ...RECONNECT, retries: 0 };
  const base = reconnectTimeout ? { ...RECONNECT, minDelay: reconnectTimeout } : RECONNECT;
  // Both paths share the invariants: a `reconnectTimeout` above the default
  // cap would otherwise silently reconnect FASTER than asked, since the cap
  // wins inside backoffDelay.
  const merged = reconnect ? { ...base, ...reconnect } : { ...base };
  if (!(merged.minDelay > 0)) merged.minDelay = RECONNECT.minDelay;
  if (!(merged.maxDelay >= merged.minDelay)) merged.maxDelay = merged.minDelay;
  return merged;
};

const normalizeHeartbeat = (options) => {
  const { heartbeat } = options;
  if (heartbeat === false || heartbeat === 0) return null;
  if (!heartbeat) return HEARTBEAT;
  const merged = { ...HEARTBEAT, ...heartbeat };
  if (!(merged.interval > 0) || !(merged.timeout > 0)) return null;
  return merged;
};

const toByteView = async (input) => {
  if (typeof input.arrayBuffer === 'function') {
    const buffer = await input.arrayBuffer();
    return new Uint8Array(buffer);
  }
  return new Uint8Array(input);
};

class WrpcError extends Error {
  constructor({ message, code }) {
    super(message);
    this.code = code;
  }
}

class ClientTransport extends Emitter {
  active = false;
  // Opt-in: only a transport that can silently die needs an app-level
  // heartbeat. A request/response transport has nothing to keep alive, and
  // a MessagePort to a Service Worker cannot half-close.
  heartbeat = false;

  constructor(url) {
    super();
    this.url = url;
  }

  send(obj) {
    this.write(JSON.stringify(obj));
  }

  // Drop the connection without waiting for a close handshake. The default
  // is a graceful close; transports that can hang on an unresponsive peer
  // override it.
  terminate() {
    this.close();
  }

  // eslint-disable-next-line class-methods-use-this
  online() {}

  // eslint-disable-next-line class-methods-use-this
  offline() {}
}

class WrpcClient extends Emitter {
  static connections = new Set();
  static isOnline = true;

  static online() {
    WrpcClient.isOnline = true;
    for (const connection of WrpcClient.connections) {
      connection.#transport.online();
      if (!connection.active) {
        connection.open().catch((error) => {
          connection.emit('error', error);
        });
      }
    }
  }

  static offline() {
    WrpcClient.isOnline = false;
    for (const connection of WrpcClient.connections) {
      connection.#transport.offline();
    }
  }

  static initialize() {
    if (typeof window !== 'undefined') {
      window.addEventListener('online', WrpcClient.online);
      window.addEventListener('offline', WrpcClient.offline);
      return;
    }
    if (typeof self !== 'undefined') {
      self.addEventListener('online', WrpcClient.online);
      self.addEventListener('offline', WrpcClient.offline);
    }
  }

  api = {};
  #transport = null;
  #calls = new Map();
  #streams = new Map();
  #callTimeout = CALL_TIMEOUT;
  #reconnect = RECONNECT;
  #reconnectTimer = null;
  #attempt = 0;
  #connected = false;
  #random = Math.random;
  #heartbeat = null;
  #pingTimer = null;
  #pongTimer = null;
  #loaded = new Set();
  #unitMethods = new Map();
  #proxyPacket = null;
  #options = {};

  get active() {
    return this.#transport.active;
  }

  /** How many reconnect attempts have been made since the last open. */
  get attempt() {
    return this.#attempt;
  }

  constructor(url, transport, options = {}) {
    super();
    const { callTimeout, proxy, random } = options;
    if (callTimeout) this.#callTimeout = callTimeout;
    if (proxy) this.#proxyPacket = proxy;
    if (random) this.#random = random; // deterministic jitter in tests
    this.#reconnect = normalizeReconnect(options);
    this.#heartbeat = normalizeHeartbeat(options);
    this.url = url;
    this.#transport = transport;
    this.#options = options;
    this.#bindTransport();
  }

  static async connect(url, options = {}) {
    if (options.worker) {
      const transport = WrpcClient.transport.event.getInstance(url);
      const client = new WrpcClient(url, transport, options);
      await client.open();
      return client;
    }
    const isHttp = url.startsWith('http');
    const Transport = isHttp ? WrpcClient.transport.http : WrpcClient.transport.ws;
    const transport = new Transport(url);
    const client = new WrpcClient(url, transport, options);
    await client.open();
    return client;
  }

  // An 'error' with no listener throws by design (see Emitter), which is
  // right for a synchronous mistake and wrong for a background failure: a
  // throw out of a timer or a promise chain would take the process down for
  // a reconnect that is about to be retried anyway.
  #escalate(error) {
    if (this.listenerCount('error') > 0) return void this.emit('error', error);
    globalThis.console?.error?.(error);
  }

  #bindTransport() {
    this.#transport.on('open', () => {
      clearTimeout(this.#reconnectTimer);
      this.#reconnectTimer = null;
      const attempts = this.#attempt;
      this.#attempt = 0;
      this.#startHeartbeat();
      const reconnected = this.#connected;
      this.#connected = true;
      this.emit('open');
      if (reconnected) this.#restore(attempts).catch((error) => this.#escalate(error));
    });

    this.#transport.on('close', () => {
      this.#stopHeartbeat();
      this.emit('close');
      this.#scheduleReconnect();
    });

    this.#transport.on('error', (error) => {
      this.#escalate(error);
    });

    this.#transport.on('message', (data) => {
      const escalate = (error) => this.#escalate(error);
      if (typeof data === 'string') this.#handlePacket(data).catch(escalate);
      else this.#handleBinary(data).catch(escalate);
    });
  }

  // A reconnected socket is a NEW server-side client: whatever `load()` set
  // up (the introspected method list) belongs to the connection that just
  // died, so it is rebuilt before 'reconnect' is announced. The api unit
  // objects themselves are reused, so event listeners registered on them
  // survive — that is the whole point of reloading rather than reconnecting
  // and leaving `api` quietly stale.
  async #restore(attempts) {
    const units = Array.from(this.#loaded);
    if (units.length > 0) await this.load(...units);
    await this.emit('reconnect', { units, attempts });
  }

  #scheduleReconnect() {
    if (this.active) return;
    if (!WrpcClient.connections.has(this)) return;
    if (this.#reconnectTimer) return;
    const { retries } = this.#reconnect;
    if (this.#attempt >= retries) {
      return void this.emit('reconnect-failed', { attempts: this.#attempt });
    }
    const delay = backoffDelay({ ...this.#reconnect, attempt: this.#attempt, random: this.#random });
    this.#attempt++;
    this.emit('reconnecting', { attempt: this.#attempt, delay });
    // Not unref'd: see the note on `unref` above.
    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = null;
      this.open().catch((error) => {
        // A rejected open never emitted 'close', so nothing else would
        // schedule the next attempt — the loop has to continue here.
        this.#escalate(error);
        this.#scheduleReconnect();
      });
    }, delay);
  }

  // ---------------------------------------------------------------------
  // Heartbeat: ping -> await pong -> ping. One timer is armed at a time, so
  // a stalled connection cannot pile pings up behind the missing pong.

  #startHeartbeat() {
    this.#stopHeartbeat();
    if (!this.#heartbeat || !this.#transport.heartbeat) return;
    this.#armPing();
  }

  #stopHeartbeat() {
    if (this.#pingTimer) clearTimeout(this.#pingTimer);
    if (this.#pongTimer) clearTimeout(this.#pongTimer);
    this.#pingTimer = null;
    this.#pongTimer = null;
  }

  #armPing() {
    this.#pingTimer = unref(
      setTimeout(() => {
        this.#pingTimer = null;
        this.#sendPing();
      }, this.#heartbeat.interval),
    );
  }

  #sendPing() {
    if (!this.active) return;
    try {
      this.send({ type: 'ping' });
    } catch (error) {
      return void this.#escalate(error);
    }
    this.#pongTimer = unref(
      setTimeout(() => {
        this.#pongTimer = null;
        this.#onHeartbeatTimeout();
      }, this.#heartbeat.timeout),
    );
  }

  #onPong() {
    if (!this.#pongTimer) return; // unsolicited pong: nothing was waiting
    clearTimeout(this.#pongTimer);
    this.#pongTimer = null;
    if (this.#heartbeat && this.active) this.#armPing();
  }

  // The peer stopped answering: close so the transport reports 'close' and
  // the normal reconnect path takes over.
  #onHeartbeatTimeout() {
    this.emit('heartbeat-timeout');
    try {
      this.#transport.terminate();
    } catch (error) {
      this.#escalate(error);
    }
  }

  async open() {
    WrpcClient.connections.add(this);
    await this.#transport.open(this.#options);
  }

  close() {
    clearTimeout(this.#reconnectTimer);
    this.#reconnectTimer = null;
    this.#stopHeartbeat();
    // An explicit close ends the session: a later open() is a fresh start,
    // not a reconnect, so it must not replay 'reconnect'.
    this.#connected = false;
    this.#attempt = 0;
    WrpcClient.connections.delete(this);
    this.#transport.close();
  }

  write(data) {
    this.#transport.write(data);
  }

  send(data) {
    this.#transport.send(data);
  }

  getStream(id) {
    const stream = this.#streams.get(id);
    if (stream) return stream;
    throw new Error(`Stream ${id} is not initialized`);
  }

  createStream(name, size) {
    const id = generateUUID();
    return new WrpcWritable(id, name, size, this);
  }

  createBlobUploader(blob) {
    const { name = 'blob', size } = blob;
    const consumer = this.createStream(name, size);
    const { id } = consumer;
    const upload = async () => {
      for await (const chunk of blob.stream()) {
        consumer.write(chunk);
      }
      consumer.end();
    };
    return { id, upload };
  }

  async #handlePacket(data) {
    const packet = jsonParse(data);
    if (!packet) {
      if (this.#proxyPacket) return void this.#proxyPacket(data, null);
      throw new Error('Invalid JSON packet');
    }
    const { type, id, name } = packet;
    // Heartbeat first, and before the proxy hand-off: the pong answers a
    // ping this client sent, so it is never anyone else's packet.
    if (type === 'pong') return void this.#onPong();
    if (type === 'ping') return void this.send({ type: 'pong' });
    if (this.#proxyPacket) return void this.#proxyPacket(data, packet);
    if (type === 'event') return void (await this.#handleEvent(name, packet.data));
    if (!id) throw new Error('Packet structure error');
    if (type === 'callback') {
      const promised = this.#calls.get(id);
      if (!promised) throw new Error(`Callback ${id} not found`);
      const resolve = promised[0];
      const reject = promised[1];
      const timeout = promised[2];
      this.#calls.delete(id);
      clearTimeout(timeout);
      if (packet.error) {
        return void reject(new WrpcError(packet.error));
      }
      resolve(packet.result);
      return;
    }
    if (type === 'stream') await this.#handleStream(packet);
  }

  // Events are addressed 'unit/event'. One that reaches no listener — an
  // unloaded unit, or a loaded one nobody subscribed to — surfaces as
  // 'unhandled-event' rather than vanishing: a silently dropped broadcast is
  // indistinguishable from a broken server.
  async #handleEvent(name, data) {
    if (typeof name === 'string') {
      const slash = name.indexOf('/');
      if (slash > 0) {
        const unit = name.slice(0, slash);
        const eventName = name.slice(slash + 1);
        // `api` is a plain object, so a wire-supplied unit like 'constructor'
        // or 'toString' resolves up the prototype chain. Only an Emitter this
        // client put there itself is a real unit — anything else is an event
        // nobody is listening for.
        const apiUnit = this.api[unit];
        if (eventName && apiUnit instanceof Emitter && apiUnit.listenerCount(eventName) > 0) {
          return void (await apiUnit.emit(eventName, data));
        }
      }
    }
    await this.emit('unhandled-event', { name, data });
  }

  async #handleStream(packet) {
    const { id, name, size, status } = packet;
    const stream = this.#streams.get(id);
    if (status === undefined) {
      if (stream) {
        throw new Error(`Stream ${name} is already initialized`);
      }
      const readableStream = new WrpcReadable(id, name, size);
      this.#streams.set(id, readableStream);
      return;
    }
    if (!stream) throw new Error(`Stream ${id} is not initialized`);
    if (status === 'end') {
      await stream.close();
      this.#streams.delete(id);
    } else if (status === 'terminate') {
      await stream.terminate();
      this.#streams.delete(id);
    }
  }

  async #handleBinary(input) {
    const byteView = await toByteView(input);
    const { id, payload } = chunkDecode(byteView);
    const stream = this.#streams.get(id);
    if (!stream) {
      throw new Error(`Stream ${id} is not initialized`);
    }
    await stream.push(payload);
  }

  async load(...units) {
    if (!this.active) throw new Error('Not connected');
    const introspect = this.#scaffold('system')('introspect');
    const introspection = await introspect(units);
    for (const unit of units) {
      const instance = introspection[unit];
      if (!instance) continue;
      this.#loaded.add(unit);
      // Reuse the unit's emitter when it already exists: a reconnect reloads
      // every unit, and replacing the object would silently drop every event
      // listener the caller registered on it.
      let methods = this.api[unit];
      if (!(methods instanceof Emitter)) {
        methods = new Emitter();
        // defineProperty, not assignment: a unit named '__proto__' would go
        // through Object.prototype's setter and mutate the prototype instead
        // of becoming a unit.
        Object.defineProperty(this.api, unit, {
          value: methods,
          enumerable: true,
          writable: true,
          configurable: true,
        });
      }
      const request = this.#scaffold(unit);
      const methodNames = Object.keys(instance);
      const previous = this.#unitMethods.get(unit);
      if (previous) {
        // A method the server no longer exposes must stop being callable.
        for (const methodName of previous) {
          if (!methodNames.includes(methodName)) delete methods[methodName];
        }
      }
      for (const methodName of methodNames) {
        methods[methodName] = request(methodName);
      }
      this.#unitMethods.set(unit, new Set(methodNames));
    }
  }

  /** Sends a fire-and-forget event to the server; `name` is 'unit/event'. */
  sendEvent(name, data) {
    this.send({ type: 'event', name, data });
  }

  #scaffold(unit, version) {
    const createMethod = (methodName) => {
      const method = async (args = {}) => {
        const id = generateUUID();
        const ver = version ? `.${version}` : '';
        const target = `${unit}${ver}/${methodName}`;
        const packet = { type: 'call', id, method: target, args };
        return new Promise((resolve, reject) => {
          const timeout = setTimeout(() => {
            if (!this.#calls.has(id)) return;
            this.#calls.delete(id);
            reject(new Error('Request timeout'));
          }, this.#callTimeout);
          this.#calls.set(id, [resolve, reject, timeout]);
          this.send(packet);
        });
      };
      return method;
    };
    return createMethod;
  }
}

class ClientWsTransport extends ClientTransport {
  // The one transport that can die without saying so.
  heartbeat = true;

  #socket = null;
  #opening = null;

  async open() {
    if (this.active) return Promise.resolve();
    if (this.#opening) return this.#opening;
    const opening = new Promise((resolve, reject) => {
      const socket = new WebSocket(this.url);
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
  async open() {
    if (this.active) return;
    this.active = true;
    this.emit('open');
  }

  close() {
    if (!this.active) return;
    this.active = false;
    this.emit('close');
  }

  write(data) {
    const headers = { 'Content-Type': 'application/json' };
    const options = { method: 'POST', headers, body: data };
    const send = async () => {
      try {
        const res = await fetch(this.url, options);
        const packet = await res.text();
        this.emit('message', packet);
      } catch (error) {
        this.emit('error', error);
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
    this.#worker.postMessage({ type: 'wrpc:connect' }, [port2]);
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

class WrpcClientProxy extends Emitter {
  #ports = new Set();
  #pending = new Map();
  #connection = null;
  #callTimeout = CALL_TIMEOUT;
  #reconnect = null;
  #heartbeat = undefined;

  constructor(options = {}) {
    super();
    const { callTimeout, heartbeat } = options;
    if (callTimeout) this.#callTimeout = callTimeout;
    this.#reconnect = normalizeReconnect(options);
    this.#heartbeat = heartbeat;
    if (typeof self === 'undefined') {
      throw new Error('WrpcClientProxy must run in ServiceWorker context');
    }
    self.addEventListener('message', (event) => {
      const { type } = event.data;
      if (type?.startsWith('wrpc')) this.#handleEvent(event);
    });
  }

  async open() {
    if (this.#connection) {
      if (this.#connection.active) return;
      await this.#connection.open();
      return;
    }
    const protocol = self.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${protocol}//${self.location.host}`;
    const options = {
      callTimeout: this.#callTimeout,
      reconnect: this.#reconnect,
      heartbeat: this.#heartbeat,
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
    if (type === 'callback') return void this.#pending.delete(id);
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

WrpcClient.transport = {
  ws: ClientWsTransport,
  http: ClientHttpTransport,
  event: ClientEventTransport,
};

WrpcClient.initialize();

module.exports = { WrpcClient, WrpcClientProxy, WrpcError };
