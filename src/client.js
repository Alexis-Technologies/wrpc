'use strict';

const { Emitter, jsonParse, backoffDelay, createEventStream } = require('./utils.js');
const { generateUUID } = require('./runtime/node.js');
const { WebSocket } = globalThis;
const { chunkDecode } = require('./chunks.js');
const { WrpcReadable, WrpcWritable } = require('./streams.js');
const { createLoggerWriter } = require('./logging.js');
const { createClientTelemetry } = require('./telemetry/client.js');

const CALL_TIMEOUT = 7 * 1000;

// Monotonic where available; Date.now is the fallback for a host without it.
const now = () => (typeof performance === 'object' ? performance.now() : Date.now());
const RECONNECT_TIMEOUT = 2 * 1000;

// 499, nginx's "client closed request": the caller took the call back, so
// it is neither a server fault nor a success.
const CANCELLED_ERROR = { message: 'Cancelled by the caller', code: 499 };

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

// Call batching. Several calls issued in the same tick travel as ONE frame
// (a JSON array), which on HTTP is one request instead of N and on a
// WebSocket is one frame instead of N. Only `call` packets batch: a ping, a
// cancel or an unsubscribe is a control packet whose whole point is to
// arrive now.
const BATCH = { flush: 'microtask', maxSize: 16, maxBytes: 64 * 1024 };

const normalizeBatch = (options) => {
  const { batch } = options;
  if (!batch) return null;
  const merged = { ...BATCH, ...(batch === true ? {} : batch) };
  const timed = typeof merged.flush === 'number' && merged.flush >= 0;
  if (!timed && merged.flush !== 'microtask') merged.flush = BATCH.flush;
  if (!(merged.maxSize > 1)) merged.maxSize = BATCH.maxSize;
  if (!(merged.maxBytes > 0)) merged.maxBytes = BATCH.maxBytes;
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
  // Whether the connection stays open: only a persistent one can carry a
  // cancel, an unsubscribe or a subscription's values.
  persistent = true;
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
  #log = null;
  #otel = null;
  #transport = null;
  #calls = new Map();
  #cancelled = new Set();
  #subscriptions = new Map();
  #batch = null;
  #pending = [];
  #pendingBytes = 0;
  #flushTimer = null;
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
    const { callTimeout, proxy, random, logger, telemetry } = options;
    // Off by default, unlike the server: a client that printed on every
    // reconnect would be noise in a browser console nobody asked for.
    this.#log = createLoggerWriter(logger);
    this.#otel = createClientTelemetry(telemetry);
    if (callTimeout) this.#callTimeout = callTimeout;
    if (proxy) this.#proxyPacket = proxy;
    if (random) this.#random = random; // deterministic jitter in tests
    this.#reconnect = normalizeReconnect(options);
    this.#heartbeat = normalizeHeartbeat(options);
    this.#batch = normalizeBatch(options);
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
    // The scheme picks the transport unless one is named. 'sse' only exists
    // once '@alexify/wrpc/sse' has been required, which is what registers it.
    const name = options.transport ?? (url.startsWith('http') ? 'http' : 'ws');
    const Transport = WrpcClient.transport[name];
    if (typeof Transport !== 'function') {
      throw new Error(`Unknown transport '${name}'`);
    }
    const transport = new Transport(url);
    const client = new WrpcClient(url, transport, options);
    await client.open();
    return client;
  }

  // An 'error' with no listener throws by design (see Emitter), which is
  // right for a synchronous mistake and wrong for a background failure: a
  // throw out of a timer or a promise chain would take the process down for
  // a reconnect that is about to be retried anyway.
  #escalate(error, event = 'client.error') {
    // A logger observes; a listener handles. Both run — the log line is not
    // a fallback for a missing listener.
    this.#log.error({ err: error, event });
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
      this.#log.info({ event: reconnected ? 'reconnected' : 'open', url: this.url, attempts });
      this.#otel.recordConnection(1);
      if (reconnected) this.#otel.recordReconnect('recovered', attempts);
      this.emit('open');
      if (reconnected) this.#restore(attempts).catch((error) => this.#escalate(error, 'reconnect.restore'));
    });

    this.#transport.on('close', () => {
      this.#stopHeartbeat();
      this.#log.info({ event: 'close', url: this.url });
      this.#otel.recordConnection(-1);
      this.emit('close');
      this.#scheduleReconnect();
    });

    this.#transport.on('error', (error) => {
      this.#escalate(error, 'transport.error');
    });

    this.#transport.on('message', (data) => {
      const escalate = (error) => this.#escalate(error, 'message');
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
    // Each subscription is re-opened from the last eventId it saw, so the
    // server can replay what was missed instead of starting over. A feed
    // that yields untracked values has no eventId and simply resumes live.
    const subscriptions = Array.from(this.#subscriptions.values());
    for (const record of subscriptions) this.#openSubscription(record);
    await this.emit('reconnect', { units, attempts, subscriptions: subscriptions.length });
  }

  #scheduleReconnect() {
    if (this.active) return;
    if (!WrpcClient.connections.has(this)) return;
    if (this.#reconnectTimer) return;
    const { retries } = this.#reconnect;
    if (this.#attempt >= retries) {
      this.#log.warn({ event: 'reconnect.failed', attempts: this.#attempt, url: this.url });
      this.#otel.recordReconnect('exhausted', this.#attempt);
      return void this.emit('reconnect-failed', { attempts: this.#attempt });
    }
    const delay = backoffDelay({ ...this.#reconnect, attempt: this.#attempt, random: this.#random });
    this.#attempt++;
    this.#log.debug({ event: 'reconnecting', attempt: this.#attempt, delay, url: this.url });
    this.emit('reconnecting', { attempt: this.#attempt, delay });
    // Not unref'd: see the note on `unref` above.
    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = null;
      this.open().catch((error) => {
        // A rejected open never emitted 'close', so nothing else would
        // schedule the next attempt — the loop has to continue here.
        this.#escalate(error, 'reconnect.open');
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
      return void this.#escalate(error, 'heartbeat.ping');
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
    this.#log.warn({ event: 'heartbeat.timeout', url: this.url });
    this.emit('heartbeat-timeout');
    try {
      this.#transport.terminate();
    } catch (error) {
      this.#escalate(error, 'heartbeat.terminate');
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
    // Anything still queued leaves before the socket does; a call whose
    // packet never shipped would otherwise wait out its whole timeout.
    this.flush();
    // Snapshotted and cleared BEFORE anyone is told: a listener that reacts by
    // unsubscribing, or by closing again, then finds nothing left to do
    // instead of mutating the map being walked — and `handle.closed` already
    // reads true by the time it is asked.
    const ended = Array.from(this.#subscriptions.values());
    this.#subscriptions.clear();
    // An explicit close ends the session: a later open() is a fresh start,
    // not a reconnect, so it must not replay 'reconnect'.
    this.#connected = false;
    this.#attempt = 0;
    WrpcClient.connections.delete(this);
    this.#transport.close();
    // Last, with nothing left to tear down. Closing the client ends every
    // subscription it carried, and a subscription that ended has to say so:
    // `unsubscribe()` stays silent because the caller named that one feed,
    // whereas close() is usually called by something else entirely (a page
    // teardown, a shutdown hook), so the code owning the feed never asked.
    for (const record of ended) this.#endSubscription(record);
  }

  /**
   * The terminal signal for one subscription, in the order the `end` packet
   * delivers it. A listener that throws is contained rather than allowed to
   * rob the rest of theirs — or, inside close(), to abandon the teardown.
   */
  #endSubscription(record) {
    try {
      record.onRelease?.();
      record.onEnd?.();
    } catch (error) {
      this.#escalate(error, 'subscription.listener');
    }
    // Always, even if a listener threw: an `iterate()` consumer is parked in
    // next() and would otherwise wait there forever.
    record.stream?.end();
  }

  write(data) {
    this.#transport.write(data);
  }

  send(data) {
    // Only calls batch: everything else is a control packet whose value is
    // that it leaves now.
    if (this.#batch && data?.type === 'call') return void this.#enqueue(data);
    this.#transport.send(data);
  }

  #enqueue(packet) {
    const size = JSON.stringify(packet).length;
    this.#pending.push({ packet, size });
    this.#pendingBytes += size;
    const { maxSize, maxBytes } = this.#batch;
    if (this.#pending.length >= maxSize || this.#pendingBytes >= maxBytes) return void this.flush();
    this.#schedule();
  }

  #schedule() {
    if (this.#flushTimer !== null) return;
    const { flush } = this.#batch;
    if (flush === 'microtask') {
      this.#flushTimer = 'microtask';
      queueMicrotask(() => this.flush());
      return;
    }
    this.#flushTimer = unref(setTimeout(() => this.flush(), flush));
  }

  /** Sends whatever calls are waiting to be batched. Safe to call anytime. */
  flush() {
    if (this.#flushTimer !== null && this.#flushTimer !== 'microtask') clearTimeout(this.#flushTimer);
    this.#flushTimer = null;
    if (this.#pending.length === 0) return;
    const pending = this.#pending;
    this.#pending = [];
    this.#pendingBytes = 0;
    // A batch of one is that packet: no reason to make the peer unwrap it.
    const payload = pending.length === 1 ? pending[0].packet : pending.map((entry) => entry.packet);
    try {
      this.#transport.send(payload);
    } catch (error) {
      this.#escalate(error, 'batch.flush');
    }
  }

  // A call cancelled before its batch left never has to be cancelled on the
  // wire — dropping it here is both cheaper and safer than racing a `cancel`
  // packet ahead of the `call` it refers to.
  #unqueue(id) {
    const index = this.#pending.findIndex((entry) => entry.packet.id === id);
    if (index < 0) return false;
    const [entry] = this.#pending.splice(index, 1);
    this.#pendingBytes -= entry.size;
    return true;
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
    // A batch frame answers several packets at once; each one is dispatched
    // exactly as it would have been on its own.
    if (Array.isArray(packet)) {
      if (this.#proxyPacket) return void this.#proxyPacket(data, packet);
      // Contained per item: one answer this client cannot route must not
      // strand the rest of the batch, which is the guarantee the protocol
      // makes about a failure inside a batch.
      for (const item of packet) {
        try {
          await this.#dispatch(item ?? {});
        } catch (error) {
          this.#escalate(error, 'batch.dispatch');
        }
      }
      return;
    }
    const { type } = packet;
    // Heartbeat first, and before the proxy hand-off: the pong answers a
    // ping this client sent, so it is never anyone else's packet.
    if (type === 'pong') return void this.#onPong();
    if (type === 'ping') return void this.send({ type: 'pong' });
    if (this.#proxyPacket) return void this.#proxyPacket(data, packet);
    await this.#dispatch(packet);
  }

  async #dispatch(packet) {
    const { type, id, name } = packet;
    if (type === 'event') return void (await this.#handleEvent(name, packet.data));
    if (!id) throw new Error('Packet structure error');
    if (type === 'callback') return void this.#settle(packet);
    if (type === 'data' || type === 'end') return void this.#handleSubscriptionPacket(packet);
    if (type === 'stream') await this.#handleStream(packet);
  }

  #settle(packet) {
    const { id } = packet;
    const call = this.#calls.get(id);
    // The 499 acknowledging a cancel this client sent: the caller was
    // rejected the moment it aborted, so the ack is expected, not an error.
    if (!call && this.#cancelled.delete(id)) return;
    if (!call) throw new Error(`Callback ${id} not found`);
    this.#calls.delete(id);
    clearTimeout(call.timeout);
    call.release?.();
    if (packet.error) return void call.reject(new WrpcError(packet.error));
    call.resolve(packet.result);
  }

  // `data` carries one value of a subscription; `end` closes it, with an
  // `error` when the server-side generator threw. A packet for a
  // subscription this client already dropped is ignored: the unsubscribe and
  // the last values in flight cross on the wire, and that is normal.
  #handleSubscriptionPacket(packet) {
    const { type, id } = packet;
    const record = this.#subscriptions.get(id);
    if (!record) return;
    if (type === 'data') {
      // Remembered for the resume after a reconnect. Untracked values leave
      // it alone: a feed with no ids simply has no resume point.
      if (packet.eventId !== undefined) record.lastEventId = packet.eventId;
      record.onData?.(packet.data);
      record.stream?.push(packet.data);
      return;
    }
    this.#subscriptions.delete(id);
    if (!packet.error) return void this.#endSubscription(record);
    record.onRelease?.();
    const error = new WrpcError(packet.error);
    record.stream?.fail(error);
    if (record.onError) return void record.onError(error);
    // Nobody asked to hear about it, but a subscription that died must not
    // die quietly.
    if (!record.stream) this.#escalate(error, 'subscription.error');
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
    const introspection = await this.#call('system/introspect', units);
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
        methods[methodName] = request(methodName, instance[methodName]);
      }
      this.#unitMethods.set(unit, new Set(methodNames));
    }
  }

  /** Sends a fire-and-forget event to the server; `name` is 'unit/event'. */
  sendEvent(name, data) {
    this.send({ type: 'event', name, data });
  }

  #target(unit, version, methodName) {
    const ver = version ? `.${version}` : '';
    return `${unit}${ver}/${methodName}`;
  }

  #call(target, args, options = {}) {
    const id = generateUUID();
    const { signal } = options;
    const packet = { type: 'call', id, method: target, args };
    if (!this.#otel.enabled) return this.#dispatchCall(target, packet, id, signal);
    const started = now();
    return this.#otel.withSpan({ packet, target }, (handle) => {
      // Injected INSIDE the span so the traceparent names this call's span,
      // which is what the server will pick up as its parent.
      this.#otel.inject(packet);
      return this.#dispatchCall(target, packet, id, signal).then(
        (result) => {
          this.#otel.endSpan(handle, { 'wrpc.status': 'ok' });
          this.#otel.recordCall(target, 'ok', now() - started);
          return result;
        },
        (error) => {
          this.#otel.recordError(handle, error);
          this.#otel.endSpan(handle, { 'wrpc.status': 'error', 'rpc.wrpc.status_code': error?.code });
          this.#otel.recordCall(target, 'error', now() - started);
          throw error;
        },
      );
    });
  }

  #dispatchCall(target, packet, id, signal) {
    return new Promise((resolve, reject) => {
      if (signal?.aborted) return void reject(new WrpcError(CANCELLED_ERROR));
      const timeout = setTimeout(() => {
        if (!this.#calls.has(id)) return;
        this.#calls.delete(id);
        this.#unqueue(id);
        release();
        reject(new Error('Request timeout'));
      }, this.#callTimeout);
      const onAbort = () => {
        if (!this.#calls.has(id)) return;
        this.#calls.delete(id);
        clearTimeout(timeout);
        // Still queued: drop it instead of racing a cancel ahead of the
        // call. And a request/response transport cannot carry a cancel at
        // all — sending one there only earns a 400 nobody can route.
        if (!this.#unqueue(id) && this.active && this.#transport.persistent !== false) {
          this.#cancelled.add(id);
          this.send({ type: 'cancel', id });
        }
        reject(new WrpcError(CANCELLED_ERROR));
      };
      const release = () => signal?.removeEventListener('abort', onAbort);
      signal?.addEventListener('abort', onAbort, { once: true });
      this.#calls.set(id, { resolve, reject, timeout, release });
      this.send(packet);
    });
  }

  /**
   * Opens a subscription. `onData` receives every value; the returned handle
   * carries the last seen eventId and can stop it. The subscription is
   * re-opened from that eventId automatically after a reconnect.
   */
  #subscribe(target, args, options = {}) {
    const id = generateUUID();
    const record = {
      id,
      target,
      args,
      lastEventId: options.lastEventId,
      onData: options.onData ?? null,
      onError: options.onError ?? null,
      onEnd: options.onEnd ?? null,
      stream: options.stream ?? null,
      // Lets iterate() drop its abort listener however the subscription ends.
      onRelease: options.onRelease ?? null,
    };
    const live = this.#subscriptions;
    live.set(id, record);
    this.#openSubscription(record);
    return {
      id,
      unsubscribe: () => this.#unsubscribe(id),
      get lastEventId() {
        return record.lastEventId;
      },
      get closed() {
        return !live.has(id);
      },
    };
  }

  #openSubscription(record) {
    const packet = { type: 'subscribe', id: record.id, method: record.target, args: record.args };
    if (record.lastEventId !== undefined && record.lastEventId !== null) {
      packet.lastEventId = record.lastEventId;
    }
    this.send(packet);
  }

  #unsubscribe(id) {
    const record = this.#subscriptions.get(id);
    if (!record) return false;
    this.#subscriptions.delete(id);
    record.onRelease?.();
    record.stream?.end();
    // The server answers with `end`, which lands on a record that is gone —
    // ignored on purpose, the caller already knows.
    if (this.active) this.send({ type: 'unsubscribe', id });
    return true;
  }

  #scaffold(unit, version) {
    const createMethod = (methodName, info = {}) => {
      const target = this.#target(unit, version, methodName);
      if (info.kind !== 'subscription') {
        return (args = {}, options = {}) => this.#call(target, args, options);
      }
      // A subscription is not callable: it answers with a stream, so it
      // exposes the two ways to consume one instead of pretending to be a
      // function that resolves once.
      return {
        kind: 'subscription',
        subscribe: (args = {}, options = {}) => this.#subscribe(target, args, options),
        iterate: (args = {}, options = {}) => this.#iterate(target, args, options),
      };
    };
    return createMethod;
  }

  #iterate(target, args, options = {}) {
    const { signal } = options;
    const stream = createEventStream({ signal, highWaterMark: options.highWaterMark });
    // An already-aborted signal means the caller is gone before it started:
    // opening a subscription nobody will consume would leave a generator
    // running on the server with no handle to stop it.
    if (signal?.aborted) {
      const iterator = stream[Symbol.asyncIterator]();
      iterator.subscription = { id: null, lastEventId: undefined, closed: true, unsubscribe: () => false };
      return iterator;
    }
    // Breaking out of `for await` (or aborting) has to reach the server, and
    // whichever ends it first has to release the listener the other used.
    let stop = null;
    const release = () => {
      if (!stop) return;
      signal?.removeEventListener('abort', stop);
      stop = null;
    };
    const handle = this.#subscribe(target, args, {
      ...options,
      stream,
      onData: options.onData ?? null,
      onRelease: release,
    });
    stop = () => {
      stop = null;
      handle.unsubscribe();
    };
    signal?.addEventListener('abort', stop, { once: true });
    const iterator = stream[Symbol.asyncIterator]();
    const originalReturn = iterator.return.bind(iterator);
    iterator.return = () => {
      release();
      handle.unsubscribe();
      return originalReturn();
    };
    iterator.subscription = handle;
    return iterator;
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
  // One request, one response: nothing to cancel or subscribe on.
  persistent = false;

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
  #logger = undefined;
  #telemetry = undefined;

  constructor(options = {}) {
    super();
    const { callTimeout, heartbeat, logger, telemetry } = options;
    if (callTimeout) this.#callTimeout = callTimeout;
    this.#reconnect = normalizeReconnect(options);
    this.#heartbeat = heartbeat;
    // The proxy rebuilds its own options bag, so anything not forwarded here
    // is silently lost on the connection it owns.
    this.#logger = logger;
    this.#telemetry = telemetry;
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

WrpcClient.transport = {
  ws: ClientWsTransport,
  http: ClientHttpTransport,
  event: ClientEventTransport,
};

WrpcClient.initialize();

// The contract-first entry point. `connect<Api>(url)` is the same call as
// `WrpcClient.connect(url)` under a name that reads as a function, because
// that is what carries the type argument in `connect<Api>('wss://host')` —
// the whole typed-client story is the ONE place a user names their contract.
// A wrapper rather than a re-export of the static: the name of the thing a
// user calls should not depend on how the class happens to expose it.
const connect = (url, options) => WrpcClient.connect(url, options);

// ClientTransport is exported for transports that live in their own subpath
// (see src/sse/client.js); it is deliberately NOT re-exported from the
// package barrel, where transports stay type-only.
module.exports = { WrpcClient, WrpcClientProxy, WrpcError, ClientTransport, connect };
