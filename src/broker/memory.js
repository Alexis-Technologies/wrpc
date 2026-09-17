'use strict';

// The in-process broker: the reference implementation of every capability
// in port.js, and what makes feeds, consumers and RPC-over-a-broker
// testable (and genuinely usable inside one process) without standing up
// infrastructure — two RpcServers sharing one MemoryBroker behave like two
// processes sharing a real one.
//
// Delivery is deferred to a microtask everywhere, like a real broker's:
// code that accidentally relied on synchronous delivery would break the
// moment Redis or NATS is swapped in.

const { MemoryBackplane, DEFAULT_PREFIX } = require('../scaling/memory.js');
const { createLoggerWriter } = require('../logging.js');
const { generateUUID } = require('../runtime/node.js');
const { codedError, toText, toHeaders } = require('./ids.js');

const DEFAULT_LOG_ENTRIES = 10_000;
const DEFAULT_PREFETCH = 16;
const MAX_ID_LENGTH = 128;
const LOG_ID = /^[A-Za-z0-9_-]{1,64}\.\d{1,16}$/;

const requireName = (value, what, label) => {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${label}: ${what} must be a non-empty string`);
  }
};

const copyBody = (body) => (body instanceof Uint8Array ? body.slice() : typeof body === 'string' ? body : toText(body));

class MemoryBroker {
  #closed = false;
  #epoch;
  #maxEntries;
  #log;
  #backplane;
  #topics = new Map(); // topic -> { entries, head, first, next, waiters }
  #queues = new Map(); // queue -> { ready, consumers, cursor, timers }
  #addresses = new Map(); // address -> { plain: Set, groups: Map(group -> { members, cursor }) }
  #counter = 0;

  constructor({ prefix = DEFAULT_PREFIX, logger = globalThis.console, epoch = null, retention = {} } = {}) {
    this.#epoch = epoch === null ? generateUUID().replace(/-/g, '').slice(0, 12) : String(epoch);
    if (!/^[A-Za-z0-9_-]{1,64}$/.test(this.#epoch)) {
      throw new TypeError('MemoryBroker: epoch must be 1-64 characters of [A-Za-z0-9_-]');
    }
    const maxEntries = retention.maxEntries ?? DEFAULT_LOG_ENTRIES;
    if (!Number.isInteger(maxEntries) || maxEntries <= 0) {
      throw new TypeError('MemoryBroker: retention.maxEntries must be a positive integer');
    }
    this.#maxEntries = maxEntries;
    this.#log = createLoggerWriter(logger).child({ component: 'broker', broker: 'memory' });
    this.#backplane = new MemoryBackplane({ prefix, logger });
    // Capability objects are built once: `broker.log === broker.log`, and
    // each carries the broker's name for metric labels.
    this.log = Object.freeze({
      name: 'memory',
      append: (topic, value, options) => this.#append(topic, value, options),
      read: (topic, options) => this.#read(topic, options),
      parseId: (text) => this.#parseId(text),
    });
    this.queue = Object.freeze({
      name: 'memory',
      produce: (queue, body, options) => this.#produce(queue, body, options),
      consume: (queue, onDelivery, options) => this.#consume(queue, onDelivery, options),
    });
    this.direct = Object.freeze({
      name: 'memory',
      inbox: () => `_inbox.${this.#epoch}.${++this.#counter}`,
      listen: (address, onMessage, options) => this.#listen(address, onMessage, options),
      send: (address, body, options) => this.#send(address, body, options),
    });
  }

  get name() {
    return 'memory';
  }

  get backplane() {
    return this.#backplane;
  }

  /** The epoch stamped into every log id this broker mints. */
  get epoch() {
    return this.#epoch;
  }

  // -------------------------------------------------------------------------
  // log

  #topic(topic) {
    let state = this.#topics.get(topic);
    if (!state) {
      state = { entries: [], head: 0, first: 1, next: 1, waiters: new Set() };
      this.#topics.set(topic, state);
    }
    return state;
  }

  #append(topic, value, { headers = null } = {}) {
    requireName(topic, 'topic', 'MemoryBroker.log.append');
    if (this.#closed) return Promise.reject(codedError('Broker is closed', 503));
    const state = this.#topic(topic);
    const n = state.next++;
    const id = `${this.#epoch}.${n}`;
    state.entries.push({ id, value: toText(value), headers: toHeaders(headers) });
    const retained = state.entries.length - state.head;
    if (retained > this.#maxEntries) {
      const excess = retained - this.#maxEntries;
      state.head += excess;
      state.first += excess;
      // A head index instead of shift(): trimming at the cap would otherwise
      // be O(retained) per append. Compacted once the dead prefix dominates.
      if (state.head > 1024 && state.head * 2 > state.entries.length) {
        state.entries = state.entries.slice(state.head);
        state.head = 0;
      }
    }
    if (state.waiters.size > 0) {
      const waiters = Array.from(state.waiters);
      state.waiters.clear();
      queueMicrotask(() => {
        for (const wake of waiters) wake();
      });
    }
    return Promise.resolve(id);
  }

  /** Drops all but the newest `keep` entries of a topic — XTRIM's analogue. */
  trim(topic, keep) {
    const state = this.#topics.get(topic);
    if (!state || !Number.isInteger(keep) || keep < 0) return;
    const excess = state.entries.length - state.head - keep;
    if (excess <= 0) return;
    state.head += excess;
    state.first += excess;
  }

  #parseId(text) {
    if (typeof text !== 'string' || text.length > MAX_ID_LENGTH || !LOG_ID.test(text)) return null;
    return text;
  }

  #read(topic, { after = null, from = 'latest', signal = null } = {}) {
    requireName(topic, 'topic', 'MemoryBroker.log.read');
    if (from !== 'latest' && from !== 'earliest') {
      throw new TypeError("MemoryBroker.log.read: from must be 'latest' or 'earliest'");
    }
    const state = this.#topic(topic);
    let position = 0;
    let failure = null;
    if (after !== null && after !== undefined) {
      const id = this.#parseId(after);
      if (id === null) failure = codedError('Malformed event id', 400);
      else {
        const dot = id.lastIndexOf('.');
        const n = Number(id.slice(dot + 1));
        if (id.slice(0, dot) !== this.#epoch) failure = codedError('Event id belongs to another log', 410);
        else if (n >= state.next) failure = codedError('Event id is beyond the end of the log', 400);
        else if (n + 1 < state.first) failure = codedError('Event history was trimmed past this id', 410);
        else position = n + 1;
      }
    } else {
      position = from === 'earliest' ? state.first : state.next;
    }
    const broker = this;
    const iterate = async function* () {
      if (failure) throw failure;
      for (;;) {
        if (signal?.aborted || broker.#closed) return;
        if (position < state.first) throw codedError('Event history was trimmed past this reader', 410);
        if (position < state.next) {
          const entry = state.entries[state.head + (position - state.first)];
          position++;
          yield { id: entry.id, value: entry.value, headers: { ...entry.headers } };
          continue;
        }
        await broker.#wait(state, signal);
      }
    };
    return { ready: Promise.resolve(), [Symbol.asyncIterator]: () => iterate() };
  }

  #wait(state, signal) {
    return new Promise((resolve) => {
      const wake = () => {
        state.waiters.delete(wake);
        signal?.removeEventListener('abort', wake);
        resolve();
      };
      state.waiters.add(wake);
      signal?.addEventListener('abort', wake, { once: true });
    });
  }

  // -------------------------------------------------------------------------
  // queue

  #queueState(queue) {
    let state = this.#queues.get(queue);
    if (!state) {
      state = { ready: [], consumers: [], cursor: 0, timers: new Set() };
      this.#queues.set(queue, state);
    }
    return state;
  }

  #enqueue(queue, body, headers) {
    const state = this.#queueState(queue);
    state.ready.push({
      id: `${this.#epoch}.m${++this.#counter}`,
      body: toText(body),
      headers: toHeaders(headers),
      attempt: 1,
      redelivered: false,
    });
    this.#pump(state);
  }

  #produce(queue, body, { headers = null } = {}) {
    requireName(queue, 'queue', 'MemoryBroker.queue.produce');
    if (this.#closed) return Promise.reject(codedError('Broker is closed', 503));
    this.#enqueue(queue, body, headers);
    return Promise.resolve();
  }

  #consume(queue, onDelivery, { prefetch = DEFAULT_PREFETCH, deadLetter = null, signal = null } = {}) {
    requireName(queue, 'queue', 'MemoryBroker.queue.consume');
    if (typeof onDelivery !== 'function') {
      throw new TypeError('MemoryBroker.queue.consume: onDelivery must be a function');
    }
    if (!Number.isInteger(prefetch) || prefetch <= 0) {
      throw new TypeError('MemoryBroker.queue.consume: prefetch must be a positive integer');
    }
    if (deadLetter !== null && (typeof deadLetter !== 'string' || deadLetter.length === 0)) {
      throw new TypeError('MemoryBroker.queue.consume: deadLetter must be a queue name or null');
    }
    if (this.#closed) return Promise.reject(codedError('Broker is closed', 503));
    const state = this.#queueState(queue);
    const consumer = { onDelivery, prefetch, deadLetter, inflight: new Set(), active: true };
    state.consumers.push(consumer);
    const stop = () => {
      if (!consumer.active) return Promise.resolve();
      consumer.active = false;
      const index = state.consumers.indexOf(consumer);
      if (index >= 0) state.consumers.splice(index, 1);
      // Unsettled work goes back to the FRONT, in its original order, and
      // counts as redelivered — a late ack from the stopped consumer is a
      // no-op, exactly as it is on a broker that already reassigned it.
      const orphans = Array.from(consumer.inflight);
      consumer.inflight.clear();
      for (let i = orphans.length - 1; i >= 0; i--) {
        const delivery = orphans[i];
        delivery.settled = true;
        delivery.message.redelivered = true;
        state.ready.unshift(delivery.message);
      }
      this.#pump(state);
      return Promise.resolve();
    };
    signal?.addEventListener('abort', () => void stop(), { once: true });
    queueMicrotask(() => this.#pump(state));
    const broker = this;
    return Promise.resolve({
      stop,
      get healthy() {
        return consumer.active && !broker.#closed;
      },
    });
  }

  // Hands ready messages to consumers with spare prefetch, round-robin.
  #pump(state) {
    if (this.#closed) return;
    while (state.ready.length > 0) {
      const consumer = this.#nextConsumer(state);
      if (!consumer) return;
      this.#dispatch(state, consumer, state.ready.shift());
    }
  }

  #nextConsumer(state) {
    const { consumers } = state;
    for (let i = 0; i < consumers.length; i++) {
      const consumer = consumers[(state.cursor + i) % consumers.length];
      if (consumer.active && consumer.inflight.size < consumer.prefetch) {
        state.cursor = (state.cursor + i + 1) % consumers.length;
        return consumer;
      }
    }
    return null;
  }

  #dispatch(state, consumer, message) {
    const broker = this;
    const record = { message, settled: false };
    const settle = (apply) => {
      if (record.settled || broker.#closed) return Promise.resolve();
      record.settled = true;
      consumer.inflight.delete(record);
      apply();
      broker.#pump(state);
      return Promise.resolve();
    };
    const delivery = Object.freeze({
      id: message.id,
      body: message.body,
      headers: { ...message.headers },
      attempt: message.attempt,
      redelivered: message.redelivered,
      ack: () => settle(() => {}),
      retry: ({ delay = 0 } = {}) =>
        settle(() => {
          message.attempt++;
          message.redelivered = true;
          if (!(delay > 0)) return void state.ready.push(message);
          const timer = setTimeout(() => {
            state.timers.delete(timer);
            state.ready.push(message);
            broker.#pump(state);
          }, delay);
          state.timers.add(timer);
        }),
      release: () =>
        settle(() => {
          message.redelivered = true;
          state.ready.unshift(message);
        }),
      deadLetter: (reason = '') =>
        settle(() => {
          if (consumer.deadLetter === null) return;
          broker.#enqueue(consumer.deadLetter, message.body, {
            ...message.headers,
            'x-wrpc-dead-reason': String(reason),
            'x-wrpc-attempt': String(message.attempt),
          });
        }),
    });
    consumer.inflight.add(record);
    queueMicrotask(() => {
      if (record.settled) return;
      let result;
      try {
        result = consumer.onDelivery(delivery);
      } catch (error) {
        result = Promise.reject(error);
      }
      // A handler that throws has not settled anything: the message goes
      // back rather than disappearing with the exception.
      Promise.resolve(result).catch((error) => {
        broker.#log.error({ err: error, event: 'broker.delivery' });
        void delivery.release();
      });
    });
  }

  // -------------------------------------------------------------------------
  // direct

  #listen(address, onMessage, { group = null } = {}) {
    requireName(address, 'address', 'MemoryBroker.direct.listen');
    if (typeof onMessage !== 'function') {
      throw new TypeError('MemoryBroker.direct.listen: onMessage must be a function');
    }
    if (this.#closed) return Promise.reject(codedError('Broker is closed', 503));
    let entry = this.#addresses.get(address);
    if (!entry) {
      entry = { plain: new Set(), groups: new Map() };
      this.#addresses.set(address, entry);
    }
    const listener = { onMessage };
    if (group === null || group === undefined) entry.plain.add(listener);
    else {
      let members = entry.groups.get(group);
      if (!members) {
        members = { list: [], cursor: 0 };
        entry.groups.set(group, members);
      }
      members.list.push(listener);
    }
    const stop = () => {
      const current = this.#addresses.get(address);
      if (!current) return Promise.resolve();
      current.plain.delete(listener);
      for (const [name, members] of current.groups) {
        const index = members.list.indexOf(listener);
        if (index >= 0) members.list.splice(index, 1);
        if (members.list.length === 0) current.groups.delete(name);
      }
      if (current.plain.size === 0 && current.groups.size === 0) this.#addresses.delete(address);
      return Promise.resolve();
    };
    return Promise.resolve(stop);
  }

  #send(address, body, { headers = null, correlationId = null, replyTo = null } = {}) {
    requireName(address, 'address', 'MemoryBroker.direct.send');
    if (this.#closed) return Promise.reject(codedError('Broker is closed', 503));
    const entry = this.#addresses.get(address);
    if (!entry) return Promise.reject(codedError(`No listener at ${address}`, 503));
    const targets = Array.from(entry.plain);
    for (const members of entry.groups.values()) {
      targets.push(members.list[members.cursor % members.list.length]);
      members.cursor = (members.cursor + 1) % members.list.length;
    }
    const payload = copyBody(body);
    const normalized = toHeaders(headers);
    for (const listener of targets) {
      const message = {
        body: payload,
        headers: { ...normalized },
        correlationId: correlationId ?? null,
        replyTo: replyTo ?? null,
      };
      queueMicrotask(() => {
        if (this.#closed) return;
        try {
          const result = listener.onMessage(message);
          if (result && typeof result.catch === 'function') {
            result.catch((error) => this.#log.error({ err: error, event: 'broker.listener', address }));
          }
        } catch (error) {
          this.#log.error({ err: error, event: 'broker.listener', address });
        }
      });
    }
    return Promise.resolve();
  }

  // -------------------------------------------------------------------------

  close() {
    if (this.#closed) return;
    this.#closed = true;
    this.#backplane.close();
    for (const state of this.#topics.values()) {
      const waiters = Array.from(state.waiters);
      state.waiters.clear();
      for (const wake of waiters) wake();
    }
    for (const state of this.#queues.values()) {
      for (const timer of state.timers) clearTimeout(timer);
      state.timers.clear();
      for (const consumer of state.consumers) consumer.active = false;
      state.consumers.length = 0;
    }
    this.#addresses.clear();
  }
}

const createMemoryBroker = (options) => new MemoryBroker(options);

module.exports = { MemoryBroker, createMemoryBroker };
