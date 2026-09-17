'use strict';

// A small in-process NATS + JetStream for the adapter's unit tests: exactly
// the surface src/broker/nats/index.js uses — core publish/subscribe with
// queue groups and reply subjects, streams with sequences, durable pull
// consumers with explicit acks, ack_wait redelivery, nak delays, term, and
// max_ack_pending. The same suites run against a real server in
// tests/broker/nats.integration.test.js.
//
// Not a *.test.js: node --test must not run it.

class FakeHeaders {
  #map = new Map();
  set(name, value) {
    this.#map.set(name, String(value));
  }
  get(name) {
    return this.#map.get(name) ?? '';
  }
  has(name) {
    return this.#map.has(name);
  }
  keys() {
    return Array.from(this.#map.keys());
  }
  clone() {
    const copy = new FakeHeaders();
    for (const [name, value] of this.#map) copy.set(name, value);
    return copy;
  }
}

const headers = () => new FakeHeaders();

const encoder = new TextEncoder();
const bytes = (data) => (typeof data === 'string' ? encoder.encode(data) : new Uint8Array(data ?? []));

class FakeNatsServer {
  subscriptions = new Set(); // { subject, queue, callback }
  streams = new Map();
  clock = 0;

  deliver(subject, message) {
    const plain = [];
    const groups = new Map();
    for (const subscription of this.subscriptions) {
      if (subscription.subject !== subject) continue;
      if (subscription.queue === undefined) plain.push(subscription);
      else {
        const members = groups.get(subscription.queue) ?? [];
        members.push(subscription);
        groups.set(subscription.queue, members);
      }
    }
    for (const [name, members] of groups) {
      const cursor = (this.cursors ??= new Map()).get(name) ?? 0;
      plain.push(members[cursor % members.length]);
      this.cursors.set(name, cursor + 1);
    }
    for (const subscription of plain) {
      queueMicrotask(() => {
        if (subscription.closed) return;
        subscription.callback(null, message);
      });
    }
    return plain.length;
  }
}

class FakeNatsConnection {
  #closed = false;

  constructor(server = new FakeNatsServer()) {
    this.server = server;
  }

  isClosed() {
    return this.#closed;
  }

  publish(subject, data, options = {}) {
    if (this.#closed) throw new Error('connection closed');
    this.server.deliver(subject, {
      subject,
      data: bytes(data),
      headers: options.headers ? options.headers.clone() : undefined,
      reply: options.reply,
      string: () => String(data),
    });
  }

  subscribe(subject, options = {}) {
    const subscription = { subject, queue: options.queue, callback: options.callback, closed: false };
    this.server.subscriptions.add(subscription);
    return {
      unsubscribe: () => {
        subscription.closed = true;
        this.server.subscriptions.delete(subscription);
      },
    };
  }

  async flush() {
    await Promise.resolve();
  }

  async drain() {
    this.#closed = true;
  }
}

const nextSeq = (stream) => ++stream.lastSeq;

class FakeStream {
  messages = new Map(); // seq -> { seq, subject, data, headers }
  consumers = new Map();
  lastSeq = 0;

  constructor(config) {
    this.config = config;
    this.workqueue = config.retention === 'workqueue';
  }

  get first() {
    let first = 0;
    for (const seq of this.messages.keys()) {
      if (first === 0 || seq < first) first = seq;
    }
    return first;
  }

  add(subject, data, carrier) {
    const seq = nextSeq(this);
    this.messages.set(seq, { seq, subject, data: bytes(data), headers: carrier ? carrier.clone() : undefined });
    for (const consumer of this.consumers.values()) consumer.wake();
    return seq;
  }

  remove(seq) {
    this.messages.delete(seq);
  }
}

// A durable pull consumer: explicit acks, ack_wait redelivery, nak delays.
class FakeConsumer {
  #waiters = new Set();

  constructor(stream, config) {
    this.stream = stream;
    this.config = config;
    this.cursor = (config.opt_start_seq ?? 1) - 1;
    this.pending = new Map(); // seq -> { timer, deliveries }
    this.ready = []; // seqs re-queued by nak/ack_wait
    this.deliveries = new Map(); // seq -> count
    this.ackWait = config.ack_wait ? config.ack_wait / 1_000_000 : 30_000;
    this.maxAckPending = config.max_ack_pending ?? 1024;
    this.ephemeral = config.durable_name === undefined;
  }

  wake() {
    const waiters = Array.from(this.#waiters);
    this.#waiters.clear();
    for (const resolve of waiters) resolve();
  }

  wait(ms) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.#waiters.delete(resolve);
        resolve();
      }, ms);
      if (typeof timer.unref === 'function') timer.unref();
      this.#waiters.add(resolve);
    });
  }

  #next() {
    if (this.pending.size >= this.maxAckPending) return null;
    while (this.ready.length > 0) {
      const seq = this.ready.shift();
      if (this.stream.messages.has(seq)) return seq;
    }
    for (const seq of this.stream.messages.keys()) {
      if (seq <= this.cursor) continue;
      this.cursor = seq;
      return seq;
    }
    return null;
  }

  #build(seq) {
    const stored = this.stream.messages.get(seq);
    const deliveries = (this.deliveries.get(seq) ?? 0) + 1;
    this.deliveries.set(seq, deliveries);
    const settle = () => {
      const held = this.pending.get(seq);
      if (held) clearTimeout(held.timer);
      this.pending.delete(seq);
    };
    const timer = setTimeout(() => {
      // ack_wait expired: back in line, one delivery more.
      this.pending.delete(seq);
      this.ready.push(seq);
      this.wake();
    }, this.ackWait);
    if (typeof timer.unref === 'function') timer.unref();
    this.pending.set(seq, { timer, deliveries });
    return {
      seq,
      subject: stored.subject,
      data: stored.data,
      headers: stored.headers,
      info: { deliveryCount: deliveries, redelivered: deliveries > 1 },
      string: () => new TextDecoder().decode(stored.data),
      ack: () => {
        settle();
        if (this.stream.workqueue) this.stream.remove(seq);
      },
      nak: (delay) => {
        settle();
        const back = () => {
          this.ready.push(seq);
          this.wake();
        };
        if (!delay) return void back();
        const retry = setTimeout(back, delay);
        if (typeof retry.unref === 'function') retry.unref();
      },
      term: () => {
        settle();
        this.stream.remove(seq);
      },
      working: () => {
        const held = this.pending.get(seq);
        if (!held) return;
        clearTimeout(held.timer);
        held.timer = setTimeout(() => {
          this.pending.delete(seq);
          this.ready.push(seq);
          this.wake();
        }, this.ackWait);
        if (typeof held.timer.unref === 'function') held.timer.unref();
      },
    };
  }

  consume({ max_messages: max = 16 } = {}) {
    void max;
    let closed = false;
    const consumer = this;
    const iterable = {
      close: async () => {
        closed = true;
        consumer.wake();
      },
      [Symbol.asyncIterator]: async function* () {
        for (;;) {
          if (closed) return;
          const seq = consumer.#next();
          if (seq === null) {
            await consumer.wait(20);
            continue;
          }
          yield consumer.#build(seq);
        }
      },
    };
    return Promise.resolve(iterable);
  }

  fetch({ max_messages: max = 16, expires = 1000 } = {}) {
    const consumer = this;
    const deadline = Date.now() + expires;
    const iterable = {
      [Symbol.asyncIterator]: async function* () {
        let taken = 0;
        while (taken < max && Date.now() < deadline) {
          const seq = consumer.#next();
          if (seq === null) {
            // A fetch answers what it has; it does not park for the full
            // window once the stream has nothing more.
            if (taken > 0) return;
            await consumer.wait(10);
            continue;
          }
          taken++;
          yield consumer.#build(seq);
        }
      },
    };
    return Promise.resolve(iterable);
  }
}

const createFakeNats = () => {
  const server = new FakeNatsServer();
  const nc = new FakeNatsConnection(server);

  const streamFor = (subject) => {
    for (const stream of server.streams.values()) {
      for (const pattern of stream.config.subjects ?? []) {
        if (pattern === subject) return stream;
        if (pattern.endsWith('.>') && subject.startsWith(pattern.slice(0, -1))) return stream;
      }
    }
    return null;
  };

  const jetstream = () => ({
    publish: async (subject, data, options = {}) => {
      const stream = streamFor(subject);
      if (!stream) throw new Error(`no stream matches subject ${subject}`);
      const seq = stream.add(subject, data, options.headers);
      return { seq, stream: stream.config.name, duplicate: false };
    },
    consumers: {
      get: async (name, durableOrConfig) => {
        const stream = server.streams.get(name);
        if (!stream) throw new Error(`stream not found: ${name}`);
        if (typeof durableOrConfig === 'string') {
          const consumer = stream.consumers.get(durableOrConfig);
          if (!consumer) throw new Error(`consumer not found: ${durableOrConfig}`);
          return consumer;
        }
        // An ephemeral consumer, as the log tail and its catch-up use.
        return new FakeConsumer(stream, durableOrConfig ?? {});
      },
    },
  });

  const jetstreamManager = async () => ({
    streams: {
      info: async (name) => {
        const stream = server.streams.get(name);
        if (!stream) throw new Error(`stream not found: ${name}`);
        return {
          config: stream.config,
          state: { first_seq: stream.first, last_seq: stream.lastSeq, messages: stream.messages.size },
        };
      },
      add: async (config) => {
        if (server.streams.has(config.name)) throw new Error('stream name already in use');
        server.streams.set(config.name, new FakeStream(config));
        return { config };
      },
      delete: async (name) => server.streams.delete(name),
      purge: async (name, options = {}) => {
        const stream = server.streams.get(name);
        if (!stream) return { purged: 0 };
        let purged = 0;
        for (const seq of Array.from(stream.messages.keys())) {
          if (options.seq !== undefined && seq >= options.seq) continue;
          stream.remove(seq);
          purged++;
        }
        return { purged };
      },
    },
    consumers: {
      add: async (name, config) => {
        const stream = server.streams.get(name);
        if (!stream) throw new Error(`stream not found: ${name}`);
        if (stream.consumers.has(config.durable_name)) throw new Error('consumer already exists');
        stream.consumers.set(config.durable_name, new FakeConsumer(stream, config));
        return { config };
      },
    },
  });

  let inboxes = 0;
  const createInbox = () => `_INBOX.fake.${++inboxes}`;

  return { nc, server, headers, jetstream, jetstreamManager, createInbox };
};

module.exports = { createFakeNats, FakeHeaders };
