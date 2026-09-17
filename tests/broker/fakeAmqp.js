'use strict';

// A small in-process RabbitMQ for the AMQP adapter's unit tests: the
// command surface src/broker/amqp/index.js uses, with the behaviours that
// matter — per-channel prefetch, unacked messages returning to the queue
// when a channel closes, `nack(requeue)` NOT counting a delivery (RabbitMQ
// 4), per-message expiration dead-lettering through a queue's DLX, stream
// queues with `x-stream-offset`, and `mandatory` + basic.return.
//
// Not a *.test.js: node --test must not run it.

const { EventEmitter } = require('node:events');

const now = () => Date.now();

class Queue {
  messages = []; // { content, properties, offset }
  consumers = new Map(); // tag -> { channel, handler, noAck, streamOffset }
  unacked = new Map(); // deliveryTag -> { message, channel }
  offset = 0;
  cursor = 0;

  constructor(name, options = {}) {
    this.name = name;
    this.options = options;
    this.arguments = options.arguments ?? {};
    this.stream = this.arguments['x-queue-type'] === 'stream';
  }

  get count() {
    return this.stream ? this.messages.length : this.messages.length;
  }
}

class FakeAmqpServer {
  queues = new Map();
  exchanges = new Map(); // name -> Map(routingKey -> Set(queueName))
  #tag = 0;
  #delivery = 0;

  nextTag(prefix) {
    return `${prefix}-${++this.#tag}`;
  }

  nextDelivery() {
    return ++this.#delivery;
  }

  queue(name) {
    return this.queues.get(name);
  }

  deliver(queueName) {
    const queue = this.queues.get(queueName);
    if (!queue) return;
    queueMicrotask(() => this.#pump(queue));
  }

  #pump(queue) {
    if (queue.consumers.size === 0) return;
    // A stream is read, not consumed: every consumer has its own cursor and
    // sees every message.
    if (queue.stream) {
      for (const consumer of queue.consumers.values()) {
        while (consumer.channel.inflight.size < consumer.channel.prefetchLimit) {
          const from = consumer.streamCursor ?? 0;
          const entry = queue.messages.find((message) => message.offset >= from);
          if (!entry) break;
          consumer.streamCursor = entry.offset + 1;
          this.#hand(queue, consumer, entry, { keep: true });
        }
      }
      return;
    }
    // A work queue: one message, one consumer, round-robin over whoever has
    // room left under its channel's prefetch.
    for (;;) {
      const consumers = Array.from(queue.consumers.values()).filter(
        (consumer) => consumer.channel.inflight.size < consumer.channel.prefetchLimit,
      );
      if (consumers.length === 0 || queue.messages.length === 0) return;
      const consumer = consumers[queue.cursor++ % consumers.length];
      this.#hand(queue, consumer, queue.messages.shift(), { keep: false });
    }
  }

  #hand(queue, consumer, entry, { keep }) {
    void keep;
    const deliveryTag = this.nextDelivery();
    const message = {
      content: entry.content,
      properties: { ...entry.properties, headers: { ...(entry.properties.headers ?? {}) } },
      fields: { deliveryTag, redelivered: entry.redelivered === true, consumerTag: consumer.tag },
    };
    if (queue.stream) message.properties.headers['x-stream-offset'] = entry.offset;
    if (!consumer.noAck) {
      consumer.channel.inflight.set(deliveryTag, { message: entry, queue });
      queue.unacked.set(deliveryTag, { message: entry, channel: consumer.channel });
    }
    const handler = consumer.handler;
    queueMicrotask(() => handler(message));
  }

  publish(exchange, routingKey, content, properties) {
    const targets = [];
    if (exchange === '') targets.push(routingKey);
    else {
      const bindings = this.exchanges.get(exchange);
      for (const name of bindings?.get(routingKey) ?? []) targets.push(name);
    }
    if (targets.length === 0) return false;
    let routed = false;
    for (const name of targets) {
      const queue = this.queues.get(name);
      if (!queue) continue;
      routed = true;
      const entry = { content, properties: { ...properties }, offset: queue.offset++ };
      if (properties.expiration) entry.expires = now() + Number(properties.expiration);
      queue.messages.push(entry);
      if (entry.expires) {
        const timer = setTimeout(() => this.#expire(queue, entry), Number(properties.expiration));
        if (typeof timer.unref === 'function') timer.unref();
      }
      this.deliver(name);
    }
    return routed;
  }

  #expire(queue, entry) {
    const at = queue.messages.indexOf(entry);
    if (at < 0) return;
    queue.messages.splice(at, 1);
    const exchange = queue.arguments['x-dead-letter-exchange'];
    const routingKey = queue.arguments['x-dead-letter-routing-key'];
    if (exchange === undefined || routingKey === undefined) return;
    this.publish(exchange, routingKey, entry.content, entry.properties);
  }
}

class FakeChannel extends EventEmitter {
  inflight = new Map();
  prefetchLimit = Infinity;
  closed = false;

  constructor(server, { confirm = false } = {}) {
    super();
    this.server = server;
    this.confirm = confirm;
  }

  async assertExchange(name, type, options) {
    if (!this.server.exchanges.has(name)) this.server.exchanges.set(name, new Map());
    return { exchange: name, type, options };
  }

  async assertQueue(name, options = {}) {
    const queueName = name === '' ? this.server.nextTag('amq.gen') : name;
    let queue = this.server.queues.get(queueName);
    if (!queue) {
      queue = new Queue(queueName, options);
      this.server.queues.set(queueName, queue);
    }
    return { queue: queueName, messageCount: queue.messages.length, consumerCount: queue.consumers.size };
  }

  async checkQueue(name) {
    const queue = this.server.queues.get(name);
    if (!queue) throw new Error(`NOT_FOUND - no queue '${name}'`);
    return { queue: name, messageCount: queue.messages.length, consumerCount: queue.consumers.size };
  }

  async deleteQueue(name) {
    this.server.queues.delete(name);
    return { messageCount: 0 };
  }

  async bindQueue(queue, exchange, routingKey) {
    const bindings = this.server.exchanges.get(exchange) ?? new Map();
    this.server.exchanges.set(exchange, bindings);
    const set = bindings.get(routingKey) ?? new Set();
    set.add(queue);
    bindings.set(routingKey, set);
  }

  async prefetch(count) {
    this.prefetchLimit = count;
  }

  async consume(name, handler, options = {}) {
    const queue = this.server.queues.get(name);
    if (!queue) throw new Error(`NOT_FOUND - no queue '${name}'`);
    const tag = this.server.nextTag('ctag');
    const consumer = { tag, channel: this, handler, noAck: options.noAck === true };
    const from = options.arguments?.['x-stream-offset'];
    if (queue.stream) {
      if (from === 'first' || from === undefined) consumer.streamCursor = 0;
      else if (from === 'last') consumer.streamCursor = Math.max(0, queue.offset - 1);
      else if (from === 'next') consumer.streamCursor = queue.offset;
      else consumer.streamCursor = Number(from);
    }
    queue.consumers.set(tag, consumer);
    this.server.deliver(name);
    return { consumerTag: tag };
  }

  async cancel(tag) {
    for (const queue of this.server.queues.values()) {
      if (queue.consumers.delete(tag)) break;
    }
  }

  ack(message) {
    const tag = message.fields.deliveryTag;
    const held = this.inflight.get(tag);
    if (!held) return;
    this.inflight.delete(tag);
    held.queue.unacked.delete(tag);
    // The slot this delivery held is free: whatever is waiting can move.
    this.server.deliver(held.queue.name);
  }

  nack(message, _all, requeue) {
    const tag = message.fields.deliveryTag;
    const held = this.inflight.get(tag);
    if (!held) return;
    this.inflight.delete(tag);
    held.queue.unacked.delete(tag);
    if (!requeue) {
      // Dead-letter it where the queue says, or drop it.
      const exchange = held.queue.arguments['x-dead-letter-exchange'];
      const routingKey = held.queue.arguments['x-dead-letter-routing-key'];
      if (exchange !== undefined && routingKey !== undefined) {
        this.server.publish(exchange, routingKey, held.message.content, held.message.properties);
      }
      return;
    }
    // RabbitMQ 4: a requeue does NOT count a delivery attempt.
    held.message.redelivered = true;
    held.queue.messages.unshift(held.message);
    this.server.deliver(held.queue.name);
  }

  // A no-op on this fake, but amqplib channels have it.
  async recover() {}

  publish(exchange, routingKey, content, properties = {}, callback) {
    const routed = this.server.publish(exchange, routingKey, content, properties);
    if (!routed && properties.mandatory) {
      queueMicrotask(() => this.emit('return', { content, properties, fields: { replyText: 'NO_ROUTE' } }));
    }
    if (this.confirm && typeof callback === 'function') queueMicrotask(() => callback(null));
    return true;
  }

  async close() {
    if (this.closed) return;
    this.closed = true;
    // Unacked messages go back to their queues, as a real close does — a
    // stream's never left it.
    for (const [tag, held] of this.inflight) {
      if (held.queue.stream) {
        held.queue.unacked.delete(tag);
        continue;
      }
      held.message.redelivered = true;
      held.queue.messages.unshift(held.message);
      held.queue.unacked.delete(tag);
      this.server.deliver(held.queue.name);
    }
    this.inflight.clear();
    for (const queue of this.server.queues.values()) {
      for (const [tag, consumer] of queue.consumers) {
        if (consumer.channel === this) queue.consumers.delete(tag);
      }
    }
    this.emit('close');
  }
}

class FakeAmqpConnection extends EventEmitter {
  constructor(server = new FakeAmqpServer()) {
    super();
    this.server = server;
    this.channels = [];
  }

  async createChannel() {
    const channel = new FakeChannel(this.server);
    this.channels.push(channel);
    return channel;
  }

  async createConfirmChannel() {
    const channel = new FakeChannel(this.server, { confirm: true });
    this.channels.push(channel);
    return channel;
  }

  async close() {
    for (const channel of this.channels) await channel.close();
  }
}

const createFakeAmqp = () => new FakeAmqpConnection();

module.exports = { createFakeAmqp, FakeAmqpConnection, FakeAmqpServer };
