'use strict';

// The RabbitMQ broker (AMQP 0-9-1): all four capabilities over an INJECTED
// amqplib connection (`amqplib` is a devDependency here, never a runtime
// one).
//
//   const amqp = require('amqplib');
//   const broker = createAmqpBroker({ connection: await amqp.connect(url) });
//
// Capability map:
//   backplane  a direct exchange; each instance binds its own exclusive
//              queue per channel — fan-out without a shared queue
//   log        a STREAM queue (`x-queue-type: stream`); the stream offset is
//              the feed's resume token
//   queue      a quorum queue per name, prefetch per consumer channel, a TTL
//              retry queue that dead-letters back for delays, a dead-letter
//              queue for what is exhausted
//   direct     an exclusive queue per inbox, a durable shared queue per
//              service group, `mandatory` + basic.return for "nobody there"
//
// Two RabbitMQ 4 behaviours shape this adapter (both measured in the
// phase-0 spike):
// - `nack(requeue)` does NOT count a delivery attempt, so the attempt lives
//   in an `x-wrpc-attempt` header and a retry is a republish;
// - a transient non-exclusive queue is refused with a CONNECTION-level 541,
//   so every shared queue here is durable.

const { createLoggerWriter } = require('../../logging.js');
const { generateUUID } = require('../../runtime/node.js');
const { TopicTails } = require('../tail.js');
const { codedError, toBytes, toHeaders, encodeToken } = require('../ids.js');

const DEFAULT_PREFIX = 'wrpc';
const DEFAULT_PREFETCH = 16;
const DEFAULT_INBOX_TTL = 60_000;
const DEFAULT_QUEUE_TYPE = 'quorum';
const OFFSET = /^\d{1,19}$/;

const ATTEMPT_HEADER = 'x-wrpc-attempt';
const REDELIVERED_HEADER = 'x-wrpc-redelivered';
const DEAD_REASON_HEADER = 'x-wrpc-dead-reason';
const STREAM_OFFSET = 'x-stream-offset';

const isFunction = (value) => typeof value === 'function';
const name = (prefix, kind, value) => `${prefix}.${kind}.${encodeToken(value, { maxLength: 180 })}`;

const createAmqpBroker = (options = {}) => {
  const {
    connection,
    prefix = DEFAULT_PREFIX,
    logger = globalThis.console,
    queueType = DEFAULT_QUEUE_TYPE,
    inboxTtl = DEFAULT_INBOX_TTL,
    streamMaxBytes = 0,
  } = options;
  if (!connection || !isFunction(connection.createChannel) || !isFunction(connection.createConfirmChannel)) {
    throw new TypeError('createAmqpBroker: options.connection must be an amqplib connection');
  }
  const log = createLoggerWriter(logger).child({ component: 'broker', broker: 'amqp' });
  const report = (event, error, extra = {}) => log.error({ err: error, event, ...extra });
  let closed = false;
  const channels = new Set();

  const openChannel = async (confirm = false) => {
    const channel = confirm ? await connection.createConfirmChannel() : await connection.createChannel();
    // A channel-level error (a mismatched queue declaration) closes the
    // channel, not the process.
    channel.on?.('error', (error) => report('broker.amqp.channel', error));
    channel.on?.('close', () => channels.delete(channel));
    channels.add(channel);
    return channel;
  };

  const closeChannel = async (channel) => {
    channels.delete(channel);
    try {
      await channel.close();
    } catch {
      // Already closed by the broker, or the connection went with it.
    }
  };

  // Both memoized as PROMISES, not as resolved channels: two concurrent
  // callers awaiting a lazily created channel would otherwise each create
  // one — and a publisher spread over two channels loses AMQP's ordering
  // guarantee, which is per channel.
  let topologyChannel = null;
  const topology = () => {
    if (!topologyChannel) {
      topologyChannel = openChannel();
      topologyChannel.catch(() => {
        topologyChannel = null;
      });
    }
    return topologyChannel;
  };

  let publishChannel = null;
  const publisher = () => {
    if (!publishChannel) {
      publishChannel = openChannel(true);
      publishChannel.catch(() => {
        publishChannel = null;
      });
    }
    return publishChannel;
  };

  // Publishes and waits for the broker's confirm: what makes an append or a
  // produce a promise the caller can trust.
  const confirmPublish = async (exchange, routingKey, body, options = {}) => {
    const channel = await publisher();
    await new Promise((resolve, reject) => {
      channel.publish(exchange, routingKey, Buffer.from(toBytes(body)), options, (error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  };

  const headersOf = (message) => {
    const bag = toHeaders(message?.properties?.headers ?? null);
    return bag;
  };

  // ---------------------------------------------------------------------
  // backplane

  const exchange = `${prefix}.bp`;
  let exchangeReady = null;
  const ensureExchange = async () => {
    if (!exchangeReady) {
      exchangeReady = (async () => {
        const channel = await topology();
        await channel.assertExchange(exchange, 'direct', { durable: true });
      })();
      exchangeReady.catch(() => {
        exchangeReady = null;
      });
    }
    return exchangeReady;
  };

  const backplane = {
    name: 'amqp',
    publish(channel, message) {
      if (closed) return;
      void (async () => {
        try {
          await ensureExchange();
          await confirmPublish(exchange, encodeToken(channel, { maxLength: 180 }), message, {
            contentType: 'application/json',
          });
        } catch (error) {
          report('broker.amqp.publish', error, { channel });
        }
      })();
    },
    subscribe(channel, handler) {
      if (!isFunction(handler)) throw new TypeError('amqp backplane.subscribe: handler must be a function');
      const routingKey = encodeToken(channel, { maxLength: 180 });
      const ready = (async () => {
        await ensureExchange();
        const consumerChannel = await openChannel();
        // Exclusive and auto-delete: this instance's own copy of the
        // channel's traffic, gone the moment it disconnects.
        const { queue } = await consumerChannel.assertQueue('', { exclusive: true, autoDelete: true });
        await consumerChannel.bindQueue(queue, exchange, routingKey);
        const { consumerTag } = await consumerChannel.consume(
          queue,
          (message) => {
            // null = the broker cancelled this consumer.
            if (message === null) {
              return void report('broker.amqp.cancelled', new Error('consumer cancelled'), { channel });
            }
            try {
              handler(message.content.toString());
            } catch (error) {
              report('broker.amqp.handler', error, { channel });
            }
          },
          { noAck: true },
        );
        return async () => {
          try {
            await consumerChannel.cancel(consumerTag);
          } catch {
            // The channel is already gone.
          }
          await closeChannel(consumerChannel);
        };
      })();
      return ready;
    },
    close() {
      // Channels are closed by the broker's own close().
    },
  };

  // ---------------------------------------------------------------------
  // log — a stream queue, the offset as the id

  const logQueue = (topic) => name(prefix, 'log', topic);
  const ensuredLogs = new Map();
  const ensureLog = (topic) => {
    const queue = logQueue(topic);
    let pending = ensuredLogs.get(queue);
    if (pending) return pending;
    pending = (async () => {
      const channel = await topology();
      const args = { 'x-queue-type': 'stream' };
      if (streamMaxBytes > 0) args['x-max-length-bytes'] = streamMaxBytes;
      await channel.assertQueue(queue, { durable: true, arguments: args });
      return queue;
    })();
    ensuredLogs.set(queue, pending);
    pending.catch(() => ensuredLogs.delete(queue));
    return pending;
  };

  const offsetOf = (message) => Number(message.properties?.headers?.[STREAM_OFFSET] ?? -1);

  // One stream consumer, reading from `from` and handing every message to
  // `onMessage` until `stop()`. Streams need a prefetch, so each reader gets
  // its own channel.
  const openStreamReader = async (topic, from, onMessage) => {
    const queue = await ensureLog(topic);
    const channel = await openChannel();
    await channel.prefetch(64);
    const { consumerTag } = await channel.consume(
      queue,
      (message) => {
        if (message === null) return;
        channel.ack(message);
        onMessage(message);
      },
      { noAck: false, arguments: { 'x-stream-offset': from } },
    );
    return async () => {
      try {
        await channel.cancel(consumerTag);
      } catch {
        // Already cancelled with the channel.
      }
      await closeChannel(channel);
    };
  };

  const entryOf = (message) => {
    const headers = headersOf(message);
    // The offset is the entry's id, not one of its headers.
    delete headers[STREAM_OFFSET];
    return { offset: offsetOf(message), value: message.content.toString(), headers };
  };

  const tails = new TopicTails({
    live: async (topic, { signal, onEntry }) => {
      // The tip BEFORE the reader starts: `next` then guarantees the reader
      // sees everything appended from here on and nothing before it. (The
      // count is exact until something truncates the stream, and an
      // under-reported tip only means a live entry is not filtered — never
      // that one is dropped.)
      const queue = await ensureLog(topic);
      const channel = await topology();
      const { messageCount } = await channel.checkQueue(queue);
      const stop = await openStreamReader(topic, 'next', (message) => onEntry(entryOf(message)));
      signal.addEventListener('abort', () => void stop(), { once: true });
      return messageCount > 0 ? messageCount - 1 : null;
    },
    range: async (topic, { after, limit }) => {
      const entries = [];
      await new Promise((resolve, reject) => {
        let settled = false;
        let idle = null;
        const finish = async () => {
          if (settled) return;
          settled = true;
          clearTimeout(idle);
          await stopReader?.();
          resolve();
        };
        let stopReader = null;
        const rearm = () => {
          clearTimeout(idle);
          // A stream read has no "end": what has arrived when the flow
          // pauses IS the page.
          idle = setTimeout(() => void finish(), 50);
          if (isFunction(idle.unref)) idle.unref();
        };
        openStreamReader(topic, after === null || after === undefined ? 'first' : after + 1, (message) => {
          entries.push(entryOf(message));
          if (entries.length >= limit) return void finish();
          rearm();
        }).then((stop) => {
          stopReader = stop;
          if (settled) void stop();
          else rearm();
        }, reject);
      });
      return entries;
    },
    covered: (cursor, entry) => entry.offset <= cursor,
    advance: (_cursor, entry) => entry.offset,
  });

  const parseId = (text) => (typeof text === 'string' && OFFSET.test(text) ? text : null);

  const failedRead = (error) => {
    const rejected = Promise.reject(error);
    rejected.catch(() => {});
    return {
      ready: rejected,
      [Symbol.asyncIterator]: () => ({
        next: () => Promise.reject(error),
        return: () => Promise.resolve({ value: undefined, done: true }),
      }),
    };
  };

  const stringifyIds = (inner, guard) => {
    const ready = guard ? Promise.all([guard, inner.ready]).then(() => undefined) : inner.ready;
    ready.catch(() => {});
    return {
      ready,
      [Symbol.asyncIterator]: () => {
        const iterator = inner[Symbol.asyncIterator]();
        return {
          next: async () => {
            const result = await iterator.next();
            return result.done ? result : { done: false, value: { ...result.value, id: String(result.value.id) } };
          },
          return: (value) => iterator.return?.(value) ?? Promise.resolve({ value, done: true }),
        };
      },
    };
  };

  const read = (topic, options = {}) => {
    const { after = null, from = 'latest', signal = null } = options;
    if (from !== 'latest' && from !== 'earliest') {
      throw new TypeError("amqp log.read: from must be 'latest' or 'earliest'");
    }
    if (after === null || after === undefined) {
      return stringifyIds(tails.read(topic, from === 'earliest' ? { after: -1, signal } : { from, signal }), null);
    }
    if (parseId(after) === null) return failedRead(codedError('Malformed event id', 400));
    const position = Number(after);
    const inner = tails.read(topic, { after: position, signal });
    // RabbitMQ silently starts a stream reader at the OLDEST retained
    // message when the requested offset is gone, so the gap shows up as a
    // first entry further along than asked for.
    const wrapped = {
      ready: inner.ready,
      [Symbol.asyncIterator]: () => {
        const iterator = inner[Symbol.asyncIterator]();
        let first = true;
        return {
          next: async () => {
            const result = await iterator.next();
            if (result.done) return result;
            if (first) {
              first = false;
              if (result.value.id > position + 1) {
                await iterator.return?.();
                throw codedError('Event history was trimmed past this id', 410);
              }
            }
            return result;
          },
          return: (value) => iterator.return?.(value) ?? Promise.resolve({ value, done: true }),
        };
      },
    };
    return stringifyIds(wrapped, null);
  };

  // AMQP 0-9-1 does not report the offset a publish landed on — only the
  // stream protocol does — and `messageCount` lags a stream badly. So the
  // append reads the tip back: one message from `last`, cancelled at once.
  // That is an extra round trip per append, which is why a high-rate
  // producer should publish through a `log` on a broker that answers with
  // the offset (Redis, NATS, Kafka) or ignore the receipt and let readers
  // yield the authoritative ids.
  const tipOffset = async (topic) => {
    const queue = await ensureLog(topic);
    const channel = await openChannel();
    try {
      await channel.prefetch(1);
      return await new Promise((resolve, reject) => {
        const timer = setTimeout(() => resolve(null), 2000);
        if (isFunction(timer.unref)) timer.unref();
        channel
          .consume(
            queue,
            (message) => {
              if (message === null) return;
              clearTimeout(timer);
              channel.ack(message);
              resolve(offsetOf(message));
            },
            { noAck: false, arguments: { 'x-stream-offset': 'last' } },
          )
          .catch(reject);
      });
    } finally {
      await closeChannel(channel);
    }
  };

  const append = async (topic, value, { headers = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    const queue = await ensureLog(topic);
    await confirmPublish('', queue, value, { persistent: true, headers: toHeaders(headers) });
    const offset = await tipOffset(topic);
    return String(offset === null ? 0 : offset);
  };

  // ---------------------------------------------------------------------
  // queue

  const workQueue = (queue) => name(prefix, 'q', queue);
  const retryQueue = (queue) => `${workQueue(queue)}.retry`;
  const ensuredQueues = new Map();

  const ensureQueue = (queue, deadLetter = null) => {
    const key = `${queue}|${deadLetter ?? ''}`;
    let pending = ensuredQueues.get(key);
    if (pending) return pending;
    pending = (async () => {
      const channel = await topology();
      const main = workQueue(queue);
      const args = { 'x-queue-type': queueType };
      if (deadLetter) {
        await channel.assertQueue(workQueue(deadLetter), {
          durable: true,
          arguments: { 'x-queue-type': queueType },
        });
      }
      await channel.assertQueue(main, { durable: true, arguments: args });
      // The retry queue has no consumers: a message expires and is
      // dead-lettered back into the main queue, which is the delay.
      await channel.assertQueue(retryQueue(queue), {
        durable: true,
        arguments: {
          'x-queue-type': 'classic',
          'x-dead-letter-exchange': '',
          'x-dead-letter-routing-key': main,
        },
      });
      return main;
    })();
    ensuredQueues.set(key, pending);
    pending.catch(() => ensuredQueues.delete(key));
    return pending;
  };

  const produce = async (queue, body, { headers = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    const main = await ensureQueue(queue);
    await confirmPublish('', main, body, { persistent: true, headers: toHeaders(headers) });
  };

  const consume = async (queue, onDelivery, options = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    if (!isFunction(onDelivery)) throw new TypeError('amqp queue.consume: onDelivery must be a function');
    const { prefetch = DEFAULT_PREFETCH, deadLetter = null, signal = null } = options;
    if (!Number.isInteger(prefetch) || prefetch <= 0) {
      throw new TypeError('amqp queue.consume: prefetch must be a positive integer');
    }
    const main = await ensureQueue(queue, deadLetter);
    const channel = await openChannel();
    await channel.prefetch(prefetch);
    const state = { running: true, healthy: true, tag: null };

    const dispatch = (message) => {
      if (message === null) {
        // The broker cancelled this consumer (the queue was deleted, or the
        // node it lived on went away).
        state.healthy = false;
        return void report('broker.amqp.cancelled', new Error('consumer cancelled'), { queue });
      }
      const headers = headersOf(message);
      const attempt = Number(headers[ATTEMPT_HEADER] ?? '1') || 1;
      let settled = false;
      const finish = async (work) => {
        if (settled) return;
        settled = true;
        try {
          await work();
        } catch (error) {
          report('broker.amqp.settle', error, { queue });
        }
      };
      const carry = (extra) => ({ ...headers, [REDELIVERED_HEADER]: '1', ...extra });
      const delivery = Object.freeze({
        id: String(message.properties?.messageId ?? message.fields?.deliveryTag ?? ''),
        body: message.content.toString(),
        headers,
        attempt,
        redelivered: message.fields?.redelivered === true || headers[REDELIVERED_HEADER] === '1',
        ack: () => finish(() => channel.ack(message)),
        // A requeue would NOT count the attempt on RabbitMQ 4, so a retry is
        // a republish — through the TTL queue when it must wait.
        retry: ({ delay = 0 } = {}) =>
          finish(async () => {
            const headersOut = carry({ [ATTEMPT_HEADER]: String(attempt + 1) });
            if (delay > 0) {
              await confirmPublish('', retryQueue(queue), message.content, {
                persistent: true,
                headers: headersOut,
                expiration: String(Math.round(delay)),
              });
            } else {
              await confirmPublish('', main, message.content, { persistent: true, headers: headersOut });
            }
            channel.ack(message);
          }),
        // Exactly what a requeue means on RabbitMQ 4: back in line, attempt
        // untouched.
        release: () => finish(() => channel.nack(message, false, true)),
        deadLetter: (reason = '') =>
          finish(async () => {
            if (deadLetter) {
              await confirmPublish('', workQueue(deadLetter), message.content, {
                persistent: true,
                headers: {
                  ...headers,
                  [DEAD_REASON_HEADER]: String(reason),
                  [ATTEMPT_HEADER]: String(attempt),
                },
              });
            }
            channel.ack(message);
          }),
      });
      Promise.resolve()
        .then(() => onDelivery(delivery))
        .catch((error) => {
          report('broker.amqp.delivery', error, { queue });
          void delivery.release();
        });
    };

    const start = async () => {
      const { consumerTag } = await channel.consume(main, dispatch, { noAck: false });
      state.tag = consumerTag;
      state.healthy = true;
    };
    await start();

    const cancel = async () => {
      if (state.tag === null) return;
      const tag = state.tag;
      state.tag = null;
      try {
        await channel.cancel(tag);
      } catch {
        // The channel is gone; its unacked messages are already back.
      }
    };
    const stop = async () => {
      if (!state.running) return;
      state.running = false;
      await cancel();
      // Closing the channel returns whatever was unacked to the queue.
      await closeChannel(channel);
    };
    if (signal) signal.addEventListener('abort', () => void stop(), { once: true });
    return {
      stop,
      // basic.cancel keeps the channel, so the messages this consumer holds
      // stay ackable — which is what a draining node needs.
      pause: cancel,
      resume: async () => {
        if (!state.running || state.tag !== null) return;
        await start();
      },
      get healthy() {
        return state.running && state.healthy && !closed;
      },
    };
  };

  // ---------------------------------------------------------------------
  // direct

  // One fanout exchange per address: plain listeners each bind their own
  // exclusive queue (everyone receives), a group binds ONE durable queue
  // (its members compete). `mandatory` then reports "nobody bound" as a
  // basic.return, which is the 503 an RPC caller wants.
  const addressExchange = (address) => name(prefix, 'direct', address);
  const groupQueue = (address, group) => `${addressExchange(address)}.${encodeToken(group, { maxLength: 60 })}`;

  const returned = new Set();
  let directChannel = null;
  // Memoized as a PROMISE: two concurrent sends must share one channel, or
  // their publishes could interleave across two — and a channel is what
  // AMQP orders messages on.
  const directing = () => {
    if (directChannel) return directChannel;
    directChannel = (async () => {
      const channel = await openChannel(true);
      // `mandatory` + basic.return is how a sender learns that nobody is
      // there — an unroutable message comes back before its confirm.
      channel.on?.('return', (message) => {
        const id = message.properties?.messageId;
        if (id) returned.add(id);
      });
      return channel;
    })();
    directChannel.catch(() => {
      directChannel = null;
    });
    return directChannel;
  };

  // The exchange must exist before a mandatory publish: publishing to a
  // missing one is a channel-level 404, not a basic.return.
  const declaredAddresses = new Map();
  const ensureAddress = (address) => {
    const exchangeName = addressExchange(address);
    let pending = declaredAddresses.get(exchangeName);
    if (pending) return pending;
    pending = (async () => {
      const channel = await topology();
      await channel.assertExchange(exchangeName, 'fanout', { durable: true });
      return exchangeName;
    })();
    declaredAddresses.set(exchangeName, pending);
    pending.catch(() => declaredAddresses.delete(exchangeName));
    return pending;
  };

  const listen = async (address, onMessage, { group = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    if (!isFunction(onMessage)) throw new TypeError('amqp direct.listen: onMessage must be a function');
    if (typeof address !== 'string' || address.length === 0) {
      throw new TypeError('amqp direct.listen: address must be a non-empty string');
    }
    const channel = await openChannel();
    const exchangeName = await ensureAddress(address);
    // RabbitMQ 4 refuses a transient non-exclusive queue, so a group's
    // shared queue is durable and expires when nobody consumes it.
    const queue =
      group === null || group === undefined
        ? (await channel.assertQueue('', { exclusive: true, autoDelete: true })).queue
        : (
            await channel.assertQueue(groupQueue(address, group), {
              durable: true,
              arguments: { 'x-expires': inboxTtl, 'x-queue-type': 'classic' },
            })
          ).queue;
    await channel.bindQueue(queue, exchangeName, '');
    const { consumerTag } = await channel.consume(
      queue,
      (message) => {
        if (message === null) return void report('broker.amqp.cancelled', new Error('consumer cancelled'), { address });
        const headers = headersOf(message);
        const text = headers['wrpc-text'] === '1';
        delete headers['wrpc-text'];
        try {
          const result = onMessage({
            body: text ? message.content.toString() : new Uint8Array(message.content),
            headers,
            correlationId: message.properties?.correlationId ?? null,
            replyTo: message.properties?.replyTo ?? null,
          });
          if (result && isFunction(result.catch)) result.catch((error) => report('broker.amqp.listener', error));
        } catch (error) {
          report('broker.amqp.listener', error, { address });
        }
      },
      { noAck: true },
    );
    return async () => {
      try {
        await channel.cancel(consumerTag);
      } catch {
        // Already cancelled.
      }
      await closeChannel(channel);
    };
  };

  const send = async (address, body, { headers = null, correlationId = null, replyTo = null, timeout } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    const [channel, exchangeName] = await Promise.all([directing(), ensureAddress(address)]);
    const text = typeof body === 'string';
    const messageId = generateUUID();
    const properties = {
      headers: { ...toHeaders(headers), 'wrpc-text': text ? '1' : '0' },
      messageId,
      persistent: false,
      mandatory: true,
    };
    if (correlationId !== null && correlationId !== undefined) properties.correlationId = String(correlationId);
    if (replyTo !== null && replyTo !== undefined) properties.replyTo = replyTo;
    // A request nobody takes in time is dropped by the broker rather than
    // answered late.
    if (timeout > 0) properties.expiration = String(Math.round(timeout));
    await new Promise((resolve, reject) => {
      channel.publish(exchangeName, '', Buffer.from(toBytes(body)), properties, (error) => {
        if (error) return void reject(error);
        if (returned.delete(messageId)) return void reject(codedError(`No listener at ${address}`, 503));
        resolve();
      });
    });
  };

  // ---------------------------------------------------------------------

  const close = async () => {
    if (closed) return;
    closed = true;
    tails.close();
    ensuredLogs.clear();
    ensuredQueues.clear();
    declaredAddresses.clear();
    for (const channel of Array.from(channels)) await closeChannel(channel);
    topologyChannel = null;
    publishChannel = null;
    directChannel = null;
    // The CONNECTION is injected: closing it is the caller's business.
  };

  return {
    name: 'amqp',
    backplane,
    log: Object.freeze({ name: 'amqp', append, read, parseId }),
    queue: Object.freeze({ name: 'amqp', produce, consume }),
    direct: Object.freeze({ name: 'amqp', inbox: () => `${prefix}.inbox.${generateUUID()}`, listen, send }),
    close,
  };
};

module.exports = { createAmqpBroker };
