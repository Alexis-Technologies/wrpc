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
//   direct     ONE direct exchange, the address as the routing key; an
//              exclusive queue per inbox, a durable shared queue per
//              service group, `mandatory` + basic.return for "nobody there"
//
// Two RabbitMQ 4 behaviours shape this adapter (both measured in the
// phase-0 spike):
// - `nack(requeue)` does NOT count a delivery attempt, so the attempt lives
//   in an `x-wrpc-attempt` header and a retry is a republish;
// - a transient non-exclusive queue is refused with a CONNECTION-level 541,
//   so every shared queue here is durable.

const { createLoggerWriter } = require('../../logging.js');
const { backoffDelay } = require('../../utils.js');
const { TopicTails } = require('../tail.js');
const { codedError, toBytes, toHeaders, reasonText, encodeToken } = require('../ids.js');
const { positiveInteger } = require('../retry.js');
const { withHealth } = require('../port.js');
const {
  ATTEMPT_HEADER,
  REDELIVERED_HEADER,
  DEAD_REASON_HEADER,
  TEXT_HEADER,
  DEFAULT_PREFETCH,
  idFactory,
  failedRead,
  guardedRead,
  checkConsume,
  checkListen,
  runDelivery,
} = require('../adapter.js');

const DEFAULT_PREFIX = 'wrpc';
const DEFAULT_INBOX_TTL = 60_000;
const DEFAULT_QUEUE_TYPE = 'quorum';
const OFFSET = /^\d{1,19}$/;

const STREAM_OFFSET = 'x-stream-offset';

const isFunction = (value) => typeof value === 'function';
const name = (prefix, kind, value) => `${prefix}.${kind}.${encodeToken(value, { maxLength: 180 })}`;
const sleep = (ms) =>
  new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    if (isFunction(timer.unref)) timer.unref();
  });
// A consumer's channel is re-opened on this schedule after the server
// closed it; a lost CONNECTION stops the loop (nothing to re-open on).
const REOPEN_BACKOFF = Object.freeze({ minDelay: 200, maxDelay: 5000, factor: 2, jitter: true });
// A settlement that has to PUBLISH (a retry's copy, a dead letter) and was
// refused is tried again on this schedule; when it still fails, the
// message goes back to the queue with a requeue — at least once, attempt
// untouched — rather than being acked away or left unacked on a channel
// that may close later.
const SETTLE_ATTEMPTS = 3;
const SETTLE_BACKOFF = Object.freeze({ minDelay: 100, maxDelay: 1000, factor: 2, jitter: false });
// A catch-up page over a stream queue: a stream reader has no "end", so a
// page is complete when it reaches the tip read BEFORE the reader started
// — never when the deliveries merely paused. `RANGE_IDLE` is how long a
// pause may last before the page is handed back incomplete (TopicTails
// asks again from the cursor), `RANGE_TIMEOUT` the most one page waits
// overall, `RANGE_TIP_TIMEOUT` how long an EMPTY stream is given to
// answer "last" before it is taken for empty.
const RANGE_IDLE = 250;
const RANGE_TIMEOUT = 10_000;
const RANGE_TIP_TIMEOUT = 500;

const createAmqpBroker = (options = {}) => {
  const {
    connection,
    prefix = DEFAULT_PREFIX,
    logger = globalThis.console,
    queueType = DEFAULT_QUEUE_TYPE,
    inboxTtl = DEFAULT_INBOX_TTL,
    streamMaxBytes = 0,
    generateId = null,
  } = options;
  // Ids: an injected generator is used verbatim — see idFactory.
  const { nextId } = idFactory(generateId, 'createAmqpBroker');
  if (!connection || !isFunction(connection.createChannel) || !isFunction(connection.createConfirmChannel)) {
    throw new TypeError('createAmqpBroker: options.connection must be an amqplib connection');
  }
  // Strict, at construction: these become queue ARGUMENTS, and RabbitMQ
  // refuses a redeclaration with different ones with a channel-level error
  // long after the typo — a `queueType` it does not know, an `inboxTtl`
  // read as a string.
  if (queueType !== 'quorum' && queueType !== 'classic') {
    throw new TypeError("createAmqpBroker: options.queueType must be 'quorum' or 'classic'");
  }
  if (!positiveInteger(inboxTtl)) {
    throw new TypeError('createAmqpBroker: options.inboxTtl must be a positive integer of milliseconds');
  }
  if (!Number.isInteger(streamMaxBytes) || streamMaxBytes < 0) {
    throw new TypeError('createAmqpBroker: options.streamMaxBytes must be a non-negative integer (0 for unbounded)');
  }
  const log = createLoggerWriter(logger).child({ component: 'broker', broker: 'amqp' });
  const report = (event, error, extra = {}) => log.error({ err: error, event, ...extra });
  let closed = false;
  // The injected connection went away: every channel with it, and nothing
  // here can open another — the connection is the caller's to reopen, with
  // a new broker on it. Set once, reported once, and what every retry loop
  // and health getter reads.
  let lost = false;
  const channels = new Set();

  // `onClose` hears the channel close, with the error that closed it when
  // the server did (a channel-level error arrives first, the close after).
  const openChannel = async (confirm = false, onClose = null) => {
    const channel = confirm ? await connection.createConfirmChannel() : await connection.createChannel();
    // A channel-level error (a mismatched queue declaration, a missing
    // queue) closes the channel, not the process.
    let failure = null;
    channel.on?.('error', (error) => {
      failure = error;
      report('broker.amqp.channel', error);
    });
    channel.on?.('close', () => {
      channels.delete(channel);
      if (onClose !== null) onClose(failure);
    });
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

  // A lazily opened channel memoized as a PROMISE, not as a resolved
  // channel — two concurrent callers must share it, since a publisher
  // spread over two channels loses AMQP's ordering guarantee, which is
  // per channel — and FORGOTTEN the moment the channel closes. It used to
  // be kept: a channel-level error (a mismatched declaration, a missing
  // queue) closed the channel, the memo went on handing out the corpse,
  // and every later declaration, publish and send failed with "channel
  // closed" until the broker itself was closed.
  const memoChannel = (confirm, { setup = null, onClose = null } = {}) => {
    let memo = null;
    const get = () => {
      if (memo !== null) return memo;
      const pending = openChannel(confirm, (error) => {
        if (memo === pending) memo = null;
        if (onClose !== null) onClose(error);
      }).then(async (channel) => {
        if (setup !== null) await setup(channel);
        return channel;
      });
      memo = pending;
      pending.catch(() => {
        if (memo === pending) memo = null;
      });
      return pending;
    };
    get.reset = () => {
      memo = null;
    };
    return get;
  };

  const topology = memoChannel(false);
  const publisher = memoChannel(true);

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

  // The two exchanges this adapter declares — the backplane's and the
  // direct one — memoized as promises; a failed declaration is forgotten,
  // so the next caller tries again. Nothing is declared per address.
  const exchanges = new Map();
  const ensureExchange = (exchangeName, type) => {
    let pending = exchanges.get(exchangeName);
    if (pending) return pending;
    pending = (async () => {
      const channel = await topology();
      await channel.assertExchange(exchangeName, type, { durable: true });
      return exchangeName;
    })();
    exchanges.set(exchangeName, pending);
    pending.catch(() => exchanges.delete(exchangeName));
    return pending;
  };

  const exchange = `${prefix}.bp`;

  // One consumer channel and ONE exclusive queue for the whole backplane,
  // bound and unbound by routing key as rooms come and go. It used to be a
  // channel and a queue per subscription — and a room is a subscription —
  // so a server with a few hundred rooms ran into RabbitMQ's channel_max
  // (2047 by default, lower behind many proxies), after which every new
  // room failed to subscribe. The channel count is constant now, and the
  // same idea as src/scaling/redis.js: one connection, many channels.
  const handlers = new Map(); // routing key -> Set<handler>
  const bound = new Set(); // routing keys bound on the current queue
  const chains = new Map(); // routing key -> its last bind/unbind, so they run in order
  let consumerQueue = null;
  let rebinding = false;
  const consuming = memoChannel(false, {
    setup: async (consumerChannel) => {
      await ensureExchange(exchange, 'direct');
      // Exclusive and auto-delete: this instance's own copy of the traffic,
      // gone the moment it disconnects.
      const { queue } = await consumerChannel.assertQueue('', { exclusive: true, autoDelete: true });
      consumerQueue = queue;
      bound.clear();
      await consumerChannel.consume(
        queue,
        (message) => {
          // null = the server cancelled this consumer (its queue deleted,
          // its node gone). The channel stays open and hears nothing — this
          // used to return, as if it closed, and the backplane went deaf
          // without a line. Closed here, so onClose rebinds on a fresh one.
          if (message === null) {
            report('broker.amqp.cancelled', new Error('consumer cancelled'), { queue: 'backplane' });
            void closeChannel(consumerChannel);
            return;
          }
          const set = handlers.get(message.fields?.routingKey);
          if (set === undefined) return;
          const text = message.content.toString();
          // Handlers are synchronous (a RoomsBackplane's), so the copy is
          // what lets one unsubscribe from inside its own callback.
          for (const handler of Array.from(set)) {
            try {
              handler(text);
            } catch (error) {
              report('broker.amqp.handler', error, { channel: message.fields?.routingKey });
            }
          }
        },
        { noAck: true },
      );
    },
    onClose: () => {
      consumerQueue = null;
      bound.clear();
      if (closed || lost || handlers.size === 0) return;
      void rebind();
    },
  });

  // Binds or unbinds one routing key to match `handlers`, serialized per
  // key: AMQP answers bind and unbind in order on one channel, so a
  // subscribe racing an unsubscribe on the same room settles as the last
  // caller asked.
  const reconcile = (key) => {
    const step = async () => {
      const want = handlers.has(key);
      if (want === bound.has(key)) return;
      const consumerChannel = await consuming();
      const queue = consumerQueue;
      if (queue === null) throw new Error('backplane channel closed');
      if (want) await consumerChannel.bindQueue(queue, exchange, key);
      else await consumerChannel.unbindQueue(queue, exchange, key);
      // The queue may have gone while the bind was in flight: onClose
      // cleared `bound`, and the rebind will do this key again.
      if (consumerQueue !== queue) return;
      if (want) bound.add(key);
      else bound.delete(key);
    };
    const next = (chains.get(key) ?? Promise.resolve()).then(step, step);
    chains.set(key, next);
    next
      .finally(() => {
        if (chains.get(key) === next) chains.delete(key);
      })
      .catch(() => {});
    return next;
  };

  // The consumer channel closed under live subscriptions (a node went
  // away, the server cancelled the consumer): a fresh channel, a fresh
  // queue, every key bound again — with a backoff, and one line when it
  // is back. Events published meanwhile are lost, which the rooms layer
  // sees as a sequence gap (a backplane is at-most-once).
  const rebind = async () => {
    if (rebinding) return;
    rebinding = true;
    try {
      for (let attempt = 0; ; attempt++) {
        if (closed || lost || handlers.size === 0) return;
        await sleep(backoffDelay({ ...REOPEN_BACKOFF, attempt }));
        if (closed || lost || handlers.size === 0) return;
        try {
          await consuming();
          await Promise.all(Array.from(handlers.keys(), (key) => reconcile(key)));
          log.warn({ event: 'broker.amqp.backplane.rebind', channels: handlers.size, attempt: attempt + 1 });
          return;
        } catch (error) {
          report('broker.amqp.backplane.rebind', error, { attempt: attempt + 1 });
        }
      }
    } finally {
      rebinding = false;
    }
  };

  const backplane = {
    name: 'amqp',
    publish(channel, message) {
      if (closed) return;
      void (async () => {
        try {
          await ensureExchange(exchange, 'direct');
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
      const key = encodeToken(channel, { maxLength: 180 });
      let set = handlers.get(key);
      if (set === undefined) {
        set = new Set();
        handlers.set(key, set);
      }
      set.add(handler);
      let active = true;
      const unsubscribe = async () => {
        if (!active) return;
        active = false;
        const current = handlers.get(key);
        if (current === undefined) return;
        current.delete(handler);
        if (current.size > 0) return;
        handlers.delete(key);
        if (closed || lost) return;
        await reconcile(key).catch((error) => report('broker.amqp.unbind', error, { channel }));
      };
      return reconcile(key).then(
        () => unsubscribe,
        (error) => {
          set.delete(handler);
          if (set.size === 0) handlers.delete(key);
          throw error;
        },
      );
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
  // `onEnd(error)` hears the reader die on its own — the server cancelled
  // the consumer (the stream deleted, its node gone) or closed the channel
  // — once, and never after the returned stop().
  const openStreamReader = async (topic, from, onMessage, onEnd = null) => {
    const queue = await ensureLog(topic);
    let stopped = false;
    const ended = (error) => {
      if (stopped || onEnd === null) return;
      stopped = true;
      onEnd(error);
    };
    const channel = await openChannel(false, (error) => ended(error ?? new Error('channel closed')));
    await channel.prefetch(64);
    const { consumerTag } = await channel.consume(
      queue,
      (message) => {
        if (message === null) return void ended(new Error('consumer cancelled'));
        channel.ack(message);
        onMessage(message);
      },
      { noAck: false, arguments: { 'x-stream-offset': from } },
    );
    return async () => {
      stopped = true;
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
    // Readers that fell behind the tail and are catching up through range():
    // info, not a fault — but the first thing to look at when a feed is slow.
    onLag: (topic, readers) => log.info({ event: 'broker.tail.lag', topic, readers }),
    live: async (topic, { signal, onEntry, onEnd }) => {
      // The tip BEFORE the reader starts: `next` then guarantees the reader
      // sees everything appended from here on and nothing before it. (The
      // count is exact until something truncates the stream, and an
      // under-reported tip only means a live entry is not filtered — never
      // that one is dropped.)
      const queue = await ensureLog(topic);
      const channel = await topology();
      let messageCount;
      try {
        ({ messageCount } = await channel.checkQueue(queue));
      } catch (error) {
        // The stream is gone under the memo (deleted by hand): forgotten,
        // so the next read declares it again instead of failing forever.
        ensuredLogs.delete(queue);
        throw error;
      }
      // A tail that died on its own (the node it lived on went away, the
      // stream was deleted) used to leave every subscription of the topic
      // frozen for good; now the readers move to a fresh tail.
      const stop = await openStreamReader(
        topic,
        'next',
        (message) => onEntry(entryOf(message)),
        (error) => {
          report('broker.amqp.tail', error, { topic });
          onEnd(error);
        },
      );
      signal.addEventListener('abort', () => void stop(), { once: true });
      return messageCount > 0 ? messageCount - 1 : null;
    },
    range: async (topic, { after, limit }) => {
      const start = after === null || after === undefined ? -1 : after;
      // The tip FIRST, then the page: it is complete once it reaches that
      // offset, whatever the timing of the deliveries. "Nothing for 50 ms"
      // used to be the end of a page, and a broker that paused for 60 ms
      // handed back a page called complete with entries still behind it —
      // which the reader then joined the live tail past.
      const tip = await tipOffset(topic, RANGE_TIP_TIMEOUT);
      if (tip === null || start >= tip) return { entries: [], done: true };
      const entries = [];
      let last = start;
      let failure = null;
      await new Promise((resolve, reject) => {
        let settled = false;
        let idle = null;
        let stopReader = null;
        const finish = async () => {
          if (settled) return;
          settled = true;
          clearTimeout(idle);
          clearTimeout(overall);
          await stopReader?.();
          resolve();
        };
        const rearm = () => {
          clearTimeout(idle);
          idle = setTimeout(() => void finish(), RANGE_IDLE);
          if (isFunction(idle.unref)) idle.unref();
        };
        const overall = setTimeout(() => void finish(), RANGE_TIMEOUT);
        if (isFunction(overall.unref)) overall.unref();
        openStreamReader(topic, start + 1, (message) => {
          if (settled) return;
          const entry = entryOf(message);
          // RabbitMQ silently starts a stream reader at the OLDEST retained
          // message when the requested offset is gone: a first entry past
          // the one asked for is history the retention took, not a page.
          if (entries.length === 0 && start >= 0 && entry.offset > start + 1) {
            failure = codedError('Event history was trimmed past this id', 410);
            return void finish();
          }
          entries.push(entry);
          last = entry.offset;
          if (entries.length >= limit || last >= tip) return void finish();
          rearm();
        }).then((stop) => {
          stopReader = stop;
          if (settled) void stop();
          else rearm();
        }, reject);
      });
      if (failure !== null) throw failure;
      return { entries, done: last >= tip };
    },
    // Stream offsets are dense: the entry after `n` is `n + 1`, and a
    // page whose head is further along came from a reader that skipped.
    contiguous: (cursor, entry) => entry.offset === cursor + 1,
    covered: (cursor, entry) => entry.offset <= cursor,
    advance: (_cursor, entry) => entry.offset,
  });

  const parseId = (text) => (typeof text === 'string' && OFFSET.test(text) ? text : null);

  // An offset is a number inside and a string outside.
  const stringifyIds = (inner) => guardedRead(inner, { mapId: String });

  const read = (topic, options = {}) => {
    const { after = null, from = 'latest', signal = null } = options;
    if (from !== 'latest' && from !== 'earliest') {
      throw new TypeError("amqp log.read: from must be 'latest' or 'earliest'");
    }
    if (after === null || after === undefined) {
      return stringifyIds(tails.read(topic, from === 'earliest' ? { after: -1, signal } : { from, signal }));
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
    return stringifyIds(wrapped);
  };

  // AMQP 0-9-1 does not report the offset a publish landed on — only the
  // stream protocol does — and `messageCount` lags a stream badly. So the
  // append reads the tip back: one message from `last`, cancelled at once.
  // That is an extra round trip per append, which is why a high-rate
  // producer should publish through a `log` on a broker that answers with
  // the offset (Redis, NATS, Kafka) or ignore the receipt and let readers
  // yield the authoritative ids.
  const tipOffset = async (topic, timeout = 2000) => {
    const queue = await ensureLog(topic);
    const channel = await openChannel();
    try {
      await channel.prefetch(1);
      return await new Promise((resolve, reject) => {
        const timer = setTimeout(() => resolve(null), timeout);
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
    if (closed || lost) throw codedError(closed ? 'Broker is closed' : 'Broker connection lost', 503);
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
    if (closed || lost) throw codedError(closed ? 'Broker is closed' : 'Broker connection lost', 503);
    const main = await ensureQueue(queue);
    await confirmPublish('', main, body, { persistent: true, headers: toHeaders(headers) });
  };

  const consume = async (queue, onDelivery, options = {}) => {
    if (closed || lost) throw codedError(closed ? 'Broker is closed' : 'Broker connection lost', 503);
    const { prefetch = DEFAULT_PREFETCH, deadLetter = null, signal = null } = options;
    checkConsume('amqp queue.consume', onDelivery, prefetch, deadLetter);
    const main = await ensureQueue(queue, deadLetter);
    const key = `${queue}|${deadLetter ?? ''}`;
    // The consumer's channel is not for life: a channel-level error — the
    // queue deleted under it, a declaration mismatch, the node it lived on
    // going away — closes the channel, and the consumer used to sit on the
    // corpse forever, `healthy` still true, taking nothing. It re-opens
    // with a backoff instead; every delivery settles on the channel it
    // arrived on, whichever that was.
    // `paused`: the application's pause(), which a re-open must respect — a
    // channel the server closed under a paused consumer came back consuming.
    const state = { running: true, healthy: true, paused: false, tag: null, channel: null, reopening: false };
    const attach = async () => {
      let opened = null;
      opened = await openChannel(false, (error) => void onChannelClosed(opened, error));
      await opened.prefetch(prefetch);
      state.channel = opened;
      return opened;
    };

    const dispatch = (channel, message) => {
      if (message === null) {
        // The server cancelled this consumer: the queue was deleted, or the
        // node it lived on went away. The channel is still open and useless
        // — it goes, and the consumer comes back on a fresh one with the
        // queue declared again.
        if (state.channel !== channel) return;
        state.healthy = false;
        state.tag = null;
        state.channel = null;
        report('broker.amqp.cancelled', new Error('consumer cancelled'), { queue });
        ensuredQueues.delete(key);
        void closeChannel(channel);
        if (state.running && !closed && !lost) void reopen();
        return;
      }
      const headers = headersOf(message);
      const attempt = Number(headers[ATTEMPT_HEADER] ?? '1') || 1;
      let settled = false;
      // `publish`: the copy a settlement writes before it acks (a retry, a
      // dead letter). A refused publish used to be swallowed after one log
      // line with the original still unacked — which the channel's eventual
      // close handed back, or a channel that lived on never did. Tried
      // again, then handed back on purpose. The ack (`work`) runs ONCE after
      // it: retried together, an ack refused by a consumer channel that had
      // died ran the publish again — three copies of one delivery — under a
      // `channel.closed` amqplib does not have. A channel is gone when it is
      // no longer this consumer's (onChannelClosed).
      const finish = async (work, publish = null) => {
        if (settled) return;
        settled = true;
        if (publish !== null) {
          for (let round = 0; ; round++) {
            try {
              await publish();
              break;
            } catch (error) {
              if (round < SETTLE_ATTEMPTS - 1 && state.channel === channel) {
                await sleep(backoffDelay({ ...SETTLE_BACKOFF, attempt: round }));
                continue;
              }
              report('broker.amqp.settle', error, { queue, attempt, round: round + 1 });
              try {
                // Back in line for another consumer, attempt untouched (a
                // requeue does not count one on RabbitMQ 4): at least once.
                channel.nack(message, false, true);
              } catch {
                // The channel is gone; its unacked messages are already back.
              }
              return;
            }
          }
        }
        try {
          await work();
        } catch (error) {
          // A channel gone after the copy was written hands the original
          // back itself: one more delivery, never one more copy.
          report('broker.amqp.settle', error, { queue, attempt, round: 1 });
        }
      };
      const ack = () => channel.ack(message);
      const carry = (extra) => ({ ...headers, [REDELIVERED_HEADER]: '1', ...extra });
      const delivery = Object.freeze({
        id: String(message.properties?.messageId ?? message.fields?.deliveryTag ?? ''),
        body: message.content.toString(),
        headers,
        attempt,
        redelivered: message.fields?.redelivered === true || headers[REDELIVERED_HEADER] === '1',
        ack: () => finish(ack),
        // A requeue would NOT count the attempt on RabbitMQ 4, so a retry is
        // a republish — through the TTL queue when it must wait.
        retry: ({ delay = 0 } = {}) =>
          finish(ack, async () => {
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
          }),
        // Exactly what a requeue means on RabbitMQ 4: back in line, attempt
        // untouched.
        release: () => finish(() => channel.nack(message, false, true)),
        deadLetter: (reason = '') =>
          finish(ack, async () => {
            if (!deadLetter) return;
            await confirmPublish('', workQueue(deadLetter), message.content, {
              persistent: true,
              headers: {
                ...headers,
                [DEAD_REASON_HEADER]: reasonText(reason),
                [ATTEMPT_HEADER]: String(attempt),
              },
            });
          }),
      });
      runDelivery(onDelivery, delivery, report, 'broker.amqp.delivery', queue);
    };

    const start = async (channel) => {
      const { consumerTag } = await channel.consume(main, (message) => dispatch(channel, message), { noAck: false });
      state.tag = consumerTag;
      state.healthy = true;
    };
    const reopen = async () => {
      if (state.reopening) return;
      state.reopening = true;
      try {
        // `closed` and `lost` flip from outside this loop, between its awaits.
        for (let attempt = 0; ; attempt++) {
          if (!state.running || closed || lost) return;
          await sleep(backoffDelay({ ...REOPEN_BACKOFF, attempt }));
          if (!state.running || closed || lost) return;
          try {
            await ensureQueue(queue, deadLetter);
            const channel = await attach();
            if (!state.paused) await start(channel);
            else state.healthy = true;
            return;
          } catch (error) {
            report('broker.amqp.consumer.reopen', error, { queue, attempt: attempt + 1 });
          }
        }
      } finally {
        state.reopening = false;
      }
    };
    const onChannelClosed = (channel, error) => {
      if (state.channel !== channel) return;
      state.channel = null;
      state.tag = null;
      if (!state.running || closed || lost) return;
      state.healthy = false;
      report('broker.amqp.consumer.closed', error ?? new Error('channel closed'), { queue });
      // A queue that is gone is declared again on the way back.
      if (error?.code === 404) ensuredQueues.delete(key);
      void reopen();
    };
    await start(await attach());

    const cancel = async () => {
      if (state.tag === null || state.channel === null) return;
      const tag = state.tag;
      state.tag = null;
      try {
        await state.channel.cancel(tag);
      } catch {
        // The channel is gone; its unacked messages are already back.
      }
    };
    const stop = async () => {
      if (!state.running) return;
      state.running = false;
      await cancel();
      // Closing the channel returns whatever was unacked to the queue.
      if (state.channel !== null) await closeChannel(state.channel);
    };
    if (signal) signal.addEventListener('abort', () => void stop(), { once: true });
    return {
      stop,
      // basic.cancel keeps the channel, so the messages this consumer holds
      // stay ackable — which is what a draining node needs.
      pause: async () => {
        state.paused = true;
        await cancel();
      },
      resume: async () => {
        state.paused = false;
        if (!state.running || state.tag !== null) return;
        // Closed while paused: the re-open (running, or about to) starts it.
        if (state.channel === null) return void (await reopen());
        await start(state.channel);
      },
      get healthy() {
        return state.running && state.healthy && !closed && !lost;
      },
    };
  };

  // ---------------------------------------------------------------------
  // direct

  // ONE direct exchange for every address, the address as the routing key:
  // plain listeners each bind their own exclusive queue (everyone
  // receives), a group binds ONE durable queue (its members compete), and
  // `mandatory` reports "nobody bound to that key" as a basic.return — the
  // 503 an RPC caller wants. It used to be a durable fanout exchange PER
  // address, and an address is whatever a peer puts in `replyTo` — one per
  // client inbox — so a service's topology grew by one exchange per client
  // for as long as the broker lived (a durable exchange is never deleted
  // by itself). Two other shapes were weighed and refused: the default
  // exchange `''` routes by queue NAME, so several ungrouped listeners on
  // one address, each on its own exclusive queue, could not all receive
  // (the direct contract); and an `autoDelete` exchange per address goes
  // away with its last binding, after which a publish to it is a
  // channel-level 404 on the shared confirm channel, not a basic.return.
  const directExchange = `${prefix}.direct`;
  const routingKeyOf = (address) => encodeToken(address, { maxLength: 180 });
  const groupQueue = (address, group) => `${name(prefix, 'direct', address)}.${encodeToken(group, { maxLength: 60 })}`;

  const returned = new Set();
  // One memoized confirm channel for every send: two concurrent sends must
  // share it, or their publishes could interleave across two — and a
  // channel is what AMQP orders messages on.
  const directing = memoChannel(true, {
    // `mandatory` + basic.return is how a sender learns that nobody is
    // there — an unroutable message comes back before its confirm.
    setup: (channel) => {
      channel.on?.('return', (message) => {
        const id = message.properties?.messageId;
        if (id) returned.add(id);
      });
    },
    // Publishing to an exchange somebody deleted closes the channel with a
    // 404: declared again on the next send.
    onClose: (error) => {
      if (error?.code === 404) exchanges.delete(directExchange);
    },
  });

  // The exchange must exist before a mandatory publish: publishing to a
  // missing one is a channel-level 404, not a basic.return.
  const ensureDirect = () => ensureExchange(directExchange, 'direct');

  const listen = async (address, onMessage, { group = null } = {}) => {
    if (closed || lost) throw codedError(closed ? 'Broker is closed' : 'Broker connection lost', 503);
    checkListen('amqp direct.listen', address, onMessage);
    // What `stop.healthy` answers: the consumer was cancelled by the broker,
    // or its channel closed (with it an exclusive inbox queue is gone) —
    // either way nothing is delivered here any more.
    let listening = true;
    const channel = await openChannel(false, () => {
      listening = false;
    });
    const exchangeName = await ensureDirect();
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
    await channel.bindQueue(queue, exchangeName, routingKeyOf(address));
    const { consumerTag } = await channel.consume(
      queue,
      (message) => {
        if (message === null) {
          listening = false;
          return void report('broker.amqp.cancelled', new Error('consumer cancelled'), { address });
        }
        const headers = headersOf(message);
        const text = headers[TEXT_HEADER] === '1';
        delete headers[TEXT_HEADER];
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
    const stop = async () => {
      listening = false;
      // An inbox's own queue is unbound FIRST, so a publish racing this stop
      // gets a basic.return (nobody there) rather than a nack from a queue
      // being deleted under it. A group's queue is shared and stays bound.
      if (group === null || group === undefined) {
        await channel.unbindQueue(queue, exchangeName, routingKeyOf(address)).catch(() => {});
      }
      try {
        await channel.cancel(consumerTag);
      } catch {
        // Already cancelled.
      }
      await closeChannel(channel);
    };
    return withHealth(stop, () => listening && !closed && !lost);
  };

  const send = async (address, body, { headers = null, correlationId = null, replyTo = null, timeout } = {}) => {
    if (closed || lost) throw codedError(closed ? 'Broker is closed' : 'Broker connection lost', 503);
    const [channel, exchangeName] = await Promise.all([directing(), ensureDirect()]);
    const text = typeof body === 'string';
    const messageId = nextId();
    const properties = {
      headers: { ...toHeaders(headers), [TEXT_HEADER]: text ? '1' : '0' },
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
      channel.publish(exchangeName, routingKeyOf(address), Buffer.from(toBytes(body)), properties, (error) => {
        // A nack: the broker took nothing — the queue it routed to was
        // being deleted under a listener that just stopped, an overflow, a
        // node on its way out. Whichever, the message reached nobody, which
        // is the 503 the RPC binding retries or fails on.
        if (error) return void reject(codedError(`Broker refused the message for ${address}: ${error.message}`, 503));
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
    exchanges.clear();
    for (const channel of Array.from(channels)) await closeChannel(channel);
    topology.reset();
    publisher.reset();
    directing.reset();
    consuming.reset();
    handlers.clear();
    bound.clear();
    // The CONNECTION is injected: closing it is the caller's business.
  };

  connection.on?.('close', () => {
    if (closed || lost) return;
    lost = true;
    report('broker.amqp.connection', new Error('connection closed'));
    // Every memo is a corpse now, and every consumer's re-open loop reads
    // `lost` and stops: there is nothing to re-open on.
    exchanges.clear();
    ensuredLogs.clear();
    ensuredQueues.clear();
  });

  return {
    name: 'amqp',
    backplane,
    log: Object.freeze({ name: 'amqp', append, read, parseId }),
    queue: Object.freeze({ name: 'amqp', produce, consume }),
    direct: Object.freeze({ name: 'amqp', inbox: () => `${prefix}.inbox.${nextId()}`, listen, send }),
    close,
  };
};

module.exports = { createAmqpBroker };
