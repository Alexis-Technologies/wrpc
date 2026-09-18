'use strict';

// The NATS broker: all four capabilities over an INJECTED nats.js
// connection (`@nats-io/transport-node` and, for logs and queues,
// `@nats-io/jetstream` — devDependencies here, never runtime ones).
//
//   const { connect, headers } = require('@nats-io/transport-node');
//   const { jetstream, jetstreamManager } = require('@nats-io/jetstream');
//   const broker = createNatsBroker({
//     nc: await connect({ servers }),
//     headers,                       // a PACKAGE export, not a method on nc
//     jetstream,
//     jetstreamManager,              // omit both to get backplane + direct only
//   });
//
// Capability map:
//   backplane  core subjects — publish/subscribe, at-most-once
//   log        a JetStream stream per topic; the message sequence IS the
//              feed's resume token
//   queue      a work-queue stream with a durable pull consumer per group
//   direct     core subjects: plain subscriptions for inboxes, queue groups
//              for a service address — request/reply is what NATS is FOR
//
// Every application name (a room, a topic, a queue, an address) becomes ONE
// subject token: `.` would silently mean another subject and `*`/`>` are
// wildcards — a room called `room:*` must never subscribe to every room.

const { createLoggerWriter } = require('../../logging.js');
const { generateUUID } = require('../../runtime/node.js');
const { resolveGenerateId } = require('../../utils.js');

// An injected `generateId` is used VERBATIM for every id this adapter mints
// — never truncated. Trimming a user's id would quietly weaken the
// uniqueness they chose it for, and all wrpc knows about their generator is
// that it answers a string. The cost is that a generator answering
// characters a broker refuses in a consumer name, subject or queue name
// fails at the driver, not here.
const { TopicTails } = require('../tail.js');
const { codedError, toText, toBytes, toHeaders, encodeToken } = require('../ids.js');

const DEFAULT_PREFIX = 'wrpc';
const DEFAULT_PREFETCH = 16;
const DEFAULT_ACK_WAIT = 30_000;
const DEFAULT_FETCH_EXPIRES = 2_000;
const SEQUENCE = /^\d{1,19}$/;

const ATTEMPT_HEADER = 'x-wrpc-attempt';
const REDELIVERED_HEADER = 'x-wrpc-redelivered';
const DEAD_REASON_HEADER = 'x-wrpc-dead-reason';
const CORRELATION_HEADER = 'wrpc-correlation';
const TEXT_HEADER = 'wrpc-text';

const MILLIS = 1_000_000; // JetStream durations are nanoseconds

const isFunction = (value) => typeof value === 'function';

// One subject token, always: see the header comment.
const token = (name) => encodeToken(name, { safe: /[A-Za-z0-9_-]/, escape: '~', maxLength: 120 });
// A stream NAME may not contain `.`, spaces or wildcards either.
const streamName = (prefix, kind, name) => `${prefix}_${kind}_${token(name)}`.replace(/~/g, '_');

const createNatsBroker = (options = {}) => {
  const {
    nc,
    headers: headersFactory,
    jetstream = null,
    jetstreamManager = null,
    createInbox = null,
    prefix = DEFAULT_PREFIX,
    logger = globalThis.console,
    ackWait = DEFAULT_ACK_WAIT,
    stream: streamConfig = {},
    generateId = null,
  } = options;
  // Strict: a new option, so a bad generator is refused at construction
  // rather than producing a name the broker rejects at connect time.
  const nextId = generateId === null ? generateUUID : resolveGenerateId(generateId, 'createNatsBroker').generate;
  if (!nc || !isFunction(nc.publish) || !isFunction(nc.subscribe)) {
    throw new TypeError('createNatsBroker: options.nc must be a NATS connection (publish/subscribe/...)');
  }
  if (!isFunction(headersFactory)) {
    throw new TypeError('createNatsBroker: options.headers must be the nats headers() factory (a package export)');
  }
  if ((jetstream === null) !== (jetstreamManager === null)) {
    throw new TypeError('createNatsBroker: options.jetstream and options.jetstreamManager come together');
  }
  if (jetstream !== null && (!isFunction(jetstream) || !isFunction(jetstreamManager))) {
    throw new TypeError('createNatsBroker: options.jetstream/jetstreamManager must be the nats JetStream factories');
  }
  const log = createLoggerWriter(logger).child({ component: 'broker', broker: 'nats' });
  const report = (event, error, extra = {}) => log.error({ err: error, event, ...extra });
  let closed = false;

  const encodeHeaders = (bag, extra = null) => {
    const merged = { ...toHeaders(bag), ...(extra ?? {}) };
    const names = Object.keys(merged);
    if (names.length === 0) return undefined;
    const carrier = headersFactory();
    for (const name of names) carrier.set(name, merged[name]);
    return carrier;
  };

  const decodeHeaders = (carrier) => {
    const bag = Object.create(null);
    if (!carrier || !isFunction(carrier.keys)) return bag;
    for (const name of carrier.keys()) bag[name] = carrier.get(name);
    return bag;
  };

  // ---------------------------------------------------------------------
  // backplane

  const channelSubject = (channel) => `${prefix}.bp.${token(channel)}`;

  const backplane = {
    name: 'nats',
    publish(channel, message) {
      if (closed) return;
      try {
        nc.publish(channelSubject(channel), message);
      } catch (error) {
        report('broker.nats.publish', error, { channel });
      }
    },
    subscribe(channel, handler) {
      if (!isFunction(handler)) throw new TypeError('nats backplane.subscribe: handler must be a function');
      const subscription = nc.subscribe(channelSubject(channel), {
        callback: (error, message) => {
          if (error) return void report('broker.nats.subscription', error, { channel });
          try {
            handler(toText(message.data));
          } catch (failure) {
            report('broker.nats.handler', failure, { channel });
          }
        },
      });
      // Resolved once the SUBSCRIBE reached the server: a publish after
      // this is delivered, which is what the backplane's start() needs.
      return nc.flush().then(() => async () => {
        subscription.unsubscribe();
        await nc.flush().catch(() => {});
      });
    },
    close() {
      // The connection is the caller's; subscriptions die with it.
    },
  };

  // ---------------------------------------------------------------------
  // JetStream plumbing (log + queue)

  const requireJetStream = (what) => {
    if (jetstream === null) {
      throw codedError(`nats ${what}: this broker was built without JetStream (pass jetstream/jetstreamManager)`, 501);
    }
  };

  let js = null;
  let jsm = null;
  const managers = async () => {
    if (!js) js = jetstream(nc);
    if (!jsm) jsm = await jetstreamManager(nc);
    return { js, jsm };
  };

  const ensured = new Map(); // stream name -> Promise
  const ensureStream = (name, subject, config) => {
    let pending = ensured.get(name);
    if (pending) return pending;
    pending = (async () => {
      const { jsm: manager } = await managers();
      try {
        await manager.streams.info(name);
      } catch {
        try {
          await manager.streams.add({ name, subjects: [subject], ...config });
        } catch (error) {
          // Another instance created it between the info and the add.
          if (!/already in use|exists/i.test(String(error?.message))) throw error;
        }
      }
      return name;
    })();
    ensured.set(name, pending);
    pending.catch(() => ensured.delete(name));
    return pending;
  };

  const logStream = (topic) => ({
    name: streamName(prefix, 'log', topic),
    subject: `${prefix}.log.${token(topic)}`,
    config: { ...streamConfig.log },
  });

  const queueStream = (name) => ({
    name: streamName(prefix, 'q', name),
    subject: `${prefix}.q.${token(name)}`,
    // A work queue: an acked message leaves the stream, so a queue that
    // keeps up stays small.
    config: { retention: 'workqueue', ...streamConfig.queue },
  });

  // ---------------------------------------------------------------------
  // log

  const streamState = async (topic) => {
    const { name, subject, config } = logStream(topic);
    await ensureStream(name, subject, config);
    const { jsm: manager } = await managers();
    const info = await manager.streams.info(name);
    return { name, subject, first: Number(info.state.first_seq), last: Number(info.state.last_seq) };
  };

  const entryOf = (message) => ({
    seq: Number(message.seq),
    id: String(message.seq),
    value: toText(message.data),
    headers: decodeHeaders(message.headers),
  });

  const tails = new TopicTails({
    live: async (topic, { signal, onEntry }) => {
      const { js: stream } = await managers();
      const { name } = await streamState(topic);
      const state = await streamState(topic);
      const consumer = await stream.consumers.get(name, {
        opt_start_seq: state.last + 1,
        deliver_policy: 'by_start_sequence',
      });
      const messages = await consumer.consume({ max_messages: 256 });
      signal.addEventListener('abort', () => void messages.close().catch(() => {}), { once: true });
      void (async () => {
        try {
          for await (const message of messages) {
            message.ack();
            if (signal.aborted) break;
            onEntry(entryOf(message));
          }
        } catch (error) {
          if (!signal.aborted && !closed) report('broker.nats.tail', error, { topic });
        }
      })();
      return state.last > 0 ? state.last : null;
    },
    range: async (topic, { after, limit }) => {
      const { js: stream } = await managers();
      const { name } = await streamState(topic);
      const consumer = await stream.consumers.get(name, {
        opt_start_seq: (after ?? 0) + 1,
        deliver_policy: 'by_start_sequence',
      });
      const batch = await consumer.fetch({ max_messages: limit, expires: DEFAULT_FETCH_EXPIRES });
      const entries = [];
      for await (const message of batch) {
        message.ack();
        entries.push(entryOf(message));
        if (entries.length >= limit) break;
      }
      return entries;
    },
    covered: (cursor, entry) => entry.seq <= cursor,
    advance: (_cursor, entry) => entry.seq,
  });

  const parseId = (text) => (typeof text === 'string' && SEQUENCE.test(text) ? text : null);

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

  const wrapRead = (inner, checked) => {
    const ready = Promise.all([checked, inner.ready]).then(() => undefined);
    ready.catch(() => {});
    return {
      ready,
      [Symbol.asyncIterator]: () => {
        const iterator = inner[Symbol.asyncIterator]();
        let verified = false;
        return {
          next: async () => {
            if (!verified) {
              try {
                await checked;
              } catch (error) {
                await iterator.return?.();
                throw error;
              }
              verified = true;
            }
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
      throw new TypeError("nats log.read: from must be 'latest' or 'earliest'");
    }
    requireJetStream('log.read');
    if (after === null || after === undefined) {
      if (from === 'latest') return wrapRead(tails.read(topic, { from, signal }), Promise.resolve());
      // 'earliest': every retained message, then the live tail.
      const earliest = (async () => {
        const state = await streamState(topic);
        return Math.max(0, state.first - 1);
      })();
      earliest.catch(() => {});
      return wrapRead(
        tails.read(topic, { after: 0, signal }),
        earliest.then(() => undefined),
      );
    }
    if (parseId(after) === null) return failedRead(codedError('Malformed event id', 400));
    const position = Number(after);
    const checked = (async () => {
      const state = await streamState(topic);
      if (position > state.last) throw codedError('Event id is beyond the end of the log', 400);
      if (state.first > 0 && position + 1 < state.first) {
        throw codedError('Event history was trimmed past this id', 410);
      }
    })();
    checked.catch(() => {});
    return wrapRead(tails.read(topic, { after: position, signal }), checked);
  };

  const append = async (topic, value, { headers = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    requireJetStream('log.append');
    const { js: stream } = await managers();
    const { name, subject, config } = logStream(topic);
    await ensureStream(name, subject, config);
    const ack = await stream.publish(subject, toText(value), { headers: encodeHeaders(headers) });
    return String(ack.seq);
  };

  // ---------------------------------------------------------------------
  // queue

  const produce = async (name, body, { headers = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    requireJetStream('queue.produce');
    const { js: stream } = await managers();
    const { name: streamId, subject, config } = queueStream(name);
    await ensureStream(streamId, subject, config);
    await stream.publish(subject, toText(body), { headers: encodeHeaders(headers) });
  };

  const consume = async (name, onDelivery, options = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    requireJetStream('queue.consume');
    if (!isFunction(onDelivery)) throw new TypeError('nats queue.consume: onDelivery must be a function');
    const { group = name, prefetch = DEFAULT_PREFETCH, deadLetter = null, signal = null } = options;
    if (!Number.isInteger(prefetch) || prefetch <= 0) {
      throw new TypeError('nats queue.consume: prefetch must be a positive integer');
    }
    const { js: stream, jsm: manager } = await managers();
    const { name: streamId, subject, config } = queueStream(name);
    await ensureStream(streamId, subject, config);
    const durable = token(group);
    try {
      await manager.consumers.add(streamId, {
        durable_name: durable,
        ack_policy: 'explicit',
        ack_wait: ackWait * MILLIS,
        // JetStream itself caps what this consumer holds unacked, which is
        // exactly the prefetch the contract promises.
        max_ack_pending: prefetch,
      });
    } catch (error) {
      if (!/already exists|in use/i.test(String(error?.message))) throw error;
    }
    const consumer = await stream.consumers.get(streamId, durable);
    // seq -> the `working()` keepalive of a delivery still in flight. A
    // consumer that stops must let its leases expire, or the messages it
    // held would never be redelivered to anyone.
    const state = { running: true, paused: false, healthy: true, messages: null, keepalives: new Map() };

    const dispatch = (message) => {
      const headers = decodeHeaders(message.headers);
      const base = Number(headers[ATTEMPT_HEADER] ?? '0');
      const deliveries = Number(message.info?.deliveryCount ?? 1);
      // A message this adapter re-published carries its attempt; further
      // naks of the SAME message add JetStream's own redeliveries.
      const attempt = base > 0 ? base + deliveries - 1 : deliveries;
      let settled = false;
      // A handler slower than ack_wait would see its message redelivered
      // underneath it; `working()` is JetStream's "still on it".
      const keepalive = setInterval(
        () => {
          try {
            message.working();
          } catch {
            // The message is settled or the consumer is gone.
          }
        },
        Math.max(1000, ackWait / 2),
      );
      if (isFunction(keepalive.unref)) keepalive.unref();
      state.keepalives.set(message.seq, keepalive);
      const finish = async (work) => {
        if (settled) return;
        settled = true;
        clearInterval(keepalive);
        state.keepalives.delete(message.seq);
        try {
          await work();
        } catch (error) {
          report('broker.nats.settle', error, { queue: name });
        }
      };
      const republish = async (extra) => {
        const carried = { ...headers, ...extra };
        await stream.publish(subject, message.data, { headers: encodeHeaders(carried) });
      };
      const delivery = Object.freeze({
        id: String(message.seq),
        body: toText(message.data),
        headers,
        attempt,
        // JetStream's own redelivery (a nak, an expired ack_wait) OR a copy
        // this adapter re-published for a release.
        redelivered: deliveries > 1 || headers[REDELIVERED_HEADER] === '1',
        ack: () => finish(() => message.ack()),
        // nak() carries the delay AND counts the attempt — no republish.
        retry: ({ delay = 0 } = {}) => finish(() => message.nak(delay > 0 ? delay : undefined)),
        // A release must NOT count an attempt, and JetStream has no such
        // signal: the message is re-published with its attempt carried over
        // and the original terminated.
        release: () =>
          finish(async () => {
            await republish({ [ATTEMPT_HEADER]: String(attempt), [REDELIVERED_HEADER]: '1' });
            message.term();
          }),
        deadLetter: (reason = '') =>
          finish(async () => {
            if (deadLetter) {
              const dlq = queueStream(deadLetter);
              await ensureStream(dlq.name, dlq.subject, dlq.config);
              await stream.publish(dlq.subject, message.data, {
                headers: encodeHeaders({
                  ...headers,
                  [DEAD_REASON_HEADER]: String(reason),
                  [ATTEMPT_HEADER]: String(attempt),
                }),
              });
            }
            message.term();
          }),
      });
      Promise.resolve()
        .then(() => onDelivery(delivery))
        .catch((error) => {
          report('broker.nats.delivery', error, { queue: name });
          void delivery.release();
        });
    };

    // The iterator is sequential, so the handler is never awaited here:
    // concurrency is the adapter's job, and max_ack_pending is the cap.
    const pump = async () => {
      for (;;) {
        if (!state.running || state.paused || closed) return;
        let messages;
        try {
          messages = await consumer.consume({ max_messages: prefetch });
          state.messages = messages;
          state.healthy = true;
          for await (const message of messages) {
            if (!state.running || state.paused) break;
            dispatch(message);
          }
        } catch (error) {
          if (!state.running || closed) return;
          state.healthy = false;
          report('broker.nats.consume', error, { queue: name });
          await new Promise((resolve) => setTimeout(resolve, 200));
        }
      }
    };
    void pump();

    const halt = async () => {
      const messages = state.messages;
      state.messages = null;
      for (const keepalive of state.keepalives.values()) clearInterval(keepalive);
      state.keepalives.clear();
      if (messages) await messages.close().catch(() => {});
    };
    const stop = async () => {
      if (!state.running) return;
      state.running = false;
      await halt();
    };
    if (signal) signal.addEventListener('abort', () => void stop(), { once: true });
    return {
      stop,
      pause: async () => {
        state.paused = true;
        await halt();
      },
      resume: async () => {
        if (!state.paused) return;
        state.paused = false;
        void pump();
      },
      get healthy() {
        return state.running && state.healthy && !closed && !nc.isClosed?.();
      },
    };
  };

  // ---------------------------------------------------------------------
  // direct

  const addressSubject = (address) => `${prefix}.direct.${token(address)}`;

  const inbox = () => {
    const name = isFunction(createInbox) ? createInbox() : `_INBOX.${nextId()}`;
    return name;
  };

  const listen = async (address, onMessage, { group = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    if (!isFunction(onMessage)) throw new TypeError('nats direct.listen: onMessage must be a function');
    if (typeof address !== 'string' || address.length === 0) {
      throw new TypeError('nats direct.listen: address must be a non-empty string');
    }
    const subscription = nc.subscribe(addressSubject(address), {
      queue: group === null || group === undefined ? undefined : token(group),
      callback: (error, message) => {
        if (error) return void report('broker.nats.subscription', error, { address });
        const headers = decodeHeaders(message.headers);
        const text = headers[TEXT_HEADER] === '1';
        delete headers[TEXT_HEADER];
        const correlationId = headers[CORRELATION_HEADER] ?? null;
        delete headers[CORRELATION_HEADER];
        try {
          const result = onMessage({
            body: text ? toText(message.data) : new Uint8Array(message.data),
            headers,
            correlationId,
            replyTo: message.reply ?? null,
          });
          if (result && isFunction(result.catch)) result.catch((failure) => report('broker.nats.listener', failure));
        } catch (failure) {
          report('broker.nats.listener', failure, { address });
        }
      },
    });
    await nc.flush();
    return async () => {
      subscription.unsubscribe();
      await nc.flush().catch(() => {});
    };
  };

  const send = async (address, body, { headers = null, correlationId = null, replyTo = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    const text = typeof body === 'string';
    const extra = { [TEXT_HEADER]: text ? '1' : '0' };
    if (correlationId !== null && correlationId !== undefined) extra[CORRELATION_HEADER] = String(correlationId);
    const payload = text ? toText(body) : toBytes(body);
    nc.publish(addressSubject(address), payload, {
      headers: encodeHeaders(headers, extra),
      // NATS carries the reply address natively — that is what it is for.
      reply: replyTo === null || replyTo === undefined ? undefined : replyTo,
    });
  };

  // ---------------------------------------------------------------------

  const close = async () => {
    if (closed) return;
    closed = true;
    tails.close();
    ensured.clear();
    // The connection is INJECTED: draining or closing it is the caller's.
  };

  const broker = {
    name: 'nats',
    backplane,
    direct: Object.freeze({ name: 'nats', inbox, listen, send }),
    close,
  };
  if (jetstream !== null) {
    broker.log = Object.freeze({ name: 'nats', append, read, parseId });
    broker.queue = Object.freeze({ name: 'nats', produce, consume });
  }
  return broker;
};

module.exports = { createNatsBroker };
