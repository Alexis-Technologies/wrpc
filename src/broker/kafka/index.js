'use strict';

// The Kafka broker: log and queue over an INJECTED KafkaJS-shaped client,
// plus a backplane with costs the guide spells out. There is deliberately
// NO `direct` capability — consumer-group rebalances and a topic per
// instance make Kafka a poor carrier for request/response, and refusing is
// better than a binding that limps.
//
//   const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
//   const broker = createKafkaBroker({ kafka: new Kafka({ kafkaJS: { brokers } }) });
//
// Capability map:
//   backplane  one topic, the channel in a header, a UNIQUE consumer group
//              per instance (so every instance sees everything) — caveated
//   log        a topic per log; the resume token is a VECTOR of partition
//              offsets (`k1:<p>=<o>,…`), which is why the id a read yields
//              is the advanced cursor rather than one message's offset
//   queue      a topic per queue, one consumer group, manual commits; a
//              retry is a republish carrying `x-wrpc-attempt` (Kafka has no
//              nack), and an exhausted message goes to a dead-letter topic

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
const { codedError, toText, toHeaders, encodeToken } = require('../ids.js');
const {
  detectFlavor,
  consumerConfig,
  producerConfig,
  subscribeArgs,
  runArgs,
  metadataTopics,
  joinWatcher,
} = require('./shape.js');

const DEFAULT_PREFIX = 'wrpc';
const DEFAULT_PREFETCH = 16;
const DEFAULT_PARTITIONS = 3;
const DEFAULT_MAX_RETRY_DELAY = 60_000;
const CHANNEL_HEADER = 'wrpc-channel';
const ATTEMPT_HEADER = 'x-wrpc-attempt';
const REDELIVERED_HEADER = 'x-wrpc-redelivered';
const DEAD_REASON_HEADER = 'x-wrpc-dead-reason';
const VECTOR = /^k1:(\d{1,5}=\d{1,19})(,\d{1,5}=\d{1,19})*$/;

const isFunction = (value) => typeof value === 'function';

// A Kafka topic name: letters, digits, dot, underscore and dash only.
const topicName = (prefix, kind, value) =>
  `${prefix}.${kind}.${encodeToken(value, { safe: /[A-Za-z0-9_-]/, escape: '_', maxLength: 120 })}`;

// The resume token: every partition this reader has passed, sorted.
const encodeVector = (cursor) => {
  const parts = Object.keys(cursor)
    .map(Number)
    .sort((a, b) => a - b)
    .map((partition) => `${partition}=${cursor[partition]}`);
  return `k1:${parts.join(',')}`;
};

const decodeVector = (text) => {
  if (typeof text !== 'string' || !VECTOR.test(text)) return null;
  const cursor = {};
  for (const pair of text.slice(3).split(',')) {
    const [partition, offset] = pair.split('=');
    cursor[Number(partition)] = Number(offset);
  }
  return cursor;
};

const headersOf = (message) => {
  const bag = {};
  for (const [key, value] of Object.entries(message.headers ?? {})) {
    if (value === undefined || value === null) continue;
    bag[key] = Buffer.isBuffer(value) ? value.toString() : String(value);
  }
  return toHeaders(bag);
};

const createKafkaBroker = (options = {}) => {
  const {
    kafka,
    flavor: flavorOption = null,
    prefix = DEFAULT_PREFIX,
    logger = globalThis.console,
    partitions = DEFAULT_PARTITIONS,
    // A log's order is per PARTITION, so a feed topic is single-partition
    // unless the application says otherwise — a durable feed that reordered
    // itself under load would be a subtle, permanent bug.
    logPartitions = 1,
    replicationFactor = 1,
    backplane: backplaneOptions = {},
    maxRetryDelay = DEFAULT_MAX_RETRY_DELAY,
    generateId = null,
  } = options;
  // Strict: a new option, so a bad generator is refused at construction
  // rather than producing a name the broker rejects at connect time.
  const nextId = generateId === null ? generateUUID : resolveGenerateId(generateId, 'createKafkaBroker').generate;
  // Two names the broker itself repeats in every log line and metric label
  // it emits, so the DEFAULT stays short; an injected generator is used
  // whole, per nextId above.
  const shortName = generateId === null ? () => generateUUID().slice(0, 8) : nextId;
  if (!kafka || !isFunction(kafka.producer) || !isFunction(kafka.consumer) || !isFunction(kafka.admin)) {
    throw new TypeError('createKafkaBroker: options.kafka must be a KafkaJS-shaped client (producer/consumer/admin)');
  }
  if (flavorOption !== null && flavorOption !== 'kafkajs' && flavorOption !== 'confluent') {
    throw new TypeError("createKafkaBroker: options.flavor must be 'kafkajs', 'confluent' or omitted");
  }
  if (!Number.isInteger(partitions) || partitions <= 0) {
    throw new TypeError('createKafkaBroker: options.partitions must be a positive integer');
  }
  if (!Number.isInteger(logPartitions) || logPartitions <= 0) {
    throw new TypeError('createKafkaBroker: options.logPartitions must be a positive integer');
  }
  const flavor = flavorOption ?? detectFlavor(kafka);
  const log = createLoggerWriter(logger).child({ component: 'broker', broker: 'kafka' });
  const report = (event, error, extra = {}) => log.error({ err: error, event, ...extra });
  let closed = false;
  const consumers = new Set();
  const groups = new Set(); // groups this broker created, deleted on close

  let adminClient = null;
  const admin = () => {
    if (!adminClient) {
      adminClient = (async () => {
        const client = kafka.admin();
        await client.connect();
        return client;
      })();
      adminClient.catch(() => {
        adminClient = null;
      });
    }
    return adminClient;
  };

  let producerClient = null;
  const producer = () => {
    if (!producerClient) {
      producerClient = (async () => {
        const client = kafka.producer(producerConfig(flavor, { acks: -1 }));
        await client.connect();
        return client;
      })();
      producerClient.catch(() => {
        producerClient = null;
      });
    }
    return producerClient;
  };

  const ensured = new Map();
  const ensureTopic = (topic, numPartitions = partitions) => {
    let pending = ensured.get(topic);
    if (pending) return pending;
    pending = (async () => {
      const client = await admin();
      try {
        await client.createTopics({ topics: [{ topic, numPartitions, replicationFactor }] });
        return topic;
      } catch (error) {
        // Already there (another instance, or a previous run).
        if (!/already exists|TOPIC_ALREADY_EXISTS/i.test(String(error?.message))) throw error;
      }
      // An existing topic keeps the partition count it was created with,
      // which is worth saying out loud for a FEED: more partitions than the
      // adapter would have made means its order is per partition, not global.
      try {
        const metadata = metadataTopics(await client.fetchTopicMetadata({ topics: [topic] }));
        const actual = metadata[0]?.partitions?.length;
        if (actual !== undefined && actual !== numPartitions) {
          log.debug({ event: 'broker.kafka.partitions', topic, expected: numPartitions, actual });
        }
      } catch (error) {
        log.debug({ event: 'broker.kafka.metadata', topic, err: error });
      }
      return topic;
    })();
    ensured.set(topic, pending);
    pending.catch(() => ensured.delete(topic));
    return pending;
  };

  const send = async (topic, value, { headers = null, key = null } = {}) => {
    const client = await producer();
    const message = { value: toText(value) };
    if (headers) message.headers = toHeaders(headers);
    if (key !== null && key !== undefined) message.key = String(key);
    const [result] = await client.send({ topic, messages: [message] });
    return result;
  };

  // A consumer that this broker owns: tracked so close() disconnects it and
  // removes the group it created.
  const openConsumer = async (groupId, config = {}) => {
    const consumer = kafka.consumer(consumerConfig(flavor, { groupId, ...config }));
    await consumer.connect();
    consumers.add(consumer);
    groups.add(groupId);
    return consumer;
  };

  const closeConsumer = async (consumer) => {
    consumers.delete(consumer);
    try {
      await consumer.disconnect();
    } catch (error) {
      report('broker.kafka.disconnect', error);
    }
  };

  // ---------------------------------------------------------------------
  // backplane

  const backplaneTopic = backplaneOptions.topic ?? `${prefix}.backplane`;
  // Publishes are CHAINED: a backplane's envelopes carry a per-channel
  // sequence, and two sends in flight at once on a non-idempotent producer
  // can land out of order — which the receiver would report as a gap.
  let publishChain = Promise.resolve();
  const backplanePartitions = backplaneOptions.partitions ?? 1;
  const handlers = new Map(); // channel -> Set<handler>
  let backplaneConsumer = null;

  const startBackplane = async () => {
    if (backplaneConsumer) return backplaneConsumer;
    backplaneConsumer = (async () => {
      await ensureTopic(backplaneTopic, backplanePartitions);
      // A group per INSTANCE: every instance must see every envelope, which
      // is exactly what a shared group would prevent.
      const consumer = await openConsumer(`${prefix}-bp-${shortName()}`, { fromBeginning: false });
      await consumer.subscribe(subscribeArgs(flavor, [backplaneTopic], false));
      // Registered BEFORE run(): kafkajs' join event fires once.
      const joined = joinWatcher(flavor, consumer);
      await consumer.run(
        runArgs(flavor, {
          concurrency: 1,
          eachMessage: async ({ message }) => {
            const channel = headersOf(message)[CHANNEL_HEADER];
            const set = channel === undefined ? null : handlers.get(channel);
            if (!set) return;
            const text = message.value === null ? '' : message.value.toString();
            for (const handler of Array.from(set)) {
              try {
                handler(text);
              } catch (error) {
                report('broker.kafka.handler', error, { channel });
              }
            }
          },
        }),
      );
      // Only now is this instance actually listening.
      await joined;
      return consumer;
    })();
    backplaneConsumer.catch(() => {
      backplaneConsumer = null;
    });
    return backplaneConsumer;
  };

  const backplane = {
    name: 'kafka',
    publish(channel, message) {
      if (closed) return;
      publishChain = publishChain
        .then(() => ensureTopic(backplaneTopic, backplanePartitions))
        .then(() => send(backplaneTopic, message, { headers: { [CHANNEL_HEADER]: channel } }))
        .catch((error) => report('broker.kafka.publish', error, { channel }));
    },
    subscribe(channel, handler) {
      if (!isFunction(handler)) throw new TypeError('kafka backplane.subscribe: handler must be a function');
      let set = handlers.get(channel);
      if (!set) {
        set = new Set();
        handlers.set(channel, set);
      }
      set.add(handler);
      // Resolved once the group is JOINED: a publish after this is seen.
      return startBackplane().then(() => async () => {
        const current = handlers.get(channel);
        if (!current || !current.delete(handler)) return;
        if (current.size === 0) handlers.delete(channel);
      });
    },
    close() {
      handlers.clear();
    },
  };

  // ---------------------------------------------------------------------
  // log

  const logTopic = (topic) => topicName(prefix, 'log', topic);

  const watermarks = async (topic) => {
    const client = await admin();
    const rows = await client.fetchTopicOffsets(logTopic(topic));
    const low = {};
    const high = {};
    for (const row of rows) {
      low[row.partition] = Number(row.low);
      high[row.partition] = Number(row.high);
    }
    return { low, high };
  };

  const readerGroup = () => `${prefix}-read-${shortName()}`;

  // A seek right after a join can still land before the group is
  // initialized; the readers filter by offset anyway, so a failure here is
  // a slower read, never a wrong one.
  const seekAll = async (consumer, topic, offsets) => {
    for (const [partition, offset] of Object.entries(offsets)) {
      for (let attempt = 0; attempt < 5; attempt++) {
        try {
          consumer.seek({ topic, partition: Number(partition), offset: String(offset) });
          break;
        } catch (error) {
          if (attempt === 4) log.debug({ event: 'broker.kafka.seek', err: error, topic });
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
      }
    }
  };

  const tails = new TopicTails({
    live: async (topic, { signal, onEntry }) => {
      const name = await ensureTopic(logTopic(topic), logPartitions);
      const { high } = await watermarks(topic);
      const consumer = await openConsumer(readerGroup(), { fromBeginning: false });
      await consumer.subscribe(subscribeArgs(flavor, [name], false));
      const joined = joinWatcher(flavor, consumer);
      await consumer.run(
        runArgs(flavor, {
          concurrency: 1,
          eachMessage: async ({ partition, message }) => {
            // Defensive: a reader positioned by the group rather than by the
            // seek above could see what the tip already covered.
            if (Number(message.offset) < (high[partition] ?? 0)) return;
            onEntry({
              partition,
              offset: Number(message.offset),
              value: message.value === null ? '' : message.value.toString(),
              headers: headersOf(message),
            });
          },
        }),
      );
      await joined;
      // A fresh group resolves `latest` at its FIRST FETCH, which can be
      // after the next append — so the position is pinned to the watermark
      // captured above instead of left to that race.
      await seekAll(consumer, name, high);
      signal.addEventListener('abort', () => void closeConsumer(consumer), { once: true });
      // The tip as a vector: every partition's next offset minus one.
      const cursor = {};
      let any = false;
      for (const [partition, offset] of Object.entries(high)) {
        cursor[Number(partition)] = offset - 1;
        if (offset > 0) any = true;
      }
      return any ? cursor : null;
    },
    range: async (topic, { after, limit }) => {
      const name = await ensureTopic(logTopic(topic), logPartitions);
      const { high } = await watermarks(topic);
      const wanted = {};
      let pending = 0;
      for (const [partition, tip] of Object.entries(high)) {
        const from = after === null || after === undefined ? 0 : (after[Number(partition)] ?? -1) + 1;
        if (from < tip) {
          wanted[Number(partition)] = from;
          pending += tip - from;
        }
      }
      if (pending === 0) return [];
      const entries = [];
      const consumer = await openConsumer(readerGroup(), { fromBeginning: true });
      await consumer.subscribe(subscribeArgs(flavor, [name], true));
      const joined = joinWatcher(flavor, consumer);
      await new Promise((resolve, reject) => {
        let done = false;
        const finish = () => {
          if (done) return;
          done = true;
          resolve();
        };
        const timer = setTimeout(finish, 10_000);
        if (isFunction(timer.unref)) timer.unref();
        consumer
          .run(
            runArgs(flavor, {
              concurrency: 1,
              eachMessage: async ({ partition, message }) => {
                const offset = Number(message.offset);
                const from = wanted[partition];
                if (from === undefined || offset < from) return;
                entries.push({
                  partition,
                  offset,
                  value: message.value === null ? '' : message.value.toString(),
                  headers: headersOf(message),
                });
                if (entries.length >= limit || entries.length >= pending) {
                  clearTimeout(timer);
                  finish();
                }
              },
            }),
          )
          .then(async () => {
            await joined;
            await seekAll(consumer, name, wanted);
          })
          .catch(reject);
      });
      await closeConsumer(consumer);
      entries.sort((a, b) => (a.partition === b.partition ? a.offset - b.offset : a.partition - b.partition));
      return entries.slice(0, limit);
    },
    covered: (cursor, entry) => entry.offset <= (cursor?.[entry.partition] ?? -1),
    advance: (cursor, entry) => ({ ...(cursor ?? {}), [entry.partition]: entry.offset }),
  });

  const parseId = (text) => (decodeVector(text) === null ? null : text);

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

  const encodeIds = (inner, guard) => {
    const ready = guard ? Promise.all([guard, inner.ready]).then(() => undefined) : inner.ready;
    ready.catch(() => {});
    return {
      ready,
      [Symbol.asyncIterator]: () => {
        const iterator = inner[Symbol.asyncIterator]();
        let verified = guard === null;
        return {
          next: async () => {
            if (!verified) {
              try {
                await guard;
              } catch (error) {
                await iterator.return?.();
                throw error;
              }
              verified = true;
            }
            const result = await iterator.next();
            return result.done
              ? result
              : { done: false, value: { ...result.value, id: encodeVector(result.value.id) } };
          },
          return: (value) => iterator.return?.(value) ?? Promise.resolve({ value, done: true }),
        };
      },
    };
  };

  const read = (topic, options = {}) => {
    const { after = null, from = 'latest', signal = null } = options;
    if (from !== 'latest' && from !== 'earliest') {
      throw new TypeError("kafka log.read: from must be 'latest' or 'earliest'");
    }
    if (after === null || after === undefined) {
      return encodeIds(tails.read(topic, from === 'earliest' ? { after: {}, signal } : { from, signal }), null);
    }
    const cursor = decodeVector(after);
    if (cursor === null) return failedRead(codedError('Malformed event id', 400));
    const guard = (async () => {
      const { low, high } = await watermarks(topic);
      for (const [partition, offset] of Object.entries(cursor)) {
        const tip = high[Number(partition)];
        if (tip === undefined || offset + 1 > tip) throw codedError('Event id is beyond the end of the log', 400);
        if (offset + 1 < (low[Number(partition)] ?? 0)) {
          throw codedError('Event history was trimmed past this id', 410);
        }
      }
    })();
    guard.catch(() => {});
    return encodeIds(tails.read(topic, { after: cursor, signal }), guard);
  };

  const append = async (topic, value, { headers = null, key = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    const name = await ensureTopic(logTopic(topic), logPartitions);
    const result = await send(name, value, { headers, key });
    const offset = Number(result.baseOffset ?? result.offset ?? 0);
    return encodeVector({ [result.partition]: offset });
  };

  // ---------------------------------------------------------------------
  // queue

  const queueTopic = (queue) => topicName(prefix, 'q', queue);

  const produce = async (queue, body, { headers = null, key = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    const topic = await ensureTopic(queueTopic(queue));
    await send(topic, body, { headers, key });
  };

  const consume = async (queue, onDelivery, options = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    if (!isFunction(onDelivery)) throw new TypeError('kafka queue.consume: onDelivery must be a function');
    const { group = queue, prefetch = DEFAULT_PREFETCH, deadLetter = null, signal = null } = options;
    if (!Number.isInteger(prefetch) || prefetch <= 0) {
      throw new TypeError('kafka queue.consume: prefetch must be a positive integer');
    }
    const topic = await ensureTopic(queueTopic(queue));
    if (deadLetter) await ensureTopic(queueTopic(deadLetter));
    const groupId = encodeToken(group, { safe: /[A-Za-z0-9_-]/, escape: '_', maxLength: 120 });
    const consumer = await openConsumer(groupId, { fromBeginning: true });
    await consumer.subscribe(subscribeArgs(flavor, [topic], true));
    const state = { running: true, paused: false, healthy: true };

    const commit = (partition, offset) =>
      consumer.commitOffsets([{ topic, partition, offset: String(Number(offset) + 1) }]);

    const handle = async ({ partition, message }) => {
      if (!state.running) return;
      const headers = headersOf(message);
      const attempt = Number(headers[ATTEMPT_HEADER] ?? '1') || 1;
      const body = message.value === null ? '' : message.value.toString();
      let settled = false;
      const finish = async (work) => {
        if (settled) return;
        settled = true;
        try {
          await work();
        } catch (error) {
          report('broker.kafka.settle', error, { queue });
        }
      };
      const republish = async (extra, delay = 0) => {
        // Kafka has no nack and no server-side delay: a retry is a new
        // message, and the wait happens here. The ORIGINAL stays uncommitted
        // until the copy is written, so a crash mid-wait redelivers rather
        // than loses.
        if (delay > 0) await new Promise((resolve) => setTimeout(resolve, Math.min(delay, maxRetryDelay)));
        await send(topic, body, { headers: { ...headers, ...extra, [REDELIVERED_HEADER]: '1' } });
      };
      const delivery = Object.freeze({
        id: `${partition}:${message.offset}`,
        body,
        headers,
        attempt,
        redelivered: headers[REDELIVERED_HEADER] === '1',
        ack: () => finish(() => commit(partition, message.offset)),
        retry: ({ delay = 0 } = {}) =>
          finish(async () => {
            await republish({ [ATTEMPT_HEADER]: String(attempt + 1) }, delay);
            await commit(partition, message.offset);
          }),
        release: () =>
          finish(async () => {
            await republish({ [ATTEMPT_HEADER]: String(attempt) });
            await commit(partition, message.offset);
          }),
        deadLetter: (reason = '') =>
          finish(async () => {
            if (deadLetter) {
              await send(queueTopic(deadLetter), body, {
                headers: {
                  ...headers,
                  [DEAD_REASON_HEADER]: String(reason),
                  [ATTEMPT_HEADER]: String(attempt),
                },
              });
            }
            await commit(partition, message.offset);
          }),
      });
      // eachMessage is sequential per partition: the handler is awaited so
      // Kafka's own ordering guarantee is not broken. Concurrency comes from
      // partitions (partitionsConsumedConcurrently).
      try {
        await onDelivery(delivery);
      } catch (error) {
        report('broker.kafka.delivery', error, { queue });
        await delivery.release();
      }
    };

    const ready = joinWatcher(flavor, consumer);
    await consumer.run(runArgs(flavor, { concurrency: prefetch, eachMessage: handle }));
    await ready;

    const stop = async () => {
      if (!state.running) return;
      state.running = false;
      await closeConsumer(consumer);
    };
    if (signal) signal.addEventListener('abort', () => void stop(), { once: true });
    return {
      stop,
      pause: async () => {
        state.paused = true;
        try {
          consumer.pause([{ topic }]);
        } catch (error) {
          report('broker.kafka.pause', error, { queue });
        }
      },
      resume: async () => {
        state.paused = false;
        try {
          consumer.resume([{ topic }]);
        } catch (error) {
          report('broker.kafka.resume', error, { queue });
        }
      },
      get healthy() {
        return state.running && state.healthy && !closed;
      },
    };
  };

  // ---------------------------------------------------------------------

  const close = async () => {
    if (closed) return;
    closed = true;
    tails.close();
    handlers.clear();
    ensured.clear();
    for (const consumer of Array.from(consumers)) await closeConsumer(consumer);
    if (producerClient) {
      const client = await producerClient.catch(() => null);
      producerClient = null;
      if (client) await client.disconnect().catch((error) => report('broker.kafka.disconnect', error));
    }
    if (adminClient) {
      const client = await adminClient.catch(() => null);
      adminClient = null;
      if (client) {
        // An empty group lingers until offsets.retention otherwise — every
        // reader and every backplane instance would leave one behind.
        await client.deleteGroups(Array.from(groups)).catch(() => {});
        await client.disconnect().catch((error) => report('broker.kafka.disconnect', error));
      }
    }
    groups.clear();
  };

  return {
    name: 'kafka',
    backplane,
    log: Object.freeze({ name: 'kafka', append, read, parseId }),
    queue: Object.freeze({ name: 'kafka', produce, consume }),
    close,
  };
};

module.exports = { createKafkaBroker, encodeVector, decodeVector };
