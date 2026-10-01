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
const { backoffDelay } = require('../../utils.js');
const { TopicTails } = require('../tail.js');
const { codedError, toText, toHeaders, reasonText, encodeToken } = require('../ids.js');
const { crashDelay } = require('../retry.js');
const {
  ATTEMPT_HEADER,
  REDELIVERED_HEADER,
  DEAD_REASON_HEADER,
  DEFAULT_PREFETCH,
  idFactory,
  failedRead,
  guardedRead,
  checkConsume,
} = require('../adapter.js');
const {
  detectFlavor,
  consumerConfig,
  producerConfig,
  subscribeArgs,
  runArgs,
  metadataTopics,
  joinWatcher,
  healthWatcher,
} = require('./shape.js');

const DEFAULT_PREFIX = 'wrpc';
const DEFAULT_PARTITIONS = 3;
const DEFAULT_MAX_RETRY_DELAY = 60_000;
const DEFAULT_MAX_CATCH_UP = 4;
// A settlement the broker refused (a leader election, a producer that
// cannot reach it) is tried again on this schedule before the consumer
// gives the message back; and a retry's in-process delay is cut into
// steps of this length, each ending in a heartbeat, so a long wait does
// not look like a dead member to the group coordinator.
const SETTLE_ATTEMPTS = 3;
const SETTLE_BACKOFF = Object.freeze({ minDelay: 100, maxDelay: 1000, factor: 2, jitter: false });
const HEARTBEAT_STEP = 3000;
const CHANNEL_HEADER = 'wrpc-channel';
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
    // -1 is the broker's own `default.replication.factor` (KIP-464, Kafka
    // 2.4+): a topic the adapter creates is as replicated as the cluster
    // says, not a single-replica one because a library said 1.
    replicationFactor = -1,
    backplane: backplaneOptions = {},
    maxRetryDelay = DEFAULT_MAX_RETRY_DELAY,
    // Catch-up pages read at once. Each is a consumer group of its own (the
    // KafkaJS shape has no manual assignment): a connection, a JoinGroup, a
    // rebalance of nobody — seconds of a coordinator's time. A resume storm
    // after a deploy used to open hundreds of them together.
    maxCatchUp = DEFAULT_MAX_CATCH_UP,
    generateId = null,
  } = options;
  // Ids: an injected generator is used verbatim — see idFactory.
  const { shortName } = idFactory(generateId, 'createKafkaBroker');
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
  // -1 is the broker's own `default.replication.factor` (KIP-464); a string
  // from the environment would be sent to createTopics as it is.
  if (!Number.isInteger(replicationFactor) || (replicationFactor !== -1 && replicationFactor < 1)) {
    throw new TypeError(
      'createKafkaBroker: options.replicationFactor must be a positive integer, or -1 for the broker default',
    );
  }
  if (!Number.isInteger(maxCatchUp) || maxCatchUp <= 0) {
    throw new TypeError('createKafkaBroker: options.maxCatchUp must be a positive integer');
  }
  if (!Number.isInteger(maxRetryDelay) || maxRetryDelay < 0) {
    throw new TypeError('createKafkaBroker: options.maxRetryDelay must be a non-negative integer of milliseconds');
  }
  const flavor = flavorOption ?? detectFlavor(kafka);
  const log = createLoggerWriter(logger).child({ component: 'broker', broker: 'kafka' });
  const report = (event, error, extra = {}) => log.error({ err: error, event, ...extra });
  let closed = false;
  const consumers = new Set();
  // The groups this broker made for ITSELF — a backplane instance's, a
  // reader's — by the consumer that holds them: deleted the moment that
  // consumer is closed (an empty group otherwise lingers until
  // offsets.retention), and swept in one call on close(). A queue's durable
  // group is never among them: its committed offsets ARE the queue's
  // progress, and deleting it on the last instance's clean restart used to
  // redeliver the whole retention.
  const groups = new Map(); // consumer -> groupId, ephemeral groups only

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
      let created = false;
      try {
        // kafkajs answers false for a topic that already exists, the
        // confluent facade throws: both are "already there".
        created = (await client.createTopics({ topics: [{ topic, numPartitions, replicationFactor }] })) !== false;
      } catch (error) {
        // Already there (another instance, or a previous run).
        if (!/already exists|TOPIC_ALREADY_EXISTS/i.test(String(error?.message))) throw error;
      }
      if (created) {
        // Once per topic, at creation: what the cluster now holds is the
        // adapter's doing, and an operator sizing replication wants to know.
        log.info({ event: 'broker.kafka.topic', topic, partitions: numPartitions, replicationFactor });
        return topic;
      }
      // An existing topic keeps the partition count — and the replication
      // factor — it was created with, which is worth saying out loud for a
      // FEED: more partitions than the adapter would have made means its
      // order is per partition, not global.
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

  // A consumer that this broker owns: tracked so close() disconnects it.
  // `ephemeral` says the group is this broker's own (deleted with the
  // consumer); a queue's durable group is opened without it.
  const openConsumer = async (groupId, config = {}, { ephemeral = false } = {}) => {
    const consumer = kafka.consumer(consumerConfig(flavor, { groupId, ...config }));
    await consumer.connect();
    consumers.add(consumer);
    if (ephemeral) groups.set(consumer, groupId);
    return consumer;
  };

  // A group the broker cannot delete yet (a member still joined — the
  // broker's NON_EMPTY_GROUP — or an admin that cannot be asked) lingers
  // until offsets.retention, as every group used to: a debug line, no more.
  const dropGroups = async (ids) => {
    if (ids.length === 0) return;
    try {
      const client = await admin();
      await client.deleteGroups(ids);
    } catch (error) {
      log.debug({ err: error, event: 'broker.kafka.groups', groups: ids });
    }
  };

  const closeConsumer = async (consumer) => {
    consumers.delete(consumer);
    try {
      await consumer.disconnect();
    } catch (error) {
      report('broker.kafka.disconnect', error);
    }
    // Its group goes with it, right away — a reader's is done the moment
    // its page or tail is, and used to wait for close(). During close()
    // itself the sweep below deletes what is left in ONE call.
    const groupId = groups.get(consumer);
    if (groupId !== undefined && !closed) {
      groups.delete(consumer);
      void dropGroups([groupId]);
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
      const consumer = await openConsumer(`${prefix}-bp-${shortName()}`, { fromBeginning: false }, { ephemeral: true });
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
    // Readers that fell behind the tail and are catching up through range():
    // info, not a fault — but the first thing to look at when a feed is slow.
    onLag: (topic, readers) => log.info({ event: 'broker.tail.lag', topic, readers }),
    live: async (topic, { signal, onEntry, onEnd }) => {
      const name = await ensureTopic(logTopic(topic), logPartitions);
      const { high } = await watermarks(topic);
      const consumer = await openConsumer(readerGroup(), { fromBeginning: false }, { ephemeral: true });
      // A crash of the tail's consumer is the end of the tail, whether or
      // not kafkajs restarts it: its group is ephemeral and commits
      // nothing, so a restarted consumer resolves `latest` afresh and
      // whatever was appended meanwhile is gone from it. The readers move
      // to a fresh tail and catch up from their own cursors instead.
      let unwatch = () => {};
      unwatch = healthWatcher(flavor, consumer, {
        onDown: (error) => {
          unwatch();
          report('broker.kafka.tail', error ?? new Error('consumer crashed'), { topic });
          void closeConsumer(consumer);
          onEnd(error ?? new Error('consumer crashed'));
        },
        onUp: () => {},
      });
      // Everything after the open under one catch: a subscribe or run that
      // fails used to leave the consumer connected and its group behind —
      // one per failed read, for the life of the broker.
      try {
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
      } catch (error) {
        unwatch();
        await closeConsumer(consumer);
        throw error;
      }
      signal.addEventListener(
        'abort',
        () => {
          unwatch();
          void closeConsumer(consumer);
        },
        { once: true },
      );
      // The tip as a vector: every partition's next offset minus one.
      const cursor = {};
      let any = false;
      for (const [partition, offset] of Object.entries(high)) {
        cursor[Number(partition)] = offset - 1;
        if (offset > 0) any = true;
      }
      return any ? cursor : null;
    },
    // Two bounds on what a resume storm asks of the cluster. Identical pages
    // — the same topic, cursor and limit, which is what a room of clients
    // that all lost the same instance ask for — are ONE read, shared (the
    // tail only reads a page, so nobody needs a copy). And no more than
    // `maxCatchUp` pages are read at once; the rest wait their turn, first
    // come first served.
    range: (topic, { after, limit }) => {
      const key = `${topic}\0${after === null || after === undefined ? '' : encodeVector(after)}\0${limit}`;
      let page = pages.get(key);
      if (page === undefined) {
        page = catchUp(() => readPage(topic, after, limit)).finally(() => pages.delete(key));
        pages.set(key, page);
      }
      return page;
    },
    covered: (cursor, entry) => entry.offset <= (cursor?.[entry.partition] ?? -1),
    advance: (cursor, entry) => ({ ...(cursor ?? {}), [entry.partition]: entry.offset }),
  });

  // Pages in flight, by what they read; and the turnstile in front of them.
  const pages = new Map();
  let catching = 0;
  const waiting = [];
  const catchUp = async (read) => {
    if (catching < maxCatchUp) catching++;
    else await new Promise((resolve) => waiting.push(resolve));
    try {
      // Woken by close(): nothing is opened on a broker that is gone.
      if (closed) throw codedError('Broker is closed', 503);
      return await read();
    } finally {
      // The slot goes straight to the next in line, whatever this page did
      // — a page that failed must not strand the ones behind it.
      const next = waiting.shift();
      if (next === undefined) catching--;
      else next();
    }
  };

  const readPage = async (topic, after, limit) => {
    const name = await ensureTopic(logTopic(topic), logPartitions);
    const { low, high } = await watermarks(topic);
    // Per partition: where the page starts (never below the low
    // watermark — what retention took cannot be waited for) and the
    // offset it must reach to be complete.
    const wanted = {};
    const target = {};
    let pending = 0;
    for (const [key, tip] of Object.entries(high)) {
      const partition = Number(key);
      const asked = after === null || after === undefined ? 0 : (after[partition] ?? -1) + 1;
      const from = Math.max(asked, low[partition] ?? 0);
      if (from < tip) {
        wanted[partition] = from;
        target[partition] = tip - 1;
        pending += tip - from;
      }
    }
    if (pending === 0) return { entries: [], done: true };
    const entries = [];
    const consumer = await openConsumer(readerGroup(), { fromBeginning: true }, { ephemeral: true });
    let timer = null;
    // Closed whatever happens — a subscribe that fails, a run that
    // rejects, the page that completes: a page's consumer and group are
    // done with the page, and used to outlive a failure.
    try {
      await consumer.subscribe(subscribeArgs(flavor, [name], true));
      const joined = joinWatcher(flavor, consumer);
      await new Promise((resolve, reject) => {
        let done = false;
        const finish = () => {
          if (done) return;
          done = true;
          resolve();
        };
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
                if (entries.length >= limit || entries.length >= pending) finish();
              },
            }),
          )
          .then(async () => {
            // The page's window opens once the group is joined and the
            // seek landed: a window that opened before the join — a slow
            // rebalance eats seconds — closed on a page that had not
            // started, and the short page was taken for the tip.
            await joined;
            await seekAll(consumer, name, wanted);
            timer = setTimeout(finish, 10_000);
            if (isFunction(timer.unref)) timer.unref();
          })
          .catch(reject);
      });
    } finally {
      clearTimeout(timer);
      await closeConsumer(consumer);
    }
    entries.sort((a, b) => (a.partition === b.partition ? a.offset - b.offset : a.partition - b.partition));
    const page = entries.slice(0, limit);
    // Complete when every wanted partition reached its tip WITHIN the
    // page — what was fetched beyond `limit` is not handed over.
    const reached = {};
    for (const entry of page) reached[entry.partition] = entry.offset;
    const done = Object.keys(target).every((partition) => (reached[partition] ?? -1) >= target[partition]);
    return { entries: page, done };
  };

  const parseId = (text) => (decodeVector(text) === null ? null : text);

  // A cursor is a vector of partition offsets inside and its text outside.
  const encodeIds = (inner, guard) => guardedRead(inner, { guard, mapId: encodeVector });

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
    const { group = queue, prefetch = DEFAULT_PREFETCH, deadLetter = null, signal = null } = options;
    checkConsume('kafka queue.consume', onDelivery, prefetch, deadLetter);
    const topic = await ensureTopic(queueTopic(queue));
    if (deadLetter) await ensureTopic(queueTopic(deadLetter));
    const groupId = encodeToken(group, { safe: /[A-Za-z0-9_-]/, escape: '_', maxLength: 120 });
    const consumer = await openConsumer(groupId, { fromBeginning: true });
    await consumer.subscribe(subscribeArgs(flavor, [topic], true));
    const state = { running: true, paused: false, healthy: true };
    // `healthy` used to be true from the first run() to stop(), whatever the
    // consumer went through: a crashed kafkajs consumer (a broker gone, a
    // rebalance that failed) reported a binding that consumed nothing as
    // fine, and readiness kept the instance in rotation. The watcher is
    // registered BEFORE run(), like the join watcher — kafkajs' events are
    // missed by a listener that comes after.
    const unwatch = healthWatcher(flavor, consumer, {
      onDown: (error, restart) => {
        state.healthy = false;
        report('broker.kafka.crash', error ?? new Error('consumer crashed'), { queue, restart: restart === true });
      },
      onUp: () => {
        if (state.running) state.healthy = true;
      },
    });

    const commit = (partition, offset) =>
      consumer.commitOffsets([{ topic, partition, offset: String(Number(offset) + 1) }]);

    const handle = async ({ partition, message, heartbeat }) => {
      if (!state.running) return;
      const headers = headersOf(message);
      const attempt = Number(headers[ATTEMPT_HEADER] ?? '1') || 1;
      const body = message.value === null ? '' : message.value.toString();
      const id = `${partition}:${message.offset}`;
      let settled = false;
      // A settlement the broker refused used to be swallowed after one log
      // line, and the loop went on: the next message's commit moved the
      // group's offset past this one, and it was gone. Now the settlement
      // is tried again (SETTLE_ATTEMPTS), and when it still fails the
      // consumer seeks BACK to this offset — the message is fetched and
      // handed over again, attempt unchanged, and nothing commits past it.
      // A heartbeat that failed mid-wait means the group is rebalancing:
      // this member no longer owns the partition, the message will be
      // fetched by whoever does, and neither a republish nor a commit is
      // this member's to make.
      const finish = async (action, work) => {
        if (settled) return;
        settled = true;
        for (let round = 0; ; round++) {
          try {
            await work();
            state.healthy = true;
            return;
          } catch (error) {
            if (error?.rebalanced === true) {
              log.info({ event: 'broker.kafka.rebalanced', queue, id, action });
              return;
            }
            if (round < SETTLE_ATTEMPTS - 1 && state.running) {
              await new Promise((resolve) => setTimeout(resolve, backoffDelay({ ...SETTLE_BACKOFF, attempt: round })));
              continue;
            }
            state.healthy = false;
            report('broker.kafka.settle', error, { queue, id, action, attempt, partition });
            try {
              consumer.seek({ topic, partition, offset: String(message.offset) });
            } catch (seekError) {
              report('broker.kafka.seek', seekError, { queue, id });
            }
            return;
          }
        }
      };
      const beat = async () => {
        if (!isFunction(heartbeat)) return;
        try {
          await heartbeat();
        } catch (error) {
          throw Object.assign(error, { rebalanced: true });
        }
      };
      const republish = async (extra, delay = 0) => {
        // Kafka has no nack and no server-side delay: a retry is a new
        // message, and the wait happens here. The ORIGINAL stays uncommitted
        // until the copy is written, so a crash mid-wait redelivers rather
        // than loses. The wait is cut into heartbeat-sized steps: a member
        // silent for the whole delay looks dead to the coordinator.
        let remaining = Math.min(delay, maxRetryDelay);
        while (remaining > 0) {
          const step = Math.min(remaining, HEARTBEAT_STEP);
          await new Promise((resolve) => setTimeout(resolve, step));
          remaining -= step;
          await beat();
        }
        await send(topic, body, { headers: { ...headers, ...extra, [REDELIVERED_HEADER]: '1' } });
      };
      const delivery = Object.freeze({
        id,
        body,
        headers,
        attempt,
        redelivered: headers[REDELIVERED_HEADER] === '1',
        ack: () => finish('ack', () => commit(partition, message.offset)),
        retry: ({ delay = 0 } = {}) =>
          finish('retry', async () => {
            await republish({ [ATTEMPT_HEADER]: String(attempt + 1) }, delay);
            await commit(partition, message.offset);
          }),
        release: () =>
          finish('release', async () => {
            await republish({ [ATTEMPT_HEADER]: String(attempt) });
            await commit(partition, message.offset);
          }),
        deadLetter: (reason = '') =>
          finish('dead', async () => {
            if (deadLetter) {
              await send(queueTopic(deadLetter), body, {
                headers: {
                  ...headers,
                  [DEAD_REASON_HEADER]: reasonText(reason),
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
        // Settled nothing: retried after a backoff, attempt + 1 — the
        // delivery contract (port.js), not a release to the head.
        report('broker.kafka.delivery', error, { queue });
        await delivery.retry({ delay: crashDelay(attempt) });
      }
    };

    const ready = joinWatcher(flavor, consumer);
    await consumer.run(runArgs(flavor, { concurrency: prefetch, eachMessage: handle }));
    // A join that never came within the window is NOT a failure: a group
    // with more members than partitions leaves some with nothing, and this
    // consumer is one of them until a rebalance says otherwise. Said once,
    // and the binding stays healthy — a readiness that flipped here would
    // crash-loop the fourth instance of a three-partition queue.
    if (!(await ready)) log.info({ event: 'broker.kafka.join-timeout', queue, group: groupId });

    const stop = async () => {
      if (!state.running) return;
      state.running = false;
      unwatch();
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
    // Pages waiting for their turn are let through, to fail as closed.
    for (const next of waiting.splice(0)) next();
    handlers.clear();
    ensured.clear();
    for (const consumer of Array.from(consumers)) await closeConsumer(consumer);
    // The ephemeral groups still held — the backplane instance's, a tail's
    // — in one call, once every member left; a queue's durable group is not
    // this broker's to delete.
    const lingering = Array.from(groups.values());
    groups.clear();
    if (producerClient) {
      const client = await producerClient.catch(() => null);
      producerClient = null;
      if (client) await client.disconnect().catch((error) => report('broker.kafka.disconnect', error));
    }
    if (adminClient) {
      const client = await adminClient.catch(() => null);
      adminClient = null;
      if (client) {
        if (lingering.length > 0) {
          await client
            .deleteGroups(lingering)
            .catch((error) => log.debug({ err: error, event: 'broker.kafka.groups', groups: lingering }));
        }
        await client.disconnect().catch((error) => report('broker.kafka.disconnect', error));
      }
    }
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
