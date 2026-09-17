'use strict';

// The two KafkaJS-shaped clients differ in CONFIG, not in method names, and
// the differences are exactly the ones the phase-0 spike measured:
//
//   kafkajs (2.2.4, no release since 2023-02-27 — not deprecated, but
//     inactive): `new Kafka({ brokers })`, `kafka.consumer({ groupId })`,
//     `fromBeginning` on subscribe(), `autoCommit` on run(), a GROUP_JOIN
//     event, `fetchTopicMetadata` -> { topics }.
//   @confluentinc/kafka-javascript (.KafkaJS, librdkafka, actively
//     released): every KafkaJS option nests under `kafkaJS`, the join
//     signal is polling `consumer.assignment()`, `consumer.events` THROWS
//     "Not implemented", `fetchTopicMetadata` -> a bare array.
//
// One module normalizes both so the adapter never branches.

const isFunction = (value) => typeof value === 'function';

/** kafkajs' client exposes `logger()`; the confluent facade does not. */
const detectFlavor = (kafka) => (isFunction(kafka.logger) ? 'kafkajs' : 'confluent');

const consumerConfig = (flavor, { groupId, fromBeginning = false, autoCommit = false, ...rest }) =>
  flavor === 'confluent' ? { kafkaJS: { groupId, fromBeginning, autoCommit, ...rest } } : { groupId, ...rest };

const producerConfig = (flavor, options = {}) =>
  flavor === 'confluent' ? { kafkaJS: { ...options } } : { ...options };

const subscribeArgs = (flavor, topics, fromBeginning) =>
  flavor === 'confluent' ? { topics } : { topics, fromBeginning };

const runArgs = (flavor, { eachMessage, concurrency }) => {
  const args = { eachMessage };
  if (concurrency > 1) args.partitionsConsumedConcurrently = concurrency;
  if (flavor !== 'confluent') args.autoCommit = false;
  return args;
};

/** kafkajs answers `{ topics: [...] }`, confluent the bare array. */
const metadataTopics = (result) => (Array.isArray(result) ? result : (result?.topics ?? []));

/**
 * Watches for this consumer JOINING its group — the moment from which its
 * subscription is live. It must be called BEFORE run(): kafkajs' GROUP_JOIN
 * fires once, and a listener registered afterwards never sees it.
 *
 * A fresh group reading `latest` loses everything published before the
 * join, which is what makes the Kafka backplane's caveat real (see the
 * guide).
 */
const joinWatcher = (flavor, consumer, { timeout = 30_000, step = 20 } = {}) => {
  if (flavor !== 'confluent') {
    let events = null;
    try {
      events = consumer.events;
    } catch {
      events = null; // the confluent facade throws from this getter
    }
    if (events?.GROUP_JOIN && isFunction(consumer.on)) {
      return new Promise((resolve) => {
        const remove = consumer.on(events.GROUP_JOIN, () => {
          if (isFunction(remove)) remove();
          resolve();
        });
        const timer = setTimeout(resolve, timeout);
        if (isFunction(timer.unref)) timer.unref();
      });
    }
  }
  // The confluent facade has no join event: its assignment is the signal.
  return (async () => {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      try {
        if (isFunction(consumer.assignment) && consumer.assignment().length > 0) return;
      } catch {
        // Not implemented on this client.
      }
      await new Promise((resolve) => setTimeout(resolve, step));
    }
  })();
};

module.exports = { detectFlavor, consumerConfig, producerConfig, subscribeArgs, runArgs, metadataTopics, joinWatcher };
