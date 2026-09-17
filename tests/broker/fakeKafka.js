'use strict';

// A small in-process Kafka for the adapter's unit tests, in BOTH KafkaJS
// shapes: `flavor: 'kafkajs'` (plain config, `fromBeginning` on subscribe,
// `autoCommit` on run, a GROUP_JOIN event) and `flavor: 'confluent'` (every
// option nested under `kafkaJS`, `consumer.events` THROWS, the join signal
// is `assignment()`), so src/broker/kafka/shape.js is exercised both ways.
//
// Partitions, offsets, consumer groups with manual commits, rebalancing on
// join/leave, pause/resume and seek are modelled; everything else is not.
//
// Not a *.test.js: node --test must not run it.

const { EventEmitter } = require('node:events');

class Topic {
  constructor(name, partitions) {
    this.name = name;
    this.partitions = Array.from({ length: partitions }, () => []);
    this.cursor = 0;
  }

  append(message) {
    const partition =
      message.key === undefined || message.key === null
        ? this.cursor++ % this.partitions.length
        : Math.abs([...String(message.key)].reduce((hash, char) => hash * 31 + char.charCodeAt(0), 7)) %
          this.partitions.length;
    const offset = this.partitions[partition].length;
    this.partitions[partition].push({ ...message, offset, partition });
    return { partition, offset };
  }
}

class FakeKafkaServer {
  topics = new Map();
  groups = new Map(); // groupId -> { offsets: Map(`${topic}:${partition}` -> next), members: Set }
  #waiters = new Set();

  topic(name, partitions = 3) {
    let topic = this.topics.get(name);
    if (!topic) {
      topic = new Topic(name, partitions);
      this.topics.set(name, topic);
    }
    return topic;
  }

  group(groupId) {
    let group = this.groups.get(groupId);
    if (!group) {
      group = { offsets: new Map(), members: new Set() };
      this.groups.set(groupId, group);
    }
    return group;
  }

  notify() {
    const waiters = Array.from(this.#waiters);
    this.#waiters.clear();
    for (const wake of waiters) wake();
  }

  wait(ms) {
    return new Promise((resolve) => {
      // NOT unref'd: this fake has no I/O of its own, and an unref'd timer
      // would let a test process exit in the middle of a delivery.
      setTimeout(() => {
        this.#waiters.delete(resolve);
        resolve();
      }, ms);
      this.#waiters.add(resolve);
    });
  }
}

const config = (flavor, options = {}) => (flavor === 'confluent' ? (options.kafkaJS ?? {}) : options);

class FakeProducer {
  constructor(server) {
    this.server = server;
  }

  async connect() {}
  async disconnect() {}

  async send({ topic, messages }) {
    const target = this.server.topic(topic);
    const results = [];
    for (const message of messages) {
      const { partition, offset } = target.append(message);
      results.push({ topicName: topic, partition, baseOffset: String(offset), errorCode: 0 });
    }
    this.server.notify();
    return results;
  }
}

class FakeConsumer extends EventEmitter {
  #running = false;
  #paused = new Set();
  #subscribed = [];
  #fromBeginning = false;
  #handler = null;
  #seeks = new Map();
  #joined = false;
  #concurrency = 1;

  static events = { GROUP_JOIN: 'consumer.group_join' };

  constructor(server, flavor, options) {
    super();
    this.server = server;
    this.flavor = flavor;
    const settings = config(flavor, options);
    this.groupId = settings.groupId;
    this.#fromBeginning = settings.fromBeginning === true;
    this.autoCommit = settings.autoCommit !== false;
  }

  // kafkajs exposes the event names; the confluent facade throws here.
  get events() {
    if (this.flavor === 'confluent') {
      const error = new Error('Not implemented');
      error.code = -170;
      throw error;
    }
    return FakeConsumer.events;
  }

  assignment() {
    if (this.flavor !== 'confluent') return [];
    if (!this.#joined) return [];
    const out = [];
    for (const name of this.#subscribed) {
      const topic = this.server.topic(name);
      for (let partition = 0; partition < topic.partitions.length; partition++) out.push({ topic: name, partition });
    }
    return out;
  }

  async connect() {
    this.server.group(this.groupId).members.add(this);
  }

  async disconnect() {
    this.#running = false;
    this.server.group(this.groupId).members.delete(this);
  }

  // `topics` is top-level in BOTH clients; only kafkajs takes
  // `fromBeginning` here (the confluent facade takes it in the consumer
  // config, which the constructor already read).
  async subscribe(options = {}) {
    this.#subscribed = options.topics ?? (options.topic ? [options.topic] : []);
    if (options.fromBeginning !== undefined) this.#fromBeginning = options.fromBeginning === true;
    for (const name of this.#subscribed) this.server.topic(name);
  }

  pause(list) {
    for (const entry of list) this.#paused.add(entry.topic);
  }

  resume(list) {
    for (const entry of list) this.#paused.delete(entry.topic);
    this.server.notify();
  }

  seek({ topic, partition, offset }) {
    if (!this.#running) throw new Error('Consumer group was not initialized, consumer#run must be called first');
    this.#seeks.set(`${topic}:${partition}`, Number(offset));
  }

  async commitOffsets(list) {
    const group = this.server.group(this.groupId);
    for (const entry of list) group.offsets.set(`${entry.topic}:${entry.partition}`, Number(entry.offset));
  }

  async run(options = {}) {
    this.#handler = options.eachMessage;
    // kafkajs takes autoCommit here, the confluent facade in the consumer
    // config (which the constructor already read).
    if (options.autoCommit !== undefined) this.autoCommit = options.autoCommit !== false;
    // Real Kafka runs this many partitions at once; within one partition it
    // is always sequential.
    this.#concurrency = options.partitionsConsumedConcurrently ?? 1;
    this.#running = true;
    void this.#loop();
    // The join is announced a tick later, as a real rebalance would be.
    setTimeout(() => {
      this.#joined = true;
      this.emit(FakeConsumer.events.GROUP_JOIN, { memberId: 'fake' });
    }, 5);
  }

  // One partition at a time, in offset order, honouring commits, pauses,
  // seeks and the other members of the group.
  async #loop() {
    const group = this.server.group(this.groupId);
    const busy = new Set();
    while (this.#running) {
      const work = [];
      for (const name of this.#subscribed) {
        if (this.#paused.has(name)) continue;
        const topic = this.server.topic(name);
        for (let partition = 0; partition < topic.partitions.length; partition++) {
          const key = `${name}:${partition}`;
          if (busy.has(key) || busy.size >= this.#concurrency) continue;
          // Partitions are split across the group's members, in order.
          const members = Array.from(group.members).filter((member) => member.#running);
          const owner = members[partition % Math.max(1, members.length)];
          if (owner && owner !== this) continue;
          if (!this.#seeks.has(key)) {
            // The starting position is fixed when the partition is first
            // seen — `latest` means "from here", not "from wherever the end
            // happens to be each time we look".
            const committed = group.offsets.get(key);
            this.#seeks.set(key, committed ?? (this.#fromBeginning ? 0 : topic.partitions[partition].length));
          }
          const committed = this.#seeks.get(key);
          const entry = topic.partitions[partition][committed];
          if (!entry) continue;
          this.#seeks.set(key, committed + 1);
          const message = {
            key: entry.key === undefined ? null : Buffer.from(String(entry.key)),
            value: entry.value === null || entry.value === undefined ? null : Buffer.from(String(entry.value)),
            headers: entry.headers ?? {},
            offset: String(entry.offset),
            timestamp: String(Date.now()),
          };
          busy.add(key);
          work.push(
            Promise.resolve()
              .then(() => this.#handler({ topic: name, partition, message }))
              .catch(() => {
                // eachMessage threw: a real consumer would retry the batch.
              })
              .then(() => {
                busy.delete(key);
                if (this.autoCommit) group.offsets.set(key, committed + 1);
              }),
          );
        }
      }
      if (!this.#running) return;
      if (work.length === 0) await this.server.wait(10);
      else if (this.#concurrency === 1) await Promise.all(work);
      else await Promise.race([Promise.all(work), this.server.wait(5)]);
    }
  }
}

class FakeAdmin {
  constructor(server) {
    this.server = server;
  }

  async connect() {}
  async disconnect() {}

  async createTopics({ topics }) {
    for (const entry of topics) {
      if (this.server.topics.has(entry.topic)) {
        const error = new Error('Topic already exists');
        throw error;
      }
      this.server.topic(entry.topic, entry.numPartitions ?? 3);
    }
    return true;
  }

  async deleteTopics({ topics }) {
    for (const name of topics) this.server.topics.delete(name);
  }

  async fetchTopicMetadata({ topics }) {
    const list = topics.map((name) => ({
      name,
      partitions: this.server.topic(name).partitions.map((_, partition) => ({ partitionId: partition })),
    }));
    // kafkajs answers `{ topics }`, the confluent facade the bare array.
    return this.flavor === 'confluent' ? list : { topics: list };
  }

  async fetchTopicOffsets(name) {
    const topic = this.server.topic(name);
    return topic.partitions.map((messages, partition) => ({
      partition,
      offset: String(messages.length),
      high: String(messages.length),
      low: String(messages.low ?? 0),
    }));
  }

  async deleteGroups(ids) {
    for (const id of ids) this.server.groups.delete(id);
    return ids.map((groupId) => ({ groupId, errorCode: 0 }));
  }

  async listGroups() {
    return { groups: Array.from(this.server.groups.keys()).map((groupId) => ({ groupId })) };
  }
}

const createFakeKafka = ({ flavor = 'kafkajs', server = new FakeKafkaServer() } = {}) => {
  const kafka = {
    server,
    flavor,
    producer: () => new FakeProducer(server),
    consumer: (options) => {
      const settings = config(flavor, options);
      if (flavor === 'confluent' && options?.groupId !== undefined) {
        throw new Error("The 'groupId' property seems to be a KafkaJS property in the main config block.");
      }
      if (!settings.groupId) throw new Error('Consumer groupId must be a non-empty string.');
      return new FakeConsumer(server, flavor, options);
    },
    admin: () => {
      const admin = new FakeAdmin(server);
      admin.flavor = flavor;
      return admin;
    },
  };
  // Only kafkajs' client has logger(): that is how the adapter tells them
  // apart without being told.
  if (flavor !== 'confluent') kafka.logger = () => ({ info() {}, error() {} });
  return kafka;
};

module.exports = { createFakeKafka, FakeKafkaServer };
