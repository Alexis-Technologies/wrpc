'use strict';

// The Kafka broker over the in-process fake (tests/broker/fakeKafka.js), in
// BOTH KafkaJS shapes — kafkajs and the confluent facade — so the
// normalization in src/broker/kafka/shape.js is exercised both ways. The
// same suites run against a real broker in tests/broker/kafka.integration.test.js.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { createKafkaBroker, encodeVector, decodeVector } = require('../../broker/kafka.js');
const { isBroker, isBrokerDirect } = require('../../broker.js');
const { createFakeKafka } = require('./fakeKafka.js');
const { runBackplaneContract } = require('./backplaneContract.js');
const { runLogContract } = require('./logContract.js');
const { runQueueContract } = require('./queueContract.js');
const { quiet, unique, waitFor, collect } = require('./support.js');

const FLAVORS = ['kafkajs', 'confluent'];

// Brokers a harness opened, closed when its suite is done.
const opened = [];

const open = (flavor, extra = {}) => {
  const kafka = createFakeKafka({ flavor });
  const broker = createKafkaBroker({ kafka, logger: quiet, partitions: 2, ...extra });
  return { kafka, broker };
};

for (const flavor of FLAVORS) {
  test(`kafka broker (fake, ${flavor}): backplane contract`, async (t) => {
    await runBackplaneContract(t, `kafka/${flavor}`, {
      open: async () => {
        const kafka = createFakeKafka({ flavor });
        const a = createKafkaBroker({ kafka, logger: quiet, partitions: 1 });
        const b = createKafkaBroker({ kafka, logger: quiet, partitions: 1 });
        opened.push(a, b);
        return [a.backplane, b.backplane];
      },
      // A consumer left running would keep the fake's timers — and the test
      // process — alive.
      close: async () => {
        for (const broker of opened.splice(0)) await broker.close();
      },
      settle: 50,
      timeout: 4000,
    });
  });

  test(`kafka broker (fake, ${flavor}): log contract`, async (t) => {
    await runLogContract(t, `kafka/${flavor}`, {
      open: async () => {
        const { kafka, broker } = open(flavor);
        const peer = createKafkaBroker({ kafka, logger: quiet, partitions: 2 });
        return {
          log: broker.log,
          peer: peer.log,
          close: async () => {
            await broker.close();
            await peer.close();
          },
          // Joined consumers on the fake: what an abandoned read leaks as.
          liveReads: () => kafka.server.members,
          // Every reader-group consumer crashes for good (kafkajs shape only:
          // the confluent facade has no events, so its tail cannot know).
          endLiveRead:
            flavor === 'kafkajs'
              ? async () => {
                  for (const [groupId, group] of kafka.server.groups) {
                    if (!groupId.startsWith('wrpc-read-')) continue;
                    for (const member of Array.from(group.members)) member.crash(new Error('lost'), false);
                  }
                }
              : undefined,
          beyondTip: (_topic, id) => {
            const cursor = decodeVector(id);
            for (const partition of Object.keys(cursor)) cursor[partition] += 1000;
            return encodeVector(cursor);
          },
        };
      },
      timeout: 5000,
    });
  });

  test(`kafka broker (fake, ${flavor}): queue contract`, async (t) => {
    let world = null;
    await runQueueContract(t, `kafka/${flavor}`, {
      open: async () => {
        world = open(flavor);
        const { kafka, broker } = world;
        const peer = createKafkaBroker({ kafka, logger: quiet, partitions: 2 });
        return {
          queue: broker.queue,
          peer: peer.queue,
          close: async () => {
            await broker.close();
            await peer.close();
          },
        };
      },
      failNextPublish: (times) => {
        world.kafka.server.failures.send = { error: new Error('LEADER_NOT_AVAILABLE'), times };
      },
      // kafkajs announces a crash and the rejoin; the confluent facade has
      // no events at all, so `healthy` cannot follow a crash there.
      breakConsumer:
        flavor === 'kafkajs'
          ? (name) => {
              const [member] = world.kafka.server.group(name).members;
              member.crash(new Error('fetch loop died'), true);
            }
          : undefined,
      timeout: 6000,
      redelivery: 1500,
    });
  });
}

test('kafka broker: no direct capability — and it says so', async () => {
  const { broker } = open('kafkajs');
  assert.strictEqual(isBroker(broker), true);
  assert.strictEqual(broker.direct, undefined);
  assert.strictEqual(isBrokerDirect(broker.direct), false);
  const { attachBrokerRpc } = require('../../broker.js');
  await assert.rejects(attachBrokerRpc({ rpc: null }, broker, { service: 'x' }), /Server or an RpcServer/);
  await broker.close();
});

test('kafka broker: the flavor is detected, and can be forced', async () => {
  const kafkajs = createFakeKafka({ flavor: 'kafkajs' });
  const confluent = createFakeKafka({ flavor: 'confluent' });
  // kafkajs' client has logger(); the confluent facade does not.
  for (const broker of [
    createKafkaBroker({ kafka: kafkajs, logger: quiet }),
    createKafkaBroker({ kafka: confluent, logger: quiet }),
    createKafkaBroker({ kafka: confluent, flavor: 'confluent', logger: quiet }),
  ]) {
    assert.strictEqual(broker.name, 'kafka');
    await broker.close();
  }
  assert.throws(() => createKafkaBroker({ kafka: kafkajs, flavor: 'librdkafka' }), /flavor must be/);
  assert.throws(() => createKafkaBroker({}), /KafkaJS-shaped client/);
  assert.throws(() => createKafkaBroker({ kafka: { producer() {} } }), /KafkaJS-shaped client/);
  assert.throws(() => createKafkaBroker({ kafka: kafkajs, partitions: 0 }), /partitions/);
});

test('kafka broker: the resume token is a vector of partition offsets', () => {
  const { broker } = open('kafkajs');
  assert.strictEqual(encodeVector({ 2: 5, 0: 1 }), 'k1:0=1,2=5');
  assert.deepStrictEqual(decodeVector('k1:0=1,2=5'), { 0: 1, 2: 5 });
  assert.strictEqual(decodeVector('k1:'), null);
  assert.strictEqual(decodeVector('0=1'), null);
  assert.strictEqual(decodeVector(42), null);
  assert.strictEqual(broker.log.parseId('k1:0=1'), 'k1:0=1');
  assert.strictEqual(broker.log.parseId('k1:x=1'), null);
  assert.strictEqual(broker.log.parseId(''), null);
  assert.throws(() => broker.log.read('t', { from: 'middle' }), /from must be/);
  return broker.close();
});

test('kafka broker: a feed resumes exactly, across partitions', async (t) => {
  // A feed topic is single-partition by default (order); this one opts into
  // two, which is where the vector id earns its keep.
  const { broker } = open('kafkajs', { logPartitions: 2 });
  t.after(() => broker.close());
  const topic = unique('feed');
  for (const value of ['1', '2', '3', '4']) await broker.log.append(topic, value);
  const first = await collect(broker.log.read(topic, { from: 'earliest' }), 2, { timeout: 5000 });
  const rest = await collect(broker.log.read(topic, { after: first[1].id }), 2, { timeout: 5000 });
  // Order is per partition, so the set is what matters across two of them.
  assert.deepStrictEqual([...first, ...rest].map((entry) => entry.value).sort(), ['1', '2', '3', '4']);
  // Every id is a full vector, so a resume covers every partition.
  assert.match(rest[1].id, /^k1:0=\d+,1=\d+$/);
});

// A catch-up page is a consumer group of its own — a connection, a JoinGroup,
// a rebalance of nobody — and a resume storm after a deploy used to open one
// per reader, all at once.
test('kafka broker: catch-up pages are read maxCatchUp at a time, and identical pages are one read', async (t) => {
  const kafka = createFakeKafka({ flavor: 'kafkajs' });
  const broker = createKafkaBroker({ kafka, logger: quiet, partitions: 1, maxCatchUp: 3 });
  t.after(() => broker.close());
  const topic = unique('storm');
  const ids = [];
  for (let n = 0; n < 40; n++) ids.push(await broker.log.append(topic, String(n)));
  // Reader consumers connected at once (a tail's live reader is one too),
  // counted where they connect and disconnect.
  let live = 0;
  let peak = 0;
  const opened = [];
  const consumer = kafka.consumer.bind(kafka);
  kafka.consumer = (config) => {
    const made = consumer(config);
    if (!String(config.groupId).startsWith('wrpc-read-')) return made;
    opened.push(config.groupId);
    const connect = made.connect.bind(made);
    const disconnect = made.disconnect.bind(made);
    made.connect = async () => {
      peak = Math.max(peak, ++live);
      return connect();
    };
    made.disconnect = async () => {
      live--;
      return disconnect();
    };
    return made;
  };
  // Thirty readers, each resuming from a different entry: thirty different pages.
  const controllers = [];
  const resumed = await Promise.all(
    ids.slice(0, 30).map((after) => {
      const controller = new AbortController();
      controllers.push(controller);
      return collect(broker.log.read(topic, { after, signal: controller.signal }), 2, { timeout: 20_000 });
    }),
  );
  for (let n = 0; n < 30; n++) {
    assert.deepStrictEqual(
      resumed[n].map((entry) => entry.value),
      [String(n + 1), String(n + 2)],
      `reader ${n} resumed exactly`,
    );
  }
  assert.ok(peak <= 3 + 1, `at most maxCatchUp pages at once, beside the one live tail — saw ${peak} reader groups`);
  assert.ok(peak >= 2, 'and they did run side by side');
  for (const controller of controllers) controller.abort();

  // The same cursor from twenty readers — a room that lost one instance —
  // is ONE page: one consumer opened for it, not twenty.
  opened.length = 0;
  const again = await Promise.all(
    Array.from({ length: 20 }, () => {
      const controller = new AbortController();
      controllers.push(controller);
      return collect(broker.log.read(topic, { after: ids[9], signal: controller.signal }), 3, { timeout: 20_000 });
    }),
  );
  for (const entries of again) {
    assert.deepStrictEqual(
      entries.map((entry) => entry.value),
      ['10', '11', '12'],
    );
  }
  assert.ok(opened.length <= 2, `one page (and at most a live tail), not twenty — opened ${opened.length}`);
  for (const controller of controllers) controller.abort();
});

test('kafka broker: a catch-up page that fails gives its turn to the next; maxCatchUp is validated', async (t) => {
  const kafka = createFakeKafka({ flavor: 'kafkajs' });
  const broker = createKafkaBroker({ kafka, logger: quiet, partitions: 1, maxCatchUp: 1 });
  t.after(() => broker.close());
  const topic = unique('turns');
  const ids = [];
  for (let n = 0; n < 6; n++) ids.push(await broker.log.append(topic, String(n)));
  // The topic's live tail first — every resume below joins it — so that the
  // failure injected next lands on a PAGE.
  const tail = new AbortController();
  t.after(() => tail.abort());
  await broker.log.read(topic, { signal: tail.signal }).ready;
  // One page at a time, and the first one's subscribe fails.
  kafka.server.failures.subscribe = new Error('coordinator not available');
  const reads = [0, 1, 2].map((n) => {
    const controller = new AbortController();
    t.after(() => controller.abort());
    return collect(broker.log.read(topic, { after: ids[n], signal: controller.signal }), 1, { timeout: 20_000 }).then(
      (entries) => entries[0].value,
      (error) => error.message,
    );
  });
  const outcomes = await Promise.all(reads);
  assert.strictEqual(outcomes.filter((value) => /coordinator not available/.test(value)).length, 1, 'one page failed');
  assert.strictEqual(outcomes.filter((value) => /^\d$/.test(value)).length, 2, 'the two behind it were still read');
  for (const maxCatchUp of [0, -1, 1.5, '4']) {
    assert.throws(
      () => createKafkaBroker({ kafka, logger: quiet, maxCatchUp }),
      /options\.maxCatchUp must be a positive integer/,
    );
  }
});

test('kafka broker: the backplane loses what was published before the group joined', async (t) => {
  const { kafka, broker } = open('kafkajs', { partitions: 1 });
  t.after(() => broker.close());
  const other = createKafkaBroker({ kafka, logger: quiet, partitions: 1 });
  t.after(() => other.close());
  // Published while this instance has no consumer at all.
  other.backplane.publish('room', 'before');
  await timers.setTimeout(30);
  const seen = [];
  await broker.backplane.subscribe('room', (message) => seen.push(message));
  other.backplane.publish('room', 'after');
  await waitFor(() => seen.length === 1, { timeout: 4000 });
  await timers.setTimeout(50);
  assert.deepStrictEqual(seen, ['after'], 'a fresh group reads from `latest`');
});

test("kafka broker: closing disconnects every consumer and deletes its OWN groups — never a queue's durable one", async () => {
  const { kafka, broker } = open('kafkajs');
  const seen = [];
  await broker.backplane.subscribe('room', (message) => seen.push(message));
  const queue = unique('q');
  const consumer = await broker.queue.consume(queue, (delivery) => delivery.ack());
  assert.strictEqual(consumer.healthy, true);
  assert.ok(kafka.server.groups.size >= 2);
  await broker.close();
  await broker.close();
  assert.strictEqual(consumer.healthy, false);
  assert.strictEqual(kafka.server.members, 0, 'every consumer left');
  // The backplane instance's group is gone; the queue's — whose committed
  // offsets are the queue's progress — is exactly what stays.
  assert.deepStrictEqual(Array.from(kafka.server.groups.keys()), [queue]);
  await assert.rejects(broker.log.append('t', 'x'), (error) => error.code === 503);
  await assert.rejects(broker.queue.produce('q', 'x'), (error) => error.code === 503);
  await assert.rejects(
    broker.queue.consume('q', () => {}),
    (error) => error.code === 503,
  );
});

for (const flavor of FLAVORS) {
  test(`kafka broker (${flavor}): a clean restart of the last instance does not redeliver the queue`, async (t) => {
    // The bug: close() deleted every group it had opened, the durable
    // queue group included — with its committed offsets — so the next
    // instance, joining a fresh group from the beginning, redelivered the
    // whole retention on every deploy.
    const { kafka, broker } = open(flavor);
    const queue = unique('orders');
    const first = [];
    const consumer = await broker.queue.consume(queue, (delivery) => {
      first.push(delivery.body);
      return delivery.ack();
    });
    for (const body of ['a', 'b', 'c']) await broker.queue.produce(queue, body);
    await waitFor(() => first.length === 3);
    await timers.setTimeout(20);
    await consumer.stop();
    await broker.close();
    assert.ok(kafka.server.groups.has(queue), 'the durable group survived the close');

    const next = createKafkaBroker({ kafka, logger: quiet, partitions: 2 });
    t.after(() => next.close());
    const again = [];
    const resumed = await next.queue.consume(queue, (delivery) => {
      again.push(delivery.body);
      return delivery.ack();
    });
    t.after(() => resumed.stop());
    await next.queue.produce(queue, 'd');
    await waitFor(() => again.length === 1);
    await timers.setTimeout(50);
    assert.deepStrictEqual(again, ['d'], 'only what was produced after the restart');
  });

  test(`kafka broker (${flavor}): a reader whose subscribe or run fails leaves no consumer and no group behind`, async (t) => {
    const { kafka, broker } = open(flavor);
    t.after(() => broker.close());
    const topic = unique('feed');
    await broker.log.append(topic, 'one');
    const readerGroups = () => Array.from(kafka.server.groups.keys()).filter((id) => id.includes('-read-'));
    for (const step of ['subscribe', 'run']) {
      // A catch-up page (range) and a live tail: both open a reader.
      kafka.server.failures[step] = new Error(`${step} refused`);
      await assert.rejects(collect(broker.log.read(topic, { from: 'earliest' }), 1), new RegExp(`${step} refused`));
      kafka.server.failures[step] = new Error(`${step} refused`);
      await assert.rejects(collect(broker.log.read(topic), 1, { timeout: 500 }), /refused|timeout/);
      await waitFor(() => kafka.server.members === 0 && readerGroups().length === 0, {
        message: `after a failed ${step}: ${kafka.server.members} members, groups ${readerGroups()}`,
      });
    }
    // And a page that completes drops its group at once, not at close().
    assert.deepStrictEqual(await collect(broker.log.read(topic, { from: 'earliest' }), 1).then((e) => e.length), 1);
    await waitFor(() => readerGroups().length <= 1, { message: `lingering reader groups: ${readerGroups()}` });
  });
}

test('kafka broker: pause and resume ride the consumer, not the group', async (t) => {
  const { broker } = open('kafkajs');
  t.after(() => broker.close());
  const name = unique('paused');
  const seen = [];
  const consumer = await broker.queue.consume(name, (delivery) => {
    seen.push(delivery.body);
    return delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'first');
  await waitFor(() => seen.length === 1, { timeout: 5000 });
  await consumer.pause();
  await broker.queue.produce(name, 'second');
  await timers.setTimeout(150);
  assert.deepStrictEqual(seen, ['first']);
  await consumer.resume();
  await waitFor(() => seen.length === 2, { timeout: 5000 });
});

test('kafka broker: replicationFactor and maxRetryDelay are refused at construction, deadLetter at consume', async () => {
  const kafka = createFakeKafka({ flavor: 'kafkajs' });
  for (const replicationFactor of [0, -2, 1.5, '3']) {
    assert.throws(
      () => createKafkaBroker({ kafka, logger: quiet, replicationFactor }),
      /options\.replicationFactor must be a positive integer, or -1/,
    );
  }
  for (const maxRetryDelay of [-1, 0.5, '1s']) {
    assert.throws(() => createKafkaBroker({ kafka, logger: quiet, maxRetryDelay }), /options\.maxRetryDelay/);
  }
  const broker = createKafkaBroker({ kafka, logger: quiet, replicationFactor: -1, maxRetryDelay: 0 });
  await assert.rejects(
    broker.queue.consume('q', () => {}, { deadLetter: '' }),
    /deadLetter must be a queue name or null/,
  );
  await broker.close();
});

// Every line the adapter logs, for the settlement asserts.
const recording = () => {
  const entries = [];
  const logger = {
    log() {},
    info: (entry) => entries.push({ level: 'info', ...entry }),
    debug() {},
    warn: (entry) => entries.push({ level: 'warn', ...entry }),
    error: (entry) => entries.push({ level: 'error', ...entry }),
    child: () => logger,
  };
  return { logger, entries };
};

for (const flavor of FLAVORS) {
  test(`kafka broker (fake, ${flavor}): a settlement the broker keeps refusing is handed back, never committed past`, async (t) => {
    const kafka = createFakeKafka({ flavor });
    const { logger, entries } = recording();
    const broker = createKafkaBroker({ kafka, logger, partitions: 1 });
    t.after(() => broker.close());
    const name = unique('refused');
    const seen = [];
    // Both produced before the consumer starts: the refusal armed by the
    // handler must hit the retry's copy, not a produce.
    await broker.queue.produce(name, 'a');
    await broker.queue.produce(name, 'b');
    const consumer = await broker.queue.consume(
      name,
      async (delivery) => {
        seen.push([delivery.body, delivery.attempt]);
        // The first time 'a' comes, a retry — whose copy the broker refuses
        // every time. The settlement used to be swallowed after one log
        // line, and 'b' committed past 'a'; now it gives up and seeks back.
        if (delivery.body === 'a' && seen.filter(([body]) => body === 'a').length === 1) {
          kafka.server.failures.send = { error: new Error('NOT_LEADER_FOR_PARTITION'), times: 3 };
          return delivery.retry({ delay: 0 });
        }
        await delivery.ack();
      },
      { prefetch: 1 },
    );
    t.after(() => consumer.stop());
    await waitFor(() => seen.length === 3, { timeout: 5000 });
    assert.deepStrictEqual(seen, [
      ['a', 1],
      ['a', 1],
      ['b', 1],
    ]);
    const settle = entries.find((entry) => entry.event === 'broker.kafka.settle');
    assert.deepStrictEqual([settle?.action, settle?.id, settle?.partition], ['retry', '0:0', 0]);
    assert.strictEqual(consumer.healthy, true, 'back to healthy after a settlement landed');
  });

  test(`kafka broker (fake, ${flavor}): a retry's in-process delay heartbeats, and a rebalance mid-wait leaves the message to the group`, async (t) => {
    const kafka = createFakeKafka({ flavor });
    const { logger, entries } = recording();
    const broker = createKafkaBroker({ kafka, logger, partitions: 1 });
    t.after(() => broker.close());
    const name = unique('beat');
    const seen = [];
    let waited = false;
    const consumer = await broker.queue.consume(name, async (delivery) => {
      seen.push([delivery.body, delivery.attempt]);
      if (!waited) {
        waited = true;
        return delivery.retry({ delay: 150 });
      }
      await delivery.ack();
    });
    t.after(() => consumer.stop());
    await broker.queue.produce(name, 'x');
    await waitFor(() => seen.length === 2, { timeout: 4000 });
    assert.deepStrictEqual(seen, [
      ['x', 1],
      ['x', 2],
    ]);
    const [member] = kafka.server.group(name).members;
    assert.ok(member.heartbeats >= 1, 'a wait of 150 ms sent at least one heartbeat');
    // A rebalance during the wait: the heartbeat fails, and neither the
    // copy nor the commit is this member's to make — the partition's new
    // owner fetches the message from the last committed offset.
    waited = false;
    await broker.queue.produce(name, 'y');
    await waitFor(() => seen.length === 3);
    member.rebalancing = true;
    await waitFor(() => entries.some((entry) => entry.event === 'broker.kafka.rebalanced'), { timeout: 4000 });
    await timers.setTimeout(50);
    assert.strictEqual(seen.length, 3, 'no copy was published, nothing committed');
    member.rebalancing = false;
    member.crash(new Error('rebalance'), true);
    await waitFor(() => seen.length === 4, { timeout: 4000 });
    assert.deepStrictEqual(seen[3], ['y', 1], 'fetched again from the committed offset, attempt untouched');
  });
}

for (const flavor of FLAVORS) {
  test(`kafka broker (fake, ${flavor}): a topic is created with the broker's default replication factor, and said once`, async (t) => {
    const kafka = createFakeKafka({ flavor });
    const { logger, entries } = recording();
    const broker = createKafkaBroker({ kafka, logger, partitions: 2 });
    t.after(() => broker.close());
    const name = unique('topic');
    await broker.queue.produce(name, 'a');
    await broker.queue.produce(name, 'b');
    const topic = kafka.server.topics.get(`wrpc.q.${name}`);
    assert.ok(topic, 'the queue topic was created');
    // -1 is KIP-464: the broker's own default.replication.factor, so a
    // production cluster's three replicas are three, not the one a library
    // default used to ask for.
    assert.strictEqual(topic.replicationFactor, -1);
    assert.strictEqual(topic.partitions.length, 2);
    const created = entries.filter((entry) => entry.event === 'broker.kafka.topic');
    assert.deepStrictEqual(created, [
      { level: 'info', event: 'broker.kafka.topic', topic: `wrpc.q.${name}`, partitions: 2, replicationFactor: -1 },
    ]);
    // A second broker finds the topic: no creation, nothing logged.
    const other = createKafkaBroker({ kafka, logger, partitions: 5, replicationFactor: 3 });
    t.after(() => other.close());
    await other.queue.produce(name, 'c');
    assert.strictEqual(topic.replicationFactor, -1, 'an existing topic keeps its replication factor');
    assert.strictEqual(topic.partitions.length, 2, 'and its partition count');
    assert.strictEqual(entries.filter((entry) => entry.event === 'broker.kafka.topic').length, 1);
    // An explicit factor is passed as it is.
    const explicit = unique('explicit');
    await other.queue.produce(explicit, 'd');
    assert.strictEqual(kafka.server.topics.get(`wrpc.q.${explicit}`).replicationFactor, 3);
  });
}
