'use strict';

// The NATS broker over the in-process fake (tests/broker/fakeNats.js): the
// same contract suites a real server runs in tests/broker/nats.integration.test.js.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { createNatsBroker } = require('../../broker/nats.js');
const { isBroker } = require('../../broker.js');
const { createFakeNats } = require('./fakeNats.js');
const { runBackplaneContract } = require('./backplaneContract.js');
const { runLogContract } = require('./logContract.js');
const { runQueueContract } = require('./queueContract.js');
const { runDirectContract } = require('./directContract.js');
const { quiet, unique, waitFor, collect } = require('./support.js');

const open = (extra = {}) => {
  const world = createFakeNats();
  const broker = createNatsBroker({ ...world, logger: quiet, ackWait: 300, ...extra });
  return { world, broker, close: () => broker.close() };
};

test('nats broker (fake): backplane contract', async (t) => {
  await runBackplaneContract(t, 'nats', {
    open: async () => {
      const world = createFakeNats();
      const a = createNatsBroker({ ...world, logger: quiet });
      const b = createNatsBroker({ ...world, logger: quiet });
      return [a.backplane, b.backplane];
    },
    close: async () => {},
  });
});

test('nats broker (fake): log contract', async (t) => {
  await runLogContract(t, 'nats', {
    open: async () => {
      const { world, broker, close } = open();
      const peer = createNatsBroker({ ...world, logger: quiet });
      return {
        log: broker.log,
        peer: peer.log,
        close,
        trim: async (topic, keep) => {
          const jsm = await world.jetstreamManager();
          const name = `wrpc_log_${topic.replace(/[^A-Za-z0-9_-]/g, '_')}`;
          const info = await jsm.streams.info(name);
          await jsm.streams.purge(name, { seq: Number(info.state.last_seq) - keep + 1 });
        },
        // The server ends every open pull on the topic's stream.
        endLiveRead: async (topic) => {
          const name = `wrpc_log_${topic.replace(/[^A-Za-z0-9_-]/g, '_')}`;
          const stream = world.server.streams.get(name);
          for (const live of Array.from(stream?.live ?? [])) await live.close();
        },
        // No foreignId: a JetStream sequence carries no epoch, so an id
        // from another stream is indistinguishable from a future one (the
        // feed's signed ids are the mitigation — see the NATS guide).
        beyondTip: (_topic, id) => String(Number(id) + 100),
      };
    },
    timeout: 4000,
  });
});

test('nats broker (fake): queue contract', async (t) => {
  await runQueueContract(t, 'nats', {
    open: async () => {
      const { world, broker, close } = open();
      const peer = createNatsBroker({ ...world, logger: quiet, ackWait: 300 });
      return { queue: broker.queue, peer: peer.queue, close };
    },
    timeout: 4000,
    redelivery: 600,
  });
});

test('nats broker (fake): direct contract', async (t) => {
  await runDirectContract(t, 'nats', {
    open: async () => {
      const { world, broker, close } = open();
      const peer = createNatsBroker({ ...world, logger: quiet });
      return { direct: broker.direct, peer: peer.direct, close };
    },
    timeout: 4000,
  });
});

test('nats broker: injection is validated structurally', () => {
  const world = createFakeNats();
  assert.throws(() => createNatsBroker({}), /options.nc must be a NATS connection/);
  assert.throws(() => createNatsBroker({ nc: world.nc }), /options.headers must be the nats headers/);
  assert.throws(
    () => createNatsBroker({ nc: world.nc, headers: world.headers, jetstream: world.jetstream }),
    /come together/,
  );
  assert.throws(
    () => createNatsBroker({ nc: world.nc, headers: world.headers, jetstream: 'js', jetstreamManager: 'jsm' }),
    /JetStream factories/,
  );
  const broker = createNatsBroker({ ...world, logger: quiet });
  assert.strictEqual(isBroker(broker), true);
  assert.strictEqual(broker.name, 'nats');
  assert.match(broker.direct.inbox(), /^_INBOX\./);
  assert.throws(() => broker.backplane.subscribe('x', 'nope'), /handler must be a function/);
  assert.throws(() => broker.log.read('t', { from: 'middle' }), /from must be/);
});

test('nats broker: without JetStream there is no log and no queue', async () => {
  const world = createFakeNats();
  const broker = createNatsBroker({ nc: world.nc, headers: world.headers, logger: quiet });
  assert.strictEqual(broker.log, undefined);
  assert.strictEqual(broker.queue, undefined);
  assert.strictEqual(isBroker(broker), true);
  // The backplane and direct halves still work.
  const seen = [];
  const unsubscribe = await broker.backplane.subscribe('room', (message) => seen.push(message));
  broker.backplane.publish('room', 'hi');
  await waitFor(() => seen.length === 1);
  await unsubscribe();
  await broker.close();
});

test('nats broker: an inbox uses createInbox when one is injected', () => {
  const world = createFakeNats();
  const withInbox = createNatsBroker({ ...world, logger: quiet });
  assert.match(withInbox.direct.inbox(), /^_INBOX\.fake\./);
  const withoutInbox = createNatsBroker({ ...world, createInbox: null, logger: quiet });
  assert.match(withoutInbox.direct.inbox(), /^_INBOX\.[0-9a-f-]{36}$/);
});

test('nats broker: names become one subject token', async (t) => {
  const { world, broker, close } = open();
  t.after(close);
  const seen = [];
  await broker.backplane.subscribe('room:a.b', (message) => seen.push(message));
  broker.backplane.publish('room:a.b', 'exact');
  await waitFor(() => seen.length === 1);
  const subjects = Array.from(world.server.subscriptions).map((subscription) => subscription.subject);
  assert.ok(
    subjects.every((subject) => subject.split('.').length === 3),
    `a name leaked into the subject hierarchy: ${subjects}`,
  );
});

test('nats broker: a message slower than ack_wait keeps its lease through working()', async (t) => {
  // Only setInterval is mocked — the keepalive's clock. setTimeout stays
  // real: the fake's ack_wait timer (300 ms) is what working() must keep
  // resetting, and timers/promises would never resolve under a mock.
  t.mock.timers.enable({ apis: ['setInterval'] });
  const { world, broker, close } = open({ ackWait: 300 });
  t.after(close);
  const name = unique('slow');
  const seen = [];
  let release;
  const consumer = await broker.queue.consume(name, async (delivery) => {
    seen.push(delivery.attempt);
    await new Promise((resolve) => (release = resolve));
    await delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'work');
  await waitFor(() => seen.length === 1, { timeout: 3000 });
  const [fake] = world.server.streams.get(`wrpc_q_${name}`).consumers.values();
  assert.strictEqual(fake.workingCalls, 0);
  // Seven keepalive ticks (a third of ack_wait each) while the handler
  // holds the message: every one is a working() to the server. The test
  // used to run 900 ms of real time against a 2 s window, which could not
  // tell a keepalive from a window that had not expired yet.
  for (let i = 0; i < 7; i++) {
    t.mock.timers.tick(100);
    await timers.setTimeout(10);
  }
  assert.ok(fake.workingCalls >= 2, `working() was called ${fake.workingCalls} times`);
  assert.deepStrictEqual(seen, [1], 'never redelivered under a live handler');
  release();
  await waitFor(() => fake.pending.size === 0, { timeout: 2000 });
});

test('nats broker: a released message keeps its attempt across the republish', async (t) => {
  const { broker, close } = open();
  t.after(close);
  const name = unique('released');
  const seen = [];
  const consumer = await broker.queue.consume(name, (delivery) => {
    seen.push([delivery.attempt, delivery.redelivered]);
    if (seen.length === 1) return delivery.retry({ delay: 0 });
    if (seen.length === 2) return delivery.release();
    return delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'work');
  await waitFor(() => seen.length === 3, { timeout: 4000 });
  assert.deepStrictEqual(seen, [
    [1, false],
    [2, true],
    [2, true],
  ]);
});

test('nats broker: closing stops the tails and refuses new work', async (t) => {
  const { broker } = open();
  await broker.close();
  await broker.close();
  await assert.rejects(broker.log.append('t', 'x'), (error) => error.code === 503);
  await assert.rejects(broker.queue.produce('q', 'x'), (error) => error.code === 503);
  await assert.rejects(
    broker.queue.consume('q', () => {}),
    (error) => error.code === 503,
  );
  await assert.rejects(
    broker.direct.listen('a', () => {}),
    (error) => error.code === 503,
  );
  await assert.rejects(broker.direct.send('a', 'x'), (error) => error.code === 503);
});

test('nats broker: a feed resumes across instances on the shared stream', async (t) => {
  const { world, broker, close } = open();
  t.after(close);
  const other = createNatsBroker({ ...world, logger: quiet });
  const topic = unique('feed');
  const first = await broker.log.append(topic, 'one');
  await broker.log.append(topic, 'two');
  const resumed = await collect(other.log.read(topic, { after: first }), 1, { timeout: 4000 });
  assert.deepStrictEqual(
    resumed.map((entry) => entry.value),
    ['two'],
  );
});

test('nats broker: ackWait is refused at construction, deadLetter at consume', async () => {
  const world = createFakeNats();
  // ackWait becomes nanoseconds in the consumer config: a string from the
  // environment multiplied is NaN, refused by the server at the first
  // consume() rather than here.
  for (const ackWait of [0, -1, 1.5, '30000']) {
    assert.throws(() => createNatsBroker({ ...world, logger: quiet, ackWait }), /options\.ackWait/);
  }
  const broker = createNatsBroker({ ...world, logger: quiet });
  await assert.rejects(
    broker.queue.consume('q', () => {}, { deadLetter: '' }),
    /deadLetter must be a queue name or null/,
  );
  await broker.close();
});

test('nats broker: close() stops its queue consumers', async (t) => {
  const timersPromises = require('node:timers/promises');
  const { world, broker } = open();
  const other = createNatsBroker({ ...world, logger: quiet });
  t.after(() => other.close());
  const queue = `closing-${Date.now().toString(36)}`;
  const seen = [];
  const consumer = await broker.queue.consume(queue, (delivery) => {
    seen.push(delivery.body);
    return delivery.ack();
  });
  await other.queue.produce(queue, 'before');
  await waitFor(() => seen.length === 1);
  // The pull in flight used to outlive close(): a message arriving inside
  // it was still dispatched, and the keepalives kept ticking.
  await broker.close();
  await other.queue.produce(queue, 'after');
  await timersPromises.setTimeout(80);
  assert.deepStrictEqual(seen, ['before'], 'a closed broker takes no more deliveries');
  assert.strictEqual(consumer.healthy, false);
  await consumer.stop();
});

test('nats broker: prefetch is per instance, maxAckPending is the group cap', async (t) => {
  // Two instances of the same group, prefetch 4 each: eight in flight
  // between them. The prefetch used to be written into the durable's
  // max_ack_pending — the GROUP's cap — so the fleet held four, not eight.
  const world = createFakeNats();
  const a = createNatsBroker({ ...world, logger: quiet, ackWait: 2000 });
  const b = createNatsBroker({ ...world, logger: quiet, ackWait: 2000 });
  t.after(() => Promise.all([a.close(), b.close()]));
  const name = unique('prefetch');
  const held = [];
  const handler = async (delivery) => {
    await new Promise((resolve) => held.push({ resolve, delivery }));
  };
  const first = await a.queue.consume(name, handler, { prefetch: 4 });
  const second = await b.queue.consume(name, handler, { prefetch: 4 });
  t.after(() => Promise.all([first.stop(), second.stop()]));
  for (let i = 0; i < 40; i++) await a.queue.produce(name, `work-${i}`);
  await waitFor(() => held.length === 8, { timeout: 3000 });
  await timers.setTimeout(100);
  assert.strictEqual(held.length, 8, 'each instance holds its own prefetch');
  const [fake] = world.server.streams.get(`wrpc_q_${name}`).consumers.values();
  assert.strictEqual(fake.config.max_ack_pending, undefined, 'the group cap is not set unless asked for');
  // A settlement frees one slot on the instance that held it.
  const { resolve, delivery } = held.shift();
  await delivery.ack();
  resolve();
  await waitFor(() => held.length === 8, { timeout: 3000 });
  for (const entry of held.splice(0)) {
    await entry.delivery.ack();
    entry.resolve();
  }
});

test('nats broker: maxAckPending is the durable consumer config, validated at construction', async (t) => {
  const world = createFakeNats();
  for (const maxAckPending of [0, -1, 1.5, '64']) {
    assert.throws(() => createNatsBroker({ ...world, logger: quiet, maxAckPending }), /options\.maxAckPending/);
  }
  const broker = createNatsBroker({ ...world, logger: quiet, maxAckPending: 64 });
  t.after(() => broker.close());
  const name = unique('cap');
  const consumer = await broker.queue.consume(name, (delivery) => delivery.ack(), { prefetch: 4 });
  t.after(() => consumer.stop());
  const [fake] = world.server.streams.get(`wrpc_q_${name}`).consumers.values();
  assert.strictEqual(fake.config.max_ack_pending, 64);
});

test('nats broker: a durable that already exists with other settings is served as is, and said once', async (t) => {
  const world = createFakeNats();
  const entries = [];
  const logger = {
    level: 'debug',
    child: () => logger,
    warn: (entry) => entries.push(entry),
    info: () => {},
    debug: () => {},
    error: () => {},
  };
  const older = createNatsBroker({ ...world, logger: quiet, ackWait: 5000, maxAckPending: 16 });
  const newer = createNatsBroker({ ...world, logger, ackWait: 3000, maxAckPending: 64 });
  t.after(() => Promise.all([older.close(), newer.close()]));
  const name = unique('drift');
  const first = await older.queue.consume(name, (delivery) => delivery.ack());
  t.after(() => first.stop());
  const second = await newer.queue.consume(name, (delivery) => delivery.ack());
  t.after(() => second.stop());
  const drift = entries.find((entry) => entry.event === 'broker.nats.consumer.config');
  assert.ok(drift, 'the configuration drift is logged');
  assert.strictEqual(drift.queue, name);
  assert.strictEqual(drift.ackWait, 5000);
  assert.strictEqual(drift.maxAckPending, 16);
});

test('nats broker: a paused consumer keeps the leases of what it still holds', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] });
  const { world, broker, close } = open({ ackWait: 300 });
  t.after(close);
  const name = unique('paused');
  const seen = [];
  let release;
  const consumer = await broker.queue.consume(name, async (delivery) => {
    seen.push(delivery.attempt);
    await new Promise((resolve) => (release = resolve));
    await delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'work');
  await waitFor(() => seen.length === 1, { timeout: 3000 });
  const [fake] = world.server.streams.get(`wrpc_q_${name}`).consumers.values();
  // A draining node pauses with the message still in its handler. The
  // keepalives used to stop here, and JetStream redelivered the message
  // to a healthy member while this one was still working on it.
  await consumer.pause();
  for (let i = 0; i < 7; i++) {
    t.mock.timers.tick(100);
    await timers.setTimeout(10);
  }
  assert.ok(fake.workingCalls >= 2, `working() was called ${fake.workingCalls} times under pause`);
  assert.deepStrictEqual(seen, [1], 'never redelivered while paused');
  release();
  await waitFor(() => fake.pending.size === 0, { timeout: 2000 });
  // A stop lets the leases go: nothing is kept alive after it.
  await consumer.stop();
  assert.strictEqual(fake.pending.size, 0);
});

test('nats broker: a reader deletes its consumer when the read is done', async (t) => {
  const { world, broker, close } = open();
  t.after(close);
  const topic = unique('readers');
  for (let i = 0; i < 5; i++) await broker.log.append(topic, `entry-${i}`);
  const stream = world.server.streams.get(`wrpc_log_${topic}`);
  // A catch-up page: the consumer is gone once the page is.
  const caught = await collect(broker.log.read(topic, { from: 'earliest' }), 5, { timeout: 4000 });
  assert.strictEqual(caught.length, 5);
  await waitFor(() => stream.ephemeral.size === 0, { timeout: 2000 });
  // A live tail: the consumer is gone once the tail is closed.
  const controller = new AbortController();
  const tail = broker.log.read(topic, { from: 'latest', signal: controller.signal });
  const iterator = tail[Symbol.asyncIterator]();
  const pending = iterator.next();
  await waitFor(() => stream.ephemeral.size === 1, { timeout: 2000 });
  const config = Array.from(stream.ephemeral)[0].config;
  assert.strictEqual(config.inactive_threshold, 30_000 * 1_000_000, 'the server reaps a reader that died');
  controller.abort();
  await pending.catch(() => {});
  await iterator.return?.().catch(() => {});
  await waitFor(() => stream.ephemeral.size === 0, { timeout: 2000 });
});
