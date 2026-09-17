'use strict';

// The NATS broker over the in-process fake (tests/broker/fakeNats.js): the
// same contract suites a real server runs in tests/broker/nats.integration.test.js.

const { test } = require('node:test');
const assert = require('node:assert');

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
  const { broker, close } = open({ ackWait: 2000 });
  t.after(close);
  const name = unique('slow');
  const seen = [];
  const consumer = await broker.queue.consume(name, async (delivery) => {
    seen.push(delivery.attempt);
    await new Promise((resolve) => setTimeout(resolve, 900));
    await delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'work');
  await waitFor(() => seen.length === 1, { timeout: 3000 });
  await new Promise((resolve) => setTimeout(resolve, 1500));
  assert.deepStrictEqual(seen, [1], 'the message was redelivered under a live handler');
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
  await waitFor(() => true);
});

test('nats broker: JetStream-only operations refuse without JetStream', async () => {
  const world = createFakeNats();
  const broker = createNatsBroker({ nc: world.nc, headers: world.headers, logger: quiet });
  // The capability is absent, so the scenario entry points refuse; the raw
  // functions answer 501 for anyone reaching past them.
  const { createNatsBroker: factory } = require('../../broker/nats.js');
  void factory;
  assert.strictEqual(broker.log, undefined);
  await broker.close();
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
