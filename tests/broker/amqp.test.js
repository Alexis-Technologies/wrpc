'use strict';

// The AMQP broker over the in-process fake (tests/broker/fakeAmqp.js): the
// same contract suites a real RabbitMQ runs in
// tests/broker/amqp.integration.test.js.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { createAmqpBroker } = require('../../broker/amqp.js');
const { isBroker } = require('../../broker.js');
const { createFakeAmqp } = require('./fakeAmqp.js');
const { runBackplaneContract } = require('./backplaneContract.js');
const { runLogContract } = require('./logContract.js');
const { runQueueContract } = require('./queueContract.js');
const { runDirectContract } = require('./directContract.js');
const { quiet, unique, waitFor, collect } = require('./support.js');

const open = (connection, extra = {}) =>
  createAmqpBroker({ connection, logger: quiet, queueType: 'classic', ...extra });

test('amqp broker (fake): backplane contract', async (t) => {
  await runBackplaneContract(t, 'amqp', {
    open: async () => {
      const connection = createFakeAmqp();
      return [open(connection).backplane, open(connection).backplane];
    },
    close: async () => {},
    settle: 10,
    timeout: 3000,
  });
});

test('amqp broker (fake): log contract', async (t) => {
  await runLogContract(t, 'amqp', {
    open: async () => {
      const connection = createFakeAmqp();
      // Every 40th stream delivery arrives 120 ms late: the pauses a real
      // broker's flow control puts in, which a catch-up page must not take
      // for the end of the stream (it did, at a 50 ms idle).
      connection.server.streamStall = { every: 40, ms: 120 };
      const broker = open(connection);
      const peer = open(connection);
      return {
        log: broker.log,
        peer: peer.log,
        close: async () => {
          await broker.close();
          await peer.close();
        },
        trim: async (topic, keep) => {
          const queue = connection.server.queues.get(`wrpc.log.${topic}`);
          if (queue) queue.messages.splice(0, Math.max(0, queue.messages.length - keep));
        },
        // The node the live reader's channel lives on goes away.
        endLiveRead: async (topic) => {
          const queue = connection.server.queues.get(`wrpc.log.${topic}`);
          for (const consumer of queue?.consumers.values() ?? []) connection.server.killChannel(consumer.channel, 320);
        },
      };
    },
    timeout: 4000,
  });
});

test('amqp broker (fake): queue contract', async (t) => {
  let connection = null;
  await runQueueContract(t, 'amqp', {
    open: async () => {
      connection = createFakeAmqp();
      const broker = open(connection);
      const peer = open(connection);
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
      connection.server.failures.publish = { error: new Error('channel closed by server'), times };
    },
    // The node the consumer's channel lives on goes away.
    breakConsumer: (name) => {
      const [held] = connection.server.queue(`wrpc.q.${name}`).consumers.values();
      connection.server.killChannel(held.channel, 320);
    },
    timeout: 4000,
    redelivery: 1000,
  });
});

test('amqp broker (fake): a consumer the server cancels is reported, unhealthy, and comes back on a fresh channel', async (t) => {
  const { recorder } = require('../helpers/recorder.js');
  const log = recorder();
  const connection = createFakeAmqp();
  const broker = createAmqpBroker({ connection, logger: log.writer });
  t.after(() => broker.close());
  const got = [];
  const consumer = await broker.queue.consume('cancelled', (delivery) => {
    got.push(delivery.body);
    delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce('cancelled', 'one');
  await waitFor(() => got.length === 1);
  assert.strictEqual(consumer.healthy, true);
  const [tag] = connection.server.queue('wrpc.q.cancelled').consumers.keys();
  assert.strictEqual(connection.server.cancelConsumer(tag), true);
  await waitFor(() => consumer.healthy === false, 'unhealthy on cancel');
  await waitFor(() => consumer.healthy === true, 'back on a fresh channel');
  await broker.queue.produce('cancelled', 'two');
  await waitFor(() => got.length === 2);
  assert.deepStrictEqual(got, ['one', 'two']);
  const line = log.find('broker.amqp.cancelled');
  assert.strictEqual(line.level, 'error');
  assert.strictEqual(line.queue, 'cancelled');
});

test('amqp broker (fake): a direct listener cancelled, or whose channel is gone, is unhealthy — and comes back', async (t) => {
  const { recorder } = require('../helpers/recorder.js');
  const log = recorder();
  const connection = createFakeAmqp();
  const broker = createAmqpBroker({ connection, logger: log.writer });
  t.after(() => broker.close());
  const heard = { cancel: [], kill: [] };
  const cancelled = await broker.direct.listen('svc-cancel', (message) => void heard.cancel.push(message.body), {
    group: 'svc-cancel',
  });
  const killed = await broker.direct.listen('svc-kill', (message) => void heard.kill.push(message.body), {
    group: 'svc-kill',
  });
  const kept = await broker.direct.listen(broker.direct.inbox(), () => {});
  assert.deepStrictEqual([cancelled.healthy, killed.healthy, kept.healthy], [true, true, true]);
  const consumersOf = (address) =>
    Array.from(connection.server.queues.entries()).find(([name]) => name.includes(address))[1].consumers;
  const [tag] = consumersOf('svc-cancel').keys();
  assert.strictEqual(connection.server.cancelConsumer(tag), true);
  await waitFor(() => cancelled.healthy === false, 'unhealthy on cancel');
  assert.strictEqual(log.all('broker.amqp.cancelled').length, 1);
  const [held] = consumersOf('svc-kill').values();
  connection.server.killChannel(held.channel, 320);
  await waitFor(() => killed.healthy === false, 'unhealthy on a closed channel');
  assert.strictEqual(kept.healthy, true, 'a listener on its own channel is untouched');
  // Both come back — they used to stay dead, and a send to them was taken
  // and delivered nowhere.
  await waitFor(() => cancelled.healthy && killed.healthy, { message: 'back', timeout: 4000 });
  await broker.direct.send('svc-cancel', 'after the cancel');
  await broker.direct.send('svc-kill', 'after the kill');
  await waitFor(() => heard.cancel.length === 1 && heard.kill.length === 1, 'delivered again');
  assert.deepStrictEqual(heard, { cancel: ['after the cancel'], kill: ['after the kill'] });
  await kept();
  assert.strictEqual(kept.healthy, false, 'stopped');
  await cancelled();
  await killed();
  assert.strictEqual(cancelled.healthy, false, 'stopped stays stopped');
});

test('amqp broker (fake): direct contract', async (t) => {
  await runDirectContract(t, 'amqp', {
    open: async () => {
      const connection = createFakeAmqp();
      const broker = open(connection);
      const peer = open(connection);
      return {
        direct: broker.direct,
        peer: peer.direct,
        close: async () => {
          await broker.close();
          await peer.close();
        },
      };
    },
    timeout: 4000,
    settle: 10,
  });
});

test('amqp broker: injection is validated structurally', async () => {
  assert.throws(() => createAmqpBroker({}), /amqplib connection/);
  assert.throws(() => createAmqpBroker({ connection: { createChannel() {} } }), /amqplib connection/);
  const broker = open(createFakeAmqp());
  assert.strictEqual(isBroker(broker), true);
  assert.strictEqual(broker.name, 'amqp');
  assert.match(broker.direct.inbox(), /^wrpc\.inbox\./);
  assert.throws(() => broker.backplane.subscribe('x', 'nope'), /handler must be a function/);
  assert.throws(() => broker.log.read('t', { from: 'middle' }), /from must be/);
  await assert.rejects(
    broker.queue.consume('q', () => {}, { prefetch: 0 }),
    /prefetch/,
  );
  await assert.rejects(broker.queue.consume('q', 'nope'), /onDelivery must be a function/);
  await assert.rejects(
    broker.direct.listen('', () => {}),
    /address must be a non-empty string/,
  );
  await broker.close();
});

test('amqp broker: a retry rides the TTL queue and is dead-lettered back', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  const name = unique('delayed');
  const seen = [];
  const consumer = await broker.queue.consume(name, (delivery) => {
    seen.push({ attempt: delivery.attempt, at: Date.now() });
    return delivery.attempt === 1 ? delivery.retry({ delay: 120 }) : delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'work');
  await waitFor(() => seen.length === 1, { timeout: 3000 });
  // It is parked on the retry queue, not the main one.
  await timers.setTimeout(30);
  assert.strictEqual(connection.server.queues.get(`wrpc.q.${name}.retry`).messages.length, 1);
  await waitFor(() => seen.length === 2, { timeout: 3000 });
  assert.ok(seen[1].at - seen[0].at >= 100);
  assert.strictEqual(connection.server.queues.get(`wrpc.q.${name}.retry`).messages.length, 0);
});

test('amqp broker: release is a requeue, which RabbitMQ 4 does not count', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  const name = unique('released');
  const seen = [];
  const consumer = await broker.queue.consume(name, (delivery) => {
    seen.push([delivery.attempt, delivery.redelivered]);
    return seen.length === 1 ? delivery.release() : delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'work');
  await waitFor(() => seen.length === 2, { timeout: 3000 });
  assert.deepStrictEqual(seen, [
    [1, false],
    [1, true],
  ]);
});

test('amqp broker: pause cancels the consumer but keeps held messages ackable', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  const name = unique('paused');
  const seen = [];
  const consumer = await broker.queue.consume(name, (delivery) => void seen.push(delivery), { prefetch: 4 });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'held');
  await waitFor(() => seen.length === 1, { timeout: 3000 });
  await consumer.pause();
  await broker.queue.produce(name, 'waits');
  await timers.setTimeout(80);
  assert.strictEqual(seen.length, 1);
  await seen[0].ack();
  await consumer.resume();
  await waitFor(() => seen.length === 2, { timeout: 3000 });
  assert.strictEqual(seen[1].body, 'waits');
  await seen[1].ack();
});

test('amqp broker: an unroutable send is refused with 503', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  await assert.rejects(broker.direct.send(unique('nobody'), 'x'), (error) => error.code === 503);
  // With a listener the same send goes through.
  const address = unique('svc');
  const seen = [];
  const stop = await broker.direct.listen(address, (message) => seen.push(message), { group: 'svc' });
  await broker.direct.send(address, 'hello', { timeout: 500 });
  await waitFor(() => seen.length === 1, { timeout: 3000 });
  await stop();
});

test('amqp broker: a feed resumes from a yielded offset, and a trimmed one answers 410', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  const topic = unique('feed');
  for (const value of ['1', '2', '3', '4']) await broker.log.append(topic, value);
  const first = await collect(broker.log.read(topic, { from: 'earliest' }), 2, { timeout: 3000 });
  const rest = await collect(broker.log.read(topic, { after: first[1].id }), 2, { timeout: 3000 });
  assert.deepStrictEqual(
    rest.map((entry) => entry.value),
    ['3', '4'],
  );
  // The retention overtakes the reader's position.
  const queue = connection.server.queues.get(`wrpc.log.${topic}`);
  queue.messages.splice(0, 3);
  const iterator = broker.log.read(topic, { after: first[0].id })[Symbol.asyncIterator]();
  await assert.rejects(iterator.next(), (error) => error.code === 410);
});

test('amqp broker: closing releases every channel and refuses new work', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  await broker.log.append(unique('t'), 'x');
  await broker.close();
  await broker.close();
  assert.ok(connection.channels.every((channel) => channel.closed));
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
  void t;
});

test('amqp broker: queue arguments are refused at construction, deadLetter at consume', async () => {
  const connection = createFakeAmqp();
  // These become queue ARGUMENTS: RabbitMQ refuses a redeclaration with
  // different ones with a channel-level error long after the typo.
  assert.throws(() => createAmqpBroker({ connection, logger: quiet, queueType: 'lazy' }), /options\.queueType/);
  for (const inboxTtl of [0, -5, 1.5, '60000']) {
    assert.throws(() => createAmqpBroker({ connection, logger: quiet, inboxTtl }), /options\.inboxTtl/);
  }
  for (const streamMaxBytes of [-1, 2.5, '1gb']) {
    assert.throws(() => createAmqpBroker({ connection, logger: quiet, streamMaxBytes }), /options\.streamMaxBytes/);
  }
  const broker = open(connection, { streamMaxBytes: 0 });
  await assert.rejects(
    broker.queue.consume('q', () => {}, { deadLetter: '' }),
    /deadLetter must be a queue name or null/,
  );
  await broker.close();
});

test('amqp broker: direct addresses share one exchange — a client inbox declares nothing that outlives it', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  const address = 'wrpc.svc';
  const seen = [];
  const stopService = await broker.direct.listen(address, (message) => void seen.push(message.correlationId), {
    group: address,
  });
  t.after(() => stopService());
  const before = connection.server.exchanges.size;
  // 200 clients, each with its own inbox, each sending one request and going
  // away: this used to leave 200 durable `wrpc.direct.<inbox>` exchanges.
  for (let i = 0; i < 200; i++) {
    const inbox = broker.direct.inbox();
    const replies = [];
    const stop = await broker.direct.listen(inbox, (message) => void replies.push(message.body));
    await broker.direct.send(address, 'hello', { correlationId: String(i), replyTo: inbox });
    await broker.direct.send(inbox, 'reply');
    await waitFor(() => replies.length === 1);
    await stop();
  }
  assert.strictEqual(seen.length, 200);
  assert.strictEqual(connection.server.exchanges.size, before, 'no exchange per address');
  assert.deepStrictEqual(
    Array.from(connection.server.exchanges.keys()).filter((exchange) => exchange.startsWith('wrpc.direct')),
    ['wrpc.direct'],
  );
  // Nobody bound under that routing key any more: the 503 still comes back.
  await assert.rejects(broker.direct.send('wrpc.nobody', 'x'), (error) => error.code === 503);
});

// Every line the adapter logs, for the recovery asserts.
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

test('amqp broker: a channel the server closed is not handed out again', async (t) => {
  const connection = createFakeAmqp();
  const broker = open(connection);
  t.after(() => broker.close());
  const other = open(connection, { streamMaxBytes: 4096 });
  t.after(() => other.close());
  const topic = unique('stream');
  assert.strictEqual(typeof (await broker.log.append(topic, 'one')), 'string');
  // The same stream declared with other arguments: RabbitMQ answers 406 and
  // closes the channel that asked — the memoized topology channel of `other`.
  await assert.rejects(other.log.append(topic, 'two'), /PRECONDITION_FAILED/);
  // That corpse used to stay memoized, and every later declaration on the
  // broker failed with "channel closed" until it was closed itself.
  assert.strictEqual(typeof (await other.log.append(unique('fresh'), 'three')), 'string');
  await other.queue.produce(unique('jobs'), 'work');
});

test('amqp broker: a consumer whose channel the server closed re-consumes with a backoff', async (t) => {
  const connection = createFakeAmqp();
  const { logger, entries } = recording();
  const broker = open(connection, { logger });
  t.after(() => broker.close());
  const name = unique('jobs');
  const seen = [];
  const consumer = await broker.queue.consume(name, (delivery) => {
    seen.push(delivery.body);
    return delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'a');
  await waitFor(() => seen.length === 1);
  // The node the channel lived on goes away: the consumer used to sit on
  // the closed channel forever, `healthy` still true, taking nothing.
  const [held] = connection.server.queue(`wrpc.q.${name}`).consumers.values();
  connection.server.killChannel(held.channel, 320);
  await waitFor(() => consumer.healthy === false);
  await broker.queue.produce(name, 'b');
  await waitFor(() => seen.length === 2, { timeout: 3000 });
  assert.strictEqual(consumer.healthy, true);
  assert.deepStrictEqual(
    entries.filter((entry) => entry.event === 'broker.amqp.consumer.closed').map((entry) => entry.queue),
    [name],
  );
  // The queue deleted under the consumer (the server cancels it): declared
  // again on the way back, on a fresh channel.
  const admin = await connection.createChannel();
  await admin.deleteQueue(`wrpc.q.${name}`);
  await waitFor(() => consumer.healthy === false);
  await waitFor(() => consumer.healthy === true, { timeout: 3000 });
  assert.ok(connection.server.queue(`wrpc.q.${name}`), 'the queue was declared again');
  await broker.queue.produce(name, 'c');
  await waitFor(() => seen.length === 3, { timeout: 3000 });
  assert.deepStrictEqual(
    entries.filter((entry) => entry.event === 'broker.amqp.cancelled').map((entry) => entry.queue),
    [name],
  );
});

test('amqp broker: a lost connection is reported once, marks everything unhealthy and stops the re-open loops', async (t) => {
  const connection = createFakeAmqp();
  const { logger, entries } = recording();
  const broker = open(connection, { logger });
  t.after(() => broker.close());
  const name = unique('jobs');
  const consumer = await broker.queue.consume(name, (delivery) => delivery.ack());
  const stopListening = await broker.direct.listen(broker.direct.inbox(), () => {});
  await broker.queue.produce(name, 'a');
  await connection.kill();
  await waitFor(() => consumer.healthy === false);
  await timers.setTimeout(400);
  assert.strictEqual(entries.filter((entry) => entry.event === 'broker.amqp.connection').length, 1);
  assert.strictEqual(entries.filter((entry) => entry.event === 'broker.amqp.consumer.reopen').length, 0);
  await assert.rejects(broker.queue.produce(name, 'b'), (error) => error.code === 503);
  await assert.rejects(broker.direct.send('wrpc.x', 'b'), (error) => error.code === 503);
  await stopListening();
  await consumer.stop();
});

test('amqp broker: the backplane multiplexes every room over one channel and one queue', async (t) => {
  // A server's channel_max: 16 is plenty for the adapter's fixed set of
  // channels, and far fewer than the rooms below.
  const connection = createFakeAmqp({ channelMax: 16 });
  const { logger, entries } = recording();
  const broker = open(connection, { logger });
  t.after(() => broker.close());
  const seen = new Map();
  const unsubscribes = [];
  for (let i = 0; i < 300; i++) {
    const room = `room-${i}`;
    seen.set(room, []);
    unsubscribes.push(await broker.backplane.subscribe(room, (message) => seen.get(room).push(message)));
  }
  assert.ok(connection.openChannels <= 16, `${connection.openChannels} channels for 300 rooms`);
  const bindings = () => connection.server.exchanges.get('wrpc.bp')?.size ?? 0;
  assert.strictEqual(bindings(), 300);
  broker.backplane.publish('room-7', 'seven');
  broker.backplane.publish('room-299', 'last');
  await waitFor(() => seen.get('room-7').length === 1 && seen.get('room-299').length === 1);
  assert.deepStrictEqual(seen.get('room-8'), []);
  // Two subscribers on one room share the binding; the room unbinds with its last one.
  const twice = [];
  const stopTwice = await broker.backplane.subscribe('room-7', (message) => twice.push(message));
  broker.backplane.publish('room-7', 'again');
  await waitFor(() => twice.length === 1 && seen.get('room-7').length === 2);
  await unsubscribes[7]();
  assert.strictEqual(bindings(), 300, 'still bound for the second subscriber');
  await stopTwice();
  assert.strictEqual(bindings(), 299);
  for (let i = 0; i < 150; i++) if (i !== 7) await unsubscribes[i]();
  assert.strictEqual(bindings(), 150);
  broker.backplane.publish('room-7', 'gone');
  await timers.setTimeout(20);
  assert.strictEqual(seen.get('room-7').length, 2, 'an unsubscribed room receives nothing');
  // The consumer channel closed under the live rooms: a fresh channel and
  // queue, every remaining room bound again, one line when it is back.
  const consumerChannel = connection.channels.find(
    (channel) =>
      !channel.closed &&
      channel.prefetchLimit === Infinity &&
      channel.confirm === false &&
      [...connection.server.queues.values()].some((queue) =>
        [...queue.consumers.values()].some(
          (consumer) => consumer.channel === channel && queue.name.startsWith('amq.gen'),
        ),
      ),
  );
  assert.ok(consumerChannel, 'the one backplane consumer channel');
  connection.server.killChannel(consumerChannel, 320);
  await waitFor(
    () => entries.some((entry) => entry.event === 'broker.amqp.backplane.rebind' && entry.level === 'warn'),
    { timeout: 3000 },
  );
  assert.strictEqual(bindings(), 150);
  broker.backplane.publish('room-200', 'after');
  await waitFor(() => seen.get('room-200').length === 1);
  assert.ok(connection.openChannels <= 16);
});

test('amqp broker: a settlement the broker keeps refusing hands the message back with a requeue', async (t) => {
  const connection = createFakeAmqp();
  const { logger, entries } = recording();
  const broker = open(connection, { logger });
  t.after(() => broker.close());
  const name = unique('refused');
  const seen = [];
  const consumer = await broker.queue.consume(name, async (delivery) => {
    seen.push([delivery.attempt, delivery.redelivered]);
    if (seen.length === 1) {
      // Every confirm publish is refused from here: the retry's copy cannot
      // be written. It used to be logged once with the original left
      // unacked; now the settlement is tried again, then the message goes
      // back to the queue.
      connection.server.failures.publish = { error: new Error('channel closed by server'), times: 10 };
      return delivery.retry({ delay: 0 });
    }
    connection.server.failures.publish = null;
    await delivery.ack();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'x');
  await waitFor(() => seen.length === 2, { timeout: 4000 });
  assert.deepStrictEqual(
    seen,
    [
      [1, false],
      [1, true],
    ],
    'requeued: the same attempt, redelivered',
  );
  const settle = entries.find((entry) => entry.event === 'broker.amqp.settle');
  assert.deepStrictEqual([settle?.queue, settle?.round], [name, 3]);
});

test('amqp broker: a retry on a consumer channel that died publishes one copy, not three', async (t) => {
  // amqplib's channel has no `closed`: the adapter used to read it, find it
  // undefined, and retry the publish AND the ack together while the ack kept
  // failing on the dead channel. The fake's channels hide it here as amqplib's do.
  const connection = createFakeAmqp();
  const createChannel = connection.createChannel.bind(connection);
  const hide = (channel) =>
    new Proxy(channel, {
      get: (target, key) => {
        if (key === 'closed') return undefined;
        const value = Reflect.get(target, key, target);
        // The fake keeps its state in private fields: its methods run on it.
        return typeof value === 'function' ? value.bind(target) : value;
      },
      has: (target, key) => key !== 'closed' && key in target,
    });
  connection.createChannel = async () => hide(await createChannel());
  const broker = open(connection);
  t.after(() => broker.close());
  const name = unique('jobs');
  let deliveries = 0;
  const consumer = await broker.queue.consume(name, (delivery) => {
    deliveries++;
    if (deliveries > 1) return delivery.ack();
    // The node the consumer's channel lived on goes, between the work and
    // the settlement.
    const [held] = connection.server.queue(`wrpc.q.${name}`).consumers.values();
    connection.server.killChannel(held.channel, 320);
    return delivery.retry();
  });
  t.after(() => consumer.stop());
  await broker.queue.produce(name, 'once');
  await timers.setTimeout(1500);
  // The first delivery, the original handed back by the dead channel, and
  // the retry's ONE copy: three. Three copies made it five.
  assert.ok(deliveries <= 3, `deliveries: ${deliveries}`);
});

// The fake is where a client's shape is easy to get wrong: FakeChannel grew
// a `closed` the adapter trusted, amqplib has none, and a dead channel
// published three copies of one delivery — invisible to every test here.
// So what the adapter reads off a channel or a connection is checked
// against amqplib's own classes (a devDependency), not against the fake.
test('amqp adapter: every member it reads off a channel or a connection exists on amqplib', (t) => {
  const { readFileSync } = require('node:fs');
  const path = require('node:path');
  let model;
  try {
    // Not an exported subpath: the module beside the package's main file.
    model = require(path.join(path.dirname(require.resolve('amqplib')), 'lib', 'channel_model.js'));
  } catch {
    return void t.skip('amqplib is not installed');
  }
  const source = readFileSync(path.join(__dirname, '..', '..', 'src', 'broker', 'amqp', 'index.js'), 'utf8')
    // Comments name members on purpose ("a `channel.closed` amqplib does not have").
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/\/\/.*$/gm, '');
  const read = (pattern) => new Set(Array.from(source.matchAll(pattern), (match) => match[1]));
  const channelMembers = read(/\b(?:channel|consumerChannel|opened|state\.channel|live\.channel)\??\.(\w+)/g);
  const connectionMembers = read(/\bconnection\??\.(\w+)/g);
  assert.ok(channelMembers.size >= 10, `members found: ${[...channelMembers]}`);
  for (const member of channelMembers) {
    assert.ok(member in model.ConfirmChannel.prototype, `channel.${member} is not on amqplib's ConfirmChannel`);
  }
  for (const member of connectionMembers) {
    assert.ok(member in model.ChannelModel.prototype, `connection.${member} is not on amqplib's ChannelModel`);
  }
});

test('amqp broker: a paused consumer stays paused across a re-open; the backplane hears again after a cancel', async (t) => {
  const connection = createFakeAmqp();
  const { logger, entries } = recording();
  const broker = open(connection, { logger });
  t.after(() => broker.close());
  const name = unique('jobs');
  const seen = [];
  const consumer = await broker.queue.consume(name, (delivery) => {
    seen.push(delivery.body);
    return delivery.ack();
  });
  t.after(() => consumer.stop());
  const [held] = connection.server.queue(`wrpc.q.${name}`).consumers.values();
  const channel = held.channel;
  await consumer.pause();
  connection.server.killChannel(channel, 320);
  await broker.queue.produce(name, 'during the pause');
  // Re-opened, and — paused — not consuming: it used to come back delivering.
  await timers.setTimeout(1500);
  assert.deepStrictEqual(seen, []);
  await consumer.resume();
  await waitFor(() => seen.length === 1, { message: 'resumed', timeout: 3000 });
  // The backplane: a cancel by the server used to leave it deaf, silently.
  const room = unique('room');
  const heard = [];
  await broker.backplane.subscribe(room, (message) => heard.push(message));
  const other = open(connection);
  t.after(() => other.close());
  other.backplane.publish(room, 'one');
  await waitFor(() => heard.length === 1);
  // The backplane's own queue: server-named, and consumed.
  const [, backplaneQueue] = Array.from(connection.server.queues.entries()).find(
    ([queueName, queue]) => queueName.startsWith('amq.gen') && queue.consumers.size > 0,
  );
  const [tag] = backplaneQueue.consumers.keys();
  connection.server.cancelConsumer(tag);
  await waitFor(() => entries.some((entry) => entry.event === 'broker.amqp.backplane.rebind'), {
    message: 'rebound',
    timeout: 4000,
  });
  other.backplane.publish(room, 'two');
  await waitFor(() => heard.length === 2, 'heard again');
  assert.deepStrictEqual(heard, ['one', 'two']);
});
