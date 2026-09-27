'use strict';

// One behavioral specification of brokerFeed, several brokers.
//
// The feed's resume, gap and snapshot behaviour used to be tested on the
// MemoryBroker alone, and the reference and an adapter can disagree without
// anybody noticing (TopicTails positions a reader differently from the
// in-memory log did, once). So the scenarios are written once, here, and
// each broker test replays them: tests/broker/feed.test.js over the
// MemoryBroker, tests/broker/redis.test.js over the fake Redis server. The
// same shape as tests/adapters/spec.js — not a *.test.js, imported.
//
// harness.open() -> { broker, close(), trim(topic, keep), foreignId() }:
//   `trim` drops all but the newest `keep` entries of a topic (what the
//   broker's retention would do); `foreignId` is an id shaped like the
//   broker's that this log never minted (410).

const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure } = require('../../index.js');
const { brokerFeed } = require('../../broker.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const TOPIC = 'orders';

// Two instances on one broker — two processes behind a balancer, in
// production. Each serves the same feed procedure.
const bootPair = async (t, broker, feedOptions = {}) => {
  const router = defineRouter({
    orders: {
      feed: procedure.subscription({ access: 'public', handler: brokerFeed(broker, TOPIC, feedOptions) }),
    },
  });
  const a = await bootServer(t, { router, backplane: broker.backplane });
  const b = await bootServer(t, { router, backplane: broker.backplane });
  return { a, b };
};

const subscribe = (client, options = {}) => {
  const values = [];
  const errors = [];
  const handle = client.api.orders.feed.subscribe(
    {},
    {
      ...options,
      onData: (value) => values.push(value),
      onError: (error) => errors.push(error),
    },
  );
  return { handle, values, errors };
};

const append = (broker, ...values) =>
  Promise.all(values.map((value) => broker.log.append(TOPIC, JSON.stringify(value))));

const runFeedSpec = async (t, name, harness) => {
  const opened = async (sub) => {
    const env = await harness.open();
    sub.after(() => env.close());
    return env;
  };

  await t.test(`${name}: a client resumes on ANOTHER instance without a gap or a duplicate`, async (sub) => {
    const { broker } = await opened(sub);
    const { a, b } = await bootPair(sub, broker);
    const first = await connectClient(sub, a.url);
    await first.load('orders');
    const one = subscribe(first);
    await timers.setTimeout(20);
    await append(broker, { n: 1 }, { n: 2 });
    await waitFor(() => one.values.length === 2);
    const resumeFrom = one.handle.lastEventId;
    assert.strictEqual(typeof resumeFrom, 'string');
    one.handle.unsubscribe();
    await first.close();
    // Published while nobody was connected.
    await append(broker, { n: 3 }, { n: 4 });
    const second = await connectClient(sub, b.url);
    await second.load('orders');
    const two = subscribe(second, { lastEventId: resumeFrom });
    await waitFor(() => two.values.length === 2);
    await append(broker, { n: 5 });
    await waitFor(() => two.values.length === 3);
    assert.deepStrictEqual(
      [...one.values, ...two.values].map((value) => value.n),
      [1, 2, 3, 4, 5],
    );
    assert.deepStrictEqual(two.errors, []);
  });

  await t.test(`${name}: a fresh subscription reads latest by default, or earliest`, async (sub) => {
    const { broker } = await opened(sub);
    await append(broker, { n: 'old' });
    const router = defineRouter({
      orders: {
        feed: procedure.subscription({ access: 'public', handler: brokerFeed(broker, TOPIC) }),
        all: procedure.subscription({
          access: 'public',
          handler: brokerFeed(broker.log, TOPIC, { from: 'earliest' }),
        }),
      },
    });
    const { url } = await bootServer(sub, { router });
    const client = await connectClient(sub, url);
    await client.load('orders');
    const latest = [];
    const all = [];
    client.api.orders.feed.subscribe({}, { onData: (value) => latest.push(value.n) });
    client.api.orders.all.subscribe({}, { onData: (value) => all.push(value.n) });
    await waitFor(() => all.length === 1);
    await timers.setTimeout(20);
    await append(broker, { n: 'new' });
    await waitFor(() => latest.length === 1 && all.length === 2);
    assert.deepStrictEqual(latest, ['new']);
    assert.deepStrictEqual(all, ['old', 'new']);
  });

  await t.test(
    `${name}: without onGap, an unusable lastEventId ends the feed: 400 garbage, 410 trimmed or foreign`,
    async (sub) => {
      const env = await opened(sub);
      const { broker } = env;
      const { a } = await bootPair(sub, broker);
      const client = await connectClient(sub, a.url);
      await client.load('orders');
      const ids = await append(broker, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 });
      await env.trim(TOPIC, 2);
      const garbage = subscribe(client, { lastEventId: '{"$gt":0}' });
      const tooLong = subscribe(client, { lastEventId: 'x'.repeat(600) });
      const trimmed = subscribe(client, { lastEventId: ids[0] });
      const foreign = subscribe(client, { lastEventId: env.foreignId() });
      await waitFor(() => [garbage, tooLong, trimmed, foreign].every((s) => s.errors.length === 1));
      assert.deepStrictEqual(
        [garbage, tooLong, trimmed, foreign].map((s) => s.errors[0].code),
        [400, 400, 410, 410],
      );
    },
  );

  await t.test(
    `${name}: with onGap, the snapshot, then everything appended from the moment of the gap`,
    async (sub) => {
      const env = await opened(sub);
      const { broker } = env;
      const gaps = [];
      const { a } = await bootPair(sub, broker, {
        onGap: async (_ctx, _args, info) => {
          gaps.push(info);
          // Appended while the snapshot is being assembled: must not be lost.
          await append(broker, { n: 'during-snapshot' });
          return [{ snapshot: true }];
        },
      });
      const client = await connectClient(sub, a.url);
      await client.load('orders');
      const ids = await append(broker, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 });
      // Retention 2 over four appends: the first two ids are gone.
      await env.trim(TOPIC, 2);
      const feed = subscribe(client, { lastEventId: ids[0] });
      await waitFor(() => feed.values.length === 2);
      await append(broker, { n: 'after' });
      await waitFor(() => feed.values.length === 3);
      assert.deepStrictEqual(feed.values, [{ snapshot: true }, { n: 'during-snapshot' }, { n: 'after' }]);
      assert.deepStrictEqual(gaps, [{ lastEventId: ids[0], code: 410 }]);
      assert.deepStrictEqual(feed.errors, []);
    },
  );

  await t.test(`${name}: onGap may answer a single value, an async iterable, or nothing`, async (sub) => {
    const { broker } = await opened(sub);
    const answers = [
      () => ({ single: true }),
      () =>
        (async function* () {
          yield { streamed: 1 };
          yield { streamed: 2 };
        })(),
      () => undefined,
    ];
    let call = 0;
    const { a } = await bootPair(sub, broker, { onGap: () => answers[call++]() });
    const client = await connectClient(sub, a.url);
    await client.load('orders');
    const single = subscribe(client, { lastEventId: 'nope' });
    await waitFor(() => single.values.length === 1);
    const streamed = subscribe(client, { lastEventId: 'nope' });
    await waitFor(() => streamed.values.length === 2);
    const nothing = subscribe(client, { lastEventId: 'nope' });
    await timers.setTimeout(20);
    await append(broker, { n: 1 });
    await waitFor(() => nothing.values.length === 1);
    // All three stay live after their snapshot, so the append reaches each.
    await waitFor(() => single.values.length === 2 && streamed.values.length === 3);
    assert.deepStrictEqual(single.values, [{ single: true }, { n: 1 }]);
    assert.deepStrictEqual(streamed.values, [{ streamed: 1 }, { streamed: 2 }, { n: 1 }]);
    assert.deepStrictEqual(nothing.values, [{ n: 1 }]);
  });

  // Not here: "a slow reader the retention outgrew resumes through onGap
  // mid-stream". On a broker with a live tail the entries a reader has not
  // pulled yet already sit in the tail's buffer, so trimming the topic
  // creates no gap for it — the outgrow happens past the high-water mark,
  // which the log and tail contracts cover. The scenario is driven against
  // the reference broker's own retention in feed.test.js.
};

module.exports = { runFeedSpec };
