'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure } = require('../../index.js');
const { MemoryBroker, brokerFeed } = require('../../broker.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');
const { quiet } = require('./support.js');

// Two instances on one broker — two processes behind a balancer, in
// production. Each serves the same feed procedure.
const bootPair = async (t, broker, feedOptions = {}, topic = 'orders') => {
  const router = defineRouter({
    orders: {
      feed: procedure.subscription({ access: 'public', handler: brokerFeed(broker, topic, feedOptions) }),
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
  Promise.all(values.map((value) => broker.log.append('orders', JSON.stringify(value))));

test('brokerFeed: a client resumes on ANOTHER instance without a gap or a duplicate', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const { a, b } = await bootPair(t, broker);

  const first = await connectClient(t, a.url);
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

  const second = await connectClient(t, b.url);
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

test('brokerFeed: a fresh subscription reads latest by default, or earliest', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  await append(broker, { n: 'old' });
  const router = defineRouter({
    orders: {
      feed: procedure.subscription({ access: 'public', handler: brokerFeed(broker, 'orders') }),
      all: procedure.subscription({
        access: 'public',
        handler: brokerFeed(broker.log, 'orders', { from: 'earliest' }),
      }),
    },
  });
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('orders');
  const latest = [];
  const all = [];
  client.api.orders.feed.subscribe({}, { onData: (value) => latest.push(value.n) });
  client.api.orders.all.subscribe({}, { onData: (value) => all.push(value.n) });
  await waitFor(() => all.length === 1);
  await append(broker, { n: 'new' });
  await waitFor(() => latest.length === 1 && all.length === 2);
  assert.deepStrictEqual(latest, ['new']);
  assert.deepStrictEqual(all, ['old', 'new']);
});

test('brokerFeed: an unusable lastEventId ends the feed with a coded error, or snapshots through onGap', async (t) => {
  await t.test('without onGap: 400 for garbage, 410 for trimmed history', async (sub) => {
    const broker = new MemoryBroker({ logger: quiet, retention: { maxEntries: 2 } });
    sub.after(() => broker.close());
    const { a } = await bootPair(sub, broker);
    const client = await connectClient(sub, a.url);
    await client.load('orders');
    const ids = await append(broker, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 });

    const garbage = subscribe(client, { lastEventId: '{"$gt":0}' });
    const tooLong = subscribe(client, { lastEventId: `${broker.epoch}.${'1'.repeat(600)}` });
    const trimmed = subscribe(client, { lastEventId: ids[0] });
    const foreign = subscribe(client, { lastEventId: 'another-epoch.1' });
    await waitFor(() => [garbage, tooLong, trimmed, foreign].every((s) => s.errors.length === 1));
    assert.deepStrictEqual(
      [garbage, tooLong, trimmed, foreign].map((s) => s.errors[0].code),
      [400, 400, 410, 410],
    );
  });

  await t.test('with onGap: the snapshot, then everything appended from the moment of the gap', async (sub) => {
    const broker = new MemoryBroker({ logger: quiet, retention: { maxEntries: 2 } });
    sub.after(() => broker.close());
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
    // Retention 2 over four appends: the first two ids are gone.
    const ids = await append(broker, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 });
    const feed = subscribe(client, { lastEventId: ids[0] });
    await waitFor(() => feed.values.length === 2);
    await append(broker, { n: 'after' });
    await waitFor(() => feed.values.length === 3);
    assert.deepStrictEqual(feed.values, [{ snapshot: true }, { n: 'during-snapshot' }, { n: 'after' }]);
    assert.deepStrictEqual(gaps, [{ lastEventId: ids[0], code: 410 }]);
    assert.deepStrictEqual(feed.errors, []);
  });

  await t.test('onGap may answer a single value, an async iterable, or nothing', async (sub) => {
    const broker = new MemoryBroker({ logger: quiet });
    sub.after(() => broker.close());
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
});

test('brokerFeed: signed ids refuse a position the feed never issued', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const { a, b } = await bootPair(t, broker, { secret: 'feed-secret' });
  const client = await connectClient(t, a.url);
  await client.load('orders');
  const feed = subscribe(client);
  await timers.setTimeout(20);
  const [raw] = await append(broker, { n: 1 });
  await waitFor(() => feed.values.length === 1);
  const signed = feed.handle.lastEventId;
  assert.match(signed, /^.+!.+$/);
  assert.notStrictEqual(signed, raw);

  const other = await connectClient(t, b.url);
  await other.load('orders');
  // The raw id is well-formed but unsigned: refused.
  const forged = subscribe(other, { lastEventId: raw });
  // The signed one resumes on the other instance.
  const honest = subscribe(other, { lastEventId: signed });
  await waitFor(() => forged.errors.length === 1);
  assert.strictEqual(forged.errors[0].code, 400);
  await timers.setTimeout(20);
  await append(broker, { n: 2 });
  await waitFor(() => honest.values.length === 1);
  assert.deepStrictEqual(honest.values, [{ n: 2 }]);
});

test('brokerFeed: map filters and reshapes; decode, dynamic topics, undecodable entries', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const warnings = [];
  const router = defineRouter({
    orders: {
      mine: procedure.subscription({
        access: 'public',
        handler: brokerFeed(broker, (_ctx, { tenant }) => `orders.${tenant}`, {
          map: (order, entry) => (order.hidden ? undefined : { ...order, tp: entry.headers.tp ?? null }),
        }),
      }),
      text: procedure.subscription({
        access: 'public',
        handler: brokerFeed(broker, 'plain', { decode: 'text' }),
      }),
      custom: procedure.subscription({
        access: 'public',
        handler: brokerFeed(broker, 'csv', { decode: (text) => text.split(',') }),
      }),
      broken: procedure.subscription({ access: 'public', handler: brokerFeed(broker, () => '') }),
    },
  });
  const { url } = await bootServer(t, {
    router,
    logger: { ...quiet, child: () => ({ ...quiet, warn: (entry) => warnings.push(entry) }) },
  });
  const client = await connectClient(t, url);
  await client.load('orders');
  const mine = [];
  const text = [];
  const custom = [];
  const brokenErrors = [];
  client.api.orders.mine.subscribe({ tenant: 't1' }, { onData: (value) => mine.push(value) });
  client.api.orders.text.subscribe({}, { onData: (value) => text.push(value) });
  client.api.orders.custom.subscribe({}, { onData: (value) => custom.push(value) });
  client.api.orders.broken.subscribe({}, { onError: (error) => brokenErrors.push(error) });
  await timers.setTimeout(30);
  await broker.log.append('orders.t2', JSON.stringify({ id: 'other tenant' }));
  await broker.log.append('orders.t1', JSON.stringify({ id: 'hidden', hidden: true }));
  await broker.log.append('orders.t1', '{not json');
  await broker.log.append('orders.t1', JSON.stringify({ id: 'visible' }), { headers: { tp: '00-trace' } });
  await broker.log.append('plain', 'just text');
  await broker.log.append('csv', 'a,b');
  await waitFor(() => mine.length === 1 && text.length === 1 && custom.length === 1 && brokenErrors.length === 1);
  assert.deepStrictEqual(mine, [{ id: 'visible', tp: '00-trace' }]);
  assert.deepStrictEqual(text, ['just text']);
  assert.deepStrictEqual(custom, [['a', 'b']]);
  assert.strictEqual(brokenErrors[0].code, 500);
  await waitFor(() => warnings.some((entry) => entry.event === 'feed.decode'));
});

test('brokerFeed: unsubscribing releases the broker read', async (t) => {
  const reads = { open: 0 };
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  // A log wrapper that counts live reads.
  const log = {
    append: (...rest) => broker.log.append(...rest),
    parseId: (text) => broker.log.parseId(text),
    read: (topic, options) => {
      const read = broker.log.read(topic, options);
      return {
        ready: read.ready,
        [Symbol.asyncIterator]: () => {
          reads.open++;
          const iterator = read[Symbol.asyncIterator]();
          let released = false;
          const release = () => {
            if (!released) reads.open--;
            released = true;
          };
          return {
            next: async () => {
              const result = await iterator.next();
              if (result.done) release();
              return result;
            },
            return: (value) => {
              release();
              return iterator.return(value);
            },
          };
        },
      };
    },
  };
  const router = defineRouter({
    orders: { feed: procedure.subscription({ access: 'public', handler: brokerFeed(log, 'orders') }) },
  });
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('orders');
  const feed = subscribe(client);
  await waitFor(() => reads.open === 1);
  feed.handle.unsubscribe();
  await waitFor(() => reads.open === 0);
});

test('brokerFeed: a feed a slow reader outgrew resumes through onGap mid-stream', async () => {
  // Driven directly: the retention overtakes a reader that stopped pulling.
  const broker = new MemoryBroker({ logger: quiet, retention: { maxEntries: 2 } });
  const gaps = [];
  const handler = brokerFeed(broker, 'orders', {
    onGap: (_ctx, _args, info) => {
      gaps.push(info.code);
      return { snapshot: true };
    },
  });
  const controller = new AbortController();
  const iterator = handler({}, {}, { signal: controller.signal });
  const pending = iterator.next();
  await timers.setTimeout(5);
  await append(broker, { n: 1 });
  assert.deepStrictEqual((await pending).value.data, { n: 1 });
  await append(broker, { n: 2 }, { n: 3 }, { n: 4 }, { n: 5 });
  assert.deepStrictEqual((await iterator.next()).value, { snapshot: true });
  const next = iterator.next();
  await timers.setTimeout(5);
  await append(broker, { n: 6 });
  assert.deepStrictEqual((await next).value.data, { n: 6 });
  assert.deepStrictEqual(gaps, [410]);
  controller.abort();
  assert.strictEqual((await iterator.next()).done, true);
  broker.close();
});

test('brokerFeed: options are validated', () => {
  const broker = new MemoryBroker({ logger: quiet });
  assert.throws(() => brokerFeed({ name: 'queue-only', close() {}, queue: broker.queue }, 't'), /no 'log' capability/);
  assert.throws(() => brokerFeed(broker, ''), /topic must be/);
  assert.throws(() => brokerFeed(broker, 't', { from: 'middle' }), /from must be/);
  assert.throws(() => brokerFeed(broker, 't', { decode: 'xml' }), /decode must be/);
  assert.throws(() => brokerFeed(broker, 't', { map: 1 }), /map must be/);
  assert.throws(() => brokerFeed(broker, 't', { onGap: 'snapshot' }), /onGap must be/);
  assert.throws(() => brokerFeed(broker, 't', { secret: '' }), /secret must be/);
  assert.throws(() => brokerFeed(broker, 't', { maxIdLength: 0 }), /maxIdLength/);
  broker.close();
});
