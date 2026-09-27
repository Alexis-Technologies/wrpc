'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure } = require('../../index.js');
const { MemoryBroker, brokerFeed } = require('../../broker.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');
const { quiet } = require('./support.js');
const { runFeedSpec } = require('./feedSpec.js');

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

test('brokerFeed: the feed spec over the MemoryBroker', async (t) => {
  await runFeedSpec(t, 'memory', {
    open: () => {
      const broker = new MemoryBroker({ logger: quiet });
      return {
        broker,
        close: () => broker.close(),
        trim: (topic, keep) => broker.trim(topic, keep),
        foreignId: () => 'another-epoch.1',
      };
    },
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

  // Bound to the topic: a feed on ANOTHER topic under the same secret
  // refuses the token — it used to accept it, and a reader could position
  // itself on any feed of the deployment with an id issued by one.
  const { a: elsewhere } = await bootPair(t, broker, { secret: 'feed-secret' }, 'invoices');
  const third = await connectClient(t, elsewhere.url);
  await third.load('orders');
  const crossed = subscribe(third, { lastEventId: signed });
  await waitFor(() => crossed.errors.length === 1);
  assert.strictEqual(crossed.errors[0].code, 400);
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
  // Driven directly against the reference broker's retention: it overtakes
  // a reader that stopped pulling (a broker with a live tail buffers what
  // the reader has not pulled — see the note in feedSpec.js).
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
