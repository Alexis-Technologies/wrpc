'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure } = require('../../index.js');
const { MemoryBroker, createPublisher, attachConsumers, brokerFeed } = require('../../broker.js');
const { generateKey } = require('../../encryption.js');
const { HEADER_SEALED } = require('../../src/broker/sealing.js');
const { quiet, waitFor, collect } = require('./support.js');
const { bootServer, connectClient } = require('../helpers/server.js');

const SECRET = 'not for whoever can read the topic: 4111 1111 1111 1111';
const fastRetry = { attempts: 2, delay: 1 };

const logs = () => {
  const warnings = [];
  const logger = {
    log() {},
    info() {},
    debug() {},
    error() {},
    warn: (entry) => warnings.push(entry),
    child: () => logger,
  };
  return { logger, warnings };
};

const publisherOf = (t, broker, options) => {
  const router = defineRouter({
    'orders.v1': {
      emits: { created: { data: { id: 'string' } }, charged: { data: { id: 'string' } } },
      place: procedure({ access: 'public', handler: async () => {} }),
    },
  });
  const rpc = new RpcServer({ router, logger: quiet, sse: false });
  t.after(() => rpc.close());
  return createPublisher(
    rpc,
    broker,
    {
      'orders.v1/created': { topic: 'orders' },
      'orders.v1/charged': { to: 'queue', topic: 'charges', key: (v) => v.id },
    },
    options,
  );
};

test('events encryption: what rests in the log is sealed — the value and its headers; the key stays for the broker', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const publisher = publisherOf(t, broker, { encryption: { keys: generateKey() } });
  const read = broker.log.read('orders', { from: 'latest' });
  await read.ready;
  const pending = collect(read, 1);
  await publisher.publish('orders.v1/created', { id: 'o-1', note: SECRET }, { headers: { 'x-tenant': 'acme' } });
  const [entry] = await pending;
  assert.strictEqual(typeof entry.value, 'string', 'a string, as every log keeps one');
  assert.match(entry.value, /^[A-Za-z0-9+/]+=*$/);
  assert.deepStrictEqual(Object.keys(entry.headers), [HEADER_SEALED]);
  for (const needle of ['4111', 'o-1', 'acme']) {
    assert.ok(!entry.value.includes(needle) && !Buffer.from(entry.value, 'base64').includes(needle), needle);
  }
  const produced = [];
  const spy = { name: 'spy', produce: async (...args) => void produced.push(args), consume: broker.queue.consume };
  const toQueue = publisherOf(
    t,
    { name: 'spy', queue: spy, log: broker.log, close() {} },
    { encryption: { keys: generateKey() } },
  );
  await toQueue.publish('orders.v1/charged', { id: 'o-2', note: SECRET });
  assert.strictEqual(produced[0][2].key, 'o-2', "the partition key is the broker's to read");
  assert.ok(!produced[0][1].includes('4111'));
});

test('events encryption: a feed opens what the publisher sealed, headers included — and skips what it cannot', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const keys = generateKey();
  const publisher = publisherOf(t, broker, { encryption: { keys } });
  const { logger, warnings } = logs();
  const router = defineRouter({
    orders: {
      feed: procedure.subscription({
        access: 'public',
        handler: brokerFeed(broker, 'orders', {
          from: 'earliest',
          encryption: { keys },
          map: (order, entry) => ({ ...order, tenant: entry.headers['x-tenant'] ?? null }),
        }),
      }),
      elsewhere: procedure.subscription({
        access: 'public',
        handler: brokerFeed(broker, 'invoices', { from: 'earliest', encryption: { keys } }),
      }),
    },
  });
  await publisher.publish('orders.v1/created', { id: 'o-1', note: SECRET }, { headers: { 'x-tenant': 'acme' } });
  // In the same topic: a plaintext entry, one under another key, and one lifted from this topic into another
  await broker.log.append('orders', JSON.stringify({ id: 'forged' }), { headers: {} });
  const stranger = publisherOf(t, broker, { encryption: { keys: generateKey() } });
  await stranger.publish('orders.v1/created', { id: 'stranger' });
  const read = broker.log.read('orders', { from: 'earliest' });
  await read.ready;
  const [sealed] = await collect(read, 1);
  await broker.log.append('invoices', sealed.value, { headers: sealed.headers });
  await publisher.publish('orders.v1/created', { id: 'o-2' });

  const { url } = await bootServer(t, { router, logger });
  const client = await connectClient(t, url);
  await client.load('orders');
  const orders = [];
  const moved = [];
  client.api.orders.feed.subscribe({}, { onData: (value) => orders.push(value) });
  client.api.orders.elsewhere.subscribe({}, { onData: (value) => moved.push(value) });
  await waitFor(() => orders.length === 2);
  assert.deepStrictEqual(orders, [
    { id: 'o-1', note: SECRET, tenant: 'acme' },
    { id: 'o-2', tenant: null },
  ]);
  await waitFor(() => warnings.filter((w) => w.event === 'feed.refused').length === 3);
  assert.deepStrictEqual(
    warnings
      .filter((w) => w.event === 'feed.refused')
      .map((w) => [w.topic, w.reason])
      .sort(),
    [
      ['invoices', 'open'],
      ['orders', 'open'],
      ['orders', 'unsealed'],
    ],
  );
  assert.deepStrictEqual(moved, [], 'an entry moved to another topic is not an entry of that topic');
});

test('events encryption: a consumer opens a sealed delivery — its credential headers too — and dead-letters what it cannot', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const keys = generateKey();
  const publisher = publisherOf(t, broker, { encryption: { keys } });
  const calls = [];
  const router = defineRouter({
    billing: {
      consumes: {
        charges: procedure({
          access: 'public',
          consume: { meta: ['x-tenant'], retry: fastRetry, deadLetter: 'charges.dead' },
          handler: async (ctx, args) => void calls.push({ args, tenant: ctx.callMeta['x-tenant'] }),
        }),
      },
    },
  });
  const rpc = new RpcServer({ router, logger: quiet, sse: false });
  t.after(() => rpc.close());
  const dead = [];
  const graveyard = await broker.queue.consume('charges.dead', (delivery) => {
    dead.push(delivery);
    return delivery.ack();
  });
  t.after(() => graveyard.stop());
  const { logger, warnings } = logs();
  const consumers = await attachConsumers(rpc, broker, {}, { encryption: { keys }, logger });
  t.after(() => consumers.stop());
  await publisher.publish('orders.v1/charged', { id: 'o-1', note: SECRET }, { headers: { 'x-tenant': 'acme' } });
  await broker.queue.produce('charges', JSON.stringify({ id: 'forged' }), { headers: {} });
  await waitFor(() => calls.length === 1 && dead.length === 1);
  assert.deepStrictEqual(calls, [{ args: { id: 'o-1', note: SECRET }, tenant: 'acme' }]);
  assert.match(dead[0].headers['x-wrpc-dead-reason'], /^400/);
  assert.deepStrictEqual(
    warnings.filter((w) => w.event === 'broker.refused').map((w) => [w.queue, w.reason]),
    [['charges', 'unsealed']],
  );
});

test('events encryption: the rollout and the validation', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const keys = generateKey();
  // Deploy 1: readers learn to open, the publisher still writes plaintext
  const publisher = publisherOf(t, broker, { encryption: { keys, seal: false, acceptPlaintext: true } });
  const read = broker.log.read('orders', { from: 'latest' });
  await read.ready;
  const pending = collect(read, 1);
  await publisher.publish('orders.v1/created', { id: 'o-1' });
  const [entry] = await pending;
  assert.deepStrictEqual(JSON.parse(entry.value), { id: 'o-1' });
  assert.strictEqual(entry.headers[HEADER_SEALED], undefined);
  assert.throws(
    () => publisherOf(t, broker, { encryption: true }),
    /createPublisher: options: encryption must be \{ keys/,
  );
  assert.throws(
    () => brokerFeed(broker, 'orders', { encryption: { keys: 'short' } }),
    /brokerFeed: options: encryption\.keys/,
  );
  const rpc = new RpcServer({
    router: defineRouter({ a: { b: procedure(async () => 1) } }),
    logger: quiet,
    sse: false,
  });
  t.after(() => rpc.close());
  await assert.rejects(
    attachConsumers(rpc, broker, {}, { encryption: {} }),
    /attachConsumers: options: encryption must be/,
  );
});
