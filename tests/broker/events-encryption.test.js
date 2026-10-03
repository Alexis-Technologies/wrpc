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
  await waitFor(() => warnings.filter((w) => w.event === 'broker.feed.refused').length === 3);
  assert.deepStrictEqual(
    warnings
      .filter((w) => w.event === 'broker.feed.refused')
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

// Every level, for the aggregation asserts.
const allLogs = () => {
  const entries = [];
  const at = (level) => (entry) => entries.push({ level, ...entry });
  const logger = {
    log() {},
    info: at('info'),
    debug: at('debug'),
    error: at('error'),
    warn: at('warn'),
    child: () => logger,
  };
  return { logger, entries };
};

test('events encryption: a consumer retries a delivery under a key id it does not hold, then dead-letters it', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  // The publisher already rotated to k2; this consumer's ring has no k2.
  const k1 = generateKey();
  const k2 = generateKey();
  const publisher = publisherOf(t, broker, { encryption: { keys: { current: 'k2', ring: { k1, k2 } } } });
  const calls = [];
  const router = defineRouter({
    billing: {
      consumes: {
        charges: procedure({
          access: 'public',
          consume: { retry: { attempts: 3, backoff: { base: 5, max: 10, jitter: false } }, deadLetter: 'charges.dead' },
          handler: async (_ctx, args) => void calls.push(args),
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
  const { logger, entries } = allLogs();
  const consumers = await attachConsumers(
    rpc,
    broker,
    {},
    { encryption: { keys: { current: 'k1', ring: { k1 } } }, logger },
  );
  t.after(() => consumers.stop());
  await publisher.publish('orders.v1/charged', { id: 'o-1' });
  await waitFor(() => dead.length === 1);
  assert.deepStrictEqual(calls, []);
  const refused = entries.filter((entry) => entry.event === 'broker.refused');
  assert.deepStrictEqual(
    refused.map((entry) => [entry.reason, entry.attempt]),
    [
      ['kid', 1],
      ['kid', 2],
      ['kid', 3],
    ],
    'retried to the binding attempts as a 503, not dead on the first refusal',
  );
  assert.match(dead[0].headers['x-wrpc-dead-reason'], /^503 Sealed delivery refused: unknown key id/);
  assert.strictEqual(dead[0].headers['x-wrpc-attempt'], '3');
});

test('events encryption: a key provider that throws once is retried, not dead-lettered', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const k1 = generateKey();
  const publisher = publisherOf(t, broker, { encryption: { keys: { current: 'k1', ring: { k1 } } } });
  const calls = [];
  const router = defineRouter({
    billing: {
      consumes: {
        charges: procedure({
          access: 'public',
          consume: { retry: { attempts: 3, backoff: { base: 5, max: 10, jitter: false } }, deadLetter: 'charges.dead' },
          handler: async (_ctx, args) => void calls.push(args),
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
  // The vault the provider reads blips once: the first open throws.
  let blips = 1;
  const provider = {
    current: () => 'k1',
    get: (kid) => {
      if (blips-- > 0) throw new Error('vault unreachable');
      return kid === 'k1' ? k1 : null;
    },
  };
  const consumers = await attachConsumers(rpc, broker, {}, { encryption: { keys: provider }, logger: quiet });
  t.after(() => consumers.stop());
  await publisher.publish('orders.v1/charged', { id: 'o-2' });
  // It went to the dead-letter queue on that first failure: 400, never handled.
  await waitFor(() => calls.length === 1);
  assert.deepStrictEqual(calls, [{ id: 'o-2' }]);
  assert.deepStrictEqual(dead, []);
});

test('events encryption: a feed logs a burst of refusals once per reason, with the count, and summarizes at the end', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const keys = generateKey();
  // Five plaintext entries where none is accepted, then one the feed opens.
  for (let i = 0; i < 5; i++) await broker.log.append('orders', JSON.stringify({ forged: i }), { headers: {} });
  const publisher = publisherOf(t, broker, { encryption: { keys } });
  await publisher.publish('orders.v1/created', { id: 'o-1' });
  const { logger, entries } = allLogs();
  const router = defineRouter({
    orders: {
      feed: procedure.subscription({
        access: 'public',
        handler: brokerFeed(broker, 'orders', { from: 'earliest', encryption: { keys } }),
      }),
    },
  });
  const { url } = await bootServer(t, { router, logger });
  const client = await connectClient(t, url);
  await client.load('orders');
  const orders = [];
  const subscription = client.api.orders.feed.subscribe({}, { onData: (value) => orders.push(value) });
  await waitFor(() => orders.length === 1);
  assert.deepStrictEqual(orders, [{ id: 'o-1' }]);
  const refused = () => entries.filter((entry) => entry.event === 'broker.feed.refused');
  await waitFor(() => refused().length === 5);
  assert.deepStrictEqual(
    refused().map((entry) => [entry.level, entry.reason, entry.count]),
    [
      ['warn', 'unsealed', 1],
      ['debug', 'unsealed', 2],
      ['debug', 'unsealed', 3],
      ['debug', 'unsealed', 4],
      ['debug', 'unsealed', 5],
    ],
    'one warning for the burst, the rest at debug with the running count',
  );
  await subscription.unsubscribe();
  await waitFor(() => refused().some((entry) => entry.summary === true));
  const summary = refused().find((entry) => entry.summary === true);
  assert.deepStrictEqual([summary.level, summary.reason, summary.count], ['info', 'unsealed', 5]);
});

test('events encryption: headers are strings whether the message was sealed or not', () => {
  const { createBrokerSealing } = require('../../src/broker/sealing.js');
  const keys = generateKey();
  const sealing = createBrokerSealing({ keys }, 'test', { layer: 'broker-log', replay: false, text: true });
  const sealed = sealing.seal('t', { n: 7, flag: true }, 'body');
  const opened = sealing.open('t', sealed);
  assert.deepStrictEqual({ ...opened.headers }, { n: '7', flag: 'true' });
  const plain = createBrokerSealing({ keys, seal: false, acceptPlaintext: true }, 'test', {
    layer: 'broker-log',
    replay: false,
    text: true,
  });
  assert.deepStrictEqual({ ...plain.seal('t', { n: 7, flag: true }, 'body').headers }, { n: '7', flag: 'true' });
});

test('events encryption: a dead letter stays sealed on the queue, opens for onDeadLetter, a re-drive and by hand', async (t) => {
  const { openSealedMessage } = require('../../broker.js');
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const keys = generateKey();
  const publisher = publisherOf(t, broker, { encryption: { keys } });
  const reviewed = [];
  const router = defineRouter({
    billing: {
      consumes: {
        charges: procedure({
          access: 'public',
          consume: { retry: false, deadLetter: 'charges.dead' },
          handler: async () => {
            throw Object.assign(new Error('card declined'), { code: 402 });
          },
        }),
      },
    },
    ops: {
      review: procedure({
        access: 'public',
        handler: async (ctx, args) => void reviewed.push({ args, attempt: ctx.callMeta.attempt }),
      }),
    },
  });
  const rpc = new RpcServer({ router, logger: quiet, sse: false });
  t.after(() => rpc.close());
  const graveyard = [];
  const drained = await broker.queue.consume('charges.dead', (delivery) => {
    graveyard.push(delivery);
    return delivery.ack();
  });
  const seen = [];
  const consumers = await attachConsumers(
    rpc,
    broker,
    {},
    { encryption: { keys }, onDeadLetter: (info) => seen.push({ code: info.code, opened: info.opened }) },
  );
  t.after(() => consumers.stop());
  await publisher.publish('orders.v1/charged', { id: 'o-1', note: SECRET }, { headers: { 'x-tenant': 'acme' } });
  await waitFor(() => graveyard.length === 1 && seen.length === 1);
  // The hook holds the plaintext; the queue holds the seal.
  assert.deepStrictEqual(seen[0].code, 402);
  assert.deepStrictEqual(JSON.parse(seen[0].opened.body), { id: 'o-1', note: SECRET });
  assert.strictEqual(seen[0].opened.headers['x-tenant'], 'acme');
  const dead = graveyard[0];
  assert.strictEqual(typeof dead.headers[HEADER_SEALED], 'string');
  assert.ok(!dead.body.includes('4111'), 'the dead letter is still sealed');
  assert.match(dead.headers['x-wrpc-dead-reason'], /^402/);
  // By hand, under the queue it was sealed for — and not under the DLQ's name.
  const opened = openSealedMessage({ keys }, { topic: 'charges', headers: dead.headers, body: dead.body });
  assert.deepStrictEqual(JSON.parse(opened.body), { id: 'o-1', note: SECRET });
  assert.strictEqual(opened.headers['x-tenant'], 'acme');
  assert.deepStrictEqual(
    openSealedMessage({ keys }, { topic: 'charges.dead', headers: dead.headers, body: dead.body }),
    {
      refused: 'open',
    },
  );
  assert.throws(() => openSealedMessage(null, { topic: 'charges', headers: {}, body: '' }), /encryption is required/);
  assert.throws(() => openSealedMessage({ keys }, { headers: {}, body: '' }), /topic must be/);
  // A re-drive binding on the dead-letter queue names the queue the messages were sealed for.
  await drained.stop();
  await broker.queue.produce('charges.dead', dead.body, { headers: dead.headers });
  const redrive = await attachConsumers(
    rpc,
    broker,
    { 'charges.dead': { target: 'ops/review', sealedFor: 'charges' } },
    { encryption: { keys }, auto: false },
  );
  t.after(() => redrive.stop());
  await waitFor(() => reviewed.length === 1);
  assert.deepStrictEqual(reviewed[0].args, { id: 'o-1', note: SECRET });
  await assert.rejects(
    attachConsumers(rpc, broker, { 'charges.dead': { target: 'ops/review', sealedFor: '' } }, { auto: false }),
    /sealedFor must be/,
  );
});
