'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const otelApi = require('@opentelemetry/api');
const { W3CTraceContextPropagator } = require('@opentelemetry/core');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace-base');
const {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} = require('@opentelemetry/sdk-metrics');

const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure } = require('../../index.js');
const { MemoryBroker, createPublisher, attachConsumers, brokerFeed } = require('../../broker.js');
const { SCOPE_NAME } = require('../../src/telemetry/index.js');
const { quiet, waitFor, collect } = require('./support.js');
const { bootServer, connectClient } = require('../helpers/server.js');

const units = (handlers = {}) => ({
  'orders.v1': {
    emits: {
      created: { data: { id: 'string' } },
      shipped: { data: { id: 'string' } },
    },
    place: procedure({ access: 'public', handler: handlers.place ?? (async () => {}) }),
  },
});

const boot = (t, options = {}) => {
  const broker = new MemoryBroker({ logger: quiet });
  const rpc = new RpcServer({
    router: defineRouter(options.units ?? units()),
    logger: quiet,
    sse: false,
    ...options.rpc,
  });
  t.after(async () => {
    await rpc.close();
    broker.close();
  });
  return { broker, rpc };
};

test('createPublisher: a declared event is appended to the log as JSON and returns its id', async (t) => {
  const { broker, rpc } = boot(t);
  const publisher = createPublisher(rpc, broker, { 'orders.v1/created': {} });
  assert.deepStrictEqual(publisher.events, ['orders.v1/created']);
  const read = broker.log.read('orders.v1.created', { from: 'latest' });
  await read.ready;
  const pending = collect(read, 1);
  const id = await publisher.publish('orders.v1/created', { id: 'o-1' }, { headers: { 'x-tenant': 't1' } });
  const [entry] = await pending;
  assert.strictEqual(entry.id, id);
  assert.deepStrictEqual(JSON.parse(entry.value), { id: 'o-1' });
  assert.strictEqual(entry.headers['x-tenant'], 't1');
});

test('createPublisher: to a queue, with a topic and a key', async (t) => {
  const { broker, rpc } = boot(t);
  const produced = [];
  const queue = {
    name: 'spy',
    produce: async (name, body, options) => {
      produced.push({ name, body, options });
      return broker.queue.produce(name, body, options);
    },
    consume: broker.queue.consume,
  };
  const publisher = createPublisher(
    rpc,
    { name: 'spy', close() {}, queue },
    {
      'orders.v1/shipped': { topic: 'shipping', key: (data) => data.id },
    },
  );
  const result = await publisher.publish('orders.v1/shipped', { id: 'o-9' });
  assert.strictEqual(result, undefined);
  await publisher.publish('orders.v1/shipped', { id: 'o-9' }, { key: 'override' });
  assert.deepStrictEqual(
    produced.map((entry) => [entry.name, entry.body, entry.options.key]),
    [
      ['shipping', '{"id":"o-9"}', 'o-9'],
      ['shipping', '{"id":"o-9"}', 'override'],
    ],
  );
  const staticKey = createPublisher(rpc, broker, { 'orders.v1/shipped': { to: 'queue', key: 'fixed' } });
  await staticKey.publish('orders.v1/shipped', { id: 'x' });
});

test('createPublisher: validation refuses a bad payload with 400, function or Standard Schema', async (t) => {
  const { broker, rpc } = boot(t);
  const schema = {
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: (value) =>
        typeof value?.id === 'string'
          ? { value: { id: value.id.toUpperCase() } }
          : { issues: [{ message: 'id must be a string', path: ['id'] }] },
    },
  };
  const publisher = createPublisher(rpc, broker, {
    'orders.v1/created': { validate: schema },
    'orders.v1/shipped': {
      validate: (value) => {
        if (!value.id) throw new Error('id required');
      },
    },
  });
  await assert.rejects(publisher.publish('orders.v1/created', { id: 1 }), (error) => {
    assert.strictEqual(error.code, 400);
    assert.match(error.message, /Invalid orders.v1\/created payload: id must be a string/);
    assert.ok(error.details.issues);
    return true;
  });
  await assert.rejects(publisher.publish('orders.v1/shipped', {}), (error) => error.code === 400);
  // A validator's returned value is what gets published.
  const read = broker.log.read('orders.v1.created', { from: 'latest' });
  await read.ready;
  const pending = collect(read, 1);
  await publisher.publish('orders.v1/created', { id: 'o-1' });
  assert.deepStrictEqual(JSON.parse((await pending)[0].value), { id: 'O-1' });
});

test('createPublisher: the table is checked against emits', async (t) => {
  const { broker, rpc } = boot(t);
  assert.throws(() => createPublisher(rpc, broker, { 'orders.v1/deleted': {} }), /declares no such event/);
  assert.throws(() => createPublisher(rpc, broker, { 'nope/created': {} }), /declares no such event/);
  assert.ok(createPublisher(rpc, broker, { 'orders.v1/deleted': {} }, { strict: false }));
  const cases = [
    [{ created: {} }, /an event name is 'unit\/event'/],
    [{ 'orders.v1/': {} }, /an event name is 'unit\/event'/],
    [{ 'orders.v1/created': { topic: '' } }, /topic must be/],
    [{ 'orders.v1/created': { to: 'direct' } }, /to must be 'log' or 'queue'/],
    [{ 'orders.v1/created': { key: 5 } }, /key must be/],
    [{ 'orders.v1/created': { validate: 'zod' } }, /validate must be/],
  ];
  for (const [table, message] of cases) assert.throws(() => createPublisher(rpc, broker, table), message);
  assert.throws(() => createPublisher(rpc, broker, []), /event table must be an object/);
  assert.throws(() => createPublisher(rpc, { close() {}, direct: broker.direct }, {}), /neither a log nor a queue/);
  assert.throws(
    () => createPublisher(rpc, { name: 'q', close() {}, queue: broker.queue }, { 'orders.v1/created': { to: 'log' } }),
    /no 'log' capability/,
  );
  const publisher = createPublisher(rpc, broker, {});
  await assert.rejects(publisher.publish('orders.v1/created', {}), /not in this publisher's table/);
});

test('createPublisher: a broker failure is counted and rethrown', async (t) => {
  const { rpc } = boot(t);
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 3_600_000 });
  const meter = new MeterProvider({ readers: [reader] }).getMeter(SCOPE_NAME);
  const metered = new RpcServer({ router: rpc.router, logger: quiet, sse: false, telemetry: { meter } });
  t.after(() => metered.close());
  const failing = {
    name: 'down',
    close() {},
    log: {
      append: async () => {
        throw Object.assign(new Error('broker unreachable'), { code: 503 });
      },
      read() {},
      parseId() {},
    },
  };
  const publisher = createPublisher(metered, failing, { 'orders.v1/created': {} });
  await assert.rejects(publisher.publish('orders.v1/created', { id: 'x' }), /broker unreachable/);
  await reader.forceFlush();
  const metrics = exporter.getMetrics().flatMap((batch) => batch.scopeMetrics.flatMap((scope) => scope.metrics));
  const published = metrics.find((metric) => metric.descriptor.name === 'wrpc.broker.published');
  assert.deepStrictEqual(published.dataPoints[0].attributes, {
    'messaging.system': 'down',
    'wrpc.broker.outcome': 'error',
  });
});

test('publish -> queue -> consumer: one trace, PRODUCER then CONSUMER, and a delivery metric', async (t) => {
  const manager = new AsyncLocalStorageContextManager().enable();
  otelApi.context.setGlobalContextManager(manager);
  otelApi.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  t.after(() => {
    otelApi.context.disable();
    otelApi.propagation.disable();
  });
  const spans = new InMemorySpanExporter();
  const tracer = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(spans)] }).getTracer(SCOPE_NAME);
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 3_600_000 });
  const meter = new MeterProvider({ readers: [reader] }).getMeter(SCOPE_NAME);
  const handled = [];
  const router = defineRouter({
    'orders.v1': {
      emits: { created: { data: { id: 'string' } } },
      consumes: {
        created: procedure({ access: 'public', handler: async (_ctx, args) => void handled.push(args) }),
      },
    },
  });
  const broker = new MemoryBroker({ logger: quiet });
  const rpc = new RpcServer({
    router,
    logger: quiet,
    sse: false,
    telemetry: { tracer, meter, propagation: otelApi.propagation, context: otelApi.context },
  });
  t.after(async () => {
    await rpc.close();
    broker.close();
  });
  const consumers = await attachConsumers(rpc, broker, { 'orders.v1/created': { queue: 'orders' } });
  const publisher = createPublisher(rpc, broker, { 'orders.v1/created': { to: 'queue', topic: 'orders' } });
  await publisher.publish('orders.v1/created', { id: 'o-1' });
  await waitFor(() => handled.length === 1);
  await waitFor(() => spans.getFinishedSpans().length === 2);
  const [produced, consumed] = spans.getFinishedSpans();
  assert.strictEqual(produced.name, 'orders publish');
  assert.strictEqual(produced.kind, otelApi.SpanKind.PRODUCER);
  assert.strictEqual(produced.attributes['messaging.destination.name'], 'orders');
  assert.strictEqual(consumed.name, 'orders.v1/consumes.created');
  assert.strictEqual(consumed.kind, otelApi.SpanKind.CONSUMER);
  assert.strictEqual(consumed.attributes['messaging.system'], 'memory');
  assert.strictEqual(consumed.attributes['wrpc.transport'], 'broker');
  assert.strictEqual(consumed.spanContext().traceId, produced.spanContext().traceId);
  assert.strictEqual(consumed.parentSpanContext?.spanId ?? consumed.parentSpanId, produced.spanContext().spanId);
  await reader.forceFlush();
  const metrics = exporter.getMetrics().flatMap((batch) => batch.scopeMetrics.flatMap((scope) => scope.metrics));
  const deliveries = metrics.find((metric) => metric.descriptor.name === 'wrpc.broker.deliveries');
  assert.deepStrictEqual(deliveries.dataPoints[0].attributes, {
    'messaging.system': 'memory',
    'wrpc.broker.outcome': 'ack',
  });
  await consumers.stop();
});

test('publisher + feed: what a handler publishes reaches a durable feed', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const wiring = {};
  const router = defineRouter({
    'orders.v1': {
      emits: { created: { data: { id: 'string' } } },
      place: procedure({
        access: 'public',
        handler: async (_ctx, { id }) => ({ eventId: await wiring.publisher.publish('orders.v1/created', { id }) }),
      }),
      feed: procedure.subscription({ access: 'public', handler: brokerFeed(broker, 'orders.v1.created') }),
    },
  });
  const { server, url } = await bootServer(t, { router });
  wiring.publisher = createPublisher(server, broker, { 'orders.v1/created': {} });
  const client = await connectClient(t, url);
  await client.load('orders.v1');
  const seen = [];
  const feed = client.api['orders.v1'].feed.subscribe({}, { onData: (value) => seen.push(value) });
  await new Promise((resolve) => setTimeout(resolve, 20));
  const { eventId } = await client.api['orders.v1'].place({ id: 'o-7' });
  await waitFor(() => seen.length === 1);
  assert.deepStrictEqual(seen, [{ id: 'o-7' }]);
  assert.strictEqual(feed.lastEventId, eventId);
});
