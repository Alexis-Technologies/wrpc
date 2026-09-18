'use strict';

// The instruments added in the observability sweep, asserted on VALUES
// rather than on existence — three of them replaced a series that was
// either never recorded or recorded with one value forever, and "the
// instrument exists" would have passed on that too.

const test = require('node:test');
const assert = require('node:assert');

const {
  MeterProvider,
  InMemoryMetricExporter,
  PeriodicExportingMetricReader,
  AggregationTemporality,
} = require('@opentelemetry/sdk-metrics');

const { Server, defineRouter, procedure, WrpcClient } = require('../index.js');

const createMetrics = () => {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 3_600_000 });
  const provider = new MeterProvider({ readers: [reader] });
  const collect = async () => {
    await reader.forceFlush();
    const metrics = [];
    for (const batch of exporter.getMetrics()) {
      for (const scope of batch.scopeMetrics) metrics.push(...scope.metrics);
    }
    return metrics;
  };
  return { meter: provider.getMeter('test'), collect };
};

const find = (metrics, name) => metrics.find((metric) => metric.descriptor.name === name);
const points = (metrics, name) => find(metrics, name)?.dataPoints ?? [];

test('sessions: every operation is recorded, not only restore', async (t) => {
  const { meter, collect } = createMetrics();
  const router = defineRouter({
    auth: {
      login: procedure({
        access: 'public',
        handler: async (context) => {
          context.client.startSession('tok-1', { user: 'a' });
          return 'in';
        },
      }),
      logout: procedure({
        access: 'public',
        handler: async (context) => {
          await context.client.finalizeSession();
          return 'out';
        },
      }),
    },
  });
  const server = new Server({
    router,
    logger: false,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    telemetry: { meter },
  });
  await server.listen();
  t.after(() => server.close());
  const { port } = server.address();
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    logger: false,
    reconnect: false,
    heartbeat: false,
  });
  t.after(() => void client.close());
  await client.load('auth');
  assert.strictEqual(await client.api.auth.login(), 'in');
  assert.strictEqual(await client.api.auth.logout(), 'out');

  const ops = new Set(points(await collect(), 'wrpc.server.sessions').map((p) => p.attributes['wrpc.session.op']));
  // `create` and `destroy` were asserted by a unit test that nothing in the
  // package could actually produce — the metric only ever said 'restore'.
  assert.ok(ops.has('create'), `no create operation recorded; saw ${[...ops]}`);
  assert.ok(ops.has('destroy'), `no destroy operation recorded; saw ${[...ops]}`);
});

test('streams: the send direction exists, so the attribute is no longer one-valued', async (t) => {
  const { meter, collect } = createMetrics();
  const router = defineRouter({
    files: {
      download: procedure({
        access: 'public',
        handler: async (context) => {
          const stream = context.client.createStream('report.bin', 6);
          stream.write(Buffer.from('abcdef'));
          stream.end();
          return { id: stream.id };
        },
      }),
    },
  });
  const server = new Server({
    router,
    logger: false,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    telemetry: { meter },
  });
  await server.listen();
  t.after(() => server.close());
  const { port } = server.address();
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    logger: false,
    reconnect: false,
    heartbeat: false,
  });
  t.after(() => void client.close());
  await client.load('files');
  await client.api.files.download();
  await new Promise((resolve) => setTimeout(resolve, 60));

  const directions = points(await collect(), 'wrpc.server.stream.bytes').map(
    (p) => p.attributes['wrpc.stream.direction'],
  );
  assert.ok(directions.includes('send'), `wrpc.stream.direction still has no 'send'; saw ${directions}`);
});

test('router: queue wait is measurable on its own, not folded into call duration', async (t) => {
  const { meter, collect } = createMetrics();
  const router = defineRouter({
    slow: {
      work: procedure({
        access: 'public',
        // One at a time, so the second and third calls must wait.
        queue: { concurrency: 1, size: 10 },
        handler: () => new Promise((resolve) => setTimeout(() => resolve(1), 40)),
      }),
    },
  });
  const server = new Server({
    router,
    logger: false,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    telemetry: { meter },
  });
  await server.listen();
  t.after(() => server.close());
  const { port } = server.address();
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    logger: false,
    reconnect: false,
    heartbeat: false,
  });
  t.after(() => void client.close());
  await client.load('slow');
  await Promise.all([client.api.slow.work(), client.api.slow.work(), client.api.slow.work()]);

  const metrics = await collect();
  const wait = find(metrics, 'wrpc.server.queue.wait');
  assert.ok(wait, 'queue wait was never recorded');
  assert.ok(wait.dataPoints[0].value.count >= 3, 'every queued call contributes a sample');
  // A saturated queue and a slow handler are opposite problems; the whole
  // point is that the waiting is visible apart from the running.
  assert.ok(wait.dataPoints[0].value.max >= 30, 'the wait is real time, not zero');
  const depth = find(metrics, 'wrpc.server.queue.depth');
  assert.ok(depth, 'queue depth was never recorded');
  assert.strictEqual(depth.dataPoints[0].value, 0, 'the gauge returns to zero once the queue drains');
});
