'use strict';

// Telemetry on a peer, against the real OpenTelemetry SDK exporting into
// memory (the same rig as tests/telemetry.test.js): server spans for the
// calls a peer answers, linked to the calling peer's client spans through
// the packet's traceparent; the connection gauge under the 'webrtc'
// transport; and the three rtc instruments — open links, redials, ICE
// restarts — over a failure that the redial cycle recovers from.

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

const { defineRouter, procedure } = require('../../index.js');
const { SCOPE_NAME } = require('../../src/telemetry/index.js');
const { WrpcPeer, PeerHost } = require('../../src/webrtc/index.js');
const { createFakeRtc } = require('./fakeRtc.js');
const { FakeSignalHub } = require('./fakeSignalHub.js');
const { within, waitFor } = require('./portContract.js');

const quiet = {
  log() {},
  info() {},
  warn() {},
  error() {},
  debug() {},
  child() {
    return this;
  },
};

const createTracing = () => {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  return { provider, tracer: provider.getTracer(SCOPE_NAME), spans: () => exporter.getFinishedSpans() };
};

const createMetrics = () => {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 3_600_000 });
  const provider = new MeterProvider({ readers: [reader] });
  // The exporter keeps every flush; the LAST batch is the current cumulative
  // snapshot, and asserting on the first would read the values as they
  // were at the first collect.
  const collect = async () => {
    await reader.forceFlush();
    const metrics = [];
    const batch = exporter.getMetrics().at(-1);
    for (const scope of batch?.scopeMetrics ?? []) metrics.push(...scope.metrics);
    return metrics;
  };
  return { provider, meter: provider.getMeter(SCOPE_NAME), collect };
};

const point = (metrics, name, match = () => true) =>
  metrics.find((metric) => metric.descriptor.name === name)?.dataPoints.find((entry) => match(entry.attributes));

const onceEvent = (emitter, name) => new Promise((resolve) => emitter.once(name, resolve));

// What real propagation needs and wrpc deliberately does not supply: an
// active context manager and a W3C propagator, both from the injected api.
const withOtelGlobals = (t) => {
  const manager = new AsyncLocalStorageContextManager().enable();
  otelApi.context.setGlobalContextManager(manager);
  otelApi.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  t.after(() => {
    otelApi.context.disable();
    otelApi.propagation.disable();
  });
};

const routerOf = () =>
  defineRouter({
    calc: {
      add: procedure({ handler: async (_ctx, { a, b }) => a + b }),
      boom: procedure({
        handler: async () => {
          throw Object.assign(new Error('nope'), { code: 418 });
        },
      }),
    },
  });

test('peer telemetry: spans on both ends of a link, joined by the traceparent', async (t) => {
  withOtelGlobals(t);
  const tracing = createTracing();
  const metrics = createMetrics();
  t.after(() => metrics.provider.shutdown());
  const telemetry = {
    tracer: tracing.tracer,
    meter: metrics.meter,
    propagation: otelApi.propagation,
    context: otelApi.context,
  };
  const fake = createFakeRtc();
  const hub = new FakeSignalHub();
  const peer = (id) =>
    new WrpcPeer({
      router: routerOf(),
      signaler: hub.signaler(id),
      rtc: fake.adapter,
      logger: quiet,
      telemetry,
      client: { heartbeat: false, telemetry, reconnect: { minDelay: 5, maxDelay: 20, jitter: false } },
      connectTimeout: 500,
      restartTimeout: 30,
      // A redial window wide enough for the metric collects between the
      // failure and the unmute to finish first: the recovery below must be
      // the first redial, not a later one racing the assertions.
      redial: { minDelay: 150, maxDelay: 150, jitter: false, retries: 5 },
    });
  const a = peer('a');
  const b = peer('b');
  t.after(() => {
    a.close();
    b.close();
    fake.world.close();
  });
  const ab = await within(a.connect('b'), 'open');
  await within(b.link('a').ready(), 'open');
  await ab.load('calc');
  assert.strictEqual(await ab.api.calc.add({ a: 2, b: 3 }), 5);
  await assert.rejects(ab.api.calc.boom(), (error) => error.code === 418);

  await t.test('the call a peer answered has a SERVER span under the webrtc transport', () => {
    const server = tracing.spans().find((span) => span.name === 'calc/add' && span.kind === 1);
    assert.ok(server, 'b traced the call it answered');
    assert.strictEqual(server.attributes['wrpc.status'], 'ok');
    assert.strictEqual(server.attributes['wrpc.transport'], 'webrtc');
    assert.strictEqual(server.attributes['rpc.service'], 'calc');
    const failed = tracing.spans().find((span) => span.name === 'calc/boom' && span.kind === 1);
    assert.strictEqual(failed.attributes['wrpc.status'], 'error');
    assert.strictEqual(failed.attributes['rpc.wrpc.status_code'], 418);
  });

  await t.test("the calling peer's CLIENT span is the server span's parent", () => {
    const client = tracing.spans().find((span) => span.name === 'calc/add' && span.kind === 2);
    const server = tracing.spans().find((span) => span.name === 'calc/add' && span.kind === 1);
    assert.ok(client, 'a traced the call it made');
    assert.strictEqual(server.spanContext().traceId, client.spanContext().traceId, 'one trace across the link');
    assert.strictEqual(server.parentSpanContext?.spanId ?? server.parentSpanId, client.spanContext().spanId);
  });

  await t.test('the connection gauge counts each direction of the link as a webrtc connection', async () => {
    const exported = await metrics.collect();
    const connections = point(exported, 'wrpc.server.connections', (attrs) => attrs['wrpc.transport'] === 'webrtc');
    assert.ok(connections, 'the gauge has a webrtc series');
    // One writer per peer, two peers sharing one meter: a's host holds b's
    // client and b's host holds a's — two live connections.
    assert.strictEqual(connections.value, 2);
    const links = point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'initiator');
    const responder = point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'responder');
    assert.strictEqual(links.value, 1);
    assert.strictEqual(responder.value, 1);
  });

  await t.test('a failure counts an ICE restart, a redial, and drops the gauge until the link is back', async () => {
    hub.mute('a');
    hub.mute('b');
    ab.link.pc.failIce();
    await waitFor(() => ab.link.state === 'failed', 'the restart failed');
    let exported = await metrics.collect();
    assert.strictEqual(point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'initiator').value, 0);
    assert.strictEqual(
      point(exported, 'wrpc.rtc.ice_restarts', (attrs) => attrs['wrpc.rtc.outcome'] === 'requested').value,
      2,
    );
    assert.strictEqual(
      point(exported, 'wrpc.rtc.ice_restarts', (attrs) => attrs['wrpc.rtc.outcome'] === 'failed').value,
      2,
    );
    assert.strictEqual(
      point(exported, 'wrpc.server.connections', (attrs) => attrs['wrpc.transport'] === 'webrtc').value,
      0,
    );
    const reconnected = onceEvent(ab, 'reconnect');
    hub.unmute('a');
    hub.unmute('b');
    await within(reconnected, 'redialled');
    await waitFor(() => b.link('a').state === 'open', 'the responder followed');
    exported = await metrics.collect();
    assert.strictEqual(point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'initiator').value, 1);
    assert.ok(point(exported, 'wrpc.rtc.redials', (attrs) => attrs['wrpc.rtc.role'] === 'initiator').value >= 1);
    assert.ok(point(exported, 'wrpc.rtc.redials', (attrs) => attrs['wrpc.rtc.role'] === 'responder').value >= 1);
    assert.strictEqual(
      point(exported, 'wrpc.server.connections', (attrs) => attrs['wrpc.transport'] === 'webrtc').value,
      2,
    );
  });

  await t.test('a goodbye takes the link out of the gauge exactly once', async () => {
    ab.close();
    await waitFor(() => b.links.size === 0, 'b saw the goodbye');
    const exported = await metrics.collect();
    assert.strictEqual(point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'initiator').value, 0);
    assert.strictEqual(point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'responder').value, 0);
    assert.strictEqual(
      point(exported, 'wrpc.server.connections', (attrs) => attrs['wrpc.transport'] === 'webrtc').value,
      0,
    );
  });
});

test('peer telemetry: a client-only peer still counts its links; a bare PeerHost exposes its writer', async (t) => {
  const metrics = createMetrics();
  t.after(() => metrics.provider.shutdown());
  const fake = createFakeRtc();
  const hub = new FakeSignalHub();
  const lone = new WrpcPeer({
    signaler: hub.signaler('lone'),
    rtc: fake.adapter,
    logger: quiet,
    telemetry: { meter: metrics.meter },
    client: { heartbeat: false },
  });
  const served = new WrpcPeer({
    router: routerOf(),
    signaler: hub.signaler('served'),
    rtc: fake.adapter,
    logger: quiet,
    client: { heartbeat: false },
  });
  t.after(() => {
    lone.close();
    served.close();
    fake.world.close();
  });
  await within(lone.connect('served'), 'open');
  const exported = await metrics.collect();
  assert.strictEqual(point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'initiator').value, 1);
  assert.strictEqual(point(exported, 'wrpc.server.connections'), undefined, 'no host, no connections');
  assert.strictEqual(served.host.otel.enabled, false, 'nothing injected: the disabled writer');
  const host = new PeerHost({ router: routerOf(), logger: quiet, telemetry: { meter: metrics.meter } });
  assert.strictEqual(host.otel.enabled, true);
});
