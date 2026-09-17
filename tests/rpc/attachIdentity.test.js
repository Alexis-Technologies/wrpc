'use strict';

// The core seams the broker bindings stand on: RpcServer.attach giving a
// client an identity (a vouched-for pseudo-session, or a request restored
// through the token carrier), the 'draining' announcement, and the
// telemetry hooks a host-built client uses (spanKind/spanAttributes,
// withMessagingSpan, the broker instruments).

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
const { defineRouter, procedure } = require('../../src/rpc/router.js');
const { Emitter } = require('../../src/utils.js');
const { handleRpc } = require('../../src/rpc/dispatcher.js');
const { createServerTelemetry, SCOPE_NAME } = require('../../src/telemetry/index.js');
const { DISABLED, SPAN_KIND_PRODUCER, SPAN_KIND_CONSUMER } = require('../../src/telemetry/shared.js');
const { bearerTransport } = require('../../auth.js');
const { waitFor } = require('../helpers/server.js');

const quiet = { log() {}, info() {}, warn() {}, error() {}, debug() {} };

// The smallest inbound transport: captures what the dispatcher sends.
class CaptureTransport extends Emitter {
  kind = 'capture';
  source = 'capture';
  connection = true;
  sent = [];
  write() {
    return true;
  }
  send(packet) {
    this.sent.push(packet);
    return true;
  }
  error(code, { id }) {
    this.sent.push({ type: 'callback', id, error: { code } });
    return true;
  }
  close() {
    this.emit('close');
  }
}

const routerOf = (seen) =>
  defineRouter(
    {
      acct: {
        whoami: procedure({
          handler: async (ctx) => ({ session: ctx.session?.state ? { ...ctx.session.state } : null }),
        }),
      },
    },
    {
      hooks: {
        // The documented recipe: a hook awaits the restore it needs.
        onConnect: [
          async (client) => {
            await client.sessionReady;
            seen.push(client.session?.state ? { ...client.session.state } : null);
          },
        ],
      },
    },
  );

const call = async (client, transport, router, method) => {
  const before = transport.sent.length;
  await handleRpc(client, { type: 'call', id: `c${before}`, method, args: {} }, router);
  return transport.sent[before];
};

test('attach: a vouched-for session is in place before the onConnect hooks', async () => {
  const seen = [];
  const rpc = new RpcServer({ router: routerOf(seen), logger: quiet, sse: false });
  const transport = new CaptureTransport();
  const session = { token: 'svc', state: { service: 'billing' } };
  const client = rpc.attach(transport, { session });
  await client.ready;
  assert.deepStrictEqual(seen, [{ service: 'billing' }]);
  const answer = await call(client, transport, rpc.router, 'acct/whoami');
  assert.deepStrictEqual(answer.result, { session: { service: 'billing' } });
  await rpc.close();
});

test('attach: a request restores a real session through the token carrier', async () => {
  const seen = [];
  const rpc = new RpcServer({
    router: routerOf(seen),
    logger: quiet,
    sse: false,
    sessions: { transport: bearerTransport() },
  });
  const created = rpc.sessions.create(undefined, { user: 'ada' });
  await new Promise((resolve) => setTimeout(resolve, 5));
  const transport = new CaptureTransport();
  const client = rpc.attach(transport, {
    request: { headers: { authorization: `Bearer ${created.token}`, 'x-tenant': 't1' }, url: '/q?x=1' },
  });
  await client.ready;
  assert.deepStrictEqual(seen, [{ user: 'ada' }]);
  assert.strictEqual(client.meta.headers['x-tenant'], 't1');
  const answer = await call(client, transport, rpc.router, 'acct/whoami');
  assert.deepStrictEqual(answer.result, { session: { user: 'ada' } });

  // No token: anonymous, and a session procedure answers 403.
  const anonymousTransport = new CaptureTransport();
  const anonymous = rpc.attach(anonymousTransport, { request: {} });
  await anonymous.ready;
  const refused = await call(anonymous, anonymousTransport, rpc.router, 'acct/whoami');
  assert.strictEqual(refused.error.code, 403);
  await rpc.close();
});

test('attach: identity options are validated', () => {
  const rpc = new RpcServer({ router: routerOf([]), logger: quiet, sse: false });
  const transport = new CaptureTransport();
  assert.throws(() => rpc.attach(transport, { session: 'token' }), /session must be an object/);
  assert.throws(() => rpc.attach(transport, { session: [] }), /session must be an object/);
  assert.throws(() => rpc.attach(transport, { session: {}, request: {} }), /mutually exclusive/);
  assert.throws(() => rpc.attach(transport, { request: 'GET /' }), /request must be an object/);
  void rpc.close();
});

test("drain: 'draining' is announced once, only when draining actually starts", async () => {
  const rpc = new RpcServer({ router: routerOf([]), logger: quiet, sse: false });
  let announced = 0;
  rpc.on('draining', () => announced++);
  await rpc.drain();
  assert.strictEqual(announced, 0, 'a no-op drain announces nothing');
  await rpc.drain(10);
  await rpc.drain(10);
  assert.strictEqual(announced, 1);
  assert.strictEqual(rpc.draining, true);
  await rpc.close();
});

const createTracing = () => {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  return { tracer: provider.getTracer(SCOPE_NAME), spans: () => exporter.getFinishedSpans() };
};

test('telemetry: a host-built client chooses its span kind and adds attributes', async () => {
  const { tracer, spans } = createTracing();
  const otel = createServerTelemetry({ tracer });
  const client = {
    source: 'broker',
    persistent: true,
    transportKind: 'broker',
    spanKind: SPAN_KIND_CONSUMER,
    spanAttributes: { 'messaging.system': 'memory', 'messaging.destination.name': 'orders' },
  };
  otel.withSpan({ client, packet: { type: 'call', id: 'p' }, target: 'billing/charge' }, (handle) =>
    otel.endSpan(handle),
  );
  // An explicit kind still wins over the client's.
  otel.withSpan({ client, packet: { type: 'event' }, target: 'a/b', kind: 2 }, (handle) => otel.endSpan(handle));
  const [consumed, explicit] = spans();
  assert.strictEqual(consumed.kind, otelApi.SpanKind.CONSUMER);
  assert.strictEqual(consumed.attributes['messaging.system'], 'memory');
  assert.strictEqual(consumed.attributes['messaging.destination.name'], 'orders');
  assert.strictEqual(explicit.kind, otelApi.SpanKind.CLIENT);
});

test('telemetry: a messaging span is active for inject and parents on a carrier', async (t) => {
  const manager = new AsyncLocalStorageContextManager().enable();
  otelApi.context.setGlobalContextManager(manager);
  otelApi.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  t.after(() => {
    otelApi.context.disable();
    otelApi.propagation.disable();
  });
  const { tracer, spans } = createTracing();
  const otel = createServerTelemetry({ tracer, propagation: otelApi.propagation, context: otelApi.context });
  const headers = {};
  otel.withMessagingSpan({ name: 'orders publish', kind: SPAN_KIND_PRODUCER, attributes: { a: 1 } }, (handle) => {
    otel.inject(headers);
    otel.endSpan(handle);
  });
  assert.match(headers.tp, /^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/);
  otel.withMessagingSpan({ name: 'orders process', kind: SPAN_KIND_CONSUMER, carrier: headers }, (handle) =>
    otel.endSpan(handle),
  );
  const [produced, consumed] = spans();
  assert.strictEqual(produced.kind, otelApi.SpanKind.PRODUCER);
  assert.strictEqual(consumed.parentSpanContext?.spanId ?? consumed.parentSpanId, produced.spanContext().spanId);
  assert.strictEqual(consumed.spanContext().traceId, produced.spanContext().traceId);

  // Without a tracer the callback still runs, with a null-span handle.
  const meterOnly = createServerTelemetry({ meter: new MeterProvider().getMeter(SCOPE_NAME) });
  assert.strictEqual(
    meterOnly.withMessagingSpan({ name: 'x' }, (handle) => handle.span),
    null,
  );
});

test('telemetry: broker instruments count by system and outcome', async () => {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 3_600_000 });
  const provider = new MeterProvider({ readers: [reader] });
  const otel = createServerTelemetry({ meter: provider.getMeter(SCOPE_NAME) });
  otel.recordBrokerDelivery('memory', 'ack');
  otel.recordBrokerDelivery('memory', 'ack');
  otel.recordBrokerDelivery('memory', 'dead');
  otel.recordBrokerPublish('memory', 'ok');
  await reader.forceFlush();
  const metrics = exporter.getMetrics().flatMap((batch) => batch.scopeMetrics.flatMap((scope) => scope.metrics));
  const find = (name) => metrics.find((metric) => metric.descriptor.name === name);
  const deliveries = find('wrpc.broker.deliveries').dataPoints;
  const ack = deliveries.find((point) => point.attributes['wrpc.broker.outcome'] === 'ack');
  assert.strictEqual(ack.value, 2);
  assert.strictEqual(ack.attributes['messaging.system'], 'memory');
  assert.strictEqual(find('wrpc.broker.published').dataPoints[0].value, 1);
});

test('telemetry: the disabled writer answers the broker members', async () => {
  assert.strictEqual(
    DISABLED.withMessagingSpan({}, (handle) => handle),
    null,
  );
  assert.doesNotThrow(() => DISABLED.recordBrokerDelivery('x', 'ack'));
  assert.doesNotThrow(() => DISABLED.recordBrokerPublish('x', 'ok'));
  // A broken meter degrades to no instruments rather than throwing.
  const broken = createServerTelemetry({
    meter: {
      createCounter: () => {
        throw new Error('meter exploded');
      },
      createHistogram: () => ({ record() {} }),
    },
  });
  assert.doesNotThrow(() => broken.recordBrokerDelivery('x', 'ack'));
  await waitFor(() => true);
});
