'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const otelApi = require('@opentelemetry/api');
const { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace-base');
const {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} = require('@opentelemetry/sdk-metrics');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');

const { createServerTelemetry, createClientTelemetry, SCOPE_NAME, TRACEPARENT } = require('../src/telemetry/index.js');

// A real SDK, exporting into memory. Anything less would not prove that what
// wrpc emits is actually a valid span or a valid metric.
const createTracing = () => {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  return { provider, tracer: provider.getTracer(SCOPE_NAME), spans: () => exporter.getFinishedSpans() };
};

const createMetrics = () => {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  // A very long interval: the reader is flushed by hand so the assertions
  // are not racing a background timer.
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 3_600_000 });
  const provider = new MeterProvider({ readers: [reader] });
  const collect = async () => {
    await reader.forceFlush();
    const batches = exporter.getMetrics();
    const metrics = [];
    for (const batch of batches) {
      for (const scope of batch.scopeMetrics) metrics.push(...scope.metrics);
    }
    return metrics;
  };
  return { provider, meter: provider.getMeter(SCOPE_NAME), collect, reader };
};

const fakeClient = (overrides = {}) => ({
  source: '10.0.0.1',
  persistent: true,
  transportKind: 'ws',
  ...overrides,
});

test('createServerTelemetry: what disables it', async (t) => {
  await t.test('nothing, a non-object, or an empty bag is disabled', () => {
    for (const value of [undefined, null, false, 'otel', 42, {}]) {
      assert.strictEqual(createServerTelemetry(value).enabled, false, String(value));
    }
  });

  await t.test('an api module with neither trace nor metrics is disabled', () => {
    assert.strictEqual(createServerTelemetry({ api: { trace: {}, metrics: {} } }).enabled, false);
  });

  await t.test('a getter that throws is disabled rather than fatal', () => {
    const telemetry = {
      get api() {
        throw new Error('exploded while resolving');
      },
    };
    assert.strictEqual(createServerTelemetry(telemetry).enabled, false);
  });

  await t.test('the disabled writer still runs the callback', () => {
    const otel = createServerTelemetry(null);
    let handle = 'untouched';
    const result = otel.withSpan({ client: fakeClient(), packet: {}, target: 'a/b' }, (value) => {
      handle = value;
      return 'returned';
    });
    assert.strictEqual(result, 'returned');
    assert.strictEqual(handle, null);
    assert.doesNotThrow(() => otel.recordCall('a/b', 'ok', 200, 1));
  });
});

test('spans', async (t) => {
  await t.test('a call span carries the rpc semconv attributes', () => {
    const { tracer, spans } = createTracing();
    const otel = createServerTelemetry({ tracer });
    otel.withSpan({ client: fakeClient(), packet: { type: 'call', id: 'p1' }, target: 'chat.v2/send' }, (handle) =>
      otel.endSpan(handle, { 'wrpc.status': 'ok' }),
    );
    const [span] = spans();
    assert.strictEqual(span.name, 'chat.v2/send');
    assert.strictEqual(span.attributes['rpc.system'], 'wrpc');
    assert.strictEqual(span.attributes['rpc.service'], 'chat.v2');
    assert.strictEqual(span.attributes['rpc.method'], 'send');
    assert.strictEqual(span.attributes['wrpc.packet.type'], 'call');
    assert.strictEqual(span.attributes['wrpc.packet.id'], 'p1');
    assert.strictEqual(span.attributes['wrpc.transport'], 'ws');
    assert.strictEqual(span.attributes['wrpc.status'], 'ok');
    assert.strictEqual(span.instrumentationScope.name, SCOPE_NAME);
  });

  await t.test('an error sets status 2 and records the exception', () => {
    const { tracer, spans } = createTracing();
    const otel = createServerTelemetry({ tracer });
    const boom = new TypeError('bad args');
    otel.withSpan({ client: fakeClient(), packet: { type: 'call' }, target: 'a/b' }, (handle) => {
      otel.recordError(handle, boom, 400);
      otel.endSpan(handle);
    });
    const [span] = spans();
    assert.strictEqual(span.status.code, 2);
    assert.strictEqual(span.status.message, 'bad args');
    assert.strictEqual(span.attributes['error.type'], 'TypeError');
    assert.strictEqual(span.attributes['rpc.wrpc.status_code'], 400);
    assert.strictEqual(span.events[0].name, 'exception');
  });

  await t.test('nested spans parent correctly through startActiveSpan', (t) => {
    // Without a context manager `startActiveSpan` opens a span but nothing
    // sees it as active — which is the whole reason wrpc prefers that method.
    const manager = new AsyncLocalStorageContextManager().enable();
    otelApi.context.setGlobalContextManager(manager);
    t.after(() => otelApi.context.disable());
    const { provider, tracer, spans } = createTracing();
    const otel = createServerTelemetry({ tracer });
    otel.withSpan({ client: fakeClient(), packet: { type: 'call' }, target: 'a/b' }, (handle) => {
      const inner = provider.getTracer('inner').startSpan('handler-work');
      inner.end();
      otel.endSpan(handle);
    });
    const [inner, outer] = spans();
    assert.strictEqual(inner.name, 'handler-work');
    assert.strictEqual(inner.parentSpanContext?.spanId, outer.spanContext().spanId);
  });

  await t.test('the identity attribute is gated, the session token never appears', () => {
    const { tracer, spans } = createTracing();
    const otel = createServerTelemetry({ tracer, includeIdentity: false });
    otel.withSpan({ client: fakeClient(), packet: { type: 'call' }, target: 'a/b' }, (handle) => otel.endSpan(handle));
    const [span] = spans();
    assert.strictEqual(span.attributes['network.peer.address'], undefined);
    assert.strictEqual(
      Object.keys(span.attributes).some((key) => key.includes('token')),
      false,
    );
  });
});

test('a broken tracer never breaks a call', async (t) => {
  await t.test('a tracer that throws before the callback still runs it', () => {
    const otel = createServerTelemetry({
      tracer: {
        startActiveSpan() {
          throw new Error('tracer exploded');
        },
      },
    });
    let ran = false;
    const result = otel.withSpan({ client: fakeClient(), packet: {}, target: 'a/b' }, (handle) => {
      ran = true;
      assert.strictEqual(handle.span, null);
      return 'value';
    });
    assert.strictEqual(ran, true);
    assert.strictEqual(result, 'value');
  });

  await t.test('an error thrown by the callback propagates untouched', () => {
    const { tracer } = createTracing();
    const otel = createServerTelemetry({ tracer });
    const boom = new Error('handler exploded');
    assert.throws(
      () =>
        otel.withSpan({ client: fakeClient(), packet: {}, target: 'a/b' }, () => {
          throw boom;
        }),
      (error) => error === boom,
    );
  });

  await t.test('a tracer without startActiveSpan falls back to startSpan', () => {
    const started = [];
    const otel = createServerTelemetry({
      tracer: {
        startSpan(name) {
          started.push(name);
          return { end() {} };
        },
      },
    });
    otel.withSpan({ client: fakeClient(), packet: {}, target: 'a/b' }, (handle) => {
      assert.ok(handle.span);
      otel.endSpan(handle);
    });
    assert.deepStrictEqual(started, ['a/b']);
  });

  await t.test('a startSpan that throws yields a null span, not a failed call', () => {
    const otel = createServerTelemetry({
      tracer: {
        startSpan() {
          throw new Error('startSpan exploded');
        },
      },
    });
    let ran = false;
    otel.withSpan({ client: fakeClient(), packet: {}, target: 'a/b' }, (handle) => {
      ran = true;
      assert.strictEqual(handle.span, null);
    });
    assert.strictEqual(ran, true);
  });

  await t.test('a tracer with neither method yields a null span', () => {
    const otel = createServerTelemetry({ tracer: { name: 'useless' } });
    otel.withSpan({ client: fakeClient(), packet: {}, target: 'a/b' }, (handle) => {
      assert.strictEqual(handle.span, null);
      assert.doesNotThrow(() => otel.endSpan(handle, { 'wrpc.status': 'ok' }));
      assert.doesNotThrow(() => otel.recordError(handle, new Error('x'), 500));
    });
  });

  await t.test('a span whose methods throw is contained', () => {
    const explode = () => {
      throw new Error('span exploded');
    };
    const otel = createServerTelemetry({
      tracer: {
        startSpan: () => ({
          setAttribute: explode,
          setStatus: explode,
          recordException: explode,
          end: explode,
        }),
      },
    });
    otel.withSpan({ client: fakeClient(), packet: {}, target: 'a/b' }, (handle) => {
      assert.doesNotThrow(() => otel.recordError(handle, new Error('x'), 500));
      assert.doesNotThrow(() => otel.endSpan(handle, { 'wrpc.status': 'error' }));
    });
  });
});

test('metrics', async (t) => {
  await t.test('calls and duration are recorded together', async () => {
    const { meter, collect, provider } = createMetrics();
    t.after(() => provider.shutdown());
    const otel = createServerTelemetry({ meter });
    otel.recordCall('chat/send', 'ok', 200, 12.5);
    otel.recordCall('chat/send', 'error', 500, 3);
    const metrics = await collect();
    const calls = metrics.find((metric) => metric.descriptor.name === 'wrpc.server.calls');
    const duration = metrics.find((metric) => metric.descriptor.name === 'rpc.server.duration');
    assert.strictEqual(calls.dataPoints.length, 2);
    assert.strictEqual(calls.dataPoints[0].attributes['wrpc.status'], 'ok');
    assert.strictEqual(calls.dataPoints[1].attributes['rpc.wrpc.status_code'], 500);
    assert.strictEqual(duration.dataPoints[0].value.count, 1);
    assert.strictEqual(duration.descriptor.unit, 'ms');
  });

  await t.test('the gauges go up and back down', async () => {
    const { meter, collect, provider } = createMetrics();
    t.after(() => provider.shutdown());
    const otel = createServerTelemetry({ meter });
    otel.recordConnection(1, 'ws');
    otel.recordConnection(1, 'ws');
    otel.recordConnection(-1, 'ws');
    otel.recordSseChannel(1);
    const metrics = await collect();
    const connections = metrics.find((metric) => metric.descriptor.name === 'wrpc.server.connections');
    const channels = metrics.find((metric) => metric.descriptor.name === 'wrpc.server.sse.channels');
    assert.strictEqual(connections.dataPoints[0].value, 1);
    assert.strictEqual(connections.dataPoints[0].attributes['wrpc.transport'], 'ws');
    assert.strictEqual(channels.dataPoints[0].value, 1);
  });

  await t.test('broadcast, stream, backpressure and session counters', async () => {
    const { meter, collect, provider } = createMetrics();
    t.after(() => provider.shutdown());
    const otel = createServerTelemetry({ meter });
    otel.recordBroadcast('msg', 3, true);
    otel.recordStreamBytes('receive', 2048);
    otel.recordBackpressure('ws');
    otel.recordSession('restore', 'hit');
    otel.recordSubscription(1, 'feed/live');
    otel.recordSubscriptionValues(7, 'feed/live');
    const names = (await collect()).map((metric) => metric.descriptor.name);
    for (const name of [
      'wrpc.server.broadcasts',
      'wrpc.server.broadcast.recipients',
      'wrpc.server.stream.bytes',
      'wrpc.server.backpressure',
      'wrpc.server.sessions',
      'wrpc.server.subscriptions',
      'wrpc.server.subscription.values',
    ]) {
      assert.ok(names.includes(name), `${name} was exported`);
    }
  });

  await t.test('a zero count is not recorded', async () => {
    const { meter, collect, provider } = createMetrics();
    t.after(() => provider.shutdown());
    const otel = createServerTelemetry({ meter });
    otel.recordSubscriptionValues(0, 'feed/live');
    const names = (await collect()).map((metric) => metric.descriptor.name);
    assert.strictEqual(names.includes('wrpc.server.subscription.values'), false);
  });
});

test('partial and broken meters', async (t) => {
  await t.test('a meter without createUpDownCounter keeps its counters', () => {
    const created = [];
    const otel = createServerTelemetry({
      meter: {
        createCounter: (name) => {
          created.push(name);
          return { add() {} };
        },
        createHistogram: (name) => {
          created.push(name);
          return { record() {} };
        },
      },
    });
    assert.strictEqual(otel.enabled, true);
    assert.ok(created.includes('wrpc.server.calls'), 'counters were still built');
    assert.doesNotThrow(() => otel.recordCall('a/b', 'ok', 200, 1));
    assert.doesNotThrow(() => otel.recordConnection(1, 'ws'));
  });

  await t.test('a meter whose factories throw disables its instruments, not the writer', () => {
    const otel = createServerTelemetry({
      meter: {
        createCounter() {
          throw new Error('meter exploded');
        },
        createHistogram: () => ({ record() {} }),
        createUpDownCounter: () => ({ add() {} }),
      },
    });
    assert.strictEqual(otel.enabled, true);
    assert.doesNotThrow(() => otel.recordCall('a/b', 'ok', 200, 1));
  });

  await t.test('an up/down counter factory that throws leaves the counters alone', () => {
    const otel = createServerTelemetry({
      meter: {
        createCounter: () => ({ add() {} }),
        createHistogram: () => ({ record() {} }),
        createUpDownCounter() {
          throw new Error('gauge factory exploded');
        },
      },
    });
    assert.strictEqual(otel.enabled, true);
    assert.doesNotThrow(() => otel.recordCall('a/b', 'ok', 200, 1));
    assert.doesNotThrow(() => otel.recordConnection(1, 'ws'));
  });

  await t.test('an instrument that throws on use is contained', () => {
    const explode = () => {
      throw new Error('instrument exploded');
    };
    const otel = createServerTelemetry({
      meter: {
        createCounter: () => ({ add: explode }),
        createHistogram: () => ({ record: explode }),
        createUpDownCounter: () => ({ add: explode }),
      },
    });
    assert.doesNotThrow(() => otel.recordCall('a/b', 'ok', 200, 1));
    assert.doesNotThrow(() => otel.recordConnection(1, 'ws'));
    assert.doesNotThrow(() => otel.recordBroadcast('msg', 1, false));
    assert.doesNotThrow(() => otel.recordStreamBytes('receive', 1));
    assert.doesNotThrow(() => otel.recordBackpressure('ws'));
    assert.doesNotThrow(() => otel.recordSession('create', 'ok'));
    assert.doesNotThrow(() => otel.recordSseChannel(1));
    assert.doesNotThrow(() => otel.recordSubscription(1, 'a/b'));
    assert.doesNotThrow(() => otel.recordSubscriptionValues(1, 'a/b'));
  });
});

test('the { api } injection mode', async (t) => {
  await t.test('tracer and meter are derived with the wrpc scope', async () => {
    const tracing = createTracing();
    const metrics = createMetrics();
    otelApi.trace.setGlobalTracerProvider(tracing.provider);
    otelApi.metrics.setGlobalMeterProvider(metrics.provider);
    t.after(() => {
      otelApi.trace.disable();
      otelApi.metrics.disable();
      return metrics.provider.shutdown();
    });

    const otel = createServerTelemetry({ api: otelApi });
    assert.strictEqual(otel.enabled, true);
    otel.withSpan({ client: fakeClient(), packet: { type: 'call' }, target: 'a/b' }, (handle) => otel.endSpan(handle));
    otel.recordCall('a/b', 'ok', 200, 1);

    const [span] = tracing.spans();
    assert.strictEqual(span.instrumentationScope.name, SCOPE_NAME);
    const exported = await metrics.collect();
    assert.ok(exported.some((metric) => metric.descriptor.name === 'wrpc.server.calls'));
  });
});

test('tracer-only and meter-only both work', async (t) => {
  await t.test('tracer only: spans, no metric calls throw', () => {
    const { tracer, spans } = createTracing();
    const otel = createServerTelemetry({ tracer });
    otel.withSpan({ client: fakeClient(), packet: {}, target: 'a/b' }, (handle) => otel.endSpan(handle));
    assert.strictEqual(spans().length, 1);
    assert.doesNotThrow(() => otel.recordCall('a/b', 'ok', 200, 1));
  });

  await t.test('meter only: metrics, and withSpan still runs its callback', async () => {
    const { meter, collect, provider } = createMetrics();
    t.after(() => provider.shutdown());
    const otel = createServerTelemetry({ meter });
    let ran = false;
    otel.withSpan({ client: fakeClient(), packet: {}, target: 'a/b' }, (handle) => {
      ran = true;
      assert.strictEqual(handle.span, null);
      otel.endSpan(handle);
    });
    otel.recordCall('a/b', 'ok', 200, 1);
    assert.strictEqual(ran, true);
    assert.ok((await collect()).some((metric) => metric.descriptor.name === 'wrpc.server.calls'));
  });
});

test('a live server emits spans and metrics for real traffic', async (t) => {
  const { Server, WrpcClient, defineRouter, procedure } = require('../index.js');
  const tracing = createTracing();
  const metrics = createMetrics();
  t.after(() => metrics.provider.shutdown());

  const server = new Server({
    router: defineRouter({
      probe: {
        echo: procedure({ access: 'public', handler: async (_context, args) => args }),
        boom: procedure({
          access: 'public',
          handler: async () => {
            throw new Error('handler exploded');
          },
        }),
        feed: procedure({
          access: 'public',
          subscription: true,
          handler: async function* () {
            yield 1;
            yield 2;
          },
        }),
      },
    }),
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    timeouts: { bind: 100 },
    logger: false,
    telemetry: { tracer: tracing.tracer, meter: metrics.meter },
  });
  await server.listen();
  t.after(() => server.close());

  const client = await WrpcClient.connect(`ws://127.0.0.1:${server.address().port}/api`, { reconnect: false });
  await client.load('probe');
  await client.api.probe.echo({ ok: 1 });
  await assert.rejects(client.api.probe.boom());
  const values = [];
  await new Promise((resolve) => {
    client.api.probe.feed.subscribe({}, { onData: (value) => values.push(value), onEnd: resolve });
  });
  client.close();
  await new Promise((resolve) => setTimeout(resolve, 20));

  await t.test('the successful call has an ok span', () => {
    const span = tracing.spans().find((entry) => entry.name === 'probe/echo');
    assert.ok(span, 'probe/echo was traced');
    assert.strictEqual(span.attributes['wrpc.status'], 'ok');
    assert.strictEqual(span.attributes['rpc.service'], 'probe');
    assert.strictEqual(span.attributes['wrpc.transport'], 'ws');
  });

  await t.test('the failing call has an error span with status 2', () => {
    const span = tracing.spans().find((entry) => entry.name === 'probe/boom');
    assert.ok(span, 'probe/boom was traced');
    assert.strictEqual(span.status.code, 2);
    assert.strictEqual(span.attributes['wrpc.status'], 'error');
    assert.strictEqual(span.attributes['rpc.wrpc.status_code'], 500);
  });

  await t.test('the subscription span counts what it yielded', () => {
    const span = tracing.spans().find((entry) => entry.name === 'probe/feed subscribe');
    assert.ok(span, 'the subscription was traced');
    assert.deepStrictEqual(values, [1, 2]);
    assert.strictEqual(span.attributes['wrpc.subscription.values'], 2);
    assert.strictEqual(span.attributes['wrpc.subscription.terminal'], 'complete');
  });

  await t.test('durations and connection counts reached the meter', async () => {
    const exported = await metrics.collect();
    const duration = exported.find((metric) => metric.descriptor.name === 'rpc.server.duration');
    const connections = exported.find((metric) => metric.descriptor.name === 'wrpc.server.connections');
    assert.ok(duration, 'call durations were recorded');
    assert.ok(
      duration.dataPoints.some(
        (point) => point.attributes['rpc.service'] === 'probe' && point.attributes['rpc.method'] === 'echo',
      ),
      'including the echo call — split per the RPC semconv, matching the spans',
    );
    assert.ok(connections, 'the connection gauge was recorded');
    assert.strictEqual(connections.dataPoints[0].attributes['wrpc.transport'], 'ws');
  });
});

test('createClientTelemetry', async (t) => {
  await t.test('what disables it', () => {
    for (const value of [undefined, null, false, 'otel', 42, {}]) {
      assert.strictEqual(createClientTelemetry(value).enabled, false, String(value));
    }
    assert.strictEqual(createClientTelemetry({ api: { trace: {}, metrics: {} } }).enabled, false);
    const throwing = {
      get api() {
        throw new Error('exploded');
      },
    };
    assert.strictEqual(createClientTelemetry(throwing).enabled, false);
  });

  await t.test('the disabled writer answers to everything the client calls', () => {
    const otel = createClientTelemetry(null);
    assert.strictEqual(
      otel.withSpan({ packet: {}, target: 'a/b' }, () => 'value'),
      'value',
    );
    const packet = { type: 'call' };
    assert.doesNotThrow(() => otel.inject(packet));
    assert.deepStrictEqual(packet, { type: 'call' }, 'no fields were added');
    assert.doesNotThrow(() => otel.recordCall('a/b', 'ok', 1));
    assert.doesNotThrow(() => otel.recordReconnect('recovered', 2));
    assert.doesNotThrow(() => otel.recordConnection(1));
  });

  await t.test('a call span is a CLIENT span with the rpc attributes', () => {
    const { tracer, spans } = createTracing();
    const otel = createClientTelemetry({ tracer });
    otel.withSpan({ packet: { type: 'call', id: 'p9' }, target: 'chat/send' }, (handle) =>
      otel.endSpan(handle, { 'wrpc.status': 'ok' }),
    );
    const [span] = spans();
    assert.strictEqual(span.name, 'chat/send');
    assert.strictEqual(span.kind, 2);
    assert.strictEqual(span.attributes['rpc.system'], 'wrpc');
    assert.strictEqual(span.attributes['rpc.service'], 'chat');
    assert.strictEqual(span.attributes['rpc.method'], 'send');
    assert.strictEqual(span.attributes['wrpc.packet.id'], 'p9');
    assert.strictEqual(span.attributes['wrpc.status'], 'ok');
  });

  await t.test('an error sets status 2 and the error type', () => {
    const { tracer, spans } = createTracing();
    const otel = createClientTelemetry({ tracer });
    otel.withSpan({ packet: { type: 'call' }, target: 'a/b' }, (handle) => {
      otel.recordError(handle, new RangeError('too big'));
      otel.endSpan(handle);
    });
    const [span] = spans();
    assert.strictEqual(span.status.code, 2);
    assert.strictEqual(span.attributes['error.type'], 'RangeError');
  });

  await t.test('inject without a propagator writes nothing', () => {
    const { tracer } = createTracing();
    const otel = createClientTelemetry({ tracer });
    const packet = { type: 'call', id: '1' };
    otel.inject(packet);
    assert.strictEqual(packet[TRACEPARENT], undefined);
  });

  await t.test('a propagator that throws is contained', () => {
    const { tracer } = createTracing();
    const otel = createClientTelemetry({
      tracer,
      propagation: {
        inject() {
          throw new Error('propagator exploded');
        },
        extract: () => undefined,
      },
    });
    assert.doesNotThrow(() => otel.inject({ type: 'call' }));
  });

  await t.test('a tracer that throws before the callback still runs it', () => {
    const otel = createClientTelemetry({
      tracer: {
        startActiveSpan() {
          throw new Error('tracer exploded');
        },
      },
    });
    let ran = false;
    const result = otel.withSpan({ packet: {}, target: 'a/b' }, (handle) => {
      ran = true;
      assert.strictEqual(handle.span, null);
      return 'value';
    });
    assert.strictEqual(ran, true);
    assert.strictEqual(result, 'value');
  });

  await t.test('an error thrown by the callback propagates untouched', () => {
    const { tracer } = createTracing();
    const otel = createClientTelemetry({ tracer });
    const boom = new Error('caller exploded');
    assert.throws(
      () =>
        otel.withSpan({ packet: {}, target: 'a/b' }, () => {
          throw boom;
        }),
      (error) => error === boom,
    );
  });

  await t.test('startSpan fallback, and a startSpan that throws', () => {
    const names = [];
    const viaStartSpan = createClientTelemetry({
      tracer: {
        startSpan(name) {
          names.push(name);
          return { end() {}, setAttribute() {} };
        },
      },
    });
    viaStartSpan.withSpan({ packet: {}, target: 'a/b' }, (handle) => {
      assert.ok(handle.span);
      viaStartSpan.endSpan(handle, { 'wrpc.status': 'ok' });
    });
    assert.deepStrictEqual(names, ['a/b']);

    const broken = createClientTelemetry({
      tracer: {
        startSpan() {
          throw new Error('startSpan exploded');
        },
      },
    });
    broken.withSpan({ packet: {}, target: 'a/b' }, (handle) => {
      assert.strictEqual(handle.span, null);
      assert.doesNotThrow(() => broken.recordError(handle, new Error('x')));
      assert.doesNotThrow(() => broken.endSpan(handle, { 'wrpc.status': 'error' }));
    });

    const useless = createClientTelemetry({ tracer: { name: 'nothing' } });
    useless.withSpan({ packet: {}, target: 'a/b' }, (handle) => assert.strictEqual(handle.span, null));
  });

  await t.test('client metrics reach the meter', async () => {
    const { meter, collect, provider } = createMetrics();
    t.after(() => provider.shutdown());
    const otel = createClientTelemetry({ meter });
    otel.recordCall('chat/send', 'ok', 9);
    otel.recordReconnect('recovered', 3);
    otel.recordConnection(1);
    const names = (await collect()).map((metric) => metric.descriptor.name);
    for (const name of ['rpc.client.duration', 'wrpc.client.reconnects', 'wrpc.client.connections']) {
      assert.ok(names.includes(name), `${name} was exported`);
    }
  });

  await t.test('broken and partial meters are contained', () => {
    const explode = () => {
      throw new Error('meter exploded');
    };
    const factoriesThrow = createClientTelemetry({
      meter: { createHistogram: explode, createCounter: explode, createUpDownCounter: explode },
    });
    assert.strictEqual(factoriesThrow.enabled, true);
    assert.doesNotThrow(() => factoriesThrow.recordCall('a/b', 'ok', 1));
    assert.doesNotThrow(() => factoriesThrow.recordConnection(1));

    const instrumentsThrow = createClientTelemetry({
      meter: {
        createHistogram: () => ({ record: explode }),
        createCounter: () => ({ add: explode }),
        createUpDownCounter: () => ({ add: explode }),
      },
    });
    assert.doesNotThrow(() => instrumentsThrow.recordCall('a/b', 'ok', 1));
    assert.doesNotThrow(() => instrumentsThrow.recordReconnect('exhausted', 5));
    assert.doesNotThrow(() => instrumentsThrow.recordConnection(-1));

    const noGauge = createClientTelemetry({
      meter: { createHistogram: () => ({ record() {} }), createCounter: () => ({ add() {} }) },
    });
    assert.strictEqual(noGauge.enabled, true);
    assert.doesNotThrow(() => noGauge.recordConnection(1));

    const gaugeFactoryThrows = createClientTelemetry({
      meter: {
        createHistogram: () => ({ record() {} }),
        createCounter: () => ({ add() {} }),
        createUpDownCounter: explode,
      },
    });
    assert.doesNotThrow(() => gaugeFactoryThrows.recordConnection(1));
  });

  await t.test('recordCall without an elapsed value records nothing', async () => {
    const { meter, collect, provider } = createMetrics();
    t.after(() => provider.shutdown());
    const otel = createClientTelemetry({ meter });
    otel.recordCall('a/b', 'ok');
    const names = (await collect()).map((metric) => metric.descriptor.name);
    assert.strictEqual(names.includes('rpc.client.duration'), false);
  });
});

test('unresolved names never mint metric series or spans', async (t) => {
  const { Server, WrpcClient, defineRouter, procedure } = require('../index.js');
  const { InMemorySpanExporter, BasicTracerProvider, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace-base');
  const {
    MeterProvider,
    InMemoryMetricExporter,
    PeriodicExportingMetricReader,
    AggregationTemporality,
  } = require('@opentelemetry/sdk-metrics');
  const spanExporter = new InMemorySpanExporter();
  const tracerProvider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(spanExporter)] });
  const metricExporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter: metricExporter, exportIntervalMillis: 60_000 });
  const meterProvider = new MeterProvider({ readers: [reader] });

  const server = new Server({
    router: defineRouter({ probe: { echo: procedure({ access: 'public', handler: async (_c, a) => a }) } }),
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 100 },
    telemetry: {
      tracer: tracerProvider.getTracer('@alexify/wrpc'),
      meter: meterProvider.getMeter('@alexify/wrpc'),
    },
  });
  await server.listen();
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${server.address().port}/api`, {
    heartbeat: false,
    reconnect: false,
  });
  t.after(() => void client.close());

  // A scanner spraying random names: every one must land in ONE bucket.
  for (let i = 0; i < 30; i++) {
    client.send({ type: 'call', id: `spray-${i}`, method: `guess_${i}/m${i}`, args: {} });
    client.send({ type: 'event', name: `guess_${i}/e${i}`, data: {} });
  }
  await new Promise((resolve) => setTimeout(resolve, 100));

  await reader.forceFlush();
  const metrics = metricExporter.getMetrics().flatMap((rm) => rm.scopeMetrics.flatMap((sm) => sm.metrics));
  const duration = metrics.find((metric) => metric.descriptor.name === 'rpc.server.duration');
  const services = new Set(
    (duration?.dataPoints ?? []).map((point) => point.attributes['rpc.service']).filter(Boolean),
  );
  for (const service of services) {
    assert.ok(!service.startsWith('guess_'), `peer-controlled name '${service}' minted a metric series`);
  }
  assert.ok(services.has('<unknown>') || services.size <= 2, 'unresolved calls collapse into the bucket');

  const sprayedSpans = spanExporter.getFinishedSpans().filter((span) => span.name.includes('guess_'));
  assert.strictEqual(sprayedSpans.length, 0, 'an unresolved name must not become a span name');
});
