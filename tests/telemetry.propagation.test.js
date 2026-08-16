'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const otelApi = require('@opentelemetry/api');
const { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace-base');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const { W3CTraceContextPropagator } = require('@opentelemetry/core');

const { Server, WrpcClient, defineRouter, procedure } = require('../index.js');
const { createServerTelemetry, TRACEPARENT } = require('../src/telemetry/index.js');

// Client and server each get their own exporter, exactly as two processes
// would: if the ids line up across them, propagation genuinely crossed the
// wire rather than leaking through a shared in-process context.
const createTracing = () => {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  return { provider, tracer: provider.getTracer('@alexify/wrpc'), spans: () => exporter.getFinishedSpans() };
};

const createRouter = () =>
  defineRouter({
    probe: {
      echo: procedure({ access: 'public', handler: async (_context, args) => args }),
    },
  });

const boot = async (t, telemetry) => {
  const server = new Server({
    router: createRouter(),
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    timeouts: { bind: 100 },
    logger: false,
    telemetry,
  });
  await server.listen();
  t.after(() => server.close());
  return `ws://127.0.0.1:${server.address().port}/api`;
};

// Both halves of what real propagation needs: a context manager so a span
// opened by startActiveSpan is actually *active*, and a propagator that
// knows the W3C format. wrpc supplies neither — that is the whole point of
// delegating to the injected api.
const withOtelGlobals = (t) => {
  const manager = new AsyncLocalStorageContextManager().enable();
  otelApi.context.setGlobalContextManager(manager);
  otelApi.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  t.after(() => {
    otelApi.context.disable();
    otelApi.propagation.disable();
  });
};

test('a client span parents the server span across the wire', async (t) => {
  withOtelGlobals(t);
  const clientSide = createTracing();
  const serverSide = createTracing();

  // Distinct global providers are impossible in one process, so each side is
  // given its tracer directly and shares only the api's propagator.
  const url = await boot(t, {
    tracer: serverSide.tracer,
    propagation: otelApi.propagation,
    context: otelApi.context,
  });
  const client = await WrpcClient.connect(url, {
    reconnect: false,
    telemetry: { tracer: clientSide.tracer, propagation: otelApi.propagation, context: otelApi.context },
  });
  await client.load('probe');
  await client.api.probe.echo({ ok: 1 });
  client.close();
  await new Promise((resolve) => setTimeout(resolve, 20));

  const callSpan = clientSide.spans().find((span) => span.name === 'probe/echo');
  const serverSpan = serverSide.spans().find((span) => span.name === 'probe/echo');
  assert.ok(callSpan, 'the client traced the call');
  assert.ok(serverSpan, 'the server traced the call');
  assert.strictEqual(callSpan.kind, 2, 'the client span is a CLIENT span');
  assert.strictEqual(serverSpan.kind, 1, 'the server span is a SERVER span');
  assert.strictEqual(serverSpan.spanContext().traceId, callSpan.spanContext().traceId, 'both sides are in ONE trace');
  assert.strictEqual(
    serverSpan.parentSpanContext?.spanId,
    callSpan.spanContext().spanId,
    'and the server span hangs off the client span',
  );
});

test('the tp field is optional in both directions', async (t) => {
  withOtelGlobals(t);

  await t.test('a client without telemetry sends no tp, and the server roots', async () => {
    const serverSide = createTracing();
    const url = await boot(t, {
      tracer: serverSide.tracer,
      propagation: otelApi.propagation,
      context: otelApi.context,
    });
    const sent = [];
    const client = await WrpcClient.connect(url, {
      reconnect: false,
      proxy: null,
    });
    const originalSend = client.send.bind(client);
    client.send = (packet) => {
      sent.push(packet);
      return originalSend(packet);
    };
    await client.load('probe');
    await client.api.probe.echo({ ok: 1 });
    client.close();
    await new Promise((resolve) => setTimeout(resolve, 20));

    assert.ok(sent.length > 0, 'packets were captured');
    for (const packet of sent) {
      assert.strictEqual(packet[TRACEPARENT], undefined, 'no trace context was injected');
    }
    const serverSpan = serverSide.spans().find((span) => span.name === 'probe/echo');
    assert.ok(serverSpan, 'the server still traced the call');
    assert.strictEqual(serverSpan.parentSpanContext, undefined, 'as a root span');
  });

  await t.test('a tp a server does not understand is inert', async () => {
    // No telemetry at all on the server: the extra field must be ignored,
    // not answered with an error.
    const url = await boot(t, null);
    const client = await WrpcClient.connect(url, { reconnect: false });
    await client.load('probe');
    assert.deepStrictEqual(await client.api.probe.echo({ ok: 1 }), { ok: 1 });
    client.close();
  });
});

test('trustRemoteContext', async (t) => {
  withOtelGlobals(t);

  const injected = () => {
    // A syntactically valid W3C traceparent from a peer we may or may not
    // trust: version-traceId-spanId-flags.
    return '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
  };

  await t.test('trusted by default: the remote span becomes the parent', () => {
    const { tracer, spans } = createTracing();
    const otel = createServerTelemetry({ tracer, propagation: otelApi.propagation, context: otelApi.context });
    const packet = { type: 'call', id: '1', [TRACEPARENT]: injected() };
    otel.withSpan({ client: { source: 'x', persistent: true, transportKind: 'ws' }, packet, target: 'a/b' }, (h) =>
      otel.endSpan(h),
    );
    const [span] = spans();
    assert.strictEqual(span.spanContext().traceId, '4bf92f3577b34da6a3ce929d0e0e4736');
    assert.strictEqual(span.parentSpanContext?.spanId, '00f067aa0ba902b7');
  });

  await t.test('trustRemoteContext:false ignores it and roots the span', () => {
    const { tracer, spans } = createTracing();
    const otel = createServerTelemetry({
      tracer,
      propagation: otelApi.propagation,
      context: otelApi.context,
      trustRemoteContext: false,
    });
    const packet = { type: 'call', id: '1', [TRACEPARENT]: injected() };
    otel.withSpan({ client: { source: 'x', persistent: true, transportKind: 'ws' }, packet, target: 'a/b' }, (h) =>
      otel.endSpan(h),
    );
    const [span] = spans();
    assert.notStrictEqual(span.spanContext().traceId, '4bf92f3577b34da6a3ce929d0e0e4736');
    assert.strictEqual(span.parentSpanContext, undefined);
  });

  await t.test('a malformed tp is ignored rather than fatal', () => {
    const { tracer, spans } = createTracing();
    const otel = createServerTelemetry({ tracer, propagation: otelApi.propagation, context: otelApi.context });
    const packet = { type: 'call', id: '1', [TRACEPARENT]: 'not-a-traceparent' };
    otel.withSpan({ client: { source: 'x', persistent: true, transportKind: 'ws' }, packet, target: 'a/b' }, (h) =>
      otel.endSpan(h),
    );
    assert.strictEqual(spans().length, 1);
    assert.strictEqual(spans()[0].parentSpanContext, undefined);
  });

  await t.test('without a propagator, an inbound tp does nothing', () => {
    const { tracer, spans } = createTracing();
    // { tracer } alone: injecting or extracting W3C context is impossible
    // without a propagator, and wrpc refuses to hand-roll the format.
    const otel = createServerTelemetry({ tracer });
    const packet = { type: 'call', id: '1', [TRACEPARENT]: injected() };
    otel.withSpan({ client: { source: 'x', persistent: true, transportKind: 'ws' }, packet, target: 'a/b' }, (h) =>
      otel.endSpan(h),
    );
    assert.strictEqual(spans()[0].parentSpanContext, undefined);
  });
});

test('the startActiveSpan arity guard', async (t) => {
  withOtelGlobals(t);

  await t.test('a 3-argument startActiveSpan still runs its callback', () => {
    const calls = [];
    // Exactly the trap: handing four arguments to this would silently never
    // invoke the callback, and the call would answer nothing.
    const tracer = {
      startActiveSpan(name, options, fn) {
        calls.push(name);
        return fn({ end() {}, setAttribute() {} });
      },
    };
    const otel = createServerTelemetry({ tracer, propagation: otelApi.propagation, context: otelApi.context });
    const packet = {
      type: 'call',
      id: '1',
      [TRACEPARENT]: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
    };
    let ran = false;
    const result = otel.withSpan(
      { client: { source: 'x', persistent: true, transportKind: 'ws' }, packet, target: 'a/b' },
      (handle) => {
        ran = true;
        assert.ok(handle.span, 'the span reached the callback');
        return 'answered';
      },
    );
    assert.strictEqual(ran, true);
    assert.strictEqual(result, 'answered');
    assert.deepStrictEqual(calls, ['a/b']);
  });
});

test('batching keeps trace context per packet, not per frame', async (t) => {
  withOtelGlobals(t);
  const clientSide = createTracing();
  const serverSide = createTracing();
  const url = await boot(t, {
    tracer: serverSide.tracer,
    propagation: otelApi.propagation,
    context: otelApi.context,
  });
  const client = await WrpcClient.connect(url, {
    reconnect: false,
    batch: true,
    telemetry: { tracer: clientSide.tracer, propagation: otelApi.propagation, context: otelApi.context },
  });
  await client.load('probe');
  // Issued in one tick, so they leave as ONE frame carrying two packets.
  await Promise.all([client.api.probe.echo({ n: 1 }), client.api.probe.echo({ n: 2 })]);
  client.close();
  await new Promise((resolve) => setTimeout(resolve, 20));

  const clientSpans = clientSide.spans().filter((span) => span.name === 'probe/echo');
  const serverSpans = serverSide.spans().filter((span) => span.name === 'probe/echo');
  assert.strictEqual(clientSpans.length, 2);
  assert.strictEqual(serverSpans.length, 2);
  const clientIds = clientSpans.map((span) => span.spanContext().spanId).sort();
  const parentIds = serverSpans.map((span) => span.parentSpanContext?.spanId).sort();
  assert.deepStrictEqual(parentIds, clientIds, 'each batched packet kept its own parent');
});

test('subscribe and event packets carry trace context too', async (t) => {
  withOtelGlobals(t);
  const clientSide = createTracing();

  // The wire is observed directly: what matters here is the packet fields,
  // not a server span — so a plain router without telemetry suffices.
  const router = defineRouter({
    feed: {
      ticks: procedure.subscription({
        access: 'public',
        handler: async function* () {
          yield { n: 1 };
        },
      }),
      on: {
        nudge: procedure({ access: 'public', handler: async () => {} }),
      },
    },
  });
  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    timeouts: { bind: 100 },
    logger: false,
  });
  await server.listen();
  t.after(() => server.close());

  const url = `ws://127.0.0.1:${server.address().port}/api`;
  const client = await WrpcClient.connect(url, {
    reconnect: false,
    telemetry: { tracer: clientSide.tracer, propagation: otelApi.propagation, context: otelApi.context },
  });
  t.after(() => void client.close());
  await client.load('feed');

  const sent = [];
  const rawSend = client.send.bind(client);
  client.send = (packet) => {
    sent.push(packet);
    return rawSend(packet);
  };

  await clientSide.tracer.startActiveSpan('outer', async (span) => {
    await new Promise((resolve) => {
      client.api.feed.ticks.subscribe({}, { onEnd: resolve });
    });
    client.sendEvent('feed/nudge', { poke: true });
    span.end();
  });

  const subscribe = sent.find((packet) => packet.type === 'subscribe');
  assert.ok(subscribe, 'the subscribe packet was captured');
  assert.match(subscribe[TRACEPARENT], /^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/);
  const event = sent.find((packet) => packet.type === 'event');
  assert.ok(event, 'the event packet was captured');
  assert.match(event[TRACEPARENT], /^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/);
});
