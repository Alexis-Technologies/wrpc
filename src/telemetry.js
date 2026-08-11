'use strict';

// OpenTelemetry with no dependency on OpenTelemetry.
//
// Like src/logging.js this file imports nothing: the caller injects either
// the `@opentelemetry/api` module or pre-built tracer/meter instances, and
// everything here is duck-typed. The writer that comes back always has the
// same shape, disabled or not, so no call site branches on whether telemetry
// is configured.
//
// Every recording method contains its own failures. A broken exporter, a
// meter that throws, a span implementation missing half its methods — none
// of them may turn a working RPC call into a failed one.

// 0 = UNSET, 1 = OK, 2 = ERROR. Frozen by the OTel specification, so
// hardcoding it is what avoids importing @opentelemetry/api for a constant.
const SPAN_STATUS_ERROR = 2;

// Instrumentation scope. The version argument of getTracer/getMeter is
// deliberately omitted: reading it would mean requiring package.json, which
// would drag this file's whole dependency graph into browser bundles.
const SCOPE_NAME = '@alexify/wrpc';

// SpanKind, also frozen by the spec: 0 INTERNAL, 1 SERVER, 2 CLIENT,
// 3 PRODUCER, 4 CONSUMER.
const SPAN_KIND_SERVER = 1;
const SPAN_KIND_CONSUMER = 4;

const hasMethod = (value, name) => typeof value?.[name] === 'function';

/**
 * Two injection modes:
 * - `{ api }` — the @opentelemetry/api module, from which wrpc derives its
 *   own tracer and meter so spans carry the right instrumentation scope.
 * - `{ tracer, meter }` — pre-built instances; either may be absent, and a
 *   tracer-only or meter-only configuration is fully supported.
 */
const resolveTracerAndMeter = (telemetry) => {
  if (telemetry.api) {
    const { api } = telemetry;
    return {
      tracer: hasMethod(api.trace, 'getTracer') ? api.trace.getTracer(SCOPE_NAME) : null,
      meter: hasMethod(api.metrics, 'getMeter') ? api.metrics.getMeter(SCOPE_NAME) : null,
    };
  }
  return { tracer: telemetry.tracer ?? null, meter: telemetry.meter ?? null };
};

const noop = () => {};

const DISABLED = Object.freeze({
  enabled: false,
  // The lifecycle wrapper still runs its callback — that is what lets the
  // dispatcher bracket a call the same way whether telemetry is on or off.
  withSpan(_options, fn) {
    return fn(null);
  },
  recordError: noop,
  endSpan: noop,
  recordCall: noop,
  recordConnection: noop,
  recordSubscription: noop,
  recordSubscriptionValues: noop,
  recordBroadcast: noop,
  recordStreamBytes: noop,
  recordBackpressure: noop,
  recordSession: noop,
  recordSseChannel: noop,
});

// A target is the wire `method` string: 'unit/name' or 'unit.ver/name'. The
// span name is that string verbatim, following the OTel `rpc.*` convention
// ($service/$method) rather than inventing a wrpc-specific scheme — APM
// tools group RPC spans by exactly this.
const buildCallAttributes = (client, packet, target, includeIdentity) => {
  const slash = target.indexOf('/');
  const attributes = {
    'rpc.system': 'wrpc',
    'rpc.service': slash > 0 ? target.slice(0, slash) : target,
    'rpc.method': slash > 0 ? target.slice(slash + 1) : '',
    'wrpc.packet.type': packet.type,
    'wrpc.transport': client.transportKind,
    'wrpc.persistent': client.persistent,
  };
  if (packet.id) attributes['wrpc.packet.id'] = packet.id;
  // `client.source` is a remote address, so it rides behind the PII gate.
  // A session TOKEN never appears at any setting: that is a credential, not
  // an identity, and the two must not share one switch.
  if (includeIdentity && client.source) attributes['network.peer.address'] = client.source;
  return attributes;
};

const createServerTelemetry = (telemetry) => {
  if (!telemetry || typeof telemetry !== 'object') return DISABLED;

  let tracer = null;
  let meter = null;
  try {
    ({ tracer, meter } = resolveTracerAndMeter(telemetry));
  } catch {
    return DISABLED;
  }
  if (!tracer && !meter) return DISABLED;

  const includeIdentity = telemetry.includeIdentity !== false;

  let duration = null;
  let calls = null;
  let connections = null;
  let subscriptions = null;
  let subscriptionValues = null;
  let broadcasts = null;
  let recipients = null;
  let streamBytes = null;
  let backpressure = null;
  let sessions = null;
  let sseChannels = null;

  // Checked separately from the others: a meter with counters and histograms
  // but no up/down counter would otherwise disable every instrument here.
  const canCount = hasMethod(meter, 'createCounter') && hasMethod(meter, 'createHistogram');
  const canGauge = hasMethod(meter, 'createUpDownCounter');
  if (canCount) {
    try {
      duration = meter.createHistogram('rpc.server.duration', {
        unit: 'ms',
        description: 'Duration of RPC calls answered by this server',
      });
      calls = meter.createCounter('wrpc.server.calls', {
        unit: '{call}',
        description: 'RPC calls answered, by outcome and status code',
      });
      subscriptionValues = meter.createCounter('wrpc.server.subscription.values', {
        unit: '{value}',
        description: 'Values yielded to subscribers',
      });
      broadcasts = meter.createCounter('wrpc.server.broadcasts', {
        unit: '{event}',
        description: 'Room broadcasts, by whether they also went to the backplane',
      });
      recipients = meter.createHistogram('wrpc.server.broadcast.recipients', {
        unit: '{client}',
        description: 'Local fan-out size of a broadcast',
      });
      streamBytes = meter.createCounter('wrpc.server.stream.bytes', {
        unit: 'By',
        description: 'Binary stream bytes, by direction',
      });
      backpressure = meter.createCounter('wrpc.server.backpressure', {
        unit: '{event}',
        description: 'Times a producer parked waiting for the transport to drain',
      });
      sessions = meter.createCounter('wrpc.server.sessions', {
        unit: '{operation}',
        description: 'Session operations, by kind and result',
      });
    } catch {
      duration = null;
      calls = null;
      subscriptionValues = null;
      broadcasts = null;
      recipients = null;
      streamBytes = null;
      backpressure = null;
      sessions = null;
    }
  }
  if (canGauge) {
    try {
      connections = meter.createUpDownCounter('wrpc.server.connections', {
        unit: '{connection}',
        description: 'Live connections, by transport',
      });
      subscriptions = meter.createUpDownCounter('wrpc.server.subscriptions', {
        unit: '{subscription}',
        description: 'Live subscriptions',
      });
      sseChannels = meter.createUpDownCounter('wrpc.server.sse.channels', {
        unit: '{channel}',
        description: 'Live SSE channels',
      });
    } catch {
      connections = null;
      subscriptions = null;
      sseChannels = null;
    }
  }

  const startSpan = (name, options, handle, fn) => {
    if (hasMethod(tracer, 'startActiveSpan')) {
      let invoked = false;
      try {
        return tracer.startActiveSpan(name, options, (span) => {
          handle.span = span ?? null;
          invoked = true;
          return fn(handle);
        });
      } catch (error) {
        // An error thrown by `fn` itself must propagate untouched; only a
        // tracer that broke BEFORE running the callback is swallowed.
        if (invoked) throw error;
        return fn(handle);
      }
    }
    try {
      handle.span = hasMethod(tracer, 'startSpan') ? tracer.startSpan(name, options) : null;
    } catch {
      handle.span = null;
    }
    return fn(handle);
  };

  return {
    enabled: true,

    /**
     * Opens a span around one packet's whole lifetime and invokes `fn(handle)`
     * exactly once. The handle is mutable — `{ span, error }` — rather than a
     * raw span, because the span is NOT ended here: `endSpan` does that from
     * the caller's `finally`, which is the only place that knows the call
     * actually finished.
     */
    withSpan({ client, packet, target, kind = SPAN_KIND_SERVER, suffix = '' }, fn) {
      const handle = { span: null, error: false };
      if (!tracer) return fn(handle);
      const attributes = buildCallAttributes(client, packet, target, includeIdentity);
      return startSpan(`${target}${suffix}`, { kind, attributes }, handle, fn);
    },

    recordError(handle, error, code) {
      try {
        handle.error = true;
        const span = handle?.span;
        if (!span) return;
        span.recordException?.(error);
        span.setStatus?.({ code: SPAN_STATUS_ERROR, message: error?.message });
        if (code !== undefined) span.setAttribute?.('rpc.wrpc.status_code', code);
        if (error?.name) span.setAttribute?.('error.type', error.name);
      } catch {
        // Telemetry must never break the request path.
      }
    },

    endSpan(handle, attributes) {
      try {
        const span = handle?.span;
        if (!span) return;
        if (attributes) {
          for (const [key, value] of Object.entries(attributes)) {
            if (value !== undefined && value !== null) span.setAttribute?.(key, value);
          }
        }
        span.end();
      } catch {}
    },

    // Fires with or without a tracer: a meter-only configuration is a
    // supported way to run wrpc.
    recordCall(target, status, code, elapsed) {
      try {
        const attributes = { 'rpc.system': 'wrpc', 'rpc.method': target, 'wrpc.status': status };
        if (code !== undefined) attributes['rpc.wrpc.status_code'] = code;
        calls?.add(1, attributes);
        if (elapsed !== undefined) duration?.record(elapsed, attributes);
      } catch {}
    },

    recordConnection(delta, transport) {
      try {
        connections?.add(delta, { 'wrpc.transport': transport });
      } catch {}
    },

    recordSubscription(delta, target) {
      try {
        subscriptions?.add(delta, { 'rpc.method': target });
      } catch {}
    },

    recordSubscriptionValues(count, target) {
      try {
        if (count > 0) subscriptionValues?.add(count, { 'rpc.method': target });
      } catch {}
    },

    recordBroadcast(name, local, published) {
      try {
        broadcasts?.add(1, { 'wrpc.event.name': name, 'wrpc.published': Boolean(published) });
        recipients?.record(local, { 'wrpc.event.name': name });
      } catch {}
    },

    recordStreamBytes(direction, bytes) {
      try {
        streamBytes?.add(bytes, { 'wrpc.stream.direction': direction });
      } catch {}
    },

    recordBackpressure(transport) {
      try {
        backpressure?.add(1, { 'wrpc.transport': transport });
      } catch {}
    },

    recordSession(operation, result) {
      try {
        sessions?.add(1, { 'wrpc.session.op': operation, 'wrpc.session.result': result });
      } catch {}
    },

    recordSseChannel(delta) {
      try {
        sseChannels?.add(delta);
      } catch {}
    },
  };
};

module.exports = {
  createServerTelemetry,
  SCOPE_NAME,
  SPAN_STATUS_ERROR,
  SPAN_KIND_SERVER,
  SPAN_KIND_CONSUMER,
};
