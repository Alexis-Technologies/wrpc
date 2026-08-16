'use strict';

// OpenTelemetry with no dependency on OpenTelemetry.
//
// Nothing here imports anything: the caller injects either the
// `@opentelemetry/api` module or pre-built tracer/meter instances, and every
// one of them is duck-typed.
//
// The file is split three ways — shared, server, client — for one measured
// reason: `src/client.js` is browser-reachable, and pulling the server's
// twelve instruments into a browser bundle put it over its size budget. The
// browser gets shared + client and nothing else.

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
const SPAN_KIND_CLIENT = 2;
const SPAN_KIND_CONSUMER = 4;

// The two packet fields that carry W3C trace context. Short on purpose:
// they ride on every call packet, and 'traceparent' would cost 11 more bytes
// per call than 'tp' for no gain.
const TRACEPARENT = 'tp';
const TRACESTATE = 'ts';

// wrpc does NOT parse or serialize W3C trace context — that is spec surface
// that drifts. The propagator the user configured does the work; these two
// adapters only rename its header keys to the packet's field names.
const SETTER = {
  set(packet, key, value) {
    if (key === 'traceparent') packet[TRACEPARENT] = value;
    else if (key === 'tracestate') packet[TRACESTATE] = value;
  },
};

const GETTER = {
  keys: (packet) => Object.keys(packet).filter((key) => key === TRACEPARENT || key === TRACESTATE),
  get(packet, key) {
    if (key === 'traceparent') return packet[TRACEPARENT];
    if (key === 'tracestate') return packet[TRACESTATE];
    return undefined;
  },
};

// Injecting is impossible without a propagator, so `{ tracer, meter }` alone
// leaves propagation off unless `propagation` is supplied too.
const resolvePropagation = (telemetry) => {
  const propagation = telemetry.api?.propagation ?? telemetry.propagation ?? null;
  const context = telemetry.api?.context ?? telemetry.context ?? null;
  if (!hasMethod(propagation, 'inject') || !hasMethod(propagation, 'extract')) return null;
  return { propagation, context };
};

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

// The OTel RPC semantic conventions split a target into service ($unit) and
// method: one helper, used by spans AND metrics on both halves, so the two
// never disagree about what 'rpc.method' means.
const targetAttributes = (target) => {
  const slash = target.indexOf('/');
  return {
    'rpc.service': slash > 0 ? target.slice(0, slash) : target,
    'rpc.method': slash > 0 ? target.slice(slash + 1) : '',
  };
};

// Opens a span (active when the tracer can, detached otherwise) and invokes
// `fn(handle)` exactly once. Shared verbatim by both halves — the arity
// check on startActiveSpan and the invoked-guard are hazards nobody should
// maintain twice.
const startSpanWith = (tracer, name, options, parent, handle, fn) => {
  if (hasMethod(tracer, 'startActiveSpan')) {
    let invoked = false;
    try {
      const run = (span) => {
        handle.span = span ?? null;
        invoked = true;
        return fn(handle);
      };
      // The 4-argument overload is not universal: handing four arguments
      // to a 3-argument implementation means the callback is never called
      // at all, so the arity is checked rather than assumed.
      const withParent = parent && tracer.startActiveSpan.length >= 4;
      return withParent
        ? tracer.startActiveSpan(name, options, parent, run)
        : tracer.startActiveSpan(name, options, run);
    } catch (error) {
      // An error thrown by `fn` itself must propagate untouched; only a
      // tracer that broke BEFORE running the callback is swallowed.
      if (invoked) throw error;
      return fn(handle);
    }
  }
  try {
    if (!hasMethod(tracer, 'startSpan')) handle.span = null;
    else handle.span = parent ? tracer.startSpan(name, options, parent) : tracer.startSpan(name, options);
  } catch {
    handle.span = null;
  }
  return fn(handle);
};

const recordSpanError = (handle, error, code) => {
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
};

const endSpanHandle = (handle, attributes) => {
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
};

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
  // Client-side members: one disabled writer serves both halves, so it has
  // to answer to everything either of them exposes.
  recordReconnect: noop,
  inject: noop,
});

module.exports = {
  targetAttributes,
  startSpanWith,
  recordSpanError,
  endSpanHandle,
  SPAN_STATUS_ERROR,
  SCOPE_NAME,
  SPAN_KIND_SERVER,
  SPAN_KIND_CLIENT,
  SPAN_KIND_CONSUMER,
  TRACEPARENT,
  TRACESTATE,
  SETTER,
  GETTER,
  hasMethod,
  resolvePropagation,
  resolveTracerAndMeter,
  DISABLED,
};
