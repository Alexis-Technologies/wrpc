'use strict';

// The server half: spans for calls, subscriptions and inbound events, plus
// the twelve instruments a server has anything to say about. Node-only —
// nothing browser-reachable requires this file.

const {
  SPAN_KIND_SERVER,
  TRACEPARENT,
  GETTER,
  hasMethod,
  resolvePropagation,
  resolveTracerAndMeter,
  DISABLED,
  targetAttributes,
  startSpanWith,
  recordSpanError,
  endSpanHandle,
} = require('./shared.js');

// A target is the wire `method` string: 'unit/name' or 'unit.ver/name'. The
// span name is that string verbatim, following the OTel `rpc.*` convention
// ($service/$method) rather than inventing a wrpc-specific scheme — APM
// tools group RPC spans by exactly this.
const buildCallAttributes = (client, packet, target, includeIdentity) => {
  const attributes = {
    'rpc.system': 'wrpc',
    ...targetAttributes(target),
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
  // Accepting an inbound traceparent means trusting a peer not to forge one.
  // gRPC and every HTTP instrumentation make the same call, and the
  // mitigation belongs at ingress; defaulting to false would mean end-to-end
  // tracing did nothing out of the box.
  const trustRemote = telemetry.trustRemoteContext !== false;
  const propagator = resolvePropagation(telemetry);

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

  // The parent context carried by the packet, or null. Extraction is what
  // links a client span in one process to the server span in another.
  const extract = (packet) => {
    if (!propagator || !trustRemote) return null;
    if (typeof packet?.[TRACEPARENT] !== 'string') return null;
    try {
      const root = propagator.context?.active?.() ?? undefined;
      return propagator.propagation.extract(root, packet, GETTER) ?? null;
    } catch {
      return null;
    }
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
      return startSpanWith(tracer, `${target}${suffix}`, { kind, attributes }, extract(packet), handle, fn);
    },

    recordError(handle, error, code) {
      recordSpanError(handle, error, code);
    },

    endSpan(handle, attributes) {
      endSpanHandle(handle, attributes);
    },

    // Fires with or without a tracer: a meter-only configuration is a
    // supported way to run wrpc.
    recordCall(target, status, code, elapsed) {
      try {
        const attributes = { 'rpc.system': 'wrpc', ...targetAttributes(target), 'wrpc.status': status };
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
        subscriptions?.add(delta, targetAttributes(target));
      } catch {}
    },

    recordSubscriptionValues(count, target) {
      try {
        if (count > 0) subscriptionValues?.add(count, targetAttributes(target));
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

module.exports = { createServerTelemetry };
