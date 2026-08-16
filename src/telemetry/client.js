'use strict';

// The client half. Smaller than the server's by design: a client owns one
// connection, so there is no fan-out, no rooms and no sessions to count.
// Browser-reachable, which is why it does not require the server half.

const {
  SPAN_KIND_CLIENT,
  SETTER,
  hasMethod,
  resolvePropagation,
  resolveTracerAndMeter,
  DISABLED,
  targetAttributes,
  startSpanWith,
  recordSpanError,
  endSpanHandle,
} = require('./shared.js');

/**
 * The client half. Smaller than the server's by design: a client owns one
 * connection, so there is no fan-out, no rooms and no sessions to count.
 *
 * Its distinguishing job is `inject` — writing the active trace context into
 * an outgoing packet, which is what makes a client span in one process the
 * parent of a server span in another.
 */
const createClientTelemetry = (telemetry) => {
  if (!telemetry || typeof telemetry !== 'object') return DISABLED;

  let tracer = null;
  let meter = null;
  try {
    ({ tracer, meter } = resolveTracerAndMeter(telemetry));
  } catch {
    return DISABLED;
  }
  if (!tracer && !meter) return DISABLED;

  const propagator = resolvePropagation(telemetry);

  let duration = null;
  let reconnects = null;
  let connections = null;
  if (hasMethod(meter, 'createHistogram') && hasMethod(meter, 'createCounter')) {
    try {
      duration = meter.createHistogram('rpc.client.duration', {
        unit: 'ms',
        description: 'Duration of RPC calls made by this client',
      });
      reconnects = meter.createCounter('wrpc.client.reconnects', {
        unit: '{attempt}',
        description: 'Reconnect attempts, by outcome',
      });
    } catch {
      duration = null;
      reconnects = null;
    }
  }
  if (hasMethod(meter, 'createUpDownCounter')) {
    try {
      connections = meter.createUpDownCounter('wrpc.client.connections', {
        unit: '{connection}',
        description: 'Live client connections',
      });
    } catch {
      connections = null;
    }
  }

  return {
    enabled: true,

    withSpan({ packet, target, suffix = '' }, fn) {
      const handle = { span: null, error: false };
      if (!tracer) return fn(handle);
      const attributes = {
        'rpc.system': 'wrpc',
        ...targetAttributes(target),
        'wrpc.packet.type': packet?.type,
      };
      if (packet?.id) attributes['wrpc.packet.id'] = packet.id;
      return startSpanWith(tracer, `${target}${suffix}`, { kind: SPAN_KIND_CLIENT, attributes }, null, handle, fn);
    },

    /**
     * Writes the active trace context into `packet` as `tp`/`ts`. A no-op
     * without a propagator — you cannot serialize W3C context without one,
     * and wrpc refuses to hand-roll that.
     */
    inject(packet) {
      if (!propagator) return;
      try {
        const active = propagator.context?.active?.();
        propagator.propagation.inject(active, packet, SETTER);
      } catch {}
    },

    recordError(handle, error) {
      recordSpanError(handle, error);
    },

    endSpan(handle, attributes) {
      endSpanHandle(handle, attributes);
    },

    recordCall(target, status, elapsed) {
      try {
        if (elapsed !== undefined) {
          duration?.record(elapsed, { 'rpc.system': 'wrpc', ...targetAttributes(target), 'wrpc.status': status });
        }
      } catch {}
    },

    recordReconnect(outcome, attempt) {
      try {
        // The attempt COUNT stays off the attributes: an unbounded integer
        // as a label is a new time series per value. The outcome is the
        // dimension; the count is the metric's own value.
        void attempt;
        reconnects?.add(1, { 'wrpc.reconnect.outcome': outcome });
      } catch {}
    },

    recordConnection(delta) {
      try {
        connections?.add(delta);
      } catch {}
    },
  };
};

module.exports = { createClientTelemetry };
