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
  let heartbeat = null;
  let calls = null;
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
      // The app-level ping/pong pair is an exact round trip the client
      // already measures the ends of — the one true latency signal it has
      // that does not need a call to produce it, and the only one that
      // keeps reporting while the application is idle.
      heartbeat = meter.createHistogram('wrpc.client.heartbeat.rtt', {
        unit: 'ms',
        description: 'Round-trip time of the app-level ping/pong',
      });
      // `rpc.client.duration` alone could not answer "what fraction of my
      // calls fail": a histogram records nothing for a call whose elapsed
      // time is unknown, so error RATE was not derivable from it.
      calls = meter.createCounter('wrpc.client.calls', {
        unit: '{call}',
        description: 'RPC calls made, by outcome',
      });
    } catch {
      // Every instrument of this block, including the new ones: a meter
      // whose factory throws must leave no half-initialized writer behind.
      duration = null;
      reconnects = null;
      heartbeat = null;
      calls = null;
    }
  }
  let refreshes = null;
  if (hasMethod(meter, 'createCounter')) {
    try {
      refreshes = meter.createCounter('wrpc.client.refreshes', {
        unit: '{run}',
        description: 'Credential refresh runs, by outcome',
      });
    } catch {
      refreshes = null;
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

    /**
     * Writes the active trace context into a plain header bag under the
     * REAL W3C names (traceparent/tracestate) — the REST leg's carrier,
     * where there is no packet to hold tp/ts. The propagator's default
     * setter does exactly that, so no adapter is needed.
     */
    injectHeaders(carrier) {
      if (!propagator) return;
      try {
        const active = propagator.context?.active?.();
        propagator.propagation.inject(active, carrier);
      } catch {}
    },

    recordError(handle, error) {
      recordSpanError(handle, error);
    },

    endSpan(handle, attributes) {
      endSpanHandle(handle, attributes);
    },

    recordCall(target, status, elapsed) {
      const attributes = { 'rpc.system': 'wrpc', ...targetAttributes(target), 'wrpc.status': status };
      try {
        calls?.add(1, attributes);
      } catch {}
      if (elapsed === undefined) return;
      try {
        duration?.record(elapsed, attributes);
      } catch {}
    },

    // `outcome` is 'ok' for a pong that answered our ping, 'timeout' for one
    // that never came. A timeout records no sample: there is no round trip
    // to measure, and a made-up one would poison the percentiles.
    recordHeartbeat(outcome, rtt) {
      try {
        if (outcome === 'ok') heartbeat?.record(rtt);
      } catch {}
    },

    recordReconnect(outcome, attempt) {
      try {
        // The attempt COUNT stays off the attributes: an unbounded integer
        // as a label is a new time series per value. Every SCHEDULED attempt
        // is recorded once (outcome 'attempted'), so the rate of that series
        // is the reconnect pressure an operator alerts on; 'recovered' and
        // 'exhausted' are the terminal markers saying how episodes end — a
        // storm that keeps recovering is no longer invisible.
        void attempt;
        reconnects?.add(1, { 'wrpc.reconnect.outcome': outcome });
      } catch {}
    },

    recordRefresh(outcome) {
      try {
        refreshes?.add(1, { 'wrpc.refresh.outcome': outcome });
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
