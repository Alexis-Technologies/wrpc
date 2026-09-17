'use strict';

// The server half: spans for calls, subscriptions and inbound events, plus
// the instruments a server has anything to say about. Browser-reachable
// through PeerHost (the webrtc browser entry), so it counts against that
// entry's budget in scripts/size.js.

const {
  SPAN_KIND_SERVER,
  TRACEPARENT,
  GETTER,
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
  // A host-built client (a broker binding) adds its own semantic
  // attributes — `messaging.*` for a consumed message.
  const extra = client.spanAttributes;
  if (extra) for (const key in extra) attributes[key] = extra[key];
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
  let backplaneGaps = null;
  let sessions = null;
  let sseChannels = null;
  let clusterMessages = null;
  let clusterRequests = null;
  let clusterInstances = null;
  let sseEvents = null;
  let rtcLinks = null;
  let rtcRedials = null;
  let rtcRestarts = null;
  let brokerDeliveries = null;
  let brokerPublished = null;

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
      backplaneGaps = meter.createCounter('wrpc.server.backplane.gaps', {
        unit: '{envelope}',
        description: 'Backplane envelopes a publisher sent that this instance never received',
      });
      backpressure = meter.createCounter('wrpc.server.backpressure', {
        unit: '{event}',
        description: 'Times a producer parked waiting for the transport to drain',
      });
      sessions = meter.createCounter('wrpc.server.sessions', {
        unit: '{operation}',
        description: 'Session operations, by kind and result',
      });
      clusterMessages = meter.createCounter('wrpc.cluster.messages', {
        unit: '{message}',
        description: 'Backplane envelopes received, by type',
      });
      clusterRequests = meter.createCounter('wrpc.cluster.requests', {
        unit: '{request}',
        description: 'Cluster requests settled, by op and completeness',
      });
      rtcRedials = meter.createCounter('wrpc.rtc.redials', {
        unit: '{attempt}',
        description: 'Redials (initiator) and knocks (responder) after a peer link failed, by role',
      });
      rtcRestarts = meter.createCounter('wrpc.rtc.ice_restarts', {
        unit: '{restart}',
        description: 'ICE restarts on peer links, by outcome',
      });
      sseEvents = meter.createCounter('wrpc.server.sse.events', {
        unit: '{event}',
        description: 'SSE channel lifecycle events (open/reattach/replay/gap/expired)',
      });
      brokerDeliveries = meter.createCounter('wrpc.broker.deliveries', {
        unit: '{message}',
        description: 'Broker messages consumed into procedures, by broker and settlement',
      });
      brokerPublished = meter.createCounter('wrpc.broker.published', {
        unit: '{message}',
        description: 'Messages published to a broker, by broker and outcome',
      });
    } catch {
      duration = null;
      calls = null;
      subscriptionValues = null;
      broadcasts = null;
      recipients = null;
      streamBytes = null;
      backpressure = null;
      backplaneGaps = null;
      sessions = null;
      clusterMessages = null;
      clusterRequests = null;
      sseEvents = null;
      rtcRedials = null;
      rtcRestarts = null;
      brokerDeliveries = null;
      brokerPublished = null;
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
      rtcLinks = meter.createUpDownCounter('wrpc.rtc.links', {
        unit: '{link}',
        description: 'Open peer links, by role',
      });
      clusterInstances = meter.createUpDownCounter('wrpc.cluster.instances', {
        unit: '{instance}',
        description: 'Peer instances this node currently sees on the backplane',
      });
    } catch {
      connections = null;
      subscriptions = null;
      sseChannels = null;
      clusterInstances = null;
      rtcLinks = null;
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
    withSpan({ client, packet, target, kind, suffix = '' }, fn) {
      const handle = { span: null, error: false };
      if (!tracer) return fn(handle);
      const attributes = buildCallAttributes(client, packet, target, includeIdentity);
      // An explicit kind wins; otherwise the client's own (a broker consumer
      // binding marks its client CONSUMER), SERVER for everything else.
      const spanKind = kind ?? client.spanKind ?? SPAN_KIND_SERVER;
      return startSpanWith(tracer, `${target}${suffix}`, { kind: spanKind, attributes }, extract(packet), handle, fn);
    },

    /**
     * A span that is not a packet's: a message published to (PRODUCER) or
     * taken from (CONSUMER) a broker outside the dispatcher. `carrier` is the
     * packet-shaped `{ tp, ts }` a consumed message arrived with; `fn`
     * receives the handle and runs with the span active, so an `inject`
     * inside it writes this span's context into the outgoing headers.
     */
    withMessagingSpan({ name, kind, attributes, carrier = null }, fn) {
      const handle = { span: null, error: false };
      if (!tracer) return fn(handle);
      return startSpanWith(tracer, name, { kind, attributes }, carrier ? extract(carrier) : null, handle, fn);
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

    recordBackplaneGap(channel, missed) {
      try {
        backplaneGaps?.add(missed, { 'wrpc.channel': channel });
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

    recordSseEvent(kind) {
      try {
        // A closed kind set: open | reattach | replay | gap | expired. The
        // gap and expired series are REAL event loss — what replay sizing
        // is tuned from.
        sseEvents?.add(1, { 'wrpc.sse.event': kind });
      } catch {}
    },

    recordClusterMessage(type) {
      try {
        // The envelope type set is closed (hello/state/delta/bye/e/cmd/q/a)
        // — a bounded label, unlike anything peer-named.
        clusterMessages?.add(1, { 'wrpc.cluster.type': type });
      } catch {}
    },

    // The WebRTC peer layer. `role` is 'initiator' or 'responder', an
    // ICE restart's outcome is 'requested', 'recovered' or 'failed'.
    recordRtcLink(delta, role) {
      try {
        rtcLinks?.add(delta, { 'wrpc.rtc.role': role });
      } catch {}
    },

    recordRtcRedial(role) {
      try {
        rtcRedials?.add(1, { 'wrpc.rtc.role': role });
      } catch {}
    },

    recordRtcRestart(outcome) {
      try {
        rtcRestarts?.add(1, { 'wrpc.rtc.outcome': outcome });
      } catch {}
    },

    // `outcome` is a closed set: ack | retry | release | dead (consumed) and
    // ok | error (published) — bounded labels, never a queue name a peer
    // could multiply.
    recordBrokerDelivery(system, outcome) {
      try {
        brokerDeliveries?.add(1, { 'messaging.system': system, 'wrpc.broker.outcome': outcome });
      } catch {}
    },

    recordBrokerPublish(system, outcome) {
      try {
        brokerPublished?.add(1, { 'messaging.system': system, 'wrpc.broker.outcome': outcome });
      } catch {}
    },

    recordClusterRequest(op, complete) {
      try {
        clusterRequests?.add(1, { 'wrpc.cluster.op': op, 'wrpc.cluster.complete': complete === true });
      } catch {}
    },

    recordClusterInstances(delta) {
      try {
        clusterInstances?.add(delta);
      } catch {}
    },

    /**
     * Writes the active trace context into a node-to-node envelope as
     * tp/ts — the cluster's cross-node hop is where a trace is most
     * valuable and used to be exactly where context was dropped.
     */
    inject(carrier) {
      if (!propagator) return;
      try {
        const active = propagator.context?.active?.();
        propagator.propagation.inject(active, carrier, SETTER);
      } catch {}
    },
  };
};

module.exports = { createServerTelemetry };
