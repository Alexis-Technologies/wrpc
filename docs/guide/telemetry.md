# OpenTelemetry

Spans and metrics, from an OpenTelemetry SDK you own:

```js
const api = require('@opentelemetry/api');
const { Server } = require('@alexify/wrpc');

const server = new Server({ router, telemetry: { api }, port: 8000, protocol: 'http' });
```

Pass the same option to a client and one trace covers both sides of the wire
— the packet transports, the mapped **REST leg** (context rides real
`traceparent`/`tracestate` headers there), the fastify adapter's delegated
routes, and the cluster's node-to-node hop (context rides the backplane
envelope).

::: info OpenTelemetry is injected, never depended on
`@alexify/wrpc` has no dependencies and never will. The two constants it would
otherwise need `@opentelemetry/api` for — `SpanStatusCode.ERROR` and the
`SpanKind` values — are frozen by the specification and hardcoded. The
`@opentelemetry/*` packages here are devDependencies, used only by the tests.
:::

## Two injection modes

```js
telemetry: { api }                  // the @opentelemetry/api module
telemetry: { tracer, meter }        // instances you built
```

With `{ api }`, wrpc derives its own tracer and meter so everything it emits
carries the `@alexify/wrpc` instrumentation scope, and it can reach the
propagator — which is what [trace context](#trace-context) needs.

With instances, either alone is fine: tracer-only gives spans, meter-only
gives metrics. **Propagation is off** in this mode unless you also pass
`propagation` (and `context`), because serializing W3C trace context without a
propagator is not something wrpc will hand-roll.

Without a tracer and without a meter, telemetry is off and every recording
path is a no-op.

## Spans

Span names follow the OpenTelemetry `rpc.*` convention — `$service/$method`,
which is the wire `method` string verbatim — because that is what Jaeger,
Tempo and Datadog group RPC spans by.

| Span | Kind |
| ---- | ---- |
| `{unit}/{method}` | SERVER — one RPC call |
| `{unit}/{method} subscribe` | SERVER — a subscription's whole life |
| `{unit}/{method} event` | CONSUMER — one inbound event |
| `{unit}/{method}` | CLIENT — the calling side |

The call span brackets the **whole** invocation: the session wait, the access
check, input validation and the timeout race. An argument error gets an error
span and a duration sample exactly as a slow handler does.

### Attributes

| Attribute | On |
| --------- | -- |
| `rpc.system` (`'wrpc'`), `rpc.service`, `rpc.method` | every span |
| `rpc.wrpc.status_code` | 400, 403, 404, 408, 499, 500, 503 |
| `wrpc.status` | `ok`, `error`, `cancelled`, `timeout` |
| `error.type` | the error's constructor name |
| `wrpc.packet.type`, `wrpc.packet.id` | every span |
| `wrpc.transport` | `ws`, `http`, `sse`, `event`, `webrtc` |
| `wrpc.persistent` | whether the connection stays open |
| `wrpc.subscription.values` | values yielded |
| `wrpc.subscription.terminal` | `complete`, `error`, `unsubscribed` |
| `network.peer.address` | the peer's address — see below |

## Privacy

`includeIdentity` defaults to `true`. Setting it to `false` leaves
`network.peer.address` — a remote IP — off every span:

```js
telemetry: { api, includeIdentity: false }
```

**A session token is never recorded, at any setting.** A token is a
credential, not an identity, and the two do not share a switch.

## Metrics

| Metric | Type | Unit |
| ------ | ---- | ---- |
| `rpc.server.duration` | Histogram | `ms` |
| `rpc.client.duration` | Histogram | `ms` |
| `wrpc.server.calls` | Counter | `{call}` |
| `wrpc.server.connections` | UpDownCounter | `{connection}` |
| `wrpc.server.subscriptions` | UpDownCounter | `{subscription}` |
| `wrpc.server.subscription.values` | Counter | `{value}` |
| `wrpc.server.broadcasts` | Counter | `{event}` |
| `wrpc.server.broadcast.recipients` | Histogram | `{client}` |
| `wrpc.server.stream.bytes` | Counter | `By` |
| `wrpc.server.backpressure` | Counter | `{event}` |
| `wrpc.server.backplane.gaps` | Counter | `{envelope}` — envelopes a publisher sent that this instance never received, by `wrpc.channel`; see [scaling](./scaling#loss-detection) |
| `wrpc.server.sessions` | Counter | `{operation}` |
| `wrpc.server.sse.channels` | UpDownCounter | `{channel}` |
| `wrpc.server.sse.events` | Counter | `{event}` |
| `wrpc.cluster.messages` | Counter | `{message}` |
| `wrpc.cluster.requests` | Counter | `{request}` |
| `wrpc.cluster.instances` | UpDownCounter | `{instance}` |
| `wrpc.rtc.links` | UpDownCounter | `{link}` |
| `wrpc.rtc.redials` | Counter | `{attempt}` |
| `wrpc.rtc.ice_restarts` | Counter | `{restart}` |
| `wrpc.broker.deliveries` | Counter | `{message}` |
| `wrpc.broker.published` | Counter | `{message}` |
| `wrpc.client.reconnects` | Counter | `{attempt}` |
| `wrpc.client.refreshes` | Counter | `{run}` |
| `wrpc.client.connections` | UpDownCounter | `{connection}` |
| `wrpc.client.heartbeat.rtt` | Histogram | `ms` |
| `wrpc.client.calls` | Counter | `{call}` |
| `wrpc.server.queue.depth` | UpDownCounter | `{call}` |
| `wrpc.server.queue.wait` | Histogram | `ms` |
| `wrpc.server.rooms` | UpDownCounter | `{room}` |
| `wrpc.cluster.verifications` | Counter | `{envelope}` |
| `wrpc.rtc.assertions` | Counter | `{assertion}` |
| `wrpc.broker.delivery.attempts` | Histogram | `{attempt}` |

The three `wrpc.rtc.*` instruments come from a [WebRTC peer](./webrtc): open
links by `wrpc.rtc.role` (`initiator` / `responder`), redials and knocks
after a link failed by role, and ICE restarts by `wrpc.rtc.outcome`
(`requested`, `recovered`, `failed`). A peer's host half also records the
ordinary server spans and `wrpc.server.connections` under
`wrpc.transport: 'webrtc'`.

The two `wrpc.broker.*` instruments come from the
[message-broker](./brokers) bindings: messages consumed into procedures by
`messaging.system` and `wrpc.broker.outcome` (`ack`, `retry`, `release`,
`dead`), and messages published by outcome (`ok`, `error`). A consumed
message's call span is a `CONSUMER` span carrying `messaging.*` attributes,
parented on the trace context the message arrived with; a published one is a
`PRODUCER` span whose context rides in the message headers.

`wrpc.server.sse.events` labels a closed kind set — `open`, `reattach`,
`replay`, `gap`, `expired` — and the `gap`/`expired` series are **real event
loss**, the signal replay sizing is tuned from. `wrpc.client.reconnects`
counts every *scheduled* attempt (`attempted`) plus the terminal outcomes
(`recovered`, `exhausted`), so its rate is the reconnect pressure and a storm
that keeps recovering stays visible. Early HTTP/SSE refusals that happen
before any client exists (CORS 403, 404, 405, capacity 429/503) are counted
on `wrpc.server.calls` under the `<unknown>` target.

`wrpc.client.heartbeat.rtt` is the client's only true latency signal that
does not need a call to produce it: the app-level ping/pong is an exact round
trip, and it keeps reporting while the application is idle. A heartbeat that
timed out records no sample — there is no round trip to measure, and a value
invented from the timeout would say more about your configuration than about
the network — it is counted instead, as `timeout` on the same instrument's
`ok`/`timeout` outcome.

`wrpc.server.sessions` labels five operations — `create`, `restore`,
`touch`, `destroy`, `evict`, `expire` — where it once only ever said
`restore`. `evict` is the one to alert on: unlike `expire` it discards
sessions that are still live, so it means signed-in users were signed out to
stay under `maxSessions`.

`wrpc.stream.direction` finally has both values. `send` is recorded only by
the server: on the client the counter has nothing to feed and the bytes
would only cost the browser bundle.

`wrpc.broker.delivery.attempts` is recorded when a message SETTLES — on an
ack or a dead-letter, never on a retry — so it is the distribution of how
many deliveries each message took, not a triangle counting the same message
once per attempt.

Metric attributes deliberately stay low-cardinality: the method, the status,
the transport. Packet ids and peer addresses go on spans, never on a metric
series. `wrpc.server.queue.depth` carries **no** attribute at all, per
procedure or otherwise: one series per procedure is a cardinality bomb, and
the question it answers — "is this server queueing?" — is a whole-process
one.

## Queue saturation

A call that waits for a concurrency slot spends that time inside
`rpc.server.duration`, which makes a saturated queue and a slow handler look
identical on a dashboard. They are opposite problems with opposite fixes:
one wants a bigger `queue`, the other wants the handler looked at.

`wrpc.server.queue.wait` separates them. It is the time between a call
asking for a slot and getting one, sampled per queued call;
`wrpc.server.queue.depth` is how many are waiting right now. A rising
`wait` with a flat `rpc.server.duration` minus the wait is a capacity
problem; the reverse is a handler problem.

Both are recorded only when telemetry is on — with no meter there is not even
a clock read on the path.

## Units

`rpc.server.duration` and `rpc.client.duration` are in **milliseconds**,
while current OpenTelemetry semantic conventions specify seconds for RPC
duration histograms. This is a deliberate, documented divergence rather than
an oversight.

Changing the unit under the same metric name is the worst shape a breaking
change can take: nothing errors, no alert fires, and every existing
dashboard silently becomes wrong by a factor of 1000. The `@experimental`
note on the telemetry option covers *additions* to the metric set, not
redefining a series already in use. If wrpc moves to seconds it will be
under new instrument names, at a major version, with both published for one
release.

## A note on the gauges

The five UpDownCounter-backed gauges (`connections`, `subscriptions`,
`sse.channels`, `rooms`, `rtc.links`, `cluster.instances`, `queue.depth`)
are incremented and decremented at lifecycle edges rather than observed from
a registry. A missed decrement therefore leaks for the life of the process.
The edges are latched where that risk is real, but if you see a gauge that
never returns to zero on an idle server, that is the mechanism — report it
rather than working around it.

## Trace context

wrpc is a wire protocol, so the OpenTelemetry context manager alone cannot
link the two sides — the caller is in another process. `call`, `subscribe` and
`event` packets therefore carry two optional fields:

| Field | Carries |
| ----- | ------- |
| `tp` | W3C `traceparent` |
| `ts` | W3C `tracestate`, omitted when empty |

With `{ api }` on both ends, a client span becomes the parent of the server
span and one trace spans the network hop.

Both fields are optional in both directions. A peer that sends none leaves the
receiver to start a root span; a peer that does not understand them ignores
them like any other unknown field. The context is per **packet**, so each call
in a [batch](./client#batching) keeps its own parent.

wrpc does not parse or serialize the W3C format — it hands the field to your
propagator, so whichever one you configured globally (W3C, B3, Jaeger) is what
runs. The full wire description is in the
[protocol reference](../reference/protocol#trace-context).

### Untrusted peers

`trustRemoteContext` defaults to `true`, as in gRPC and every HTTP
instrumentation: an inbound `traceparent` becomes the server span's parent.

A hostile client can forge trace ids, inflating cardinality or poisoning the
trace graph. The usual mitigation is at ingress, but a server facing untrusted
clients can refuse them outright:

```js
telemetry: { api, trustRemoteContext: false }
```

Every trace then starts on your side.

### In a browser

The client injects trace context the same way. Without a
`ZoneContextManager` or `StackContextManager` from the OpenTelemetry web SDK,
`context.active()` returns the root — so the injected traceparent names the
span wrpc just created. Still correct, just not linked to the surrounding page
interaction. Register a web context manager if you want that link.

## Failures

Telemetry never breaks a call. A broken exporter, a meter that throws, a span
implementation missing half its methods, a tracer that dies before running its
callback — each is contained, and each has a test. What propagates untouched
is an error from your own handler; that is the one thing the wrapper must not
swallow.
