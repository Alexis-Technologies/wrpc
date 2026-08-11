# OpenTelemetry

Spans and metrics, from an OpenTelemetry SDK you own:

```js
const api = require('@opentelemetry/api');
const { Server } = require('@alexify/wrpc');

const server = new Server({ router, telemetry: { api }, port: 8000, protocol: 'http' });
```

Pass the same option to a client and one trace covers both sides of the wire.

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
| `wrpc.transport` | `ws`, `http`, `sse`, `event` |
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
| `wrpc.server.sessions` | Counter | `{operation}` |
| `wrpc.server.sse.channels` | UpDownCounter | `{channel}` |
| `wrpc.client.reconnects` | Counter | `{attempt}` |
| `wrpc.client.connections` | UpDownCounter | `{connection}` |

Metric attributes deliberately stay low-cardinality: the method, the status,
the transport. Packet ids and peer addresses go on spans, never on a metric
series.

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
