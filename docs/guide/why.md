# Why wrpc?

wrpc is a WebSocket-based RPC protocol with **no runtime dependencies at all** —
`package.json` has no `dependencies` field, and no `peerDependencies` or
`optionalDependencies` either, because those are dependencies with extra steps.

That constraint is the design, not a boast. It is what forces every
integration — uWebSockets.js, Redis, Fastify, Express, TanStack Query,
OpenTelemetry, Pino — to be **injected** and duck-typed at the boundary rather
than imported. The integrations you do not use cost you nothing, and the ones
you do use run on your version, not ours.

## What you get on one connection

Most stacks make you choose a lane: an RPC layer *or* a realtime layer, typed
calls *or* binary transfer. wrpc puts them on one socket and one protocol:

| | |
| --- | --- |
| [Calls](./router) | Router of procedures, access levels, Standard Schema validators, timeouts, queues. |
| [Events & rooms](./rooms) | Fire-and-forget in both directions, plus acks when you need an answer. |
| [Subscriptions](./subscriptions) | Async-generator feeds with real backpressure and **resume after reconnect**. |
| [Binary streams](./streams) | Upload and download interleaved with everything else, backpressure into TCP. |
| [Scaling](./scaling) & [cluster](./cluster) | Rooms across instances over any pub/sub; presence, introspection, commands. |
| [REST](./rest) | Declare `http: { method, path, status }` on a procedure: real endpoints, `/vN` version paths, fastify schemas, OpenAPI via the CLI. |
| [Hooks](./hooks) | Named lifecycle phases at router, unit and procedure level — no `(ctx, next)` middleware. |
| [Sessions & auth](./sessions) | Cookie or bearer, pluggable stores and carriers, restored on reconnect; `authenticate`/`refresh` client hooks. |
| [Types](./typed-client) | A contract you declare or [generate](./cli) — no build step, no TypeScript at runtime; typed server→client events. |

## Fast where it counts

wrpc is not the fastest way to echo one message over a bare socket —
nothing with a router, an access check and a correlation ID between the
wire and your handler can be. It's fast at the things that are actually
hard to make fast:

- **Broadcasting.** A room's update is serialized, framed and compressed
  **once per emit**, not once per member, so throughput climbs with the
  room instead of collapsing under it — compressed fan-out that used to
  cost one `deflateRaw` per recipient now costs one per emit, a ~34×
  difference on the same room.
- **Payloads that aren't toy-sized.** Bare-socket benchmarks favor tiny
  messages; at 10 KB, wrpc is at or ahead of every raw WebSocket library
  measured against it, because the send path never re-encodes or
  re-copies what it already built.
- **Failure.** An instance can die with clients on it and, with a shared
  session store and any pub/sub as a backplane, those clients reconnect,
  re-authenticate and rejoin their rooms inside a single browser repaint
  — no sticky load balancer, no manual failover choreography.

None of that needed a flag. The knobs that trade memory or latency for
more — context takeover, async compression, client-side batching, the uws
engine — are opt-in on purpose: the right default for a chat app is not
the right default for a market-data feed, and a library that picks one
universal "fast" setting is guessing on your behalf. See
[Performance](./performance) for the full picture — the numbers, what they
mean in practice, and which knob to reach for and when.

## Against the alternatives

| | **wrpc** | **tRPC** | **Socket.IO** |
| --- | --- | --- | --- |
| Transport | WebSocket, HTTP, SSE (full duplex, resume), worker port | HTTP, WebSocket, SSE (subscriptions only, via `httpSubscriptionLink`) | WebSocket + long-poll fallback |
| Types | contract-first + codegen, no build step; typed server→client events | TypeScript inference | `ServerToClientEvents` maps (hand-written) |
| Realtime | subscriptions, events, rooms, acks | subscriptions | events, rooms, acks |
| HTTP/REST surface | declared per procedure: verbs, `/vN` paths, schemas, `--openapi` | third-party (`trpc-to-openapi`) | ❌ |
| Binary | streams with backpressure | ❌ | ❌ (messages only) |
| Scaling | rooms backplane (any pub/sub) | your own | adapters |
| Cluster ops | `fetchClients`, commands, presence (local read) | your own | `fetchSockets` (round-trip) |
| Observability | structured logging + OTel spans/metrics built in (SDK injected) | your own | your own |
| Cross-cutting | named lifecycle hooks | `(ctx, next)` middleware | middleware |
| Runtime deps | **0** | a few | several |
| Needs TypeScript | ❌ | effectively yes | ❌ |

See [Performance](./performance) for the receipts on all of the above — the
comparison table against raw sockets and other frameworks, the honest
footnotes (tRPC's sequential row is a flush-timer artifact, not throughput),
and how to reproduce every number on your own hardware.

## When **not** to use wrpc

An honest list is more useful than a feature grid.

- **You want an API wrpc does not shape.** A procedure can declare a real
  [REST mapping](./rest) — proper verb, path and status, `/vN` version
  paths, fastify-shaped schemas, `wrpc types --openapi` — but the URL
  surface follows your router, and content negotiation, HATEOAS and
  non-JSON payloads are out of scope. The caveat that remains is about the
  *conventional* `/:unit/:method` fallback: that one is a convenience for
  reaching procedures, not an API design. If the HTTP contract itself is
  the product, design it first.
- **Your stack is polyglot.** The [protocol](../reference/protocol) is
  documented and small enough to reimplement, but the only implementation today
  is JavaScript. gRPC exists for a reason.
- **You need guaranteed delivery.** Rooms are at-most-once fan-out, not a
  queue. [Subscriptions](./subscriptions#resuming) with `tracked()` values and
  an event log let a client catch up; if you need a broker's guarantees, use a
  broker.
- **You want end-to-end type inference from server code.** tRPC's model — where
  the client's types come from the server's implementation with nothing written
  twice — is genuinely nicer if your whole stack is TypeScript. wrpc's contract
  is declared or generated, deliberately, because it must also work for
  JavaScript users and across a network boundary the compiler cannot see.

## The other constraints worth knowing up front

- **CommonJS, shipped as-is.** No build step, no transpile; `src/` is what
  installs. `require()` and `import` both work.
- **Node.js ≥ 22** on the server; any modern browser through a bundler on the
  client. Other runtimes, honestly: the **client** (and the `query`/`auth`/
  `sse` browser halves) runs anywhere `WebSocket` and `fetch` exist — Bun,
  Deno, workers included; the **server** targets Node — `src/websocket/` is
  written against `node:net` — and `@alexify/wrpc/engine` is the documented
  seam for a `Bun.serve`/`Deno.serve` engine, with
  `tests/engine/engineContract.js` as the conformance suite a contributor
  would run.
- **One protocol implementation, both sides.** The browser build contains zero
  Node builtins, so the client talking to your server is the same code, not a
  reimplementation.
- **The wire protocol is frozen at 1.0.** An independent implementation written
  against [the reference](../reference/protocol) keeps working for the life of
  the major version.

Convinced enough to try it? [Getting Started](./getting-started) is a working
server and client in about thirty lines.
