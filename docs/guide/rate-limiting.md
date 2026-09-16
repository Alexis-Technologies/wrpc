# Rate limiting & throttling

wrpc ships **no rate limiter** — consistent with the [zero-dependency
guarantee](./why): a token bucket is a dozen lines, and shipping one would
mean picking a policy (sliding window? fixed window? per-IP? per-session?)
every app disagrees with. What wrpc ships instead is a single seam —
[`onRequest`](./hooks) — that runs for **every** call, event and subscription
attempt on **every** transport (WebSocket, HTTP, SSE, WebTransport, WebRTC),
before the handler and before a slow bucket check can be bypassed by picking
a different transport.

This page is the map: what is capped out of the box, how to write a limiter
once and have it apply everywhere, and — because half of wrpc's transports
never pass through a host HTTP framework at all — exactly where a package
like `@fastify/rate-limit` or `express-rate-limit` can and cannot see the
traffic.

## What is already capped (not a rate limiter)

These bound *how much state one connection can hold open*, not *how often it
may call* — see [Security → Resource limits](./security#resource-limits) for
the full table:

| Limit | Bounds |
| --- | --- |
| `maxCalls` | In-flight calls per connection (`429` past it) |
| `maxSubscriptions` | Live subscriptions per connection |
| `maxBatch` | Packets in one [batch frame](./client#batching) |
| `sse.maxChannelsPerAddress` | Open SSE channels per address (`429` past it) |

A connection that opens one call at a time, waits for the reply, and repeats
as fast as the network allows is unbounded by every one of these — that is
what an actual rate limiter is for.

## The mechanism: `onRequest`

`onRequest` fires once per accepted packet, **before** session restore and
the access check, and the target procedure is already resolved — so
`context.procedure` and `context.method` are available inside it. It is
plain code, not a policy DSL:

```js
const buckets = new WeakMap();

const rateLimit = (limit = 50, windowMs = 1000) => async (context) => {
  const key = context.client; // or context.session — survives a reconnect
  let bucket = buckets.get(key);
  const now = Date.now();
  if (!bucket || now - bucket.start >= windowMs) {
    bucket = { start: now, used: 0 };
    buckets.set(key, bucket);
  }
  if (++bucket.used > limit) {
    const error = new Error('Rate limit exceeded');
    error.code = 429;
    error.expose = true;
    throw error;
  }
};

const router = defineRouter(units, { hooks: { onRequest: rateLimit(50) } });
```

See [Hooks](./hooks#the-pipeline) for the full phase list and how router,
unit and procedure hooks flatten into one pipeline.

### Different limits for different procedures

`onRequest` can only be registered at router or unit level (it runs before a
procedure-level hook would apply), but by the time it runs
`context.procedure` — including `context.procedure.meta` — is already set.
One global hook reading per-procedure config is the idiomatic way to vary
limits without registering a hook per procedure:

```js
const router = defineRouter(
  {
    auth: {
      login: procedure({
        access: 'public',
        meta: { rateLimit: { limit: 5, windowMs: 60_000 } }, // brute-force guard
        handler: loginHandler,
      }),
    },
    search: {
      query: procedure({
        meta: { rateLimit: { limit: 200, windowMs: 1000 } },
        handler: searchHandler,
      }),
    },
  },
  { hooks: { onRequest: rateLimitFromMeta } }, // reads context.procedure.meta.rateLimit
);
```

A unit-level hook (`hooks: { onRequest: ... }` inside a unit's own
definition) works too when the limit is the same for every procedure in that
unit but different from the router's default — see
[Hooks → Three levels, one flat pipeline](./hooks#three-levels-one-flat-pipeline).

### Subscription quotas

Subscriptions are gated on open, not per value — the value loop is the
hottest path in the library and stays hook-free by design. Use
`onSubscribe`/`onUnsubscribe` instead; `onUnsubscribe` fires on **every**
ending (completion, error, unsubscribe, disconnect), so a quota cannot leak.
See [Hooks → Recipe: subscription quotas](./hooks#recipe-subscription-quotas).

## Per-transport: where a host framework's limiter can (and cannot) see traffic

`onRequest` is transport-agnostic by construction — the dispatcher does not
know or care whether the packet arrived as a WebSocket frame, an HTTP body,
an SSE POST, a WebTransport stream frame, a WebTransport datagram or a
WebRTC data-channel message. A framework-level limiter (`@fastify/rate-limit`,
`express-rate-limit`, a custom uws hook, …) is different: it only sees what
passes through the host framework's own request/response cycle, and **that
differs sharply per transport**.

| Transport | Every message is a host-framework request? | A framework limiter sees |
| --- | --- | --- |
| HTTP (packet mode) & declarative REST | Yes — each call is its own request | Every call |
| SSE | Inbound only — each client→server call is a POST; the server→client stream is one long-lived GET | Every inbound call, none of the outbound push |
| WebSocket | Only the upgrade, once per connection | The handshake, never a message after it |
| WebTransport | No — the session lives on a separate HTTP/3 host the framework never runs | Nothing, not even the handshake |
| WebRTC (data channel) | No — peer-to-peer after signaling | Nothing |
| WebRTC signaling | Only if the signaling unit is itself served over HTTP/WS through a host framework | The signaling exchange, not the data channel it sets up |

### HTTP and declarative REST

Both the generic RPC endpoint (`{basePath}/:unit/:method`) and procedures
with an `http` mapping are registered as **real routes** on the host
framework — [fastify's "upgrade-through-router" design](./adapters/fastify),
express's ordinary middleware chain, or your own uws/bare-`node:http`
plumbing. A framework-level limiter therefore sees every one of these calls
individually, exactly like it would for any other route.

```js
// Fastify
const fastify = Fastify();
await fastify.register(require('@fastify/rate-limit'), { max: 100, timeWindow: '1 minute' });
await fastify.register(wrpcFastify, { router });
```

```js
// Express — an ordinary middleware in front of wrpc.handler
app.use(require('express-rate-limit')({ windowMs: 60_000, max: 100 }));
app.use(wrpc.handler);
```

One caveat on the fastify REST bridge: the routes it registers for
`http`-mapped procedures carry a fixed `config: { wrpc: true }`, not merged
with a procedure's own `meta` — so `@fastify/rate-limit`'s per-route
`config.rateLimit` override is not wired through automatically. Either
compute `max` from `request.url` in the plugin's own options, or handle
per-procedure limits with the `meta`-driven `onRequest` hook above, which
works identically for REST and packet-mode calls.

The batteries-included [`Server`](./server) owns its whole `node:http`
listener rather than composing as middleware — multiple `'request'`
listeners on a bare `http.Server` all fire for every request with no way to
short-circuit, so a third-party HTTP limiter does not compose cleanly there.
Use the `express` or `fastify` adapter instead if you need one, or rely on
the `onRequest` hook alone (it covers `Server` just as well).

### Server-Sent Events

Each client→server call over [SSE](./sse) is a real `POST` to
`rpc.eventsPath`, registered as a host-framework route the same way as any
other HTTP call — a framework limiter applies to it. The server→client
direction is a single long-lived `GET` per channel: a framework limiter
counts that connection **once**, not once per event pushed down it, so it
cannot throttle how fast the server (or a subscription/room broadcast) emits
into an open channel — that is application logic, not a rate-limiting
concern (you control what you push). Connection-count pressure on that axis
is `sse.maxChannelsPerAddress`, not a limiter (see
[Security → SSE channels](./security#sse-channels)).

### WebSocket

Only the upgrade request touches the host framework, once per connection —
`@fastify/rate-limit` or `express-rate-limit` registered in front of it can
throttle *new connections*, never the messages sent over one already open.
Every call/event/subscribe packet on an established socket goes straight to
the dispatcher, so `onRequest` is the only layer that sees them.

### WebTransport

[WebTransport](./wt) sessions are more isolated than WebSockets, not less:
the `CONNECT` handshake is accepted by an entirely separate, injected HTTP/3
host (`Http3Server`, `quico`, …) on its own port — the fastify/express
instance never runs it and has no visibility into it, not even for the
initial session. `verify` passed to `attachSession`/`acceptSessions` is the
WebTransport analogue of `verifyClient`: a one-time admission gate at
session accept, not a rate limiter.

Once accepted, the session is attached with `RpcServer.attachSocket()` —
exactly like a WebSocket — and every message on the control stream **and**
every unreliable datagram (`sendEvent(..., { unreliable: true })`) is
emitted through the identical socket contract the dispatcher reads. So
`onRequest` covers WebTransport calls, events and inbound datagrams the same
way it covers WebSocket packets, with nothing transport-specific to write.

### WebRTC

A [WebRTC](./webrtc) data channel is peer-to-peer once connected — there is
no HTTP framework anywhere in that path for a limiter to attach to. The host
side runs on a `PeerHost`, composed from the same `src/rpc/` router and
dispatcher an `RpcServer` uses, so the same `onRequest` hook (and
`meta`-based per-procedure limits) applies unchanged.

The signaling exchange that negotiates the connection is a separate concern:
it is itself ordinary wrpc procedures (`createSignalingHooks()`, relayed
through `RpcServer.sendTo`). If your signaling server is served over
HTTP/WebSocket through a host framework, a framework-level limiter sees the
signaling traffic — offers, answers, ICE candidates — but never the data
channel traffic that results from it, which stays on `onRequest` alone.

## Combining both layers

The pattern that covers every transport: a framework-level limiter in front
of whatever HTTP surface exists (protects connection/handshake churn and
plain HTTP/REST calls cheaply, before wrpc even runs), plus one
`meta`-driven `onRequest` hook for per-message throttling that applies
uniformly regardless of which transport the client is actually using:

```js
await fastify.register(require('@fastify/rate-limit'), { max: 300, timeWindow: '1 minute' });
await fastify.register(wrpcFastify, {
  router: defineRouter(units, { hooks: { onRequest: rateLimitFromMeta } }),
});
```

A client that exhausts its WebSocket budget and reconnects, falls back from
`wt` to `ws`, or moves from packet-mode HTTP to REST still hits the same
`onRequest` bucket — the framework-level limiter is defense in depth for the
surfaces it can see, not the source of truth.

## See also

- [Hooks](./hooks) — the full phase pipeline and per-level registration
- [Security → Resource limits](./security#resource-limits) — the caps that
  exist without any hook
- [Server-Sent Events](./sse), [WebTransport](./wt), [WebRTC](./webrtc) —
  per-transport detail
