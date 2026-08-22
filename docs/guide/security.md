# Security

wrpc's defaults are convenient for local development and deliberately loud
about what has to change before production. This page collects the whole
picture in one place: what is on by default, what you must configure, and
which attacks the library already refuses on your behalf.

::: tip Reporting a vulnerability
See [SECURITY.md](https://github.com/Alexis-Technologies/wrpc/blob/main/SECURITY.md).
:::

## Before production: the short list

| Do this | Why |
| --- | --- |
| Set `cors.origins` | Without it the WebSocket upgrade accepts **any** origin — and browsers do not apply the same-origin policy to WebSockets. |
| Set `cors.credentials: true` alongside it | A wildcard origin cannot carry the session cookie. |
| Set `sessions.cookie.secure` / `sameSite` | Defaults suit `http://localhost`, not the public internet. |
| Replace `MemorySessionStore` | It is a bounded in-process map; sessions vanish on restart and do not span instances. |
| Decide on `introspection` | `true` publishes your method list to anyone. `'session'` or `false` narrows it. |
| Lower `maxCalls` / `maxSubscriptions` / `maxBatch` if a connection is untrusted | The defaults are generous. |

## The trust boundary

Everything a peer sends is untrusted input: packet fields, procedure names,
args, cookies, `Last-Event-ID`, channel ids, and — with
[trace context](./telemetry) — even `traceparent`. Everything below is about
keeping that input from becoming something else.

### Connection metadata

[Declared headers and `meta`](./metadata) are peer-controlled **labels** —
never authorization inputs. wrpc holds that line structurally: both bags are
size-capped on the encoded input (`metaMaxBytes`), plain-object-checked,
`__proto__`-stripped and frozen; the ws query path cannot spoof an observed
header (observed always wins the merge, and reserved names — `cookie`,
`host`, `origin`, `sec-*`, `content-*`, `proxy-*`, `x-wrpc-*` — are dropped
from it outright); and every violation is a refusal that leaves the
connection unlabelled, never an error that leaks parsing internals. The one
rule the framework cannot enforce for you: never branch an access decision
on `client.meta` — that is the session's job.

### Access control

`access: 'session'` is the built-in gate: no [session](./sessions), `403`,
before the handler runs. It is coarse on purpose. Anything finer — roles,
tenancy, ownership — belongs in a [hook](./hooks), where a throw ends the call
with your code:

```js
const requireRole = (role) => async (context) => {
  if (!context.session?.state.roles?.includes(role)) {
    const error = new Error('Forbidden');
    error.code = 403;
    throw error;
  }
};
```

`system/introspect` is a procedure like any other, so router-level hooks apply
to it too — which is the third option beyond `introspection: 'session' | false`.

### The cross-site GET gate

A `SameSite=Lax` cookie rides along on cross-site **navigation**. Convenient
for web pages, dangerous for an RPC endpoint: an attacker's page could point a
browser at `{basePath}/account/deleteEverything` and the cookie would come
along.

So in [REST mode](./server#what-it-serves) a `GET`/`HEAD` only gets its
cookie-restored session when the request proves same-origin intent through
`Sec-Fetch-Site`. A cross-site one runs anonymously — public procedures only,
`403` for the rest. Packet-mode `POST`s are unaffected, and non-browser peers
that send no Fetch metadata keep their session.

### Origin, on the upgrade

CORS in a browser is a courtesy the browser extends. On the **WebSocket
upgrade** it is a real defence, because browsers do not apply the same-origin
policy to WebSockets: without an allowlist, any page anywhere can open a socket
to your server with the user's cookies attached. Configure `cors.origins` and
the default `verifyClient` refuses a mismatched `Origin` outright. See
[Server → CORS](./server#cors).

## Prototype pollution

Peer-controlled strings become object keys in three places, and each one is
defended rather than trusted:

- **Session state.** `context.session.state.x = v` goes through a proxy that
  uses `Object.defineProperty`, not assignment — so `state['__proto__'] = x`
  stores a value instead of swapping the object's prototype.
- **Unit and method names.** `assignKey` in the router applies the same rule at
  definition time.
- **Query parameters.** REST mode parses them with
  `Object.fromEntries(new URLSearchParams(...))`, which uses `CreateDataProperty` —
  so `?__proto__=x` lands as an own property. A hand-rolled `obj[key] = value`
  loop here would reintroduce the hole, which is why that allocation is
  deliberately not optimized away.
- **Declared metadata.** `JSON.parse` defines `"__proto__"` as an *own* data
  property and pollutes nothing by itself — but application code that
  spreads or `Object.assign`s [`client.meta`](./metadata) bags into a config
  would carry the key along, so wrpc drops it during sanitizing, and both
  bags are null-prototyped and frozen besides.

The same reasoning covers the dispatch tables: any table keyed on peer-controlled
input uses `Object.hasOwn` or a `null` prototype, so `TABLE['toString']` cannot
answer with an inherited function that quietly swallows a packet.

## Resource limits

Every limit below exists because the alternative is memory or CPU a peer can
spend on your behalf.

| Limit | Default | What it bounds |
| --- | --- | --- |
| `maxCalls` | `1000` | In-flight calls per connection — each holds a controller, a context and possibly a queue slot. |
| `maxSubscriptions` | `256` | Live subscriptions per connection. |
| `maxBatch` | `128` | Packets in one [batch frame](./client#batching). |
| `maxPayload` | 16 MiB | Largest inbound WebSocket message. |
| `maxBodySize` | 10 MiB | Largest HTTP body. |
| `maxBuffer` / `maxBackpressure` | engine | Outbound buffering before a socket is dropped. |
| `queue: { concurrency, size }` | per procedure | Work admitted to one handler; overflow answers `503`. |
| `timeout` | per procedure | A handler that never returns answers `408`. |

Over-limit paths answer with a code and stay up: `429` for too many calls,
`503` for a full queue or a draining server, `408` for a timeout. They are
[errors](../reference/errors), not disconnects.

### Compression bombs

`permessage-deflate` inflates on the receive path, where a small frame can
expand into a large buffer. Decompression is therefore capped by
`maxOutputLength` — a frame that would exceed it fails instead of allocating.
Enable deflate when bandwidth is the constraint; see the
[cost on fan-out](./performance#fan-out) before turning it on everywhere.

### SSE channels

The [SSE transport](./sse) keeps server-side state per channel, so it is
bounded on three axes: `maxChannels` (10,000), `maxChannelsPerAddress` (100 —
answering `429`), and `replayBytes` (1 MiB of retained frames per channel). A
channel whose stream dropped is held only for `retention` (30 s).

## Channel and session binding

**The server mints the SSE channel id** and hands it out once, in the `ready`
frame. The channel is bound to the cookie identity of the GET that created it,
and every re-attach and POST must present the same one — a request naming a
live id without it is refused with `403`. So a leaked id (URLs end up in logs
and referrers) is **not** a bearer token for that channel's session. An id the
server no longer holds answers `409`, and the built-in transport reacts by
starting a fresh channel.

Session cookies are built against the RFC 6265 grammar, and a name or value
outside it throws rather than being emitted — a token from an injected
`generateToken` cannot smuggle attributes or a second cookie into the
`Set-Cookie` line.

## Error messages do not leak

A 4xx is part of the protocol conversation and travels with its message: the
peer sent something wrong and deserves to know what. A **5xx is a server
internal**, so its message is replaced with the generic status text on the wire
and the real one goes to the log — unless the error explicitly opts in with
`error.expose = true`.

```js
const error = new Error('Rate limited: 100/min');
error.code = 429;   // 4xx — the message travels
throw error;
```

Stack traces never travel. See the [error reference](../reference/errors) for
the full code table.

## Subprotocol negotiation

The client offers `wrpc.v1`. A server can require it through `protocols` /
`handleProtocols` on the [engine](../reference/engine#attach-options), where
returning `false` rejects the handshake — useful when a shared listener must
tell wrpc clients apart from everything else pointed at the same port.

## What wrpc does not do for you

- **It is not a firewall.** Rate limiting beyond the per-connection caps is an
  `onRequest` [hook](./hooks#recipe-a-rate-limit) plus your own bucket.
- **It does not authenticate.** `startSession` trusts whatever your login
  handler verified.
- **It does not encrypt.** Run `protocol: 'https'`, or terminate TLS at a proxy
  and run `'http'` behind it.
- **It does not sanitize your data.** `input`/`output` validators are yours to
  write; wrpc only runs them.
