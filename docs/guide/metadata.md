# Connection & call metadata

Three different things answer to the word "meta" in wrpc. They are deliberately
separate channels with separate contracts:

| Channel | Phase | Validated? | Read as |
| --- | --- | --- | --- |
| `headers` (client option) | connection only | **yes** — a procedure's [`schema.headers`](./rest#the-schema-option) | `client.meta.headers` / `context.meta.headers` |
| `meta` (client option + per call) | connection **and** every call | **no** — deliberately outside `validation` | `client.meta.data` (connection), `context.callMeta` (call) |
| [`procedure({ meta })`](./router#procedures) | declared by the router | — | `context.procedure.meta` |

Everything on this page is a **label**: something to log, meter, gate a
feature on, or route a support ticket by. None of it is an authorization
input — authorization is the [session's](./sessions) job, and nothing here
substitutes for one.

## What the server always observes

Every `Client` carries a frozen snapshot of what the peer presented when the
connection was made, whether or not the client declared anything:

```js
client.meta = {
  headers,       // request/upgrade headers ({} on a worker port)
  data,          // the client's declared `meta` option ({} when none)
  url,           // the request/upgrade URL with its query string
  remoteAddress,
  protocol,      // the negotiated WebSocket subprotocol ('' off ws)
};
```

`context.meta` mirrors it, the way `context.session` mirrors the session.
The whole object and both nested bags are frozen; `headers` and `data` are
null-prototyped, so a peer-chosen name like `toString` reads as `undefined`
rather than a real function.

## Declared headers: the `headers` client option

```js
const client = await connect(url, {
  headers: () => ({ 'x-app-version': pkg.version, 'x-device': deviceId }),
});
```

Re-evaluated on **every** open — function form included — so a reconnect
presents fresh values instead of ones frozen at construction. How they
travel depends on the transport:

| Transport | Carrier |
| --- | --- |
| `http` | real request headers, on every packet POST and REST leg |
| `sse` | real request headers, on the stream GET and every POST |
| `ws` | **one query parameter** (`wrpc_h`) on the connect URL |
| `event` (worker) | a field in the `wrpc:connect` message |

The ws exception exists because the WHATWG `WebSocket` constructor takes no
headers — in the browser by specification, and in Node because the client
uses the same `globalThis.WebSocket`. The server compensates with its read
order: **observed upgrade headers first, the query only for names they do
not carry** — so a custom transport that *can* set real upgrade headers
(registered via `WrpcClient.transport`) needs no query at all, and the query
can never override a real header.

The query path is sanitized server-side, and every rule is a refusal, never
an error — an oversize or malformed label leaves the connection with no
label, not without a connection:

- capped on the **encoded** length (`metaMaxBytes`, default 2048);
- a flat `string → string` map only; names lowercased (node lowercases
  observed names, and `schema.headers` must see one casing);
- reserved names dropped: `cookie`, `host`, `origin`, and the `sec-`,
  `content-`, `proxy-`, `x-wrpc-` prefixes. On http/sse `fetch` itself
  refuses to send these, so the deny list exists exactly for the query path
  — without it a peer could spoof `cookie` through the URL;
- an own `__proto__` key never carried over.

::: warning The ws form lands in logs
The connect URL — query included — ends up in proxy access logs and the
browser's network panel. A device id belongs there; a token does not. For
credentials, use the [`authenticate` hook](./client#authenticating) and the
session, where the ws leg's default carrier is the cookie precisely because
of this.
:::

### Gating the handshake

Declared headers arrive with the upgrade request, so `verifyClient` can
refuse a client **before** the connection exists:

```js
const server = new Server({
  router,
  ws: {
    verifyClient: ({ req }) => {
      const declared = new URL(req.url, 'http://x').searchParams.get('wrpc_h');
      const headers = declared ? JSON.parse(declared) : {};
      return supported(headers['x-app-version'] ?? req.headers['x-app-version']);
    },
  },
});
```

For per-procedure requirements, declare [`schema.headers`](./rest#the-schema-option)
instead — it validates `context.meta.headers` with the injected ajv and
answers 400 with `/headers`-prefixed issues.

### CORS for custom names

`x-wrpc-meta` is already on the default `Access-Control-Allow-Headers` list.
A **custom** declared header on http/sse is a real request header, so a
cross-origin client's preflight fails until you name it:

```js
cors: { origins: [...], headers: 'Content-Type, x-wrpc-channel, last-event-id, x-wrpc-meta, x-app-version' }
```

Forget this and the metadata silently never arrives — the browser refuses
the request before it is sent.

## Unvalidated `meta`: connection and per call

The `meta` channel never runs through the `validation` option — by design.
It is a label for cross-cutting hooks (an idempotency key, a per-call trace
tag, an A/B cohort), and a schema on it would turn a cross-cutting concern
into something every procedure has to know about. A hook that wants checks
writes them in its own `onRequest`.

**Connection phase** — the option mirrors `headers`, lands on
`client.meta.data`:

```js
const client = await connect(url, { meta: { v: pkg.version, locale } });
```

Carried by the `x-wrpc-meta` request header (http/sse; percent-encoded
JSON), the `wrpc_meta` connect-URL parameter (ws), or the `wrpc:connect`
message (worker).

**Per call** — an optional `meta` field on `call`, `subscribe` and `event`
packets, surfaced as `context.callMeta`:

```js
// The escape-hatch spelling...
await client.call('orders/create', args, { meta: { idem: key } });
// ...and the bound variant on a scaffolded method.
await client.api.orders.create.withMeta({ idem: key })(args);
```

```js
// Server side — a frozen EMPTY object when the packet carried none:
onRequest: async (context) => {
  context.log.info({ idem: context.callMeta.idem, method: context.method });
},
```

A REST caller (curl, another service) passes the same thing over headers —
for a per-request client the connection *is* the call, so they double as
`context.callMeta`. Two spellings are accepted:

```bash
# The prefixed form — the S3 x-amz-meta-* idiom, one header per key,
# nothing to encode. Values arrive as STRINGS (the same by-design
# semantics as REST query args) and keys are lowercased by HTTP itself.
curl -H 'x-wrpc-meta-idem: 9f3c' -H 'x-wrpc-meta-locale: de-CH' …

# The canonical form — percent-encoded JSON in ONE header. Type-faithful
# (numbers stay numbers), case-preserving, and one stable name on the CORS
# allowlist — which is why the wrpc client emits this one, and why it wins
# a key collision with the prefixed form.
curl -H "x-wrpc-meta: $(node -p 'encodeURIComponent(JSON.stringify({ idem: \"9f3c\" }))')" …
```

The prefixed form exists for humans and infrastructure — gateways can
inject or strip individual keys without JSON surgery. Browser cross-origin
callers should stay on the canonical header: with credentials, CORS has no
header wildcards, so every `x-wrpc-meta-<key>` name would need its own
`Access-Control-Allow-Headers` entry. The one place per-call
meta does not reach is the client's **mapped REST leg** (a procedure with
`http` called over the http transport): there the connection-phase header
already rides every request, and the packet field has no packet to ride.

Sanitizing is shared with the connection phase: a plain object or nothing,
capped by `metaMaxBytes` on the serialized size, own `__proto__` dropped
(so application code can spread the bag safely), frozen. A refused label
yields the empty default — the call itself always proceeds.

## Sharing metadata across a cluster

`client.meta` is **not** replicated into cluster
[`ClientDescriptor`s](./cluster) — descriptors travel per selected client on
every `fetchClients`, and replicating an arbitrary peer-controlled object
would multiply cross-node message size by the fan-in. `client.data` is the
replicated application bag; copy the two fields you care about, bounded and
explicit:

```js
hooks: {
  onConnect: (client) => {
    client.data.appVersion = client.meta.headers['x-app-version'] ?? null;
  },
},
```
