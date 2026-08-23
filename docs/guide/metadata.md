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

## Key casing

Both declared bags — `headers` and `meta` — have their **keys normalized to
kebab-case**, on every transport and in both directions:

| You write | It arrives as |
| --- | --- |
| `userId` | `user-id` |
| `xAppVersion` | `x-app-version` |
| `XMLHttpRequest` | `xml-http-request` |
| `x-app-version` | `x-app-version` (already kebab, untouched) |
| `user_id` | `user_id` (underscores are left alone) |

One rule exists because HTTP forces it. A header name is case-insensitive
and every stack lowercases it, so `xAppVersion` would otherwise reach the
server as the unreadable run-on `xappversion` — while the same key over
WebSocket kept its capitals. Kebab-casing gives one spelling that survives
every carrier, and it is the spelling to write in
[`schema.headers`](./rest#the-schema-option).

::: warning Two consequences worth knowing
**Keys that differ only in acronym casing become the same key.** `userId`
and `userID` both reduce to `user-id`; the last one written wins. Normalize
at the source rather than relying on the difference.

**An external caller must write kebab themselves.** wrpc can only normalize
what its own client produced. By the time a hand-written
`-H 'x-wrpc-meta-userId: 1'` reaches the server, HTTP has already lowercased
it to `userid` and the word boundary is gone for good — no transform can
recover it.
:::

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

- capped on the **encoded** length (`metaMaxBytes`, default 2048). On ws the
  cap is measured over the **whole connect-URL query**, so application query
  parameters share the budget with the declared bags; the client refuses an
  oversize bag with a `meta.oversize` warning instead of sending what the
  server would silently drop whole;
- a flat `string → string` map only; names normalized to
  [kebab-case](#key-casing);
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

The per-key meta spelling has its own option, because it adds a *set* of
names rather than one — `metaHeaders` appends to the list instead of
replacing it, so the defaults cannot be dropped by accident:

```js
cors: { origins: [...], metaHeaders: ['userId', 'locale'] }  // grants x-wrpc-meta-user-id, x-wrpc-meta-locale
```

Forget either and the metadata silently never arrives — the browser refuses
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

### Choosing a spelling: `metaFormat`

```js
const client = await connect(url, { meta: { userId, locale }, metaFormat: 'prefixed' });
```

| | `'json'` (default) | `'prefixed'` |
| --- | --- | --- |
| http / sse wire | one `x-wrpc-meta` header | `x-wrpc-meta-user-id: 7`, one per key |
| ws / worker wire | one `wrpc_meta` parameter | **unchanged** — one `wrpc_meta` parameter |
| Value types | JSON (numbers stay numbers) | strings on **every** transport |
| CORS | one stable allowlist name | one entry per key — see [`cors.metaHeaders`](./server#cors) |

Reach for `'prefixed'` when something between your client and your handler
needs to read a single key: an API gateway routing on it, a WAF rule, an
access log. That is what the per-key spelling buys, and it is the same
`x-amz-meta-*` idiom S3 uses.

::: tip The guarantee is about the bag, not the wire
`'prefixed'` does not change the ws or worker wire — neither has headers.
What it changes on *every* transport is the **values**: they flatten to
strings everywhere, so the bag your handler observes never depends on which
carrier brought it. Objects are JSON-stringified; `null` and `undefined`
drop the key, because a header cannot say "absent".
:::

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
`context.callMeta`. Both spellings are accepted, the same two the wrpc
client chooses between with [`metaFormat`](#choosing-a-spelling-metaformat):

```bash
# The prefixed form — the S3 x-amz-meta-* idiom, one header per key,
# nothing to encode. Values arrive as STRINGS (the same by-design
# semantics as REST query args). Write the key in kebab-case: HTTP has
# lowercased the name before wrpc sees it, so `userId` arrives as the
# run-on `userid` and no transform can recover the boundary.
curl -H 'x-wrpc-meta-user-id: 9f3c' -H 'x-wrpc-meta-locale: de-CH' …

# The canonical form — percent-encoded JSON in ONE header. Type-faithful
# (numbers stay numbers) and one stable name on the CORS allowlist, which
# is why it is the client's default and why it wins a key collision with
# the prefixed form. Both spellings normalize to the same key, so that
# collision is real rather than two lookalike entries.
curl -H "x-wrpc-meta: $(node -p 'encodeURIComponent(JSON.stringify({ idem: \"9f3c\" }))')" …
```

The prefixed form exists for humans and infrastructure — gateways can
inject, strip or route on individual keys without JSON surgery. Its cost is
CORS: there are no header wildcards there, so a cross-origin caller needs
every key named in [`cors.metaHeaders`](./server#cors), where the canonical
header needs one entry forever.

On the client's **mapped REST leg** (a procedure with `http` called over the
http transport) the two channels are the same one, because there is no
packet for the field to ride: `withMeta` merges over the connection bag and
travels as request headers, the per-call half winning a key collision. A
REST request carries exactly one call, so nothing has to be aggregated.

### Per-call meta under batching

Over the http transport with [`batch`](./client#batching) enabled, several
calls share one POST — and one POST has one header block. So the headers
carry an **aggregate**: every call's meta merged in order, last write
winning. The two channels do different jobs:

| Channel | Carries | Under a batch |
| --- | --- | --- |
| the packet's `meta` field → `context.callMeta` | each call's exact meta | **untouched** — the source of truth |
| request headers → `context.meta.data` | the request's aggregate | lossy summary |

Nothing is lost by this: the exact per-call meta is in each packet, which
batching never rewrites. The header aggregate exists for what sits *between*
the two ends — a gateway routing on a tenant id, a WAF rule, an access log —
none of which will parse a JSON body to find it. Read `context.callMeta` in
a handler; read `context.meta.data` when you want what the request as a
whole was labelled with.

If the aggregate would exceed `metaMaxBytes`, the client drops it with a
`meta.oversize` warning and sends the connection bag alone. That refusal is
deliberate: the server's own cap discards the *entire* bag, which would look
like metadata that silently stopped arriving.

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
