# Wire protocol

The wire protocol is deliberately small: **JSON packets** for everything
addressable, plus a **binary framing** for stream payloads. It rides on a
WebSocket, an HTTP request/response pair, or a `MessagePort` to a worker (a
Service Worker or a SharedWorker) — the packets are identical on all three,
which is what lets the same client code work behind a worker.

## Stability

**This page is the frozen 1.0 protocol.** Everything below is what
`@alexify/wrpc@1.x` speaks, and an independent implementation written against
it will keep working for the life of the major version.

What that promise covers, and what it does not:

- **Packet types, their fields and their meanings do not change** in 1.x. A
  new optional field may be added; an existing one will not change shape or
  disappear.
- **Unknown packet types are answered with a `callback` carrying code 500**,
  and unknown fields are ignored — which is what makes an additive change safe
  for an older peer.
- **Error codes keep their meanings.** New codes may appear for new failure
  modes.
- The **JavaScript API** on top of this is versioned by the package's own
  semver and is a separate promise from the wire format.

A change to any of the above is a major version, with the reasoning in the
[CHANGELOG](https://github.com/Alexis-Technologies/wrpc/blob/main/CHANGELOG.md).

## Versioning

The wire carries a revision marker, negotiated as a WebSocket subprotocol:

```
Sec-WebSocket-Protocol: wrpc.v1        (client offer)
Sec-WebSocket-Protocol: wrpc.v1        (server echo)
```

A wrpc client **offers** `wrpc.v1` by default; a wrpc server with no
app-configured `protocols`/`handleProtocols` **echoes** it back. Both sides
therefore know, before the first packet, which revision the other speaks —
and the selected name is on `connection.protocol` server-side.

The rules that keep this compatible in every direction:

- A peer that offers **nothing** gets no subprotocol and both sides speak
  this page as written — the pre-marker handshake stays valid forever.
- A server whose app configures its own `protocols` list takes over
  negotiation entirely; offering `wrpc.v1` alongside app protocols is the
  app's decision.
- A future `wrpc.v2` will be offered ALONGSIDE `wrpc.v1`
  (`Sec-WebSocket-Protocol: wrpc.v2, wrpc.v1`), so an old server picks the
  one it knows and nothing breaks. What `v2` may change is exactly what the
  stability section above says `v1` never will.
- The name `wrpc.` is reserved as a prefix: applications must not mint their
  own subprotocols under it.

HTTP and SSE requests carry no subprotocol. Their marker is the reserved
**`wrpc-version`** header: every response echoes `wrpc-version: 1`, and a
request MAY send one — revision 1 accepts and ignores it, which is exactly
what reserves the negotiation seam inside the freeze (a future revision can
branch on it without breaking a v1 peer). Beyond that, the HTTP side stays
versioned by this page (additive changes only), which is safe because every
request/response pair is self-contained.

## Framing

| Transport | JSON packets | Binary chunks |
| --------- | ------------ | ------------- |
| WebSocket | text frames | binary frames |
| HTTP | request body / response body | not available |
| SSE | `POST` body out, `data:` lines back | not available |
| Worker port | `postMessage(string)` | `postMessage(Uint8Array)` |
| WebRTC data channel | binary frames, `KIND = 0` | binary frames, `KIND = 1` |
| WebTransport | control stream, `KIND = 0` | control stream, `KIND = 1` |

A transport that cannot stay open (plain HTTP) carries calls only: events,
subscriptions, cancellation and streams need a persistent connection, and
asking for one over HTTP is an error rather than a silent no-op.

Besides packet mode (`POST {basePath}` with a packet body) and the
conventional REST mode (`ANY {basePath}/:unit/:method`, answered with a
`callback` packet), a server MAY serve **declaratively mapped routes**: a
procedure carrying an `http` descriptor (surfaced through introspection,
below) is addressable as `method {basePath}{path}`, args arrive structured
as `{ params, query, body }`, and the response is the **plain result** (or
the wire error object) with the mapped status — external REST semantics.
This is additive: a peer that ignores `http` keeps every existing mode.

Everything on this page describes the **JSON framing**, which is the
protocol. An application MAY replace the framing on both of its own ends
with an injected [wire codec](../guide/codec) — that is an opt-in
arrangement outside this page's interoperability promise: an independent
implementation is only guaranteed to interoperate over JSON. The same
holds for REST bodies: both REST modes' bodies (declared routes' plain
results and the conventional mode's callback envelopes, requests and
errors alike) MAY be re-framed — binary included — by an injected
`codec.rest` section, under the same outside-the-promise terms.

SSE is persistent but text-only, so it carries everything except binary
streams — see [Server-Sent Events](#server-sent-events) below. A WebRTC data
channel is persistent and binary but caps the size of one message, so both
packets and chunks travel fragmented under a one-byte header — see
[WebRTC](#webrtc) below. A WebTransport session is persistent and binary
but its streams carry bytes with no message boundary, so packets and chunks
travel length-prefixed on one stream — see [WebTransport](#webtransport)
below.

### Connection metadata

A client may declare connection-phase metadata at connect time. This is a
transport-level convention, not a packet: real request headers where the
transport can send them (HTTP, SSE), and two reserved query parameters on
the WebSocket connect URL where it cannot — `wrpc_h` (declared headers) and
`wrpc_meta` (declared data), each a percent-encoded JSON object. A peer that
sends none is perfectly normal, and a server that ignores them is
conformant. A server that consumes them MUST treat them as untrusted labels:
size-capped, sanitized, and never able to override an observed request
header — wrpc's own implementation reads observed headers first and drops
reserved names (`cookie`, `host`, `origin`, `sec-*`, `content-*`, `proxy-*`,
`x-wrpc-*`) from the query path outright. The `x-wrpc-meta` request header
(percent-encoded JSON) carries per-request metadata for plain HTTP callers,
with `x-wrpc-meta-<key>: <value>` accepted as an equivalent per-key
spelling — string values only, the JSON header winning a key collision. A
conformant client MAY emit either; on a transport without headers the
prefixed *mode* still travels as the `wrpc_meta` parameter, because the
guarantee is about the bag the server observes, not the wire.

Keys of both declared bags are normalized to **kebab-case**
(`userId` → `user-id`), so one spelling addresses a value whatever carrier
brought it and `schema.headers` has a single casing to validate. Because
both spellings reduce to the same key, a collision between them is real
rather than two lookalike keys. A conformant server MAY apply this
normalization; a caller writing header names by hand must write kebab
itself, since HTTP lowercases header names before the server observes
them and the word boundary cannot be recovered afterwards.

## Packets

Every packet is a JSON object with a `type`. Unknown types are answered with
a `callback` carrying code 500.

### `call` — client → server {#call-client-server}

```json
{ "type": "call", "id": "b1f0…", "method": "chat/send", "args": { "text": "hi" } }
```

`method` is `unit/name` or `unit.vN/name` (`auth.v1/signIn`); the version defaults to `*`.
`id` correlates the answer and is generated by the caller.

`call`, `subscribe` and `event` packets may additionally carry an optional
**`meta`** field — a plain JSON object of caller-declared, unvalidated
per-invocation metadata (an idempotency key, a locale, an A/B cohort):

```json
{ "type": "call", "id": "b1f0…", "method": "orders/create", "args": { … },
  "meta": { "idem": "9f3c…" } }
```

A server that understands it surfaces the (sanitized: plain-object-only,
size-capped, frozen) bag to handlers and hooks; one that does not ignores it
like any other unknown field. It is a label, never an authorization input,
and deliberately outside schema validation. The trace-context fields
`tp`/`ts` (below) are separate on purpose — they belong to the telemetry
propagator, not to the application.

A `call` packet may also carry an optional **`timeout`** field — the
caller's per-call deadline in milliseconds (`CallOptions.timeout`). A server
that understands it uses it to *shorten* the procedure's own time budget
(never to widen it — the caller's budget is a courtesy, the procedure's
timeout is the server's protection); one that does not ignores it, and only
the caller's local timer applies. Additive, absent unless set.

### `callback` — server → client {#callback-server-client}

```json
{ "type": "callback", "id": "b1f0…", "result": { "ok": true } }
```

```json
{ "type": "callback", "id": "b1f0…", "error": { "message": "Not found", "code": 404 } }
```

Exactly one of `result` / `error`. Error codes reuse HTTP semantics where
they fit: `400` invalid input, `403` no session (or a refused origin), `404`
unknown method, `408` timeout, `429` too many in-flight calls from one
connection, `500` handler failure or invalid output, `503` queue overflow.
Over HTTP the same code becomes the response status.

What the `message` carries depends on the class of the code. 4xx messages
are written for the caller — validation text, quota refusals — and travel
verbatim. A 5xx message is a server internal: the peer receives the status
line (`"Internal Server Error"`) and the exception text stays in the server
log, correlated by the same packet `id`. A server-side error that WANTS its
message on the wire opts in with `error.expose = true`; the errors wrpc
itself constructs (timeout, queue overflow, invalid output) are marked so.

The error object MAY carry a third, optional field — `details` — with
structured, JSON-serializable data about the failure. Validation failures
use it for their issue list:

```json
{
  "type": "callback",
  "id": "b1f0…",
  "error": {
    "message": "Invalid arguments: text is required",
    "code": 400,
    "details": { "issues": [{ "message": "text is required", "path": ["text"] }] }
  }
}
```

`details` obeys exactly the exposure rule `message` does: it travels on a
4xx (or with `expose = true`) and is stripped from a 5xx. A peer that does
not know the field ignores it, per the additive-fields rule above.

### `event` — both directions {#event-both-directions}

```json
{ "type": "event", "name": "chat/message", "data": { "text": "hi" } }
```

A plain event is **fire-and-forget in both directions**: no id, no
acknowledgement, no answer. That is what makes it cheap, and it is why a
rejected inbound event (unknown handler, missing session, failing handler,
invalid input) is recorded in the server log rather than sent back — there
is no id to answer on.

The one exception is a transport that cannot carry events at all. An event
sent over HTTP is refused with a `callback` carrying code 400 and an empty
id: fire-and-forget on a request/response transport would leave the request
unanswered forever, so it is an error rather than a silent no-op.

- **server → client**: emitted by `client.sendEvent(name, data)` or by a room
  broadcast (`server.to(room).emit(...)`). The client dispatches on the `unit`
  part of the name to `client.api[unit]`; anything that reaches no listener
  surfaces as an `unhandled-event` on the client itself.
- **client → server**: sent by `client.sendEvent('unit/name', data)` and
  handled by the unit's `on` map in the router. Handlers are procedures, so
  `access`, `input` validation and `queue` all apply.

#### Asks: an event with an `id`

```json
{ "type": "event", "name": "chat/confirm", "data": { "q": 1 }, "id": "b1f0…" }
```

A **server → client** event MAY carry an `id`. It then expects an answer:
the client MUST reply with an ordinary [`callback`](#callback-server-client)
packet carrying the same `id` — `result` with what its registered responder
returned, or `error` when it has no responder for the name (`501`) or the
responder threw (the error's `code`, default `500`). No new packet type is
involved; the field is additive, and a peer that never sends asks never
sees the difference.

Client → server events keep no id — a client asking the server is simply a
`call`. A `callback` arriving at the server that matches no pending ask is
logged and dropped, never answered: answering a callback with a callback
could ping-pong.

### `ping` / `pong` — both directions {#ping-pong-both-directions}

```json
{ "type": "ping" }
```

```json
{ "type": "pong" }
```

The application-level heartbeat. A browser `WebSocket` exposes no
protocol-level ping, so a connection that died without a close frame — a
dropped NAT mapping, a suspended laptop, a proxy that stopped forwarding —
looks perfectly open from JavaScript until the first call times out. Both
sides answer a `ping` with a `pong` immediately; the client additionally
measures the round trip and reconnects when the answer does not arrive
within its timeout. On a WebRTC link each direction has its own client and
its own channel (see [WebRTC](#webrtc)), so each direction heartbeats on its
own channel.

### `subscribe` / `data` / `end` / `unsubscribe` — a stream of values

A subscription is a procedure that answers with many values instead of one.
It needs a connection that stays open, so it is refused (400) on HTTP.

```json
{ "type": "subscribe", "id": "b1f0…", "method": "chat/onMessage", "args": {}, "lastEventId": "41" }
```

```json
{ "type": "data", "id": "b1f0…", "eventId": "42", "data": { "text": "hi" } }
```

```json
{ "type": "end", "id": "b1f0…" }
```

`end` is the terminal packet in every case — normal completion, a generator
that threw (`error: { message, code, details? }` — the same object shape,
including the optional `details`, as a callback error), and refusals such as
404, 403 or 429. `{"type":"unsubscribe","id"}` ends one early; the server aborts the
handler's `signal`, runs its `finally`, and answers `end`.

`eventId` appears only on values the handler wrapped in `tracked(id, data)`.
The client remembers the last one and sends it back as `lastEventId` when it
re-subscribes, which the handler uses to replay what was missed —
`createEventLog({ size })` is the ring buffer for exactly that. A feed of
untracked values simply has no resume point and picks up live.

Server-side backpressure is real: the pump waits for the transport to drain
before pulling the next value, so a slow consumer stops the producer instead
of filling memory.

### `cancel` — taking a call back

```json
{ "type": "cancel", "id": "b1f0…" }
```

Cancellation is best-effort by nature — a handler that never looks at
`ctx.signal` keeps running. What is guaranteed is that the caller is
rejected immediately with code **499** and that whatever the handler
eventually returns is dropped rather than delivered late. Like
`unsubscribe`, it needs a persistent connection.

### Batch frames

A JSON **array** in place of a packet is a batch: several packets in one
frame. Each is dispatched and answered on its own, so a failure inside a
batch stays attached to its own id.

```json
[{ "type": "call", "id": "a", "method": "math/double", "args": { "n": 1 } },
 { "type": "call", "id": "b", "method": "math/double", "args": { "n": 2 } }]
```

On a WebSocket the answers come back individually. On HTTP — where a request
has exactly one response — they come back as one array **in request order**,
so a caller can zip requests to responses positionally. The server caps a
frame at `maxBatch` packets (128 by default); one frame asking for unbounded
work is otherwise a denial of service.

### Trace context

`call`, `subscribe` and `event` packets may carry two optional fields that
link a client's span to the server's, so one distributed trace spans both
sides of the wire:

| Field | Carries |
| ----- | ------- |
| `tp` | W3C [`traceparent`](https://www.w3.org/TR/trace-context/#traceparent-header) |
| `ts` | W3C `tracestate`; omitted when empty |

```json
{ "type": "call", "id": "b1f0", "method": "chat/send", "args": { "text": "hi" },
  "tp": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" }
```

They are short because they ride on every call packet: `"traceparent"` would
cost eleven more bytes per call than `"tp"` for nothing.

Both fields are **optional in both directions**. A peer that does not send
one leaves the receiver to start a root span; a peer that does not understand
one ignores it, like any other unknown field. The context is **per packet,
not per frame**, so each call in a batch keeps its own parent.

An implementation is not required to parse these — wrpc itself does not. It
hands the field to whatever OpenTelemetry propagator the application
configured, which is also why a wrpc server only reads `tp` when it was given
the `@opentelemetry/api` module (or an explicit propagator) rather than a
bare tracer.

A server accepts an inbound `tp` by default, exactly as gRPC and HTTP
instrumentations do. Since a hostile peer can forge trace ids, a server that
faces untrusted clients can refuse them with `telemetry.trustRemoteContext:
false` and start every trace itself.

### `stream` — both directions

```json
{ "type": "stream", "id": "9a2c…", "name": "video.mp4", "size": 104857600 }
```

```json
{ "type": "stream", "id": "9a2c…", "status": "end" }
```

The opening packet announces a stream; `status` is `end` or `terminate` and
closes it. The payload travels as binary chunks in between.

### Binary chunks

Each binary frame is one chunk of one stream — unless its first byte is
`0x00`, which no chunk has (a stream id is at least one byte long): such a
frame is a **framed message**, its second byte the kind:

```
0x00  kind  payload
      3     a packet, compressed with the codec negotiated on ping/pong (below); the receiver inflates, then reads it as a text frame
      4     a chunk, compressed likewise; the receiver inflates, then reads it as a binary chunk
      other reserved — a 400 error packet
```

A peer MAY send a kind 3 or 4 frame only after a `pong` named the codec;
before that, or with a payload that does not inflate under the receiver's
cap, the frame is answered with an id-less `400` and the connection goes
on. Today only a Node client sends them (a browser's WebSocket compresses
both directions under permessage-deflate, and a server's own frames use
that extension too).

Each ordinary binary frame is one chunk of one stream:

```
┌────────┬──────────────────┬──────────────────────────┐
│ 1 byte │  id (idLength)   │        payload           │
│ idLen  │  utf-8 stream id │        bytes             │
└────────┴──────────────────┴──────────────────────────┘
```

Prefixing the id rather than opening a frame per stream is what lets several
streams interleave over one connection without head-of-line blocking. Byte
layout and the `chunkEncode`/`chunkDecode` helpers are in
[the wire format reference](./wire-format#binary-chunks).

## Introspection

Every server answers one procedure it did not declare: `system/introspect`,
injected unless the router already defines it. It is `access: 'public'` — a
client has to be able to ask what it may call before it has a session.

```jsonc
// -> { "type": "call", "id": "1", "method": "system/introspect", "args": ["chat"] }
// <- { "type": "callback", "id": "1", "result": { ... } }
{
  "chat": {
    "send":      { "access": "public", "meta": { "description": "Post a message" } },
    "onMessage": { "access": "session", "kind": "subscription" }
  },
  "auth.v1": { "signIn": { "access": "public" } }
}
```

`args` is an array of unit keys to filter by; **anything else means no
filter** (REST-mode calls deliver a plain object here). A unit key is `unit`
for the default version and `unit.vN` otherwise — the same string
`client.load()` takes.

Each method carries:

| Key | Meaning |
| --- | --- |
| `access` | `'public'`, `'session'`, or whatever the router declared |
| `kind` | `'subscription'`, and **only** then — a client scaffolds a call unless told otherwise |
| `meta` | The procedure's `meta`, when it is not empty. `meta.description` is the **one** home for prose — `wrpc types` turns it into a doc comment, so it does not also live in `signature` |
| `signature` | An optional descriptor, below |
| `http` | The declarative REST mapping (`{ method, path, status?, headers?, cache? }`), when the procedure carries one. `headers` are the route's static response headers, `cache` its `{ maxAge, public, staleWhileRevalidate, etag }` policy — both additive, present only when declared. |

A unit object may additionally carry two **reserved keys** — impossible to
collide with a method, since both are reserved in the router definition too:

| Key | Meaning |
| --- | --- |
| `on` | The unit's inbound (client → server) event handlers: `{ [event]: { access, signature? } }`. What `wrpc types` turns into the contract's `sends` key, typing `client.sendEvent`. |
| `emits` | The unit's *declared* outbound (server → client) events, verbatim from the router's declaration-only `emits` key: `{ [event]: { data?, returns? } }` in the `signature` shape language. What `wrpc types` turns into the contract's `events` key, typing the unit emitter and `client.respond`. |

This is what `load()` consumes to build `client.api` (the two reserved keys
are skipped — they are declarations, not methods), and what the `wrpc types`
CLI consumes to generate a contract interface — where a `kind: 'subscription'`
method becomes a `SubscriptionContract<Args, Data>` member rather than a
callable one.

Being an ordinary procedure, it is reachable over **every** transport: a
WebSocket frame, a plain `POST {basePath}` with the packet as the body (which
is how the CLI asks, needing no socket), an SSE channel, or a worker port. It
also answers in REST mode at `{basePath}/system/introspect`.

### The `signature` descriptor

`signature` describes a procedure's arguments and result well enough to
generate TypeScript from. It is deliberately a small **closed** format rather
than a schema language: it crosses the network and ends up inside a file
someone compiles, so a generator must be able to reject everything it does
not recognise.

```js
procedure({
  signature: { args: { room: 'string', 'limit?': 'number' }, returns: [{ id: 'string', text: 'string' }] },
  handler: async (context, { room }) => [...],
})
```

* `args` — what the procedure takes. Omitted means undescribed.
* `returns` — what a **call** answers with.
* `data` — what a **subscription** yields. (`returns` on a subscription and
  `data` on a call are both ignored, with a warning.)

Each of those is a *shape*, and a shape is one of three things:

| Shape | Example | TypeScript |
| --- | --- | --- |
| a type name | `'string'`, `'number[]'`, `'string\|null'` | `string`, `number[]`, `string \| null` |
| a field map | `{ id: 'string', 'note?': 'string' }` | `{ id: string; note?: string }` |
| a one-element array | `[{ id: 'string' }]` | `Array<{ id: string }>` |

A field name ending in `?` is optional. Field maps nest. The type names are
exactly `string`, `number`, `boolean`, `object`, `null`, `unknown`, `any` and
`never` — there is no `Date`, because there is no `Date` in JSON — plus `[]`
suffixes and `|` unions of those. **Anything else generates `unknown`**, so a
descriptor can never widen what a generator is willing to emit.

Validation is not part of it. A signature says what a procedure looks like;
`input`/`output` (a function or a
[Standard Schema](https://standardschema.dev)) are what actually enforce it,
and the two are deliberately separate — a schema library is the user's choice,
while this has to survive a JSON round trip.

## Sessions

A session is a token plus a state object held in a `SessionStore`. The token
travels in a cookie (`HttpOnly; Secure; SameSite=Lax` by default), which is
read on both HTTP requests and the WebSocket upgrade, so a reconnect restores
the session without a round trip.

Because a `SameSite=Lax` cookie rides along on cross-site *navigation*,
`GET`/`HEAD` requests in REST mode only get their cookie-restored session
when the request proves same-origin intent through `Sec-Fetch-Site`.
Otherwise they run as public — ambient-authority dispatch would be a CSRF
hole.

## Rooms

A room is a named set of clients. Broadcasting to one is not a new packet
type: it is the ordinary `event` packet, sent to every member.

```js
client.join('lobby');                     // from a handler: ctx.client.join(...)
server.to('lobby').emit('chat/message', { text: 'hi' });
server.to('lobby').except(ctx.client).emit('chat/message', { text: 'hi' });
server.broadcast('system/announce', { up: true });
```

`emit()` returns how many clients received the event **on this instance**.
Rooms are released automatically when a client disconnects. `to(...rooms)`
is a union — a client in several targeted rooms still gets one copy — and
`to()` with no rooms reaches **nobody**, so a computed room list that came
back empty never falls back to every connected client.

### Across instances

With a backplane configured (`new Server({ backplane })`, see
`@alexify/wrpc/scaling`) a room spans every instance sharing it. Each
non-local emit is published as an envelope:

```json
{ "v": 1, "instance": "c0ffee…", "epoch": "k3x9…", "seq": 42, "rooms": ["lobby"], "name": "chat/message", "data": { "text": "hi" } }
```

- `instance` identifies the publisher, and every instance drops its own
  envelopes — that is the echo suppression.
- `epoch` is the publisher's boot marker and `seq` its per-channel counter:
  a receiver that sees `seq` jump within one epoch knows how many envelopes
  the broker lost between them and reports the gap (`backplane.gap`, the
  `wrpc.server.backplane.gaps` metric). Additive: an envelope without them
  is delivered untracked. Detection only — the contract stays at-most-once.
- `rooms` is `null` for a broadcast to everyone.
- An emit targeting **exactly one** room is published on that room's channel
  (`room:<name>`), which only instances holding members subscribe to;
  anything else goes to the single `broadcast` channel every instance holds.
  So an envelope reaches a given instance through exactly one channel and no
  receiver-side deduplication is needed.

Delivery is **at-most-once**. A message published while an instance is
between subscriptions, or dropped by the broker, is gone: rooms are a fan-out
mechanism, not a queue.

With `rooms: { compression }` on, an envelope past the threshold is
published as `wrpc-enc:<codec id>:<base64 of the codec's output over the
JSON text>` — a message that starts with `w` rather than `{`. There is
nothing to negotiate against on a fan-out, so a receiver without the same
codec drops it (and logs); the deployment turns it on only once every
instance can read it. The cluster channels below do the same under
`cluster: { compression }`, applied after the HMAC signature.

### Cluster channels

The cluster layer (`server.cluster` — presence, introspection, node-to-node
messaging) adds two channels on the same backplane, both held for the life
of the process: `cluster`, which every instance subscribes to, and
`inst:<instanceId>`, one instance's own inbox for request answers and
addressed commands. Its envelopes are `{ v, from, epoch, t, … }`, where
`from` is the publisher (self-messages are dropped), `epoch` is the boot
marker that distinguishes a restart from a live node, and `t` selects the
message kind (presence hello/state/delta/bye, request `q`/answer `a`,
command `cmd`, node event `e`). These envelopes are an implementation
detail of `@alexify/wrpc`'s own cluster layer, not part of the frozen wire
protocol clients speak — they never reach a client connection.

## Server-Sent Events

SSE is one-way, so a channel is two halves that find each other by id:

```
GET  {basePath}/events                        opens a NEW channel
GET  {basePath}/events  x-wrpc-channel: <id>  re-attaches to an existing one
POST {basePath}         x-wrpc-channel: <id>  client -> server
```

**The channel id is minted by the server** and handed out exactly once, in
the `ready` frame that opens every stream:

```
event: ready
data: {"channel":"b1f0…"}
```

A client cannot propose its own id: a GET naming an id the server does not
hold answers `409`, which is the client's signal to drop its channel state
and start a fresh one. (`?channel=<id>` in the query string is accepted for
re-attach as well, but the header is preferred — URLs end up in proxy logs.)

The channel is **bound to the identity that created it**: the session token
in the opening GET's cookie (or "anonymous" when it carries none). Every
re-attach and every POST must present the same cookie identity — a request
that names a live channel id without it is refused with `403`. Knowing an id
is never enough to act on someone else's channel. Channel creation is also
capped (`maxChannels`, `maxChannelsPerAddress`): past the caps a new GET
answers `503` or `429`.

Both halves belong to **one** server-side client, which is what lets a
subscription opened by a POST deliver its values down the stream. A POST
answers `202` with no body: every reply, callbacks included, travels on the
stream — the same shape the worker port transport has. A POST naming
an unknown channel answers `409`, like the GET.

Each frame carries the channel's own monotonic `id:`, and a dropped stream
does not destroy the channel. It is held for `retention` (30 s by default),
so a reconnect with `Last-Event-ID` re-attaches and replays the frames it
missed instead of starting over — subscriptions and all. Comment frames
(`: ping`) keep proxies from deciding an idle response is a dead one, and
`X-Accel-Buffering: no` keeps nginx from buffering the stream into oblivion.

Serverless-friendly by construction: no upgrade, no socket beyond the
response body, nothing but HTTP in either direction. What it cannot carry is
binary — SSE frames are text, so wrpc's binary streams are refused on this
transport rather than silently corrupted.

## WebRTC

Two peers speak wrpc to each other over one `RTCPeerConnection` carrying
**two negotiated data channels**, one per direction of the protocol's
client → server relationship:

```
channel initiator   (negotiated, id 0 by default)   initiator's client  → responder's host
channel responder   (negotiated, id 1 by default)   responder's client  → initiator's host
```

The *initiator* is the peer whose id sorts first (plain string comparison);
it makes the offer, and it is the *impolite* side of
[perfect negotiation](https://w3c.github.io/webrtc-pc/#perfect-negotiation-example).
Both peers create both channels before the first offer, with the same ids
and the same label (`wrpc`) — negotiated channels are not described in the
SDP, so the two configurations must agree. The ids are configurable for an
application that keeps its own channels on the same connection.

Because each channel is one ordinary client → server wire, every packet on
this page travels unchanged: a peer runs a client on the channel it
initiates and a dispatcher on the other, and nothing in a packet says which
peer is "the server". Signaling (descriptions and ICE candidates) is
application-level and reaches the peers through any channel the
application chooses — `@alexify/wrpc/webrtc` ships one over an ordinary
wrpc connection — and is not part of the wire either.

### Data-channel framing {#webrtc-framing}

A data channel message has a size limit (16 KiB is the only size every
implementation supports; `sctp.maxMessageSize` reports what a pair actually
negotiated) and wrpc's batch frames and stream chunks are routinely larger.
So on a data channel **every message is binary** (the channel's
`binaryType` is `arraybuffer`), and a packet or chunk is sent as one or
more fragments, each under a one-byte header:

```
bit 0   KIND   0 = a wrpc packet (UTF-8 JSON — what a WebSocket text frame carries)
               1 = a binary stream chunk (a chunkEncode frame, see the wire-format page)
bit 1   FIN    1 = the last fragment of this message
bit 2   DEFLATE 1 = the message is compressed with the negotiated codec — only once negotiated
bit 3–7        reserved, MUST be 0
```

- Fragments of one message are sent back to back on one ordered, reliable
  channel, so there is no message id and no sequence number: a receiver
  concatenates fragments until FIN.
- The KIND and DEFLATE bits of a continuation MUST equal those of the
  message it continues; a set reserved bit, a DEFLATE bit before both peers
  named the same codec, a mismatched continuation, a text message that is
  not valid UTF-8, a reassembly past the receiver's cap (16 MiB by default)
  or a compressed message that does not inflate under that cap is a
  protocol error, and the receiver closes the channel — the data-channel
  analogue of a WebSocket `1002`.
- **Compression** is negotiated through signaling, since the channels have
  no handshake: a `description` signal MAY carry `caps`, a JSON object whose
  known key is `deflate` (a codec id, `"deflate-raw"` for raw DEFLATE, RFC
  1951). A peer MAY set the DEFLATE bit only once the other peer's last
  description named the same codec; the payload is then the codec's output
  over the bytes the message would otherwise carry, compressed before
  fragmentation and inflated after reassembly. Over a channel the
  application negotiated itself, whether the bit is in use is the
  application's agreement.
- Fragment size is the negotiated `sctp.maxMessageSize` capped at 256 KiB,
  and 16 KiB when nothing is reported. A peer MAY send smaller fragments.

Everything above the header is exactly the WebSocket wire: the ordering rule
"a `stream` packet precedes the first chunk with its id" holds per channel,
`ping`/`pong` run per direction, and a subscription resumes with
`lastEventId` across a renegotiated connection the way it does across a
reconnected socket.

### Trust assertions {#webrtc-assertions}

Signaling is not part of the wire, but the token a signaling server may
attach to it is a format two parties written independently must agree on
— a peer verifying, and whoever issues (the wrpc unit or any service that
can sign a JWS). An assertion is a **JWS in compact serialization**
(RFC 7515): `base64url(header).base64url(payload).base64url(signature)`.

```
header   { "alg": "ES256", "typ": "wrpc-rtc+jwt", "kid"?: string }
payload  { "sub": string,      the peer id, as the signaling layer names it
           "fp":  string,      the DTLS certificate fingerprint: "sha-256 AB:CD:…"
           "iat": number,      seconds since the epoch
           "exp": number,
           "iss"?: string,
           …any other claims }
```

- `alg` MUST be `ES256` (ECDSA over P-256 with SHA-256; the signature is
  the raw 64-byte `r || s`, as JWS specifies). A verifier MUST refuse any
  other `alg`, `none` included, and any `typ` but `wrpc-rtc+jwt`.
- `kid` selects the public key when the issuer publishes several; a
  verifier configured with a single unlabelled key accepts any `kid`.
- **Binding.** An assertion travels inside a `description` signal, as its
  `assertion` field, and is valid for that description only: `sub` MUST be
  the peer id the signaling layer reports as the sender, and `fp` MUST be
  the fingerprint the description's SDP declares (an `a=fingerprint:`
  line — algorithm, a space, colon-separated hex — compared after
  normalization: algorithm in lower case, hex in upper case). The DTLS handshake then
  proves the sender holds that certificate. A verifier MUST apply the
  description only after the assertion verified.
- `exp` MUST be in the future by the issuer's clock (a verifier SHOULD
  allow a small skew and MAY learn the issuer's clock from tokens issued to
  itself). `iss`, when the deployment sets one, MUST match.
- A token is at most 4 KiB. Anything else is a refusal; the reason is the
  verifier's business, not the wire's — the link is simply closed.

## WebTransport

A client speaks wrpc to a server over a WebTransport session (HTTP/3) the
way it does over a WebSocket: the packets are the same and travel in the
same order. **This section is experimental**: it describes revision 1 of
the carrier, may change in a minor release, and sits outside this page's
interoperability promise until it stabilizes — the way an injected wire
codec does.

The client establishes the session (an extended `CONNECT` with `:protocol
webtransport`, per the WebTransport specification) and opens **one
bidirectional stream, the control stream**; the server treats the first
bidirectional stream the client opens as such, and a session that opens
none within the server's accept window is closed with code 408. Every
packet and every stream chunk, in both directions, travels on the control
stream. The session's datagrams carry unreliable events, and — once both
ends have announced it — a binary stream's chunks travel on a stream of
their own; both are described below.

Connection metadata travels as on a browser WebSocket: the `CONNECT`
request's headers are the observed bag, and the `wrpc_h` / `wrpc_meta`
query parameters of the request path carry what the client declares. A
`CONNECT` carries no cookies (its credentials mode is `omit`), so a session
token is presented as a declared `authorization` header or a payload field
— what the bearer and payload session transports read.

### Stream framing {#webtransport-framing}

A QUIC stream is a byte stream with no message boundaries, so every message
on the control stream is prefixed with a five-byte header:

```
bytes 0–3  LENGTH  payload byte length, unsigned, big-endian
byte  4    KIND    0 = a wrpc packet (UTF-8 JSON — what a WebSocket text frame carries)
                   1 = a binary stream chunk (a chunkEncode frame, see the wire-format page)
                   2 = a capabilities message (UTF-8 JSON) — see below
                   3 = a packet, compressed with the negotiated codec — only once `deflate` was negotiated
                   4 = a chunk, compressed — likewise
                   5–255 reserved
```

- A message is one contiguous run of bytes; there is no fragmentation and
  no FIN bit, because the stream is ordered and reliable. A receiver
  buffers until LENGTH bytes have arrived, however the transport split them.
- A reserved KIND, a LENGTH past the receiver's cap (16 MiB by default) or a
  packet that is not valid UTF-8 is a protocol error: the receiver closes
  the session with code 1002 — the WebTransport analogue of a WebSocket
  `1002`.
- Close codes are the WebSocket close codes carried in the session's
  `closeCode`: a server closes with 1001 on shutdown and 1002 on a protocol
  error; an application close is 1000 or 0. Ending the control stream (a
  FIN) ends the connection, answered by a session close with 1000.

Everything above the header is exactly the WebSocket wire: the ordering rule
"a `stream` packet precedes the first chunk with its id" holds, `ping`/`pong`
run per direction, and a subscription resumes with `lastEventId` across a
new session the way it does across a reconnected socket.

### Capabilities and per-stream transport {#webtransport-streams}

Each end's **first message** on the control stream MAY be a capabilities
message (KIND 2): a JSON object whose known keys are `streams`
(`{"streams":true}`) and `deflate` (a codec id, `"deflate-raw"` for the
platform codec). Unknown keys are ignored; an end that sends none, or `{}`,
has announced nothing, and its peer treats it as a revision-1 peer.

When **both** ends announced the same `deflate` id, either MAY send a packet
as KIND 3 or a chunk as KIND 4: the payload is the codec's output over the
bytes KIND 0 or 1 would have carried, and the receiver inflates it before
reading — a KIND 3 payload is UTF-8 JSON only after inflation. The choice is
per message and the sender's (a message under its threshold, or one the
codec did not shrink, goes as KIND 0 or 1 at any time). A compressed kind
before both ends named the same codec, a payload that does not inflate, or
one that inflates past the receiver's cap is a protocol error (1002).
`deflate-raw` is raw DEFLATE (RFC 1951), no zlib or gzip wrapper. Datagrams
and chunks on their own streams are never compressed.

When **both** ends announced `streams`, a sender MAY carry a binary
stream's chunks on a **unidirectional WebTransport stream of their own**
instead of the control stream:

- The `stream` packet that opens the wrpc stream (`{ type: 'stream', id,
  name, size }`) stays on the control stream — its order against the call
  that names the id is what the control stream guarantees.
- The unidirectional stream opens with the chunk header — one byte of id
  length, then the id (exactly the prefix of a `chunkEncode` frame) — once,
  and then carries the payload bytes of every chunk of that id, in order,
  with no per-chunk header. Its FIN is the stream's `end`; a RESET (an
  aborted stream) is its `terminate`. The sender MUST NOT also send the
  `end` or `terminate` packet for such a stream on the control stream.
- A QUIC stream is ordered only against itself. A receiver MUST therefore
  hold chunks that arrive before their `stream` packet has been read from
  the control stream, and MUST deliver the synthesized `end` or `terminate`
  only after the unidirectional stream has ended — never on the strength
  of the control stream alone.
- The choice is per stream and the sender's: a stream whose chunks were
  sent on the control stream ends with an `end` packet there, exactly as in
  revision 1, and a receiver accepts both forms at any time.

### Datagrams {#webtransport-datagrams}

A session's datagrams carry **unreliable events**: a packet a sender chose
to deliver at most once, unordered, for state a later packet supersedes (a
cursor, a position). A datagram is one whole message under a one-byte
header — the KIND byte alone, since a datagram announces its own length:

```
byte 0   KIND   0 = a wrpc packet (UTF-8 JSON); other values reserved
bytes 1…        the packet
```

- Only `event` packets without an `id` MAY be sent as datagrams: a call, a
  callback, a stream packet, a chunk or an ask expects an order or an
  answer that a datagram cannot promise. A receiver handles a datagram's
  packet exactly as it would the same packet from the control stream.
- A sender MUST NOT split a packet over datagrams: a packet larger than
  the session's `maxDatagramSize` goes on the control stream instead. The
  choice is the sender's alone and invisible to the receiver; a peer with
  no datagrams sends every event on the control stream.
- A receiver ignores a datagram it cannot read (a reserved KIND, invalid
  UTF-8, an empty one) — a lost datagram is the norm, and an unreadable
  one is not worth a hangup.

## Broker binding

A client speaks wrpc to a server **through a message broker** — RabbitMQ,
NATS, Redis — instead of over a socket: the packets are unmodified wrpc
packets, carried as broker messages with a few headers around them. **This
section is experimental**: it describes revision 1 of the carrier, may change
in a minor release, and sits outside this page's interoperability promise
until it stabilizes, like the WebTransport carrier above.

The broker supplies addressable inboxes with at-most-once delivery, per-sender
ordering and optional competing groups (the `direct` capability,
[Message brokers](../guide/brokers#direct)). A message carries a body, string
headers, a `correlationId` and a `replyTo` address. The binding's own headers
all start with `wrpc-`; a peer's connection headers (`authorization`,
`x-wrpc-meta`, …) ride next to them, lower-cased, and any peer-supplied
`wrpc-*` name is dropped by the receiver.

| Header | Values |
| --- | --- |
| `wrpc-kind` | `request`, `response`, `hello`, `welcome`, `packet`, `chunk`, `bye` |
| `wrpc-seq` | a session frame's number, from `1`, per direction |
| `wrpc-inbox` | on `welcome`: the address the session's frames go to |
| `wrpc-reason` | on `bye`: why, for logs |
| `wrpc-enc` | on a `request` or `hello`: the compression codec the sender accepts (`deflate-raw`); on a `welcome`: the codec the server agreed to; on a `response`, `packet` or `chunk`: that its body is that codec's output. A marked frame a receiver cannot inflate ends the session. |

Every server instance consumes one **service address** — `wrpc.<service>` by
default — as members of one competing group, so each message addressed to the
service reaches exactly one instance.

### Stateless requests {#broker-stateless}

```
client --request {correlationId, replyTo = client inbox}--> service address
client <--response {correlationId}------------------------- the instance that took it
```

The `request` body is exactly what a packet-mode HTTP POST carries — one
packet or a batch frame — and the `response` body exactly what its answer
carries. The request's peer headers are that POST's headers: session tokens,
declared metadata and trace context are read from them the same way. Any
instance answers any request; nothing binds a client to one. A request that
cannot carry its answer on a later frame — a `subscribe`, a `cancel`, an
`event` — is refused as it is on HTTP, with code `400`.

### Sessions {#broker-sessions}

```
client --hello {correlationId = session id, replyTo = client inbox}--> service address
client <--welcome {wrpc-inbox = instance inbox}------------------------ the instance that took it
client --packet | chunk {wrpc-seq, correlationId}--> instance inbox
client <--packet | chunk {wrpc-seq, correlationId}-- the instance
either --bye {correlationId}--> the other
```

- The `session id` is chosen by the client and names the session in every
  later message, in both directions (`correlationId`). The `hello`'s peer
  headers are the session's connection metadata, as a WebSocket upgrade's
  headers are.
- A `packet` body is one packet or a batch frame, UTF-8; a `chunk` body is a
  binary stream chunk (a `chunkEncode` frame, see the
  [wire format](./wire-format)). Everything a WebSocket carries — calls,
  events, subscriptions, cancellation, streams — travels this way.
- Frames are numbered from `1` in each direction. A receiver that sees a
  number other than the next one treats the session as **lost**: the carrier
  is at-most-once, and a missing frame is a missing packet nobody would ever
  answer. The server ends the session; the client closes and reconnects.
- A frame for a session the instance does not hold (it restarted, or the
  session idled out) is answered with a `bye` to its `replyTo`, and the client
  reconnects.
- The server ends a session nothing arrived on for its idle timeout. The
  client's app-level heartbeat (`ping`/`pong`) is what keeps a live session
  inside it.
- A server that refuses sessions answers `hello` with `bye`.

A client that reconnects says `hello` again and may be welcomed by another
instance; its subscriptions resume with `lastEventId` as on any reconnect.

## Compression

Every transport carries plain bytes unless the application turns compression
on; nothing negotiates it by default. Where it exists it is the carrier's own
mechanism, never a field of a wrpc packet:

| Transport | Mechanism | Negotiated by |
| --- | --- | --- |
| WebSocket | RFC 7692 `permessage-deflate` (`perMessageDeflate` on the engine) | the upgrade handshake |
| HTTP, packet mode and REST | `Content-Encoding: gzip` on the response (`http.compression`) | the request's `Accept-Encoding`; the response carries `Vary: Accept-Encoding` |
| Server-Sent Events | `Content-Encoding: gzip` on the stream — one gzip member, sync-flushed after every event (`sse.compression`) | the opening GET's `Accept-Encoding`, per response |
| WebTransport | per message, KIND 3/4 on the control stream (`compression` on both ends) | the `deflate` key of the capabilities message — on only when both ends named the same codec |
| WebSocket, client → server from Node | per message, framed binary (`0x00 03` / `0x00 04`) above the extension (`compression` on both ends) | `{ type: 'ping', enc }` from the client, answered by `{ type: 'pong', enc }` when the server has the same codec |
| WebRTC | per message, the DEFLATE bit of the data-channel header (`compression` on both peers) | the `caps.deflate` field of the description signal — on only when both peers named the same codec; a raw channel by the application's agreement |
| The broker binding | per frame, `wrpc-enc` on the frame (`compression` on both ends) | `hello`/`welcome` for a session; a stateless request names what it accepts and only the answer is compressed |
| The rooms backplane, the cluster channels | the whole envelope, base64 under a `wrpc-enc:<id>:` marker (`rooms.compression`, `cluster.compression`) | none — every instance must run it, in a two-step rollout |

An HTTP response is encoded only when its body is at or over the configured
threshold and nothing upstream already set a `Content-Encoding`; a `204` and
a `304` never are. A REST route's weak ETag is computed over the plain body,
so it is the same validator whichever encoding the peer asked for. On an
event stream the encoding is a property of one response: a re-attach
negotiates it again, and a replay goes out in whatever the new response
negotiated.

## Reconnect

The client reconnects on its own with truncated exponential backoff and full
jitter:

```
delay = random(0, min(maxDelay, minDelay * factor ** attempt))
```

Jittering the whole window rather than adding a small offset is what breaks
up the thundering herd — after a server restart, a thousand clients that
disconnected in the same millisecond would otherwise all come back in the
same millisecond.

On a successful reconnect the client reloads every unit it had loaded (the
new connection is a new server-side client, so its introspected method list
has to be rebuilt) and then emits `reconnect`. The `api` unit objects
themselves are reused, so event listeners registered on them survive the
outage.

Re-authentication is application-level and carries nothing new on the wire:
a client configured with an `authenticate` hook issues ordinary `call`
packets (its credential leg) after the socket opens and **before** it
re-sends its `subscribe` packets and re-introspects — packets on one
connection are ordered, so the server observes the credential first. A
server needs no support for this beyond answering the calls.
