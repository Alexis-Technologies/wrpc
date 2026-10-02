# Wire protocol

The wire protocol is deliberately small: **JSON packets** for everything
addressable, plus a **binary framing** for stream payloads. It rides on a
WebSocket, an HTTP request/response pair, or a `MessagePort` to a worker (a
Service Worker or a SharedWorker) — the packets are identical on all three,
which is what lets the same client code work behind a worker.

## Stability

**This page is the wire protocol at revision 2** — what `@alexify/wrpc@2.x`
speaks, next to revision 1, which it still speaks to a 1.0 peer
([Versioning](#versioning)). An independent implementation written against
it keeps working for the life of the major version. It has three tiers, and
the promise differs by tier:

- **The core** — the [packets](#packets), their fields and their meanings,
  the error codes, batch frames, binary chunks,
  [introspection](#introspection), [sessions](#sessions) and
  [reconnect](#reconnect) — **does not change** inside 2.x. A new optional
  field may be added; an existing one will not change shape or disappear.
  Unknown packet types are answered with a `callback` carrying code 500 and
  unknown fields are ignored, which is what makes an additive change safe
  for an older peer; error codes keep their meanings, and new codes may
  appear for new failure modes. Revision 2 changed one thing in the core
  1.0 shipped: a binary frame whose first byte is `0x00` is a
  [framed message](#binary-chunks), and a packet holding bytes travels as
  one. No JSON packet changed — and because a 1.0 peer reads no framed
  message, that one thing is what the revision marker negotiates.
- **The carrier conventions** — how a transport carries the core:
  [connection metadata](#connection-metadata) and its carriers, the `enc`
  negotiation on `ping`/`pong`, the [SSE](#server-sent-events) channel
  handshake, the [rooms](#rooms) envelope, the [WebRTC](#webrtc) framing and
  signaling messages, the [compression](#compression) rule. Additive within
  2.x under the same rule as the core. Most of them are new since 1.0 —
  every `##` section 1.0 did not have carries a <Badge type="info" text="since 2.0" />
  — and none of them asks anything of a 1.0 client or server: the table
  under [Changes since 1.0](#changes-since-1-0) says what happens when the
  versions meet. What is left to configure is between the instances of one
  fleet, which have no handshake to negotiate on.
- **The experimental sections** — [WebTransport](#webtransport), the
  [broker binding](#broker-binding), [session encryption](#session-encryption)
  with [sealed requests](#sealed-requests) and
  [broker sealing](#broker-sealing) — describe revision 1 of a carrier or a
  format that may change in a **minor**, with the change in the CHANGELOG;
  each says so at its top, and its subpath is marked `@experimental`
  ([stability](./stability#experimental-carve-outs)).

The **JavaScript API** on top of this is versioned by the package's own
semver and is a separate promise from the wire format. A change to a stable
tier of this page is a major version, with the reasoning in the
[CHANGELOG](https://github.com/Alexis-Technologies/wrpc/blob/main/CHANGELOG.md).

## Versioning

The wire carries a revision marker. **Revision 1** (`wrpc.v1`) is what 1.0
speaks. **Revision 2** (`wrpc.v2`) is revision 1 plus the
[framed messages](#binary-chunks): a packet whose byte values travel as
bytes. A 2.x peer speaks both, and the rule is one line — **a framed message
is sent only where the peer said it reads one**. Everywhere else a packet
holding bytes travels as the JSON 1.0 made of it (`{ "0": 137, … }` for a
typed array, `{ "type": "Buffer", "data": [ … ] }` for a Node `Buffer`), so
a 1.0 client and a 1.0 server keep working against 2.x with nothing to set.

On a WebSocket the revision is negotiated as a subprotocol:

```
Sec-WebSocket-Protocol: wrpc.v2, wrpc.v1   (client offer, newest first)
Sec-WebSocket-Protocol: wrpc.v2            (server selection)
```

A wrpc client **offers** the revisions it speaks; a wrpc server with no
app-configured `protocols`/`handleProtocols` **selects** the newest one
offered, wherever it sits in the list. Both sides therefore know, before the
first packet, which revision the connection speaks — the selected name is on
`connection.protocol` server-side, and the number on `client.revision` at
both ends.

The rules that keep this compatible in every direction:

- A **1.0 client** offers `wrpc.v1` alone, and gets it. A **1.0 server**
  picks `wrpc.v1` out of a 2.x client's offer. Either way the connection
  speaks revision 1.
- A peer that offers **nothing** gets no subprotocol and the connection
  speaks revision 1 — the pre-marker handshake stays valid forever.
- An end that sends and reads no framed messages — `attachments: false`, or
  an injected packet codec — has nothing of revision 2 to offer: it offers,
  or selects, `wrpc.v1` alone. Two 2.x ends whose options disagree therefore
  settle on revision 1 instead of one refusing the other's frames.
- A server whose app configures its own `protocols` list takes over
  negotiation entirely; the connection speaks revision 2 only when `wrpc.v2`
  is what was selected.
- A future `wrpc.v3` will be offered ALONGSIDE the older names, so an old
  server picks the one it knows and nothing breaks.
- The name `wrpc.` is reserved as a prefix: applications must not mint their
  own subprotocols under it. Besides revisions it holds the **carrier
  tokens** of [connection metadata](#connection-metadata) — offers that are
  read, never selected.

The other carriers have no subprotocol, and each says the revision where it
can:

| Carrier | How the revision is said |
| --- | --- |
| WebSocket | the subprotocol, above |
| HTTP | the response header `wrpc-version`, and the request's `Accept` |
| SSE | nothing to say — the transport is text-only and carries no framed message |
| Worker port | `v` on the port's first `ping` and its `pong` |
| WebRTC, WebTransport, the broker binding | carriers 1.0 never had: always revision 2 |

**HTTP.** Every response carries **`wrpc-version`**: the newest revision the
server speaks — `2`, or `1` for a server that reads no framed messages, which
is also what 1.0 answers — named in `Access-Control-Expose-Headers` so a page
on another origin can read it. A client sends a request whose body is a
framed message only after a response said `2`; until then, and against a 1.0
server always, its bytes travel as JSON. In the other direction a server
answers with a framed message only when the request's `Accept` names
`application/octet-stream`: a 1.0 client, `curl` and a browser's own `fetch`
name no such thing and read JSON. `Accept` rather than a request header of
wrpc's own because it is CORS-safelisted — a new request header needs a
preflight that a 1.0 server, or an application's own `cors.headers` list,
refuses. (A request MAY still send `wrpc-version`; it is accepted and
ignored.) Every request/response pair is self-contained, so a fleet behind
one address must agree: an instance that answers `2` promises that every
instance reads a frame.

**Worker port.** A `MessagePort` has no handshake, and the two ends are
deployed apart — a tab that loaded before a release, a Service Worker that
outlives its pages. The page's first packet is a `ping` naming the revision
it speaks, and a 2.x end answers with its own:

```json
{ "type": "ping", "v": 2 }
```

```json
{ "type": "pong", "v": 2 }
```

The port speaks the older of the two. A 1.0 end ignores the field and
answers a plain `pong` — revision 1 — and a 1.0 page sends no such ping, so
it is sent no framed message. The field means nothing on a transport that
settled the revision elsewhere.

## Changes since 1.0 {#changes-since-1-0}

What a 2.0 peer speaks that a 1.0 peer does not, section by section, and
what happens when the two meet. Between a client and a server nothing needs
setting: the revision is negotiated, and what is not negotiated is additive.
Two rows are **not additive**, both between the instances of one fleet —
where there is no handshake to negotiate on — and each says what to set
until every instance is upgraded; the
[CHANGELOG](https://github.com/Alexis-Technologies/wrpc/blob/main/CHANGELOG.md#migrating-from-10)
has the upgrade order.

| Since 2.0 | Where | Meeting a 1.0 peer | Until every peer is 2.0 |
| --- | --- | --- | --- |
| **Revision 2**: framed messages of kind 1 — **binary attachments**, a packet whose byte values travel as bytes, on WebSocket, a worker port and packet-mode HTTP | [Versioning](#versioning), [Binary chunks](#binary-chunks) | Negotiated: a connection with a 1.0 peer speaks revision 1 and carries no frame; its bytes travel as the `{ "0": … }` / `{ type: 'Buffer', data }` JSON of 1.0. | nothing |
| `wrpc-version: 2` on an HTTP response, and the `Accept` that asks for a framed answer | [Versioning](#versioning) | Additive: a 1.0 server answers `1` and is sent no frame; a 1.0 client sends no such `Accept` and is answered JSON. | nothing behind one address per version; a mixed fleet behind ONE address sets `attachments: false` on its 2.0 instances, so they answer `1` |
| `v` on a worker port's first `ping`/`pong` | [Versioning](#versioning) | Additive: a 1.0 end answers a plain `pong`, a 1.0 page sends no such ping — revision 1 either way. | nothing |
| **Framed messages** of kind 3 and 4 — a packet or a chunk compressed by a Node client — and the `enc` field of `ping`/`pong` that negotiates them | [Binary chunks](#binary-chunks), [Compression](#compression) | Negotiated: a 1.0 server ignores `enc` on the `ping` and answers a plain `pong`, so a 2.0 client never sends one; a 1.0 client never sends one. | nothing |
| The subprotocol **carrier tokens** `wrpc.h.` and `wrpc.m.` — a browser's declared headers and data on a WebSocket handshake — and a wider deny list of declared names (`wrpc.bearer.` is 1.0's own) | [Connection metadata](#connection-metadata) | A 1.0 server selects `wrpc.v1` and reads no token — so a 2.0 browser client that was answered `wrpc.v1` dials once more with the `wrpc_h`/`wrpc_meta` query 1.0 reads. A 1.0 client never sends one. | nothing — `carrier: 'query'` on the client saves the second handshake |
| The **SSE channel secret**, presented as `x-wrpc-channel: <id>.<secret>` | [Server-Sent Events](#server-sent-events) | Additive: the `ready` frame hands out the whole reference in the field 1.0 named `channel`, and a 1.0 client presents it back as it is. A 2.0 client against a 1.0 server presents the bare id that server gave it. | nothing |
| `epoch` and `seq` on the **rooms envelope** — loss detection between instances | [Rooms](#rooms) | Additive: a 1.0 instance ignores them, a 2.0 instance delivers a 1.0 instance's envelopes untracked. | nothing |
| The `wrpc-bin:` **rooms and cluster envelope** — an event (or a node-to-node question or answer) whose data holds bytes, as an attachments frame | [Rooms](#rooms), [Cluster channels](#cluster-channels) | **Not additive.** A backplane is a broadcast with no handshake: a 1.0 instance cannot parse the envelope and drops the event, without a log line. | `attachments: false` on the 2.0 instances keeps the envelope JSON, as in 1.0 |
| The `wrpc-enc:` and `wrpc-sealed:` **rooms and cluster envelopes** — compressed, encrypted | [Rooms](#rooms) | Opt-in on the publisher; a 1.0 instance drops what it cannot read. | turn `rooms.compression`, `cluster.compression` and the `encryption` options on only once every instance is 2.0 — the two-step rollout the section describes |
| `seq`, `ch` and `at` on a **signed cluster envelope** — the sender's counter, channel and clock, under `cluster.secret` | [Cluster channels](#cluster-channels) | **Not additive** one way. A 1.0 instance verifies a 2.0 envelope as before (the fields are inside the signed bytes) and applies it; a 2.0 instance refuses a 1.0 instance's envelope, which has no counter — the two halves of a mixed cluster stop seeing each other. Accepting one unasked would be a replay hole. Clusters without `secret` are unaffected. | `cluster: { replay: 'accept' }` on the 2.0 instances while a 1.0 instance is left |
| `headers` and `cache` on a procedure's `http` descriptor in **introspection** | [Introspection](#introspection) | Additive: unknown keys of a descriptor are ignored. | nothing |
| **WebRTC**: the data-channel framing, the signaling messages, the trust assertions | [WebRTC](#webrtc) | A new carrier: a 1.0 peer has no WebRTC transport and never meets it. | nothing |
| The **compression rule** — `enc` lists, a sender compresses with the first codec of its own list the peer announced | [Compression](#compression) | Governs only what two 2.0 ends negotiate. | nothing |
| **WebTransport**, the **broker binding**, **session encryption** with **sealed requests** and **broker sealing** — experimental | [WebTransport](#webtransport), [Broker binding](#broker-binding), [Session encryption](#session-encryption) | New carriers and formats, opt-in on both ends; a 1.0 peer never meets them. | nothing |

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

### Connection metadata {#connection-metadata}

A client may declare connection-phase metadata at connect time: a bag of
**declared headers** and a bag of **declared data**. This is a
transport-level convention, not a packet, and it has one carrier per thing a
transport can actually send:

| Carrier | Declared headers | Declared data | Who can send it |
| --- | --- | --- | --- |
| Real request headers | the headers themselves | `x-wrpc-meta` (percent-encoded JSON), or one `x-wrpc-meta-<key>: <value>` per key | HTTP, SSE; a WebSocket client outside a browser |
| Subprotocol offers | `wrpc.h.<base64url>` | `wrpc.m.<base64url>` | a browser WebSocket |
| Connect-URL query | `wrpc_h` (percent-encoded JSON) | `wrpc_meta` (percent-encoded JSON) | any WebSocket client that opts into it; WebTransport |

A browser's WebSocket constructor cannot set a request header and `fetch`
refuses to perform the upgrade by hand, so the one handshake header a page
controls is `Sec-WebSocket-Protocol`. A carrier token is the JSON object,
UTF-8, base64url **without padding** — a subprotocol name is an RFC 7230
token, so neither raw JSON nor `=` is admissible:

```
Sec-WebSocket-Protocol: wrpc.v2, wrpc.v1, wrpc.h.eyJ4LXRlbmFudCI6ImFjbWUifQ, wrpc.m.eyJ1c2VySWQiOjd9
```

Carrier tokens — `wrpc.h.`, `wrpc.m.` and the credential token
`wrpc.bearer.<token>` — are **data riding the offer, never a protocol to
select**. A server MUST NOT echo one: the response would reflect a credential,
and the client did not ask for it. Because a client fails a handshake whose
offers all went unanswered, a client MUST offer a selectable protocol (a
revision — `wrpc.v2`, `wrpc.v1`) next to a carrier token, and MUST NOT emit
carrier tokens when it offers nothing else. wrpc's negotiators remove the
tokens from the offer before an application's `protocols`/`handleProtocols`
sees it.

`wrpc.h.` and `wrpc.m.` are new since 1.0, which read the declared bags from
the connect-URL query only and ignores them (`wrpc.bearer.` is 1.0's own).
The revision is how a client finds out: a handshake that offered `wrpc.v2`,
carried a token and was answered `wrpc.v1` may have reached a 1.0 server, so
a wrpc client dials **once more** with the query carrier and keeps it for
that connection's later reconnects. A client told which carrier to use
(`carrier: 'protocol'` or `'query'`), or given its own `protocols`, is left
with that choice.

The `x-wrpc-meta-<key>` spelling carries string values only and the JSON
header wins a key collision. A conformant client MAY emit either; on a
carrier without headers the prefixed *mode* still travels as the JSON bag,
because the guarantee is about the bag the server observes, not the wire.

A peer that declares nothing is perfectly normal, and a server that ignores
the declarations is conformant. A server that consumes them MUST treat them
as untrusted labels, whichever carrier brought them:

- **Size-capped on the encoded input**, before any decoding. wrpc's default is
  2048 bytes: one budget for the two offers together (headers first), the
  query measured whole, the `x-wrpc-meta` header on its own. Keep the budget
  under the smallest request-header limit in the deployment — a WebSocket
  handshake is an HTTP request, and uWebSockets.js allows 4096 bytes for
  **all** of its headers where node allows 16 KB.
- **Refused, never fatal.** A malformed or oversize declaration leaves the
  connection unlabelled; it does not close it.
- **A carrier is chosen, never merged.** For each bag: the real header, else
  the offer, else the query. An offered token that is refused still silences
  the query, so garbage cannot downgrade a connection to the other carrier.
- **Never able to override an observed request header.** Declared names only
  add, and wrpc drops the names a hostile page could otherwise forge next to
  a victim's cookie: `cookie`, `host`, `origin`, `forwarded`, `via`,
  `x-real-ip`, `x-client-ip`, `true-client-ip`, `cf-connecting-ip`, and
  everything under `sec-`, `content-`, `proxy-`, `x-wrpc-` and
  `x-forwarded-`. Real headers are not filtered — a peer that can send them
  is not a page, and no deny list binds it.

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

Two optional fields ride the pair, both ignored by a peer that does not know
them: `enc`, the [compression](#compression) offer of a Node WebSocket
client, and `v`, the revision a [worker port](#versioning) names on its
first ping.

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
frame is a **framed message**, its second byte the kind. This is what
revision 2 adds to the core — revision 1 never sent an empty stream id
either, but did not reserve the byte, and a 1.0 peer reads such a frame as a
chunk of a stream it does not know:

```
0x00  kind  payload
      1     binary attachments: u32 headerLen (big-endian), JSON `[packet, [[path, byteLength], …]]`, then the buffers back to back
      3     a packet, compressed with the codec negotiated on ping/pong (below); the receiver inflates, then reads it as a text frame
      4     a chunk, compressed likewise; the receiver inflates, then reads it as a binary chunk
      other reserved — a 400 error packet
```

An **attachments frame** (kind 1) is a packet — or a batch array — whose
byte values travel as bytes: in the JSON every typed array, `ArrayBuffer`
or `DataView` is replaced by `null`, and the index names each one by its
path from the packet root (object keys and array indexes, the Jupyter
`buffer_paths` shape) with its length; the buffers follow in index order.
A receiver MUST refuse a frame whose paths name `__proto__`, `constructor`
or `prototype`, walk through a non-container, land on anything but `null`,
exceed 32 levels, or whose lengths do not add up to exactly the bytes that
follow — with a `400`, never a partial packet — and SHOULD copy the bytes
out of the frame rather than hold views into it. The same frame is the
body of a packet-mode HTTP request or response — and of a conventional
REST answer — under `Content-Type: application/octet-stream`. It is sent for
a packet that holds bytes **only where revision 2 was negotiated**
([Versioning](#versioning)): a WebSocket that selected `wrpc.v2`, an HTTP
exchange whose other side said it reads one, a worker port whose two ends
both named revision 2. An end that opted out (`attachments: false`) or
speaks a wire codec negotiates revision 1 in the first place; one that is
sent a frame anyway answers it as a malformed packet. SSE, being text-only,
refuses one on a channel POST with `415` and answers a call whose result
holds bytes with `501`. Kind 2 is reserved.

The negotiation is a `ping` whose `enc` is the client's codec ids in its
order of preference (a list; one id is a list of one), answered by a `pong`
whose `enc` is ONE id — the first of the client's list the server holds —
or by a plain `pong` when it holds none. Only the client compresses on this
wire, so that id is the codec of every kind 3 or 4 frame that follows. A
peer MAY send one only after such a `pong`; before that, or with a payload that does not inflate under the receiver's
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
  A receiver compares epochs for equality only: a publisher whose counter
  table rotated restarts an evicted channel's count under `<epoch>.<n>`,
  and the receiver's cursor resets exactly as on a restart.
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
nothing to negotiate against on a fan-out, so the marker names the codec:
an instance encodes with the FIRST codec of its list and reads any codec on
it, and a receiver that holds no such codec drops the envelope (and logs).
A deployment turns compression on only once every instance can read it,
and changes codec the same way — every instance lists both, then the order
is swapped. The cluster channels below do the same under
`cluster: { compression }`, applied after the HMAC signature.

An event whose data holds bytes cannot be a JSON envelope. It is published
as `wrpc-bin:<base64 of the envelope as a binary attachments frame>` — the
[frame](#binary-chunks) a socket carries such a packet in, over the envelope
object instead — and delivered on the other instances as bytes.

With `rooms: { encryption }` on, every envelope is published sealed:

```
wrpc-sealed:<kid>:<base64( header ‖ AEAD( frame ) )>
header = u8 version (1) ‖ u8 suite ‖ salt (16) ‖ u64 counter
frame  = u8 flags ‖ [ u8 idLength ‖ codec id ] ‖ body        flags bit 0: compressed, bit 1: body is a binary envelope
```

`kid` names the key of the shared keyring (1–32 of `A-Z a-z 0-9 . _ -`) and
travels in the clear; `suite` is `1` AES-256-GCM, `2` ChaCha20-Poly1305,
`255` a cipher both ends were handed. A sender draws `salt` once per
process, derives its own key — `HKDF-SHA256(key[kid], salt, "wrpc rooms v1"
‖ 0 ‖ kid ‖ 0 ‖ cipher id)` — and counts under it: the nonce is the counter,
64 bits big-endian at the end of the nonce, so no two messages share a
(key, nonce) pair, and the salt is drawn again after 2^32 messages. The
additional data is `"wrpc-sealed v1" ‖ 0 ‖ layer ‖ 0 ‖ kid ‖ 0 ‖ channel`,
so an envelope moved to another channel, another layer or another key id
does not open. Compression happens INSIDE the frame (compress, then seal)
and names its codec there; a `wrpc-enc:` marker never appears inside a
sealed envelope. A receiver keeps a sliding window of each sender's
counters and drops a repeat — a sender being a (kid, suite, salt), the
window in memory only: empty when the receiver starts, and dropped with
the oldest sender once more than `maxSenders` (1024) are remembered. The
frame carries no time. An envelope that does not open is dropped and
logged, never answered. The cluster channels do the same under `cluster: {
encryption }` with the label `"wrpc cluster v1"`, after signing and
compression. What stays visible to the backplane: the channel name (and so
the room name), the kid, the sender's salt, message sizes and timing.

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

Under `cluster: { secret }` an envelope ends with `sig`, the hex
HMAC-SHA256 of the envelope's JSON text without it, and carries three
fields inside what is signed: `seq`, the publisher's counter (one per
process, across both channels, so a receiver sees gaps), `ch`, the channel
it was published on, and `at`, the publisher's clock in milliseconds. A
receiver verifies the signature, then refuses an envelope whose `ch` is not
the channel it arrived on, whose `at` is further than its `maxSkew` (30 s)
from its own clock, whose `seq` it already accepted from that `from` and
`epoch` (a sliding window of 1024), or whose `epoch` is a life of `from`
older than the one it follows. An envelope without `seq` is refused unless
the receiver runs `replay: 'accept'`.

An envelope whose application payload holds bytes — the `data` of an
addressed event (`sendTo`) or a node event, of a question, or an answer's
`payload` — is published as the binary envelope of [Across
instances](#across-instances): `wrpc-bin:<base64 of the envelope as an
attachments frame>`, or the same frame inside the sealed one. Its `sig` is
the HMAC of the frame's bytes with `sig` absent — the receiver deletes
`sig` from the decoded envelope and encodes it again — so a signature made
over one form is not valid for the other.

## Server-Sent Events

SSE is one-way, so a channel is two halves that find each other by id:

```
GET  {basePath}/events                                 opens a NEW channel
GET  {basePath}/events  x-wrpc-channel: <id>.<secret>  re-attaches to an existing one
POST {basePath}         x-wrpc-channel: <id>.<secret>  client -> server
```

**The channel id is minted by the server** — by the application's
`generateId`, so its format (a uuid, a cuid, a counter) is the application's
— and the **channel secret drawn by the server**: 18 random bytes as
base64url. Both are handed out, joined, in the `ready` frame that opens
every stream:

```
event: ready
data: {"channel":"b1f0….Kx9…"}
```

ONE string, the **channel reference** — the secret after the id — which a
client treats as opaque and presents back whole in one header; the server
splits it at the LAST dot (the secret holds none; the id may). It rides the
field 1.0 named `channel` on purpose: a 1.0 client presents whatever that
field held, so it presents the secret without knowing there is one — and a
1.0 server's `channel` is the bare id, which a 2.x client presents just the
same. **The secret is the credential.** A client cannot propose its own id, and a GET or POST naming
an id without the channel's secret answers `409` — the same answer as for an
id the server does not hold, so a guessed id learns nothing; `409` is the
client's signal to drop its channel state and start a fresh one.
(`?channel=<id>.<secret>` in the query string is accepted for re-attach as
well, but the header is preferred — URLs end up in proxy logs.) A value
without a dot presents no secret.

On top, the channel is **bound to the identity that created it**: the
session token in the opening GET's cookie (or "anonymous" when it carries
none). Every re-attach and every POST must present the same cookie identity
— a request that presents the secret without it is refused with `403`.
Channel creation is also capped (`maxChannels`, `maxChannelsPerAddress`):
past the caps a new GET answers `503` or `429`.

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

## WebRTC <Badge type="info" text="since 2.0" /> {#webrtc}

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

What signaling carries is nonetheless fixed, so that peers behind different
signalers agree: `{ type: 'description', description, caps? }`,
`{ type: 'candidate', candidate }`, `{ type: 'connect' }` (a knock — the
non-initiator asking to be dialled) and `{ type: 'close', reason? }`, the
goodbye. Its `reason` is one of `goodbye` (the application ended the
link), `refused` (the sender would not have the peer: a failed
[trust assertion](#webrtc-assertions) or its `accept()` hook) and
`gave-up` (its redial budget ran out). A receiver MUST end the link on any
`close`, MUST read one without a `reason` as `goodbye`, and MUST report
one it does not know as `unknown` rather than refuse it — the set may grow.

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
bit 2   COMPRESSED 1 = the message is compressed with the sender's negotiated codec — only once negotiated
bit 3–7        reserved, MUST be 0
```

- Fragments of one message are sent back to back on one ordered, reliable
  channel, so there is no message id and no sequence number: a receiver
  concatenates fragments until FIN.
- A fragment that is not the last MUST carry at least one payload byte, and
  a receiver bounds how many fragments one message may arrive in (16 384
  by default — its reassembly cap in 1 KiB pieces, never fewer than 1024):
  the byte cap alone would let a peer cost the receiver a buffered view per
  byte, or per empty fragment. An empty continuation, or a fragment past
  the count, is a protocol error like the ones below.
- The KIND and COMPRESSED bits of a continuation MUST equal those of the
  message it continues; a set reserved bit, a COMPRESSED bit before the two
  peers' lists shared a codec, a mismatched continuation, a text message that is
  not valid UTF-8, a reassembly past the receiver's cap (16 MiB by default)
  or a compressed message that does not inflate under that cap is a
  protocol error, and the receiver closes the channel — the data-channel
  analogue of a WebSocket `1002`.
- **Compression** is negotiated through signaling, since the channels have
  no handshake: a `description` signal MAY carry `caps`, a JSON object whose
  known key is `enc` — the peer's codec ids in its order of preference
  ([the rule](#compression)). A peer MAY set the COMPRESSED bit only once
  the other peer's last description announced a codec it holds; the payload
  is then the output of the SENDER's chosen codec over the bytes the message
  would otherwise carry, compressed before fragmentation and inflated after
  reassembly. Over a channel the application negotiated itself there is
  nothing to announce through: both ends use the first codec of their list,
  and whether the bit is in use at all is the application's agreement.
- Fragment size is the negotiated `sctp.maxMessageSize` capped at 256 KiB,
  and 16 KiB when nothing is reported. A peer MAY send smaller fragments.
  A peer whose `a=max-message-size` is under 1 KiB is refused: the link
  fails rather than fragment every packet into a thousand pieces.

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
  the fingerprint the description's SDP declares — **every**
  `a=fingerprint:` line of it, session- or media-level, of any algorithm,
  MUST equal `fp` (algorithm, a space, colon-separated hex — compared after
  normalization: algorithm in lower case, hex in upper case). DTLS binds to
  the media-level line (RFC 8122 §5), so a description naming a second
  certificate anywhere is one the relay may have edited, and a verifier
  MUST refuse it. The DTLS handshake then proves the sender holds that
  certificate. A verifier MUST apply the description only after the
  assertion verified.
- `exp` MUST be in the future by the issuer's clock (a verifier SHOULD
  allow a small skew and MAY learn the issuer's clock from tokens issued to
  itself). `iss`, when the deployment sets one, MUST match.
- A token is at most 4 KiB. Anything else is a refusal; the reason is the
  verifier's business, not the wire's — the link is simply closed.

## WebTransport <Badge type="info" text="since 2.0" /> {#webtransport}

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
                   3 = a packet, compressed with the sender's negotiated codec — only once `enc` was negotiated
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
(`{"streams":true}`) and `enc` (the end's codec ids in its order of
preference, `["deflate-raw"]` for the platform default). Unknown keys are
ignored; an end that sends none, or `{}`,
has announced nothing, and its peer treats it as a revision-1 peer.

Once the two `enc` lists share a codec, either end MAY send a packet as
KIND 3 or a chunk as KIND 4: the payload is the output of the SENDER's codec
([the rule](#compression) — no frame names it) over the
bytes KIND 0 or 1 would have carried, and the receiver inflates it before
reading — a KIND 3 payload is UTF-8 JSON only after inflation. The choice is
per message and the sender's (a message under its threshold, or one the
codec did not shrink, goes as KIND 0 or 1 at any time). A compressed kind
before the lists shared a codec, a payload that does not inflate, or one
that inflates past the receiver's cap is a protocol error (1002). Datagrams
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
- A sender SHOULD drop, not queue, a datagram its session is not ready
  for: a datagram that waits is a stale one. A dropped datagram is
  reported to the application as sent — it is not retried on the control
  stream, where it would wait behind the same congestion.

## Broker binding <Badge type="info" text="since 2.0" /> {#broker-binding}

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
| `wrpc-enc` | on a `request`, `hello` or `welcome`: the sender's codec ids, joined by commas, in its order of preference (`zstd,deflate-raw`); on a `response`, `packet` or `chunk`: the ONE codec its body is the output of — the sender's first that the other end listed. A `welcome` carries it only when the lists share a codec. A marked frame a receiver cannot inflate ends the session. |

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

### Sealed frames and events {#broker-sealing}

A broker keeps what it carries — for a topic's whole retention, for whoever
can read it or its backups. With `encryption: { keys }` on the binding (the
service and its clients share the keyring, as instances share
`rooms.encryption`) every message is sealed with the
[backplane envelope's frame](#across-instances):

```
headers   wrpc-sealed: <kid>     + wrpc-kind, wrpc-seq on an RPC frame
body      sealed( u32 headerLength ‖ JSON(headers) ‖ body )
```

The HEADERS move inside with the body — a client's `authorization` and
`x-wrpc-meta`, the trace context, `wrpc-enc`, `wrpc-inbox`, `wrpc-reason` —
so a bearer token no longer rests in the broker. An RPC frame is bound to
`address ‖ 0 ‖ kind ‖ 0 ‖ correlation id ‖ 0 ‖ seq` (label
`"wrpc broker-rpc v1"`): the readable `wrpc-kind` and `wrpc-seq` cannot be
rewritten, a frame does not open in another conversation, and a per-sender
window drops a replay. A published event is bound to its **topic** (label
`"wrpc broker-log v1"`), carried as base64 text — a log is only promised to
keep a string — and has no replay window: a log is read again and a delivery
redelivered, by design. The partition `key` stays readable; the broker
routes by it. A message that does not open is dropped (RPC), skipped (a
feed) or dead-lettered with `400` (a consumer), logged, and never answered.
A session opened by a sealed `hello` — welcomed by a sealed `welcome` — takes
sealed frames only from then on, whatever `acceptPlaintext` says: a
plaintext `packet`, `chunk` or `bye` naming it is a downgrade, dropped on
either end without consuming a sequence number.

A sealed `request` and a sealed `hello` — the frames that reach the
competing group — carry `wrpc-t`, the sender's clock in milliseconds since
the epoch, **inside** the seal. A receiver MUST refuse one whose clock is
further than its skew allowance (five minutes by default) from its own: the
envelope's per-sender replay window lives in one process, and a captured
frame replayed to another instance would otherwise be new to it. A receiver
MAY consult a memory shared by the instances (keyed by the envelope's own
header — key id, sender salt, counter) to refuse the replay within that
allowance too.

## Compression <Badge type="info" text="since 2.0" /> {#compression}

Every transport carries plain bytes unless the application turns compression
on; nothing negotiates it by default. Where it exists it is the carrier's own
mechanism, never a field of a wrpc packet:

| Transport | Mechanism | Negotiated by |
| --- | --- | --- |
| WebSocket | RFC 7692 `permessage-deflate` (`perMessageDeflate` on the engine) | the upgrade handshake |
| HTTP, packet mode and REST | `Content-Encoding` on the response — `gzip` by default, `br`, `zstd` or an application's coding by `http.compression.encodings` | the request's `Accept-Encoding` against the server's list, in the server's order; the response carries `Vary: Accept-Encoding` |
| Server-Sent Events | `Content-Encoding` on the stream — one encoder for the response (for gzip, one member), flushed after every event (`sse.compression`) | the opening GET's `Accept-Encoding`, per response |
| WebTransport | per message, KIND 3/4 on the control stream (`compression` on both ends) | the `enc` list of the capabilities message |
| WebSocket, client → server from Node | per message, framed binary (`0x00 03` / `0x00 04`) above the extension (`compression` on both ends) | `{ type: 'ping', enc: [ids] }` from the client, answered by `{ type: 'pong', enc: id }` — the first of them the server holds |
| WebRTC | per message, the COMPRESSED bit of the data-channel header (`compression` on both peers) | the `caps.enc` list of the description signal; a raw channel by the application's agreement |
| The broker binding | per frame, `wrpc-enc` on the frame (`compression` on both ends) | the `wrpc-enc` lists of `hello`/`welcome` for a session; a stateless request lists what it accepts and only the answer is compressed |
| The rooms backplane, the cluster channels | the whole envelope, base64 under a `wrpc-enc:<id>:` marker (`rooms.compression`, `cluster.compression`) | none — the marker names the codec; every instance must hold it, in a two-step rollout |

**Choosing the codec.** A codec is named by an id — `deflate-raw` (raw
DEFLATE, RFC 1951, no zlib or gzip wrapper), `brotli` (RFC 7932), `zstd`
(RFC 8878), `deflate-raw+dict:<hash>` for a preset dictionary, or an
application's own; an id holds no comma and no whitespace. Wherever two ends
negotiate, each announces the ids it can **decode**, in its order of
preference, and a sender compresses with **the first codec of its own list
the other end announced**. The receiver holds both lists, so it knows which
codec that is: no frame names it (the broker binding's frames do, its
header being there anyway), the two directions choose independently — a
server may answer in `zstd` a client that sends `deflate-raw` — and there is
no tie to break. Lists that share nothing, or an end that announced
nothing, leave the wire plain. A receiver looks at no more than the first 16
ids of a peer's list and ignores entries that are not strings.

An HTTP response is encoded only when its body is at or over the configured
threshold and nothing upstream already set a `Content-Encoding`; a `204` and
a `304` never are. A REST route's weak ETag is computed over the plain body,
so it is the same validator whichever encoding the peer asked for. On an
event stream the encoding is a property of one response: a re-attach
negotiates it again, and a replay goes out in whatever the new response
negotiated.

## Session encryption <Badge type="info" text="since 2.0" /> {#session-encryption}

**This section is experimental** — this one, [Sealed requests](#sealed-requests)
under it and [Broker sealing](#broker-sealing): they describe revision 1 of
each format, may change in a minor release, and sit outside this page's
interoperability promise until they stabilize, the way an injected wire codec
does. The subpath is marked `@experimental` whole
([Stability](./stability#experimental-carve-outs)).

Opt-in (`@alexify/wrpc/encryption`), on the persistent transports, and never
in place of TLS. A client announces it with `wrpc_e=1` in the connect URL —
the server may be the first to send, so it has to know the mode before any
frame. The flag is not a secret: a client configured to encrypt never
accepts plaintext, so stripping it buys a refusal.

Every frame of such a connection is a binary
[framed message](#binary-chunks):

```
handshake, first   00 05 ‖ u8 version (1) ‖ u8 nameLength ‖ protocol name
                         ‖ u8 kidLength ‖ kid ‖ noise message
handshake, later   00 05 ‖ noise message
sealed             00 06 ‖ AEAD( u8 inner ‖ payload )        inner: 0 text, 1 bytes
```

The handshake is the [Noise Protocol Framework](https://noiseprotocol.org/noise.html)
(revision 34) under its canonical names —
`Noise_<pattern>_25519_<AESGCM|ChaChaPoly>_SHA256`, patterns `NN`, `NK`
(the default: the client pinned the server's static key), `XX` (mutual) and
`NNpsk0`. The first message **names** the protocol and the key id of the
server static it was written for; the server holds it or closes the
connection. Nothing is negotiated back — a reply of "try this instead" would
be a downgrade channel. The prologue is
`"wrpc.v1" ‖ 0 ‖ transport kind ‖ 0 ‖ hello header`, so a header rewritten in
flight, or a handshake replayed onto another kind of connection, fails the
handshake. (That `wrpc.v1` is the label of this framing, fixed with its own
version byte — not the [revision](#versioning) the connection negotiated.)

After it, each direction is a Noise CipherState: the nonce is the message
counter (four zero bytes, then 64 bits — big-endian for AESGCM, little-endian
for ChaChaPoly), the additional data is empty, and both ends rekey
(`REKEY`, Noise §11.3) every 2^20 messages. The interval is a
per-deployment constant (`rekeyAfter`, 2^20 by default), not negotiated —
no handshake message carries it — and two ends that disagree fail at the
first rekey, as a frame that does not open. Text packets, stream chunks and
the other framed kinds all travel as the payload of a sealed frame, so
compression happens inside it. A frame that does not open — altered,
replayed, out of order — closes the connection: `1002` for anything that did
not verify or parse, `1008` for a refusal of policy (an unlisted protocol, an
unknown kid, `authorize`, the handshake timeout, plaintext under `required`).
The close reason is always `encryption`; which check it was is in the
server's log only.

A server's static keys are derived from one configured secret per kid —
`X25519(HKDF-SHA256(secret, "wrpc noise static v1"))`, and a second pair
under `"wrpc hpke static v1"` for the per-request binding — and published as
the bundle `<kid>:<noise key>:<hpke key>` (base64url), which is what a
client pins.

On **WebTransport** the same frames ride the control stream as `KIND_BINARY`
messages, after the capabilities exchange, with `wt` as the transport kind
in the prologue. A sealed session announces no `streams` capability and
uses no datagrams — binary streams fall back to chunks on the control stream
and an `unreliable` event is sent reliably — because a stream or a datagram
of its own would be a way around the channel; the carrier's own per-message
compression is left off as well, since what it would compress is ciphertext.

### Sealed requests {#sealed-requests}

A request is not a connection, so the **HTTP** transport seals each one on
its own with [HPKE](https://www.rfc-editor.org/rfc/rfc9180) (base mode,
`DHKEM(X25519, HKDF-SHA256)`, `HKDF-SHA256`, and `AES-256-GCM` or
`ChaCha20Poly1305`) to the server's `hpke` static key — the construction of
Oblivious HTTP ([RFC 9458](https://www.rfc-editor.org/rfc/rfc9458) §4),
without its relay:

```
POST <the transport's endpoint>        Content-Type: application/wrpc-sealed
request    u8 version (1) ‖ u8 aead id ‖ u8 kidLength ‖ kid ‖ enc (32) ‖ ct
plaintext  u32 headerLength ‖ JSON { m, u, h, t } ‖ body
response   nonce (32) ‖ AEAD( u32 headerLength ‖ JSON { s, h } ‖ body )
```

The real request is inside — method `m`, path and query `u`, headers `h`, the
sender's clock `t` in ms — so an observer sees one endpoint being POSTed to,
and a `GET` REST route travels as that POST too. The inner headers are laid
over the outer request's, with three exceptions a server MUST keep: what
the connection or a proxy in front of it says about the sender (`host`,
`origin`, `forwarded`, `via`, `x-forwarded-*`, `x-real-ip`, the CDNs'
client-ip spellings, `sec-*`) is never taken from inside — its absence is a
fact too, so an inner one is dropped whether or not the outer request
carried it; the framing names (`content-length`, `transfer-encoding`,
`connection`) are the outer request's; and an outer `cookie` wins over an
inner one. `headerLength` above 16 KiB is refused. HPKE `info` is
`"wrpc http v1" ‖ 0 ‖` the request prefix (version, AEAD id, kid). The answer
is sealed under a key both ends export from the same context:
`secret = Export("wrpc http response", Nk)`,
`prk = Extract(enc ‖ nonce, secret)`, then `Expand(prk, "key")` and
`Expand(prk, "nonce")`. The outer status is always `200`; the real status `s`
and headers `h` are inside, except `Set-Cookie`, which stays on the outer
response because script cannot set an `HttpOnly` cookie.

HPKE has no replay protection, and the party this exists for is exactly the
one who could replay: a request whose `t` is further than `maxSkew` (5 min)
from the server's clock, or whose `enc` was already accepted, is refused
`409`. Anything that does not parse or open is refused `400`. A refusal is
never sealed and never detailed — and a client that encrypts treats ANY
response that is not `application/wrpc-sealed` as an error, whatever its
status.

**Server-Sent Events** ride the same binding: the stream request and every
channel POST are sealed requests — so the channel id and `Last-Event-ID`
travel inside — and the stream comes back as

```
Content-Type: text/event-stream; wrpc-sealed=1; n=<base64url(nonce32)>
data: <base64( AEAD( one chunk of the real stream ) )>
```

one opaque `data:` event per chunk of the real stream, control frames
(`ready`, `gap`, the heartbeat comment) included. The key is
`Expand(Extract(enc ‖ n, Export("wrpc sse stream", Nk)), "key", Nk)`: the
secret only the two ends of the request that opened THIS stream can
export, salted with the request's `enc` and 32 bytes `n` the SERVER draws
per stream and carries in the `Content-Type` parameter (the one response
header a cross-origin page can always read; a stream without a 32-byte
`n` MUST be refused). The frame nonce is the counter from zero. So a
re-attached stream has a new key and replays under it, and so does the
same open request replayed onto another instance, whose replay memory is
its own — without `n` the two streams would count from zero under one key.
A frame that is dropped,
reordered or altered errors the stream, which the client sees as a broken
connection and re-attaches from. The type stays `text/event-stream` so that
intermediaries treat it as one; an HTTP content coding is never applied to a
sealed stream or a sealed answer.

`GET <basePath>/encryption-key` answers `{ "key": "<bundle>" }` — the one
plaintext answer under `required` (`discovery: false` removes it). It is
trust on first use.

What stays outside the sealed channel is everything the upgrade carried:
the URL, real headers, the `wrpc.bearer.` / `wrpc.h.` / `wrpc.m.` subprotocol
tokens, cookies. Under session encryption a credential belongs in
`authenticate` or a per-call `meta`, not in the handshake request.

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
