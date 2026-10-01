# Cluster

[Rooms](./rooms) reach the clients one process holds; a [backplane](./scaling)
makes a room span every process. The **cluster layer** is what sits on top of
that same backplane and answers the questions a multi-instance deployment
actually asks: *how many are connected — everywhere?* *which node holds this
client?* *ask every node something and collect the answers.*

```js
server.cluster.presence('chat');                         // { total, instances: { … } }
const clients = await server.cluster.fetchClients({ room: 'chat' });
server.cluster.disconnect({ room: 'banned' });
```

It is always present. Without a backplane every operation degrades to its
local half — `presence()` reports this instance, `ask()` asks nobody — so
application code never branches on the deployment shape.

::: info Built on the contract, not into it
`Cluster` uses only `publish`/`subscribe`/`close`, so every backplane —
`MemoryBackplane`, Redis, your own — gets presence, requests and commands
without implementing correlation, timeouts or aggregation itself.
:::

## Two channels

```mermaid
sequenceDiagram
  autonumber
  participant A as instance A
  participant CH as channel "cluster"
  participant B as instance B
  participant IB as channel "inst:A"
  A->>CH: hello { rooms, clients }
  CH->>B: hello
  B->>IB: state snapshot
  Note over A: B is now in A's presence map
  loop every presenceInterval
    A->>CH: snapshot, corrects dropped deltas
  end
  A->>CH: q { requestId, op: "fetch" }
  CH->>B: q
  B->>IB: a { requestId, payload }
  Note over A: resolves when the last live node answers
```

Both are held for the life of the process: `cluster`, which every instance
subscribes to (presence, wide requests, commands), and `inst:<instanceId>`,
one instance's own inbox for answers and addressed commands. The envelopes
are an implementation detail of wrpc's cluster layer, not part of the frozen
[wire protocol](../reference/protocol#cluster-channels) — they never reach a
client connection.

## Presence: replicated, read locally

```js
server.cluster.count('chat');     // cluster-wide membership — no network
server.cluster.presence('chat');  // { total, instances: { 'node-1': 2, … } }
server.cluster.instances();       // live instance ids, this one first
```

`count()` and `presence()` are **local sums**. Every join and leave publishes a
±1 delta, and a periodic snapshot corrects whatever the at-most-once broker
dropped, so a lost delta heals within one interval rather than skewing forever.
`rooms.count(room)` and `rooms.members(room)` stay the LOCAL numbers — a
zero-cost read for code that wants exactly this instance.

Liveness counts **every** message a node sends; the snapshot only fills
silence. A node quiet for `presenceTimeout` is evicted, a graceful `close()`
says goodbye and is evicted immediately, and a restart arrives with a fresh
**epoch** — which is how a receiver tells "restarted, replace its counters"
from "same process, merge them".

| Option (`cluster.…`) | Default | What it controls |
| --- | --- | --- |
| `presenceInterval` | `5000` ms | How often this node publishes its presence **digest** (a hash, not the room table — see below). Raising it also delays eviction: `presenceTimeout` defaults to 3× it. |
| `presenceTimeout` | `3 ×` interval | Silence after which a node is evicted. |
| `requestTimeout` | `2000` ms | Backstop for `fetchClients`/`ask`; resolves `incomplete`, never rejects. |
| `rooms` | all | Which rooms replicate: an array, predicate or RegExp. See the cardinality note below. |
| `maxFetch` | `1000` | Per-node ceiling on one `fetchClients` reply; a node over it answers its first `maxFetch` descriptors and the result carries `truncated: true` (never silent). `0` disables. |
| `secret` | — | Opt-in HMAC-SHA256 envelope authentication, with replay protection — see [Trusting the backplane](#trusting-the-backplane). |
| `replay` | `'strict'` | Under `secret`: what to do with a signed envelope that carries no counter, which is what a 1.x node sends. `'strict'` refuses it; `'accept'` lets it through during a rolling upgrade from 1.x. |
| `maxSkew` | `30000` ms | Under `secret`: how far an envelope's clock may sit from this node's, both ways. |
| `compression` | off | Deflate the cluster envelopes this node publishes, after signing — the same marker and two-step rollout rule as [`rooms.compression`](./scaling#compression); an unreadable envelope logs `cluster.encoded`. |

`cluster: false` opts out of the cluster layer entirely: presence, commands
and asks degrade to their local halves while the [rooms
backplane](./scaling) keeps working. `server.cluster` still exists, so
application code never branches on the deployment.

::: warning Presence cost scales with ROOM cardinality
Presence replicates a `room → count` table per node. With topic rooms
(`'lobby'`, `'ticker:AAPL'`) that is small; with the per-user
`user:<id>` pattern it is one entry per connection, and every node holds
every other node's table — O(nodes × rooms) heap. The periodic corrective
message is a **digest** (a 32-bit hash), so steady-state backplane traffic
stays O(nodes²) *bytes*, and the full table travels only to a node whose
view actually drifted (it asks with an addressed `sync`). If you never call
`presence()`/`count()` on a family of rooms, exclude it with
`cluster: { rooms }` — deltas and digests then skip it entirely.
:::

### Trusting the backplane

The backplane is a **trust peer** of every node: anything that can publish
on the `cluster` channel can disconnect every client or join anyone to any
room on every instance at once. Isolate the broker on its own network and
ACL it. Where that is not enough, set the same `cluster: { secret }` on
every node: envelopes are HMAC-SHA256-signed and an unsigned or mis-signed
message is dropped and logged (`cluster.unsigned` / `cluster.badsig`).
Room *events* travel on separate channels and are not signed — the secret
guards the command surface, the broker ACL guards the rest.

A signature alone says a command **was written by a node holding the
secret** — not when, and not where. So under `secret` every envelope also
carries, inside what is signed, the sender's **counter**, the **channel**
it was published on and the sender's **clock**, and a receiver refuses
(`cluster.replay`, with a `reason`):

| `reason` | What arrived |
| --- | --- |
| `seq` | A counter this node already accepted from that sender, or one older than its window of 1024 — a copy of an earlier envelope |
| `channel` | An envelope on a channel other than the one it was signed for — an addressed command moved to another instance's inbox, or onto `cluster` |
| `stale` | A clock further than `maxSkew` from this node's, or an envelope of a **previous life** of a sender whose restarted process this node already follows |
| `unsequenced` | A signed envelope with no counter at all — a 1.x node's |

So a party that can read the backplane and write to it (a compromised
broker, a client with a wider ACL than intended) cannot run a `disconnect`
again, move a `join` to another node, or bring a dead process' presence
back. Two things follow for operations:

- **Clocks.** The nodes of a cluster keep their clocks within `maxSkew`
  (30 s by default) — a node outside it is refused by the others as `stale`
  and never joins. The window is also the one bound on what a node that
  was *not listening* can be fed: a counter window only remembers what
  this process heard, so a freshly booted node accepts an envelope up to
  `maxSkew` old that it has not seen. Lower it where the clocks allow.
- **Upgrading from 1.x.** A 1.x node signs but does not count, so a 2.0
  node refuses it and the two halves of a mixed cluster do not see each
  other. Deploy 2.0 with `cluster: { secret, replay: 'accept' }` while any
  1.x node is left — that waives the counter of a node that has none, and
  nothing else — then drop the option. (A 1.x node reads a 2.0 envelope as
  it always did: the new fields are inside the bytes it verifies.)

The refusal is logged once per sender and reason each `presenceTimeout`
(`debug` in between — a copy can be published in a loop) and always
counted, as `wrpc.cluster.verifications` with outcome `replay`.

What `secret` does not do is hide anything: the envelope is still
readable JSON on the broker. Where the backplane itself is in the threat
model, add `cluster: { encryption }` — the sealed frame has its own
counter window and is bound to its channel as well — see
[Encryption](./encryption#backplane).

### Health

`cluster.healthy` (and the aggregate `server.rpc.healthy`) is `false` while
a backplane channel subscribe is failing and being retried with capped
backoff — the node can publish but cannot hear. `'degraded'` and
`'recovered'` fire on the transitions; wire them to a readiness probe so a
half-connected node is drained instead of serving with silent gaps.

```js
const server = new Server({ router, backplane, instanceId: 'node-1', cluster: { presenceInterval: 2000 } });
```

`instanceId` may be **stable** across restarts (`'node-1'` from your
orchestrator); the epoch never is, and that is the pair that makes restart
detection work.

## Finding clients

```js
const clients = await server.cluster.fetchClients({ room: 'chat' });
// [{ id, instance, rooms, data, transport, session }, …]

if (clients.incomplete) log.warn('a node did not answer in time');
```

A descriptor is deliberately small and serializable:

| Field | What it is |
| --- | --- |
| `id` | The client id, instance-prefixed (`<instanceId>.<generateId()>`). |
| `instance` | Which node holds the connection. |
| `rooms` | Its room memberships on that node. |
| `data` | `client.data` — the application's own bag; wrpc never reads it. |
| `transport` | `'ws'`, `'http'`, `'sse'` or `'event'`. |
| `session` | Whether a [session](./sessions) is attached — never the session itself. |

Only **persistent** clients are enumerated: a per-request HTTP client is not a
peer anyone means to list, join or disconnect.

`fetchClients` knows its respondent set from presence and resolves the moment
every live node has answered. When one never does, the partial array carries a
non-enumerable `incomplete: true` and the miss is logged — the timeout is a
backstop, never a silent truncation.

On this instance, the same lookup is synchronous and free:

```js
const client = server.getClient(id);   // undefined if it is on another node
if (client) client.sendEvent('system/notice', { text: 'hi' });
```

## Commands

```js
server.cluster.join(clientId, 'ops');           // addressed: ONE node hears it
server.cluster.leave({ room: 'chat' }, 'x');    // selector: applied on every node
server.cluster.disconnect({ room: 'banned' });
```

Because `client.id` is instance-prefixed, an id-addressed command travels as
**one message to one node** rather than a cluster-wide filter. A selector
(`{ room }`, or `{}` for everyone) is applied on every node instead.

Commands are fire-and-forget with the backplane's at-most-once delivery: they
are the right tool for "kick these connections", and the wrong one for
anything that must be exactly-once.

### One event to one client

```js
server.sendTo(clientId, 'chat/dm', { text: 'hi' });                   // here, or on the node its id names
server.sendTo(clientId, 'signaling/signal', payload, { room: 'lobby' }); // only while it is still in `room`
```

`server.sendTo` is the id-addressed counterpart of `to(room).emit`: a local
id is delivered directly, a foreign one becomes an addressed `event` command
on that node's channel (`server.cluster.send` is the same call without the
local short-cut). `room` bounds the delivery to a client still in that room —
a relay tied to a membership must not outlive it. It returns `true` when the
event was delivered locally or handed to the backplane and `false` when it is
known undeliverable (no such local client, a per-request HTTP client, not in
`room`, a foreign id with no backplane — or **bytes in `data` for a foreign
id**: a command envelope is JSON, so a `Uint8Array` would arrive on the
other node as the object JSON makes of it; the call answers `false` and
logs `cluster.bytes` instead, while `to(room).emit` carries bytes across
the backplane as bytes). The remote leg is at-most-once, like every
command.

## Node-to-node messaging

```js
server.cluster.sendEvent('cache/invalidate', { key });        // fire-and-forget
server.cluster.on('cache/invalidate', ({ key }) => cache.delete(key));  // on OTHER nodes

server.cluster.respond('stats', async () => ({ load: cpu() }));
const { answers, errors, incomplete } = await server.cluster.ask('stats');
```

As everywhere in wrpc, `emit` is the local `Emitter` emit and the wire send is
`sendEvent`. One responder per name — two answers to one question are
ambiguous, so a duplicate `respond()` throws. A node **without** a responder
contributes an entry in `errors`, not silence.

## Acks: asking clients

The cluster asks nodes; `ask()` on a room or a client asks the **peers**. On
the wire it is the ordinary `event` packet plus an `id`, answered by the
ordinary `callback` — no new packet type.

```js
// Browser
client.respond('chat/poll', async ({ question }) => ({ vote: 'yes' }));

// Server — one peer
const answer = await context.client.ask('chat/poll', { question: 'ready?' }, { timeout: 5000 });

// Server — a whole room, across every instance
const { answers, errors, expected, incomplete } =
  await server.to('chat').ask('chat/poll', { question: 'ready?' });
```

A room `ask()` **never rejects**: a question with many answerers has no single
failure. It resolves with:

| Field | Meaning |
| --- | --- |
| `answers` | What the responders returned. |
| `errors` | Per-client failures — `501` no responder, `408` timeout, `503` disconnected mid-question. |
| `expected` | How many clients were asked, across every instance. `local()` keeps the question here. |
| `incomplete` | `true` only when a remote instance never reported back. |

A single-peer `client.ask()` does reject, with those same codes. Under both
sits the pair `client.expectAnswer(id, timeout)` / `client.settleAnswer(packet)` —
the bookkeeping half, which `Broadcast.ask()` uses so it can write one
pre-serialized packet to many peers instead of building one per recipient.

## What the cluster does not do

- **It does not move connections.** SSE channels live on the instance that
  created them and need sticky routing; see [scaling](./scaling#what-stays-per-instance).
- **It does not make delivery exactly-once.** Presence self-heals because it is
  a replicated counter with a correcting snapshot; events and commands do not.
- **It does not share event logs.** `createEventLog()` is per-process memory,
  and its epoch-stamped ids make that *visible* to a resuming client rather
  than silently lossy — see [subscriptions](./subscriptions#event-log-ids-carry-an-epoch).
