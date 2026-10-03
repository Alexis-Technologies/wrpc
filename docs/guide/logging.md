# Logging

Every server component writes through one injected logger. Pass yours as
`logger`:

```js
const pino = require('pino');
const { Server } = require('@alexify/wrpc');

const server = new Server({ router, logger: pino(), port: 8000, protocol: 'http' });
```

That is the whole configuration. There are no levels to set, no destination to
choose and no format to pick — those are your logger's job, and wrpc does not
duplicate them.

::: info The logger is injected, never depended on
`@alexify/wrpc` has no dependencies and never will. Your logger is duck-typed:
wrpc looks at which methods it has, not at what it is. `pino` is a
devDependency here purely so the tests have something real to write into.
:::

## What you can pass

| Value | Behaviour |
| ----- | --------- |
| a **structured** logger | called as `(entry, message)` — pino, bunyan, winston, roarr |
| a **Console** | called as `(message)`; the entry is dropped |
| `true` | the global console |
| `false` / `null` | silent |
| omitted | the global console |

A logger that matches neither shape disables logging rather than throwing.
Observability is never the reason a server fails to boot.

### How the two shapes are told apart

By `child()` or a `level` property. Every structured logger worth injecting
has one or both; a `Console` has neither.

The asymmetry is deliberate. Guessing "structured" wrong prints an object
where a string belongs — loud and obvious. Guessing "Console" wrong drops
some fields — quiet. So a sink giving no signal is treated as a Console.

If you have a `(entry, message)` logger without `child` or `level`, give it a
`level` property and it takes the structured path.

## Entries

Every line carries a machine-readable entry and a human message. Structured
loggers get both, in pino's `(mergingObject, msg)` convention; a Console gets
the message alone.

```json
{ "level": 30, "component": "rooms", "peer": "10.0.0.4",
  "event": "call.ok", "method": "chat/send", "msg": "10.0.0.4\tCALL\tchat/send\tOK" }
```

`event` is on every entry and is the field to filter on. An `err` key, when
present, holds the Error itself.

### Bindings

wrpc builds child loggers so you do not have to correlate by hand:

| Binding | Scope |
| ------- | ----- |
| `component` | `rooms`, `sse`, `sessions`, `cluster` or `redis` — built once per server |
| `peer` | one connection, bound when it opens |
| `callId` | one call, subscription or inbound event |

`callId` is bound lazily, because a `Context` is allocated for every packet
and most handlers never log. It costs nothing until something asks for it.

## Logging from a handler

`context.log` is a child bound to that call:

```js
const send = procedure({
  access: 'session',
  handler: async (context, { text }) => {
    context.log.info({ event: 'message.accepted', length: text.length });
    return { ok: true };
  },
});
```

The entry it writes already carries `callId` and `peer`, so a single call is
greppable end to end without threading an id through your own code.

## Event catalogue

`event` is the field to build alerts on, so here is what wrpc emits, by
component: every `event` name in the source is in one of the tables below —
a test keeps it so, and a new line without a row fails the build. Handlers
add their own through `context.log`; those are yours to catalogue.

The first table is the request path and the engines; the ones after it are
[rooms, backplane and cluster](#rooms-backplane-and-cluster),
[sessions and SSE](#sessions-and-sse), [brokers](#brokers),
[WebRTC](#webrtc), [the server's own life](#lifecycle-and-hooks) and
[the client](#client-events).

| `event` | Level | Means |
| ------- | ----- | ----- |
| `rpc.error` | error/debug | An error answer went out. `error` when nothing else said why — a handler that threw (`err` carries the stack), a stream or chunk that could not be taken; `debug` under every refusal that has its own line above, so one refusal is one alert |
| `call.unknown` | warn | A call for a method this router does not have — a stale client, or a typo |
| `call.duplicate` | warn | Two in-flight calls with one id: a `generateId` that repeats, or a retry that reused one |
| `call.capacity` | debug | `maxCalls` reached on one connection |
| `stream.capacity` | debug | `maxStreams` reached on one connection: a `stream` packet answered `429` |
| `call.draining` | info | Refused because the server is shutting down |
| `packet.malformed` | warn | A frame that would not parse — one line per frame, whatever it was |
| `packet.unknown` | warn | Valid JSON that is not a packet — version skew, or somebody else's client |
| `revision.peer` | debug | A connection settled on [revision 1](../reference/protocol#versioning) — a 1.0 peer, or one that reads no frames (`transport`; on a WebSocket the selected `protocol`, on a worker port the revision the page `named`). Per connection, so debug: the count is `wrpc.server.connections` by `wrpc.revision` |
| `revision.mismatch` | warn, then debug | A WebSocket engine composed by hand selected `wrpc.v2` (`protocol`) for a server that sends and reads no framed messages (`attachments: false`, a packet codec): the client will send a frame this server refuses. The built-in shells narrow their engine to `wrpc.v1` themselves — pass `protocols: ['wrpc.v1']` to yours. A warn for the first connection, debug for every one after it |
| `batch.refused` | debug | A batch outside `1..maxBatch` |
| `subscribe.refused` | warn | A subscription refused before it started (`code`; the peer's `id`, clipped to 128 characters) |
| `ws.frame`, `ws.protocol`, `ws.invalid-utf8` | warn | A peer's frame violated the protocol; the connection was closed |
| `ws.upgrade` | error | The node engine's upgrade handling threw (`err`) — a `verifyClient`, `handleProtocols` or deflate `filter` of the application, or a `'connection'` listener; the socket was answered `500`. Emitted as `'error'` on the `WebsocketServer` too, when something listens |
| `ws.too-big`, `ws.overflow`, `ws.backpressure` | warn | A configured limit closed the connection — the entry names the limit |
| `ws.inflate`, `ws.deflate` | warn | permessage-deflate failed on the way in (or the inflated message was too big), or on the way out; the connection was closed |
| `ws.close.dropped` | warn | A graceful `close()` waited `closeTimeout` for messages still queued behind a compress in flight (context takeover, `async`) and gave up: `frames` and `bytes` never left, the Close frame did |
| `compression.failed` | warn | A per-message or HTTP/SSE codec threw (`carrier`: `http`, `sse`, `wt`, `rooms`, `cluster`, `broker`; `direction`: `encode` — the message left plain — or `decode` — the frame was refused; `codec`, `code`, `err`). **Once** per carrier, direction and codec for the life of the server; every failure is counted in `wrpc.compression.failures` |
| `frame.refused` | warn | A framed message from a socket peer that was not taken: a kind nobody negotiated (`kind`, `negotiated`), or one that did not inflate (`reason: 'inflate'`, the `codec`, and the `code` — `ERR_BUFFER_TOO_LARGE` is `maxMessage`, anything else the bytes). Answered with an id-less `400`; the connection lives |
| `socket.error` | debug | The engine socket under a client raised `error`; the connection is closed. The engine's own line (`ws.*`, `uws.dropped`) already said what happened |
| `uws.dropped` | error | uWebSockets.js discarded an outbound frame: a hole in the stream |
| `session.destroy`, `session.save`, `session.touch` | error | The session store rejected an operation |
| `session.evict` | warn | Live sessions dropped for capacity — signed-in users signed out |
| `session.corrupt` | warn | A stored session would not parse. **Never carries the token** |
| `backplane.gap`, `backplane.redis` | warn/error | Envelopes lost between instances; a backplane client failed |
| `backplane.keys`, `cluster.keys`, `session.keys` | error | The key provider threw while a sealed envelope, command or session row was being opened (`err`, and the `kid` when it is one) — this side's failure, not a refusal of the message |
| `backplane.open`, `cluster.open`, `session.open` | warn | A sealed envelope, command or row did not open (`reason`: `kid`, `open`, `replay`, `format`, `codec`, plus the `kid` when it is one). On the backplane and the cluster: a warn once per reason and channel in ten seconds, debug in between — as is `*.unsealed` |
| `session.adopt` | info | A plaintext session row was read under `acceptPlaintext` and re-written sealed — count these down before turning the option off |
| `backplane.seal`, `cluster.seal` | error | A sealing envelope could not seal (a keyring without its current key); the event stayed local, nothing left in the clear |
| `cluster.unsigned`, `cluster.badsig`, `cluster.verify` | warn/error | The three ways envelope authentication fails. `unsigned` and `badsig` are a warn once per sender and reason each `presenceTimeout`, debug in between, like `cluster.replay` |
| `cluster.replay` | warn, then debug | A correctly signed envelope refused all the same (`from`, `channel`, `reason`): `seq` — a counter already accepted, `channel` — signed for another channel, `stale` — outside `maxSkew`, or a previous life of a restarted sender, `unsequenced` — no counter at all, a 1.x node (`replay: 'accept'` during the upgrade). One warn per sender and reason each `presenceTimeout`, debug in between |
| `cluster.unsequenced` | info, then debug | An envelope with no counter — a 1.x node's — accepted under `cluster: { replay: 'accept' }` (`from`): once per sender each `presenceTimeout`. When no line has come for a while, every node is 2.x and the option can go |
| `cluster.bytes` | warn | Bytes in what a `Cluster` constructed by hand — without the envelope an `RpcServer` injects — was asked to send to another instance (`type`, `name`): nothing left, rather than the object JSON makes of bytes. Never logged by a server's own cluster, which carries them as a binary envelope |
| `encryption.refused` | warn/error | A session handshake or a sealed request refused — ONE line, with `kind` (`ws`, `wt`, `http`), `reason` and, on a socket, the `peer`. A peer's doing is warn: `plaintext`, `handshake`, `protocol` (with the name when it is shaped like one, else `nameLength`), `kid` (the kid when it is one, else `kidLength`), `authorize`, `timeout`, `queue`, `crypto`; on http `format`, `open`, `stale`, `replay`. This side's failure is error, with `err`: `keys` (the key provider threw), `hook` (`authorize` threw), `replay-store` |
| `encryption.established` | debug | A session handshake completed (`protocol`, `kind`, `peer`) |
| `encryption.replay`, `encryption.unwrap` | error | The shared replay store could not be asked (the request was refused `503`); a primitive threw while unwrapping a sealed request (`500`) |
| `encryption.ambient-session` | warn | The server has `encryption` and the cookie session transport: a cookie stays on the outer request, readable by the terminator the encryption keeps out — use `bearerTransport()`/`payloadTransport()`. Once, at construction |
| `encryption.queue.dropped` | warn | Room/broadcast events dropped for one client while its handshake was still running (`count`), past the 256 held for it — said once when the handshake completes; the connection lived |
| `encryption.replay.overflow` | warn | The built-in replay memory is full of live entries: sealed requests are refused `503` until some expire (`refused` since the last line, one line per ten seconds) — size `replay.max` for the traffic, or share the memory |
| `broker.dead` | warn | A message exhausted its retries |
| `broker.evict` | warn | The per-token client cache is thrashing: more distinct tokens in flight than `tokenClients`, a session restore per message. The evicted client finishes what it holds, then closes |
| `broker.amqp.consumer.closed`, `broker.amqp.cancelled`, `broker.amqp.consumer.reopen` | error | RabbitMQ closed a consumer's channel, or cancelled the consumer (its queue deleted, its node gone); the consumer re-opens with a backoff, each failed attempt logged `reopen` with its `attempt` |
| `broker.amqp.backplane.rebind` | warn/error | The backplane's consumer channel closed under live rooms: `warn` once it is back with every room bound again (`channels`, `attempt`), `error` per failed attempt. Events published meanwhile were lost — the rooms layer sees a gap |
| `broker.amqp.tail`, `broker.kafka.tail`, `broker.nats.tail` | error | A feed's live read ended on its own (a consumer cancelled, a channel closed, a crash); the subscribers move to a fresh read and catch up from their positions |
| `broker.kafka.settle`, `broker.amqp.settle` | error | The broker refused a settlement three times (`action`, `id`, `attempt`); the message was handed back — a seek to its offset on Kafka (`healthy` false until a settlement lands), a requeue on RabbitMQ — never committed or acked past |
| `broker.kafka.rebalanced` | info | A retry's in-process wait ended in a failed heartbeat: the group is rebalancing, and the message is the new owner's to fetch — no copy published, nothing committed |
| `broker.nats.consumer.config` | warn | A queue's durable consumer already existed with another `ackWait`/`maxAckPending`; the old values stay until the consumer is updated |
| `broker.kafka.topic` | info | The adapter created a topic (`topic`, `partitions`, `replicationFactor`) — once per topic, never for one that already existed |
| `broker.kafka.crash` | error | A kafkajs consumer's fetch loop died (`restart` says whether it rejoins by itself); the binding is unhealthy until it rejoins |
| `broker.kafka.join-timeout` | info | A queue consumer got no partition within the join window — more instances than partitions, most likely; it stays healthy |
| `broker.amqp.connection` | error | The injected RabbitMQ connection closed: every binding on this broker is unhealthy and stays so — open a new connection and a new broker on it |
| `broker.rpc.refused` | warn | A sealed RPC frame that did not open (`reason`: `unsealed`, `kid`, `open`, `format`, `replay`), one older than `maxSkew` (`stale`), or a plaintext frame on a session a sealed hello opened (`downgrade`) — dropped, never answered |
| `broker.rpc.replay` | error | The shared replay memory could not be asked; the frame was not served |
| `broker.rpc.session.end` | warn/info/debug | A broker RPC session ended (`session` is a fingerprint, `reason`): `warn` when the broker lost or mangled a frame — `sequence gap: expected N, got M` seen here, `the client saw a gap` when the frame was lost on this side's way out and the client's goodbye said so, `undecodable frame` — `info` for `idle`, `debug` for a routine `bye`, `replaced`, `send failed` or `server closing`. The same ends are counted, by a closed set of reasons, in `wrpc.broker.rpc.session.ends` |
| `broker.rpc.session.unknown` | debug | A frame for a session this instance does not hold (it restarted, or the session idled out); the client was told to reconnect. Debug: any participant can send these in a loop |
| `broker.rpc.send`, `broker.rpc.reply` | warn | The broker refused to carry a frame to a session's inbox, or a reply to a stateless request |
| `broker.rpc.capacity` | warn | Hellos refused at `maxSessions` since the last sweep (`refused`, `sessions`, `max`) — one line per sweep, not per hello |
| `broker.refused` | warn/error | A sealed delivery a consumer could not open (`reason`: `unsealed`, `kid`, `open`, `format`); `kid` is retried, the rest dead-letter. `keys` — the key provider threw — is an error line with `err` |
| `broker.feed.refused` | warn/error/debug/info | A sealed entry a feed could not open: `warn` once per reason per ten seconds with the running `count` (`error` with `err` when the reason is `keys`), `debug` in between, an `info` summary when the feed ends. An entry the live subscribers of a topic share is reported by the one that opened it, not by each |
| `broker.feed.resume` | warn/debug | A resume token was refused. `reason: 'signature'` means it was **tampered with** |
| `broker.feed.gap` | info | A subscriber fell behind retention; the snapshot hook ran |
| `broker.tail.lag` | info | Readers of a log topic (`topic`, `readers`) fell `highWaterMark` entries behind its live tail: their buffers were dropped and they catch up through the broker's range read — slow subscribers, and range load on the broker. Once per fall, not per entry |
| `wt.attach`, `wt.source` | error | A WebTransport session could not be attached (`verify` threw, the source died) |
| `wt.onError` | error | The `onError` handed to `acceptSessions` threw itself (`err`) — said here instead of rejecting `done` |
| `wt.refused` | warn | A session refused before attach: `status` 403 (`verify` said no) or 408 (no control stream, or `ready`/`verify` not settled, within `acceptTimeout`) |
| `wt.accept.saturated` | warn | `maxPending` sessions were still in their handshake; the next was refused 503 — said once per episode |
| `wt.backpressure` | warn | A session's queue passed `maxBackpressure` — a client that does not read — and it was terminated (`buffered`, `max`): the WebTransport `ws.backpressure` |
| `wt.violation`, `wt.idle`, `wt.session.error` | warn | A peer's frame that could not be read (`code`; closed 1002), a peer silent past `idleTimeout` (terminated), the session's own failure reported by the host |
| `wt.mux.refused`, `wt.mux.fallback` | warn / info | A side stream the peer opened for an id it never named, past the cap or unannounced (cancelled unread); the host granted no side stream, so every chunk of this session rides the control stream — once per session |
| `wt.datagram.dropped` | warn | The session had not taken the 64 datagrams before this one, so it was dropped rather than queued — **once** per session; the total is `dropped` on that session's `wt.close` line |
| `wt.close` | debug | The session ended (`code`, the peer's `reason` clipped) — the line to grep for a code |
| `rtc.channel.error` | warn | A framing error from the peer on a raw data channel attached with `attachChannel` (`peer`, `err`); the channel was closed |
| `mesh.dial` | debug | A mesh edge never formed |
| `mesh.unreachable` | warn | A member still in the room could not be linked again and the mesh's re-dial has backed off to its slowest pace (`room`, `peer`, `attempts`) — once per outage; it keeps dialling |
| `signaling.undeliverable` | debug | A signal for a peer the relay no longer has (`to`, `room`, `type`) — a trickled candidate that crossed its `leave`, a routine race |
| `rtc.signal.overflow` | warn | A peer sent more signals than are held for it — candidates before its description (256), or anything while `accept()` still thinks (64); the rest are dropped, said once |
| `http.refused` | warn | An HTTP request the core refused before any call existed (`code`, and the `path` or `method`): 404 for a path that is not the RPC's, 403 for a packet request that is not a POST |
| `http.failed` | error | Something threw while an HTTP request was being routed (`err`): the request was answered `500` — or left as it was when an answer or a stream had already started. A bug, or a peer's input nothing refused first |
| `cors.refused` | warn | A request from an `origin` the `cors` option does not allow |
| `subscribe.end` | warn/debug | A subscription ended (`id`, `method`): `warn` with the `code` when it died on the server's side, `debug` for a completion or an unsubscribe |
| `subscription.return` | warn | A subscription handler's own cleanup (`finally`, `return()`) threw after the stream had ended |
| `subscription.aborted` | debug | A subscription handler threw after the peer had already unsubscribed or gone; the error reaches nobody else |
| `stream.terminate` | error | A stream's `terminate()` threw while a closing client's streams were being torn down |
| `call.ok` | debug | A call answered without an error (`method`, `id`) — the narration line, built only when a logger takes debug |
| `rpc.warn` | warn | A warning about one connection that has no name of its own — what `client.warn(message)` writes when it is given no `event` |
| `encryption.handshake` | debug | A sealed socket's handshake step failed inside the library; the refusal that follows (`encryption.refused`) is the line to alert on |
| `encryption.respond` | error | A sealed HTTP answer could not be sealed (`err`); the request got a bare `500` |

### Rooms, backplane and cluster {#rooms-backplane-and-cluster}

| `event` | Level | Means |
| ------- | ----- | ----- |
| `broadcast.serialize`, `broadcast.send` | error | A broadcast's data could not be serialized (nobody received it), or one recipient's socket threw while the fan-out went on to the others |
| `backplane.serialize` | error | A room event could not be serialized for the backplane: delivered locally, lost for the other instances |
| `backplane.publish`, `backplane.subscribe`, `backplane.unsubscribe` | error | The backplane rejected a publish, a channel subscribe (`attempt`; retried with a backoff, `healthy` is false meanwhile) or an unsubscribe |
| `backplane.recovered`, `cluster.recovered` | warn | A subscribe that had been failing went through (`channel`, `attempt`). Whatever was published in between is lost — the gap is the news |
| `backplane.deliver`, `backplane.handler` | error | An envelope from another instance could not be delivered to local sockets; a subscriber of the in-process `MemoryBackplane` threw |
| `backplane.encoded`, `cluster.encoded` | warn | A compressed envelope (`wrpc-enc:`) reached an instance whose `compression` does not hold that codec — a rollout that changed the codec in one step |
| `backplane.sealed`, `cluster.sealed` | warn | A sealed envelope reached an instance with no `encryption` keys: it reads none of the fleet's events until it is given them. `cluster.sealed` (and `cluster.encoded`) is a warn once per channel each `presenceTimeout`, debug in between |
| `backplane.unsealed`, `cluster.unsealed` | warn | A plaintext envelope reached an instance that seals and does not `acceptPlaintext` — an instance left behind by the rollout, or somebody publishing by hand |
| `backplane.bytes` | warn | Bytes in an event that a rooms registry wired by hand — without the envelope an `RpcServer` injects — was asked to send across instances (`name`): delivered locally only |
| `rooms.option` | warn | An unknown key under `rooms` (`key`) — a typo that would otherwise be a silently ignored option |
| `cluster.join` | info | An instance joined the cluster (`instance`) |
| `cluster.evict` | warn | An instance went silent past `presenceTimeout` and was dropped from presence (`instance`, `reason: 'timeout'`) — partitioned, or dead without a goodbye. A routine goodbye is not logged |
| `cluster.fetch.incomplete`, `cluster.fetch.truncated` | warn | `fetchClients` returned less than the fleet has (`received`): an instance did not answer in time, or one answered only its first `maxFetch` clients |
| `cluster.serialize`, `cluster.publish`, `cluster.subscribe`, `cluster.unsubscribe` | error | A cluster envelope could not be serialized (`type`), published, or its channel subscribed (`attempt`; retried) or unsubscribed |
| `cluster.command`, `cluster.serve` | error | A command from another instance threw while it was applied here (`op`); a request from another instance — a `fetch`, an ask — threw while it was served |
| `cluster.listener` | error | An application listener of a cluster event (`name`) threw |

### Sessions and SSE {#sessions-and-sse}

| `event` | Level | Means |
| ------- | ----- | ----- |
| `session.restore` | error | The session store threw while a connection's session was being restored (`err`); the client continues anonymous |
| `session.unsealed` | warn | A row under a sealed store's key that is not a sealed row of that `kid` — written by something else. Read as a missing session; **never carries the token** |
| `session.migrate` | warn | A stale copy of a session row — under an older key, or a sealed one while `seal: false` — could not be deleted (`err`); the session itself was written |
| `sse.refused` | warn | A channel request refused (`code`, `reason`): an unknown or expired channel, a wrong channel secret, a cap (`maxChannels`, `maxChannelsPerAddress`) |
| `sse.gap` | warn | A reconnecting stream asked for events the replay buffer no longer holds (`channel`, `requested`): event loss, told to the client — which logs the same name when it hears it |
| `sse.expired` | debug | A channel nobody re-attached to within `retention` was dropped |
| `sse.supersede`, `sse.close` | error | Ending a stream a newer one replaced, or closing a channel, threw |

### Brokers {#brokers}

The broker bindings and adapters are in the first table above where a line
is one to alert on; these are the rest. Every adapter line carries `err` and,
where it has one, the `queue`, `channel`, `topic` or `address`.

| `event` | Level | Means |
| ------- | ----- | ----- |
| `broker.ack`, `broker.retry`, `broker.release` | debug | What a delivery's outcome became (`queue`, `method`, `code`, `attempt`, `delay`) — the narration next to `broker.dead`, built only when a logger takes debug |
| `broker.dispatch` | error | A procedure threw outside the call pipeline while a delivery was dispatched (`queue`, `method`, `id`); the delivery is settled by its code all the same |
| `broker.settle` | error | The broker refused the settlement of a delivery (`queue`); the message comes back by redelivery — a duplicate, not a loss |
| `broker.attach` | error | A delivery's client could not be attached (`queue`) — a session store that threw; the delivery is retried, then dead-lettered |
| `broker.onDeadLetter` | error | The `onDeadLetter` hook threw |
| `broker.feed.decode` | warn | A log entry that did not decode (`topic`, `id`); skipped, the feed goes on |
| `broker.rpc.hello.duplicate` | debug | A second `hello` for a session that is live, or from another inbox (`session` fingerprint, `live`); ignored — the session stays its owner's |
| `broker.delivery`, `broker.listener` | error | `MemoryBroker`: a queue handler or a direct listener threw; the delivery is retried after the crash backoff |
| `broker.redis.delivery`, `broker.nats.delivery`, `broker.amqp.delivery`, `broker.kafka.delivery` | error | A queue handler threw or rejected (`queue`): it settled nothing, so the delivery is retried after a backoff, attempt + 1 |
| `broker.redis.listener`, `broker.nats.listener`, `broker.amqp.listener` | error | A direct listener's handler threw or rejected |
| `broker.nats.handler`, `broker.amqp.handler`, `broker.kafka.handler` | error | A backplane subscriber threw (`channel`) |
| `broker.redis.read`, `broker.nats.consume` | error | A queue consumer's read failed (`queue`): the consumer is **unhealthy** and retries with a backoff — the line that says a consumer is not consuming |
| `broker.redis.settle`, `broker.nats.settle` | error | A settlement failed (`queue`); the message is redelivered by the broker |
| `broker.redis.sweep` | error | Promoting delayed retries, or reclaiming entries a dead consumer held, failed (`queue`); tried again on the next sweep |
| `broker.redis.blpop`, `broker.redis.presence` | error | A direct service listener's blocking read, or its presence lease, failed (`address`): the listener is **unhealthy** — `attachBrokerRpc().healthy` is false — until the next one works |
| `broker.nats.subscription` | error | A NATS subscription reported an error (`channel` or `address`): it delivers nothing more, and a direct listener on it is unhealthy for good |
| `broker.redis.tail` | error | The log's live read failed; retried, the subscribers catch up from their positions (the Redis twin of `broker.*.tail` above) |
| `broker.redis.pubsub`, `broker.redis.decode` | error | The pub/sub connection raised an error; a direct message that could not be decoded (`channel`) was dropped |
| `broker.redis.quit`, `broker.kafka.disconnect` | error | Closing a connection the adapter opened itself threw |
| `broker.nats.publish`, `broker.amqp.publish`, `broker.kafka.publish` | error | A backplane publish failed (`channel`): that event is lost for the other instances |
| `broker.amqp.channel` | error | A channel raised an error (the close that follows is what the consumers react to) |
| `broker.amqp.unbind` | error | Unbinding a room nobody is in any more failed (`channel`) |
| `broker.kafka.pause`, `broker.kafka.resume` | error | Pausing or resuming a queue consumer threw (`queue`) |
| `broker.nats.consumer.info` | debug | A durable consumer's configuration could not be read back to compare it |
| `broker.kafka.partitions`, `broker.kafka.metadata` | debug | A topic exists with another partition count than configured (`expected`, `actual`); its metadata could not be read |
| `broker.kafka.seek`, `broker.kafka.groups` | debug | A reader's seek to its start position still failed on the last of five tries; a reader's throwaway consumer groups could not be deleted (`groups`) and are left for the broker to expire |

### WebRTC {#webrtc}

Written through the peer's own `logger` — in a browser, off unless you pass
one — and bound to the remote `peer`.

| `event` | Level | Means |
| ------- | ----- | ----- |
| `rtc.link.state` | debug | A link changed state (`state`, `previous`) — the narration to turn on when a connection misbehaves |
| `rtc.ice.failed`, `rtc.ice.restarted` | warn / info | The connection failed and an ICE restart was started; the restart brought it back |
| `rtc.link.failed` | warn | A link failed (`reason`) — a dial or an ICE restart that did not complete; the peer link above it decides whether to redial |
| `rtc.link.error` | error | Something threw inside the link (`origin`: the dial, a negotiation step, a channel) |
| `rtc.signal.malformed`, `rtc.signal.unknown` | warn | A signal that is not an object, or of a `type` this peer does not know — version skew, or a relay passing on something else |
| `rtc.peer.redial`, `rtc.peer.gave-up` | info / warn | A dead link is being dialled again (`attempt`, `delay`); the redial budget ran out (`attempts`) and the link is closed |
| `rtc.peer.refused` | warn | A peer was refused before its description was applied (`peer`, `reason`: the assertion's code — `signature`, `fingerprint`, `expired`, … — or `assertion`) |
| `rtc.peer.incarnation` | info | A known peer id came back as another instance (`previous`, `instance`): the old link is dropped for the new one |
| `rtc.peer.reset`, `rtc.peer.replaced` | warn | The signaling connection was re-established (`changed` says whether this peer's id did — if so every link is dropped); a newer connection took this peer's id, and this one closed |
| `rtc.peer.duplicate` | error | A second link was about to be made to a peer that has a live one (`state`) — a bug guard: the existing link is kept |
| `rtc.peer.error`, `rtc.peer.unhandled` | error | A link's client or host threw (`peer`); an error nobody listens for (`'error'` has no listener on the peer) |
| `rtc.peer.client` | debug | The link's client reported an error while the link was down — its reconnect attempts fail by design then |
| `mesh.leave.stale` | debug | A `leave` naming another incarnation of a member than the one linked; ignored |

### Lifecycle and hooks {#lifecycle-and-hooks}

| `event` | Level | Means |
| ------- | ----- | ----- |
| `listen`, `listen.retry` | info / warn | The server is listening (`port`); the address was in use and the bind is tried again |
| `close` | error | The HTTP server's `close()` reported an error. (The client logs `close` at info: the connection ended) |
| `hook.error` | warn | An observational router hook — `onResponse`, `onError`, `onDisconnect` and the like — threw (`phase`, `err`): reported and contained, what it observed is unaffected |
| `onConnect.stalled` | warn | An `onConnect` hook has not settled within the stall window (`peer`, `ms`): the client's calls are waiting on it |
| `listener.close`, `listener.attach`, `listener.detach` | error | An application listener of a client's `close`, or of a peer host's `attach`/`detach`, threw |

### Client events

The client writes through its own `logger` — **off by default**, so none of
these appear until you pass one (see [below](#errors-are-observed-not-swallowed)).
They are what a "connection that works but is missing something" looks like:

| `event` | Level | Means |
| ------- | ----- | ----- |
| `meta.oversize` | warn | A declared bag (headers, meta, or the per-call aggregate under batching) did not fit `metaMaxBytes` on its carrier (`carrier`, `bytes`) and was **not sent** — the server would have dropped it whole |
| `declared.unsendable` | warn | A declared header could not travel on this carrier at all (a value the carrier's grammar refuses) |
| `declared.exposed` | warn | Declared headers were put in the connect URL's query, where access logs keep them: a credential (`key: 'authorization'`) because `carrier: 'query'` (or WebTransport) left no other carrier, or every declared header (`keys`) when the client redialled with the query on its own (`handshake.requery`) — an `x-api-key` belongs in the `authorization` header a server reads, never in a declared bag |
| `handshake.requery` | warn | The server answered `wrpc.v1` to a handshake that offered `wrpc.v2` and carried `headers`/`meta` as subprotocol tokens — possibly a 1.0 server, which reads them from the connect URL only. The client dialled again with the query carrier, and keeps it for every later reconnect; `carrier: 'query'` up front saves the extra handshake, `carrier: 'protocol'` forbids the query |
| `handshake.ambiguous` | warn | A browser client that offered `wrpc.v1` alone (`attachments: false`, or its own `protocols`) carried `headers`/`meta` as subprotocol tokens and was answered `wrpc.v1`: a 2.x server read them, a 1.0 server did not, and the answer is the same — so there was no redial. Once per transport. Against a 1.0 server set `carrier: 'query'` |
| `handshake.fallback` | warn | The Node WebSocket constructor refused the headers init bag (`reason` is the error's name, never the message: undici repeats a header value in it); the browser carriers were used instead |
| `transport.fallback` | warn | A transport of the list could not connect; the next one was tried |
| `authenticate.failed`, `refresh.failed`, `restore.failed` | warn | The `authenticate` hook, the `refresh` hook, or the re-`load()`/re-subscribe after a reconnect failed |
| `reconnecting`, `reconnect.failed` | info/warn | A reconnect attempt (`attempt`, `delay`), and the one that gave up |
| `open`, `reconnected` | info | The connection opened (`url`) — for the first time, or again after `attempts` reconnect attempts |
| `close` | info | The connection ended (`url`); a reconnect, if any, follows as `reconnecting` |
| `sse.gap` | warn | The server said this channel's replay buffer no longer holds what was missed: event loss, not a routine reconnection |
| `heartbeat.timeout` | warn | No `pong` within the heartbeat window; the connection is terminated and reconnects |
| `subscription.refresh` | warn | The credential `refresh` run for a subscription's retry failed (`id`, `err`): the subscription ends with the refusal the feed earned — the twin of `refresh.failed` on the call side |
| `encryption.failed` | warn | The session handshake did not complete, or a frame did not open; the connection is closed — never a fallback to plaintext |

An error the client has no caller to hand to is **escalated**: logged at
`error` under an `event` that names where it came from, and emitted as
`'error'` (or printed, when nobody listens). Those names:

| `event` | Means |
| ------- | ----- |
| `transport.error` | The transport itself raised an error |
| `message` | Handling an inbound message threw — a malformed frame, a handler of yours behind it |
| `batch.flush`, `batch.dispatch` | Writing a batch of calls threw (its calls are failed, not left pending); handling one answer of a batch threw |
| `heartbeat.ping`, `heartbeat.terminate` | Sending a ping, or terminating the connection after a missed pong, threw |
| `reconnect.open`, `fallback.open`, `online.open` | An `open()` nobody awaits was rejected: a reconnect attempt, the next transport of the list, the re-open when the browser came back online |
| `reconnect.restore`, `reconnect.afterOpen` | What runs after a reconnect — the re-`load()` and re-subscribe, the `authenticate` hook — failed |
| `subscription.error` | A subscription ended with an error and had no `onError` to tell |
| `subscription.listener` | A subscription's own `onEnd`/release callback threw |
| `listener.open`, `listener.close`, `listener.drain`, `listener.reconnecting`, `listener.reconnect-failed`, `listener.heartbeat-timeout`, `listener.authenticate-failed`, `listener.refresh-failed`, `listener.restore-failed`, `listener.transport-fallback` | A listener of yours for that client event threw |
| `client.error` | An escalated error with no more specific origin |

### Why some refusals are `debug`

`call.capacity` and `batch.refused` are reachable by any peer, in a loop,
without authenticating. Logging those at `warn` turns a refused flood into a
log-pipeline flood — a worse outage than the one being prevented. They go to
`debug`, which a Console writer drops outright and a structured logger's own
level decides. The codes that mean something is genuinely wrong stay at
`warn`.

The `rpc.error` line that follows a refusal — the answer going out — is
`debug` for the same reason: the refusal's own line is the alert, at the
level chosen for it, and a second one at `error` would have put every 429
back on the pager. And the strings a peer chose — a method, a packet id, a
packet type — are clipped to 128 characters before they become a field, so
a log line is never as long as `maxPayload` allows a packet to be.

## What is deliberately not logged

Four places write no lines on purpose, and should stay that way:

- `src/query/` and `src/auth/` **require nothing** — that is what keeps them
  around a kilobyte and browser-safe. Pulling the logger in would break a
  stated invariant for a handful of lines.
- Stream chunking and WebRTC framing run **per chunk and per frame**, under a
  bundle-size budget. A line there is a firehose and bytes nobody asked for.
- The telemetry writers swallow every error they meet. That is not an
  oversight: telemetry must never be the reason a request fails, and a writer
  that logged its own failures would need a logger, which would need a
  failure path of its own.

Credentials never appear in an entry. A session token, a bearer token and a
resume token are all refused-by-name in the code: `session.corrupt` logs that
a row failed to parse, `broker.feed.resume` logs the *reason* and the length,
and neither carries the value. A broker RPC session id — a credential too —
appears only as a 12-character fingerprint (`broker.rpc.session.*`, and the
transport's `source`), and the client's `handshake.fallback` carries the
error's name, not the error: undici repeats a header's value in its message.

## Turning it off

```js
new Server({ router, logger: false });
```

Silent — not "writes to a sink that discards", but never called at all. The
disabled writer is a frozen no-op singleton whose `child()` returns itself, so
the per-connection and per-call bindings cost nothing when logging is off.

## Fastify

The [fastify plugin](./adapters/fastify) uses `fastify.log` unless you pass
`logger` explicitly. Since that is a pino, it goes in as a structured logger
and your wrpc entries land in the same stream as fastify's own, with the same
request ids.

## Failures

A logger that throws cannot break a request. Every call is guarded inside the
writer, so a broken transport, a full disk or a serialization error costs you
the line, not the call.

## Errors are observed, not swallowed

On the client the `logger` is **off by default** — a browser console filling
up with reconnect noise is not a default anyone asked for:

```js
const client = await WrpcClient.connect(url, { logger: pino() });
```

When it is on, an error is both logged and emitted as `'error'`. A logger
observes; a listener handles. You get both, and adding one does not silence
the other.
