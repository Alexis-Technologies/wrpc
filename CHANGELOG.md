# Changelog

All notable changes to **`@alexify/wrpc`** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Semver here versions the **JavaScript API**. The wire protocol carries its own,
narrower promise — see
[Stability](./docs/reference/protocol.md#stability).

## [Unreleased]

### Added

**Per-message compression on the broker binding and the backplane (`compression`)**
- The broker binding: `attachBrokerRpc(server, broker, { compression })`
  and `connect('broker://…', { compression })`, off by default and
  negotiated so the two ends upgrade in any order. A session names its
  codec on `hello` (`wrpc-enc`), the server that agreed answers it on
  `welcome`, and every frame past the threshold then travels compressed
  both ways — packets, events, subscription values, stream chunks —
  marked `wrpc-enc` on the frame; `{ compress: false }` per message and
  `writeWith` as on a WebSocket. A stateless request names what it accepts
  and travels plain itself (HTTP's `Accept-Encoding` shape); only the
  answer is compressed. A marked frame the receiver cannot inflate ends
  the session like a sequence gap; `maxMessage` (16 MiB) caps the inflate.
  Node↔Node, so the codec must answer synchronously — a promise-answering
  one is refused at construction (`src/compression/sync.js`). Measured
  (`bench/broker.js`): a session call answering a 9 KB result 9,315/sec
  plain, 5,407/sec compressed — ~80 µs a round trip for ~10× fewer bytes.
- The backplane envelopes: `rooms: { compression }` and `cluster:
  { compression }` deflate what this instance publishes past the threshold
  and carry it as base64 under a `wrpc-enc:<id>:` marker, since the
  backplane contract is strings. Nothing to negotiate against on a fan-out,
  so this one is a two-step rollout: an instance without the option drops
  such an envelope and logs `backplane.encoded` / `cluster.encoded` rather
  than staying silent. The cluster signs first, then compresses, so HMAC
  verification is unchanged. The codec is injected into `RoomsBackplane`
  and `Cluster` by the core, so the browser-bundled `rooms.js` carries none
  of zlib or base64.

**Per-message compression on WebRTC (`compression` on the peer, the transports and `attachChannel`)**
- The other half of the transports with nothing under them: SCTP over
  DTLS carries a data channel's bytes as they are. `new WrpcPeer({
  compression: true })` names the codec in every description this peer
  sends (`caps: { deflate: "deflate-raw" }` in the signal, next to the
  assertion when there is one) and a link compresses only once the other
  peer named the same — a peer without the option is served plain, and
  nothing hangs up. Bit 2 of the data-channel header is the DEFLATE flag,
  a reserved bit (a protocol error) until negotiated; a message past the
  threshold is compressed **before** fragmentation, the one place it
  exists whole, and every fragment carries the flag. `{ compress: false }`
  per message and `writeWith` on the peer transport, as on a WebSocket;
  an inflate past `maxReassembly` closes the channel like any bad frame.
- Over a raw channel there is no description to announce in:
  `compression` on `attachChannel`, on `connect(url, { channel,
  compression })` and on either transport is applied as given, so both
  applications turn it on or neither — documented, and tested: the plain
  side closes on the first flagged frame.
- `RtcLink` grew `caps` (announced) and `peerCaps` (read before the
  description is applied, so the channels open already knowing). The
  webrtc browser entry's budget 48 → 50 KB (measured 49.2).

**Per-message compression on WebTransport (`compression`, both ends)**
- Nothing compresses a QUIC stream's payload — HTTP/3 does headers only —
  so a WebTransport session carried exactly the bytes wrpc handed it. Now
  `attachSession`/`acceptSessions` and the client (`connect(url,
  { compression })` or the `wt` bag) take `compression: true | { codec,
  threshold }`, off by default and negotiated: each end names its codec in
  the capabilities message (`deflate: "deflate-raw"`) and compresses only
  once the other named the same, so a lone end is served plain. A packet
  or chunk past the threshold leaves as KIND 3 or 4 and is inflated before
  delivery; `{ compress: false }` per message still works; chunks on their
  own streams and datagrams are never compressed; an inflate past
  `maxMessage` is a 1002.
- `src/compression/`: the structural `Compressor` seam (`isCompressor`,
  exported from the main entry — `id`, `encode`, `decode(bytes, maxOutput)`,
  either may answer a promise), the platform codec as a browser-swapped
  pair (node:zlib raw deflate, sync, 1 KiB threshold; `CompressionStream`,
  async, 4 KiB — ~6× the cost per call and no dictionary), and the
  `Sequencer` that keeps the wire in order around an asynchronous codec
  with a synchronous fast path when nothing is in flight. Measured
  (`bench/message-compression.js`): a 1.4 KB callback 6.4× at 84K/sec, a
  24 KB one 13.8× at 12K/sec, a 108 B event 1.1× — the threshold's reason.
- The main browser entry's budget 20 → 22 KB (measured 20.8), sse 21 → 23:
  the seam every browser transport will share.

**`Content-Encoding` on the HTTP side (`http.compression`, `sse.compression`)**
- gzip for packet-mode and REST answers, opt-in and off by default:
  `http: { compression: true | { threshold, filter, level, memLevel, async } }`.
  Applied in the one funnel every HTTP answer leaves through, so packet
  POSTs, batch frames, REST results and errors all qualify. A response is
  encoded when the peer's `Accept-Encoding` admits gzip, the body is at or
  over `threshold` (1 KiB), nothing upstream set a `Content-Encoding` (a
  route's own `headers`, a framework plugin) and `filter(call)` agrees; it
  then carries `Vary: Accept-Encoding`, joined onto the CORS `Vary`. A REST
  ETag stays over the plain body, a `204`/`304` is never encoded, and
  `async: { threshold }` hands large bodies to zlib's threadpool — the same
  shape as `perMessageDeflate.async`. Nothing changes on the client: `fetch`
  inflates by itself. `bench/http-compression.js` prices it: a 1.6 KB
  callback 5.5× at 77K/sec, an 8.5 KB one 9.9× at 28K/sec.
- The event stream: `sse: { compression: true | { filter, level, memLevel } }`
  — one gzip member per response, flushed after every event, which is
  context takeover for free: a repeated 125 B tick leaves as 16 B (7.8×),
  and the `ready` frame is not held back by it. Decided per GET, so a
  re-attach negotiates again and replays through the new member. The wrpc
  SSE client needed no change; every host's `stream` writer is wrapped the
  same way.
- `rpcOptions` learned the `http` key: an `http` option given to `Server`,
  `wrpcFastify` or `createWrpc` reaches the core. Fastify's delegated REST
  routes stay fastify's — `@fastify/compress` encodes those.

**Compression, documented as the choice it is**
- Nothing in wrpc compresses by default — `perMessageDeflate` on the
  built-in engine, `compression` on uws — and that stays: a default would
  spend a deflate on every frame of every peer to save bytes only some of
  them need. The performance guide now says so in one place ("Compression is
  off by default"), with the cost of the knob from `bench/deflate-context.js`
  next to the ratio it buys; the knob table names `perMessageDeflate` itself
  ahead of `contextTakeover` and `async`, which do nothing without it; the
  server options, the rooms page, the proxy notes and the deploy checklist
  point at the same section. No behavior changed.
- The honest limits are written down too: the knob covers the WebSocket
  only (HTTP, SSE, WebRTC, WebTransport and broker frames are uncompressed
  today), and a Node client never sends a compressed frame — the built-in
  `WebSocket` offers `permessage-deflate` but only inflates, so a Node↔Node
  link compresses server→client only.

**Telemetry: the honest set**
- `wrpc.server.sessions` records all five operations. It only ever said
  `restore`, while a unit test asserted a `create` shape nothing in the
  package could produce — the metric described a surface that did not exist.
  `evict` is the one worth alerting on: unlike `expire` it discards sessions
  that are still live, which is signed-in users being signed out to stay
  under `maxSessions`.
- `wrpc.stream.direction` has both values. The `send` half was never
  recorded anywhere, so an attribute that promised two values had exactly
  one for the life of the metric. The server records it; the client does
  not, where the counter has nothing to feed and the bytes would only cost
  the browser bundle.
- **`wrpc.client.heartbeat.rtt`** — the client's only true latency signal
  that needs no call to produce it. The ping/pong pair is an exact round
  trip and both ends were already timestamped; nothing read them. A
  heartbeat that times out is counted, never timed: there is no round trip
  to measure, and a sample invented from the timeout would describe your
  configuration rather than the network.
- **`wrpc.server.queue.wait`** and **`wrpc.server.queue.depth`**. Time spent
  waiting for a concurrency slot was folded into `rpc.server.duration`,
  which made a saturated queue and a slow handler indistinguishable — two
  opposite problems with opposite fixes. `depth` carries no attribute at
  all: one series per procedure is a cardinality bomb, and the question is a
  whole-process one.
- **`wrpc.broker.delivery.attempts`**, recorded on settlement only (an ack
  or a dead-letter, never a retry), so it is the distribution of deliveries
  per message rather than a triangle counting each message once per
  attempt. The number was on every delivery all along and never read.
- **`wrpc.cluster.verifications`** and **`wrpc.rtc.assertions`** put a
  counter behind two security signals that had none: a backplane envelope
  that fails authentication, and a peer presenting a trust assertion that
  does not bind to the DTLS fingerprint of the connection it arrived on.
- **`wrpc.client.calls`** and **`wrpc.server.rooms`**: a client error *rate*
  was not derivable from the duration histogram alone (a call with no
  elapsed time recorded nothing), and live rooms had no gauge. The room
  gauge rides the existing first-member/last-member callbacks, so the hot
  join/leave path is untouched.

**Logging reaches the silent paths**
- `RpcServer` gains a `log` getter, the pair of the `otel` one that already
  existed for the same reason: a framework adapter or an external attacher
  now has somewhere to report a failure that never reaches a `Client`,
  instead of emitting an `'error'` nobody listens for. `Client` had both
  getters all along; the server having only one was the anomaly.
- The **dispatcher's six refused calls** — unknown method, duplicate id,
  `maxCalls`, draining, unknown packet type, oversize batch — now log. Their
  subscription twin had logged from the day it was written, so half of what
  a dispatcher rejects was invisible to an operator, and the invisible half
  was the one that says "your client and my router disagree about what
  exists". The levels are not uniform on purpose: the two any peer can drive
  in a loop go to `debug`, so a refused flood cannot become a log flood.
- The **WebSocket engine** logs for the first time. Every way a connection
  died at the framing layer — invalid UTF-8, a protocol violation, a message
  past `maxBuffer`, a backpressure limit, a failed inflate — emitted an
  `'error'` a server rarely listens for and then closed, which is the
  "it just disconnects sometimes" report with nothing to work from. They go
  through one seam now and name the limit that closed the connection, since
  raising it is the operator's decision to make. `WebsocketServer` takes a
  `logger` option; the `Server` shell passes its own writer down.
- `acceptSessions` (WebTransport) defaulted `onError` to `null`, so a session
  that failed to attach vanished without a trace — the one failure mode of a
  WebTransport server nothing above could observe. It now reports through the
  server's writer; an explicit `onError` still wins.
- Broker consumers log every settlement decision, not only the terminal one:
  a queue retrying itself in a circle used to look exactly like a healthy
  one. The dead-letter line carries the error, not just its code, and the
  per-token client cache says when it is thrashing.
- A tampered **resume token** on a broker feed is now a `warn` naming
  `reason: 'signature'` — previously indistinguishable from a stale or
  garbled one, and silent. The token itself is never logged.
- Session capacity eviction says so. Unlike a TTL sweep it discards sessions
  that are still live — signed-in users signed out with no error anywhere —
  and it logs once per sweep with a count, never once per session.
- `createUwsEngine` and `createRedisSessionStore` take a `logger`.
  uWebSockets.js dropping an outbound frame, and a stored session row that
  will not parse, both had no channel at all.
- `docs/guide/logging.md` gains an **event catalogue** — the `event` names to
  alert on, by component — and a section on what is deliberately left
  unlogged, so the four principled zeroes are not "fixed" later.

**Pluggable identifiers, everywhere an id is minted**
- `generateId` had shipped on the server, the client, the cluster, a peer
  host and the signaler, and then fourteen other places went on calling
  `randomUUID` directly — including the server's own `instanceId`, the SSE
  channel id and the broker RPC session id. One resolver in `src/utils.js`
  now backs every one of them, and the ids that had no seam got one.
- The **SSE channel id** is the one that mattered most: it is peer-visible,
  it keys the channel registry, and holding one is most of what proves a
  request belongs to a channel. An `RpcServer` passes its own `generateId`
  down, so a server that injected a generator now covers channel ids too;
  `SseChannels` takes the option directly when driven standalone.
- The **broker RPC session id** is the server's key for a connection's frame
  state, so a guessable one lets a sender inject frames into somebody else's
  session. The broker transport now mints it with the owning client's
  generator, handed down the same seam as `codec` and `log` — one option,
  resolved once, rather than the same option resolved twice in two modes.
- `instanceId` on `RpcServer` and `PeerHost` is minted by `generateId` when
  omitted, instead of a `randomUUID` the option could not reach. The "must
  not contain `.`" rule now covers a generated id too, and says which of the
  two options produced the offending one.
- Each broker adapter (`redis`, `nats`, `amqp`, `kafka`) and `MemoryBroker`
  takes a `generateId` for the names it puts on the wire — consumer names,
  inboxes, groups, message ids. An injected generator is used **verbatim**:
  the two sites that shortened a uuid still shorten the default, never a
  value you supplied.
- `rooms.epoch` is declared and forwarded. `RoomsBackplane` had accepted it
  all along, but `RpcServer` passed only `linger`, so it was unreachable
  from the public surface.
- Validation is uniform and happens once: a generator must be a function
  answering a non-empty string of at most 255 characters — the binary chunk
  header's own limit, now applied to every id rather than only to stream
  ids. The check calls the generator, and that first id becomes the
  `instanceId` rather than being discarded, so a counter-based generator
  still starts where you expect. The per-stream check stays: it catches a
  generator that only *sometimes* answers something too long.

**Message brokers, part 9: benchmarks and the finished guide**
- `bench/broker.js` measures what the bindings cost on top of a broker, on
  the in-process `MemoryBroker` so the numbers are wrpc's own overhead and
  not a network's: a queue delivery into a procedure end to end (~530k
  ops/sec bare, ~400k through a validator, a hook and a `meta` header),
  `createPublisher.publish` into a log append (~2.1M ops/sec) and a durable
  feed's pump, append to tracked value (~750k ops/sec).
- `docs/guide/brokers.md` gains a "choosing one" table, the README its
  broker rows, and `pnpm brokers:up`/`brokers:down` bring the four servers
  in `compose.yaml` up and down for the integration suites.

**Message brokers, part 8: the Kafka adapter (`@alexify/wrpc/broker/kafka`)**
- `createKafkaBroker({ kafka })` over an injected KafkaJS-shaped client —
  `kafkajs` or `@confluentinc/kafka-javascript`'s `.KafkaJS`. The two differ
  in CONFIG, not in method names, and `src/broker/kafka/shape.js`
  normalizes every difference the phase-0 spike found: the `kafkaJS` config
  nesting, `fromBeginning`/`autoCommit` placement, the `consumer.events`
  getter that THROWS on one of them, `fetchTopicMetadata` answering an
  array vs `{ topics }`, and the join signal (a GROUP_JOIN event vs polling
  `assignment()`).
- `log`: a topic per feed, single-partition by default (order), and the
  resume token is a VECTOR of partition offsets. Readers pin their position
  by seeking to a watermark captured before the join, because a fresh group
  resolves `latest` at its first fetch — a race that would otherwise drop
  the first entries.
- `queue`: one consumer group, manual commits, `partitionsConsumedConcurrently`
  as the prefetch. Kafka has no nack, so a retry is a republish carrying
  `x-wrpc-attempt` (the delay waits in-process, the original stays
  uncommitted meanwhile) and an exhausted message goes to a dead-letter
  topic.
- `backplane`: one topic, the channel in a header, a unique consumer group
  per instance, publishes chained so a per-channel sequence cannot reorder.
  Caveated in the guide: an instance is deaf until its group joins.
- **No `direct`** — and it says so: RPC over a broker refuses Kafka rather
  than limping.
- Suites run over an in-repo fake Kafka (both client shapes) in `pnpm test`
  and against a real broker in CI's new `kafka` job.

**Message brokers, part 7: the RabbitMQ adapter (`@alexify/wrpc/broker/amqp`)**
- `createAmqpBroker({ connection })` over an injected amqplib connection: a
  direct exchange with one exclusive queue per instance for the backplane,
  stream queues (`x-stream-offset` as the feed's resume token) for logs,
  quorum queues with a TTL retry queue and a dead-letter queue for work, and
  a fanout exchange per address for RPC (plain listeners each bind their own
  exclusive queue, a group binds one durable queue).
- Two RabbitMQ 4 behaviours the phase-0 spike measured are built in: a
  requeue does NOT count a delivery, so `retry()` republishes with an
  `x-wrpc-attempt` header while `release()` is the plain requeue that must
  not count one; and a transient non-exclusive queue is refused at the
  CONNECTION level, so every shared queue the adapter declares is durable.
- `mandatory` + `basic.return` turns "nobody is listening" into the fast
  `503` an RPC caller wants, and a request's timeout rides as the message's
  `expiration` so a stale one is dropped rather than executed late.
- Suites run over an in-repo fake RabbitMQ in `pnpm test` and against a real
  server in CI's new `rabbitmq` job.

**Message brokers, part 6: the NATS adapter (`@alexify/wrpc/broker/nats`)**
- `createNatsBroker({ nc, headers, jetstream, jetstreamManager })` over an
  injected nats.js connection: core subjects carry the backplane and RPC
  (queue groups for a service address, native reply subjects for inboxes),
  JetStream carries feeds (a stream per topic, the message sequence as the
  resume token) and work queues (a durable pull consumer per group,
  `max_ack_pending` as the prefetch, `working()` keeping a slow handler's
  lease and releasing it the moment the consumer stops).
- Without the JetStream factories the broker has backplane and direct only —
  the capability checks do the rest.
- Every application name becomes exactly ONE subject token, so a room called
  `room:*` can never become a wildcard subscription; stream names are encoded
  the same way.
- Suites run over an in-repo fake NATS + JetStream in `pnpm test` and against
  a real server in CI's new `nats` job.

**Message brokers, part 5: the Redis adapter (`@alexify/wrpc/broker/redis`)**
- `createRedisBroker({ client })` implements all four capabilities over an
  injected ioredis-shaped client — pub/sub for the backplane (the adapter
  `./scaling` already shipped), Streams for logs and queues (`XREADGROUP`,
  `XACK`+`XDEL`, `XAUTOCLAIM` for what a stopped consumer held, a sorted set
  for delayed retries), pub/sub plus a list for RPC inboxes and service
  groups. Valkey, KeyDB and Dragonfly need no adapter of their own.
- One blocking `XREAD` per instance serves every live feed reader
  (`TopicTails`), and a stream id is the feed's resume token. The adapter
  opens the extra connections blocking reads need through `duplicate()` (or
  an injected `connect`) and quits only those — never the injected client.
- The four contract suites run over an in-repo fake Redis in `pnpm test` and
  against a real server in CI's `redis` job.

**Message brokers, part 4: RPC over a broker (`attachBrokerRpc`, `transport: 'broker'`)**
- `attachBrokerRpc(server, broker, { service })` serves wrpc over a broker's
  `direct` capability, and `connect('broker://<service>', { transport:
  'broker', broker, mode })` calls it — no HTTP, no WebSocket, no service
  discovery between services that share a broker.
- `mode: 'stateless'` (default): a request is handled exactly like a
  packet-mode HTTP POST (`handleHttpCall`: batches, header sessions, meta),
  by whichever instance the broker's group hands it to.
- `mode: 'session'`: the full protocol (events, subscriptions, cancellation,
  binary streams) with one instance, found by a `hello`/`welcome`
  handshake. Frames are numbered per direction and a gap is a lost
  connection; `bye` both ways; a server-side `idleTimeout` (90 s) reclaims
  sessions whose client vanished; backpressure from unconfirmed sends.
  Losing the instance is an ordinary reconnect, and subscriptions resume.
- Draining stops consuming the service address while held sessions finish.
- The carrier is specified as the experimental
  [Broker binding](./docs/reference/protocol.md#broker-binding) section of
  the protocol reference.

**Message brokers, part 3: queue consumers and publishing (`attachConsumers`, `createPublisher`, `consumes`)**
- A unit's reserved `consumes` block declares queue consumers — full
  procedures with an optional `consume` policy — that no call packet can
  reach and introspection does not list. `attachConsumers(server, broker,
  table)` binds them (and, through the table, overrides their policy or binds
  ORDINARY procedures to queues) and delivers each message through the same
  pipeline a call takes: hooks, validators, access, queue/timeout, telemetry.
- Settlement follows one table for every broker: ack on success; retry with
  full-jitter backoff on `408/429/500/503` up to `attempts`; dead letter (with
  `x-wrpc-dead-reason`/`x-wrpc-attempt`, after `onDeadLetter`) on anything
  else or when exhausted; release on `503` while draining. `ctx.callMeta`
  carries `messageId`, `attempt`, `queue` and allowlisted headers.
- Identity per binding: `none` (default; a session procedure refuses to
  bind), `service` (a pseudo-session) or `token` (a bearer header restores a
  real session, one LRU-cached client per token).
- The server's `'draining'` pauses every binding (held messages finish and
  ack); `close()` now emits `'close'` first and stops them, releasing
  whatever a close cut off. `RpcServer#limits` exposes the per-connection
  caps; `attach(transport, { persistent: false })` attaches a
  request/response carrier that is not counted among connected clients.
- `createPublisher(server, broker, table)` publishes a unit's declared
  `emits` by name to a log or a queue, with an optional validator and key;
  each publish is a `PRODUCER` span whose context rides in the headers, so a
  trace runs from the call that placed an order to the consumer that charged
  it.
- The queue contract gains `pause()`/`resume()`: no new deliveries, held
  ones stay settleable.

**Message brokers, part 2: durable subscription feeds (`brokerFeed`)**
- `procedure.subscription({ handler: brokerFeed(broker, topic, options) })`
  reads a broker log and `tracked()`s every value with the log's resume
  token, so a client re-subscribing with `lastEventId` resumes on ANY
  instance — the "broker-backed feed" recipe of the rooms guide, made
  first-class. The topic may be a function of the context and arguments;
  `map` reshapes or skips entries; an undecodable entry is logged
  (`feed.decode`) and skipped.
- A `lastEventId` the log cannot resume from ends the feed with `400`
  (malformed, forged, past the tip) or `410` (history gone, another log) —
  or, with `onGap`, becomes a snapshot followed by everything appended from
  the moment of the gap (the read is positioned before the snapshot is
  built). A reader the retention overtook mid-stream takes the same path.
- `secret` HMAC-signs the ids a feed hands out and refuses any it never
  issued; `maxIdLength` caps what a peer may send.

**Message brokers, part 1: the broker-agnostic core (`@alexify/wrpc/broker`, experimental)**
- A broker is described by four capabilities — `backplane` (the existing
  at-most-once fan-out), `log` (ordered, replayable), `queue` (at-least-once,
  competing consumers) and `direct` (addressable inboxes) — and every adapter
  implements the subset natural to it. `isBroker`/`isBrokerLog`/
  `isBrokerQueue`/`isBrokerDirect` are the structural checks.
- `MemoryBroker` implements all four in one process: the reference the
  contracts are written against, and what makes a multi-instance setup
  testable without infrastructure. The contracts are executable
  (`tests/broker/*Contract.js`) and run over the memory broker, the scaling
  backplanes (a first shared backplane contract: `MemoryBackplane` and the
  Redis adapter over its fake — which surfaced that the fake delivered
  synchronously inside `publish()`, as no real Redis does) and, from the
  adapter releases on, real servers.
- Adapter building blocks: `TopicTails` (one live reader per topic shared by
  every local read, catch-up joined without a gap or a duplicate, bounded
  memory per slow reader) and `encodeToken` (arbitrary names into a broker's
  alphabet, injectively — a room called `room:*` must never become a NATS
  wildcard subscription).
- Core seams, additive: `RpcServer.attach(transport, { session })` gives an
  attached client a vouched-for identity before the onConnect hooks run, and
  `{ request: { headers, url } }` restores a real one through the token
  carrier; `drain()` announces itself once as a `'draining'` event; a
  host-built client may set `spanKind`/`spanAttributes` for its call spans,
  and the server telemetry writer gains `withMessagingSpan` and the
  `wrpc.broker.deliveries`/`wrpc.broker.published` counters. The webrtc
  browser budget moves 46 → 47 KB for those telemetry bytes (PeerHost
  bundles the server writer).
- `compose.yaml` starts RabbitMQ 4, Kafka 3.9 (KRaft) and NATS 2 with
  JetStream next to Redis.


**Injectable `fetch` for the http/sse client transports (`options.fetch`)**
- `WrpcClient.connect(url, { transport: 'http' | 'sse', fetch })` lets the
  transport call an injected `fetch` instead of the runtime's global one,
  re-resolved on every open like `headers`/`meta`. The seam is for a Node
  process that talks wrpc to another wrpc server and wants undici's
  connection pooling, proxying, or a caching interceptor tuned for that
  traffic — without wrpc ever depending on undici (it stays uninstalled;
  the caller supplies the function). Not a way to reach arbitrary
  third-party REST APIs through wrpc: the http/sse transports still only
  call the one connected wrpc server.

**Backplane loss detection (`epoch`/`seq`, `backplane.gap`, `wrpc.server.backplane.gaps`)**
- Every backplane envelope carries the publisher's boot `epoch` and a
  per-channel `seq`; a receiver that sees the sequence jump within one
  epoch logs `backplane.gap` — `{ channel, instance, missed }` — and adds
  `missed` to the new `wrpc.server.backplane.gaps` counter. Redis pub/sub
  loses envelopes silently (a subscriber blip, an output-buffer eviction);
  the loss is now a number on a dashboard. Additive fields: an older
  instance's envelopes are delivered untracked; a new epoch resets rather
  than reports. At-most-once stays the contract — the rooms guide gains
  the three recipes for the cases that need more (a broker-backed
  subscription, a room-backed event log, an acknowledged `ask`).

**A multi-node bench and the Redis it needs (`bench/cluster-nodes.js`, `compose.yaml`)**
- `pnpm redis:up` starts a Redis (`compose.yaml`); with `REDIS_URL` set,
  `bench/cluster-nodes.js` forks N wrpc processes over that Redis (backplane
  + session store, bearer sessions) with M clients spread across them and
  measures cross-instance emit rate and latency, presence convergence after
  a join/leave storm, broadcast ask across instances, and an instance loss —
  its clients rehomed with their session token and no sticky routing, the
  presence count converging, the loss detector's count. 4 instances × 200
  clients: emit latency p50 2 ms / p99 4 ms across the broker, a 200-join
  storm converging in 6 ms, ask answered 200/200, 50 rehomed clients with
  50/50 sessions restored and 0 gaps. Without `REDIS_URL` it skips, so
  `pnpm bench` stays self-contained. CI gains a `redis` job
  (a Redis service) for `tests/scaling/redis.integration.test.js`.

**REST response headers and a cache policy (`http.headers`, `http.cache`, `context.http`)**
- The paper's "loss of intermediate HTTP caching" was a missing seam, not a
  property of the model: a mapped route can now declare static response
  `headers`, a handler (or hook) can shape the response through
  `context.http` — `{ method, url, headers, setHeader(name, value),
  status(code) }`, null on every non-REST transport and on packet-mode
  HTTP — and `http.cache: { maxAge, public?, staleWhileRevalidate?, etag? }`
  answers `Cache-Control`, a weak `ETag` and `304` on `If-None-Match`, on
  GET and HEAD (HEAD is now served by the GET route, RFC 9110 9.3.2). The
  policy is decided once the session is known and is the same function on
  every host (`cacheHeadersFor`): only a public procedure on a request that
  restored no session and set no cookie gets the declared policy; anything
  session-bearing answers `private, no-store` and no ETag. Transport-owned
  header names are refused. The fastify adapter applies both seams onto its
  `reply` and leaves ETag/304 to `@fastify/etag`. Introspection carries
  `headers`/`cache` only when declared.

**`createRedisSessionStore` (`@alexify/wrpc/scaling`)**
- An ioredis-shaped, injected session store — `get`, `set(key, value,
  'PX', ttl)`, `del`, `pexpire` for the sliding expiry — so a client that
  reconnects to another instance keeps its session and WebSocket traffic
  needs no sticky routing. The scaling guide gains an affinity table: with a
  shared store and a backplane only SSE channels stay pinned.

**Context takeover and async deflate (`perMessageDeflate.contextTakeover`, `.async`)**
- `contextTakeover: 'server' | 'client' | true` keeps a live zlib stream per
  direction per connection (`src/websocket/deflateContext.js`), so a message
  may reference the ones before it — the ratio the stateless default gives
  up, at ~160 KiB per direction per connection at the defaults (`level`,
  `memLevel` tune it). A peer's own `*_no_context_takeover` request is
  always honoured. `async: { threshold }` (256 KiB by default) runs the
  deflate and inflate of larger messages on zlib's threadpool instead of
  the event loop. Both go through per-connection ordering queues: writes
  issued behind an in-flight deflate wait for it, inbound messages are
  delivered in arrival order, `bufferedAmount` counts the queued bytes so
  the `send()` boolean and `'drain'` stay honest, and a fan-out over the
  async threshold still deflates once (the first recipient starts it, the
  rest wait on the same frame). `bench/deflate-context.js`: a repeated JSON
  event compresses 10.6× with a context against 1.1× without; a burst of
  100 × 252 KB frames stalls the loop for 328 ms synchronously and 2 ms
  async, at four times the throughput. Takeover members of a room compress per
  connection, the stateless default keeps the shared frame. Autobahn passes
  in both modes (`AUTOBAHN_DEFLATE=takeover node scripts/autobahn/run.js`).

**The call path, measured and trimmed (phase 1b of the paper's findings)**
- `bench/support/rpc-stacks.js` gains two rows: the same RPC path over the
  uws engine, and the own engine with `batch: true`. They answer the
  question the old table could not: with 64 calls in flight wrpc runs at
  95.8K over its own engine and 112K over uws against 129K for a raw uws
  echo — the JavaScript engine is ~17 % of the gap, the RPC layer and the
  client are the rest; client batching is worth as much as the engine swap.
- `pnpm bench:browser` (`scripts/bench-browser.js` + `bench/browser/calls.js`,
  `playwright-core` as a devDependency driving the installed Chrome) measures
  the client's call path where it ships, over an in-page echo on a
  `MessageChannel`. Chrome 152: 64 calls in flight 101,666 → 114,158 ops/sec,
  1024 in flight 90,665 → 104,864 (the table is in `docs/guide/performance.md`).
- Client call deadlines are one bucketed timer per client instead of a
  `setTimeout` plus three closures per call: one lane per distinct timeout,
  buckets of 1/32 of it (a call times out between its deadline and 3 % after,
  never before), settled calls leave their bucket at once so an idle client
  arms nothing. `bench/bench.js` call + server event round-trip 14.7K → 17.0K.
- `Client.isReady` (server side): true once `ready` resolved, so a call
  after the first skips the `await` and its microtask; `Context.uuid` is
  minted on first read; `Procedure.invoke` calls the handler directly
  instead of through `Promise.resolve().then()`; the dispatcher skips the
  span wrapper and its options object when telemetry is off, as the client
  already did; logger writers carry `debugEnabled` so the per-call debug
  entry is not built for a writer that drops it. The client settles a
  `callback` before the batch and heartbeat checks, without the `#dispatch`
  hop.
- Browser budgets raised with the measurement that earned them
  (`scripts/size.js`): main 19 → 20 KB, sse 20 → 21 KB, webrtc 45 → 46 KB.

**Fan-out frames encoded once (`Connection.sendPrepared`, capability `prepared`)**
- A room broadcast now hands every recipient ONE shared
  `{ text, frames, compress }` message (`Client.sendShared`,
  `ServerWsTransport.writeShared`): the built-in engine encodes the frame —
  and, with permessage-deflate, deflates it once per negotiated window — into
  the message's cache slot on first use and writes that buffer to every later
  recipient. `bench/send-path.js`: room fan-out ×50 went 102,997 → 637,248/sec,
  ×200 28,651 → 321,820/sec, and ×50 with deflate 2,708 → 101,609/sec (the
  36× compression cliff was N deflates per emit; it is now one per window).
  `WrpcSocket.sendPrepared?()` is an optional, feature-detected addition to
  the engine contract (`EngineCapabilities.prepared`; the contract suite
  exercises it when present), so third-party engines need no change.
- **Selective compression.** `perMessageDeflate: { filter: (req) => boolean }`
  decides per connection whether a peer's offer is accepted — compression for
  bandwidth-bound browsers, none for in-datacenter peers — and
  `{ compress: false }` on `server.to(room).emit()`, `client.sendEvent()`,
  `client.sendRaw()`, `client.send()` and `Connection.send()` sends one
  message uncompressed past the threshold. `WrpcSocket.send()` gains the
  optional second argument; sockets that ignore it compress as before.
- `Connection` option `coalesce` (default `true` for server connections, off
  for a bare `Connection`): the first write of an event-loop turn corks the
  socket and the next tick uncorks it, so a batch of N answers or a burst of
  events leaves in one `writev` instead of N syscalls. The `send()` boolean
  and `'drain'` are unchanged.

**WebTransport, experimental (`transport: 'wt'`, `@alexify/wrpc/wt`)**
- `connect(url, { transport: ['wt', 'ws'], wt: { serverCertificateHashes } })`
  runs the ordinary client over a WebTransport session (HTTP/3): every
  packet and stream chunk travels on ONE client-opened bidirectional
  stream — the control stream — under a five-byte length/kind header
  (`docs/reference/protocol.md#webtransport`), so calls, events,
  subscriptions with resume, binary streams with backpressure, heartbeat and
  reconnect all work unchanged and the WebSocket fallback is invisible. The
  transport is in the base client entry (no import for the fallback list;
  the browser budget went 16 → 19 KB for it, datagrams and the stream mux included); the session comes from
  `globalThis.WebTransport` or an injected `wt.WebTransport`. Declared
  headers and meta ride the connect URL as on ws; a `CONNECT` sends no
  cookies, so sessions need `bearerTransport()`/`payloadTransport()`.
- `@alexify/wrpc/wt` is the server half, and binds to no implementation —
  Node has no WebTransport of its own (`node:quic` is behind a compile-time
  flag and speaks no WebTransport): `attachSession(server, session, meta)`
  waits for the control stream, wraps the W3C-shaped session as a
  `WrpcSocket` (`WtSocket`) and attaches it through `RpcServer.attachSocket`,
  so a WebTransport client lands in the same `Server`, rooms, cluster and
  session store as a WebSocket one; `acceptSessions(server, sessions)` loops
  a host's `sessionStream()` (or any iterable); `fromFails(session)` and
  `fromQuico(req, res)` read the `CONNECT` request off
  `@fails-components/webtransport` and `quico` respectively (both
  devDependencies, exercised by `WRPC_WT=`-guarded integration tests),
  `failsRequestCallback` is the request callback a fails server needs, and
  `isWtSession` is the structural check. `idleTimeout` terminates a session
  that sends nothing — the liveness a host without its own session-end
  reporting (quico) lacks. Close codes carry over: 1001 on
  shutdown, 1002 on a framing violation, 403/408 on a refused session.
- **Binary streams on their own WebTransport streams.** Once both ends have
  announced it (each end's first message on the control stream is a KIND 2
  capabilities message), every `createStream()` on either side gets a
  unidirectional WebTransport stream: its chunks travel there, its `end()`
  is that stream's FIN and its `terminate()` a reset, while calls, events
  and subscriptions keep flowing on the control stream beside a large
  transfer instead of behind it. The receiver holds early chunks until the
  opening `stream` packet has passed and synthesizes the end only after the
  stream's own FIN, so ordering is what a WebSocket gave; a peer that
  announces nothing (an older peer, a wire codec) gets every chunk on the
  control stream as before. `ServerWtTransport` (registered as
  `ServerTransport.transport.wt`, which `attachSocket` now consults for
  `meta.kind`) is what lets the server side see a stream packet before it
  is serialized.
- **Unreliable events.** `sendEvent(name, data, { unreliable: true })` on
  the client, `client.sendEvent(...)` / `server.to(room).emit(...)` with the
  same option on the server, send an event as ONE WebTransport datagram —
  at most once, unordered, for state a later event supersedes (a cursor, a
  position) — and reliably wherever the transport has no datagrams or the
  packet does not fit in one, so application code is written once. A
  `Broadcast` picks per recipient and carries the flag across the
  backplane; `ask()` refuses it. The wire (`[KIND][packet]`) is in
  `docs/reference/protocol.md#webtransport-datagrams`; `ClientTransport`
  and `ServerTransport` gain an optional `writeUnreliable(text)` seam.
- `node:quic` was evaluated as a host on a custom Node build and does not
  qualify yet (it cannot send the WebTransport SETTINGS a client waits
  for); the findings are in the guide.
- The whole feature is marked `@experimental` and may change in a minor.

**Transport fallback on the first connect**
- `connect()` with a transport list now walks it on the FIRST connect too: a
  candidate whose `open()` rejects outright (no `WebTransport` in this
  runtime, a refused upgrade) hands over to the next one at once, with no
  reconnect budget to burn first; only the last candidate's rejection is
  `connect()`'s. Reconnect exhaustion falls through the list as before.
- `RpcServer.attachSocket(socket, meta)` takes `meta.kind` — what
  `Client.transportKind` and the log lines report for a socket that is not a
  WebSocket (`'wt'`).

**Shared Workers behind the `event` transport**
- `connect(url, { worker })` takes a `SharedWorker` (reached through its
  `port`), a dedicated `Worker` or a raw `MessagePort` as well as a
  `ServiceWorker` — the page side posts to `worker.port ?? worker`, and every
  client still gets its own `MessageChannel`.
- `WrpcClientProxy` also listens on the SharedWorker `connect` event and
  treats each page's port as the control bus `self` is in a Service Worker;
  both listeners are registered, no context sniffing.
- `WrpcClientProxy` takes a `url` option — the server the worker connects to,
  defaulting to the one derived from `self.location` as before.
- The proxy releases a port, and the answers parked on it, when the page
  closes it (the `MessagePort` `close` event; best effort in older engines).

**WebRTC: peer-to-peer wrpc (`@alexify/wrpc/webrtc`)**
- `WrpcPeer` — a router others call, a signaler to find them through, an
  RTC adapter to reach them with. Two browsers (or a browser and a Node
  process with an injected implementation) each serve a router and call the
  other's over ONE `RTCPeerConnection` carrying two negotiated data
  channels, one per client→host direction, so the ordinary `WrpcClient` and
  dispatcher speak across the link unchanged: calls, events, ask/respond,
  subscriptions with `lastEventId` resume, binary streams, heartbeat and
  reconnect. `PeerLink` is one peer, both directions (`remote`, `api`,
  `client`, `send`/`ask`/`createStream`); roles are by id order alone and
  `connect()` works from either side (the non-initiator knocks).
- `Mesh` (`peer.join(room)`) — everyone in a signaling room linked to
  everyone, with `broadcast()`/`ask()` as one `Broadcast` fan-out over the
  host room `mesh:<room>`, `respond()` covering members present and
  future, and a rebuild on a signaling reset. A member whose signaling
  connection dropped is only `away`: its link stays up until it leaves for
  real, comes back as another incarnation, or the link itself fails.
- Signaling: the structural `Signaler`/`RosterSignaler` contract
  (`isSignaler`, `hasRoster`), the built-in server unit
  `createSignalingUnit()` + `createSignalingHooks()` (relayed through
  `RpcServer.sendTo`, so it clusters with no extra state) and its client
  half `wrpcSignaler(client)` over any transport.
- Stable peer identity: `createSignalingUnit({ identity })` decides a
  connection's peer id — a user id from the session, say — from the context
  and the id the client proposed (`wrpcSignaler(client, { identity })`); the
  default stays the connection's client id. The id survives a signaling
  reconnect, so a `WrpcPeer`'s links do too (`reset` with `id === previous`
  keeps them). Each signaler carries one `instance` (`generateId` option),
  which tells two incarnations of one id apart: a signal from another
  incarnation abandons the stale link and relinks. `duplicate: 'replace' |
  'refuse'` decides a second connection under a held id; the first hears
  `replaced` (the peer emits it and closes). Rosters, `join`/`leave` and
  signals carry `instance` and the routable `address`, and `leave` a
  `reason` (`'left' | 'disconnect' | 'replaced'`).
- Trust assertions: `createSignalingUnit({ assertions: { key, ttl, issuer,
  claims } })` signs a JWS (compact, ES256, `typ: 'wrpc-rtc+jwt'`) per peer
  binding its id to the DTLS certificate fingerprint it dials with, and
  publishes its keys (`assert({ fingerprint })`, public `keys()`; the
  client half's `signaler.assert()`/`keys()`, `hasAssertions`). `WrpcPeer({
  assertions })` gets a token for every description it sends and verifies
  every one it receives — `sub` is the sender, `fp` is the description's
  fingerprint, `exp`/`iss` hold, the signature checks against the server's
  public key (rotation by `kid`) — before the link applies it and before
  `accept(from, room, { instance, claims })` runs; a redial's new
  certificate is verified anew, an ICE restart is a string compare. The
  verified claims are `link.claims`, and `PeerHost({ trust: 'assertion' })`
  requires them and exposes them as `context.session.data.claims`. Exported
  for both sides: `createAssertionVerifier`, `sdpFingerprint`,
  `normalizeFingerprint`, `isAssertion`, `AssertionError` (browser and
  Node), `createAssertionIssuer`, `generateAssertionKeys` (Node). The
  format is specified in the protocol reference.
- Bring your own data channel — the level under `RtcLink`, the `event`
  transport's arrangement for WebRTC: `connect(url, { transport: 'webrtc',
  channel })` speaks on an `RTCDataChannel` the application negotiated
  itself (a factory instead of a channel plugs the application's own
  recovery into the client's reconnect cycle), `RtcPeerTransport` takes a
  raw channel for a `PeerHost`, and `attachChannel(rpc, dc, { peer,
  headers, data, maxMessageSize })` from the Node barrel is the
  `attachPort` of WebRTC — an ordinary server, sessions and cluster
  included, reachable peer to peer. Under it, `RpcServer.attach(transport)`
  accepts any persistent transport that announces inbound traffic as
  `'packet'`/`'chunk'` events; the core stays free of any framing.
- The lower layers, all exported: the W3C-shaped `RtcAdapter` port
  (`createW3cAdapter`, structural checks — wrpc binds to no Node WebRTC
  package), `RtcLink` (perfect negotiation, trickle ICE, ICE restart,
  redial, configurable negotiated channel ids), the one-byte data-channel
  framing (`FrameEncoder`/`FrameDecoder`, fragmentation to the negotiated
  message size, documented in the protocol reference), `ClientRtcTransport`
  (registered as `WrpcClient.transport.webrtc`), `RtcPeerTransport` and the
  browser-safe `PeerHost` (`trust: 'link'` pseudo-sessions so
  `access: 'session'` procedures run on a peer).
- Peer telemetry: `WrpcPeer({ telemetry })` / `PeerHost({ telemetry })` take
  the server's injection — SERVER spans for what a peer answers, joined to
  the calling peer's CLIENT spans through the packet's traceparent,
  `wrpc.server.connections` under `wrpc.transport: 'webrtc'` — plus three
  instruments of the peer layer's own: `wrpc.rtc.links`, `wrpc.rtc.redials`,
  `wrpc.rtc.ice_restarts`. `RtcLink` emits `'restart'` with the outcome.
- The webrtc browser entry also exports `defineRouter`, `procedure`,
  `tracked` and `createEventLog` — a browser peer defines its router with
  them, and the main browser entry leaves them out for its byte budget.
- Cluster: `cluster.send(clientId, name, data, { room })` and
  `RpcServer.sendTo()` / `Server.sendTo()` — one event to one client by id,
  on this instance or through the cluster's addressed command, optionally
  bounded by a room membership.
- Types: `rpc.d.ts`, the node-free server-core types (routers, sessions,
  rooms, `Client`, `Context`) shared by the Node surface and the browser
  peer types, and `ClientHost` — the host contract `context.server` is now
  typed as (see Changed).

### Changed
- `ClientEventTransport.getInstance` is deprecated. It still returns the
  class-level singleton it always did, but `connect({ worker })` no longer
  uses it — construct one with `new WrpcClient.transport.event(url)`.
  `WrpcClient.transport.event` is now typed as that constructor.
- `Context.server` and `Client.server` are typed as `ClientHost | null`
  instead of `RpcServer | null`: the contract both an `RpcServer` and a
  WebRTC `PeerHost` satisfy (`router`, `rooms`, `getClient`, `to`, `except`,
  `broadcast`). Narrow with `instanceof RpcServer` to reach sessions, the
  cluster or `sendTo`. Runtime behaviour is unchanged.
- `ServerEventTransport` (the `attachPort` transport) now exposes
  `connection`, so a MessagePort client is `persistent`: events,
  subscriptions and streams work over it as over a socket.
- **Deprecated behaviour.** `generateId` on `RpcServer`/`Server`, on
  `WrpcClient` and on `PeerHost` used to ignore a bad value silently. It is
  now reported through the logger as `event: 'options.generateId'` and
  replaced with the default. **2.0 will make it a `TypeError`**, as it
  already is on the options added since — `SseChannels` and the broker
  adapters — which have no compatibility to keep.

### Fixed
- **Two public telemetry types rejected the things they exist to accept.**
  A real `@opentelemetry/api` `Tracer` was not assignable to `WrpcTracer`,
  because `WrpcSpan` declared `addEvent`, `setStatus` and `recordException`
  more narrowly than the article they describe — so the structural view that
  exists precisely to let an SDK in kept it out. And `startActiveSpan` was
  declared with only its 3-argument form while `startSpanWith` calls the
  4-argument one whenever the arity allows, so a hand-written parented
  tracer type-checked and then broke at runtime. Both are typed from what
  wrpc actually calls now, with tsd assertions against the real SDK.
- `connect(url, { worker })` builds its own `ClientEventTransport` per
  client instead of sharing a class-level singleton. A second `connect` to a
  DIFFERENT worker on the same page used to reuse the first `MessageChannel`
  and never reach its worker, and closing one worker client closed the port
  of every other one; now each client has its own channel and lifecycle.
- `ClientEventTransport.close()` is idempotent: `terminate()` after `close()`,
  or the cleanup after an `open()` that threw before a port existed, no
  longer throws a `TypeError` in place of the original error.
- `attachPort` routed a `Buffer` chunk to the text handler; binary chunks
  now reach `handleBinary` whatever the view type.
- `WrpcClient.write()` returns the transport's backpressure signal and the
  client re-announces the transport's `'drain'`, so a `WrpcWritable` on the
  client side actually waits for the wire.
- **`SessionManager#destroy` could take the process down.**
  `initializeSession` calls `finalizeSession()` through `void`, so a
  `store.delete` that rejected became an unhandled rejection: a Redis blip
  ended the server rather than one session. It is guarded and logged now.
- The Redis backplane wrote its entries as `{ err, component }` with **no
  `event` field** — the one writer in the package breaking the convention
  its own guide documents, so those lines could not be alerted on alongside
  the rest. A test now asserts the rule on every entry a server writes.

## [1.0.0] - 2026-08-23

### Added

**2026-08 re-review batch (resilience, security, scale, types)**
- Client resilience: `connectTimeout` (default 30 s — a handshake that never
  answers no longer parks the reconnect ladder), `reconnect.stableAfter`
  (the attempt counter resets only after the connection SURVIVES the window,
  so an accept-then-drop peer climbs the backoff instead of pinning at
  `minDelay`), coded rejections everywhere (`callTimeout` → 408 `WrpcError`,
  dead-transport sends and batched flush failures → 503), and the SSE POST
  settling the exact calls it carried.
- Refresh hardening: a call made BY the refresh handler surfaces its refusal
  instead of deadlocking the single-flight run; a refused **re-subscribe**
  now runs the same refresh and re-opens once (feeds heal like calls after a
  long outage); a THROWING refresh clears the `bearerAuth` store; failures
  log `refresh.failed`, emit `'refresh-failed'` and count on
  `wrpc.client.refreshes`.
- Auth carriers: the `TokenTransport` port now receives the core's parsed
  `declared`/`meta` bags (strategies can no longer drift from the wire
  parser — `payloadTransport` reads both `x-wrpc-meta` spellings on every
  carrier); on browser ws a Bearer credential rides a `wrpc.bearer.<token>`
  subprotocol offer instead of the connect URL; `cookieStorage` stamps
  `Secure` by default; the ws connect-URL query is client-capped with a
  `meta.oversize` warning.
- Per-call deadlines and retry: `CallOptions.timeout` (rides the packet; the
  server SHORTENS the procedure budget to match) and the opt-in client
  `retry` policy (`{ attempts, on: [503], … }`, jittered backoff, fresh
  packet id per attempt, never an offline buffer).
- Typed events: reserved contract keys `events` (server → client; narrows
  the unit emitter and types `client.respond`) and `sends` (client → server;
  types `client.sendEvent`) — declarations only. The router's inbound `on`
  handlers and the new declaration-only `emits` key now travel through
  introspection, and `wrpc types` generates both blocks.
- `wrpc types --openapi <path>`: an OpenAPI 3 document projected from every
  procedure with an `http` mapping (path/query parameters and request body
  from the fastify-shaped schema parts, wire error as the default response).
- Query bindings: `infiniteQueryOptions(path, args, { cursorKey, ... })` —
  the tRPC-v11 paging factory, cursor merged over args, lazy resolution and
  AbortSignal forwarding as ever.
- Cluster at scale: presence's periodic corrective message is now a
  **digest** (hash) with an addressed `sync`/`state` exchange only on
  drift; `cluster.rooms` replication filter; `cluster.maxFetch` (loud
  truncation, `clients.truncated`); opt-in `cluster.secret` HMAC envelope
  authentication; honest `cluster: false`; `healthy` getters with
  `'degraded'`/`'recovered'` events and backplane subscribe RETRY (rooms
  and cluster channels); `rooms: { linger }` grace window on emptied room
  channels; the SSE per-address cap gained an injected `clientAddress`
  seam (and the express adapter reports `req.ip`).
- Observability of the newest subsystems: the mapped REST leg traces on
  both ends (real `traceparent` headers ↔ the synthetic packet), delegated
  fastify routes emit the same spans/metrics as the packet path
  (`RpcServer#otel`, `@experimental`), cluster envelopes carry trace
  context and three `wrpc.cluster.*` instruments, early HTTP/SSE refusals
  log and count (`http.refused`/`sse.refused`/`cors.refused` on
  `wrpc.server.calls` under `<unknown>`), SSE gaps/expiries are logged and
  counted (`wrpc.server.sse.events`), reconnects count every attempt.
- The HTTP side's version marker: every response echoes `wrpc-version: 1`
  (requests may send one; revision 1 accepts and ignores it) — the ws
  subprotocol ladder's counterpart, reserved inside the freeze.
- Guard tests for every hand-synced pair (VitePress keywords/nav label,
  `RPC_OPTION_KEYS`, `scripts/size.js` ENTRIES, runtime-exports ⊆ d.ts,
  d.ts cross-references), a structural client-transport contract
  (`isClientTransport` + `tests/client/transportContract.js`), and docs:
  a dedicated [Authentication](https://wrpc.vercel.app/guide/auth) page,
  [Stability & deprecation](https://wrpc.vercel.app/reference/stability)
  reference, refreshed homepage grid/README/why.md (REST finally on the
  front door; honest tRPC bench footnote — the sequential number is a
  client flush-timer artifact).

### Changed
- **Breaking (nothing released yet):** `Procedure#invoke` takes an optional
  5th `budget` argument; the introspection unit object carries reserved
  `on`/`emits` keys (clients skip them; older generated artifacts are
  unaffected); `fetchClients` remote replies changed shape internally
  (`{ list, truncated }`); presence's periodic full `state` broadcast was
  replaced by the digest flow; `bearerTransport` no longer parses the raw
  `wrpc_h` URL itself (the core hands it the parsed bag);
  `maxChannelsPerAddress` keys on the injected `clientAddress`.
- Performance: `Allow-Headers` memoized per cors object
  (bench/cors-headers.js), `runValidator` synchronous fast path
  (bench/validate.js), ring-buffer replay logs (bench/replay-buffer.js),
  `sanitizeMeta` upper-bound walk and the `wrpc_meta` substring gate
  (bench/meta.js), batch flush skips the meta aggregate off-HTTP.
- Internal layout: `src/rpc/core.js` split (`client.js` — Context/Client;
  `meta.js` — the connection-metadata parser), the REST trie moved to
  `src/rpc/rest.js`, the wire names centralized in `src/wire.js`.


**Core RPC**
- `defineRouter`/`procedure`/`Router`/`Procedure`: units declared with versions
  as `'unit.vN'` keys, bare-function shorthand, per-procedure `access`
  (default `'session'`), `input`/`output` validators (plain functions or
  Standard Schema objects; failures map to 400/500), `timeout` (408), `queue`
  concurrency limits backed by a `Semaphore` (503), `meta`/`signature`
  descriptors, and lifecycle **hooks**: `onRequest`, `preValidation`,
  `preHandler`, `preSerialization`, `onSend`, `onResponse`, `onError`,
  `onTimeout`, `onSubscribe`, `onUnsubscribe`, plus router-level
  `onConnect`/`onDisconnect({ rooms })` (a pre-teardown snapshot of the
  client's rooms). Registered at three levels — `defineRouter({ hooks })`, a
  unit's reserved `hooks` key, `procedure({ preHandler })` — flattened once
  at router build, plus `router.addHook(name, fn)`. See the
  [Hooks guide](https://wrpc.vercel.app/guide/hooks).
- `system/introspect` is auto-registered from the router (`client.load()`
  needs no hand-rolled introspection); `introspection` option (`true`
  default / `'session'` / `false`).
- `context.method`/`context.procedure` set at every context creation site
  (calls, subscriptions, inbound events, fastify's delegated REST routes).
- `RpcServer`: engine-agnostic core, no `node:http` on the request path —
  `attachSocket(socket, meta)`, `handleHttpCall(call)` (abstract
  `{ method, url, headers, body, respond }`), `attachPort`. `Server` composes
  it with `node:http(s)` + a WebSocket engine.
- Sessions: `SessionManager` over a structural `SessionStore` (`{ get, set,
  delete }`, `MemorySessionStore` built in, anything store-shaped injects via
  `sessions.store`). Cookies restore automatically on HTTP calls and WS
  upgrades (`client.sessionReady`), default
  `HttpOnly; Secure; SameSite=Lax; Path=/`, survive disconnects.
  `MemorySessionStore` is bounded (LRU `maxSessions`, default 10000; `ttl`,
  default 24h) and gained `touch(token)` to slide the TTL on restore.
  CSRF: safe methods (`GET`/`HEAD`) dispatch without the cookie session
  unless Fetch metadata (`Sec-Fetch-Site`) proves same-origin intent.
- `basePath` (default `/api`): packet endpoint `POST <basePath>`, REST at
  `<basePath>/unit/method`, WS upgrades gated to `/` and basePath paths.
  REST version strategy: `defineRouter(units, { rest: { version: 'path' } })`
  maps a versioned unit under a `/vN` prefix (function form for full
  control); survives `merge()`.
- `cors: { origins, credentials, headers, methods }` — per-request origin
  echo, `Vary: Origin`, credentials only for allowed origins, origin gate on
  both HTTP calls and WS upgrades.
- `codec.rest`: an optional `rest` section on the wire codec
  (`{ encode, decode, contentType? }`) frames REST bodies (binary allowed) on
  both REST modes; rest-only codecs are valid. `RpcServer.codec` getter.
  Refused next to fastify's delegated REST routes.
- `maxCalls` (1000, 429 past cap), `maxBodySize` on the built-in `Server`,
  `logger`/`console` normalization (below), graceful shutdown:
  `server.close({ drain: ms })` stops intake, refuses new calls with 503,
  waits for in-flight calls, sends every peer a 1001 close frame; `drain()`
  and `draining` are public.

**Engine & transport**
- `@alexify/wrpc/engine` subpath: replaceable engine port (`WrpcSocket`,
  `Engine`, capability flags), `createNodeEngine()`. A **standalone** engine
  kind (`standalone: true`, uWebSockets.js) owns the whole network stack
  including `node:http`; `Server.address()` reports the bound address
  regardless of which side owns the listener (`Server.httpServer` is `null`
  under a standalone engine).
- WebSocket engine hardening: write backpressure (`send`/`sendText`/
  `sendBinary` return `false` above the high-water mark, `'drain'`,
  `bufferedAmount`, `maxBackpressure` terminates non-draining peers — control
  frames respect the cap too); end-to-end stream flow control (`Connection`
  gains `pause()`/`resume()`/`isPaused`, a slow stream consumer propagates
  backpressure through TCP); O(n) receive path (`SegmentQueue`, incremental
  header parsing, word-wise unmasking); subprotocol negotiation
  (`protocols`/`handleProtocols`, exposed as `connection.protocol`) — the
  client offers `wrpc.v1` by default and both engines echo it back;
  outgoing fragmentation (`fragmentThreshold`); permessage-deflate (RFC 7692,
  off by default, no-context-takeover, `maxOutputLength` capped by
  `maxBuffer`); inbound pings surfaced as a `'ping'` event; graceful
  `close({ code, reason })`; `wss.connections` snapshot. `maxPayload`
  (default 16 MiB) on the engine is a dedicated inflated-size cap for
  permessage-deflate, separate from `maxBuffer`.
- `WebsocketServer`'s `server` option is optional — an unbound server drives
  upgrades by hand through the new public `handleUpgrade(req, socket, head)`,
  which is what lets a middleware adapter perform the upgrade from a
  listener it does not own.
- `@alexify/wrpc/ws` subpath (`ws.js` + hand-maintained `ws.d.ts`), the
  engine's own entry point with full typings.

**Adapters** (`@alexify/wrpc/uws`, `/fastify`, `/express`) — each following
the root-shim + `.d.ts` + `exports` + `tests/*.test-d.ts` convention, and
each taking its host framework strictly by injection (devDependency used
only by adapter tests; never a runtime dependency).
- **uws engine adapter**: uws' tri-state `send()` collapses to a boolean —
  `SUCCESS`/`BACKPRESSURE` map to `true`/`false`, `DROPPED` fails loudly
  (`'error'` + terminate, since a silently dropped frame corrupts the
  protocol); every payload is copied at the callback boundary; a
  poisoned-handle guard latches `closed` once uws invalidates the handle;
  `listen()` tags a refused bind as `EADDRINUSE`. Capability flags reflect
  real behavior differences: `ping: false` (uws owns liveness via
  `idleTimeout`), `pause: false` (no receive-side flow control under uws).
- **fastify plugin** (`wrpcFastify`): two backends by feature detection — a
  plain `fastify()` gets the node engine on its `'upgrade'` event,
  `fastify({ serverFactory })` from `fastify-uws` gets a uws engine over the
  same uws app. HTTP calls run through real fastify routes, so hooks/auth
  run before wrpc sees the call; logging adapts to fastify's own logger.
- **express (and bare `node:http`) adapter**: `createWrpc({ ... })` returns
  `{ rpc, engine, wsServer, handler, upgrade, close }` and owns no listener
  — a request outside `basePath` passes to `next()` instead of a `404`, so
  wrpc composes with the rest of the app.
- Shared engine contract suite (`tests/engine/engineContract.js`) run against
  both hosted and standalone engines via a harness, plus a **swap test**
  running one behavioral spec against all five ways of standing wrpc up.

**Rooms & realtime** (`src/rpc/rooms.js`)
- `ctx.client.join(room)`/`.leave(room)`/`.rooms`/`.in(room)`;
  `server.to(room).emit(name, data)`, `.to(a, b)` (union), `.except()`,
  `.local()`, `server.broadcast()`. `emit()` returns how many clients
  received it on this instance. The chainable `Broadcast` is immutable —
  every modifier returns a new target. `to()` with no rooms reaches
  **nobody**, never everybody. `Context.server`/`Client.server` reach rooms
  from a handler.
- Client → server events: a unit's reserved **`on`** key declares inbound
  handlers, reachable with `client.sendEvent('unit/name', data)`; ordinary
  procedures (`access`/`input`/`timeout`/`queue` apply); `on` cannot be used
  as a method name and is not introspected.
- Encode-once fan-out: `Broadcast.emit` serializes the packet once and hands
  every recipient the same text through `Client.sendRaw`.
- `@alexify/wrpc/scaling` subpath: structural backplane contract
  (`publish`/`subscribe`/`close`), `MemoryBackplane`, and an ioredis-shaped
  `createRedisAdapter({ pub, sub, prefix })` (`sub` defaults to
  `pub.duplicate()`; the injected Redis clients are never `quit()`ed, only
  a self-opened `sub` is). `new Server({ backplane })` publishes every
  non-local emit as `{ v, instance, rooms, name, data }` with echo
  suppression by instance id; a room-targeted emit uses that room's
  channel, everything else the shared `broadcast` channel. Delivery is
  documented as **at-most-once**; backplane failures are isolated and local
  delivery continues.
- **Cluster layer**: `server.cluster` — presence, introspection and
  node-to-node messaging across every instance sharing a backplane, on two
  channels (`cluster`, `inst:<instanceId>`); every operation degrades to a
  local-only view without a backplane.
  - `cluster.count/presence/instances` are local reads kept warm by
    join/leave deltas plus a periodic snapshot (`presenceInterval`,
    default 5 s) that heals lost deltas; a restart's fresh epoch replaces
    counters instead of doubling them.
  - `cluster.fetchClients({ room }?)` collects descriptors
    (`{ id, instance, rooms, data, transport, session }`) from every node,
    resolving as soon as the last live node answers (`incomplete: true` on
    timeout, never silently).
  - `cluster.join/leave/disconnect(target, ...rooms)` — client ids are now
    instance-prefixed (`<instanceId>.<generateId()>`, so `instanceId` may
    not contain `'.'`), letting an id-addressed command travel as one
    message to one node. `client.data` is the app's bag, carried by
    descriptors; `rpc.getClient(id)` looks a local client up.
  - `cluster.sendEvent(name, data)` fans out to every other node's
    `cluster.on(name, fn)`; `cluster.ask(name, data)` collects one answer
    per node via `cluster.respond(name, fn)`.
- **Acks**: a server → client event may carry an `id`, making it a question
  the client answers with an ordinary `callback` packet.
  `client.ask(name, data, { timeout })` resolves with the answer (408/503/501
  on timeout/disconnect/no-responder); the client registers exactly one
  responder per name with `client.respond(name, fn)`/`unrespond(name)`.
  `server.to(room).ask(...)` aggregates
  `{ answers, errors, expected, incomplete }` cluster-wide via two-phase
  accounting, and serializes the payload once for the whole fan-out.

**Subscriptions, batching & SSE** — wire protocol v2: `subscribe`/`data`/
`end`/`unsubscribe`/`cancel` packets, plus a JSON array as a batch frame
(`docs/reference/protocol.md`).
- Subscription procedures — an async-generator handler answers with a
  stream of values (`procedure.subscription({ handler })` only needed for a
  plain function returning an async iterable). `queue`/`timeout` are
  refused on a subscription. The pump respects transport backpressure and
  always answers `end` exactly once (completion, throw, unsubscribe, or
  disconnect), closing the generator so its `finally` runs.
- Resume: `tracked(eventId, data)` labels a value; the client sends the
  last one back as `lastEventId` after reconnect. `createEventLog({ size })`
  is the ring buffer behind it — ids are `<epoch>.<n>` with a random epoch
  per instance, so `since()` answers `null` (an honest "cannot resume") for
  a foreign epoch or a gap past the buffer; persisted logs pass a stable
  `epoch`.
- `createEventStream()` (also on the browser entry): the push→pull adapter
  between "something calls me with a value" and a `for await` consumer.
  Bounded — an outrun producer drops the oldest value and reports it via
  `dropped`.
- Cancellation: `client.api.unit.method(args, { signal })` sends
  `{ type: 'cancel' }`; the caller rejects with **499** and `ctx.signal`
  aborts. Best-effort — a handler that ignores its signal keeps running, but
  its late result is dropped. A call cancelled while still queued is
  removed rather than racing the `cancel` ahead of its `call`. Cancel and
  unsubscribe are registered synchronously (a same-turn cancel is never
  missed). A disconnect aborts everything running for that peer.
- Batching: `batch: { flush: 'microtask' | ms, maxSize, maxBytes }`
  coalesces same-tick calls into one frame (a ping/cancel/unsubscribe never
  batches; a batch of one is sent bare). HTTP answers come back as one array
  in request order. `maxBatch` (128) caps a frame; `maxSubscriptions` (256)
  caps concurrent generators per client. A malformed element keeps its own
  id, preserving the array's positional guarantee; one unroutable answer no
  longer strands the rest of a batch.
- `@alexify/wrpc/sse` subpath: SSE as a full duplex transport. A channel is
  `GET {basePath}/events?channel=<id>` plus `POST {basePath}` with
  `x-wrpc-channel`, both bound to one server-side `Client` (the POST answers
  `202`; every reply travels on the stream). The channel id is
  **server-minted** (never client-proposed) and bound to the cookie identity
  that created it — a different identity gets 403, an unknown id gets 409.
  A dropped stream is held for `retention` (30 s) and a reconnect with
  `Last-Event-ID` re-attaches, replays missed frames, and **resumes**
  (rather than orphans) live subscriptions; `replayBytes` (1 MiB) bounds the
  replay buffer by bytes, and an id older than the buffer answers
  `event: gap` instead of a truncated replay. `maxChannels` (10000) and
  `maxChannelsPerAddress` (100) cap creation with 503/429. Comment frames
  and `X-Accel-Buffering: no` keep proxies from buffering/timing it out.
  Cross-origin requests carry full CORS headers including the
  channel/resume headers in the preflight allowlist. The session cookie
  from the opening GET restores on the channel exactly as `attachSocket`
  does for an upgrade. Text-only — binary streams are refused. Client half
  is browser-safe (`fetch` + a hand-written incremental parser, not
  `EventSource`), registered as `WrpcClient.transport.sse`;
  `connect(url, { transport: 'sse' })` selects it.
- `HttpCall` gained an optional `stream({ status, headers })` — the node
  shell, express and fastify implement it; a host that cannot answers the
  events endpoint with 501.
- `Context.signal`, `Client.calls`, `Client.subscriptions`, `Client.drain()`,
  `Client.binary`; `RpcServer.eventsPath`/`RpcServer.sse`.
- `client.close()` ends every live subscription uniformly (not just
  `iterate()` consumers) and delivers the same terminal callback an `end`
  packet does, exactly once; each listener is contained so one throwing
  handler cannot rob the next of its signal. `unsubscribe()` stays silent on
  purpose. `close()` also releases an `iterate()`'s `AbortSignal` listener.

**Typed client, codegen & Query bindings** — no TypeScript at runtime.
- Contract-first typed client: declare the API as an interface and thread
  it through `connect<Api>(url)` (a one-line alias of
  `WrpcClient.connect`). A call keeps its declared arguments plus a
  trailing `CallOptions` (`{ signal }`); a member typed
  `SubscriptionContract<Args, Data>` becomes `subscribe`/`iterate` instead
  of callable; `load()` only accepts declared unit keys. Utilities:
  `TypedApi`, `TypedUnit`, `TypedMethod`, `TypedParams`,
  `TypedSubscriptionMethod`, `InferArgs`, `InferResult`, `FirstArg`,
  `UntypedApi`, `IsAny`, `InvalidContractMember`. Without a contract
  everything stays exactly as loose as before. A zero-argument member keeps
  its args slot (`ping(undefined, { signal })`); a two-parameter/rest/
  non-function member maps to `InvalidContractMember`; a contract key named
  `on` is not mapped (a unit is an `Emitter` at runtime).
- `wrpc types <url> --out api.d.ts [--units ...] [--interface Api]
  [--package ...] [--schema <path> [--format cjs|esm]]` generates that
  interface (or a raw introspection artifact) from `system/introspect`;
  output is sorted for byte-identical re-runs. `client.use(introspection)`
  scaffolds units from that artifact synchronously, before `open()`, with
  zero wire traffic — a `load()`ed unit still wins and reloads on reconnect.
- The `signature` descriptor is specified (`docs/reference/protocol.md`): a
  type name, a field map (keys may end in `?`), or a one-element array
  meaning "array of". A closed, depth-capped format — names are JSON-quoted,
  types matched against an allowlist, `__proto__` is data, unrecognized
  types become `unknown`. `Signature`/`SignatureShape` type it.
- `@alexify/wrpc/query` subpath: `createQueryUtils(client, { prefix,
  queryClient })` → `queryKey`/`queryOptions`/`mutationOptions`/
  `subscriptionHandler`, as option **factories** (tRPC v11 style, not
  hooks) serving React/Solid/Svelte/Vue Query and query-core alike.
  Requires nothing (1 KB min+gzip); `queryFn` resolves `['unit','method']`
  lazily via `Object.hasOwn` on both hops (so `['chat','on']` cannot reach
  `Emitter.prototype.on`); TanStack's `AbortSignal` forwards into the call;
  `subscriptionHandler` writes through `setQueryData` and needs nothing on
  reconnect.
- Bundle-size budgets: `scripts/size.js` fails CI when a browser-reachable
  entry exceeds its min+gzip budget; an esbuild resolve plugin fails the
  build naming any non-relative import in a browser entry (catches a
  devDependency getting silently inlined, not just a bare `node:*`).
- Package consistency tests (`tests/package/consistency.test.js`): every
  exports target exists and ships, every subpath has an ordered `types`
  condition and a tsd file, shims resolve, plus the `./package.json` export.
- Browser type conditions: the surface split into `client.d.ts`
  (browser-safe) re-exported by `index.d.ts`/`browser.d.ts`/
  `sse.browser.d.ts` under the `browser` condition — importing a server name
  in a browser bundle is a compile error, and a project without
  `@types/node` compiles clean (checked by a tsc fixture compiled with
  `types: []`).
- A real Redis integration test (`tests/scaling/redis.integration.test.js`,
  manual/local, `REDIS_URL`-gated) exercises two independent
  `createRedisAdapter` instances over separate connections.
  `socket.io`/`tRPC` (`wsLink`) joined `bench/rpc-comparison.js`, each
  measured sequential and pipelined (64 in flight).

**Client resilience**
- Reconnect: truncated exponential backoff with full jitter
  (`reconnect: { minDelay, maxDelay, factor, jitter, retries }`; the
  `reconnectTimeout` shorthand now clamps the same way), `'reconnecting'`/
  `'reconnect-failed'` events, `client.attempt`. A rejected `open()` now
  reschedules the next attempt instead of stalling the loop; the reconnect
  timer is not `unref`'d (a live socket already holds one). `'reconnect'`
  fires after the api is rebuilt: loaded units reload, removed server
  methods drop, `api` unit objects are reused so their listeners survive.
- App-level heartbeat (`{type:'ping'}`/`{type:'pong'}`,
  `heartbeat: { interval, timeout }`) — the only defense against a dead
  `WebSocket` that never sent a close frame; only the WS transport starts
  one, both sides answer an inbound ping.
- An event reaching no listener surfaces as `'unhandled-event'`; a
  background failure with no `'error'` listener is logged, not thrown.
- In-flight calls reject with a coded 503 the moment the connection dies;
  client streams terminate on disconnect; reconnect-restore re-opens
  subscriptions before (and independently of) `load()` — a failing reload
  emits `'restore-failed'` and forces a clean reconnect rather than
  silently killing every subscription.

**Authentication & metadata**
- Client `authenticate` hook: awaited inside `open()` on the first connect
  (so `connect()` resolves an already-authenticated client) and on every
  reconnect **before** the subscriptions re-open and the units reload — the
  window an `'open'` listener structurally cannot reach. A throw terminates
  the transport, emits `'authenticate-failed'` and walks the normal backoff;
  `client.close()` inside the hook stops the cycle. With a hook configured,
  `'open'` fires after a successful authentication.
- Client `refresh` hook (`fn` or `{ on, handler }`, default trigger `[401]`):
  single-flight credential refresh — N concurrent refusals produce one
  handler run — with each refused call re-issued exactly once under a fresh
  packet id, on both the packet and the REST leg; on failure the original
  refusal surfaces. Never fires for calls made inside `authenticate`.
- Public `client.call(target, args, options)` — one call by wire target with
  no scaffolding: the escape hatch a first-connect hook needs, since `api`
  is built by `load()`.
- `client.meta` / `context.meta`: a frozen snapshot of what the peer
  presented — request/upgrade `headers`, `url` (every attach site used to
  drop `req.url`), `remoteAddress`, negotiated ws `protocol`, and declared
  `data`.
- Client `headers` option (connection-phase, re-evaluated per open): real
  request headers on http/sse/worker, one `wrpc_h` query parameter on
  browser ws (the WHATWG constructor takes no headers; observed headers win
  the merge, reserved names are dropped from the query path). Validated when
  a procedure declares `schema.headers` — the part was previously accepted
  and silently ignored; delegated fastify routes still validate in fastify,
  not twice.
- Client `meta` option and per-call `meta` (`client.call(..., { meta })`,
  `method.withMeta({...})(args)`): an optional additive `meta` field on
  `call`/`subscribe`/`event` packets, surfaced as `context.callMeta`
  (frozen-empty default) and `client.meta.data`; deliberately outside the
  `validation` option. Two wire spellings, both emitted by the client and
  both accepted from plain HTTP callers: the `x-wrpc-meta` header
  (percent-encoded JSON, the default — type-faithful, one CORS entry) and
  the per-key `x-wrpc-meta-<key>` form (the S3 `x-amz-meta-*` idiom; string
  values, one CORS entry per key), chosen with the client's
  `metaFormat: 'json' | 'prefixed'`. The JSON header wins a key collision.
  Both channels share one sanitizer: `metaMaxBytes` cap (default 2048) on
  the encoded input, plain-object check, `__proto__` drop, freeze.
- **Keys of both declared bags are normalized to kebab-case** (`userId` ->
  `user-id`, `xAppVersion` -> `x-app-version`) on every transport and on
  both ends, so `schema.headers` has one casing to validate and the two meta
  spellings collide on the same key instead of sitting side by side as
  lookalikes. Underscores are left alone; keys differing only in acronym
  casing merge (last write wins); an external caller must write kebab itself
  because HTTP lowercases header names before the server observes them.
- Per-call `meta` now reaches the client's **mapped REST leg**, which
  silently dropped it: with no packet to ride, it merges over the connection
  bag and travels as request headers, the per-call half winning. Under
  `batch: true` the request headers carry the batch's **aggregate**
  (last write wins) as a summary for gateways and access logs — each call's
  exact meta still rides its own packet and is what `context.callMeta`
  reports. An oversize aggregate is refused client-side with a
  `meta.oversize` warning rather than left to the server's whole-bag drop.
- `cors.metaHeaders: string[]` names the per-key meta headers CORS cannot
  wildcard (`['userId']` grants `x-wrpc-meta-user-id`, normalized with the
  same rule the client uses); appended to `cors.headers`, which now also
  accepts an array. `ClientTransport.request` takes `{ rest, meta }` in
  place of its trailing `rest` argument.
- Pluggable session token carrier (`sessions: { transport }`, structural via
  `isTokenTransport`): the cookie default is byte-identical; a non-ambient
  carrier (`ambient: false`) is exempt from the safe-method CSRF rule it
  never needed, and SSE channel identity keys on whatever the carrier reads.
- New **`@alexify/wrpc/auth`** subpath (browser-safe, own 2 KB budget):
  token stores (`memoryStore`/`webStorage`/`cookieStorage` — a `Map`
  already satisfies the contract), `bearerAuth()` composing
  `headers`+`authenticate`+`refresh` around a store, and the server halves
  `bearerTransport()` / `payloadTransport()`.
- Server dispatch now gates on `client.ready` — session restore **plus**
  settled `onConnect` hooks — so a subscribe racing the hooks can no longer
  miss a room broadcast; a hook stalled past 5 s logs `onConnect.stalled`.

**Observability**
- Structured `logger` option (replacing `console`): one injected writer
  (`src/logging.js`, zero-import) normalizing a structured logger (pino/
  bunyan/winston, `(entry, message)`), a `Console` (`(message)`), or
  nothing. `false` silences a server outright; a shape matching neither
  disables logging rather than throwing. wrpc binds children per subsystem/
  connection/call, reachable as `context.log`; a throwing sink is contained
  to the line, not the call. Client `logger` is **off by default** — when
  on, an error is logged *and* emitted as `'error'`.
  `WrpcClientProxy` forwards the option.
- Three silent failure paths now report: a server-side subscription death
  used to answer `end` and log nothing; an aborted subscription's generator
  error was dropped entirely; `jsonParse(data) || {}` conflated "malformed"
  with "empty", hiding every unparseable packet behind one `||`.
- OpenTelemetry: `telemetry` accepts the `@opentelemetry/api` module or a
  custom `{ tracer, meter }` (tracer-only and meter-only both work; the
  module itself is never imported). Spans follow the OTel `rpc.*`
  convention and bracket the whole invocation. Fourteen instruments cover
  calls, durations, connections, subscriptions, broadcasts/fan-out size,
  stream bytes, backpressure, sessions and SSE channels.
  `includeIdentity: false` drops the peer address; a session token is never
  recorded at any setting. Every recording path contains its own failures.
- W3C trace context: `call`/`subscribe`/`event` packets may carry optional
  `tp`/`ts` fields, propagated per *packet* (not per connection) so each
  call in a batch keeps its own parent. wrpc hands the field to your
  propagator rather than parsing it, so propagation needs `{ api }` (or an
  explicit `propagation`). `trustRemoteContext` defaults to `true`; set it
  `false` when peers are untrusted.
- Telemetry discipline: unresolved method/event names collapse into an
  `<unknown>` bucket (never minted from peer-controlled text); metric
  attributes match span attributes per RPC semconv; the server dispatch
  path skips telemetry allocations entirely when disabled.
- Hot path: UTF-8 validation delegates to `node:buffer.isUtf8` above the
  native-call threshold (up to 70x on large frames vs `ws`); the frame
  parser stopped copying the 4-byte mask and allocating a `Result` per
  not-enough-bytes attempt; fragmented sends cork once per message; client
  batching serializes each packet once; error logging materializes
  `error.stack` only when a logger is enabled.
- Pluggable `generateId: () => string` on client (packet/subscription/
  stream ids) and server (context uuids, server-side stream ids, synthetic
  REST packet ids); a stream id is validated against the 255-byte
  chunk-header limit at the source. The browser runtime gained a
  `Math.random`-based uuid fallback for plain-http pages.
- Robustness against malformed/adversarial input: a malformed packet
  target is type-checked rather than trusted (an un-awaited dispatch that
  threw used to take the process down); every fire-and-forget dispatch has
  a terminal catch; a wire event or contract key named after an
  `Object.prototype` key cannot resolve up the prototype chain
  (`load()` defines units instead of assigning them); an `event`/`pong`
  packet over HTTP requires a persistent transport (400) instead of hanging
  the request; a socket abandoned by `terminate()` cannot close its
  reconnected replacement (scoped by socket identity); `createRedisAdapter`
  quits the subscriber it opened itself (never an injected one).
- A graceful WebSocket close now completes in milliseconds instead of ~1 s:
  the side *answering* a Close writes the echo, half-closes, and destroys
  after a short grace, per RFC 6455 5.5.1/5.5.2.
- A real `SECURITY.md` (scope, acknowledgement/fix windows, supported
  versions) and a stability/deprecation policy in `CONTRIBUTING.md`, with
  `@experimental` markers on the telemetry shapes and the engine-port
  `capabilities`.

### Fixed

- A cluster node that asked a peer for its presence `state` and never got
  the answer (the reply travels at-most-once too — the node's own inbox
  channel may not be subscribed yet on the first digest) kept its
  `syncing` flag forever and ignored every later digest, so its view of that
  peer stayed wrong until restart. The wait is now bounded: after two
  presence intervals without an answer the sync is asked again. Found by
  the multi-node bench on its first run.
- A ping arriving after the engine had FAILED the connection (a protocol
  error, an oversized or undecodable message) was still answered with a
  pong; RFC 6455 7.1.7 says nothing after the failure is acted on. Autobahn
  cases 4.1.3–4.2.5 flagged it; the full suite now passes. A ping after an
  app-initiated `close()` is still answered, as before.
- The uws engine never compressed an outbound frame: `UwsSocket.send` did not
  pass uws' third `compress` argument (which defaults to false), so a
  configured `compression` only ever applied to inbound messages while
  `capabilities.deflate` reported true. It now asks uws to compress, and
  `tests/adapters/uws.test.js` asserts RSV1 on the wire.
- `client.sessionReady` is assigned **before** the `onConnect` hooks run —
  the documented `await client.sessionReady` recipe used to await the
  constructor's resolved default and see `session === null`; the packet-POST
  and delegated paths now share the same restore promise instead of
  discarding it.
- A restore (or authenticate) failing **after** the socket opened restores
  the attempt count before terminating, so the backoff grows and `retries`
  exhausts instead of hammering `minDelay` forever — which also means the
  transport-fallback list is actually reached from a post-open failure.
- A rejected `WrpcClient.connect()` closes the half-born client instead of
  leaking it into `WrpcClient.connections` for `online()` to revive; a
  throwing `'restore-failed'` listener is escalated rather than becoming an
  unhandled rejection; `WrpcClient.online()` no longer aborts its re-open
  loop on the first client without an `'error'` listener.

### Changed (breaking)

- Unit version keys must be `unit.vN` (`auth.v1`, not `auth.1`) — the wrong
  spelling throws a `TypeError` at router build. The token is stored
  verbatim, which is what lets the REST `/vN` prefix be a plain
  concatenation.
- `new Server(application, options)` is gone: the server takes one options
  object with `router` (`new Server({ router, host, port, protocol,
  sessions, cors, basePath, engine, ws, logger })`); the metarhia-style
  `application.getMethod()` coupling is fully removed, procedures come from
  `defineRouter`, handlers receive `(context, args)`.
- `console` is gone (not deprecated) in favor of `logger` — the fastify
  adapter's `toConsole` shim went with it, `fastify.log` (a pino) now goes
  in directly.
- `Client.emit` is the local `Emitter` emit again, not a wire send for
  every name but `'close'` — use `client.sendEvent(name, data)` to send.
- `instanceId` must not contain `'.'` (it prefixes every client id as
  `<instanceId>.<generateId()>`).
- 5xx error messages no longer travel to the caller — only the status line
  does, details stay in the server log correlated by packet id; a 4xx
  message still travels verbatim. `error.expose = true` opts a message in.
- `cors.origins` is enforced on HTTP calls too, not only the WS upgrade —
  a disallowed origin gets 403 instead of running with the grant withheld.
- Unknown `access` values throw at router construction instead of silently
  meaning "any session".
- The per-call success log line moved to `debug`, and a Console sink drops
  `debug` outright.
- `maxBackpressure` defaults to `maxBuffer` (was unbounded) — `0` opts back
  into unbounded.
- `ServerWsTransport` is constructed as `(connection, meta)`;
  `ServerHttpTransport` wraps an abstract call description instead of node
  `req`/`res`.
- `Client.restoreSession`/`finalizeSession` are async (store-backed);
  dropping a connection no longer deletes the session.
- The `Emitter` warns instead of throwing when `maxListeners` is exceeded
  (fan-out to many stalled streams is legitimate); duplicate-listener and
  unhandled-`'error'` throws remain.
- `websocketPath` option removed — path gating follows `basePath` (or pass
  `ws: { path }`/`ws: { verifyClient }` through to the engine).
- Data-send methods' boolean now means "accepted without exceeding the
  buffer", not "accepted for send" — wait for `'drain'` after `false`
  (check `closed`, since a closed transport also reports `false`).
- The WebSocket engine internals (`WebsocketServer`, `Connection`, `Frame`,
  `FrameParser`, `ParseError`, `PARSE_ERR_CODES`, `OPCODES`, `CLOSE_CODES`,
  `CLOSE_TIMEOUT`, `MAGIC`) live at `@alexify/wrpc/ws`, not the main barrel
  — the main entry keeps only the RPC-level API.

### Changed

- Writes to `session.state` coalesce: the assignments of one turn become one
  `store.set` on a microtask (`create()` still persists the initial state
  immediately), and a session finalized in the meantime is not written back
  (`Session.end()`, called by `finalizeSession`). One round trip per turn to
  a shared store instead of one per assignment.
- Unicast frames are one contiguous buffer up to 16 KiB — header and payload
  in a single socket write, text utf8-encoded from a module scratch buffer
  instead of an intermediate `Buffer.from` — and header + payload writes
  above it. `sendText 4 KB` 735,099 → 1,671,520/sec; 200 B unchanged.
- `docs/guide/performance.md` no longer claims the fan-out frame was encoded
  once — before this release only the JSON was; utf8, deflate and framing
  were per recipient.
- Server transports carry a `kind` — `ws`, `http`, `sse` or `event` — as a
  metric attribute and log field.
- Every shell and adapter funnels its option bag through `rpcOptions()`
  rather than re-listing the core's options by hand.
- `ServerTransport.send()` returns the transport's backpressure signal, so
  a producer can wait for `'drain'` instead of buffering without limit.
- `src/telemetry/` is three files (shared/server/client) instead of one, to
  keep the browser bundle under its size budget.

### Documentation

- **The wire protocol is frozen as 1.0** (`docs/reference/protocol.md`):
  packet types, fields and meanings do not change within the major version;
  a new optional field may be added, and an unknown packet type still
  answers a `callback` with code 500, which is what makes an additive
  change safe for an older peer.
- The documentation site is the full structure, not a skeleton: a guide
  track (getting started, server, router, sessions, rooms, subscriptions,
  streams, scaling, client, typed client, CLI, TanStack Query, SSE, hooks,
  logging, OpenTelemetry, one page per adapter) plus a reference track
  (wire protocol, wire format, engine port) with grouped nav/sidebar.
- README rewritten: badges, a positioning table against tRPC and
  Socket.IO, an honest *When NOT to use wrpc*, the feature matrix, an
  exports table with one row per subpath, and the `pnpm size` bundle-size
  table.
- `CONTRIBUTING.md` added: development workflow, house rules not visible in
  the code (zero dependencies, `.d.ts`/`tsd` pairing, the uws teardown trap
  that wedges `node --test`), and the manual release checklist.
