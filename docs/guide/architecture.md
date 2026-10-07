# Architecture

wRPC is a handful of pieces with narrow seams between them: a client, a wire,
a server core, and the hosts and carriers that connect the two. This page is
the map. Each diagram shows where two pieces meet, and each section points to
the page that owns the details. For the one-picture version, see
[Getting Started](./getting-started#how-it-fits-together).

## The layers {#layers}

A call crosses the same layers in both directions, and each layer has one job:

```mermaid
flowchart LR
  subgraph C["client — browser or Node.js"]
    direction TB
    c1["your code<br>client.api.chat.send(args)"] --> c2["WrpcClient<br>call ids · pending map · units"]
    c2 --> c3["encoding<br>JSON or a codec · attachments"]
    c3 --> c4["client transport<br>ws · http · sse · wt<br>webrtc · broker · event"]
  end
  subgraph S["server — Node.js"]
    direction BT
    s4["host<br>engine · framework · HTTP/3 host<br>data channel · broker"] --> s3["RpcServer<br>four entry points"]
    s3 --> s2["Client + ServerTransport<br>one pair per connection"]
    s2 --> s1["dispatcher → Router<br>→ your procedure"]
  end
  c4 <== "frames" ==> s4
  s2 -.- side["sessions · rooms · cluster<br>SSE channels · telemetry"]
```

| Layer | Owns | Swapped by | Details |
| --- | --- | --- | --- |
| `WrpcClient` | call ids and the pending map, units scaffolded from introspection, subscriptions and their `lastEventId`, streams, reconnect, heartbeat | — | [Client](./client) |
| Client transport | one wire: opening it, writing to it, reporting `open`, `close` and `message` | a registered transport, `WrpcClient.transport[name]` | [Client › Transports](./client#transports) |
| Encoding | JSON or an injected packet codec; bytes as attachments frames under revision 2; optional compression and encryption | the `codec`, `compression` and `encryption` options | [Wire codec](./codec), [Compression](./compression), [Encryption](./encryption) |
| Host | the network: listening, parsing HTTP, the WebSocket handshake and frames | an engine, a framework adapter, an injected HTTP/3 or WebRTC stack, a broker | [Server](./server), [Engine port](../reference/engine) |
| `RpcServer` | the four entry points, a `Client` per connection, sessions, rooms, the cluster, SSE channels | — (it is the core) | [Server › Using the core directly](./server#using-the-core-directly) |
| Dispatcher and `Router` | routing packets, hooks, validation, timeouts, queues | — | [Router](./router), [Hooks](./hooks) |

Nothing above the transport knows which wire it is on, and nothing below the
dispatcher knows what a procedure does. Every other section of this page
depends on that.

## Life of a call {#life-of-a-call}

One `call` over a WebSocket, through every layer of the picture above:

```mermaid
sequenceDiagram
  autonumber
  participant App as your code
  participant WC as WrpcClient
  participant W as ws transport ⇄ engine
  participant CL as server Client
  participant D as dispatcher
  App->>WC: client.api.chat.send({ text })
  Note over WC: mint an id, park the promise<br>in the pending map, arm the deadline
  WC->>W: { type: "call", id, method: "chat/send", args }
  W->>CL: text frame → socket "message"
  CL->>D: handleMessage → handleRpc
  Note over D: onRequest hooks · await client.ready<br>(session restored) · access check
  Note over D: procedure.invoke: queue slot ·<br>input · handler · output
  D->>CL: client.send({ type: "callback", id, result })
  CL-->>W: ServerTransport.write → text frame
  W-->>WC: "message"
  WC-->>App: the pending promise for id resolves
```

The hook phases at steps 5 and 6 are on [Hooks](./hooks#the-pipeline),
and the deadline and the queue slot on
[Router › Timeouts and queues](./router#timeouts-and-queues). What a timeout
or a `cancel` does to a call in flight is in the
[protocol reference](../reference/protocol#packets).

## Four ways in {#entry-points}

`RpcServer` never listens on anything. Every host hands it connections
through one of four methods, and all four end in the same place: a
server-side `Client` that the dispatcher talks to.

```mermaid
flowchart LR
  eng["WebSocket engine<br>node · uWebSockets.js"]
  wts["WebTransport session<br>attachSession"]
  req["HTTP request<br>Server · express · fastify · uws"]
  brq["broker request<br>attachBrokerRpc, stateless"]
  dc["WebRTC data channel<br>attachChannel"]
  brs["broker session<br>attachBrokerRpc, session"]
  qd["queue delivery<br>attachConsumers"]
  prt["MessagePort<br>server.emit('port')"]
  as["RpcServer<br>attachSocket(socket, meta)"]
  hh["RpcServer<br>handleHttpCall(call)"]
  at["RpcServer<br>attach(transport, options)"]
  ap["RpcServer<br>attachPort(port)"]
  eng --> as
  wts --> as
  req --> hh
  brq --> hh
  dc --> at
  brs --> at
  qd -- "persistent: false" --> at
  prt --> ap
  as & hh & at & ap --> cl["a Client per connection<br>session restore · onConnect hooks"]
  cl --> disp["dispatcher"]
```

| Entry point | Takes | Used by |
| --- | --- | --- |
| `attachSocket(socket, meta)` | anything shaped like the engine port's `WrpcSocket`: `send`, `close`, `'message'` and `'close'` events | every WebSocket engine; WebTransport, whose session becomes a `WtSocket`; under [session encryption](./encryption#session), a `SealedSocket` wrapped around either |
| `handleHttpCall(call)` | an abstract `{ method, url, headers, body, respond, stream? }`, never a node `req`/`res` pair | the `Server` shell, the express middleware, the fastify routes, the uWebSockets.js engine's `onHttpCall`, a stateless broker request; SSE channels open here too |
| `attach(transport, options)` | any transport that announces inbound text as `'packet'` and bytes as `'chunk'` | a WebRTC data channel, a broker session, a queue consumer (`persistent: false`: it runs calls, never receives events) |
| `attachPort(port)` | a `MessagePort` | a worker thread, an embedded peer, a test harness |

From there every connection takes the same steps. A `Client` is created, its
session is restored from whatever the connection presented, and the router's
`onConnect` hooks run. Both of those make up `client.ready`, which the
dispatcher awaits before the first access check. Every packet after that goes
through one dispatcher. A procedure cannot tell a WebSocket client from a
broker message, except by asking `ctx.client.transportKind`.

## One connection on the server {#connection}

```mermaid
classDiagram
  direction LR
  class RpcServer {
    router
    sessions
    rooms
    cluster
    to(room) Broadcast
    broadcast(name, data)
    sendTo(clientId, name, data)
  }
  class Client {
    id
    session
    meta
    calls
    subscriptions
    streams
    ready
    send(packet)
    sendEvent(name, data)
    ask(name, data)
    join(room)
  }
  class ServerTransport {
    kind
    revision
    send(packet)
    write(frame)
    close()
  }
  class Context {
    client
    session
    signal
    state
    callMeta
  }
  RpcServer "1" *-- "*" Client : one per connection
  Client --> ServerTransport : writes through
  Client ..> Context : one per call
  RpcServer --> Router
  Router *-- Procedure
  RpcServer --> SessionManager
  SessionManager --> SessionStore : injected
  SessionManager --> TokenTransport : injected
  RpcServer --> RoomRegistry
  RoomsBackplane --> RoomRegistry
  RoomsBackplane --> Backplane : injected
  RpcServer --> Cluster
  RpcServer --> SseChannels
```

- **`Client.id` is an address.** It is `<instanceId>.<generated id>`, so a
  cluster command or `server.sendTo(id, …)` goes straight to the instance
  that holds the client, with no broadcast.
- **A `Client` keeps what a peer can take back.** `calls` holds an
  `AbortController` per call in flight, which `cancel` and a disconnect both
  reach (it is `ctx.signal`). `subscriptions` and `streams` are capped by
  `maxSubscriptions` and `maxStreams`.
- **`ServerTransport` is the only piece that knows the wire.** There is one
  subclass per kind: `http`, `ws` (WebTransport extends it), `event`, `sse`,
  WebRTC's `RtcPeerTransport` and the broker's session transport. The
  `Client` above it is the same for all of them.
- **A `Context` lives for one call.** It reads `session` and `meta` through
  its `Client` and reaches the server as `ctx.server`. That is how
  `ctx.server.to(room)` works with no server captured in a closure.
- **Three things are injected**: the session store and the token transport
  ([Sessions](./sessions), [Authentication](./auth)), and the backplane under
  rooms and the cluster ([Scaling](./scaling)).

## The receive path {#receive}

What the core does with one inbound WebSocket message. The other entry points
join this path at `handleMessage` (or at the chunk branch), so the dispatcher
below is the only one.

```mermaid
flowchart TD
  M["socket 'message'<br>already opened by a SealedSocket<br>on an encrypted connection"] --> T{"text or bytes?"}
  T -- "text" --> HM
  T -- "bytes" --> MK{"first byte 0x00?"}
  MK -- "no: a stream chunk" --> CH["chunkDecode → stream id"]
  CH --> RD["WrpcReadable.push<br>socket paused until it settles"]
  MK -- "kind 1: attachments" --> HM
  MK -- "kind 3 / 4: compressed" --> INF["inflate with the codec<br>agreed on ping/pong"]
  INF -- "kind 3: a packet" --> HM
  INF -- "kind 4: a chunk" --> CH
  HM["handleMessage<br>attachments frame · codec · JSON"] --> B{"array?"}
  B -- "yes: a batch, item by item" --> HP
  B -- "no" --> HP["handlePacket, by type<br>call → handleRpc<br>subscribe · unsubscribe → the pump<br>event → the unit's on handler<br>stream → open or end a WrpcReadable<br>cancel → abort that call's signal<br>callback → settle a server-side ask<br>ping → pong"]
```

Pausing the socket while a chunk is being consumed is what lets
backpressure reach the peer through TCP. See
[Binary streams › Backpressure](./streams#backpressure). The frame kinds are
in the [protocol reference](../reference/protocol#binary-chunks).

## The send path {#send}

A packet is encoded **once**, in exactly one of four ways, and handed to its
transport. Everything below that is the carrier's business:

```mermaid
flowchart TD
  P["a callback, an event,<br>a subscription value"] --> E{"encode once"}
  E -- "a packet codec is set" --> E1["codec.encode"]
  E -- "a compiled serializer" --> E2["envelope around the<br>serialized result"]
  E -- "bytes, revision 2" --> E3["attachments frame"]
  E -- "otherwise" --> E4["JSON.stringify"]
  E1 & E2 & E3 & E4 --> W["ServerTransport.write"]
  W --> K["the carrier's own layers<br>compression · encryption · framing"]
```

What happens below `write` is per carrier, and some layers exclude others:

| Carrier | Below `write`, in order |
| --- | --- |
| WebSocket, server → client | sealed when the connection is encrypted (then sent with `compress: false`: ciphertext does not deflate) → engine frame → permessage-deflate when negotiated |
| WebSocket, Node client → server | compressed when the ping/pong agreed a codec → sealed when encrypted. **Compress, then seal** |
| WebTransport | five-byte length and kind header, compressed when the capabilities agreed a codec → the control stream. Under session encryption there is no compression, no stream mux and no datagram |
| WebRTC | compressed when the description agreed a codec → fragmented to the channel's message size → the data channel, which is DTLS already |
| HTTP, SSE | the response body or the SSE event → `Content-Encoding` when the request accepts one. A sealed request carries no content coding inside |
| Broker session | compressed if smaller (`wrpc-enc` names the codec) → sealed under the keyring → the broker |

[Compression](./compression) and [Encryption](./encryption) explain each
layer.

A room broadcast reuses the single encoding:

```mermaid
flowchart LR
  E["server.to('lobby').emit(name, data)"] --> F["one encoded frame<br>(prepared once on the node engine)"]
  F --> M1["local member"] & M2["local member"] & M3["local member"]
  E --> BP["publish an envelope<br>on channel room:lobby"]
  BP --> O["other instances<br>deliver to their own members"]
```

A sealed connection is the exception: it gets the shared plaintext frame
and seals it for itself, so a broadcast to *n* sealed clients costs *n*
seals.

## The client {#client}

Outbound, a call leaves through one queue and one encoder:

```mermaid
flowchart TB
  o1["client.api.unit.method(args)<br>scaffolded by load() or use()"] --> o2["call packet<br>refresh · retry wrappers,<br>when configured"]
  o2 --> o3["pending map<br>id → promise<br>one shared deadline timer"]
  o3 --> o4{"batching on?"}
  o4 -- "yes" --> o5["queue → one<br>array frame"]
  o4 -- "no" --> o6["codec · attachments<br>· JSON"]
  o5 & o6 --> o7["transport.write"]
```

Inbound, everything is sorted by what arrived:

```mermaid
flowchart LR
  i1["transport<br>'message'"] --> i2{"bytes or text?"}
  i2 -- "a chunk" --> i3["the stream's<br>WrpcReadable"]
  i2 -- "text or an<br>attachments frame" --> i4{"packet type"}
  i4 -- "callback" --> i5["settle the pending call"]
  i4 -- "event" --> i6["client.api.unit emits it"]
  i4 -- "event with an id" --> i7["a respond() handler<br>answers with a callback"]
  i4 -- "data · end" --> i8["the subscription,<br>which keeps lastEventId"]
  i4 -- "stream" --> i9["a new WrpcReadable"]
```

`client.api` is built from what the server says about itself
(`system/introspect`) or from a generated artifact
([Typed client › Static introspection](./typed-client#static-introspection)).
That is why a unit is `undefined` until `load()` resolves. The connection's
own lifecycle (connecting, authenticating, restoring, waiting for the next
attempt) is a state machine on [Client › Reconnecting](./client#reconnecting).

## Plugging in {#extension-points}

Each subpath plugs into one of a few seams. There are two registries that a
subpath fills in when it is required, the four entry points above, and a few
options that take a structural contract.

```mermaid
flowchart LR
  sse["@alexify/wrpc/sse"]
  rtc["@alexify/wrpc/webrtc"]
  brk["@alexify/wrpc/broker"]
  wt["@alexify/wrpc/wt"]
  eng["/engine · /ws · /uws"]
  fw["/fastify · /express"]
  opt["/scaling · /auth<br>/encryption · /deflate"]
  ctt["client registry<br>WrpcClient.transport<br>ws · http · event · wt built in"]
  copt["client options<br>headers · authenticate · refresh<br>encryption · compression"]
  stt["server registry<br>ServerTransport.transport<br>http · ws · event built in"]
  ent["RpcServer<br>entry points"]
  sopt["RpcServer options<br>backplane · sessions<br>encryption · compression"]
  shell["Server<br>engine option"]
  sse -- "sse" --> ctt
  rtc -- "webrtc" --> ctt
  brk -- "broker" --> ctt
  wt -- "wt" --> stt
  wt -- "attachSession" --> ent
  rtc -- "attachChannel" --> ent
  brk -- "attachBrokerRpc<br>attachConsumers" --> ent
  eng --> shell
  fw -- "an RpcServer + an engine" --> ent
  opt --> sopt
  opt --> copt
```

Every contract is checked by shape (`isEngine`, `isBackplane`,
`isCompressor`, `isWtSession`, …), never by `instanceof`. That is why no
framework, broker client or WebRTC stack is a dependency of the package. Each
one is injected. Requiring a subpath is enough to
register its transport. The registries are plain objects on purpose, so a
transport of your own registers the same way.

## Across instances {#instances}

```mermaid
flowchart TB
  cl["clients"] --> lb["load balancer<br>affinity needed only for SSE"]
  lb --> ra & rb
  subgraph A["instance A"]
    ra["RpcServer<br>clients A.*"]
    la["kept local: event logs,<br>SSE channels"]
  end
  subgraph B["instance B"]
    rb["RpcServer<br>clients B.*"]
    lb2["kept local: event logs,<br>SSE channels"]
  end
  ra <--> bp[("backplane<br>room:&lt;name&gt; · broadcast<br>cluster · inst:&lt;id&gt;")]
  rb <--> bp
  ra <--> ss[("session store")]
  rb <--> ss
```

| Channel | Carries | Subscribed by |
| --- | --- | --- |
| `room:<name>` | an emit to exactly one room | instances that hold a member of that room |
| `broadcast` | `server.broadcast()` and multi-room emits | every instance |
| `cluster` | presence snapshots and deltas, cluster-wide commands and questions | every instance |
| `inst:<id>` | one instance's inbox: `sendTo` a client it holds, answers to its questions | that instance |

The backplane carries events and requests, never connections. A client that
reconnects to another instance gets its session back from the shared store,
its rooms back when an `onConnect` hook re-joins them
([Rooms › Rooms and reconnects](./rooms#rooms-and-reconnects)), and resumes
its subscriptions from `lastEventId`. [Scaling](./scaling) covers the contract
and delivery guarantees, [Cluster](./cluster) the presence protocol, and
[Running in production](./production#sticky-routing) what still needs
affinity.
