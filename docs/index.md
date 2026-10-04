---
layout: home

title: wRPC
titleTemplate: Web RPC for Node.js and the browser

hero:
  name: wRPC
  text: Fast, zero-dependency Web RPC
  tagline: One router, every web transport — WebSocket, HTTP, SSE, WebTransport, WebRTC — and across Redis, NATS, RabbitMQ and Kafka. Nothing in your lockfile.
  image:
    src: /logo-mark.svg
    alt: wRPC
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: Why wRPC?
      link: /guide/why
    - theme: alt
      text: View on GitHub
      link: https://github.com/Alexis-Technologies/wrpc

features:
  # Row 1 — one router, every web transport
  - icon: ⚡
    title: WebTransport, with a fallback
    details: HTTP/3 streams with no head-of-line blocking between them and unreliable datagrams for events that may drop — the same router and rooms as your WebSocket clients, and <code>transport&colon; ['wt', 'ws']</code> falls back where a browser has none. Experimental.
    link: /guide/wt
    linkText: Go HTTP/3
  - icon: 🤝
    title: Peer to peer over WebRTC
    details: Two browsers serve routers to each other with no server in the data path — calls, events, subscriptions and streams over a data channel, a mesh for rooms, built-in signaling, and peer identity bound to the DTLS fingerprint.
    link: /guide/webrtc
    linkText: Connect peers
  - icon: 📨
    title: Message brokers, injected
    details: Redis, NATS, RabbitMQ or Kafka — durable feeds a client resumes on any instance, queue consumers that deliver into procedures with retry and dead-lettering, and RPC between services with no HTTP between them. Experimental.
    link: /guide/brokers
    linkText: Bring your broker
  - icon: 🔌
    title: Runs where you already run
    details: A batteries-included server, or plug into fastify, express, bare node:http or uWebSockets.js; the same protocol rides Server-Sent Events where WebSockets cannot go — with pino-style logging and OTel traces built in.
    link: /guide/adapters/fastify
    linkText: Adapters
  # Row 2 — the RPC core
  - icon: 🧩
    title: Router, procedures, hooks
    details: Declare units of procedures with access control, Standard Schema validators, timeouts and queues — and named lifecycle hooks at router, unit and procedure level instead of <code>(ctx, next)</code> middleware.
    link: /guide/router
    linkText: Write a router
  - icon: 🔔
    title: Subscriptions that resume
    details: An async generator per feed, with real backpressure. Track values with an event id and a reconnecting client replays exactly what it missed — on any instance, when the feed is a broker log.
    link: /guide/subscriptions
    linkText: Push a feed
  - icon: 🧵
    title: Bytes as bytes
    details: A <code>Uint8Array</code> in arguments, results and events travels as bytes, not as JSON digits; binary streams interleave uploads and downloads with backpressure that reaches all the way into TCP. 1 GiB streams at flat memory.
    link: /guide/streams
    linkText: Move bytes
  - icon: 🧭
    title: Real REST, declared on the procedure
    details: <code>http&colon; { method, path, status }</code> turns a procedure into a proper endpoint&colon; path params, /vN version paths, fastify-shaped schemas, and an OpenAPI document from <code>wrpc types --openapi</code>.
    link: /guide/rest
    linkText: Declare an endpoint
  # Row 3 — scale and security
  - icon: 🛰️
    title: Rooms and cluster in one
    details: Join, leave and broadcast in one process; add any pub/sub — or your broker's backplane — and a room spans every instance, with presence read from a local map, <code>fetchClients</code>, addressed commands and node-to-node asks.
    link: /guide/cluster
    linkText: Go multi-instance
  - icon: 🔐
    title: Sessions and auth, batteries included
    details: Cookie sessions by default; bearer and payload token carriers, client token stores and an authenticate/refresh lifecycle that re-presents the credential before every reconnect restore.
    link: /guide/sessions
    linkText: Wire up auth
  - icon: 🛡️
    title: Encryption where TLS ends
    details: Sealed backplane, cluster and broker messages under a rotating keyring, a session store that holds nothing readable, Noise sessions on WebSocket and WebTransport, HPKE per HTTP request, end-to-end helpers — platform crypto only. Experimental.
    link: /guide/encryption
    linkText: Seal what matters
  - icon: 🗜️
    title: Compression, chosen per wire
    details: Off by default, because its cost is paid on every frame. Turn it on where bytes are dear — deflate, Brotli or zstd, negotiated as a preference list on every transport and the backplane, with a preset dictionary built from your router.
    link: /guide/compression
    linkText: Pick a codec
  # Row 4 — platform and DX
  - icon: 📭
    title: Zero dependencies
    details: No runtime dependencies at all — no <code>dependencies</code> field, no peers, no optionals. Redis, Kafka, uWebSockets.js, fastify, the WebRTC and HTTP/3 stacks, TanStack and OpenTelemetry are injected by you, never installed by wRPC.
    link: /guide/why
    linkText: Why it matters
  - icon: 🧠
    title: Types without a build step
    details: Declare the contract once and connect&lt;Api&gt;() types every call — server-push events included — or generate it from a running server with the <code>wrpc types</code> CLI. No TypeScript at runtime, ever.
    link: /guide/typed-client
    linkText: Type the client
  - icon: 🌍
    title: Node.js and the browser
    details: One protocol implementation, under 28 KB min+gzip in a browser bundle with no Node builtins — a budget CI enforces — plus one socket for every tab through a Service Worker or a SharedWorker, and TanStack Query bindings at ~1 KB.
    link: /guide/client
    linkText: Client guide
  - icon: 🔁
    title: Upgrade in any order
    details: The wire protocol is a versioned reference. Revision 2 is negotiated per connection, so a 2.x client or server still speaks 1.0 to a 1.0 peer — and the published 1.0 is tested against this one, both ways.
    link: /reference/protocol#versioning
    linkText: Read the protocol
---

## One router, every transport

Every procedure on a wRPC server answers on every carrier below, with the
same rooms and sessions behind it — a browser on a WebSocket or on HTTP/3, a page behind a proxy that only
speaks HTTP, another browser over WebRTC, another service through Kafka. The
handler never knows which; the client picks per connection, with a fallback
list where it wants one, and an app can [talk to several
backends](/guide/multiple-backends) at once.

<!-- Mirrors the table in docs/guide/client.md#transports:
     tests/package/consistency.test.js keeps the shared columns equal. -->

| Transport | Reach for it when | Calls | Events & subscriptions | Binary streams | Compression | Encryption |
| --- | --- | --- | --- | --- | --- | --- |
| [WebSocket](/guide/client#transports) | the default — full duplex, from any browser or Node | ✅ | ✅ | ✅ | ✅ | Noise |
| [HTTP & REST](/guide/rest) | stateless calls, `curl`, partners, caches | ✅ | ❌ | ❌ | ✅ | HPKE |
| [Server-Sent Events](/guide/sse) | no upgrade path — serverless, strict proxies | ✅ | ✅ | ❌ | ✅ | HPKE |
| [WebTransport](/guide/wt) <Badge type="warning" text="exp." /> | HTTP/3 — streams without head-of-line blocking, datagrams | ✅ | ✅ | ✅ | ✅ | Noise |
| [WebRTC](/guide/webrtc) | browser to browser, no server in the data path | ✅ | ✅ | ✅ | ✅ | DTLS |
| [Worker port](/guide/client#workers) | one connection shared by every tab | ✅ | ✅ | ✅ | — | — |
| [Broker, stateless](/guide/brokers/rpc) <Badge type="warning" text="exp." /> | service to service — any instance answers | ✅ | ❌ | ❌ | ✅ | sealed |
| [Broker, session](/guide/brokers/rpc) <Badge type="warning" text="exp." /> | a long-lived conversation between services | ✅ | ✅ | ✅ | ✅ | sealed |

Compression and encryption are off until you turn them on; encryption here is
the layer above TLS, never in place of it. The full matrix — how each
transport is chosen, and which carry bytes inside a packet — is in
[Client › Transports](/guide/client#transports).

## Fast where it counts

wRPC's whole call path — router, validation, access check, correlation —
against the frameworks that do the same job, one machine, over a WebSocket
(gRPC over HTTP/2). Switch the metric: wRPC leads with calls in flight and at
10 KB, and is level with socket.io one small call at a time. Over plain HTTP,
fastify answers about a tenth more requests than wRPC does, and express about
a fifth fewer — [Performance](/guide/performance) has that too, with raw
sockets, every transport, and how to reproduce each number.

<BenchChart set="rpc" pick="wrpc,wrpc-batch,socket.io,trpc,grpc" title="RPC frameworks, calls per second" />
