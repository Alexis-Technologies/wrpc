# Performance

Every number on this page comes from a script in `bench/`, and `pnpm bench`
runs all of them. They were measured on one developer machine (Node v24.14.1,
Apple Silicon) — treat them as **shapes and ratios**, not as a spec sheet, and
re-run them on hardware you care about.

```bash
pnpm bench
```

`bench/run-all.js` discovers every top-level file in `bench/`, so a new
measurement is a new file and nothing else. Nothing in CI runs it: there is no
stored baseline, and a benchmark that gates a pipeline on a noisy laptop number
is worse than no benchmark.

## The short version

Skip to whichever section you actually need; this is the two-minute read
for everyone else — what wins outright, what's behind and expected to be,
and which knobs are worth turning for your traffic shape.

**Wins outright, no configuration needed:**

- **Broadcasting.** A room's update is serialized, framed and — once a peer
  has negotiated it — compressed **once per emit**, not once per member.
  That's why fan-out throughput climbs with room size instead of collapsing
  under it, and why compressed fan-out is ~34× what a naive
  per-recipient implementation manages — see [Fan-out](#fan-out).
- **Payloads that aren't toy-sized.** At 10 KB, wRPC is at or ahead of
  every raw WebSocket library in the table below — the send path never
  re-encodes or re-copies what it already built.
- **Surviving an instance loss.** With a shared session store and any
  pub/sub as a backplane, 50 clients whose instance just died reconnect,
  re-authenticate and rejoin their room within single-digit milliseconds —
  no sticky load balancer, no manual failover. See
  [Across instances](#across-instances).
- **A call that does not queue behind an upload.** From Chrome, a small call
  made while uploads saturate the connection answers in 9 ms over
  WebTransport against 46 ms over a WebSocket — each binary stream has a QUIC
  stream of its own. And on every transport the stack sets the ceiling, not
  wRPC: over WebTransport and WebRTC in Node, wRPC's calls run at 85–100% of a
  raw echo over the same stack. See [Across transports](#across-transports).
- **Against RPC frameworks.** With 64 calls in flight wRPC answers 117K calls
  a second — 1.15× socket.io, 5.0× tRPC, 5.6× gRPC through `@grpc/grpc-js` —
  and 201K with client batching on. See [Against other stacks](#against-other-stacks).

**Behind, and expected to be:**

- **A single tiny call against a raw echo.** A socket with no router, no
  access check and no correlation ID was always going to win that race:
  the raw echoes answer 1.25–1.48× as many. Paying that for real request
  handling is the trade for getting one, not a defect — see
  [Against other stacks](#against-other-stacks).
- **Plain HTTP against fastify.** wRPC's HTTP paths answer 0.88–0.90× of
  fastify's requests on a JSON echo, and 1.2× express's. Every request pays for
  the RPC path and for wRPC's default headers. See [Over HTTP](#over-http).
- **One call at a time in the browser.** The client's deadline scheduler
  earns its keep once many calls are in flight; a lone awaited call
  doesn't exercise it either way.
- **WebTransport and WebRTC in Node.** Their stacks are native bindings
  into userspace QUIC and SCTP, and on loopback they move a fraction of what
  Node's WebSocket does — use them for what only they do, not for
  throughput. See [Across transports](#across-transports).

**Leave these as they are by default:**

| Setting | Default | Turn it on when |
| --- | --- | --- |
| shared-frame broadcast path | always on | never — it's the fix for the fan-out cost, not a tradeoff to opt into |
| `perMessageDeflate` | off | the peer is bandwidth-bound (mobile, metered, cross-region) and frames are past a kilobyte — [Compression is off by default](#compression-is-off-by-default) has the numbers. Off because a default would spend CPU on every frame of every peer to save bytes only some of them need |
| `contextTakeover` | off | you repeatedly broadcast the **same shape** (price ticks, presence deltas) to bandwidth-constrained clients — ~10× the compression ratio, for ~160 KiB of zlib state per direction per connection |
| `async: { threshold }` | off | large broadcasts (≥256 KB payloads) would otherwise stall the event loop for the length of the burst |
| client `batch: true` | off | you fire many RPC calls back to back — bulk hydration, chart backfills — where it buys 72% more pipelined throughput below (an engine swap buys nothing on that path), at the cost of a single call waiting on a flush |
| [uws engine](./adapters/uws) | Node's own engine | you're CPU-bound on the WS engine at very high concurrency and can take a native dependency |

The rest of this page is where every one of those numbers comes from.

## Against other stacks

`bench/rpc-comparison.js` puts wRPC's **full RPC path** (router, validation,
context, callback correlation) next to raw WebSocket echoes with no RPC layer
at all, and next to three frameworks that do the same job wRPC does:
socket.io and tRPC over a WebSocket, and gRPC — `@grpc/grpc-js`, unary over
HTTP/2, the service loaded from a `.proto` at runtime. Three wRPC rows: the
default (own engine, one frame per call), the same RPC path over the
[uws engine](./adapters/uws), and the own engine with client
[batching](./client#batching) on.

<BenchChart set="rpc" />

| Stack | small payload | 10 KB payload | small ×64 in flight |
| --- | ---: | ---: | ---: |
| **wRPC** — own WS + RPC dispatch | 25,213 | **13,986** | 116,704 |
| **wRPC** — uws engine + RPC dispatch | 26,042 | **13,801** | 116,174 |
| **wRPC** — own WS, `batch: true` | 27,078 | **14,027** | **201,075** |
| `ws` — raw echo, no RPC | 34,987 | 12,206 | 112,487 |
| uWebSockets.js — raw echo, no RPC | 31,567 | 12,226 | 160,872 |
| `@fastify/websocket` — raw echo, no RPC | 37,334 | 12,746 | 112,133 |
| fastify-uws — raw echo, no RPC | 31,572 | 8,702 | 160,046 |
| socket.io — framework RPC via `emitWithAck` | 26,213 | 10,862 | 101,455 |
| tRPC — framework RPC over `wsLink` | 628¹ | 596¹ | 23,310 |
| gRPC — `@grpc/grpc-js` unary over HTTP/2 | 8,127 | 7,844² | 20,921 |

ops/sec, higher is better; one run, one machine (Node v24.14.1, Apple M3
Max), so read rows against each other rather than against an earlier table.

¹ tRPC's sequential number is a **client-side flush-timer artifact, not
throughput**: its per-call latency measures a near-constant ~1.5 ms
(median 1.53 ms, p10–p90 spread 1.48–1.69 — a fixed delay, not processing),
so one awaited call per timer tick caps the sequential rate while the
pipelined column shows what the same stack does when many calls share a
flush. Compare tRPC on the batched column, where its per-call machinery
amortizes.

² gRPC encodes protobuf, not JSON: its 10 KB payload is a string field
copied as bytes, which is why that column barely moves from the small one.
Every other row pays JSON for it.

Read it honestly:

- **A raw echo is the ceiling, not a competitor.** The raw echoes answer
  1.25–1.48× as many single small calls as wRPC's default row: that is what a
  socket costs with no router, no access check and no correlation on top.
  Read the small-payload column as one cluster, not a ladder — the three
  wRPC rows (25.2K–27.1K) and socket.io (26.2K) sit within 8% of each other
  this run, socket.io 4% ahead of the default row and behind the batched one.
- **On real payloads wRPC moves ahead.** At 10 KB every wRPC row (13.8K–14.0K)
  is ahead of every raw echo — the fastest, `@fastify/websocket`, at 12.7K —
  and 29% ahead of socket.io: the send path never re-encodes or re-copies
  what it already built.
- **Batching, not the engine, closes the pipelined gap.** With 64 calls in
  flight wRPC runs at 117K on its own engine and 116K on the uws engine,
  against 161K for a raw uws echo: swapping the JavaScript WebSocket engine
  for the native one buys nothing on this path this run, and the distance to
  the raw echo is the RPC layer plus the client. Client batching — 64 calls
  sharing frames — buys 72%, to 201K, past every raw echo in the table. Both
  are one option away; neither is the default, because a single call should
  not wait for a flush.
- **Against frameworks doing the same job**, wRPC is 15% ahead of socket.io
  with 64 calls in flight and 29% at 10 KB, and level with it on single small
  calls. tRPC reads through footnote ¹: on the pipelined column wRPC is 5.0×
  ahead, and tRPC's type story is excellent and unaffected by any of this.
  gRPC through `@grpc/grpc-js` answers 8.1K small calls a second and 20.9K
  with 64 in flight — wRPC is 3.1× and 5.6× that — and closes most of the gap
  at 10 KB (footnote ²). Connect and gRPC's other implementations were not
  measured.

Step back from the individual rows and the question this table actually
answers is: is the RPC layer the bottleneck? On every payload size
measured, no. Tens of thousands of calls per second, locally, with no
network in the loop, is a number a deployed system will rarely see even a
fraction of — real traffic is bounded by client concurrency, database
round-trips and network RTT long before a request queue backs up waiting
on wRPC's own dispatch. Read this table as proof the floor is high enough
to stop worrying about, not as a number to chase in production.

## Over HTTP

`bench/http-comparison.js` loads one JSON echo per stack the way
[fastify/benchmarks](https://github.com/fastify/benchmarks) loads Node
frameworks — autocannon, 100 connections, 10 pipelined requests each, 10 s,
every server in a process of its own — and puts wRPC's two HTTP paths next to
fastify, express, tRPC's standalone adapter and `node:http` with no framework
at all:

```bash
node bench/http-comparison.js   # WRPC_BENCH_DURATION=30 for longer runs
```

<BenchChart set="http" />

| Stack | requests/s | mean latency | p99 latency |
| --- | ---: | ---: | ---: |
| **wRPC** — declared REST route | 62,236 | 15.5 ms | 29 ms |
| **wRPC** — packet mode | 60,927 | 15.9 ms | 32 ms |
| fastify | 69,187 | 13.9 ms | 30 ms |
| express | 50,567 | 19.3 ms | 36 ms |
| tRPC — standalone HTTP adapter | 16,523 | 59.9 ms | 85 ms |
| `node:http` — no framework | 104,404 | 8.9 ms | 16 ms |

One run, one machine (Node v24.14.1, Apple M3 Max), loopback — the load
generator shares the machine with the server, as it does in
fastify/benchmarks.

Read it honestly:

- **Plain HTTP is not where wRPC is fastest.** Its two HTTP paths answer
  about 61,000 requests a second: 0.88–0.90× fastify, 1.2× express, 3.7×
  tRPC's adapter. Each request pays what a WebSocket call pays once per
  connection — parsing, routing, a server-side client with its id, its
  metadata and its session restore — on top of the RPC path itself
  (dispatcher, access check, hooks, a `Context`).
- **The default headers are a tenth of the ceiling.** Every answer carries
  CORS, `strict-transport-security`, `x-content-type-options` and
  `wrpc-version` — 338 bytes a bare fastify or express route does not send.
  Building them is ~20 ns (`bench/cors-headers.js`); `node:http` validating
  and writing them on every answer costs it about a tenth of its requests
  (`bench/http-headers.js`, the ceiling row answering with and without them).
  They are security defaults and stay on.
- **What the core costs on its own.** `bench/http-call.js` drives
  `handleHttpCall` with a stub in place of the socket: about 320,000 requests
  a second on either path, so ~3 µs of a request is wRPC's routing, client and
  dispatch with node's HTTP parser, header writing and socket taken out.
- **The two paths cost the same.** A declared REST route (plain JSON in and
  out) and packet mode (the call packet a WebSocket carries, as one POST)
  are within 3% of each other: the REST bridge's trie is not where the time
  goes.
- **Which to reach for.** For traffic that is mostly RPC, a persistent
  transport is the better carrier — the WebSocket rows above. For an
  HTTP-first API, the [fastify adapter](./adapters/fastify) serves wRPC's
  procedures from the fastify instance that already serves the rest of it.

## Fan-out

`bench/send-path.js` — one room, N members, 512-byte payload:

<BenchChart set="fanout" />

| Scenario | rate | throughput |
| --- | ---: | ---: |
| `sendText` 200 B | 6,466,955/sec | 1258 MB/s |
| `sendText` 4 KB | 1,853,364/sec | 7247 MB/s |
| `sendText` 64 KB | 63,693/sec | 3981 MB/s |
| room fan-out ×50 | 539,105/sec | 14910 MB/s |
| room fan-out ×200 | 208,685/sec | 23086 MB/s |
| room fan-out ×50 **+ deflate** | 102,914/sec | 339 MB/s |
| room fan-out ×200 **+ deflate** | 66,083/sec | 870 MB/s |
| room fan-out ×50 + deflate, windows 10/15 | 53,152/sec | 175 MB/s |

A broadcast is serialized **once**, and on the built-in engine it is also
framed once: `Broadcast.emit` hands every recipient one shared message, the
first `Connection` to write it encodes the frame into the message's cache
slot, and every later recipient writes that same buffer
(`Connection.sendPrepared`). The per-recipient cost is a socket write, which
is why throughput keeps climbing with the member count.

::: tip permessage-deflate is compressed once per emit, not once per member
The compressed bytes depend only on the payload and the peer's negotiated
window (both directions are pinned to no context takeover — see
[wire format](../reference/wire-format#permessage-deflate)), so the shared
message caches one deflated frame **per distinct window** and a room of 200
costs one `deflateRaw` for the whole fan-out. A fleet that negotiates two
window sizes pays two (the `windows 10/15` row). The remaining gap to the
uncompressed rows is that one deflate; before the shared frame it was
**one per recipient** and fan-out ×50 ran at 2,806/sec.
:::

What that means for a real room: a 200-member broadcast with compression on
costs one `deflateRaw` call and 200 socket writes, and a single instance
clears that over 60,000 times a second. No legitimate application pushes
updates to one room at that rate — a live chat, a presence feed, a stock
ticker are all in the tens-per-second range at most — so in a deployed
system the ceiling that matters moves to client-side rendering and network
egress bandwidth, not this code path. The number is headroom, not a target.

Unicast sends are one contiguous buffer up to 16 KiB — header and payload
written together, the text utf8-encoded straight from a scratch buffer — and
a separate header + payload write above it, where the copy would cost more
than the write it saves (the 64 KB row). On the server every write of one
event-loop turn is corked and flushed on the next tick, so a batch of N
answers leaves in one `writev`.

Compression is still worth choosing per peer and per message: enable it for
bandwidth-bound clients with the handshake `filter`, and skip it for a
message that is already compressed or latency-critical with
`emit(name, data, { compress: false })` — see [rooms](./rooms#compression).

## In the browser

`pnpm bench:browser` runs `bench/browser/calls.js` in the installed Chrome
(through `playwright-core`, a devDependency — no browser download): the
client's call path, id to settle, over an in-page echo on a `MessageChannel`
so no network and no server code is in the number.

| Scenario (Chrome 152) | ops/sec |
| --- | ---: |
| call echo, 1 in flight | 100,336 |
| call echo, 64 in flight | 121,487 |
| call echo, 1024 in flight | 114,242 |
| call echo, 64 in flight, per-call `timeout` | 121,308 |

The pipelined rows are what the client's deadline scheduler bought: one
bucketed timer per client instead of a `setTimeout` and three closures per
call took 64 in flight from 110,442 to 121,487 and 1024 from 102,562 to
114,242, with a single awaited call unchanged. `WRPC_ROOT=<checkout>` points
the runner at another checkout for a before/after.

Read the 1024-in-flight row as a stress test, not a target: a real UI
rarely has more than a handful of calls genuinely in flight from one user
action. What the scheduler actually buys day to day is that a burst of
calls — a dashboard hydrating a dozen panels on load — produces one timer
and one deque entry each, not a dozen independent `setTimeout`s and their
closures; the throughput numbers are evidence that the design doesn't cost
anything, not the reason it exists.

## Across transports {#across-transports}

Everything above runs over a WebSocket. The same `Server` and the same client
also run over [WebTransport](./wt) and a [WebRTC](./webrtc) data channel, and
both of those have a **native stack** under them — Node has neither in its
standard library — so the first question is whose cost a number is.
`bench/rpc-comparison.js` answers it the way it does for the WebSocket: next
to wRPC over each stack, a raw echo over the same stack with no RPC layer.
`bench/transports.js` adds what an echo cannot show — opening, a stream, a
call under load — and `pnpm bench:browser transports` does it all from
Chrome, over the browser's own WebSocket, WebTransport and WebRTC.

```bash
node scripts/wt-cert.js certs   # the 14-day certificate WebTransport needs
WRPC_WT=fails WRPC_RTC=node-datachannel node bench/rpc-comparison.js
WRPC_WT=fails WRPC_RTC=node-datachannel node bench/transports.js
WRPC_WT=fails pnpm bench:browser transports
```

The stacks are the ones the integration tests use:
[`@fails-components/webtransport`](https://github.com/fails-components/webtransport)
— Google's libquiche behind a native binding — for the HTTP/3 host and the
Node client, and [`node-datachannel`](https://github.com/murat-dogan/node-datachannel)
— libdatachannel — for both peers of a loopback pair. Without the variables
those rows are skipped with the reason, and `pnpm bench` stays
self-contained. Everything here is **loopback**: no packet loss and no round
trip to speak of, so what QUIC does about loss, and what a 0-RTT handshake
saves on a real network, are not in these numbers.

### In Node

Calls — the same run as [Against other stacks](#against-other-stacks)
(ops/sec, higher is better):

<BenchChart set="transport-calls" />

| Stack | small payload | 10 KB payload | small ×64 in flight |
| --- | ---: | ---: | ---: |
| **wRPC** — own WebSocket | 25,213 | 13,986 | 116,704 |
| **wRPC** — WebTransport (libquiche) | 10,873 | 2,351 | 17,054 |
| WebTransport — raw echo, no RPC | 12,810 | 2,396 | 18,646 |
| **wRPC** — WebRTC data channel (libdatachannel) | 7,694 | 1,417 | 13,666 |
| WebRTC data channel — raw echo, no RPC | 8,114 | 1,415 | 13,782 |

Opening, streaming, and a call under load (`bench/transports.js`, one run):

<BenchChart set="transport-node" />

| Transport | open + load + first call, p50 | 16 MiB up + 16 MiB down | the same bytes, bare stack | small call, idle p50 | small call under upload load, p50 / p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| WebSocket | 1.1 ms | 575 MiB/s | 545 MiB/s | 0.09 ms | 15.2 / 30.0 ms |
| WebTransport | 2.6 ms | 44.5 MiB/s | 52.9 MiB/s | 0.09 ms | 15.3 / 18.0 ms |
| WebRTC | 507 ms¹ | 35.2 MiB/s | 38.9 MiB/s | 0.14 ms | 84 / 154 ms |

¹ 506 ms of it is the pair's own offer/answer, ICE and DTLS in
libdatachannel, timed alone; wRPC's share of opening is about a millisecond.
In Chrome the same pair opens in a few milliseconds (below).

Read it honestly:

- **On every transport, the stack sets the ceiling — not wRPC.** Over the
  same libquiche session, wRPC's calls run at 85% of a raw echo's on small
  payloads, 98% at 10 KB and 91% with 64 in flight; over the same data
  channel at 95%, 100% and 99%. A wRPC stream moves 84% of what the bare WebTransport stream does,
  90% of the bare data channel, and 105% of the bare `ws` package.
- **In Node, both native stacks are far behind the WebSocket.** A third to a
  half of the small calls, a sixth to a tenth of the 10 KB ones, a thirteenth to a
  sixteenth of the stream throughput: a binding into a userspace QUIC or SCTP stack, against
  TCP on loopback, where the kernel is at its best. Reach for them in Node
  for what only they do — a peer-to-peer link, datagrams, a client whose
  only way in is HTTP/3 — not for throughput.
- **Under load, the Node rows do not isolate head-of-line blocking.** A call
  under a saturating upload has a lower p99 on WebTransport (18 ms) than on
  the WebSocket (30 ms), but the WebSocket is moving about ten times the
  bytes meanwhile. The browser rows below compare like with like.

### In Chrome

`pnpm bench:browser transports` — Chrome 154 against the same Node `Server`
(the WebRTC row is a pair inside the page, with a `PeerHost` at the far end):

<BenchChart set="transport-chrome" />

| Transport | open + first call | small | 10 KB | small ×64 | 16 MiB up + down | small call under upload load, p50 / p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| WebSocket | 12.5 ms | 10,789 | 6,009 | 44,769 | 330 MiB/s | 46.3 / 54.5 ms |
| WebTransport | 12.0 ms | 8,848 | 3,202 | 23,806 | 79 MiB/s | **9.0 / 11.4 ms** |
| WebRTC² | 5.4 ms | 5,353 | 1,020 | 10,400 | 14 MiB/s | 40 / 53 ms |

calls in ops/sec; ² both peers in one page, so one renderer runs both
ends' SCTP and DTLS, and the far end is a `PeerHost` in that page rather than
the Node server — a lower bound on a real link, not a measurement of one.

- **A call does not wait behind an upload on WebTransport.** While uploads
  keep the connection busy, a small call answers in 9 ms at the median over
  WebTransport and in 46 ms over the WebSocket — five times faster, with no
  packet loss involved. Over the WebSocket the call is queued in one TCP
  stream behind whatever the upload has buffered; over WebTransport every
  binary stream has a [QUIC stream of its own](./wt#streams-without-head-of-line-blocking)
  and the call waits behind none of them. The WebSocket moves the upload
  about four times faster; if what you need is a lot of bytes on one quiet
  connection, that is the one to use.
- **Opening is not faster on loopback.** A WebTransport session and a
  WebSocket both open in about 12 ms here; the round trips QUIC saves are
  network round trips, and loopback has none.
- **Calls are cheaper over the WebSocket** — 1.2× on small payloads and
  1.9× at 10 KB in Chrome; 2.3× and 5.9× in Node, where WebTransport's stack
  is a binding rather than the browser's own.

## Compression is off by default

Nothing in wRPC compresses anything unless you ask — on any transport; the
[compression guide](./compression) is the map of every knob and of the
router dictionary they can share. On the WebSocket: `perMessageDeflate`
on the built-in engine and `compression` on the [uws engine](./adapters/uws)
are both off, and the `contextTakeover` / `async` rows above are refinements
of a knob that has to be on first. That is a choice, not an oversight —
wRPC's first commitment is the cost per frame, and the same
`bench/deflate-context.js` that measures the modes below prices the knob
itself:

| One-shot deflate (the default mode, once enabled) | rate | ratio |
| --- | ---: | ---: |
| repeated 90 B JSON event | 108,461/sec | 1.1× |
| 4 KB JSON | 93,419/sec | 3.1× |
| 32 KB JSON | 17,744/sec | 3.9× |

Read the first row as the reason for the default: a small event costs a
deflate call (~9 µs here) to lose a tenth of its bytes. The other two are the
reason to turn it on for the right peer — a 4 KB call frame is a third of
its size on the wire, and every frame above the 1 KiB `threshold` pays that
once. Below the threshold nothing is compressed even when the option is on.

Turn it on where the bytes are worth the CPU, and only there:

```js
// The batteries-included server — `ws` is forwarded to the engine.
new Server({
  router,
  ws: {
    perMessageDeflate: {
      threshold: 1024,
      // Per peer: a browser on a slow link, not a service next door.
      filter: (req) => req.headers['x-forwarded-proto'] !== undefined,
    },
  },
});

// The engine directly, or uws with its own compressor constant.
createNodeEngine({ perMessageDeflate: true });
createUwsEngine({ uws, compression: uws.SHARED_COMPRESSOR });
```

Per message, `{ compress: false }` on an emit skips a frame that is already
compressed or latency-critical — see [rooms](./rooms#compression). The full
option set is on the [wire format](../reference/wire-format#permessage-deflate)
page.

Two honest limits of what the knob covers:

- **It compresses the WebSocket only.** The HTTP side has its own knobs,
  off too: [`http.compression`](./server#compression) for packet-mode and
  REST answers, [`sse.compression`](./sse#compression) for the event stream
  (`bench/http-compression.js`: a 1.6 KB answer 5.5× at 77K/sec, a repeated
  SSE tick 7.8×), and [`compression`](./wt#compression) on WebTransport and
  [WebRTC](./webrtc#compression) — per message, negotiated, off unless both
  ends turn it on (`bench/message-compression.js`: a 1.4 KB callback 6.3× at
  105K/sec) — and on the [broker binding](./brokers/rpc#compression) and the
  [backplane envelopes](./scaling#compression), both Node↔Node.
- **A Node client never compresses what it sends — on its own.** Node's
  built-in `WebSocket` offers `permessage-deflate` on the handshake but only
  ever inflates: every frame it sends leaves with `RSV1` clear, whatever
  the server negotiated. Server→client frames compress as usual; a browser
  compresses both directions itself. The way out is the client's own
  [`compression`](./server#node-client-frames) option against a server
  that accepts it: per-message frames the server inflates, negotiated over
  the first ping/pong, off on both ends by default.

## Compression modes

`bench/deflate-context.js` — the stateless default, context takeover and the
async threadpool path, on the traffic each is for:

| Scenario | rate | ratio / loop delay |
| --- | ---: | --- |
| repeated JSON event, one-shot (default) | 108,461/sec | 1.1× |
| repeated JSON event, **context takeover** | 49,898/sec | **10.6×** |
| 4 KB JSON, one-shot / async | 93,419 / 31,690 | 3.1× |
| 32 KB JSON, one-shot / async | 17,744 / 12,650 | 3.9× |
| 256 KB JSON, one-shot / async | 1,909 / 1,818 | 3.8× |
| 252 KB burst ×100, one-shot | 317/sec | loop blocked **316 ms** |
| 252 KB burst ×100, **async** | 1,253/sec | loop delay 2 ms |

Context takeover is the ratio knob: a repeated event shape compresses ten
times better against its own history, for ~160 KiB of zlib state per
direction per connection and an asynchronous, queued write path. Async is
the event-loop knob: below its threshold the threadpool hand-off costs
throughput, at 256 KB it is free, and on a burst — a fan-out's worth of
large frames issued in one turn — it is the difference between a loop
stalled for a third of a second and one that never notices. Both are
opt-in; see [wire format](../reference/wire-format#permessage-deflate).

Put a scale on the ratio: the stateless default already compresses generic
JSON about as well as deflate ever will without history to lean on — that
1.1× is normal, not a bug. Context takeover's 10.6× only shows up because
the traffic in that row is the same shape repeated (a price tick, a
presence delta) — each message looks almost like the last one, and that's
exactly what a persistent zlib window is good at. Spend the ~160 KiB per
connection on it when the shape really does repeat and the client is
bandwidth-constrained (mobile, metered); spend it on arbitrary one-off
payloads and you've paid rent on memory for a discount that never arrives.

The event-loop numbers are the more universal case: any server that
broadcasts large payloads to many recipients in one turn — that's what a
fan-out *is* — will eventually issue a burst of compressions in one tick.
Synchronous deflate on a 252 KB burst held the loop for a third of a
second, during which every other connection on the process goes
unserved — not slow, unresponsive. `async: { threshold }` is the
difference between that and a loop that never notices, and it's the one
knob on this page worth defaulting toward once your payloads regularly
cross the threshold, rather than waiting for a stall to prove it.

## The receive path

`bench/parser-throughput.js`:

| Scenario | MB/s | msgs/sec |
| --- | ---: | ---: |
| 16 MiB binary message, 16 KiB segments | 3,093 | 193 |
| 200 B text frames ×50K, 64 KiB segments | 671 | 3,516,734 |

The large-message number is what `SegmentQueue` exists for: buffering
fragmented reads with O(n) total work instead of re-concatenating a growing
buffer on every segment. See [wire format](../reference/wire-format#receive-path-performance).

3 GB/s of reassembly throughput is well past what a single gigabit network
interface can even deliver, and comfortably past most 10-gigabit links too
— a fragmented multi-megabyte upload will hit the network as its ceiling
long before it hits this code. What the O(n) behavior actually buys is the
absence of a cliff: a naive re-concatenating buffer gets quadratically
slower as a message grows, and the failure mode isn't "a bit slower", it's
"fine at 1 MB, a stall at 100 MB". `SegmentQueue` exists so that graph
stays flat.

## Batching crosses over at 16

`bench/batch-ordering.js` compares two ways of restoring order in a
[batch](./client#batching) — a linear scan versus an id-indexed map:

| batch size | linear scan | id-indexed | winner |
| ---: | ---: | ---: | --- |
| 4 | 5,435,327/s | 4,349,645/s | scan 1.25× |
| 8 | 2,231,887/s | 1,866,820/s | scan 1.20× |
| 16 | 929,481/s | 992,604/s | indexed 1.07× |
| 64 | 95,175/s | 233,888/s | indexed 2.46× |
| 128 | 31,226/s | 125,089/s | indexed 4.01× |

Which is why the client's default `batch.maxSize` is 16: the shape changes
right there. The server's own switch from the scan to the index sits at 12
(`ServerHttpTransport`, for the HTTP batch reply) — between the last size
where the scan still wins and the first where the index does; both numbers
come from this one bench, and re-running it is the way to move either.

Most request patterns never see this trade at all: a UI queues a handful
of calls at once, not dozens, so the linear scan — simpler, and the faster
choice below 16 — is what the common case actually runs. The index earns
its keep specifically when a burst gets genuinely large: bulk imports,
chart backfills, a page hydrating many widgets' data at once. If your
traffic never bursts past a dozen or so calls, this section is trivia, not
a tuning target.

## Cluster operations

`bench/cluster.js`, two instances over a `MemoryBackplane`:

| Operation | rate |
| --- | ---: |
| `cluster.count()` | 66,706,691/sec |
| `cluster.presence()` | 24,135,036/sec |
| join + leave with presence deltas | 440,433/sec |
| room fan-out ×100 emit | 203,231/sec |
| room fan-out ×100 ask, send side | 7,158/sec |

`count()` and `presence()` are local map reads by design — that is the whole
point of [replicating presence](./cluster#presence-replicated-read-locally)
instead of requesting it.

Tens of millions of reads per second is not a number an application will
ever approach — it means these two calls simply leave the "is this fast
enough" conversation entirely. Check room size on every incoming message,
poll presence from an admin dashboard every second, call `count()` inside
a hot loop: none of it registers, because the alternative this replaces —
a network round-trip to ask another process — is the one with a real,
visible cost, and that's the comparison that actually matters here.

## Across instances

The paper's numbers were one machine, one process. `bench/cluster-nodes.js`
is the multi-node stand: N wRPC processes over one Redis (backplane and
session store, `pnpm redis:up`), M clients spread across them, and every
scenario crossing a real broker between real processes:

```bash
pnpm redis:up
REDIS_URL=redis://127.0.0.1:6379 node bench/cluster-nodes.js   # WRPC_NODES, WRPC_CLIENTS to size it
```

| Scenario (4 instances, 200 clients) | result |
| --- | --- |
| cross-instance emit, one at a time, measured at the farthest member | 173/sec — delivery latency p50 3 ms, p99 6 ms |
| presence: 200 concurrent joins, then leaves | count agrees on another instance after 6 ms / 7 ms |
| broadcast `ask` across 4 instances | 259/sec, 200 of 200 answers every time |
| an instance dies, its 50 clients reconnect elsewhere | 50/50 sessions restored with no sticky routing, presence converged in 6 ms, 0 backplane gaps |

The emit row is a latency measurement (one emit, wait for the remotest
client, repeat), not a throughput one — the in-process fan-out numbers
above are the throughput side. The last row is the [affinity
table](./scaling#affinity) made concrete: with a shared session store and a
backplane, an instance can vanish and nothing needs a balancer's help. The
stand found one bug on its first run: a cluster node whose `sync` answer was
lost (at-most-once, again) never asked again and kept a stale presence view
for the life of the process — now it retries after two presence intervals.

Put a human scale on the milliseconds: a browser repaints roughly every
16 ms at 60 fps, so a 3–6 ms cross-instance delivery and a 6–7 ms presence
convergence both land inside a single frame — a user who just joined on
instance B is visible to a client on instance A before the eye could
register two separate updates. That's the concrete version of the
[affinity table](./scaling#affinity)'s claim that ws/wt connections don't
need sticky routing: losing an instance isn't invisible in the sense of
"nothing happens" — 50 clients really do drop and reconnect — it's
invisible in the sense that nothing happens *slowly enough for a person to
notice*.

## Why the code looks the way it does

Two benchmarks exist to justify code shape rather than to advertise a number.

`bench/dispatch.js` measures the packet-type branch three ways. With the branch
cost isolated, a 9-branch `if`/`else if` chain is linear (165 M ops/s at the
first branch, 69 M at the last) and a `switch` is flatter (101 M → 69 M). But
put a real handler call in the loop and `switch` and the chain come out level
(54.5 vs 55.1 M ops/s) while a **table of handler functions loses ~24%**
(41.7 M ops/s) — V8 compiles a string `switch` into comparisons it can inline
through, and cannot inline through a function *value*. `Object.freeze` and
`Map` do not close the gap. So the dispatch branches stay branches.

`bench/emitter.js` measures the event path that every packet crosses:

| Scenario | ops/sec |
| --- | ---: |
| emit fire-and-forget — 1 sync listener | 64,118,679 |
| emit awaited — 0 listeners | 16,173,603 |
| emit awaited — 1 sync listener | 14,519,086 |
| emit awaited — 2 sync listeners | 3,067,063 |

The cliff between one and two listeners is the awaited multi-listener path
allocating a snapshot so a listener may `off()` itself mid-emit. Single-listener
and zero-listener emits are special-cased around it.

## Bundle size is a budget, not a report

```bash
pnpm size
```

Browser-reachable entries carry a **budget**, and exceeding one fails the run —
it is a ratchet, and it runs in CI's lint job. See
[Browser & bundling](./browser#bundle-size) for the table and what is in each
entry.

The main entry's budget is 28 KB min+gzip ([the measured table](./browser#bundle-size)), and that is the *whole* client —
calls, subscriptions, streams, reconnection with backoff, auth hooks — not
a core that then needs a realtime add-on and a query-binding add-on layered
on top of it. It is small enough to arrive in the same round trip as your
app shell, which is the actual reason the budget is a hard CI failure
rather than a suggestion: a slow leak of a few hundred bytes per feature is
invisible in any one PR and very visible three years later.

## Memory, under load

`pnpm test:perf` streams 1 GiB through a [binary stream](./streams) and fails
if RSS grows with it. It is deliberately not in CI — it is slow and
memory-sensitive — so run it by hand after touching the stream path.

The property being guarded is boring on purpose: a stream carrying more
data than fits comfortably in RAM should not accumulate RAM. The failure
mode this catches — a forgotten backpressure check quietly buffering an
entire upload in memory — is the kind of bug that passes every functional
test and then takes a process down under real traffic, weeks after the
change that caused it shipped.
