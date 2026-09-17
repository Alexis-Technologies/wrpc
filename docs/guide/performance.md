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

## Against other stacks

`bench/rpc-comparison.js` puts wrpc's **full RPC path** (router, validation,
context, callback correlation) next to raw WebSocket echoes with no RPC layer
at all, and next to two frameworks that do the same job wrpc does. Three
wrpc rows: the default (own engine, one frame per call), the same RPC path
over the [uws engine](./adapters/uws), and the own engine with client
[batching](./client#batching) on.

| Stack | small payload | 10 KB payload | small ×64 in flight |
| --- | ---: | ---: | ---: |
| **wrpc** — own WS + RPC dispatch | 24,607 | **12,408** | 110,257 |
| **wrpc** — uws engine + RPC dispatch | 24,143 | **12,979** | 117,567 |
| **wrpc** — own WS, `batch: true` | 25,707 | 13,432 | 192,573 |
| `ws` — raw echo, no RPC | 34,191 | 11,540 | 104,971 |
| uWebSockets.js — raw echo, no RPC | 29,786 | 11,855 | 162,495 |
| `@fastify/websocket` — raw echo, no RPC | 35,194 | 12,701 | 105,817 |
| fastify-uws — raw echo, no RPC | 30,099 | 8,270 | 152,956 |
| socket.io — framework RPC via `emitWithAck` | 23,535 | 10,598 | 95,497 |
| tRPC — framework RPC over `wsLink` | 636¹ | 582¹ | 22,010 |

ops/sec, higher is better; one run, one machine, so read rows against each
other rather than against an earlier table.

¹ tRPC's sequential number is a **client-side flush-timer artifact, not
throughput**: its per-call latency measures a near-constant ~1.5 ms
(median 1.53 ms, p10–p90 spread 1.48–1.69 — a fixed delay, not processing),
so one awaited call per timer tick caps the sequential rate while the
pipelined column shows what the same stack does when many calls share a
flush. Compare tRPC on the batched column, where its per-call machinery
amortizes.

Read it honestly:

- **A raw echo is the ceiling, not a competitor.** uWebSockets.js at 1.21×
  on small payloads is what a socket costs with no router, no access check
  and no correlation on top. wrpc paying about a fifth for all of that
  still clears socket.io; this run's `ws` and `@fastify/websocket` echoes
  happen to land ahead of uWebSockets.js's own raw echo too, which is noise
  at this payload size, not a real ordering — read the whole small-payload
  column as one cluster, not a ladder.
- **On real payloads the gap narrows.** At 10 KB, wrpc's own engine (12,408)
  is within 2% of the fastest raw echo in the table (`@fastify/websocket`,
  12,701), and switching to the uws engine (12,979) or turning on batching
  (13,432) puts it back in front of every raw echo — the send path avoids
  re-encoding and re-copying what it already has.
- **Batching closes more of the pipelined gap than the engine does.** With
  64 calls in flight wrpc over its own engine runs at 110K and over the uws
  engine at 118K, against 162K for a raw uws echo: swapping the JavaScript
  WebSocket engine for the native one buys ~7% this run, and the rest of
  the distance to the raw echo is the RPC layer plus the client. Turning on
  client batching — 64 calls sharing frames — buys far more, ~75%, taking
  the own engine to 193K and past every raw echo in the table, uws's
  included. Both are one option away; neither is the default, because a
  single call should not wait for a flush.
- **Against frameworks doing the same job**, wrpc is ahead of socket.io on
  every payload shape this run. The tRPC rows read through footnote ¹: its
  sequential number is a client flush-timer artifact, and the honest
  comparison is the pipelined column — where wrpc is still ~5.0× ahead.
  tRPC's type story is excellent and unaffected by any of this.

## Fan-out

`bench/send-path.js` — one room, N members, 512-byte payload:

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

## The receive path

`bench/parser-throughput.js`:

| Scenario | MB/s | msgs/sec |
| --- | ---: | ---: |
| 16 MiB binary message, 16 KiB segments | 3,093 | 193 |
| 200 B text frames ×50K, 64 KiB segments | 671 | 3,516,734 |

The large-message number is what `SegmentQueue` exists for: buffering
fragmented reads with O(n) total work instead of re-concatenating a growing
buffer on every segment. See [wire format](../reference/wire-format#receive-path-performance).

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

## Across instances

The paper's numbers were one machine, one process. `bench/cluster-nodes.js`
is the multi-node stand: N wrpc processes over one Redis (backplane and
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

## Memory, under load

`pnpm test:perf` streams 1 GiB through a [binary stream](./streams) and fails
if RSS grows with it. It is deliberately not in CI — it is slow and
memory-sensitive — so run it by hand after touching the stream path.
