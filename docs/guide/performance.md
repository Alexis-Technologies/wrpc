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
| **wrpc** — own WS + RPC dispatch | 18,027 | **10,346** | 95,807 |
| **wrpc** — uws engine + RPC dispatch | 19,735 | **11,857** | 112,037 |
| **wrpc** — own WS, `batch: true` | 18,327 | 8,643 | 116,757 |
| `ws` — raw echo, no RPC | 13,734 | 9,725 | 83,243 |
| uWebSockets.js — raw echo, no RPC | 22,654 | 9,759 | 129,312 |
| `@fastify/websocket` — raw echo, no RPC | 22,620 | 9,638 | 86,585 |
| fastify-uws — raw echo, no RPC | 23,568 | 7,126 | 152,382 |
| socket.io — framework RPC via `emitWithAck` | 18,032 | 8,676 | 82,066 |
| tRPC — framework RPC over `wsLink` | 614¹ | 542¹ | 20,787 |

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

- **A raw echo is the ceiling, not a competitor.** uWebSockets.js at 1.26×
  on small payloads is what a socket costs with no router, no access check
  and no correlation on top. wrpc paying ~20% for all of that is the trade —
  and on this run it is level with `@fastify/websocket` and ahead of `ws`.
- **On real payloads the gap closes and inverts.** At 10 KB, wrpc is the
  fastest entry in the table — including the raw echoes — because the
  send path avoids re-encoding and re-copying what it already has.
- **The engine is not where the pipelined gap is.** With 64 calls in
  flight wrpc over its own engine runs at 95.8K and over the uws engine at
  112K, against 129K for a raw uws echo: swapping the JavaScript WebSocket
  engine for the native one buys ~17%, and the rest of the distance is the
  RPC layer plus the client. Turning on client batching — 64 calls sharing
  frames — is worth about as much as the engine swap, on the same engine.
  Both are one option away; neither is the default, because a single call
  should not wait for a flush.
- **Against frameworks doing the same job**, wrpc is level with socket.io on
  a single call and ahead pipelined. The tRPC rows read through footnote ¹:
  its sequential number is a client flush-timer artifact, and the honest
  comparison is the pipelined column — where wrpc is still ~4.6× ahead.
  tRPC's type story is excellent and unaffected by any of this.

## Fan-out

`bench/send-path.js` — one room, N members, 512-byte payload:

| Scenario | rate | throughput |
| --- | ---: | ---: |
| `sendText` 200 B | 6,252,035/sec | 1216 MB/s |
| `sendText` 4 KB | 1,671,520/sec | 6536 MB/s |
| `sendText` 64 KB | 62,840/sec | 3928 MB/s |
| room fan-out ×50 | 637,248/sec | 17624 MB/s |
| room fan-out ×200 | 321,820/sec | 35602 MB/s |
| room fan-out ×50 **+ deflate** | 101,609/sec | 334 MB/s |
| room fan-out ×200 **+ deflate** | 75,185/sec | 989 MB/s |
| room fan-out ×50 + deflate, windows 10/15 | 54,272/sec | 179 MB/s |

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
**one per recipient** and fan-out ×50 ran at 2,776/sec.
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
| call echo, 1 in flight | 94,131 |
| call echo, 64 in flight | 114,158 |
| call echo, 1024 in flight | 104,864 |
| call echo, 64 in flight, per-call `timeout` | 113,810 |

The pipelined rows are what the client's deadline scheduler bought: one
bucketed timer per client instead of a `setTimeout` and three closures per
call took 64 in flight from 101,666 to 114,158 and 1024 from 90,665 to
104,864, with a single awaited call unchanged. `WRPC_ROOT=<checkout>` points
the runner at another checkout for a before/after.

## Compression modes

`bench/deflate-context.js` — the stateless default, context takeover and the
async threadpool path, on the traffic each is for:

| Scenario | rate | ratio / loop delay |
| --- | ---: | --- |
| repeated JSON event, one-shot (default) | 97,685/sec | 1.1× |
| repeated JSON event, **context takeover** | 30,677/sec | **10.6×** |
| 4 KB JSON, one-shot / async | 94,100 / 21,547 | 3.1× |
| 32 KB JSON, one-shot / async | 16,871 / 9,737 | 3.9× |
| 256 KB JSON, one-shot / async | 1,618 / 1,789 | 3.8× |
| 252 KB burst ×100, one-shot | 306/sec | loop blocked **328 ms** |
| 252 KB burst ×100, **async** | 1,212/sec | loop delay 2 ms |

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
| 16 MiB binary message, 16 KiB segments | 3,084 | 192 |
| 200 B text frames ×50K, 64 KiB segments | 661 | 3,468,803 |

The large-message number is what `SegmentQueue` exists for: buffering
fragmented reads with O(n) total work instead of re-concatenating a growing
buffer on every segment. See [wire format](../reference/wire-format#receive-path-performance).

## Batching crosses over at 16

`bench/batch-ordering.js` compares two ways of restoring order in a
[batch](./client#batching) — a linear scan versus an id-indexed map:

| batch size | linear scan | id-indexed | winner |
| ---: | ---: | ---: | --- |
| 4 | 5,285,606/s | 4,296,103/s | scan 1.23× |
| 8 | 2,230,358/s | 2,122,554/s | scan 1.05× |
| 16 | 939,014/s | 1,103,773/s | indexed 1.18× |
| 64 | 96,936/s | 237,578/s | indexed 2.45× |
| 128 | 31,160/s | 121,834/s | indexed 3.91× |

Which is why the client's default `batch.maxSize` is 16: the shape changes
right there. The server's own switch from the scan to the index sits at 12
(`ServerHttpTransport`, for the HTTP batch reply) — between the last size
where the scan still wins and the first where the index does; both numbers
come from this one bench, and re-running it is the way to move either.

## Cluster operations

`bench/cluster.js`, two instances over a `MemoryBackplane`:

| Operation | rate |
| --- | ---: |
| `cluster.count()` | 65,797,861/sec |
| `cluster.presence()` | 23,858,163/sec |
| join + leave with presence deltas | 449,324/sec |
| room fan-out ×100 emit | 18,096/sec |
| room fan-out ×100 ask, send side | 7,204/sec |

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
| cross-instance emit, one at a time, measured at the farthest member | 166/sec — delivery latency p50 2 ms, p99 4 ms |
| presence: 200 concurrent joins, then leaves | count agrees on another instance after 0 ms / 6 ms |
| broadcast `ask` across 4 instances | 250/sec, 200 of 200 answers every time |
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
first branch, 68 M at the last) and a `switch` is flatter (105 M → 69 M). But
put a real handler call in the loop and `switch` and the chain come out level
(63.4 vs 68.8 M ops/s) while a **table of handler functions loses ~25%**
(51.7 M ops/s) — V8 compiles a string `switch` into comparisons it can inline
through, and cannot inline through a function *value*. `Object.freeze` and
`Map` do not close the gap. So the dispatch branches stay branches.

`bench/emitter.js` measures the event path that every packet crosses:

| Scenario | ops/sec |
| --- | ---: |
| emit fire-and-forget — 1 sync listener | 64,514,129 |
| emit awaited — 0 listeners | 15,170,971 |
| emit awaited — 1 sync listener | 14,128,520 |
| emit awaited — 2 sync listeners | 3,140,608 |

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
