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
at all, and next to two frameworks that do the same job wrpc does.

| Stack | small payload | 10 KB payload | small ×64 batched |
| --- | ---: | ---: | ---: |
| **wrpc** — own WS + RPC dispatch | 26,438 | **13,587** | 101,219 |
| `ws` — raw echo, no RPC | 35,635 | 12,107 | 108,955 |
| uWebSockets.js — raw echo, no RPC | 30,196 | 12,169 | 158,567 |
| `@fastify/websocket` — raw echo, no RPC | 34,888 | 12,307 | 110,184 |
| socket.io — framework RPC via `emitWithAck` | 25,254 | 11,128 | 99,472 |
| tRPC — framework RPC over `wsLink` | 661¹ | 587¹ | 23,023 |

ops/sec, higher is better.

¹ tRPC's sequential number is a **client-side flush-timer artifact, not
throughput**: its per-call latency measures a near-constant ~1.5 ms
(median 1.53 ms, p10–p90 spread 1.48–1.69 — a fixed delay, not processing),
so one awaited call per timer tick caps the sequential rate while the
pipelined column shows what the same stack does when many calls share a
flush. Compare tRPC on the batched column, where its per-call machinery
amortizes.

Read it honestly:

- **A raw echo is the ceiling, not a competitor.** `ws` at 1.35× on small
  payloads is what a socket costs with no router, no access check and no
  correlation on top. wrpc paying 26% for all of that is the trade.
- **On real payloads the gap closes and inverts.** At 10 KB, wrpc is the
  fastest entry in the table — including the raw echoes — because the
  send path avoids re-encoding and re-copying what it already has.
- **Against frameworks doing the same job**, wrpc is level with socket.io on
  a single call and the fastest measured stack at 10 KB. The tRPC rows read
  through footnote ¹: its sequential number is a client flush-timer
  artifact, and the honest comparison is the batched column — where wrpc is
  still ~4.4× ahead. tRPC's type story is excellent and unaffected by any
  of this.
- **Batching changes the ranking**, and a uWebSockets.js-backed stack wins it —
  which is exactly why the [uws engine](./adapters/uws) is a supported swap.

## Fan-out

`bench/send-path.js` — one room, N members, 512-byte payload:

| Scenario | rate | throughput |
| --- | ---: | ---: |
| `sendText` 200 B | 6,877,224/sec | 1338 MB/s |
| `sendText` 4 KB | 775,007/sec | 3030 MB/s |
| room fan-out ×50 | 99,487/sec | 2751 MB/s |
| room fan-out ×200 | 29,197/sec | 3230 MB/s |
| room fan-out ×50 **+ deflate** | 2,776/sec | 9 MB/s |

Fan-out encodes the frame **once** and writes the same buffer to every member;
throughput keeps climbing with the member count because the per-recipient cost
is a write, not a serialize.

::: warning permessage-deflate costs 36× on fan-out
Compression is per-connection by construction — the same payload has to be
compressed once per recipient, and the deflate context makes it expensive.
`sendText 200 B` drops from 6.9 M/s to 206 K/s with deflate on. Enable it for
bandwidth-bound clients, not for chatty in-datacenter fan-out.
:::

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
right there.

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
