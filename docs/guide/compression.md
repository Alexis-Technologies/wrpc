# Compression

Nothing in wrpc compresses anything by default — on any transport, in any
direction. That is a choice, not a gap: a deflate per message is CPU spent
for every peer to save bytes only some of them need, and the server's first
commitment is the cost per frame. This page is the map of where the knobs
are, what each one buys, and the two things they share — the codec seam and
the router dictionary.

## Where the knobs are

| Wire | Knob | Negotiated by | Page |
| --- | --- | --- | --- |
| WebSocket, server → client (and both ways from a browser) | `perMessageDeflate` on the engine, `contextTakeover`, `async` | the upgrade handshake (RFC 7692) | [performance](./performance#compression-is-off-by-default) |
| WebSocket, client → server from Node | `compression` on the client and `compression` on the server | the first ping/pong | [server](./server#node-client-frames) |
| HTTP, packet mode and REST | `http: { compression }`, `encodings` for Brotli / zstd / your own | `Accept-Encoding`, the server's order | [server](./server#compression) |
| Server-Sent Events | `sse: { compression }` | the opening GET's `Accept-Encoding`, per response | [SSE](./sse#compression) |
| WebTransport | `compression` on both ends | the capabilities message | [WebTransport](./wt#compression) |
| WebRTC | `compression` on both peers | the description signal (`caps`) — a raw channel by agreement | [WebRTC](./webrtc#compression) |
| The broker binding | `compression` on both ends | `hello`/`welcome`; a stateless request lists what it accepts | [broker RPC](./brokers/rpc#compression) |
| The rooms backplane, the cluster channels | `rooms: { compression }`, `cluster: { compression }` | nothing — the marker names the codec; [a list](#list) makes the rollout lossless | [scaling](./scaling#compression) |

Every one of them is off until you turn it on, and every one is negotiated
where the wire allows it: a lone end is served plain. The
[protocol reference](../reference/protocol#compression) has the wire forms.

## One codec, or a list {#list}

`codec` takes one codec or a list in order of preference. Each end announces
the ids it holds, and **a sender compresses with the first codec of its own
list the other end announced**:

```js
// A Node service that prefers zstd, serving Node clients that may be older
// and browsers whose CompressionStream may not have it:
attachBrokerRpc(server, broker, { compression: { codec: ['zstd', 'deflate-raw'] } });
new WrpcPeer({ router, signaler, compression: { codec: ['zstd', 'brotli', 'deflate-raw'] } });
```

That makes the list the fallback — a peer without zstd is served deflate
instead of plain — and lets the two directions differ: a server answers in
zstd a browser that sends deflate, and `transport.compression` shows both
(`{ encode: 'zstd', decode: 'deflate-raw' }`). A name the platform lacks
(zstd before Node 22.15, a format this browser has not) is **skipped in a
list** and refused when it is the only thing asked for. Lists that share
nothing leave the wire plain; nothing hangs up.

The backplane negotiates nothing, so there the list means: **encode with the
head, decode anything on it**. A change of codec is then a rollout without a
lost envelope — every instance lists both (`['deflate-raw', 'zstd']`), then
the order is swapped (`['zstd', 'deflate-raw']`), then the old one is dropped.
Over a raw WebRTC data channel, which has no handshake either, both ends use
the head of their list.

## What it costs, what it buys

`bench/message-compression.js` and `bench/http-compression.js`, one message
at a time on Node (`node:zlib`):

| Message | one-shot deflate | rate |
| --- | ---: | ---: |
| 108 B event | 1.1× | 143K/sec |
| 1.4 KB callback | 6.4× | 84K/sec |
| 24 KB callback | 13.8× | 12K/sec |
| SSE tick, one gzip member flushed per event | 7.8× | 90K/sec |

The first row is why every per-message knob has a threshold (1 KiB): a
small message barely shrinks without history to lean on. There are two
ways to give it history. On a WebSocket, [`contextTakeover`](../reference/wire-format#context-takeover)
keeps a zlib window per connection (10.6× on a repeated shape, for ~160 KiB
per direction per connection). Everywhere else — and with no state per
connection — a **dictionary**.

## On the loop, or on the threadpool {#async}

Every Node codec in wrpc deflates **synchronously** by default, and that is
a measured choice, not an oversight. `node:zlib` has two APIs: the
convenience calls that block the event loop for the length of the deflate,
and the callback ones that hand the work to libuv's threadpool. The hand-off
costs a fixed ~20 µs per call — a queue, a context switch, a copy back —
and a message that takes microseconds to compress does not amortize it
(`bench/zlib-async.js`):

| Message | deflate, sync | deflate, threadpool (one at a time) | inflate, sync | inflate, threadpool |
| --- | ---: | ---: | ---: | ---: |
| 291 B | 8.6 µs | 28.4 µs | 3.4 µs | 22.3 µs |
| 2.6 KB | 15.9 µs | 35.8 µs | 4.3 µs | 22.8 µs |
| 27 KB | 100 µs | 120 µs | 19 µs | 44 µs |
| 276 KB | 970 µs | 1,007 µs | 148 µs | 259 µs |
| 1.1 MB | 4.3 ms | 3.8 ms | 578 µs | 1,136 µs |

Below ~256 KB the threadpool only makes the message **slower**; at 256 KB
the two are level, and past it the loop is the thing being bought — a
1 MB result deflates in 4 ms, which is 4 ms during which no other
connection is served, and fifty peers' worth of it is 200 ms. **Inflate
never earns the hand-off**: it is eight times faster than deflate, so the
fixed cost wins at every size up to the 16 MiB cap. That is why every
knob has the same shape — synchronous, with an opt-in `async` threshold
for the encode side only:

| Wire | The knob | Default threshold |
| --- | --- | --- |
| WebSocket, permessage-deflate | `perMessageDeflate: { async: { threshold } }` — [performance](./performance#compression-modes) | 256 KiB |
| HTTP | `http: { compression: { async } }` | 256 KiB |
| Server-Sent Events | none — a gzip stream already runs its writes off the loop | — |
| WebTransport, WebRTC | `compression: { async }` on the platform codec, `dictionaryCompressor(dict, { async })` on the dictionary one | 256 KiB |
| The broker binding, the backplane, a Node WebSocket client | none — refused: these carriers have no ordering queue to hide a promise behind | — |

```js
// A host that answers megabyte results over data channels: those go to
// the threadpool, everything smaller stays on the loop.
new WrpcPeer({ router, signaler, compression: { async: true } });
acceptSessions(server, sessions, { compression: { async: { threshold: 128 * 1024 } } });
```

The WebTransport and WebRTC transports keep messages in order around the
promise (the same queue a browser's `CompressionStream` already needs), so
a small event sent after a large result still leaves after it. With
sixteen large messages in flight the four threadpool threads bring a
276 KB deflate to 242 µs of loop time each — a burst is where the option
pays; a single large message merely stops blocking. In a browser the
question does not arise: `CompressionStream` is asynchronous by
construction, and the pure-JS codec of `@alexify/wrpc/deflate` hands
messages past `nativeAbove` (4 KiB) to it for the same reason.

## The router dictionary {#dictionary}

One-shot deflate sees every field name and every method target for the
first time, in every message. A preset dictionary is those strings sitting
in the window before the first byte, and your router already knows them:

```js
const { buildDictionary, dictionaryCompressor } = require('@alexify/wrpc');

const dictionary = buildDictionary(router); // ~0.5–32 KiB, deterministic
const codec = dictionaryCompressor(dictionary);

await attachBrokerRpc(server, broker, { service: 'billing', compression: { codec } });
new Server({ router, compression: { codec }, rooms: { compression: { codec } } });
```

`buildDictionary` reads the router's introspection — field names from
`signature` and `schema`, every `unit/method` target and `unit/event` name,
then the packet skeletons every message starts with — ordered least to most
frequent (zlib finds recent bytes cheapest and looks at the last 32 KiB),
and cut from the front past the cap. Two instances of the same router
build the same bytes, and the codec's `id` carries their hash, so the
negotiation on each wire compares dictionaries, not just codecs: an
instance whose router differs — a rolling deploy — names another id, and
that pair stays plain rather than corrupt. `bench/dictionary.js`, on a
546 B dictionary from a small router:

| Message | no history | with the dictionary |
| --- | ---: | ---: |
| 108 B event | 99 B (1.1×) | **55 B (2.0×)** |
| 84 B call | 77 B (1.0×) | **30 B (2.6×)** |
| 1.1 KB callback | 201 B (5.5×) | **160 B (6.9×)** |
| 27 KB callback | 1892 B (14.3×) | 1824 B (14.9×) |

Throughput is the same (118K/sec against 111K on the event), so the
dictionary codec's threshold is 64 B rather than 1 KiB: small messages are
what it is for; a large one has all the history it needs inside itself.

Where it works with the platform codec: every Node↔Node carrier — the
broker binding, the backplane envelopes, a Node WebSocket client's frames,
a Node peer on WebRTC or WebTransport. A **browser** has only
`CompressionStream`, which takes no dictionary — which is what the next
section is for. On a WebSocket from a browser the dictionary is impossible
in principle: the browser's own permessage-deflate does the compressing,
and `contextTakeover` is the tool instead.

## The dictionary in a browser: `@alexify/wrpc/deflate` {#deflate}

A DEFLATE codec in plain JavaScript, on its own subpath so a page that
does not inject it never loads a byte of it (3.8 KB min+gzip when it does):

```js
import { createDeflateCodec } from '@alexify/wrpc/deflate';

// `dictionary`: the same bytes the server built — served by the app, or
// built here from the same router by a browser peer.
const codec = createDeflateCodec({ dictionary });
const client = await connect(url, { transport: ['wt', 'ws'], compression: { codec } });
new WrpcPeer({ router, signaler, compression: { codec: createDeflateCodec({ dictionary: buildDictionary(router) }) } });
```

Its `id` is the Node dictionary codec's for the same bytes, so a browser
peer on this codec and a Node peer on `node:zlib` negotiate with each
other; without a dictionary it is the platform id, and the platform codecs
read it. The inflater is complete — stored, fixed and dynamic blocks,
whatever zlib or a `CompressionStream` on the other end chose — takes the
dictionary, and is capped like every decoder here. The encoder is
deliberately simple: LZ77 against the dictionary, written as one
**fixed-Huffman** block, because on the messages this exists for a dynamic
tree costs more than it saves. `bench/deflate-js.js`:

| Message | own encoder, dictionary | zlib, dictionary | `CompressionStream` |
| --- | ---: | ---: | ---: |
| 108 B event | **51 B**, 100K/sec | 51 B, 136K/sec | 97 B, 24K/sec |
| 2 KB callback | 263 B, 43K/sec | 204 B, 72K/sec | 250 B, 19K/sec |
| 28 KB callback | 2645 B, 4.5K/sec | 1824 B, 11.6K/sec | 1892 B, 8K/sec |

On the small message fixed codes produce *the same bytes* as zlib's dynamic
ones at a similar rate — and inflating it back runs at a million a second,
faster than zlib's own one-shot API. Past a couple of kilobytes fixed codes
fall 30–45% behind, so the codec is a hybrid: from `nativeAbove` (4 KiB)
up, a message goes to the platform's `CompressionStream` — dynamic
Huffman, no dictionary, which a large payload does not need — and the
output still inflates on a peer holding the dictionary. That hand-off is
asynchronous and on by default in a browser only; in Node the codec
answers synchronously whatever the size, so it works on the Node↔Node
carriers too (where `dictionaryCompressor` on `node:zlib` is the faster
choice anyway).

Tests are the main body of that subpath, not the codec: an interop matrix
both ways against `node:zlib` at every level and strategy and against the
platform streams, a fuzz corpus, and every malformed input answered with a
coded `DeflateError` rather than a wrong byte.

## The codec seam {#codec}

Every per-message knob takes `{ codec }`: anything with an `id`,
`encode(bytes)` and `decode(bytes, maxOutput)` — `isCompressor` is the
structural check, exported from the main entry. `codec` also takes the
**name** of a platform codec — the ids are `CompressionStream`'s format
names, so a Node peer and a browser peer negotiate the same one:

| `codec` | Node | Browser | Default level |
| --- | --- | --- | --- |
| `'deflate-raw'` — what `compression: true` means | every Node | every `CompressionStream` | zlib 3 |
| `'brotli'` | every Node | some (the constructor answers) | quality 4 |
| `'zstd'` | 22.15+ / 23.8+, a `TypeError` before | some | 1 |

```js
const { zstdCompressor, brotliCompressor } = require('@alexify/wrpc');

attachBrokerRpc(server, broker, { compression: { codec: 'zstd' } });
new RpcServer({ router, rooms: { backplane, compression: { codec: zstdCompressor({ level: 3 }) } } });
```

The factories (`deflateCompressor({ level })`, `brotliCompressor({ quality })`,
`zstdCompressor({ level })`, Node only) are for another level, threshold or
`async` than the name takes; an id names the format, never the level, so two
ends on different levels still negotiate. The defaults are measured
(`bench/algorithms.js`), not zlib's — and for Brotli that matters: zlib's own
default is quality 11, which takes **33 ms** on a 27 KB answer where
quality 4 takes 93 µs. In a browser a format the platform lacks leaves
compression off rather than throwing. The dictionary codec above is another
injection; yours is another still.
Either method may answer a promise on the socket transports (a
`CompressionStream` can only), and the transport keeps messages in order
around it; the Node↔Node carriers require a synchronous answer and say so
at construction. `decode` must stop at `maxOutput` bytes — that cap is what
bounds a compression bomb to the size a plain message is already bounded
at.
