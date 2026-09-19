# WebTransport

WebTransport is the browser's API over HTTP/3 and QUIC: independent streams
with no head-of-line blocking between them, unreliable datagrams, and a
handshake with no TCP or TLS round trips to spare. `transport: 'wt'` runs
the ordinary wrpc client over it — the same packets, calls, events,
subscriptions with resume, binary streams with backpressure, heartbeat and
reconnect a WebSocket carries — with WebSocket as the fallback where
WebTransport is not available. On the server, `@alexify/wrpc/wt` attaches
the sessions a host implementation hands out to the same `Server` the
WebSocket clients are on: one router, one set of rooms, one cluster.

::: warning Experimental
The whole feature is `@experimental` and may change in a **minor** release
(see [stability](../reference/stability#experimental-carve-outs)). Node has
no WebTransport of its own yet, so the server side is built on an injected
implementation — the reasons and the options are below.
:::

## Using it

On the client nothing has to be imported: the transport is in the base entry
so that a fallback list needs no extra bytes.

```js
import { connect } from '@alexify/wrpc';

const client = await connect('https://host:4433/api', {
  transport: ['wt', 'ws'],
  wt: { serverCertificateHashes: [{ algorithm: 'sha-256', value: hash }] },
});
await client.load('chat');
await client.api.chat.send({ text: 'hi' });
```

`['wt', 'ws']` is an ordered fallback: a runtime without `WebTransport` (an
older browser, Node without an injected implementation) moves on to the
WebSocket at once, on the first connect, and a session that later exhausts
its reconnect budget does the same — `'transport-fallback'` fires either
way, and the application code above does not change. One thing the list
cannot do is change the host: it re-spells the *scheme* of the one URL per
candidate, so when the HTTP/3 host listens on a different port or origin
than the WebSocket (as in the repository's `examples/wt`), spell the
fallback by hand — a `transport: 'wt'` connect in a `try`, the WebSocket
connect in its `catch`. The `wt` options are
the `WebTransportOptions` of the constructor, plus `WebTransport` itself for
a runtime where the global one is not the implementation to use (a Node
client over `@fails-components/webtransport`'s, the fake in tests).

On the server, the host implementation is yours to run; wrpc attaches what
it accepts:

```js
const { Server } = require('@alexify/wrpc');
const { acceptSessions, failsRequestCallback } = require('@alexify/wrpc/wt');
const { Http3Server } = await import('@fails-components/webtransport');

const server = new Server({ router, port: 8000, sessions: { transport: bearerTransport() } });
await server.listen();

const h3 = new Http3Server({ port: 4433, host: '0.0.0.0', secret, cert, privKey });
h3.setRequestCallback(failsRequestCallback); // accepts, and routes by pathname
h3.startServer();
acceptSessions(server, h3.sessionStream('/api'));
```

The WebSocket engine keeps listening on TCP and the HTTP/3 host on UDP; a
client that arrived over either is a `Client` like any other, and
`client.transportKind` says which (`'wt'`).

## How it works

A WebTransport session is a bundle of QUIC streams and datagrams, not one
byte pipe like a WebSocket. wrpc uses **one** of them for everything: the
client opens a bidirectional stream — the _control stream_ — and every
packet and every stream chunk travels on it, in order, under a five-byte
header (length and kind; see the [wire format](../reference/wire-format#webtransport-stream-frames)).
That keeps the wire identical to the WebSocket one — a `stream` packet
precedes its first chunk, `ping`/`pong` run per direction, a subscription
resumes with `lastEventId` across a reconnect — and it is what lets the
fallback be invisible. [Streams of their own](#streams-without-head-of-line-blocking)
for binary transfers and datagrams for [unreliable events](#unreliable-events)
are additive capabilities on top, never a replacement of the packets.

On the server, the session becomes a `WrpcSocket` — the engine port's socket
contract — and is attached exactly the way a WebSocket connection is
(`RpcServer.attachSocket`), so session restore, declared headers, receive-side
flow control and the server-side transport all apply unchanged. Outbound
backpressure counts the bytes the stream's writer has not taken yet; inbound,
pausing simply stops reading, and QUIC's own flow control carries the
pressure to the peer.

Close codes carry over: the server's 1001 on shutdown and 1002 on a framing
violation arrive at the client as the session's `closeCode`, and a client
`close()` shows up on the server as the session ending.

## Streams without head-of-line blocking

On a WebSocket, a 64 MiB upload's chunks queue in front of every callback
and event sent after them: one ordered byte pipe. A WebTransport session
has as many independent, ordered streams as it wants, and wrpc uses them
for what they are good at: once both ends have announced the capability
(each end's first message on the control stream says so), every binary
stream — a `createStream()` on either side — gets a unidirectional
WebTransport stream of its own. Its chunks travel there, its end is that
stream's FIN, a `terminate()` its reset, and the control stream keeps
carrying the calls, events and subscriptions beside it, unblocked.

Nothing changes in application code: `WrpcReadable`/`WrpcWritable`,
backpressure and `getStream()` are the same, and a client whose transport
announced no such capability (an older peer, a wire codec in use — the
mapping reads stream packets, which only JSON allows) gets every chunk on
the control stream as before. The ordering subtleties are the transport's
to handle and are spelled out in
[the protocol reference](../reference/protocol#webtransport-streams).

## Unreliable events

The one thing a WebSocket cannot do: deliver a message *at most once*, with
no retransmission and no head-of-line wait behind the message before it.
Position updates, cursor moves, telemetry samples — state where the latest
value is all that matters and a lost one is superseded by the next — ride
WebTransport datagrams when asked:

```js
// client -> server
client.sendEvent('game/position', { x, y }, { unreliable: true });

// server -> one client, or a room
ctx.client.sendEvent('game/state', snapshot, { unreliable: true });
server.to('match-7').emit('game/state', snapshot, { unreliable: true });
```

The option is a hint about delivery, not a different API: on a WebSocket,
SSE or worker transport the same call sends the event reliably, so
application code is written once. A datagram carries at most
`maxDatagramSize` bytes (about 1200 on most paths) — an event that does not
fit goes reliably too, and a broadcast picks per recipient. Only events
qualify: `ask()` refuses `unreliable`, since an answer is expected. On a
cluster the flag crosses the backplane, so an instance delivering a remote
room's event does the same. The wire is documented
[here](../reference/protocol#webtransport-datagrams).

## Authentication without cookies

A WebTransport `CONNECT` request is sent with credentials mode `omit`: **no
cookies, no `Authorization` header** — by specification, in every browser.
Two consequences:

- The default cookie session transport cannot see a WebTransport client.
  Configure a token transport — `bearerTransport()` or `payloadTransport()`
  from [`@alexify/wrpc/auth`](./auth) — and the client's `bearerAuth()` (or
  the `headers` option) presents the token.
- The browser constructor cannot set headers either, so declared
  [headers and metadata](./metadata) ride the connect URL as the two reserved
  query parameters, exactly as on a browser WebSocket; the server reads
  the real `CONNECT` headers (`origin`, `user-agent`) first and the query
  only for names they lack. The same caveat as on ws applies: the URL lands
  in access logs — a device id belongs there, a secret should be short-lived.

`verify` in `attachSession` is the `verifyClient` of this path: it sees the
request (`headers`, `url`, `remoteAddress`) and the session, and a `false`
closes it with 403.

## Local development: certificates

Browsers accept a WebTransport server on a self-signed certificate only
through `serverCertificateHashes`, with three constraints: the certificate
must be **ECDSA P-256**, valid for **at most 14 days**, and the hash is the
SHA-256 of its DER encoding. `node scripts/wt-cert.js` in the repository
generates one and prints the hash; for anything public, a certificate from a
real CA needs no hash at all.

Two practical notes from the field: connect to `127.0.0.1`, not `localhost`
(a browser may resolve the latter to `::1` while the server bound IPv4), and
Firefox treats the hash as an _additional_ check rather than a replacement
for chain validation, so a self-signed certificate may still need a
development profile there.

## Hosting

Node has no WebTransport in its standard library. `node:quic` exists behind
a **compile-time** flag (`./configure --experimental-quic`; official binaries
do not carry it) and speaks raw QUIC and HTTP/3, but not the WebTransport
layer on top (the extended `CONNECT`, the session-bound streams and
datagrams). So `@alexify/wrpc/wt` binds to nothing and takes the session
from whatever the application runs:

| Host                                                                                                               | Kind                                                                                                                         | Adapter                                                                                                                                                                                                                                                                                                          |
| ------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`@fails-components/webtransport`](https://github.com/fails-components/webtransport) (+ `-transport-http3-quiche`) | native, Google's libquiche; prebuilt binaries (its install script must be allowed — `pnpm.onlyBuiltDependencies` under pnpm) | sessions are W3C-shaped and attach as-is; `fromFails(session)` reads the `CONNECT` request off `session.header`; the server needs `setRequestCallback(failsRequestCallback)` — its `sessionStream(path)` matches the request path literally, query included, and a wrpc client declares its headers in the query |
| [`quico`](https://github.com/colocohen/quico)                                                                      | pure JavaScript HTTP/3 stack, beta                                                                                           | `fromQuico(req, res)` in the request handler; quico reports no session end, so pass `idleTimeout` (the client pings on its heartbeat) — and give a pure-JS stack a heartbeat timeout of a second, not a hundred milliseconds                                                                                     |

```js
// quico: the request handler is where a session appears.
quico
  .createServer({ key, cert }, (req, res) => {
    if (req.headers[':protocol'] !== 'webtransport') return void res.end();
    const { session, ...meta } = fromQuico(req, res);
    attachSession(server, session, { ...meta, idleTimeout: 60_000 });
  })
  .listen(4433);
```

Both are devDependencies of wrpc, used only by its guarded integration
tests — never runtime dependencies. Anything else with the W3C session shape
(`incomingBidirectionalStreams`, `createBidirectionalStream`, `closed`,
`close`, optionally `datagrams`) works too: `isWtSession` is the check.

### Why not `node:quic` yet

It was tried, on a Node built from `main` with `--experimental-quic`
(September 2026, v27.0.0-pre; official binaries and nightlies of every line,
v22 to v27, still ship without it — `process.features.quic` is `false` and
`node:quic` does not resolve). The module's built-in HTTP/3 can negotiate
`SETTINGS_ENABLE_CONNECT_PROTOCOL` (`application.enableConnectProtocol`)
and HTTP/3 datagrams, but that is where it stops:

- a WebTransport client sends its `CONNECT` only after the server's
  SETTINGS carried `SETTINGS_ENABLE_WEBTRANSPORT` / `SETTINGS_WT_MAX_SESSIONS`,
  and `node:quic` has no way to send a setting nghttp3 does not know — a
  real client (libquiche's) sits on a `node:quic` server for its handshake
  timeout and gives up, before any `onheaders` fires;
- past that, WebTransport's own framing — request streams that start with
  the `0x41` frame, unidirectional streams of type `0x54`, the
  quarter-stream-id datagram prefix, the capsule protocol on the `CONNECT`
  stream — is not passed through either, as the module's own documentation
  says ("no built-in support for the `:protocol` pseudo-header, WebTransport
  datagram demultiplexing, or capsule framing").

So a `node:quic` host is a change in Node, not an adapter in wrpc. The
session contract is where it will plug in once it exists, and nothing
above it will change.

## Options

```js
attachSession(server, session, {
  headers,
  url,
  remoteAddress, // the CONNECT request, as the host saw it
  verify: ({ headers, url, remoteAddress, session }) => boolean,
  acceptTimeout: 10_000, // the client has this long to open its control stream (408 past it)
  highWaterMark: 1024 * 1024, // outbound bytes queued before send() answers false
  lowWaterMark: 256 * 1024, // 'drain' fires under this
  maxMessage: 16 * 1024 * 1024, // the largest inbound message; past it the peer is hung up (1002)
  idleTimeout: 0, // ms without inbound data before the session is terminated (0 = off)
  kind: 'wt', // what Client.transportKind reports
  compression: false, // per-message deflate on the control stream — see below
});

acceptSessions(server, sessions, {
  meta: fromFails, // reads the CONNECT request off each session
  onClient: (client, session) => {},
  onError: (error, session) => {},
  ...attachSessionOptions,
});

connect(url, {
  transport: ['wt', 'ws'],
  wt: {
    WebTransport, // the implementation; globalThis.WebTransport by default
    serverCertificateHashes,
    congestionControl,
    allowPooling,
    requireUnreliable,
    protocols,
    highWaterMark,
    lowWaterMark,
    maxMessage,
  },
  compression: false, // per-message deflate, both ends must turn it on — see below
});
```

## Compression {#compression}

Nothing compresses a QUIC stream's payload for you — HTTP/3 compresses
headers (QPACK), never bodies — so a WebTransport session carries exactly
the bytes wrpc hands it. Per-message compression is the answer, **off by
default** like every compression knob in wrpc, and negotiated: each end
names its codec in the [capabilities message](../reference/protocol#webtransport-streams)
and compresses only once the other end named the same one, so a client
with the option talking to a server without it is served plain, and the
other way round.

```js
// Server: one word for the platform codec (raw deflate through node:zlib).
acceptSessions(server, sessions, { compression: true });

// Client: connect()'s own option, or the `wt` bag's; both ends must agree.
connect(url, { transport: ['wt', 'ws'], compression: { threshold: 2048 } });
```

A packet or a chunk at or over `threshold` bytes leaves as its compressed
kind (3 for a packet, 4 for a chunk) and is inflated before delivery;
smaller ones go as they are. The threshold is the codec's own default —
1 KiB on Node, 4 KiB in a browser, where the only codec a page has is
`CompressionStream`, ~6× the cost of zlib per call and without a
dictionary, so a small message barely shrinks. The codec is a structural
seam: `{ codec }` injects anything with an `id`, `encode(bytes)` and
`decode(bytes, maxOutput)` — the dictionary codec of `@alexify/wrpc/deflate`
once it exists, or your own — and either method may answer a promise;
messages stay in order around it. Per message, `{ compress: false }` on an
emit sends that one plain, as on a WebSocket. Chunks that ride their own
WebTransport stream (the [stream mux](#streams-without-head-of-line-blocking))
and datagrams are never compressed.

What it costs and buys, `bench/message-compression.js` (one message at a
time, node:zlib): a 108 B event 143K/sec for 1.1× — the reason for the
threshold — a 1.4 KB callback 84K/sec for 6.4×, a 24 KB one 12K/sec for
13.8×. Inflating is 3–6× cheaper than deflating at every size. A page's
`CompressionStream` runs the same ratios at 23K/sec on the small message
and 9K/sec at 24 KB. An inflated message past `maxMessage` is a protocol
error (1002), the same cap a plain one has.

## What it cannot do

- **Cookies.** See above — sessions need a token transport.
- **A subprotocol.** The `wrpc.v1` offer a WebSocket makes has no carrier
  here yet (`WT-Available-Protocols` is young); the revision is implied by
  the wire, as it is on HTTP and SSE.
- **Serve HTTP.** The WebSocket engine still serves the REST and packet
  routes over HTTP/1.1; the HTTP/3 host is the session's only job.
- **Run without an implementation.** There is no default host in Node; a
  `transport: 'wt'` client in Node needs an injected `WebTransport` too.
- **Detect a vanished peer on its own.** A WebSocket engine pings; a
  WebTransport host is expected to end the session when QUIC's idle timeout
  fires. Where it does not, `idleTimeout` on the server side and the
  client's heartbeat are the liveness signal.

## Bundle size

The client transport, its framing, the datagram path and the stream mux
add about 3.3 KB min+gzip to the main browser entry (about 18.4 KB against a
19 KB budget in `pnpm size`) — the price of a fallback list that needs no
import. The server half never ships to a browser.
