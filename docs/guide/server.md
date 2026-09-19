# Server

There are two server objects, and the difference matters:

- **`RpcServer`** is the engine-agnostic core. It knows nothing about
  `node:http`, sockets or listening — you hand it a socket
  (`attachSocket`), a `MessagePort` (`attachPort`), or an abstract HTTP call
  (`handleHttpCall`), and it does the RPC. Every [adapter](./adapters/fastify)
  is a thin shell around one.
- **`Server`** is the batteries-included shell: a `node:http(s)` listener plus a
  WebSocket engine, composed around an `RpcServer`. It is what you want unless
  you already have a framework owning the port.

```js
const { Server, defineRouter, procedure } = require('@alexify/wrpc');

const server = new Server({
  router: defineRouter({ /* ... */ }),
  host: '127.0.0.1',
  port: 8000,
  protocol: 'http',
});

await server.listen();
console.log(server.address()); // { address: '127.0.0.1', family: 'IPv4', port: 8000 }
```

`server.rpc` is the core underneath, should you need it.

## Options

Everything is one options object. The RPC half is shared with `RpcServer` and
with every adapter; the network half belongs to the shell.

### RPC options

| Option | Default | Meaning |
| --- | --- | --- |
| `router` | — | The [`Router`](./router). Required. |
| `basePath` | `'/api'` | Where both endpoints live. `''` serves from the root. |
| `sessions` | `{}` | [Session](./sessions) store, cookie and token generator. |
| `cors` | `null` | See [CORS](#cors) below. |
| `backplane` | `null` | Carries room events between instances — see [Scaling](./scaling). |
| `instanceId` | a uuid | Identifies this instance on the backplane. |
| `generateId` | uuid v4 | Every id the server mints — `instanceId`, client ids, context uuids, stream ids, REST packet ids, SSE channel ids, the cluster epoch. Bring your own (cuid/ulid). See [Identifiers](./production#identifiers). |
| `introspection` | `true` | `system/introspect` exposure: `true` public, `'session'` gated, `false` off. |
| `maxBatch` | `128` | Packets accepted in one [batch frame](./client#batching). |
| `maxSubscriptions` | `256` | Concurrent [subscriptions](./subscriptions) per client. |
| `maxCalls` | `1000` | In-flight calls per client; past it a call answers `429`. |
| `sse` | `{}` | [SSE](./sse) channel options, or `false` to remove the endpoint. |
| `http` | `{}` | The HTTP side's own options: `compression`, off by default — see [Compression](#compression). |
| `compression` | off | Accept per-message compressed frames from a Node WebSocket client that negotiated them — see [Compression](#compression). |
| `maxMessage` | 16 MiB | The largest inflated client frame accepted on a socket. |
| `attachments` | `true` | Bytes in args, results and events travel as [binary attachments](./streams#attachments); `false` sends JSON as revision 1 did. |
| `logger` | `globalThis.console` | Where the server logs — a Console or a pino-shaped logger; `false` silences it. See [Logging](./logging). |
| `telemetry` | `null` | OTel traces and metrics — see [Telemetry](./telemetry). |

Lifecycle [hooks](./hooks) are not a server option: they belong to the
router (`defineRouter(units, { hooks })`), which is what every shell and
adapter shares.

### Network options

| Option | Default | Meaning |
| --- | --- | --- |
| `host` / `port` | — | Passed to `listen()`. Port `0` binds a free one. |
| `protocol` | `'https'` | `'http'` uses `node:http`; anything else uses `node:https`. |
| `key` / `cert` / `SNICallback` | — | TLS material, forwarded to `https.createServer`. |
| `nagle` | `true` | `false` sets `noDelay` on the listener. |
| `engine` | `createNodeEngine()` | The WebSocket [engine](../reference/engine). |
| `ws` | `{}` | Forwarded to the engine's `attach()` — `path`, `protocols`, `verifyClient`, `perMessageDeflate`, … Compression is **off** until you pass `perMessageDeflate` — see [performance](./performance#compression-is-off-by-default). |
| `maxBodySize` | 10 MiB | Request-body cap in bytes for the built-in HTTP path. |
| `retry` | `3` | `EADDRINUSE` bind attempts before giving up. |
| `timeouts.bind` | `2000` | Milliseconds between those attempts. |

## What it serves

Under `basePath` (`/api` by default) the server answers three shapes of
request, plus the WebSocket upgrade:

```
WS   {basePath}          the WebSocket upgrade (bare '/' is accepted too)
POST {basePath}          packet mode: a JSON call packet as the body
ANY  {basePath}/:unit/:method   REST mode: args from the query string and body
GET  {basePath}/events   the SSE stream (see the SSE guide)
```

**Packet mode** is the wire protocol verbatim — the same JSON object a
WebSocket frame carries, in an HTTP body. It is what the HTTP client transport
and the `wrpc types` CLI use.

**REST mode** exists so anything that speaks HTTP can call a procedure without
knowing the protocol. Query parameters and the JSON body are merged into the
args object, with the body winning:

```bash
curl 'http://localhost:8000/api/greeting/hello?name=World'
curl -X POST http://localhost:8000/api/greeting/hello -d '{"name":"World"}' \
  -H 'content-type: application/json'
```

A versioned unit keeps its version in the path segment:
`{basePath}/auth.v1/signIn`.

The response is always a `callback` packet, and the error code becomes the HTTP
status — `404` unknown method, `403` no session, `418` if that is what your
handler threw. See [the protocol reference](../reference/protocol#callback-server-client).

## CORS

With no `cors.origins` configured, responses carry
`Access-Control-Allow-Origin: *` and the WebSocket upgrade accepts any origin.
That is convenient and it is **not** what you want in production for anything
holding a session — configure an allowlist:

```js
new Server({
  router,
  cors: {
    origins: ['https://app.example'],   // or (origin) => boolean
    credentials: true,                  // needed for the session cookie
  },
});
```

| Field | Default | Meaning |
| --- | --- | --- |
| `origins` | — | An allowlist array, or a predicate. |
| `credentials` | `false` | Emits `Access-Control-Allow-Credentials`. |
| `methods` | `'POST, GET, OPTIONS'` | `Access-Control-Allow-Methods`. |
| `headers` | `'Content-Type, x-wrpc-channel, last-event-id, x-wrpc-meta'` | `Access-Control-Allow-Headers`. A string, or an array joined with `', '`. |
| `metaHeaders` | — | Meta keys allowed as per-key `x-wrpc-meta-<key>` headers. Appended to `headers`. |

Four things worth knowing:

- Once `origins` is set, every response carries `Vary: Origin` — allowed or
  not — so a shared cache can never serve one origin's grant to another.
  `credentials` is what the session cookie needs; a wildcard origin cannot
  carry credentials, which is the other reason to configure an allowlist.
- A disallowed origin is refused **403**, not merely denied the grant. The
  page could not read the answer either way — but without the refusal the
  call would still have *run*, with the cookie session restored, which is
  exactly the cross-site request an allowlist exists to stop.
- Replacing `headers` drops `x-wrpc-channel`, `last-event-id` and
  `x-wrpc-meta` unless you put them back — the first two are what the
  [SSE transport](./sse) sends, so removing them disables cross-origin SSE,
  and the third carries [connection metadata](./metadata).
- `metaHeaders` exists because CORS has **no wildcard for header names**. A
  client using [`metaFormat: 'prefixed'`](./metadata#choosing-a-spelling-metaformat)
  sends one real header per meta key, so each key must be named:
  `metaHeaders: ['userId']` grants `x-wrpc-meta-user-id` — normalized with
  the same rule the client uses, so a camelCase config still grants the name
  that actually arrives. The default `metaFormat: 'json'` needs none of
  this: it sends the one already-allowed `x-wrpc-meta` header whatever the
  keys are.
- A request with **no** `Origin` header always passes the upgrade check.
  Non-browser peers (curl, server-to-server) send none, and the header is not a
  credential in any case.

The same origin check gates the **WebSocket upgrade**, where it is a real
defence rather than a browser courtesy: browsers do not apply the same-origin
policy to WebSockets, so without an allowlist any page anywhere can open a
socket to your server with the user's cookies attached. Configure
`cors.origins` and the default `verifyClient` refuses a mismatched `Origin`
outright.

## Compression {#compression}

Off by default, like every compression knob in wrpc: a gzip per answer is
CPU spent for every peer to save bytes only some of them need. Turn it on
for the HTTP side with `http.compression`:

```js
new Server({
  router,
  http: {
    compression: {
      threshold: 1024,                            // bytes; smaller answers go plain
      filter: (call) => !call.headers['x-internal'], // per request, optional
      async: { threshold: 256 * 1024 },           // hand bodies this large to the threadpool
      encodings: ['zstd', 'br', 'gzip'],          // this server's order; the default is ['gzip']
    },
  },
});
```

`compression: true` takes the defaults — gzip alone. An answer is encoded
when the request's `Accept-Encoding` admits one of the server's codings, the
body is at or over `threshold`,
nothing upstream set a `Content-Encoding` already (a route's own
[`headers`](./rest#response-headers), a framework compression plugin), and
`filter(call)` — when given — returns `true`. The response then carries
`Content-Encoding`, `Vary: Accept-Encoding` (joined onto the CORS
`Vary: Origin`) and the encoded `Content-Length`. Packet-mode POSTs, batch
frames, REST results and errors all leave through the same funnel; a REST
route's [ETag](./rest#caching) is computed over the plain body, so a `304` is
the same validator whichever encoding was asked for, and a `204` or `304` is
never encoded.

**Which coding** is `encodings`, the server's list in **its** order of
preference: the first one on it the request accepts is used. A weight of
zero refuses a coding and `*` covers the ones not named; other weights say
*acceptable*, not *preferred* — which acceptable coding costs this server
least is not the client's to know (nginx reads the header the same way).

| `encodings` entry | Default level | On a 27 KB answer (`bench/algorithms.js`) |
| --- | --- | --- |
| `'gzip'` or `{ encoding: 'gzip', level, memLevel }` | zlib's 6 | 110 µs, 3,214 B — what every client accepts |
| `'br'` or `{ encoding: 'br', quality }` | 4 | 93 µs, 2,601 B — the smallest at gzip's cost |
| `'zstd'` or `{ encoding: 'zstd', level }` | 1 | 36 µs, 2,848 B — a third of the CPU; Node 22.15+ / 23.8+, a `TypeError` at construction before |
| `{ encoding, encode(bytes), createStream? }` | — | your own coding; `encode` may answer a promise, a failure answers the plain body |

Under ~2 KB the three are within a few bytes and microseconds of each other,
so the list earns its place on large answers. zlib's own Brotli default is
quality 11 — **33 ms** on that answer — which is why the default here is 4.
Browsers announce `br` and `zstd` over HTTPS only, so plain-HTTP development
sees gzip whatever the list says.

Nothing changes on the client: `fetch` sends `Accept-Encoding` and inflates
by itself, in browsers and in Node. The event stream has its own option,
[`sse.compression`](./sse#compression); the WebSocket has
[`perMessageDeflate`](./performance#compression-is-off-by-default).

### The Node client's frames {#node-client-frames}

`perMessageDeflate` compresses what the server sends; a **Node** client's
built-in `WebSocket` only ever inflates, so its uploads — a 4 KB call, a
stream chunk — arrive as they are. The server-level `compression` option
accepts per-message compressed frames from a Node client that asked for
them: the client sends `{ type: 'ping', enc: ['deflate-raw'] }` on open —
its codecs, in its [order of preference](./compression#list) — a server
answers with the first of them it holds as the `enc` of its `pong`, and
from then on the client sends every packet or chunk past the threshold as a binary
frame under a `0x00` marker (a stream chunk never starts with one) that
the server inflates before dispatch. Off on both ends by default; a lone
end stays plain. A browser never needs it — it compresses both directions
itself under `perMessageDeflate`.

```js
new Server({ router, compression: true });                   // accept them
const client = await connect('wss://host/api', { compression: true }); // send them (Node)
```

What it costs — `bench/http-compression.js`, one-shot gzip of a callback:

| Answer | rate | ratio |
| --- | ---: | ---: |
| 295 B (2 rows) | 110,649/sec | 1.7× |
| 1.6 KB (12 rows) | 76,920/sec | 5.5× |
| 8.5 KB (64 rows) | 27,563/sec | 9.9× |
| 135 KB (1000 rows) | 2,056/sec | 12.0× |

The first row is the reason for the 1 KiB threshold; the last one — half a
millisecond on the event loop — is what `async` is for. Under the fastify
adapter, [delegated REST routes](./adapters/fastify#response-headers-and-caching-on-delegated-routes)
answer through fastify's own reply and are `@fastify/compress`'s to encode;
the packet endpoint and conventional REST paths under the plugin follow this
option like any host.

## Lifecycle

```js
await server.listen();   // resolves with the server; retries EADDRINUSE
await server.close();    // closes clients, the engine, and the listener
```

`listen()` retries only `EADDRINUSE` (`retry` times, `timeouts.bind` apart) —
any other bind error rejects immediately.

::: warning Read the address through `server.address()`
With a [standalone engine](../reference/engine#hosted-vs-standalone)
(uWebSockets.js) there is no node http server at all and `server.httpServer` is
`null`. `server.address()` answers for both shapes.
:::

## Using the core directly

If something else owns the port, skip the shell. This is exactly what the
[express adapter](./adapters/express) does:

```js
const { RpcServer } = require('@alexify/wrpc');

const rpc = new RpcServer({ router });

rpc.attachSocket(socket, { headers, remoteAddress });   // a WrpcSocket or Connection
rpc.attachPort(port);                                   // a node:worker_threads MessagePort
rpc.attach(transport);                                  // any persistent 'packet'/'chunk' transport
await rpc.handleHttpCall({ method, url, headers, body, respond });
```

`handleHttpCall` takes an abstract call description rather than a node
`req`/`res` pair — that is the seam every framework adapter plugs into. The
shape is in [`index.d.ts`](https://github.com/Alexis-Technologies/wrpc/blob/main/index.d.ts)
as `HttpCall`; `src/adapters/common.js` has the helpers that build one from a
framework request.

## Ports

`attachPort(port)` speaks the protocol over a `MessagePort` instead of a
socket: JSON packets as strings, binary chunks as `Uint8Array`. That covers a
worker thread, an embedded peer, or a test harness that wants a real client
against a real server with no network in between. The `Server` shell wires it
to a `'port'` event, so a host can hand ports in without reaching for
`server.rpc`:

```js
server.emit('port', port);
```

This is *not* the browser Service Worker story — there, the worker holds a real
WebSocket to the server and the page reaches the worker over a `MessagePort`.
That is entirely a client-side arrangement; see
[Client](./client#workers).

`attach(transport)` is the seam under both: any persistent transport that
announces inbound text as `'packet'` and bytes as `'chunk'` events becomes a
client, whether this package knows the wire or not. The one it ships on top
of it is `attachChannel(rpc, dc, options)` from
[`@alexify/wrpc/webrtc`](./webrtc#your-own-connection) — a WebRTC data
channel the application negotiated itself, a browser reaching this server
peer to peer with `connect(url, { transport: 'webrtc', channel })`. Like a
port, a channel carries no request: the client starts with no session, and
what the application observed about the peer goes in `headers` / `data`.
