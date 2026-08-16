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
| `generateId` | uuid v4 | Context uuids, server stream ids, REST packet ids — bring your own (cuid/ulid). |
| `introspection` | `true` | `system/introspect` exposure: `true` public, `'session'` gated, `false` off. |
| `maxBatch` | `128` | Packets accepted in one [batch frame](./client#batching). |
| `maxSubscriptions` | `256` | Concurrent [subscriptions](./subscriptions) per client. |
| `maxCalls` | `1000` | In-flight calls per client; past it a call answers `429`. |
| `sse` | `{}` | [SSE](./sse) channel options, or `false` to remove the endpoint. |
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
| `ws` | `{}` | Forwarded to the engine's `attach()` — `path`, `protocols`, `verifyClient`, `perMessageDeflate`, … |
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
`{basePath}/auth.1/signIn`.

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
| `headers` | `'Content-Type, x-wrpc-channel, last-event-id'` | `Access-Control-Allow-Headers`. |

Four things worth knowing:

- Once `origins` is set, every response carries `Vary: Origin` — allowed or
  not — so a shared cache can never serve one origin's grant to another.
  `credentials` is what the session cookie needs; a wildcard origin cannot
  carry credentials, which is the other reason to configure an allowlist.
- A disallowed origin does **not** fail the call. CORS is enforced by the
  browser, not the server: the call runs and the grant is simply withheld.
- Replacing `headers` drops `x-wrpc-channel` and `last-event-id` unless you put
  them back — those are what the [SSE transport](./sse) sends, so removing them
  disables cross-origin SSE.
- A request with **no** `Origin` header always passes the upgrade check.
  Non-browser peers (curl, server-to-server) send none, and the header is not a
  credential in any case.

The same origin check gates the **WebSocket upgrade**, where it is a real
defence rather than a browser courtesy: browsers do not apply the same-origin
policy to WebSockets, so without an allowlist any page anywhere can open a
socket to your server with the user's cookies attached. Configure
`cors.origins` and the default `verifyClient` refuses a mismatched `Origin`
outright.

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
rpc.attachPort(port);                                   // a MessagePort (Service Worker)
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
[Client](./client#service-workers).
