# uWebSockets.js

```js
const { Server } = require('@alexify/wrpc');
const { createUwsEngine } = require('@alexify/wrpc/uws');

const engine = createUwsEngine({ uws: require('uWebSockets.js') });
const server = new Server({ router, engine, host: '127.0.0.1', port: 8000 });

await server.listen();
```

Everything above the engine is unchanged: the same router, the same clients,
the same wire protocol. What changes is who owns the socket.

::: info uWebSockets.js is injected, never depended on
It is not a dependency, not a peer dependency and not an optional dependency —
you `require` it and hand it over. Here it is a devDependency used only by the
adapter tests, pinned to a git tag. The adapter validates the injection
structurally (`app.ws`, `app.any`, `app.listen`), never with `instanceof`.
:::

## Standalone: uws owns everything

This is a **standalone** engine. uws owns the whole network stack, `node:http`
included, so the `Server` shell creates no HTTP server at all and routes its
HTTP calls into uws through the engine's `onHttpCall`.

Two consequences worth remembering:

- **`server.httpServer` is `null`.** Read the bound address through
  `server.address()`, which answers for both engine kinds.
- **It cannot be used with the [express adapter](./express)**, which needs a
  hosted engine to attach to a listener the app owns. `createWrpc({ engine })`
  refuses one with a clear error rather than half-working.

For fastify, use [`fastify-uws`](./fastify#two-backends-detected) — the plugin
detects it and puts this engine over the same uws app.

## Options

```js
createUwsEngine({
  uws: require('uWebSockets.js'),
  ssl: { key_file_name: 'key.pem', cert_file_name: 'cert.pem' },
  idleTimeout: 120,
  maxPayloadLength: 16 * 1024 * 1024,
  maxBackpressure: 64 * 1024,
  compression: null,
});
```

| Option | Default | Meaning |
| --- | --- | --- |
| `uws` | — | The injected module. |
| `app` | — | An existing uws app to attach to, instead of creating one. |
| `ssl` | — | Passed to `SSLApp()` instead of `App()`. |
| `idleTimeout` | `120` | Seconds without traffic before uws closes the peer. |
| `maxPayloadLength` | 16 MiB | Largest inbound message. |
| `maxBackpressure` | uws default | Outbound buffer cap. |
| `closeOnBackpressureLimit` | uws default | Close instead of dropping. |
| `compression` | `null` | A uws compressor constant; `null` disables permessage-deflate. |
| `sendPingsAutomatically` | uws default | uws' own protocol ping. |
| `maxBodySize` | 10 MiB | Cap for HTTP bodies the engine reads. |

## Capabilities

Engines advertise what they provide, and this one differs from the built-in
engine in three ways:

```js
engine.capabilities;
// { backpressure: true, ping: false, deflate: Boolean(compression), cork: true, pause: false }
```

- **`ping: false`** — uws owns peer liveness itself, through `idleTimeout` and
  `sendPingsAutomatically`, so wrpc runs no protocol-ping loop over it. The
  client's [application-level heartbeat](../client#heartbeat) is unaffected and
  still works.
- **`deflate`** follows `compression`, which is off by default.
- **`pause: false`** — uws exposes no socket-level pause, so receive-side flow
  control is missing: a fast uploader is not throttled by a slow
  [stream](../streams) consumer the way it is on the built-in engine. Outbound
  backpressure works normally.

See [the engine reference](../../reference/engine) for the full contract.

## Backpressure

uws' `send()` answers with one of three statuses, and the adapter maps them
onto the boolean contract every wrpc transport speaks:

| uws status | Meaning | `WrpcSocket.send()` |
| --- | --- | --- |
| `SUCCESS` | sent | `true` |
| `BACKPRESSURE` | queued, above the mark | `false` — wait for `'drain'` |
| `DROPPED` | **discarded** | `false`, plus an `'error'` and a terminate |

That last row is the one to understand. With `maxBackpressure` reached and
`closeOnBackpressureLimit` off, uws silently discards the message — and a hole
in the frame stream corrupts the RPC protocol, since a caller would wait
forever for an answer that was thrown away. The adapter refuses to continue:
it raises an error and terminates the socket, which the client sees as a
disconnect and recovers from by reconnecting.

## Stability

`uWebSockets.js` is distributed from git rather than npm and ships a native
addon, so the version you get depends on your platform having a matching
build. The adapter tests **skip** (never fail) when the binary is unavailable,
and CI pins the tag.

One trap worth knowing if you mix it with `fastify-uws`: two different
`uWebSockets.js` builds loaded in one process cannot coexist — the process
segfaults during environment cleanup, after your assertions have already
passed. `tests/adapters/boots.js` seeds the module cache so both resolve to one
binary; do the same if you hit it.

## Contract-tested

The [engine contract suite](../../reference/engine#contract-testing)
(`tests/engine/engineContract.js`) runs against this engine and the built-in
one alike — handshake, echo, binary, backpressure booleans, drain ordering,
close codes, ping/pong. Anywhere the two genuinely differ, the difference is
asserted rather than papered over.
