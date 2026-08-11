# Express

```js
const express = require('express');
const { createWrpc } = require('@alexify/wrpc/express');

const app = express();
const wrpc = createWrpc({ router });

app.use(wrpc.handler);

const httpServer = app.listen(8000);
httpServer.on('upgrade', wrpc.upgrade);
```

Nothing here owns a listener — the app does. `createWrpc()` hands back the
pieces and you wire them where they belong:

| Member | What it is |
| --- | --- |
| `handler(req, res, next)` | express/connect middleware for the HTTP half. |
| `upgrade(req, socket, head)` | Wire to `httpServer.on('upgrade', …)`. |
| `rpc` | The `RpcServer` — rooms, clients, broadcast. |
| `engine` / `wsServer` | The WebSocket [engine](../../reference/engine) and its connection source. |
| `close()` | Closes the core and the engine. |

It works with **bare `node:http`** too — `handler` is plain
`(req, res, next)`, with no express API involved:

```js
const server = http.createServer((req, res) => {
  wrpc.handler(req, res, () => {
    res.writeHead(404).end();
  });
});
server.on('upgrade', wrpc.upgrade);
```

::: info express is injected, never depended on
`express` is a devDependency here, used only by the adapter tests. This
middleware is an ordinary function the app mounts; nothing under `src/` ever
`require`s a framework.
:::

## Middleware semantics

A request **outside** `basePath` is passed to `next()` rather than answered
with a 404 — wrpc composes with the rest of your app instead of swallowing its
routes. That is the whole difference from the batteries-included
[`Server`](../server), which owns every request that reaches it.

`req.originalUrl` is used when present, so mounting under a prefix works:

```js
app.use('/v1', wrpc.handler);   // with basePath: '/v1/api'
```

## Body parsing

Both orders work. If a parser upstream already drained the stream
(`express.json()`), the middleware uses `req.body`; otherwise it reads the
stream itself, capped at `maxBodySize` (10 MiB by default):

```js
app.use(express.json());     // optional
app.use(wrpc.handler);
```

An oversized or unreadable body is answered with a `400` error packet.

## Options

Everything [`RpcServer`](../server#rpc-options) takes, plus:

| Option | Default | Meaning |
| --- | --- | --- |
| `rpc` | — | Reuse an existing core instead of building one from `router`. |
| `engine` | `createNodeEngine(ws)` | Must be a **hosted** engine. |
| `ws` | `{}` | Forwarded to the engine's `attach()`. |
| `maxBodySize` | 10 MiB | Cap for bodies this adapter reads itself. |

::: warning A standalone engine cannot be used here
[uWebSockets.js](./uws) owns its whole network stack, including `node:http` —
there is no express listener for it to attach to. `createWrpc({ engine })`
throws a clear error rather than half-working. Use
`new Server({ engine: createUwsEngine(...) })` instead.
:::

## Upgrades on a listener you do not own

`wrpc.upgrade` is the manual-handshake path. The engine is attached with **no**
server, so it binds to nothing and performs one handshake per call — which is
what lets it share a listener with everything else the app does with
`'upgrade'`. If you have several WebSocket consumers, route by path first:

```js
httpServer.on('upgrade', (req, socket, head) => {
  if (req.url.startsWith('/api')) return wrpc.upgrade(req, socket, head);
  other.handleUpgrade(req, socket, head);
});
```

## Shutdown

```js
await wrpc.close();
httpServer.closeAllConnections();
await new Promise((resolve) => httpServer.close(resolve));
```

The listener is yours, so closing it is yours too — `wrpc.close()` only
releases what wrpc opened.
