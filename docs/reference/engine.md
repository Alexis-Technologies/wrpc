# Engine port

The engine is the replaceable server-side WebSocket implementation. Everything
above it — the router, clients, rooms, subscriptions, the wire protocol — is
written against this contract and nothing else, which is what makes
[uWebSockets.js](../guide/adapters/uws) a drop-in swap rather than a fork.

```js
const { createNodeEngine, isEngine } = require('@alexify/wrpc/engine');
```

This port covers the **server** side only. Client-side extensibility is the
`WrpcClient.transport` registry — see [Client](../guide/client#transports).

## `Engine`

```ts
interface Engine {
  name: string;
  standalone?: boolean;
  capabilities: EngineCapabilities;
  attach(options: EngineAttachOptions): EngineConnectionSource;
  listen?(options: { host?: string; port?: number }): Promise<unknown>;
  close(options?: { code?: number; reason?: string }): void;
}
```

`isEngine(value)` is the structural check the `Server` shell and the adapters
run — duck typing, never `instanceof`, so an engine from a different copy of
the package still works.

### Hosted vs standalone

Two shapes satisfy the port, and the difference is who owns the network stack.

**Hosted** (`standalone` falsy) attaches to a listener someone else owns:

```js
const source = engine.attach({ server: httpServer, path: '/api', verifyClient });
```

It may also expose `handleUpgrade(req, socket, head)` on its source, for driving
handshakes by hand from an app's own `'upgrade'` listener — that is exactly what
the [express adapter](../guide/adapters/express) does, attaching with **no**
server at all.

**Standalone** (`standalone: true`) owns everything, `node:http` included. It
is attached without a server, receives the core's HTTP entry point, and must
implement `listen()`:

```js
const source = engine.attach({ path: '/*', verifyClient, onHttpCall: (call) => rpc.handleHttpCall(call) });
const address = await engine.listen({ host, port });
```

With a standalone engine, `Server` creates no `node:http` server: `httpServer`
is `null` and `server.address()` is the only way to read the bound address.

### `attach(options)`

| Option | Meaning |
| --- | --- |
| `server` | The node http(s) server. Required for hosted engines, absent otherwise. |
| `path` | Restrict upgrades to this pathname. |
| `verifyClient({ req, socket, head })` | Gate the handshake. `socket`/`head` are `null` for standalone engines. |
| `protocols` / `handleProtocols(offered, req)` | Subprotocol negotiation; `false` rejects the handshake. |
| `perMessageDeflate` | `true`, or `{ threshold }`. See [wire format](./wire-format#permessage-deflate). |
| `pingInterval` | Protocol-ping interval for engines that own liveness. |
| `maxBuffer` / `maxBackpressure` / `fragmentThreshold` / `closeTimeout` | Engine limits. |
| `onHttpCall(call)` | Standalone engines only: the core's HTTP entry point. |

It returns an `EngineConnectionSource` — an `EventEmitter` that emits
`'connection'(socket, req)`.

## `WrpcSocket`

One per peer. The built-in engine's [`Connection`](./wire-format) satisfies it
natively; an adapter normalizes its own socket to this shape.

```ts
interface WrpcSocket extends EventEmitter {
  send(data: string | Buffer): boolean;   // false = above the high-water mark
  readonly bufferedAmount: number;
  readonly remoteAddress?: string;
  protocol?: string;
  close(code?: number, reason?: string): void;
  terminate(): void;
  pause?(): void;                          // optional receive-side flow control
  resume?(): void;
}
```

Events: `'message'(data, isBinary)`, `'drain'`, `'ping'(payload)`,
`'pong'(payload)`, `'close'(code, reason)`, `'error'(error)`.

Two rules the whole stack depends on:

- **`send()` returns an honest boolean.** `false` means the buffer is above its
  high-water mark and a `'drain'` will follow. That is what makes
  [stream](../guide/streams#backpressure) and
  [subscription](../guide/subscriptions#backpressure) backpressure real rather
  than aspirational — an engine that always returned `true` would turn a slow
  consumer into unbounded server memory.
- **A received payload may share memory with the receive buffer.** Copy it if
  you retain it past the listener call.

### `EngineRequest`

The upgrade request handed to `verifyClient`, `handleProtocols` and the
`'connection'` listener. The built-in engine passes a real
`IncomingMessage`; a standalone engine never sees one and synthesizes a
look-alike, so the port promises only what both provide:

```ts
interface EngineRequest {
  method?: string;
  url?: string;
  headers: Record<string, string | Array<string> | undefined>;
  socket: { remoteAddress?: string };
}
```

Narrow it with a cast when you know which engine you have.

## Capabilities

```ts
interface EngineCapabilities {
  backpressure: boolean;
  ping: boolean;
  deflate: boolean;
  cork: boolean;
  pause: boolean;
}
```

Advertised, not enforced: nothing in the core branches on these today. They
exist so a host, a test or a future optimization can ask what it is running on
rather than guess — and so a difference between engines is stated rather than
discovered.

| | node engine | uws engine |
| --- | --- | --- |
| `backpressure` | ✅ | ✅ |
| `ping` | ✅ | ❌ — uws owns liveness through `idleTimeout` |
| `deflate` | ✅ (off by default) | follows `compression` |
| `cork` | ✅ | ✅ |
| `pause` | ✅ | ❌ — no socket-level pause |

## The built-in engine

```js
const { createNodeEngine } = require('@alexify/wrpc/engine');

const engine = createNodeEngine({ perMessageDeflate: true, maxBackpressure: 1024 * 1024 });
new Server({ router, engine });
```

A thin wrapper over the from-scratch RFC 6455 implementation in
`src/websocket/` — handshake, framing, backpressure accounting,
permessage-deflate. It is the default, so you only construct one to pass
options. The implementation itself is published as
[`@alexify/wrpc/ws`](./wire-format).

## Writing an engine

Implement `Engine`, yield sockets that satisfy `WrpcSocket`, and run the
contract suite against it:

```js
const { runEngineContract } = require('./tests/engine/engineContract.js');

test('my engine', (t) => runEngineContract(() => createMyEngine(), t));
```

`tests/engine/engineContract.js` is a shared helper (not a `*.test.js`) that
checks the whole port: engine shape and capability types, `'connection'`
delivering a socket and an upgrade request, text and binary echo, `send()`
returning a boolean, drain ordering, close codes, ping/pong. Hosted engines
pass a bare factory (the suite owns an http server); standalone ones pass
`standaloneHarness(createEngine)`, which asks the engine to listen instead.

The same suite runs against the built-in engine and the uws adapter, so a new
engine is held to exactly the behaviour the rest of the stack assumes.
