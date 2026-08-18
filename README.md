# wrpc

[![npm](https://img.shields.io/npm/v/%40alexify%2Fwrpc)](https://www.npmjs.com/package/@alexify/wrpc)
[![CI](https://github.com/Alexis-Technologies/wrpc/actions/workflows/ci.yml/badge.svg)](https://github.com/Alexis-Technologies/wrpc/actions/workflows/ci.yml)
[![node](https://img.shields.io/node/v/%40alexify%2Fwrpc)](#installation)
[![dependencies](https://img.shields.io/badge/runtime_dependencies-0-brightgreen)](#bundle-size)
[![docs](https://img.shields.io/badge/docs-online-blue)](https://wrpc.vercel.app/)
[![license](https://img.shields.io/npm/l/%40alexify%2Fwrpc)](./LICENSE)

A fast, **zero-dependency** WebSocket-based RPC protocol for Node.js and
browsers. Router and procedures, subscriptions that resume, rooms
that scale across processes, and binary streams with backpressure that reaches
all the way into TCP — [~7 KB min+gzip](#bundle-size) in a browser bundle, and
nothing at all in your lockfile.

```javascript
const { Server, WrpcClient, defineRouter, procedure } = require('@alexify/wrpc');

const router = defineRouter({
  greeting: {
    hello: procedure({
      access: 'public',
      handler: async (context, { name }) => `Hello, ${name}`,
    }),
  },
});

const server = new Server({ router, host: '127.0.0.1', port: 8000, protocol: 'http' });
await server.listen();

const client = await WrpcClient.connect('ws://127.0.0.1:8000/api');
await client.load('greeting');
await client.api.greeting.hello({ name: 'World' }); // → 'Hello, World'
```

📖 **[Full documentation](https://wrpc.vercel.app)** · [Getting
started](https://wrpc.vercel.app/guide/getting-started) · [Wire
protocol](https://wrpc.vercel.app/reference/protocol)

### Why zero dependencies?

**Nothing extra in the process handling your RPC traffic.** `package.json` has
no `dependencies` field at all — and no `peerDependencies` or
`optionalDependencies` either, because those are dependencies with extra steps.
The WebSocket engine is written from scratch (RFC 6455 + 7692), and the handful
of utilities borrowed from elsewhere were copied in and adapted rather than
installed.

**Optional integrations are injected, not depended on.** uWebSockets.js, Redis,
fastify, express and TanStack Query all work — you `require` them and hand them
over, and wrpc duck-types the injection at the boundary. So the integrations
you do not use cost you nothing, and the ones you do use are on your version,
not ours.

**One protocol, both sides.** The browser build contains zero Node builtins, so
the client that talks to your server is the same implementation, not a
reimplementation.

### Positioning

| | **wrpc** | **tRPC** | **Socket.IO** |
| --- | --- | --- | --- |
| Transport | WebSocket, HTTP, SSE, worker port | HTTP, WebSocket | WebSocket + long-poll fallback |
| Types | contract-first + codegen, no build step | TypeScript inference | none built in |
| Realtime | subscriptions, events, rooms, acks | subscriptions | events, rooms, acks |
| Binary | streams with backpressure | ❌ | ❌ (messages only) |
| Scaling | rooms backplane (any pub/sub) | your own | adapters |
| Cluster ops | fetchClients, commands, presence (local read) | your own | fetchSockets (round-trip) |
| Runtime deps | **0** | a few | several |
| Needs TypeScript | ❌ | effectively yes | ❌ |

### When NOT to use wrpc

- **You want a public HTTP API.** wrpc has a REST mode, but it is a convenience
  for reaching procedures, not an API design. If third parties consume it,
  write an HTTP API and document it.
- **Your stack is polyglot.** The protocol is documented and small enough to
  reimplement, but the only implementation today is JavaScript. gRPC exists for
  a reason.
- **You need guaranteed delivery.** Rooms are at-most-once fan-out, not a
  queue. Subscriptions with `tracked()` values and an event log let a client
  catch up; if you need a broker's guarantees, use a broker.
- **You want end-to-end type inference from server code.** tRPC's model — where
  the client's types come from the server's implementation with nothing written
  twice — is genuinely nicer if your whole stack is TypeScript. wrpc's contract
  is declared or generated, deliberately, because it must also work for
  JavaScript users and across a network boundary the compiler cannot see.

## Table of Contents

- [Installation](#installation) · [Bundle size](#bundle-size) · [Browser usage](#browser-usage)
- [Quick Start](#quick-start)
- [Features](#features)
- [Exports](#exports)
- [Documentation](#documentation)
- [Contributing](#contributing) · [Changelog](#changelog) · [Security](#security) · [License](#license)

## Installation

```bash
npm install @alexify/wrpc
```

Requires **Node.js ≥ 22** (or any modern browser through a bundler). The
package is CommonJS and ships as-is — no build step and no transpile;
`require('@alexify/wrpc')` and `import { Server } from '@alexify/wrpc'` both
work.

### Bundle size

Measured with `pnpm size` (esbuild, fully minified with identifier mangling,
then gzipped):

| Entry | min | min+gzip | budget |
| ----- | ---:| --------:| ------:|
| `@alexify/wrpc` — browser (client, streams, chunks) | 30.1 KB | **10.3 KB** | 11 KB |
| `@alexify/wrpc` — node (client + server) | 117.2 KB | 38.8 KB | — |
| `@alexify/wrpc/ws` (WebSocket engine) | 20.7 KB | 7.5 KB | — |
| `@alexify/wrpc/engine` (engine port) | 21.2 KB | 7.7 KB | — |
| `@alexify/wrpc/uws` (uWebSockets.js adapter) | 13.5 KB | 5.5 KB | — |
| `@alexify/wrpc/fastify` | 100.6 KB | 34.3 KB | — |
| `@alexify/wrpc/express` | 95.3 KB | 32.2 KB | — |
| `@alexify/wrpc/scaling` (rooms backplane) | 4.1 KB | 1.7 KB | — |
| `@alexify/wrpc/sse` — browser (client transport) | 32.8 KB | **11.2 KB** | 12 KB |
| `@alexify/wrpc/sse` — node | 41.1 KB | 13.9 KB | — |
| `@alexify/wrpc/query` (TanStack bindings) | 2.4 KB | **1.0 KB** | 2 KB |

The Node-only rows are reported for visibility into what each subpath pulls in
— they never ship to a browser, and the adapter rows include the whole core
because they bundle it. Only the browser-reachable entries carry a **budget**,
and exceeding one **fails CI**: it is a ratchet against accidental bloat, raised
deliberately in the same change that earns the bytes.

Injected packages (uWebSockets.js, ioredis, fastify, express,
`@tanstack/query-core`) are never bundled — you install what you use.

### Browser usage

The package ships a browser entry (`browser.js`), resolved automatically by any
bundler that honours the `package.json` `browser` field and the `browser`
condition in `exports` — webpack 5, Vite, esbuild (`platform: 'browser'`),
Parcel, Bun. Rollup users need
[`@rollup/plugin-node-resolve`](https://github.com/rollup/plugins/tree/master/packages/node-resolve)
with `browser: true`.

It contains `WrpcClient`/`WrpcClientProxy`, the stream and chunk helpers, and
**zero Node builtins** — the server half is not in it. The two platform-specific
pairs (`chunks`, `runtime`) are swapped automatically, and `scripts/size.js`
fails the build if any browser entry ever resolves a `node:` import or a
package.

## Quick Start

A router of procedures, a server, a client:

```javascript
const { Server, WrpcClient, defineRouter, procedure, tracked } = require('@alexify/wrpc');

const router = defineRouter({
  chat: {
    // a call
    send: procedure({
      access: 'session',
      input: (args) => { if (!args?.text) throw new Error('text is required'); },
      handler: async (context, { text }) => {
        const sent = context.server.to('lobby').emit('chat/message', { text });
        return { sent };
      },
    }),
    // a subscription: many values, one connection
    onMessage: procedure.subscription({
      access: 'session',
      handler: async function* (context, args, { lastEventId, signal }) {
        for await (const message of feed({ signal })) yield tracked(message.id, message);
      },
    }),
    // an inbound, fire-and-forget event
    on: {
      typing: procedure({ handler: async (context, data) => announce(data) }),
    },
  },
  auth: {
    login: procedure({
      access: 'public',
      handler: async (context, { user }) => {
        context.client.startSession(undefined, { user });
        return { ok: true };
      },
    }),
  },
});

const server = new Server({ router, host: '127.0.0.1', port: 8000, protocol: 'http' });
await server.listen();
```

```javascript
const client = await WrpcClient.connect('ws://127.0.0.1:8000/api');
await client.load('chat', 'auth');

await client.api.auth.login({ user: 'ada' });
client.api.chat.on('message', ({ text }) => console.log(text));
client.sendEvent('chat/typing', { who: 'ada' });

for await (const message of client.api.chat.onMessage.iterate()) {
  console.log(message);
}
```

## Features

| Area | What you get |
| ---- | ------------ |
| **RPC** | [Router and procedures](https://wrpc.vercel.app/guide/router) with access control, [Standard Schema](https://standardschema.dev) validators, timeouts, concurrency queues and versioned units |
| **Realtime** | [Events both ways](https://wrpc.vercel.app/guide/rooms), [rooms](https://wrpc.vercel.app/guide/rooms), [acks](https://wrpc.vercel.app/guide/rooms#asking-a-room) (`client.ask`, `to(room).ask`), [subscriptions with resume](https://wrpc.vercel.app/guide/subscriptions), cancellation, call batching |
| **Streams** | [Binary upload/download](https://wrpc.vercel.app/guide/streams) interleaved on one connection, with end-to-end backpressure |
| **Sessions** | [Cookie-backed sessions](https://wrpc.vercel.app/guide/sessions) restored on reconnect, pluggable store, CSRF-aware REST dispatch |
| **Scaling** | [Rooms backplane](https://wrpc.vercel.app/guide/scaling) over any pub/sub; Redis and in-memory adapters included; [cluster layer](https://wrpc.vercel.app/guide/scaling#the-cluster-layer) — replicated presence (`count` with no round-trip), `fetchClients`, cross-instance commands, node-to-node ask |
| **Transports** | WebSocket, plain HTTP, [Server-Sent Events](https://wrpc.vercel.app/guide/sse), Service Worker `MessagePort` |
| **Hosts** | Batteries-included [server](https://wrpc.vercel.app/guide/server), or [fastify](https://wrpc.vercel.app/guide/adapters/fastify) / [express](https://wrpc.vercel.app/guide/adapters/express) / [uWebSockets.js](https://wrpc.vercel.app/guide/adapters/uws) / bare `node:http` |
| **Client** | Exponential backoff with full jitter, app-level heartbeat, automatic re-`load()` and re-subscribe, offline/online |
| **Observability** | [Structured logging](https://wrpc.vercel.app/guide/logging) into your pino, [OpenTelemetry](https://wrpc.vercel.app/guide/telemetry) spans and metrics, W3C trace context across the wire |
| **DX** | [Contract-first typed client](https://wrpc.vercel.app/guide/typed-client), [`wrpc types` codegen](https://wrpc.vercel.app/guide/cli), [TanStack Query bindings](https://wrpc.vercel.app/guide/query), hand-maintained `.d.ts` for every subpath |

## Observability

Both are **injected, never depended on** — the package still has no
dependencies. Your logger and your OpenTelemetry SDK are duck-typed.

```js
const pino = require('pino');
const api = require('@opentelemetry/api');

const server = new Server({ router, logger: pino(), telemetry: { api } });
```

`logger` takes a structured logger (pino, bunyan, winston), a `Console`, or
`false` to go silent. wrpc binds children for you — `component`, `peer` and a
per-call `callId` — and `context.log` inside a handler is already scoped to
that call.

`telemetry` takes the `@opentelemetry/api` module or your own
`{ tracer, meter }`. Spans follow the OTel `rpc.*` convention, fourteen metrics
cover calls, connections, subscriptions, broadcasts and streams, and `call`
packets carry W3C trace context so a client span parents the server's across
the network hop.

Neither can break a request: a logger that throws, a broken exporter or a meter
that dies is contained at the call site.

See [Logging](https://wrpc.vercel.app/guide/logging) and
[OpenTelemetry](https://wrpc.vercel.app/guide/telemetry).

## Exports

| Subpath | Exports | Docs |
| --- | --- | --- |
| `@alexify/wrpc` | `Server`, `RpcServer`, `WrpcClient`, `connect`, `defineRouter`, `procedure`, `tracked`, `createEventLog`, `createEventStream`, `MemorySessionStore`, `WrpcReadable`, `WrpcWritable`, `WrpcError`, `chunkEncode`/`chunkDecode` | [Server](https://wrpc.vercel.app/guide/server) · [Client](https://wrpc.vercel.app/guide/client) |
| `@alexify/wrpc/ws` | `WebsocketServer`, `Connection`, `Frame`, `FrameParser`, `OPCODES`, `CLOSE_CODES` | [Wire format](https://wrpc.vercel.app/reference/wire-format) |
| `@alexify/wrpc/engine` | `createNodeEngine`, `isEngine`, the `Engine`/`WrpcSocket` contracts | [Engine port](https://wrpc.vercel.app/reference/engine) |
| `@alexify/wrpc/uws` | `createUwsEngine`, `UwsSocket` | [uWebSockets.js](https://wrpc.vercel.app/guide/adapters/uws) |
| `@alexify/wrpc/fastify` | `wrpcFastify`, `findUwsApp` | [Fastify](https://wrpc.vercel.app/guide/adapters/fastify) |
| `@alexify/wrpc/express` | `createWrpc` | [Express](https://wrpc.vercel.app/guide/adapters/express) |
| `@alexify/wrpc/scaling` | `MemoryBackplane`, `createRedisAdapter`, `isBackplane` | [Scaling](https://wrpc.vercel.app/guide/scaling) |
| `@alexify/wrpc/sse` | `SseChannels`, `ServerSseTransport`, `ClientSseTransport`, `SseParser` | [Server-Sent Events](https://wrpc.vercel.app/guide/sse) |
| `@alexify/wrpc/query` | `createQueryUtils` | [TanStack Query](https://wrpc.vercel.app/guide/query) |
| `wrpc` (bin) | `wrpc types <url> --out api.d.ts` | [Codegen CLI](https://wrpc.vercel.app/guide/cli) |

Every subpath ships hand-maintained TypeScript declarations — no generation, no
`any` where a real type belongs.

## Documentation

- **Guide** — [getting started](https://wrpc.vercel.app/guide/getting-started),
  [server](https://wrpc.vercel.app/guide/server),
  [router](https://wrpc.vercel.app/guide/router),
  [sessions](https://wrpc.vercel.app/guide/sessions),
  [rooms](https://wrpc.vercel.app/guide/rooms),
  [subscriptions](https://wrpc.vercel.app/guide/subscriptions),
  [streams](https://wrpc.vercel.app/guide/streams),
  [scaling](https://wrpc.vercel.app/guide/scaling),
  [client](https://wrpc.vercel.app/guide/client),
  [typed client](https://wrpc.vercel.app/guide/typed-client),
  [CLI](https://wrpc.vercel.app/guide/cli),
  [TanStack Query](https://wrpc.vercel.app/guide/query),
  [SSE](https://wrpc.vercel.app/guide/sse),
  [logging](https://wrpc.vercel.app/guide/logging),
  [OpenTelemetry](https://wrpc.vercel.app/guide/telemetry), adapters.
- **Reference** — [wire protocol](https://wrpc.vercel.app/reference/protocol)
  (frozen at 1.0), [wire format](https://wrpc.vercel.app/reference/wire-format),
  [engine port](https://wrpc.vercel.app/reference/engine).
- **Types** — [`index.d.ts`](./index.d.ts) is the full public surface, plus one
  `.d.ts` per subpath.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the development workflow, the
house rules (zero dependencies, `.d.ts` and `tsd` in the same change) and the
release checklist.

```bash
pnpm test           # node --test, recursive
pnpm test:coverage  # c8, thresholds 95/95/90/95
pnpm test:types     # tsd
pnpm lint           # oxlint
pnpm size           # bundle-size budgets
pnpm docs:dev       # the documentation site
```

## Changelog

See [CHANGELOG.md](./CHANGELOG.md).

## Security

See [SECURITY.md](./SECURITY.md) for how to report a vulnerability.

## License

MIT — see [LICENSE](./LICENSE).
