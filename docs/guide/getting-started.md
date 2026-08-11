# Getting Started

`@alexify/wrpc` is a WebSocket-based RPC protocol for Node.js and browsers: you
declare a **router** of procedures on the server, and the client builds a typed
object out of it at runtime. Calls, server → client events, subscriptions,
rooms and binary streams all ride on one connection — and the package has
**no runtime dependencies at all**.

## Installation

```bash
pnpm add @alexify/wrpc
```

Requires **Node.js ≥ 22** (the client also runs in any modern browser through a
bundler). The package is CommonJS and ships as-is — no build step, no
transpile. `require('@alexify/wrpc')` and
`import { Server } from '@alexify/wrpc'` both work.

::: info About the snippets
The examples use top-level `await`, which a `.mjs` file or an ESM bundle gives
you for free. In a CommonJS script, wrap them in an `async` function.
:::

## Your first server

A router maps unit names to procedures. A procedure is a handler plus the
policy around it — who may call it, what its arguments must look like, how long
it may run:

```js
const { Server, defineRouter, procedure } = require('@alexify/wrpc');

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
```

::: tip `protocol: 'http'`
`Server` creates an HTTPS listener unless you ask for `'http'` — pass `key` and
`cert` for TLS. In production, terminating TLS at a proxy and running the
server on `'http'` behind it is just as valid.
:::

That server answers on two paths under `basePath` (`/api` by default):
a WebSocket upgrade, and plain HTTP. See [Server](./server) for everything the
shell takes.

## Your first client

```js
const { WrpcClient } = require('@alexify/wrpc');

const client = await WrpcClient.connect('ws://127.0.0.1:8000/api');
await client.load('greeting');

console.log(await client.api.greeting.hello({ name: 'World' })); // Hello, World
```

`load()` asks the server what the unit contains (through the built-in
`system/introspect` procedure) and scaffolds `client.api.greeting` from the
answer. Nothing is generated ahead of time, and the server is the single source
of truth about what exists.

The same call works over plain HTTP — connect to `http://127.0.0.1:8000/api`
and every call becomes a `POST` carrying the same JSON packet. What HTTP cannot
carry is anything that needs the connection to stay open: events,
subscriptions, cancellation and binary streams. See
[Client](./client#transports).

## Adding a session

Access control is one word on a procedure. `access: 'public'` is callable by
anyone; anything else needs a session:

```js
const router = defineRouter({
  auth: {
    login: procedure({
      access: 'public',
      handler: async (context, { user }) => {
        context.client.startSession(undefined, { user });
        return { ok: true };
      },
    }),
  },
  profile: {
    // 'session' is the default, spelled out here for clarity
    whoami: procedure({
      access: 'session',
      handler: async (context) => ({ user: context.session.state.user }),
    }),
  },
});
```

The session token travels in an `HttpOnly; Secure; SameSite=Lax` cookie that is
read back on both HTTP requests and the WebSocket upgrade, so a reconnect
restores the session without a round trip. See [Sessions](./sessions).

## Adding realtime

Two things make a connection worth keeping open. **Events** are
fire-and-forget messages in either direction, and **rooms** are named groups
of clients to send them to:

```js
const router = defineRouter({
  chat: {
    join: procedure({
      access: 'public',
      handler: async (context, { room }) => {
        context.client.join(room);
        return { ok: true };
      },
    }),
    shout: procedure({
      access: 'public',
      handler: async (context, { room, text }) =>
        ({ sent: context.server.to(room).emit('chat/message', { text }) }),
    }),
  },
});
```

```js
await client.load('chat');
client.api.chat.on('message', ({ text }) => console.log(text));
await client.api.chat.join({ room: 'lobby' });
await client.api.chat.shout({ room: 'lobby', text: 'hi' });
```

See [Rooms](./rooms), and [Scaling](./scaling) for making a room span more than
one process.

## Adding a subscription

A **subscription** is a procedure that answers with many values instead of one.
Write it as an async generator; the client consumes it with a callback or a
`for await` loop:

```js
const router = defineRouter({
  chat: {
    ticks: procedure.subscription({
      access: 'public',
      handler: async function* (context, { to = 3 }) {
        for (let i = 1; i <= to; i++) yield { n: i };
      },
    }),
  },
});
```

```js
for await (const value of client.api.chat.ticks.iterate({ to: 3 })) {
  console.log(value); // { n: 1 }, { n: 2 }, { n: 3 }
}
```

Backpressure is real in both directions: the server pump waits for the
transport to drain before pulling the next value, so a slow consumer stops the
producer instead of filling memory. See [Subscriptions](./subscriptions).

## Types

Nothing above needs TypeScript. When you want it, there are two ways in and
they meet in the middle — hand-write the contract, or generate it from a
running server:

```ts
import { connect, type SubscriptionContract } from '@alexify/wrpc';

interface Api {
  greeting: { hello(args: { name: string }): Promise<string> };
  chat: { ticks: SubscriptionContract<{ to: number }, { n: number }> };
}

const client = await connect<Api>('ws://127.0.0.1:8000/api');
await client.load('greeting');
const message = await client.api.greeting.hello({ name: 'World' }); // string
```

```bash
npx wrpc types http://127.0.0.1:8000/api --out api.d.ts
```

See [Typed client](./typed-client) and [Codegen CLI](./cli).

## Where to go next

| If you want to… | Read |
| --- | --- |
| know every option the server takes | [Server](./server) |
| write procedures, validators, versions | [Router & procedures](./router) |
| authenticate users | [Sessions](./sessions) |
| broadcast to groups | [Rooms](./rooms) · [Scaling](./scaling) |
| push a feed of values | [Subscriptions](./subscriptions) |
| move files over the connection | [Binary streams](./streams) |
| tune reconnect, batching, heartbeat | [Client](./client) |
| run inside fastify / express / uWebSockets.js | [Adapters](./adapters/fastify) |
| serve realtime where WebSockets can't go | [Server-Sent Events](./sse) |
| know exactly what goes over the wire | [Wire protocol](../reference/protocol) |
