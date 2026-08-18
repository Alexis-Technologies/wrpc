# Client

```js
const { WrpcClient } = require('@alexify/wrpc');

const client = await WrpcClient.connect('wss://host/api');
await client.load('chat');

const { id } = await client.api.chat.send({ text: 'hi' });
```

`load()` fetches the unit's shape from `system/introspect` and scaffolds
`client.api.chat` out of it: each method becomes a function, each subscription
a `{ subscribe, iterate }` object, and the unit itself is an `Emitter` where
server → client [events](./rooms) arrive.

::: warning `api` exists after `load()`
`client.api.chat` is `undefined` until `load('chat')` resolves, and every unit
is rebuilt on reconnect. Do not capture `client.api.chat.send` in a long-lived
variable — call through `client.api` each time.
:::

## Transports

The URL scheme picks the transport; `options.transport` overrides it.

| Transport | Scheme | Calls | Events, subscriptions, cancel | Binary streams |
| --- | --- | --- | --- | --- |
| `ws` | `ws:` / `wss:` | ✅ | ✅ | ✅ |
| `http` | `http:` / `https:` | ✅ | ❌ (code 400) | ❌ |
| `sse` | `http:` / `https:` | ✅ | ✅ | ❌ |
| `event` | — | ✅ | ✅ | ✅ |

`sse` has to be registered before it can be named — see [Server-Sent
Events](./sse):

```js
require('@alexify/wrpc/sse');
const client = await WrpcClient.connect('https://host/api', { transport: 'sse' });
```

Anything HTTP cannot carry is an **error**, not a silent no-op: asking for a
subscription over HTTP is answered with code `400` rather than hanging.

## Options

```js
await WrpcClient.connect(url, {
  callTimeout: 7000,
  reconnect: { minDelay: 2000, maxDelay: 30000, factor: 2, jitter: true, retries: Infinity },
  heartbeat: { interval: 30000, timeout: 10000 },
  batch: { flush: 'microtask', maxSize: 16, maxBytes: 65536 },
});
```

| Option | Default | Meaning |
| --- | --- | --- |
| `callTimeout` | `7000` | Milliseconds a call waits for its answer. |
| `reconnect` | see below | `false` disables reconnection entirely. |
| `reconnectTimeout` | — | Shorthand for `reconnect.minDelay`. |
| `heartbeat` | `{ interval: 30000, timeout: 10000 }` | `false` disables it. |
| `batch` | off | `true` takes the defaults. |
| `transport` | from the URL | `'ws'`, `'http'`, `'sse'`, or anything registered. |
| `worker` | — | A `ServiceWorker` to proxy through. |
| `random` | `Math.random` | Jitter source; injectable so tests can pin the schedule. |
| `generateId` | uuid v4 | Packet/subscription/stream ids — bring your own (cuid/ulid/a test counter). Correlation ids, not secrets; stream ids must stay within 255 UTF-8 bytes. |
| `protocols` | `['wrpc.v1']` | WebSocket subprotocols to offer; the server echoes the wire revision back. `[]` offers nothing. |
| `logger` | off | A Console or pino-shaped logger; observes errors in addition to the `'error'` event. |
| `telemetry` | off | OTel tracer/meter/api — see [Telemetry](./telemetry). |

## Calls

```js
const result = await client.api.unit.method(args, { signal });
```

Slot 0 is always the procedure's arguments and slot 1 is always the client's
options — even for a procedure that takes nothing:
`client.api.system.ping(undefined, { signal })`.

Aborting `signal` sends `{ type: 'cancel', id }` and rejects the call with a
`WrpcError` carrying code **499**. Cancellation is best-effort by nature — a
handler that never looks at `context.signal` runs to completion — but the
caller is rejected immediately and whatever the handler eventually returns is
dropped rather than delivered late.

Failures reject with a `WrpcError` whose `code` is the server's:

```js
try {
  await client.api.chat.send({ text: '' });
} catch (error) {
  error.code;      // 400 invalid input, 403 no session, 404 unknown method,
                   // 408 timeout, 499 cancelled, 503 queue overflow, 500 …
  error.message;
}
```

## Batching

```js
const client = await WrpcClient.connect(url, { batch: true });

const [a, b] = await Promise.all([
  client.api.math.double({ n: 1 }),
  client.api.math.double({ n: 2 }),
]);   // one frame, two answers
```

Calls issued in the same tick travel as one JSON array — one frame on a
WebSocket, one request on HTTP. `flush` is `'microtask'` or a delay in
milliseconds; `maxSize` and `maxBytes` force an early flush. `client.flush()`
sends whatever is waiting, right now.

Only `call` packets batch. A ping, a cancel or an unsubscribe is a control
packet whose entire point is to arrive immediately.

The server caps a frame at `maxBatch` packets (128 by default) — one frame
asking for unbounded work would otherwise be a denial of service.

## Reconnecting

The client reconnects on its own, with truncated exponential backoff and full
jitter:

```
delay = random(0, min(maxDelay, minDelay * factor ** attempt))
```

Jittering the **whole window** rather than adding a small offset is what breaks
up the thundering herd: after a server restart, a thousand clients that
disconnected in the same millisecond would otherwise all come back in the same
millisecond.

On a successful reconnect the client reloads every unit it had loaded — the new
connection is a new server-side client, so its introspected method list has to
be rebuilt — re-opens every subscription from the last eventId it saw, and then
emits `reconnect`. The `api` unit objects themselves are reused, so event
listeners registered on them survive the outage.

## Heartbeat

A browser `WebSocket` exposes no protocol-level ping, so a connection that died
without a close frame — a dropped NAT mapping, a suspended laptop, a proxy that
stopped forwarding — looks perfectly open from JavaScript until the first call
times out.

The client therefore sends an application-level `{ type: 'ping' }` every
`interval` and expects a `{ type: 'pong' }` within `timeout`; a miss emits
`heartbeat-timeout` and forces a reconnect.

Each transport decides whether it wants one: the WebSocket and
[SSE](./sse) transports do, and the plain HTTP transport does not — a
request/response transport has no connection to keep alive.

## Events

```js
client.on('open', () => {});
client.on('close', () => {});
client.on('reconnecting', ({ attempt, delay }) => {});
client.on('reconnect', ({ units, attempts, subscriptions }) => {});
client.on('reconnect-failed', ({ attempts }) => {});
client.on('heartbeat-timeout', () => {});
client.on('unhandled-event', ({ name, data }) => {});
client.on('error', (error) => {});
```

Server → client events for a **loaded** unit arrive on the unit itself
(`client.api.chat.on('message', …)`); anything that reaches no listener
surfaces as `unhandled-event` rather than vanishing.

`client.attempt` is how many reconnect attempts have been made since the last
successful open, and `client.active` is whether the transport is up.

## Answering the server

The server can [ask](./rooms#asking-a-room) — an event that expects an
answer. The client registers exactly one responder per event name:

```js
client.respond('chat/confirm', async (data) => ({ ok: true }));
client.unrespond('chat/confirm');
```

The responder's return value (or thrown error, with its `code`) travels
back as the answer. An ask with no responder is answered `501` immediately
and still surfaces as `unhandled-event`. Registration is client-level, not
per-unit — unit objects carry server-named methods, where a method called
`respond` would collide — and works before `load()`.

## Offline and online

```js
WrpcClient.offline();   // close every client's transport
WrpcClient.online();    // reopen them
```

Static, because connectivity is a property of the machine rather than of one
connection. `WrpcClient.initialize()` wires them to the browser's `online` /
`offline` events; `WrpcClient.connections` is the live set.

## Service Workers

For an offline-capable app, the Service Worker holds the connection and the
page talks to the worker over a `MessagePort`. In the worker:

```js
const { WrpcClientProxy } = require('@alexify/wrpc');

const proxy = new WrpcClientProxy({ callTimeout: 7000 });
await proxy.open();
```

…and in the page:

```js
const client = await WrpcClient.connect(url, { worker: navigator.serviceWorker.controller });
```

The packets are identical on both hops, so nothing above the transport
changes — which is what makes the arrangement worth having: one socket for
every tab, and a peer that survives a page reload.

## In a browser bundle

The package ships a browser entry (`browser.js`), resolved automatically by any
bundler that honours the `browser` field — webpack, Vite, esbuild with
`platform: 'browser'`, Rollup (with `@rollup/plugin-node-resolve` and
`browser: true`), Parcel, Bun. It contains the client, the streams and the
chunk helpers, and **no Node builtins** — the server half is not in it.

The main entry is ~7 KB min+gzip in that build; `scripts/size.js` enforces a
budget on it in CI.
