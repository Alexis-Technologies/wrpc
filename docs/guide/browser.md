# Browser & bundling

The same protocol implementation runs in Node.js and in a browser — not a
reimplementation, not a subset of the wire format. What differs is which
*files* the bundler picks, and this page is about that machinery: how the swap
happens, what lands in your bundle, and what is deliberately absent.

## Nothing to configure

```js
import { WrpcClient, connect } from '@alexify/wrpc';

const client = await connect('wss://api.example/api');
await client.load('chat');
```

Any bundler that honours the `browser` field or the `browser` export condition
resolves that import to the browser build automatically: webpack, Vite, esbuild
with `platform: 'browser'`, Rollup (`@rollup/plugin-node-resolve` with
`browser: true`), Parcel, Bun.

## What gets swapped

Two mechanisms, both declared in `package.json`:

| Node file | Browser file | Why |
| --- | --- | --- |
| `index.js` | `browser.js` | The browser barrel excludes the server half entirely. |
| `sse.js` | `sse.browser.js` | A browser needs the [SSE](./sse) client transport, never the channel registry. |
| `webrtc.js` | `webrtc.browser.js` | A browser peer needs the [WebRTC](./webrtc) peer, link and signaler client, never the server-side signaling unit. |
| `src/chunks.js` | `src/chunks.browser.js` | `Buffer` vs `TextEncoder`/`TextDecoder` for [binary framing](./streams). |
| `src/runtime/node.js` | `src/runtime/browser.js` | `node:crypto` vs `globalThis.crypto` for id generation. |

The `exports` map carries the same choices under the `browser` condition, so
resolvers that read `exports` and ignore the legacy `browser` field get the
identical result.

## What is in the browser entry

```js
Emitter, WrpcClient, WrpcClientProxy, WrpcError, connect,
WrpcReadable, WrpcWritable, chunkEncode, chunkDecode,
EventStream, createEventStream
```

That is the whole runtime surface — and it contains **no Node builtins**, so
nothing pulls in a polyfill for `node:http`, `node:crypto`, `node:zlib` or
`Buffer`.

::: warning Server exports are not in the browser build
`Server`, `RpcServer`, `defineRouter`, `procedure`, `RoomRegistry`,
`MemorySessionStore`, `ServerTransport` and friends exist only in the Node
entry. Importing one in browser code is a build error, not a runtime surprise —
which is the point of splitting the barrels rather than tree-shaking one.
:::

### Types without `@types/node`

`browser.d.ts` re-exports `client.d.ts` and nothing else, so browser consumers
never inherit the Node type dependencies that `index.d.ts` carries. That split
is why a front-end `tsconfig` with no `"types": ["node"]` type-checks cleanly
against `@alexify/wrpc`.

Note the asymmetry: the browser *types* include wire-packet interfaces and
other type-only declarations that have no runtime counterpart. Types are
erased; imports are not.

## Bundle size

Measured with `pnpm size`, which bundles each entry with esbuild and reports
min+gzip. Browser-reachable entries carry a **budget**, and exceeding one fails
the run — it is a ratchet, and it runs in CI's lint job.

| Entry | min | min+gzip | budget |
| --- | ---: | ---: | ---: |
| `@alexify/wrpc` — browser | 56.4 KB | **19.0 KB** | 20.0 KB |
| `@alexify/wrpc/sse` — browser | 59.3 KB | **19.9 KB** | 21.0 KB |
| `@alexify/wrpc/query` | 2.7 KB | **1.1 KB** | 2.0 KB |
| `@alexify/wrpc/auth` | 3.3 KB | **1.5 KB** | 2.0 KB |
| `@alexify/wrpc/webrtc` — browser | 139.2 KB | **45.0 KB** | 46.0 KB |
| `@alexify/wrpc` — node | 177.1 KB | 58.7 KB | — |

The Node-only entries carry no budget because their gzip size is not a shipping
cost; they are measured so a regression is *visible*, not gated.

The SSE entry is the browser entry **plus** the SSE transport — you pay the
extra ~1 KB only if you import it. The [WebRTC](./webrtc) entry is a client
**and** a server (a peer serves a router), which is what its 40 KB buys.
[`@alexify/wrpc/query`](./query) requires nothing at all (that is what keeps it
at 1 KB); it takes the client and your `QueryClient` by injection.

::: tip Raising a budget
A budget is a ratchet with a documented escape hatch: if bytes are genuinely
earned, raise the number in `scripts/size.js` in the same change, with a
comment saying why.
:::

## Transports in a browser

| Transport | Use it when |
| --- | --- |
| `ws` (default) | Normal. Full duplex, binary streams, everything on this site. |
| `http` | One-shot calls with no connection — no events, no subscriptions, no streams. |
| `sse` | WebSockets are blocked by a proxy or corporate network. Text only. |
| `event` | The connection lives in a worker — a Service Worker or a SharedWorker; the page talks over a `MessagePort`. |
| `webrtc` | Peer to peer: the other end is another browser (or a Node process), reached through a [`WrpcPeer`](./webrtc)'s `link` or a data `channel` you negotiated yourself — not a URL. |

`WrpcClient.transport` is a plain lookup table on purpose, and a subpath
entrypoint registers into it at require time — which is how `@alexify/wrpc/sse`
adds `'sse'` without the core knowing it exists. Registering your own works the
same way.

## Workers: Service Worker and SharedWorker

The `event` transport is how one socket serves every tab and survives a page
reload: the worker holds the connection, the page talks to the worker over a
private `MessagePort`. The worker side is the same `WrpcClientProxy` whichever
kind of worker it is.

```js
// in the Service Worker
const proxy = new WrpcClientProxy({ callTimeout: 7000 });
await proxy.open();

// in the page
const client = await WrpcClient.connect(url, { worker: navigator.serviceWorker.controller });
```

```js
// in the shared worker (wrpc-worker.js)
const proxy = new WrpcClientProxy({ url: 'wss://api.example.com' });
await proxy.open();

// in the page
const worker = new SharedWorker('/wrpc-worker.js', { name: 'wrpc' });
const client = await WrpcClient.connect(url, { worker });
```

The packets are identical on both hops, so nothing above the transport changes.
Pick the worker by what you need from it: a **Service Worker** also serves the
page offline and outlives a reload, at the price of registration and a
lifecycle the browser controls; a **SharedWorker** is only the shared socket —
no registration, alive exactly as long as a tab of the site is — and can point
at another origin through the proxy's `url`. `worker` also takes a dedicated
`Worker` or a raw `MessagePort`. See [Client → Workers](./client#workers).

::: warning Browser support
SharedWorker reaches most of the mobile web now (Chrome and Firefox for
Android, recent Safari on iOS), but Samsung Internet and Opera Mobile still
don't ship it — feature-detect (`typeof SharedWorker !== 'undefined'`) and
fall back to a direct connection rather than assuming it. The proxy releases
a closed tab's port on the `MessagePort` `close` event, which older engines
never fire — there the entry lives until the worker does, as it always has
for a Service Worker.
:::

The client also listens to `online`/`offline`: going offline stops the
reconnect timer instead of burning retries, and coming back reconnects
immediately rather than waiting out the backoff.

## Frameworks

The client is framework-agnostic and has no React/Vue/Svelte bindings by
design. For data fetching, [`@alexify/wrpc/query`](./query) returns TanStack
Query **option factories** — not hooks — so one 1 KB file serves React, Solid,
Svelte and Vue Query alike.

One thing to know when wiring a framework: `client.api` does not exist until
`load()` resolves, and it is **rebuilt on every reconnect**. Resolve paths
lazily inside the query function rather than capturing `client.api.chat.list`
at module scope — which is exactly what the query bindings do.

## CommonJS in an ESM world

The package is CommonJS and ships without a build step. Every modern bundler
handles that, and `import { WrpcClient } from '@alexify/wrpc'` works through
interop. If you are running a bundler-free `<script type="module">` setup with
no build at all, bundle the package yourself — there is no prebuilt ESM
artifact, on purpose: one shipped shape means one thing to audit.
