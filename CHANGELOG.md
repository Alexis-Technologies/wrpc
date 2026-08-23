# Changelog

All notable changes to **`@alexify/wrpc`** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Semver here versions the **JavaScript API**. The wire protocol carries its own,
narrower promise — see
[Stability](./docs/reference/protocol.md#stability).

## [Unreleased]

This package has not been published to npm yet — there is no `[1.0.0]`
release section until the first publish; everything below lands there
verbatim on release day.

### Added

**2026-08 re-review batch (resilience, security, scale, types)**
- Client resilience: `connectTimeout` (default 30 s — a handshake that never
  answers no longer parks the reconnect ladder), `reconnect.stableAfter`
  (the attempt counter resets only after the connection SURVIVES the window,
  so an accept-then-drop peer climbs the backoff instead of pinning at
  `minDelay`), coded rejections everywhere (`callTimeout` → 408 `WrpcError`,
  dead-transport sends and batched flush failures → 503), and the SSE POST
  settling the exact calls it carried.
- Refresh hardening: a call made BY the refresh handler surfaces its refusal
  instead of deadlocking the single-flight run; a refused **re-subscribe**
  now runs the same refresh and re-opens once (feeds heal like calls after a
  long outage); a THROWING refresh clears the `bearerAuth` store; failures
  log `refresh.failed`, emit `'refresh-failed'` and count on
  `wrpc.client.refreshes`.
- Auth carriers: the `TokenTransport` port now receives the core's parsed
  `declared`/`meta` bags (strategies can no longer drift from the wire
  parser — `payloadTransport` reads both `x-wrpc-meta` spellings on every
  carrier); on browser ws a Bearer credential rides a `wrpc.bearer.<token>`
  subprotocol offer instead of the connect URL; `cookieStorage` stamps
  `Secure` by default; the ws connect-URL query is client-capped with a
  `meta.oversize` warning.
- Per-call deadlines and retry: `CallOptions.timeout` (rides the packet; the
  server SHORTENS the procedure budget to match) and the opt-in client
  `retry` policy (`{ attempts, on: [503], … }`, jittered backoff, fresh
  packet id per attempt, never an offline buffer).
- Typed events: reserved contract keys `events` (server → client; narrows
  the unit emitter and types `client.respond`) and `sends` (client → server;
  types `client.sendEvent`) — declarations only. The router's inbound `on`
  handlers and the new declaration-only `emits` key now travel through
  introspection, and `wrpc types` generates both blocks.
- `wrpc types --openapi <path>`: an OpenAPI 3 document projected from every
  procedure with an `http` mapping (path/query parameters and request body
  from the fastify-shaped schema parts, wire error as the default response).
- Query bindings: `infiniteQueryOptions(path, args, { cursorKey, ... })` —
  the tRPC-v11 paging factory, cursor merged over args, lazy resolution and
  AbortSignal forwarding as ever.
- Cluster at scale: presence's periodic corrective message is now a
  **digest** (hash) with an addressed `sync`/`state` exchange only on
  drift; `cluster.rooms` replication filter; `cluster.maxFetch` (loud
  truncation, `clients.truncated`); opt-in `cluster.secret` HMAC envelope
  authentication; honest `cluster: false`; `healthy` getters with
  `'degraded'`/`'recovered'` events and backplane subscribe RETRY (rooms
  and cluster channels); `rooms: { linger }` grace window on emptied room
  channels; the SSE per-address cap gained an injected `clientAddress`
  seam (and the express adapter reports `req.ip`).
- Observability of the newest subsystems: the mapped REST leg traces on
  both ends (real `traceparent` headers ↔ the synthetic packet), delegated
  fastify routes emit the same spans/metrics as the packet path
  (`RpcServer#otel`, `@experimental`), cluster envelopes carry trace
  context and three `wrpc.cluster.*` instruments, early HTTP/SSE refusals
  log and count (`http.refused`/`sse.refused`/`cors.refused` on
  `wrpc.server.calls` under `<unknown>`), SSE gaps/expiries are logged and
  counted (`wrpc.server.sse.events`), reconnects count every attempt.
- The HTTP side's version marker: every response echoes `wrpc-version: 1`
  (requests may send one; revision 1 accepts and ignores it) — the ws
  subprotocol ladder's counterpart, reserved inside the freeze.
- Guard tests for every hand-synced pair (VitePress keywords/nav label,
  `RPC_OPTION_KEYS`, `scripts/size.js` ENTRIES, runtime-exports ⊆ d.ts,
  d.ts cross-references), a structural client-transport contract
  (`isClientTransport` + `tests/client/transportContract.js`), and docs:
  a dedicated [Authentication](https://wrpc.vercel.app/guide/auth) page,
  [Stability & deprecation](https://wrpc.vercel.app/reference/stability)
  reference, refreshed homepage grid/README/why.md (REST finally on the
  front door; honest tRPC bench footnote — the sequential number is a
  client flush-timer artifact).

### Changed
- **Breaking (nothing released yet):** `Procedure#invoke` takes an optional
  5th `budget` argument; the introspection unit object carries reserved
  `on`/`emits` keys (clients skip them; older generated artifacts are
  unaffected); `fetchClients` remote replies changed shape internally
  (`{ list, truncated }`); presence's periodic full `state` broadcast was
  replaced by the digest flow; `bearerTransport` no longer parses the raw
  `wrpc_h` URL itself (the core hands it the parsed bag);
  `maxChannelsPerAddress` keys on the injected `clientAddress`.
- Performance: `Allow-Headers` memoized per cors object
  (bench/cors-headers.js), `runValidator` synchronous fast path
  (bench/validate.js), ring-buffer replay logs (bench/replay-buffer.js),
  `sanitizeMeta` upper-bound walk and the `wrpc_meta` substring gate
  (bench/meta.js), batch flush skips the meta aggregate off-HTTP.
- Internal layout: `src/rpc/core.js` split (`client.js` — Context/Client;
  `meta.js` — the connection-metadata parser), the REST trie moved to
  `src/rpc/rest.js`, the wire names centralized in `src/wire.js`.


**Core RPC**
- `defineRouter`/`procedure`/`Router`/`Procedure`: units declared with versions
  as `'unit.vN'` keys, bare-function shorthand, per-procedure `access`
  (default `'session'`), `input`/`output` validators (plain functions or
  Standard Schema objects; failures map to 400/500), `timeout` (408), `queue`
  concurrency limits backed by a `Semaphore` (503), `meta`/`signature`
  descriptors, and lifecycle **hooks**: `onRequest`, `preValidation`,
  `preHandler`, `preSerialization`, `onSend`, `onResponse`, `onError`,
  `onTimeout`, `onSubscribe`, `onUnsubscribe`, plus router-level
  `onConnect`/`onDisconnect({ rooms })` (a pre-teardown snapshot of the
  client's rooms). Registered at three levels — `defineRouter({ hooks })`, a
  unit's reserved `hooks` key, `procedure({ preHandler })` — flattened once
  at router build, plus `router.addHook(name, fn)`. See the
  [Hooks guide](https://wrpc.vercel.app/guide/hooks).
- `system/introspect` is auto-registered from the router (`client.load()`
  needs no hand-rolled introspection); `introspection` option (`true`
  default / `'session'` / `false`).
- `context.method`/`context.procedure` set at every context creation site
  (calls, subscriptions, inbound events, fastify's delegated REST routes).
- `RpcServer`: engine-agnostic core, no `node:http` on the request path —
  `attachSocket(socket, meta)`, `handleHttpCall(call)` (abstract
  `{ method, url, headers, body, respond }`), `attachPort`. `Server` composes
  it with `node:http(s)` + a WebSocket engine.
- Sessions: `SessionManager` over a structural `SessionStore` (`{ get, set,
  delete }`, `MemorySessionStore` built in, anything store-shaped injects via
  `sessions.store`). Cookies restore automatically on HTTP calls and WS
  upgrades (`client.sessionReady`), default
  `HttpOnly; Secure; SameSite=Lax; Path=/`, survive disconnects.
  `MemorySessionStore` is bounded (LRU `maxSessions`, default 10000; `ttl`,
  default 24h) and gained `touch(token)` to slide the TTL on restore.
  CSRF: safe methods (`GET`/`HEAD`) dispatch without the cookie session
  unless Fetch metadata (`Sec-Fetch-Site`) proves same-origin intent.
- `basePath` (default `/api`): packet endpoint `POST <basePath>`, REST at
  `<basePath>/unit/method`, WS upgrades gated to `/` and basePath paths.
  REST version strategy: `defineRouter(units, { rest: { version: 'path' } })`
  maps a versioned unit under a `/vN` prefix (function form for full
  control); survives `merge()`.
- `cors: { origins, credentials, headers, methods }` — per-request origin
  echo, `Vary: Origin`, credentials only for allowed origins, origin gate on
  both HTTP calls and WS upgrades.
- `codec.rest`: an optional `rest` section on the wire codec
  (`{ encode, decode, contentType? }`) frames REST bodies (binary allowed) on
  both REST modes; rest-only codecs are valid. `RpcServer.codec` getter.
  Refused next to fastify's delegated REST routes.
- `maxCalls` (1000, 429 past cap), `maxBodySize` on the built-in `Server`,
  `logger`/`console` normalization (below), graceful shutdown:
  `server.close({ drain: ms })` stops intake, refuses new calls with 503,
  waits for in-flight calls, sends every peer a 1001 close frame; `drain()`
  and `draining` are public.

**Engine & transport**
- `@alexify/wrpc/engine` subpath: replaceable engine port (`WrpcSocket`,
  `Engine`, capability flags), `createNodeEngine()`. A **standalone** engine
  kind (`standalone: true`, uWebSockets.js) owns the whole network stack
  including `node:http`; `Server.address()` reports the bound address
  regardless of which side owns the listener (`Server.httpServer` is `null`
  under a standalone engine).
- WebSocket engine hardening: write backpressure (`send`/`sendText`/
  `sendBinary` return `false` above the high-water mark, `'drain'`,
  `bufferedAmount`, `maxBackpressure` terminates non-draining peers — control
  frames respect the cap too); end-to-end stream flow control (`Connection`
  gains `pause()`/`resume()`/`isPaused`, a slow stream consumer propagates
  backpressure through TCP); O(n) receive path (`SegmentQueue`, incremental
  header parsing, word-wise unmasking); subprotocol negotiation
  (`protocols`/`handleProtocols`, exposed as `connection.protocol`) — the
  client offers `wrpc.v1` by default and both engines echo it back;
  outgoing fragmentation (`fragmentThreshold`); permessage-deflate (RFC 7692,
  off by default, no-context-takeover, `maxOutputLength` capped by
  `maxBuffer`); inbound pings surfaced as a `'ping'` event; graceful
  `close({ code, reason })`; `wss.connections` snapshot. `maxPayload`
  (default 16 MiB) on the engine is a dedicated inflated-size cap for
  permessage-deflate, separate from `maxBuffer`.
- `WebsocketServer`'s `server` option is optional — an unbound server drives
  upgrades by hand through the new public `handleUpgrade(req, socket, head)`,
  which is what lets a middleware adapter perform the upgrade from a
  listener it does not own.
- `@alexify/wrpc/ws` subpath (`ws.js` + hand-maintained `ws.d.ts`), the
  engine's own entry point with full typings.

**Adapters** (`@alexify/wrpc/uws`, `/fastify`, `/express`) — each following
the root-shim + `.d.ts` + `exports` + `tests/*.test-d.ts` convention, and
each taking its host framework strictly by injection (devDependency used
only by adapter tests; never a runtime dependency).
- **uws engine adapter**: uws' tri-state `send()` collapses to a boolean —
  `SUCCESS`/`BACKPRESSURE` map to `true`/`false`, `DROPPED` fails loudly
  (`'error'` + terminate, since a silently dropped frame corrupts the
  protocol); every payload is copied at the callback boundary; a
  poisoned-handle guard latches `closed` once uws invalidates the handle;
  `listen()` tags a refused bind as `EADDRINUSE`. Capability flags reflect
  real behavior differences: `ping: false` (uws owns liveness via
  `idleTimeout`), `pause: false` (no receive-side flow control under uws).
- **fastify plugin** (`wrpcFastify`): two backends by feature detection — a
  plain `fastify()` gets the node engine on its `'upgrade'` event,
  `fastify({ serverFactory })` from `fastify-uws` gets a uws engine over the
  same uws app. HTTP calls run through real fastify routes, so hooks/auth
  run before wrpc sees the call; logging adapts to fastify's own logger.
- **express (and bare `node:http`) adapter**: `createWrpc({ ... })` returns
  `{ rpc, engine, wsServer, handler, upgrade, close }` and owns no listener
  — a request outside `basePath` passes to `next()` instead of a `404`, so
  wrpc composes with the rest of the app.
- Shared engine contract suite (`tests/engine/engineContract.js`) run against
  both hosted and standalone engines via a harness, plus a **swap test**
  running one behavioral spec against all five ways of standing wrpc up.

**Rooms & realtime** (`src/rpc/rooms.js`)
- `ctx.client.join(room)`/`.leave(room)`/`.rooms`/`.in(room)`;
  `server.to(room).emit(name, data)`, `.to(a, b)` (union), `.except()`,
  `.local()`, `server.broadcast()`. `emit()` returns how many clients
  received it on this instance. The chainable `Broadcast` is immutable —
  every modifier returns a new target. `to()` with no rooms reaches
  **nobody**, never everybody. `Context.server`/`Client.server` reach rooms
  from a handler.
- Client → server events: a unit's reserved **`on`** key declares inbound
  handlers, reachable with `client.sendEvent('unit/name', data)`; ordinary
  procedures (`access`/`input`/`timeout`/`queue` apply); `on` cannot be used
  as a method name and is not introspected.
- Encode-once fan-out: `Broadcast.emit` serializes the packet once and hands
  every recipient the same text through `Client.sendRaw`.
- `@alexify/wrpc/scaling` subpath: structural backplane contract
  (`publish`/`subscribe`/`close`), `MemoryBackplane`, and an ioredis-shaped
  `createRedisAdapter({ pub, sub, prefix })` (`sub` defaults to
  `pub.duplicate()`; the injected Redis clients are never `quit()`ed, only
  a self-opened `sub` is). `new Server({ backplane })` publishes every
  non-local emit as `{ v, instance, rooms, name, data }` with echo
  suppression by instance id; a room-targeted emit uses that room's
  channel, everything else the shared `broadcast` channel. Delivery is
  documented as **at-most-once**; backplane failures are isolated and local
  delivery continues.
- **Cluster layer**: `server.cluster` — presence, introspection and
  node-to-node messaging across every instance sharing a backplane, on two
  channels (`cluster`, `inst:<instanceId>`); every operation degrades to a
  local-only view without a backplane.
  - `cluster.count/presence/instances` are local reads kept warm by
    join/leave deltas plus a periodic snapshot (`presenceInterval`,
    default 5 s) that heals lost deltas; a restart's fresh epoch replaces
    counters instead of doubling them.
  - `cluster.fetchClients({ room }?)` collects descriptors
    (`{ id, instance, rooms, data, transport, session }`) from every node,
    resolving as soon as the last live node answers (`incomplete: true` on
    timeout, never silently).
  - `cluster.join/leave/disconnect(target, ...rooms)` — client ids are now
    instance-prefixed (`<instanceId>.<generateId()>`, so `instanceId` may
    not contain `'.'`), letting an id-addressed command travel as one
    message to one node. `client.data` is the app's bag, carried by
    descriptors; `rpc.getClient(id)` looks a local client up.
  - `cluster.sendEvent(name, data)` fans out to every other node's
    `cluster.on(name, fn)`; `cluster.ask(name, data)` collects one answer
    per node via `cluster.respond(name, fn)`.
- **Acks**: a server → client event may carry an `id`, making it a question
  the client answers with an ordinary `callback` packet.
  `client.ask(name, data, { timeout })` resolves with the answer (408/503/501
  on timeout/disconnect/no-responder); the client registers exactly one
  responder per name with `client.respond(name, fn)`/`unrespond(name)`.
  `server.to(room).ask(...)` aggregates
  `{ answers, errors, expected, incomplete }` cluster-wide via two-phase
  accounting, and serializes the payload once for the whole fan-out.

**Subscriptions, batching & SSE** — wire protocol v2: `subscribe`/`data`/
`end`/`unsubscribe`/`cancel` packets, plus a JSON array as a batch frame
(`docs/reference/protocol.md`).
- Subscription procedures — an async-generator handler answers with a
  stream of values (`procedure.subscription({ handler })` only needed for a
  plain function returning an async iterable). `queue`/`timeout` are
  refused on a subscription. The pump respects transport backpressure and
  always answers `end` exactly once (completion, throw, unsubscribe, or
  disconnect), closing the generator so its `finally` runs.
- Resume: `tracked(eventId, data)` labels a value; the client sends the
  last one back as `lastEventId` after reconnect. `createEventLog({ size })`
  is the ring buffer behind it — ids are `<epoch>.<n>` with a random epoch
  per instance, so `since()` answers `null` (an honest "cannot resume") for
  a foreign epoch or a gap past the buffer; persisted logs pass a stable
  `epoch`.
- `createEventStream()` (also on the browser entry): the push→pull adapter
  between "something calls me with a value" and a `for await` consumer.
  Bounded — an outrun producer drops the oldest value and reports it via
  `dropped`.
- Cancellation: `client.api.unit.method(args, { signal })` sends
  `{ type: 'cancel' }`; the caller rejects with **499** and `ctx.signal`
  aborts. Best-effort — a handler that ignores its signal keeps running, but
  its late result is dropped. A call cancelled while still queued is
  removed rather than racing the `cancel` ahead of its `call`. Cancel and
  unsubscribe are registered synchronously (a same-turn cancel is never
  missed). A disconnect aborts everything running for that peer.
- Batching: `batch: { flush: 'microtask' | ms, maxSize, maxBytes }`
  coalesces same-tick calls into one frame (a ping/cancel/unsubscribe never
  batches; a batch of one is sent bare). HTTP answers come back as one array
  in request order. `maxBatch` (128) caps a frame; `maxSubscriptions` (256)
  caps concurrent generators per client. A malformed element keeps its own
  id, preserving the array's positional guarantee; one unroutable answer no
  longer strands the rest of a batch.
- `@alexify/wrpc/sse` subpath: SSE as a full duplex transport. A channel is
  `GET {basePath}/events?channel=<id>` plus `POST {basePath}` with
  `x-wrpc-channel`, both bound to one server-side `Client` (the POST answers
  `202`; every reply travels on the stream). The channel id is
  **server-minted** (never client-proposed) and bound to the cookie identity
  that created it — a different identity gets 403, an unknown id gets 409.
  A dropped stream is held for `retention` (30 s) and a reconnect with
  `Last-Event-ID` re-attaches, replays missed frames, and **resumes**
  (rather than orphans) live subscriptions; `replayBytes` (1 MiB) bounds the
  replay buffer by bytes, and an id older than the buffer answers
  `event: gap` instead of a truncated replay. `maxChannels` (10000) and
  `maxChannelsPerAddress` (100) cap creation with 503/429. Comment frames
  and `X-Accel-Buffering: no` keep proxies from buffering/timing it out.
  Cross-origin requests carry full CORS headers including the
  channel/resume headers in the preflight allowlist. The session cookie
  from the opening GET restores on the channel exactly as `attachSocket`
  does for an upgrade. Text-only — binary streams are refused. Client half
  is browser-safe (`fetch` + a hand-written incremental parser, not
  `EventSource`), registered as `WrpcClient.transport.sse`;
  `connect(url, { transport: 'sse' })` selects it.
- `HttpCall` gained an optional `stream({ status, headers })` — the node
  shell, express and fastify implement it; a host that cannot answers the
  events endpoint with 501.
- `Context.signal`, `Client.calls`, `Client.subscriptions`, `Client.drain()`,
  `Client.binary`; `RpcServer.eventsPath`/`RpcServer.sse`.
- `client.close()` ends every live subscription uniformly (not just
  `iterate()` consumers) and delivers the same terminal callback an `end`
  packet does, exactly once; each listener is contained so one throwing
  handler cannot rob the next of its signal. `unsubscribe()` stays silent on
  purpose. `close()` also releases an `iterate()`'s `AbortSignal` listener.

**Typed client, codegen & Query bindings** — no TypeScript at runtime.
- Contract-first typed client: declare the API as an interface and thread
  it through `connect<Api>(url)` (a one-line alias of
  `WrpcClient.connect`). A call keeps its declared arguments plus a
  trailing `CallOptions` (`{ signal }`); a member typed
  `SubscriptionContract<Args, Data>` becomes `subscribe`/`iterate` instead
  of callable; `load()` only accepts declared unit keys. Utilities:
  `TypedApi`, `TypedUnit`, `TypedMethod`, `TypedParams`,
  `TypedSubscriptionMethod`, `InferArgs`, `InferResult`, `FirstArg`,
  `UntypedApi`, `IsAny`, `InvalidContractMember`. Without a contract
  everything stays exactly as loose as before. A zero-argument member keeps
  its args slot (`ping(undefined, { signal })`); a two-parameter/rest/
  non-function member maps to `InvalidContractMember`; a contract key named
  `on` is not mapped (a unit is an `Emitter` at runtime).
- `wrpc types <url> --out api.d.ts [--units ...] [--interface Api]
  [--package ...] [--schema <path> [--format cjs|esm]]` generates that
  interface (or a raw introspection artifact) from `system/introspect`;
  output is sorted for byte-identical re-runs. `client.use(introspection)`
  scaffolds units from that artifact synchronously, before `open()`, with
  zero wire traffic — a `load()`ed unit still wins and reloads on reconnect.
- The `signature` descriptor is specified (`docs/reference/protocol.md`): a
  type name, a field map (keys may end in `?`), or a one-element array
  meaning "array of". A closed, depth-capped format — names are JSON-quoted,
  types matched against an allowlist, `__proto__` is data, unrecognized
  types become `unknown`. `Signature`/`SignatureShape` type it.
- `@alexify/wrpc/query` subpath: `createQueryUtils(client, { prefix,
  queryClient })` → `queryKey`/`queryOptions`/`mutationOptions`/
  `subscriptionHandler`, as option **factories** (tRPC v11 style, not
  hooks) serving React/Solid/Svelte/Vue Query and query-core alike.
  Requires nothing (1 KB min+gzip); `queryFn` resolves `['unit','method']`
  lazily via `Object.hasOwn` on both hops (so `['chat','on']` cannot reach
  `Emitter.prototype.on`); TanStack's `AbortSignal` forwards into the call;
  `subscriptionHandler` writes through `setQueryData` and needs nothing on
  reconnect.
- Bundle-size budgets: `scripts/size.js` fails CI when a browser-reachable
  entry exceeds its min+gzip budget; an esbuild resolve plugin fails the
  build naming any non-relative import in a browser entry (catches a
  devDependency getting silently inlined, not just a bare `node:*`).
- Package consistency tests (`tests/package/consistency.test.js`): every
  exports target exists and ships, every subpath has an ordered `types`
  condition and a tsd file, shims resolve, plus the `./package.json` export.
- Browser type conditions: the surface split into `client.d.ts`
  (browser-safe) re-exported by `index.d.ts`/`browser.d.ts`/
  `sse.browser.d.ts` under the `browser` condition — importing a server name
  in a browser bundle is a compile error, and a project without
  `@types/node` compiles clean (checked by a tsc fixture compiled with
  `types: []`).
- A real Redis integration test (`tests/scaling/redis.integration.test.js`,
  manual/local, `REDIS_URL`-gated) exercises two independent
  `createRedisAdapter` instances over separate connections.
  `socket.io`/`tRPC` (`wsLink`) joined `bench/rpc-comparison.js`, each
  measured sequential and pipelined (64 in flight).

**Client resilience**
- Reconnect: truncated exponential backoff with full jitter
  (`reconnect: { minDelay, maxDelay, factor, jitter, retries }`; the
  `reconnectTimeout` shorthand now clamps the same way), `'reconnecting'`/
  `'reconnect-failed'` events, `client.attempt`. A rejected `open()` now
  reschedules the next attempt instead of stalling the loop; the reconnect
  timer is not `unref`'d (a live socket already holds one). `'reconnect'`
  fires after the api is rebuilt: loaded units reload, removed server
  methods drop, `api` unit objects are reused so their listeners survive.
- App-level heartbeat (`{type:'ping'}`/`{type:'pong'}`,
  `heartbeat: { interval, timeout }`) — the only defense against a dead
  `WebSocket` that never sent a close frame; only the WS transport starts
  one, both sides answer an inbound ping.
- An event reaching no listener surfaces as `'unhandled-event'`; a
  background failure with no `'error'` listener is logged, not thrown.
- In-flight calls reject with a coded 503 the moment the connection dies;
  client streams terminate on disconnect; reconnect-restore re-opens
  subscriptions before (and independently of) `load()` — a failing reload
  emits `'restore-failed'` and forces a clean reconnect rather than
  silently killing every subscription.

**Authentication & metadata**
- Client `authenticate` hook: awaited inside `open()` on the first connect
  (so `connect()` resolves an already-authenticated client) and on every
  reconnect **before** the subscriptions re-open and the units reload — the
  window an `'open'` listener structurally cannot reach. A throw terminates
  the transport, emits `'authenticate-failed'` and walks the normal backoff;
  `client.close()` inside the hook stops the cycle. With a hook configured,
  `'open'` fires after a successful authentication.
- Client `refresh` hook (`fn` or `{ on, handler }`, default trigger `[401]`):
  single-flight credential refresh — N concurrent refusals produce one
  handler run — with each refused call re-issued exactly once under a fresh
  packet id, on both the packet and the REST leg; on failure the original
  refusal surfaces. Never fires for calls made inside `authenticate`.
- Public `client.call(target, args, options)` — one call by wire target with
  no scaffolding: the escape hatch a first-connect hook needs, since `api`
  is built by `load()`.
- `client.meta` / `context.meta`: a frozen snapshot of what the peer
  presented — request/upgrade `headers`, `url` (every attach site used to
  drop `req.url`), `remoteAddress`, negotiated ws `protocol`, and declared
  `data`.
- Client `headers` option (connection-phase, re-evaluated per open): real
  request headers on http/sse/worker, one `wrpc_h` query parameter on
  browser ws (the WHATWG constructor takes no headers; observed headers win
  the merge, reserved names are dropped from the query path). Validated when
  a procedure declares `schema.headers` — the part was previously accepted
  and silently ignored; delegated fastify routes still validate in fastify,
  not twice.
- Client `meta` option and per-call `meta` (`client.call(..., { meta })`,
  `method.withMeta({...})(args)`): an optional additive `meta` field on
  `call`/`subscribe`/`event` packets, surfaced as `context.callMeta`
  (frozen-empty default) and `client.meta.data`; deliberately outside the
  `validation` option. Two wire spellings, both emitted by the client and
  both accepted from plain HTTP callers: the `x-wrpc-meta` header
  (percent-encoded JSON, the default — type-faithful, one CORS entry) and
  the per-key `x-wrpc-meta-<key>` form (the S3 `x-amz-meta-*` idiom; string
  values, one CORS entry per key), chosen with the client's
  `metaFormat: 'json' | 'prefixed'`. The JSON header wins a key collision.
  Both channels share one sanitizer: `metaMaxBytes` cap (default 2048) on
  the encoded input, plain-object check, `__proto__` drop, freeze.
- **Keys of both declared bags are normalized to kebab-case** (`userId` ->
  `user-id`, `xAppVersion` -> `x-app-version`) on every transport and on
  both ends, so `schema.headers` has one casing to validate and the two meta
  spellings collide on the same key instead of sitting side by side as
  lookalikes. Underscores are left alone; keys differing only in acronym
  casing merge (last write wins); an external caller must write kebab itself
  because HTTP lowercases header names before the server observes them.
- Per-call `meta` now reaches the client's **mapped REST leg**, which
  silently dropped it: with no packet to ride, it merges over the connection
  bag and travels as request headers, the per-call half winning. Under
  `batch: true` the request headers carry the batch's **aggregate**
  (last write wins) as a summary for gateways and access logs — each call's
  exact meta still rides its own packet and is what `context.callMeta`
  reports. An oversize aggregate is refused client-side with a
  `meta.oversize` warning rather than left to the server's whole-bag drop.
- `cors.metaHeaders: string[]` names the per-key meta headers CORS cannot
  wildcard (`['userId']` grants `x-wrpc-meta-user-id`, normalized with the
  same rule the client uses); appended to `cors.headers`, which now also
  accepts an array. `ClientTransport.request` takes `{ rest, meta }` in
  place of its trailing `rest` argument.
- Pluggable session token carrier (`sessions: { transport }`, structural via
  `isTokenTransport`): the cookie default is byte-identical; a non-ambient
  carrier (`ambient: false`) is exempt from the safe-method CSRF rule it
  never needed, and SSE channel identity keys on whatever the carrier reads.
- New **`@alexify/wrpc/auth`** subpath (browser-safe, own 2 KB budget):
  token stores (`memoryStore`/`webStorage`/`cookieStorage` — a `Map`
  already satisfies the contract), `bearerAuth()` composing
  `headers`+`authenticate`+`refresh` around a store, and the server halves
  `bearerTransport()` / `payloadTransport()`.
- Server dispatch now gates on `client.ready` — session restore **plus**
  settled `onConnect` hooks — so a subscribe racing the hooks can no longer
  miss a room broadcast; a hook stalled past 5 s logs `onConnect.stalled`.

**Observability**
- Structured `logger` option (replacing `console`): one injected writer
  (`src/logging.js`, zero-import) normalizing a structured logger (pino/
  bunyan/winston, `(entry, message)`), a `Console` (`(message)`), or
  nothing. `false` silences a server outright; a shape matching neither
  disables logging rather than throwing. wrpc binds children per subsystem/
  connection/call, reachable as `context.log`; a throwing sink is contained
  to the line, not the call. Client `logger` is **off by default** — when
  on, an error is logged *and* emitted as `'error'`.
  `WrpcClientProxy` forwards the option.
- Three silent failure paths now report: a server-side subscription death
  used to answer `end` and log nothing; an aborted subscription's generator
  error was dropped entirely; `jsonParse(data) || {}` conflated "malformed"
  with "empty", hiding every unparseable packet behind one `||`.
- OpenTelemetry: `telemetry` accepts the `@opentelemetry/api` module or a
  custom `{ tracer, meter }` (tracer-only and meter-only both work; the
  module itself is never imported). Spans follow the OTel `rpc.*`
  convention and bracket the whole invocation. Fourteen instruments cover
  calls, durations, connections, subscriptions, broadcasts/fan-out size,
  stream bytes, backpressure, sessions and SSE channels.
  `includeIdentity: false` drops the peer address; a session token is never
  recorded at any setting. Every recording path contains its own failures.
- W3C trace context: `call`/`subscribe`/`event` packets may carry optional
  `tp`/`ts` fields, propagated per *packet* (not per connection) so each
  call in a batch keeps its own parent. wrpc hands the field to your
  propagator rather than parsing it, so propagation needs `{ api }` (or an
  explicit `propagation`). `trustRemoteContext` defaults to `true`; set it
  `false` when peers are untrusted.
- Telemetry discipline: unresolved method/event names collapse into an
  `<unknown>` bucket (never minted from peer-controlled text); metric
  attributes match span attributes per RPC semconv; the server dispatch
  path skips telemetry allocations entirely when disabled.
- Hot path: UTF-8 validation delegates to `node:buffer.isUtf8` above the
  native-call threshold (up to 70x on large frames vs `ws`); the frame
  parser stopped copying the 4-byte mask and allocating a `Result` per
  not-enough-bytes attempt; fragmented sends cork once per message; client
  batching serializes each packet once; error logging materializes
  `error.stack` only when a logger is enabled.
- Pluggable `generateId: () => string` on client (packet/subscription/
  stream ids) and server (context uuids, server-side stream ids, synthetic
  REST packet ids); a stream id is validated against the 255-byte
  chunk-header limit at the source. The browser runtime gained a
  `Math.random`-based uuid fallback for plain-http pages.
- Robustness against malformed/adversarial input: a malformed packet
  target is type-checked rather than trusted (an un-awaited dispatch that
  threw used to take the process down); every fire-and-forget dispatch has
  a terminal catch; a wire event or contract key named after an
  `Object.prototype` key cannot resolve up the prototype chain
  (`load()` defines units instead of assigning them); an `event`/`pong`
  packet over HTTP requires a persistent transport (400) instead of hanging
  the request; a socket abandoned by `terminate()` cannot close its
  reconnected replacement (scoped by socket identity); `createRedisAdapter`
  quits the subscriber it opened itself (never an injected one).
- A graceful WebSocket close now completes in milliseconds instead of ~1 s:
  the side *answering* a Close writes the echo, half-closes, and destroys
  after a short grace, per RFC 6455 5.5.1/5.5.2.
- A real `SECURITY.md` (scope, acknowledgement/fix windows, supported
  versions) and a stability/deprecation policy in `CONTRIBUTING.md`, with
  `@experimental` markers on the telemetry shapes and the engine-port
  `capabilities`.

### Fixed

- `client.sessionReady` is assigned **before** the `onConnect` hooks run —
  the documented `await client.sessionReady` recipe used to await the
  constructor's resolved default and see `session === null`; the packet-POST
  and delegated paths now share the same restore promise instead of
  discarding it.
- A restore (or authenticate) failing **after** the socket opened restores
  the attempt count before terminating, so the backoff grows and `retries`
  exhausts instead of hammering `minDelay` forever — which also means the
  transport-fallback list is actually reached from a post-open failure.
- A rejected `WrpcClient.connect()` closes the half-born client instead of
  leaking it into `WrpcClient.connections` for `online()` to revive; a
  throwing `'restore-failed'` listener is escalated rather than becoming an
  unhandled rejection; `WrpcClient.online()` no longer aborts its re-open
  loop on the first client without an `'error'` listener.

### Changed (breaking)

- Unit version keys must be `unit.vN` (`auth.v1`, not `auth.1`) — the wrong
  spelling throws a `TypeError` at router build. The token is stored
  verbatim, which is what lets the REST `/vN` prefix be a plain
  concatenation.
- `new Server(application, options)` is gone: the server takes one options
  object with `router` (`new Server({ router, host, port, protocol,
  sessions, cors, basePath, engine, ws, logger })`); the metarhia-style
  `application.getMethod()` coupling is fully removed, procedures come from
  `defineRouter`, handlers receive `(context, args)`.
- `console` is gone (not deprecated) in favor of `logger` — the fastify
  adapter's `toConsole` shim went with it, `fastify.log` (a pino) now goes
  in directly.
- `Client.emit` is the local `Emitter` emit again, not a wire send for
  every name but `'close'` — use `client.sendEvent(name, data)` to send.
- `instanceId` must not contain `'.'` (it prefixes every client id as
  `<instanceId>.<generateId()>`).
- 5xx error messages no longer travel to the caller — only the status line
  does, details stay in the server log correlated by packet id; a 4xx
  message still travels verbatim. `error.expose = true` opts a message in.
- `cors.origins` is enforced on HTTP calls too, not only the WS upgrade —
  a disallowed origin gets 403 instead of running with the grant withheld.
- Unknown `access` values throw at router construction instead of silently
  meaning "any session".
- The per-call success log line moved to `debug`, and a Console sink drops
  `debug` outright.
- `maxBackpressure` defaults to `maxBuffer` (was unbounded) — `0` opts back
  into unbounded.
- `ServerWsTransport` is constructed as `(connection, meta)`;
  `ServerHttpTransport` wraps an abstract call description instead of node
  `req`/`res`.
- `Client.restoreSession`/`finalizeSession` are async (store-backed);
  dropping a connection no longer deletes the session.
- The `Emitter` warns instead of throwing when `maxListeners` is exceeded
  (fan-out to many stalled streams is legitimate); duplicate-listener and
  unhandled-`'error'` throws remain.
- `websocketPath` option removed — path gating follows `basePath` (or pass
  `ws: { path }`/`ws: { verifyClient }` through to the engine).
- Data-send methods' boolean now means "accepted without exceeding the
  buffer", not "accepted for send" — wait for `'drain'` after `false`
  (check `closed`, since a closed transport also reports `false`).
- The WebSocket engine internals (`WebsocketServer`, `Connection`, `Frame`,
  `FrameParser`, `ParseError`, `PARSE_ERR_CODES`, `OPCODES`, `CLOSE_CODES`,
  `CLOSE_TIMEOUT`, `MAGIC`) live at `@alexify/wrpc/ws`, not the main barrel
  — the main entry keeps only the RPC-level API.

### Changed

- Server transports carry a `kind` — `ws`, `http`, `sse` or `event` — as a
  metric attribute and log field.
- Every shell and adapter funnels its option bag through `rpcOptions()`
  rather than re-listing the core's options by hand.
- `ServerTransport.send()` returns the transport's backpressure signal, so
  a producer can wait for `'drain'` instead of buffering without limit.
- `src/telemetry/` is three files (shared/server/client) instead of one, to
  keep the browser bundle under its size budget.

### Documentation

- **The wire protocol is frozen as 1.0** (`docs/reference/protocol.md`):
  packet types, fields and meanings do not change within the major version;
  a new optional field may be added, and an unknown packet type still
  answers a `callback` with code 500, which is what makes an additive
  change safe for an older peer.
- The documentation site is the full structure, not a skeleton: a guide
  track (getting started, server, router, sessions, rooms, subscriptions,
  streams, scaling, client, typed client, CLI, TanStack Query, SSE, hooks,
  logging, OpenTelemetry, one page per adapter) plus a reference track
  (wire protocol, wire format, engine port) with grouped nav/sidebar.
- README rewritten: badges, a positioning table against tRPC and
  Socket.IO, an honest *When NOT to use wrpc*, the feature matrix, an
  exports table with one row per subpath, and the `pnpm size` bundle-size
  table.
- `CONTRIBUTING.md` added: development workflow, house rules not visible in
  the code (zero dependencies, `.d.ts`/`tsd` pairing, the uws teardown trap
  that wedges `node --test`), and the manual release checklist.

[Unreleased]: https://github.com/Alexis-Technologies/wrpc/commits/main
