# Changelog

All notable changes to **`@alexify/wrpc`** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Semver here versions the **JavaScript API**. The wire protocol carries its own,
narrower promise — see
[Stability](./docs/reference/protocol.md#stability).

## [Unreleased]

### Added

- **Lifecycle hooks, in the fastify tradition.** Named phases with no `next`:
  `onRequest`, `preValidation`, `preHandler`, `preSerialization`, `onSend`,
  `onResponse`, `onError`, `onTimeout`, `onSubscribe`, `onUnsubscribe`, plus
  the router-level `onConnect`/`onDisconnect`. Registered at three levels —
  `defineRouter(units, { hooks })`, a unit's reserved `hooks` key, and
  `procedure({ preHandler })` — and flattened ONCE per procedure when the
  router is built, so dispatch walks a frozen array and an empty phase costs
  a length check. A hook throws a coded error to end the call with that code;
  the observational phases are contained. `router.addHook(name, fn)` adds
  after the fact; `merge()` carries all three levels. See the new
  [Hooks guide](https://wrpc.vercel.app/guide/hooks), including the token-
  bucket rate-limit and subscription-quota recipes.
- **Pluggable id generation.** `generateId: () => string` on both the client
  (packet, subscription and stream ids) and the server (context uuids,
  server-side stream ids, synthetic REST packet ids) — bring cuid/ulid, or a
  counter in tests for deterministic logs. Ids are correlation identifiers,
  not secrets; the session token keeps its own `generateToken`. A stream id
  is validated against the 255-byte chunk-header limit at the source. The
  browser runtime also gained a `Math.random`-based uuid fallback for plain-
  http pages, where `crypto.randomUUID` does not exist.
- **A protocol revision on the wire.** The client offers the
  `wrpc.v1` WebSocket subprotocol by default and both engines echo it back
  when the app configured no protocols of its own — see
  protocol.md#versioning for the compatibility rules. `protocols: []` opts
  out; the selected protocol is exposed on the ws transport.
- **`maxCalls`** (default 1000): in-flight calls per connection are now
  capped like subscriptions always were; past the cap a call answers 429.
- **`introspection` option**: `true` (default) keeps `system/introspect`
  public, `'session'` gates it behind a session, `false` unmounts it.
- **`maxBodySize`** on the built-in `Server`, aligned with the adapters.
- **SSE channel caps**: `maxChannels` (10000) and `maxChannelsPerAddress`
  (100) refuse channel creation past them with 503/429.
- **`maxPayload`** on the websocket engine (default 16 MiB): a dedicated
  inflated-size cap for permessage-deflate, separate from `maxBuffer` —
  a compression bomb now costs at most `maxPayload` before the close.
- **Browser type conditions.** The hand-maintained surface split into
  `client.d.ts` (the browser-safe half, no node imports) re-exported by both
  `index.d.ts` and the new `browser.d.ts`/`sse.browser.d.ts`, wired as
  `types` under the `browser` condition — importing a server name in a
  browser bundle is now a compile error instead of `undefined` at runtime,
  and a TS project without `@types/node` compiles. Machine-checked by a tsc
  fixture (`tests/package/browser-types.test.js`) compiled with `types: []`.
- **Package consistency tests** (`tests/package/consistency.test.js`): every
  exports target exists and ships, every subpath has a `types` condition
  ordered before `default` and a tsd file, shims resolve, plus the
  `./package.json` export.
- **Graceful shutdown**: `server.close({ drain: ms })` stops intake, refuses
  new calls with 503, waits for in-flight calls, sends every peer a 1001
  "going away" close frame, and only then tears down. `RpcServer.drain()`
  and `draining` are public. `ServerWsTransport.close()` is a graceful close
  frame now; hard teardown stays `terminate()`.
- **Encode-once fan-out**: `Broadcast.emit` serializes the packet ONCE and
  hands every recipient the same text through the new `Client.sendRaw` —
  room fan-out went from ~8k to ~30k emits/sec at 200 recipients in the new
  send-path benchmark, and per-recipient backpressure is now visible in the
  metrics instead of silently discarded. A payload that cannot serialize is
  reported (`broadcast.serialize`), not thrown at the broadcaster.
- **Honest SSE replay**: `replayBytes` (1 MiB) bounds the replay buffer by
  bytes alongside the frame cap, and a `Last-Event-ID` older than the buffer
  answers an `event: gap` frame instead of silently replaying a truncated
  history — the built-in transport reacts by starting a fresh channel.
- **Epoch-stamped event logs**: `createEventLog()` ids are `<epoch>.<n>`,
  random epoch per instance — a resume against another process (or a
  restart) is a foreign epoch and `since()` answers `null` honestly.
  Persisted logs pass a stable `epoch`.
- **`SessionStore.touch(token)`** (optional) slides the TTL on restore, so a
  shared-store session cannot expire under a connected client;
  `MemorySessionStore` implements it.
- **Resilience**: in-flight calls reject with a coded 503 the moment the
  connection dies (no more waiting out `callTimeout` on a corpse); client
  streams terminate on disconnect; reconnect-restore re-opens subscriptions
  BEFORE (and independently of) `load()` — a failing reload emits
  `restore-failed` and forces a clean reconnect instead of silently killing
  every subscription; the queue semaphore is abort-aware and the procedure
  timeout covers queue wait; the HTTP client transport synthesizes error
  callbacks for the exact calls a failed request carried; the Redis adapter
  owns `error`/`ready` listeners on its subscriber.
- **Telemetry discipline**: unresolved method/event names collapse into an
  `<unknown>` bucket (metric series and span names are never minted from
  peer-controlled text — regression-tested with a name spray); metric
  attributes use the same `rpc.service`/`rpc.method` split as spans, per the
  RPC semconv; the reconnect counter dropped its unbounded attempt
  attribute; the span helpers live once in `telemetry/shared.js`; and the
  server dispatch path skips telemetry allocations entirely when disabled.
- **Hot path**: UTF-8 validation delegates to `node:buffer.isUtf8` above the
  native-call threshold (the single biggest receive-path gap against `ws` —
  up to 70x on large frames); the frame parser stopped copying the 4-byte
  mask and allocating a `Result` per not-enough-bytes attempt; fragmented
  sends cork once per message instead of per fragment; client batching
  serializes each packet once (enqueue) instead of twice; error logging
  materializes `error.stack` only when a logger is enabled.
- **A real SECURITY.md** (scope, acknowledgement and fix windows, supported
  versions) and a stability/deprecation policy in CONTRIBUTING, with
  `@experimental` markers on the telemetry shapes and the engine-port
  `capabilities`.

### Changed (breaking)

- **`Client.emit` is the local `Emitter` emit again.** It used to send a
  wire event for every name except `'close'`, which made `client.on('x')`
  dead code and the class unsubstitutable for its base. The wire send is
  `client.sendEvent(name, data)`, as it always was.
- **Server-minted SSE channel ids.** A channel id is no longer proposed by
  the client: the server mints it and hands it out once, in the `ready`
  frame. The channel is bound to the cookie identity that created it —
  re-attaches and POSTs presenting a different identity answer 403, an
  unknown id answers 409 (the client's signal to start a fresh channel).
  Knowing an id is no longer enough to act on someone else's channel.
- **5xx error messages no longer travel.** A 4xx message (validation,
  quotas, refusals) is written for the caller and goes verbatim; a 5xx is a
  server internal — the peer now gets the status line and the details stay
  in the server log, correlated by packet id. `error.expose = true` opts a
  message in; wrpc's own coded errors are marked.
- **`cors.origins` is enforced on HTTP calls**, not only on the WebSocket
  upgrade: a request from a disallowed browser origin is refused with 403
  instead of running with the CORS grant withheld.
- **Unknown `access` values throw at router construction.** Anything but
  `'public'`/`'session'` used to silently mean "any session" — an access
  model that looks custom but is not is an auth bug waiting. Custom policies
  are hooks' job.
- **The per-call success log line moved to `debug`**, and a Console sink
  drops `debug` outright: a default console server reports what went wrong
  instead of narrating every call. Structured loggers keep all five levels.
- **`maxBackpressure` defaults to `maxBuffer`** (was: unbounded), arming the
  terminate-on-exceed path out of the box; control frames (ping/pong) now
  respect the cap too, so a ping flood cannot grow the write queue without
  limit. `0` opts back into unbounded.

- **A structured `logger` option, replacing `console`.** Every component writes
  through one injected writer built by `src/logging.js` — a zero-import module
  that normalizes three shapes into one: a structured logger called as
  `(entry, message)` (pino, bunyan, winston — identified by `child` or
  `level`), a `Console` called as `(message)`, or nothing at all. `false`
  silences a server outright, which the old option could not express; an
  object matching neither shape disables logging rather than throwing, because
  observability must never be why a server fails to boot. wrpc binds children
  itself — `component` per subsystem, `peer` per connection, and a lazily built
  `callId` per call, reachable from a handler as `context.log`. A logger that
  throws is contained inside the writer, so a broken sink costs the line, not
  the call. **`console` is gone**, not deprecated: the package has never been
  published, so there is nobody to migrate. The fastify adapter's `toConsole`
  shim went with it — `fastify.log` is a pino and now goes in as one.
- **Client logging.** `WrpcClient` takes a `logger` too, **off by default**: a
  browser console filling with reconnect noise is not a sensible default. When
  on, an error is logged *and* emitted as `'error'` — a logger observes, a
  listener handles, and having one does not silence the other.
  `WrpcClientProxy` forwards the option, so a Service Worker proxy keeps it.
- **Three silent failure paths now report.** A subscription that died
  server-side answered `end` and logged nothing; `runSubscription` dropped the
  generator's error entirely when aborted; and `jsonParse(data) || {}`
  conflated "malformed" with "empty", hiding every unparseable packet in the
  system behind one `||`.
- **OpenTelemetry traces and metrics.** `telemetry` accepts the
  `@opentelemetry/api` module or your own `{ tracer, meter }`; tracer-only and
  meter-only both work, and neither is ever imported — the constants that would
  require it (`SpanStatusCode.ERROR`, the `SpanKind` values) are frozen by the
  specification and hardcoded. Spans follow the OTel `rpc.*` convention and
  bracket the *whole* invocation, so an argument error gets an error span and a
  duration sample exactly as a slow handler does. Fourteen instruments cover
  calls, durations, connections, subscriptions and values yielded, broadcasts
  and fan-out size, stream bytes, backpressure, sessions and SSE channels.
  `includeIdentity: false` drops the peer address; a session token is never
  recorded at any setting, because a credential and an identity do not share a
  switch. Every recording path contains its own failures.
- **W3C trace context across the wire.** `call`, `subscribe` and `event`
  packets may carry optional `tp` (traceparent) and `ts` (tracestate) fields,
  so a client span parents the server span through the network hop — the thing
  an in-process context manager cannot do. Context is per *packet*, so each
  call in a batch keeps its own parent. wrpc does not parse the W3C format: it
  hands the field to your propagator, which is why propagation needs `{ api }`
  (or an explicit `propagation`) rather than a bare tracer.
  `trustRemoteContext` defaults to true, as in gRPC and HTTP instrumentation;
  set it false when peers are untrusted and can forge trace ids.

### Changed

- Server transports carry a `kind` — `ws`, `http`, `sse` or `event` — the
  metric attribute and log field identifying which wire a client is on.
- `src/telemetry/` is three files rather than one. Putting the client half in
  the same module as the server's twelve instruments pushed the browser bundle
  to 10.1 KB, over its 10 KB budget; splitting shared/server/client so
  `src/client.js` requires only what a client needs brought it back to 9.2 KB.

### Documentation

- **The wire protocol is frozen as 1.0.** `docs/reference/protocol.md` was
  documented per phase and explicitly provisional; it now states what
  `@alexify/wrpc@1.x` guarantees instead. Packet types, their fields and their
  meanings do not change within the major version; a new *optional* field may
  be added, and an unknown packet type is still answered with a `callback`
  carrying code 500, which is what makes an additive change safe for an older
  peer. Error codes keep their meanings. Anything else is a major version.
- Two new guide pages — **Logging** and **OpenTelemetry** — under a new
  *Operations* sidebar group, and a **Trace context** section in the wire
  protocol reference documenting the `tp`/`ts` fields.
- **The documentation site is the full structure**, not a skeleton. Sixteen
  guide pages — getting started, server, router, sessions, rooms,
  subscriptions, streams, scaling, client, typed client, CLI, TanStack Query,
  SSE, and one per adapter — plus three references: the wire protocol, the
  **wire format** (binary chunk framing, the RFC 6455/7692 engine, backpressure
  accounting, payload ownership) and the **engine port** (the `Engine` /
  `WrpcSocket` contract, hosted vs standalone, capabilities, and how to
  contract-test a new engine). `docs/.vitepress/config.mts` gains the grouped
  nav and sidebar, keywords, JSON-LD and per-page canonical URLs.
- **`docs/guide/getting-started.md` was wrong, not merely thin.** It still
  documented the `application.getMethod()` contract that F2 deleted, so its
  quick start could not run. Same for the README's, which additionally
  advertised `Server(application, options)`.
- **README rewritten**: badges, a positioning table against tRPC and Socket.IO,
  an honest *When NOT to use wrpc*, the real feature matrix, an exports table
  with one row per subpath and a deep link each, and the bundle-size table
  generated by `pnpm size` — including which entries carry a CI-enforced
  budget and why the Node-only rows do not.
- **`CONTRIBUTING.md` added**, with the development workflow, the house rules
  that are not visible in the code (zero dependencies including no peers or
  optionals; a `.d.ts` change and its `tsd` test land together; the uws
  teardown trap that wedges `node --test`) and the **manual release
  checklist** — every step the tooling deliberately does not do, from
  `npm publish --dry-run` against the `files` allowlist to verifying the packed
  tarball actually resolves.

### Fixed

- **`client.close()` now ends every live subscription, not just the iterated
  ones.** It called `record.stream?.end()`, so an `iterate()` consumer's
  `for await` finished — while a `subscribe()` consumer that passed
  `onData`/`onEnd`/`onError` was told nothing at all. Its `onEnd` never fired,
  so a listener-based caller (the `@alexify/wrpc/query` cache bridge, or any
  React effect) had no signal that its feed was dead.
  `close()` now delivers the same terminal callback an `end` packet does.
  `unsubscribe()` stays silent on purpose — the caller named that one feed and
  already knows, whereas `close()` is usually called by something else
  entirely (a page teardown, a shutdown hook), so the code owning the feed
  never asked for it to stop. Exactly one of `onEnd`/`onError` fires, and only
  for an ending the caller did not ask for; a reconnect is not an ending.
  Two smaller defects fell out of the same path:
  - A **throwing terminal listener no longer skips `stream.end()`** — on the
    ordinary `end` packet too, not only on close. It used to leave an
    `iterate()` consumer parked in `next()` with nothing else coming. Each
    listener is now contained and escalated through the client's `'error'`
    channel, so one bad listener cannot rob the next of its signal or abandon
    the rest of `close()`'s teardown.
  - `close()` did not call the internal `onRelease` hook, so an `iterate()`
    passed a caller-supplied `AbortSignal` left its `'abort'` listener
    registered on that signal after the client was gone.

### Added

- A **real Redis** integration test, `tests/scaling/redis.integration.test.js`:
  two independent `createRedisAdapter` instances over separate connections —
  what two processes sharing a Redis actually look like, unlike
  `MemoryBackplane` where both hold the same object — checking cross-instance
  room delivery, echo suppression, broadcast, and prefix isolation. This is a
  manual/local run (`REDIS_URL=redis://127.0.0.1:6379 node --test
  tests/scaling/redis.integration.test.js`), not a CI job: `pnpm test` already
  covers the adapter's contract through the in-repo ioredis-shaped fake in
  `tests/scaling/redis.test.js`, and the file skips itself without
  `REDIS_URL`. `ioredis` joins the devDependencies for it and nothing else.
- **`socket.io` and `tRPC` (over `wsLink`) join `bench/rpc-comparison.js`.**
    Until now every compared stack was a raw transport running a minimal echo
    envelope — a useful floor, but not a comparison against anything that does
    wrpc's job. Each measurement also runs **pipelined at 64 calls in flight**
    alongside the sequential one, because the two answer different questions:
    sequential measures latency, pipelined measures throughput, and a stack
    with a fixed per-call delay reads very differently under them (tRPC's
    wsLink is ~0.2x of wrpc pipelined and ~0.03x sequential — the gap is
    latency, not per-call cost). All four packages are devDependencies used
    only by the benchmark.
- Typed client, codegen and TanStack Query bindings (F6) — the DX phase, with
  **no TypeScript at runtime**: everything below is either a type or a
  dependency-free JavaScript file.
  - **Contract-first typed client.** Declare the api once as an ordinary
    interface and thread it through `connect<Api>()`:
    ```ts
    interface Api {
      chat: {
        send(args: { text: string }): Promise<{ id: string }>;
        onMessage: SubscriptionContract<{ room: string }, { text: string }>;
      };
      'auth.1': { signIn(args: { login: string }): Promise<{ token: string }> };
    }

    const client = await connect<Api>('wss://host');
    await client.load('chat');
    const { id } = await client.api.chat.send({ text: 'hi' });
    ```
    Nothing is generated and nothing is checked at runtime — `connect` is a
    one-line alias of `WrpcClient.connect`, added because a *function* is
    where a type argument reads naturally. A call keeps its declared arguments
    and gains the trailing `CallOptions` that carries `{ signal }`; its result
    is awaited whether the contract promised one or not. A member declared as
    `SubscriptionContract<Args, Data>` becomes `subscribe`/`iterate` rather
    than something callable, so calling a subscription is a compile error.
    `load()` only accepts unit keys the contract declares. Utilities:
    `TypedApi`, `TypedUnit`, `TypedMethod`, `TypedParams`,
    `TypedSubscriptionMethod`, `InferArgs`, `InferResult`, `FirstArg`,
    `UntypedApi`, `IsAny`, `InvalidContractMember`.
    **Without** a contract everything is exactly as loose as before — that is
    what the `IsAny` guard is for, and `tests/index.test-d.ts` asserts it.
    Three sharp edges the types deliberately handle:
    - A **zero-argument** member keeps its args slot — `ping(undefined, {
      signal })`, never `ping({ signal })`. Slot 0 on the wire is always the
      procedure's arguments, so collapsing the tuple would compile a call that
      ships `{"signal":{}}` as the args and silently drops the cancellation.
    - A member declared with **two parameters** (or a rest parameter, or as
      something that is not a function) maps to `InvalidContractMember`, whose
      text the compiler quotes back — a wrpc procedure receives exactly one
      args object.
    - A contract key named **`on`** is not mapped: a unit is an `Emitter` at
      runtime, so `api.chat.on` stays the listener registration. (An optional
      `on?: {...}` used to reduce the whole unit to `never`.)
  - **`wrpc types` — the codegen CLI.** `npx wrpc types http://host:8000/api
    --out api.d.ts [--units chat,auth.1] [--interface Api] [--package ...]`
    (`--out -` writes to stdout) turns `system/introspect` into exactly the
    kind of interface above. The two halves meet in the middle: hand-write the
    contract for exact types, generate it when you would rather not write it.
    Output is sorted, so re-running produces byte-identical bytes and a diff
    means the server changed.
  - **The `signature` descriptor is now specified** (it had been carried but
    never interpreted) — see `docs/reference/protocol.md`. A shape is a type
    name (`'string'`, `'number[]'`, `'string|null'`), a field map whose keys
    may end in `?`, or a one-element array meaning "an array of that";
    `args`/`returns`/`data` are the three slots. It is deliberately a **closed**
    format: it crosses the network and lands in a file someone compiles, so
    names are quoted through JSON escaping and type names are matched against
    an allowlist — anything unrecognised becomes `unknown` with a warning on
    stderr, nesting is depth-capped, and a `__proto__` key is data. A method
    with no signature still generates, as `(args?: unknown) => Promise<unknown>`.
    `Signature`/`SignatureShape` type the descriptor, so a tuple where an array
    belongs is a compile error rather than a silent `unknown`.
  - **`@alexify/wrpc/query` subpath** — TanStack Query bindings as option
    *factories* in the style of tRPC v11, never hooks, so one file serves
    React/Solid/Svelte/Vue Query and query-core alike:
    ```js
    const wq = createQueryUtils(client, { queryClient });
    useQuery(wq.queryOptions(['chat', 'list'], { room: 'a' }));
    useMutation(wq.mutationOptions(['chat', 'send']));
    const feed = wq.subscriptionHandler(['chat', 'onMessage'], { room: 'a' });
    ```
    It `require()`s **nothing** — 1.0 KB min+gzip, browser-safe, with the
    client and the `QueryClient` injected and duck-typed like every other
    optional integration here. The `AbortSignal` TanStack hands `queryFn` is
    forwarded into the call, so a cancelled query reaches the server as
    `{type:'cancel'}`. `queryKey` is `[...prefix, unit, method, args]` —
    args last, so `invalidateQueries({ queryKey: ['chat'] })` matches by
    prefix. `subscriptionHandler` writes every value through `setQueryData`
    (replacing by default; pass `update` to accumulate) and needs nothing on
    reconnect, because the client re-opens subscriptions on the same record.
    Paths resolve *lazily* inside `queryFn`, since `client.api` only exists
    after `load()` and is rebuilt on every reconnect — and resolution uses
    own-property lookups on both hops, so `['chat', 'on']` cannot reach
    `Emitter.prototype.on` and be called as though it were a procedure.
  - **Bundle-size budgets, and a real self-containment gate.**
    `scripts/size.js` now carries a min+gzip budget on every browser-reachable
    entry and **fails** when one is exceeded, so CI notices bundle growth
    instead of merely printing it. The old "does the output contain the string
    `node:crypto`" check is replaced by an esbuild **resolve plugin**: a browser
    entry may only import relative paths, so a stray `node:*` *or a package*
    fails the build naming the import and its importer. That second half
    matters — esbuild resolves devDependencies happily, so
    `require('@tanstack/query-core')` in `src/query/` would otherwise have been
    inlined into a browser bundle and shown up only as a few extra KB.
- Subscriptions, batching, cancellation and SSE (F5) — **wire protocol v2**:
  `subscribe` / `data` / `end` / `unsubscribe` / `cancel` packets, plus a
  JSON array as a batch frame. All of it is in
  `docs/reference/protocol.md`.
  - **Subscription procedures** — a procedure that answers with a *stream*
    of values, written as an async generator:
    ```js
    onMessage: procedure.subscription({
      handler: async function* (ctx, args, { lastEventId, signal }) {
        for (const missed of log.since(lastEventId) ?? []) yield missed;
        yield* createEventStream({ signal });
      },
    })
    ```
    An async generator handler IS the declaration — `procedure.subscription`
    is only needed for a plain function returning an async iterable — and
    introspection carries `kind`, so the client scaffolds `subscribe`/
    `iterate` instead of a callable. `queue` and `timeout` are *refused* on a
    subscription rather than quietly meaning something else. The pump
    respects transport backpressure (it waits for `'drain'` before pulling
    the next value), and always answers `end` exactly once — completion,
    a generator that threw, an unsubscribe, or a disconnect — after which
    the generator is closed so its `finally` runs.
  - **Resume**: `tracked(eventId, data)` labels a value, and the client sends
    the last one back as `lastEventId` after a reconnect. `createEventLog({
    size })` is the ring buffer behind that: `since(lastEventId)` returns
    what was missed, or **`null`** when the id has fallen out of the buffer —
    an honest "cannot resume" instead of a silently truncated history.
  - **`createEventStream()`** (also on the browser entry) — the push→pull
    adapter between "something calls me with a value" and "someone is
    `for await`-ing values". Bounded: a producer that outruns its consumer
    drops the *oldest* value and reports it through `dropped`.
  - **Cancellation**: `client.api.unit.method(args, { signal })` sends
    `{type:'cancel'}`, the caller is rejected with **499**, and
    `ctx.signal` is aborted. Best-effort by nature — a handler that ignores
    its signal keeps running — but its late result is *dropped*, never
    delivered to a caller that already gave up. A call cancelled while still
    in a batch queue is removed rather than racing a `cancel` ahead of the
    `call` it refers to. A disconnect aborts everything still running for
    that peer.
  - **Batching**: `batch: { flush: 'microtask' | ms, maxSize, maxBytes }`
    coalesces calls issued in one tick into a single frame. Only `call`
    packets batch — a ping, a cancel or an unsubscribe is a control packet
    whose whole point is to arrive now — and a batch of one is sent bare.
    The server accepts arrays on every transport; on HTTP the answers come
    back as one array **in request order**, so requests and responses zip
    positionally. `maxBatch` (128) caps a frame, because one frame asking
    for unbounded work is otherwise a denial of service, and
    `maxSubscriptions` (256) caps concurrent generators per client.
  - **`@alexify/wrpc/sse` subpath** — Server-Sent Events as a full transport,
    not just a one-way feed. A channel is two halves joined by an id:
    `GET {basePath}/events?channel=<id>` for the server→client stream and
    `POST {basePath}` with `x-wrpc-channel` for the other direction, both
    belonging to ONE server-side client — which is what lets a subscription
    opened by a POST deliver its values down the stream. The POST answers
    `202`; every reply travels on the stream. A dropped stream does not
    destroy the channel: it is held for `retention` (30 s), so a reconnect
    with **`Last-Event-ID`** re-attaches and replays the frames it missed,
    subscriptions included. Comment frames keep proxies from timing an idle
    stream out and `X-Accel-Buffering: no` keeps nginx from buffering it.
    The client half is browser-safe (`fetch` + a hand-written incremental
    parser, deliberately **not** `EventSource`, which cannot set headers or
    be aborted cleanly) and registers as `WrpcClient.transport.sse`;
    `connect(url, { transport: 'sse' })` selects it. Binary streams are
    *refused* on it rather than silently corrupted — SSE frames are text.
  - `HttpCall` grew an optional **`stream({ status, headers })`**: the seam
    that lets a host keep a response open. The node shell, express and
    fastify all implement it; a host that cannot answers the events endpoint
    with 501 rather than a response that never arrives.
  - `Context.signal`, `Client.calls`, `Client.subscriptions`, `Client.drain()`
    and `Client.binary`; `RpcServer.eventsPath` and `RpcServer.sse`.

### Fixed

Found by an adversarial review of F5 itself, each reproduced before fixing:

- **Cross-origin SSE could not work at all.** The channel POST answered its
  `202`/`404` without the CORS headers every other HTTP response carries, so
  a browser on another origin could not read the result — and the default
  `Access-Control-Allow-Headers` was `Content-Type` alone, which failed the
  preflight before the POST was ever sent, since neither `x-wrpc-channel` nor
  the `last-event-id` resume header is CORS-safelisted. Both are now in the
  default and the channel POST carries the same headers as everything else.
- **An aborted HTTP batch never answered.** `ServerHttpTransport.close()`
  called `error(503)`, which in batch mode only *collects* one more answer and
  keeps waiting for the rest — so a shutdown mid-batch hung the request and
  never evicted the client. It now responds immediately, filling every
  still-unanswered id with its own error packet rather than emitting a single
  id-less one the caller cannot route.
- **A malformed element in a batch lost its id.** A structure error answered
  with no id, so the response array's positional ordering — the guarantee the
  protocol makes about a batch — broke for that slot and shifted every answer
  after it. The packet's own id now travels with the refusal.
- **An SSE channel's client never restored its session cookie.** The channel
  was created without the opening GET's headers, so a browser holding a valid
  session started the stream anonymous and every `access: 'session'` procedure
  on it answered 403 until it signed in again on that channel. Those headers
  are now handed on and the session is restored exactly as `attachSocket`
  does for an upgrade.
- **An SSE reconnect killed every subscription instead of resuming it.** A
  channel outlives its stream, so a reconnecting peer re-opens its
  subscriptions on the very client that still holds them — and the
  duplicate-id guard refused them, orphaning the old generator. Re-subscribing
  an id the same client already has is now a *resume*: the previous run is
  aborted and replaced.
- **The subscription pump parked forever after an SSE stream re-attached.**
  While detached, `writeFrame` reports backpressure, so the pump waited on
  `'drain'` — which the new response could never emit, because the parked
  pump was what would have triggered it. A fresh sink now announces itself.
- **A stale SSE writer's close tore down the live one.** `#detach` could not
  tell which writer had closed, so the late close of a response already
  replaced by a reconnect detached the healthy stream. Detach and the
  heartbeat are now scoped by writer identity, and a replaced response is
  ended rather than leaked open.
- **`SseChannels.close()` left its responses open**, so a host waiting on
  `server.close()` waited forever; a channel whose host could not start the
  stream at all was left behind unreachable.
- **The fastify plugin never routed `{basePath}/events`** — it fell into the
  `/:unit/:method` route, where the core never saw it as an events request.
- **Cancel and unsubscribe were registered after the first `await`**, so one
  dispatched in the same turn (batched behind its own call) found nothing to
  cancel. Both controllers are now registered synchronously.
- **One unroutable answer stranded the rest of a batch**: the client's array
  loop had no per-item containment, contradicting the guarantee the protocol
  makes about a failure inside a batch.
- **Cancelling a call over HTTP threw at the client**, which sent a `cancel`
  the transport cannot carry and then could not route the 400. A transport
  now declares whether it is persistent, and the 499 acknowledging a cancel
  is recognized rather than reported as an unknown callback.
- **`iterate()` leaked an abort listener** on every natural end, and opening
  it with an already-aborted signal started a subscription nobody could stop.
- **`WrpcClientProxy` leaked a pending-port entry per subscription**: `end`
  is a terminal packet and now releases its slot like `callback` does.
- `index.d.ts` caught up with the backpressure booleans (`Client.send`,
  `ServerTransport.send`/`error`, `ServerHttpTransport.write`) and
  `createContext(signal)`.

### Changed

- `ServerTransport.send()` now returns the transport's backpressure signal,
  so a producer can wait for `'drain'` instead of buffering without limit.
- Every shell and adapter now funnels its option bag through `rpcOptions()`
  rather than re-listing the core's options by hand — adding a core option
  had silently required editing four files, and `maxSubscriptions` was
  dropped by all of them on the first try.

- Realtime and scaling (F4):
  - **Rooms in the core** (`src/rpc/rooms.js`) — named groups of clients on
    top of the existing `{ type: 'event' }` packets, so no new wire type was
    needed. `ctx.client.join(room)` / `.leave(room)` / `.rooms` / `.in(room)`
    on the client side; `server.to(room).emit(name, data)`,
    `.to(a, b)` (a union, each client once), `.except(client)`, `.local()`
    and `server.broadcast(name, data)` on the server. `emit()` returns how
    many clients received the event **on this instance**. A `RoomRegistry`
    owns both directions — which clients a room holds and which rooms a
    client joined — so a disconnect only has to call `leaveAll`, and there is
    no per-client room state left to drift. `Broadcast` is immutable: every
    modifier returns a new target, so a stored `server.to('chat')` cannot be
    narrowed by an `.except()` somewhere else. `to()` with no rooms reaches
    **nobody** rather than everybody — `to(...list)` with a list that came
    back empty is the `WHERE id IN ()` mistake, and broadcasting to the whole
    server is the wrong way to lose that argument. A client on a transport
    that cannot carry events (HTTP) is skipped rather than throwing
    mid-fan-out, and one dead socket does not truncate the rest of the
    delivery.
  - **`Context.server` / `Client.server`** — how a handler reaches rooms
    (`ctx.server.to(room).emit(...)`) without closing over a server that
    could not exist before the router it was built from.
  - **Client → server events**: a unit definition's reserved **`on`** key
    declares inbound event handlers
    (`defineRouter({ chat: { on: { typing: handler } } })`), reachable from
    the client with `client.sendEvent('chat/typing', data)`. Handlers are
    ordinary procedures, so `access`, `input` validation, `timeout` and
    `queue` all apply. Events stay fire-and-forget in both directions: a
    rejected one (unknown handler, missing session, failing handler, invalid
    input) is recorded in the server log, because a packet with no id has
    nothing to answer on. `on` is consequently **not** usable as a method
    name, and it is not introspected.
  - **`@alexify/wrpc/scaling` subpath** — the rooms backplane. The contract
    is structural (`publish(channel, message)`,
    `subscribe(channel, handler) -> unsubscribe`, `close()`), with a built-in
    `MemoryBackplane` and an ioredis-shaped `createRedisAdapter({ pub, sub,
    prefix })` that defaults `sub` to `pub.duplicate()`. Per the
    zero-dependency rule the Redis clients are **injected and validated
    structurally** — `ioredis` is not even a devDependency; an in-repo fake
    encodes the contract the adapter actually depends on. `close()` releases
    the adapter's own subscriptions and listener but never quits the injected
    clients.
  - **Cross-instance fan-out**: `new Server({ backplane })` /
    `new RpcServer({ backplane, instanceId })` publishes every non-local emit
    as `{ v, instance, rooms, name, data }`. Each instance drops its own
    envelopes (echo suppression by instance id). Channel scheme: an emit
    targeting exactly one room goes to that room's channel — subscribed only
    while the room has local members — and everything else to the single
    `broadcast` channel every instance holds, so an envelope reaches an
    instance through exactly one channel and needs no receiver-side
    deduplication. Backplane failures are isolated in every direction: a
    broker that throws, rejects, or refuses a subscription is reported and
    local delivery continues. Delivery is honestly documented as
    **at-most-once**.
  - **Client resilience** (`src/client.js`):
    - Reconnect is now truncated exponential backoff with **full jitter**
      (`reconnect: { minDelay, maxDelay, factor, jitter, retries }`) instead
      of a fixed 2 s, with `reconnecting` / `reconnect-failed` events and a
      `client.attempt` counter. A rejected `open()` never emitted `'close'`,
      so nothing rescheduled the next try — the loop used to give up after
      one retry; it now continues from the failure itself.
    - **App-level heartbeat** (`{type:'ping'}` / `{type:'pong'}`,
      `heartbeat: { interval, timeout }`). A browser `WebSocket` exposes no
      protocol-level ping, so a connection that died without a close frame
      stays "open" until the first call times out. Only the WebSocket
      transport starts one; both sides answer an inbound ping.
    - **`'reconnect'` fires after the api is rebuilt**: every loaded unit is
      reloaded (the new connection is a new server-side client), methods the
      server no longer exposes are removed, and the `api` unit objects are
      **reused** so listeners registered on them survive the outage — the
      "api quietly went stale after a reconnect" bug.
    - An event that reaches no listener — an unloaded unit, or a loaded one
      nobody subscribed to — now surfaces as **`'unhandled-event'`** instead
      of vanishing.
    - A background failure with no `'error'` listener is logged rather than
      thrown: an `Emitter` throw out of a reconnect timer would take the
      process down for a retry that was about to happen anyway.
  - `docs/reference/protocol.md` documents the wire protocol, including the
    new event/heartbeat packets, the room envelope and the reconnect
    schedule.

### Fixed

- **A graceful WebSocket close cost a full second.** Answering a peer's Close
  frame armed the same `closeTimeout` the initiating side uses, so both peers
  sat waiting for the other to hang up. RFC 6455 5.5.1 puts that duty on the
  side *answering* the Close: it now writes the echo, half-closes so the echo
  flushes, and destroys after a short grace. Every graceful disconnect —
  including each reconnect and each test teardown — drops from ~1 s to a few
  milliseconds. Frames pipelined behind that Close in the same TCP segment
  are no longer acted on either (RFC 6455 5.5.2 exempts an endpoint from
  answering a Ping received after a Close) — answering one would have written
  past the half-close.
- **An `event` or `pong` packet over HTTP hung the request.** Both are
  fire-and-forget, and an `ServerHttpTransport` only answers when something
  writes — so the response was never sent and the server-side `Client` was
  never evicted, letting an unauthenticated peer pin clients and sockets for
  as long as it held the connection. Events now require a persistent
  transport (a 400, mirroring the guard `stream` packets already had) and a
  stray `pong` falls through to the usual structure error.
- **A malformed packet target could take the process down.** `handleMessage`
  checked `method`/`name` for truthiness only, so `{"type":"event","name":42}`
  threw inside an un-awaited async dispatch — an unhandled rejection, which
  node terminates on by default. Targets are now type-checked, and every
  fire-and-forget dispatch has a terminal catch.
- **A socket abandoned by `terminate()` could close its own replacement.**
  The transport's close handler was scoped to the transport, not to the
  socket it was registered for, so the late `'close'` of a socket the
  heartbeat had walked away from cleared the `#socket` of the connection the
  reconnect had already established. It is now ignored by socket identity,
  which also subsumes the `'close'`-plus-`'error'` double fire.
- **A wire event named after an `Object.prototype` key was misrouted.** The
  client's `api` is a plain object, so `constructor/ping` resolved up the
  prototype chain and `listenerCount` was called on `Object`. Only an
  `Emitter` the client put there itself counts as a unit now, and `load()`
  defines its units instead of assigning them (a unit named `__proto__` went
  through the prototype setter).
- **`reconnectTimeout` above the default cap reconnected faster than asked.**
  The shorthand skipped the `maxDelay >= minDelay` clamp that the explicit
  `reconnect` object gets, so `reconnectTimeout: 60000` was silently capped
  back to 30 s. Both spellings now mean the same thing.
- **The reconnect timer is no longer `unref`'d.** During an outage there is
  no socket left holding the event loop open, so a process whose only work is
  a wrpc client exited mid-reconnect. Heartbeat timers stay unref'd: a live
  socket is already a ref'd handle.
- **`createRedisAdapter` stranded the subscriber it created.** When `sub` is
  omitted the adapter opens one through `pub.duplicate()`, and that
  connection is reachable from nowhere else — `close()` now quits it. An
  injected client is still never quit.
- `ClientWsTransport.terminate()` drops a connection without waiting for a
  close handshake, and the heartbeat uses it: a peer that stopped answering
  will never complete one, so waiting would gate the reconnect on the
  socket's own timeout.

- Server adapters (F3):
  - **`@alexify/wrpc/uws`, `@alexify/wrpc/fastify`,
    `@alexify/wrpc/express` subpaths**, each following the established
    convention (root shim + hand-maintained root `<name>.d.ts` +
    `exports` entry + `files` allowlist entry + `tests/<name>.test-d.ts`).
    The host framework is always *injected*, never required: `uWebSockets.js`,
    `fastify`, `fastify-uws` and `express` are devDependencies used by the
    adapter tests alone, so `package.json` still has no `dependencies`
    field and installing wrpc still pulls nothing.
  - **Standalone engines** — the Engine port grew a second kind. A hosted
    engine (the default) attaches to a listener someone else owns
    (`attach({ server, ... })`); a *standalone* engine (`standalone: true`,
    uWebSockets.js) owns the whole network stack, `node:http` included, so
    it is attached without a server (`attach({ path, verifyClient,
    onHttpCall, ... })`) and must implement
    `listen({ host, port }) -> Promise<address>`. `onHttpCall` receives
    exactly the abstract call description `RpcServer.handleHttpCall`
    consumes, which is what lets the engine own HTTP without the core
    learning anything about it. Consequently `Server.httpServer` is `null`
    under a standalone engine: use the new **`Server.address()`**, which
    reports the bound address whichever side owns the listener, instead of
    `server.httpServer.address()`. The bind-retry loop was factored to run
    over either shape, so `EADDRINUSE` retries still work.
  - **`WebsocketServer` without a server**: the `server` option is now
    optional and the new public **`handleUpgrade(req, socket, head)`** drives
    one handshake by hand. That is what lets a middleware adapter perform the
    upgrade from a listener it does not own — express registers
    `wrpc.upgrade` on its own `'upgrade'` event. The manual path carries the
    same guarantees as the bound one: an error handler is installed on the
    raw socket before parsing, and a throwing handshake answers `500` rather
    than leaving the socket dangling.
  - **uws engine adapter** (`createUwsEngine({ uws, app, ssl, idleTimeout,
    compression, ... })`), normalizing uws onto the `WrpcSocket` contract:
    - uws' tri-state `send()` collapses to the port's boolean —
      `SUCCESS` is `true`, `BACKPRESSURE` is `false` (buffered, wait for
      `'drain'`), and `DROPPED` **fails loudly**: it emits an `'error'` and
      terminates the connection, because a silently discarded frame leaves a
      hole in the frame stream and a corrupted RPC protocol is worse than a
      dead socket.
    - every payload is **copied at the callback boundary** (`message`,
      `ping`, `pong`, `close`, and HTTP body chunks): uws neuters the
      `ArrayBuffer` when the callback returns, while the RPC core hands
      chunks to asynchronous stream consumers.
    - `remoteAddress` is **captured during upgrade** off the response object,
      since the ws handle reports an empty address once the upgrade completed.
    - a **poisoned-handle guard**: uws invalidates the handle inside its own
      close callback and every method on it throws afterwards, so `UwsSocket`
      latches a `closed` flag and wraps each call — a closed socket reports
      `bufferedAmount === 0`, refuses sends with `false`, and swallows
      `close`/`terminate` instead of throwing at the caller.
    - `listen()` tags a refused bind as `EADDRINUSE` (uws reports only
      success/failure, never a reason) so the `Server` shell's retry keeps
      working, and only an app wrpc created is closed on teardown — one
      handed in by fastify-uws belongs to fastify.
  - **Capability differences are real behavior differences**, not cosmetics.
    `ping: false` — uws owns liveness itself through `idleTimeout` plus
    `sendPingsAutomatically`, so the RPC layer must not run a heartbeat of
    its own. `pause: false` — uws exposes no socket-level pause, so there is
    no receive-side flow control: unlike the node engine, where F1's
    end-to-end stream backpressure propagates through TCP, a fast uploader is
    **not** throttled by a slow stream consumer. `deflate` is true only when
    a `compression` option is passed.
  - **fastify plugin** (`wrpcFastify`) with two backends chosen by feature
    detection: a plain `fastify()` runs on a real `http.Server` and gets the
    node engine attached to its `'upgrade'` event, while
    `fastify({ serverFactory })` from `fastify-uws` runs on a uws-backed fake
    server and gets a uws engine over that same uws app (located through
    fastify-uws' private `uws.app` symbol — it exports no accessor, and the
    alternative was not supporting the only uws-backed fastify there is).
    HTTP calls are served by real fastify routes, so the app's hooks, auth and
    error handling run **before** wrpc sees the call. The plugin decorates the
    instance with `wrpc` and tears the core down on `preClose`, and escapes
    fastify's encapsulation via `skip-override` rather than by taking
    `fastify-plugin` as a dependency. Without an explicit `console` option the
    plugin logs through fastify's own logger, *adapted* rather than passed
    straight down: fastify's default is pino, which has `info`/`warn`/`error`
    but no `log` — and the core calls `console.log` after writing every
    successful callback. Handing it over raw threw inside the dispatcher's
    `try`, which then answered a second time, so over WebSocket (where a
    transport has no responded-once guard) every successful call came back as
    the result *and* a spurious `500`.
  - **express (and bare `node:http`) adapter**: `createWrpc({ ... })` returns
    `{ rpc, engine, wsServer, handler, upgrade, close }` and owns no listener
    — the app does. Middleware semantics are the point: a request outside
    `basePath` is passed to `next()` rather than answered with `404`, so wrpc
    composes with the rest of the app instead of swallowing its routes. An
    upstream body parser (`express.json()`) is respected when it already
    drained the stream; otherwise the body is read here. A standalone engine
    is refused with a clear message, since it cannot be driven from
    middleware.
  - **Shared engine contract, run against both kinds**: the suite in
    `tests/engine/engineContract.js` now boots through a harness
    (`hostedHarness`/`standaloneHarness`) instead of calling `attach()`
    directly, so the same assertions cover the built-in node engine and the
    uws adapter — including that payloads survive the callback that delivered
    them and that a closed socket stays quiet. Teardown is registered as an
    after-hook rather than trailed at the end of a test body: a standalone
    engine holds a native listen socket, and one leaked by a failing assertion
    would wedge the whole `node --test` run. On top of it, a **swap test**
    runs one behavioral spec (packet `POST`, REST `GET`, path miss, `OPTIONS`,
    cookie sessions, WS calls, server events, binary streams) against all five
    ways of standing wrpc up: `Server` over each engine, the fastify plugin
    over each backend, and express middleware.

- Server-agnostic RPC core (F2):
  - **Router/procedures** (`defineRouter`, `procedure`, `Router`,
    `Procedure`): units defined declaratively with versions as
    `'unit.version'` keys, bare-function shorthand, per-procedure
    `access` (default `'session'`), `input`/`output` validators (plain
    functions or Standard Schema objects; failures map to 400/500),
    `timeout` (408), `queue` concurrency limits backed by a `Semaphore`
    (503), `meta` and `signature` descriptors. `system/introspect` is
    auto-registered from the router, so `client.load()` works without
    hand-rolled introspection.
  - **`RpcServer`** — an engine-agnostic core with no `node:http` on the
    request path: `attachSocket(socket, meta)` accepts any
    WrpcSocket-shaped connection, `handleHttpCall(call)` consumes an
    abstract `{ method, url, headers, body, respond }` description
    (the seam for the F3 framework adapters), `attachPort` covers worker
    transports. The `Server` shell composes it with `node:http(s)` and a
    WebSocket engine.
  - **`@alexify/wrpc/engine` subpath**: the replaceable engine contract
    (`WrpcSocket`, `Engine`, capability flags) plus `createNodeEngine()`
    wrapping the built-in RFC 6455 implementation, and a shared engine
    contract test suite (`tests/engine/engineContract.js`) that F3 will
    run against the uWebSockets.js adapter. `Connection` gained the
    contract surface: a `close(code, reason)` alias, `remoteAddress`,
    and a `'close'` event carrying `(code, reason)`.
  - **CSRF protection for REST calls**: because HTTP calls now restore the
    session from the `SameSite=Lax` cookie, a cross-site top-level `GET`
    would otherwise run session procedures with ambient authority. Safe
    methods (`GET`/`HEAD`) therefore dispatch *without* the cookie session
    unless the request proves same-origin intent via Fetch metadata
    (`Sec-Fetch-Site`); non-browser peers, which send no such header, are
    unaffected.
  - **Store-backed sessions**: the module-global session `Map` is gone —
    each server owns a `SessionManager` over a structural `SessionStore`
    (`{ get, set, delete }`, `MemorySessionStore` built in, anything
    store-shaped injects via `sessions.store`). Session cookies are now
    actually read back: HTTP calls and WS upgrades restore the session
    automatically (`client.sessionReady` settles before access checks),
    cookies default to `HttpOnly; Secure; SameSite=Lax; Path=/`, and
    sessions survive disconnects (only `finalizeSession` or store expiry
    end them). Since sessions now outlive their connection,
    `MemorySessionStore` is bounded — LRU (`maxSessions`, default 10000)
    plus expiry (`ttl`, default 24h) — so an unauthenticated peer cannot
    grow it without limit.
  - **`basePath`** (default `'/api'`) applied uniformly: packet endpoint
    at `POST <basePath>`, REST at `<basePath>/unit/method`, WS upgrades
    gated to `'/'` and basePath paths; `''` serves from the root.
  - **CORS v2**: `cors: { origins: string[] | (origin) => boolean,
    credentials, headers, methods }` with per-request origin echo,
    `Vary: Origin`, credentials only for allowed origins, and an origin
    gate on WS upgrades; without the option the previous wildcard
    behavior is kept.

### Changed (breaking)

- `WebsocketServer`'s `server` option is optional: `new WebsocketServer()`
  no longer throws but builds an unbound server whose upgrades are driven
  by hand through `handleUpgrade(req, socket, head)`. Passing a non-server
  still throws, with a message narrowed from `'options.server (instance of
  http.Server) is required'` to `'options.server must be an http.Server'` —
  code asserting on the old text needs updating.
- `Server.httpServer` is `null` when the server runs on a standalone engine
  (uWebSockets.js), so `server.httpServer.address()` is not universally
  safe any more — use `server.address()`, which covers both engine kinds.
- `new Server(application, options)` is gone: the server takes a single
  options object with `router` (`new Server({ router, host, port,
  protocol, sessions, cors, basePath, console, engine, ws })`), and the
  metarhia-style `application.getMethod()` coupling is fully removed —
  procedures come from `defineRouter`, handlers receive
  `(context, args)`.
- `ServerWsTransport` is constructed as `(connection, meta)`;
  `ServerHttpTransport` now wraps an abstract call description instead
  of node `req`/`res`; `buildHeaders(cors, origin)` computes
  per-request CORS headers.
- `Client.restoreSession`/`finalizeSession` are async (store-backed);
  dropping a connection no longer deletes the session.
- The `utils` `Emitter` warns instead of throwing when `maxListeners`
  is exceeded (fan-out to many stalled streams is legitimate); the
  duplicate-listener and unhandled-`'error'` throws remain.
- `websocketPath` option removed — path gating follows `basePath` (or
  pass `ws: { path }` / `ws: { verifyClient }` through to the engine).

- WebSocket engine hardening (F1):
  - **Write backpressure**: `Connection` tracks the socket's writable
    buffer — data sends (`send`/`sendText`/`sendBinary`) return `false`
    when the socket is above its high-water mark, a `'drain'` event fires
    when pressure releases, `bufferedAmount` exposes queued bytes, and the
    new `maxBackpressure` option terminates peers that stop reading
    (emitting an error first). `sendPing`/`sendPong` fast paths report the
    real socket acceptance too.
  - **End-to-end stream flow control**: `WrpcWritable.write()` now returns
    the transport's real acceptance and emits `'drain'` (and `'close'` +
    a `closed` getter when the transport goes away mid-stream, so callers
    never wait for a drain that cannot come); `ServerWsTransport.write()`
    forwards the connection's boolean and re-emits `'drain'`; the RPC
    server pauses the socket while binary chunks are being consumed, so a
    slow stream consumer propagates pressure to the peer through TCP.
    `WrpcReadable`'s high-water mark applies only once a consumer
    attached — chunks that precede the consumer-starting call packet are
    buffered unconditionally (blocking there would deadlock the
    upload-then-call wire pattern). `Connection` gains
    `pause()`/`resume()`/`isPaused`, and the heartbeat spares paused
    connections (their pongs cannot be read while paused).
  - **O(n) receive path**: per-TCP-segment `Buffer.concat` accumulation is
    replaced by a `SegmentQueue` (bytes copied at most once, on
    cross-segment ranges); frame headers are parsed incrementally via the
    new `FrameParser.parseHeader`, and oversized frames are rejected on
    the header before their payload is buffered. Unmasking XORs 32 bits
    per iteration. Ownership contract: emitted payloads may share memory
    with the receive buffer — copy when retaining beyond the listener.
  - **Subprotocol negotiation**: `WebsocketServer` accepts
    `protocols: string[]` or `handleProtocols(offered, req)`; the selected
    protocol is echoed in the 101 response and exposed as
    `connection.protocol` (`handleProtocols` returning `false` rejects the
    handshake with 400).
  - **Outgoing fragmentation**: the `fragmentThreshold` option splits
    larger messages into CONTINUATION frames (off by default).
  - **permessage-deflate (RFC 7692)**, off by default: `perMessageDeflate:
    true | { threshold }` negotiates both directions with
    no-context-takeover, so each message is a self-contained deflate
    stream (one-shot zlib, `maxOutputLength` capped by `maxBuffer`);
    compressed text is UTF-8-validated after inflation; RSV bits are
    extension-aware (`allowedRsv`) instead of a blanket protocol error.
  - **API polish**: inbound pings are surfaced as a `'ping'` event (the
    automatic pong stays); `WebsocketServer.close({ code, reason })`
    gracefully closes all peers, stops the heartbeat, and rejects further
    upgrades with 503; `wss.connections` returns a snapshot of live
    connections.
- `@alexify/wrpc/ws` subpath (`ws.js` + hand-maintained `ws.d.ts`): the
  engine now has its own entry point with full typings for the new
  surface, covered by `tests/ws.test-d.ts`.
- Engine benchmarks (`bench/unmask.js`, `bench/parser-throughput.js`), an
  Autobahn Testsuite harness (`scripts/autobahn/`), and a 1 GiB stream
  memory guard (`pnpm test:perf`).
- Initial project scaffolding: package metadata, oxlint/oxfmt tooling,
  CI (lint/test/docs), `node --test` + c8 coverage setup, `tsd` type-test
  setup, bundle-size reporting (`pnpm size`), and a minimal VitePress docs
  site skeleton.
- WebSocket RPC protocol implementation: `WrpcClient`/`WrpcClientProxy`,
  `Server`/`Client`/`Context`/`Session`, chunked binary streams
  (`WrpcReadable`/`WrpcWritable`), and a from-scratch WebSocket server
  (`WebsocketServer`, `Connection`, `Frame`, `FrameParser`), all wired into
  the public `src/index.js` barrel and typed in `index.d.ts`. A slimmer
  `src/index.browser.js` excludes the Node-only server/transport modules.

### Changed (breaking)

- The WebSocket engine internals (`WebsocketServer`, `Connection`,
  `Frame`, `FrameParser`, `ParseError`, `PARSE_ERR_CODES`, `OPCODES`,
  `CLOSE_CODES`, `CLOSE_TIMEOUT`, `MAGIC`) moved from the main barrel to
  the `@alexify/wrpc/ws` subpath. The main entry keeps only the RPC-level
  API. (The package is unpublished, so this breaks no consumers.)
- Data send methods' boolean now means "the socket took the bytes without
  exceeding its buffer" rather than "accepted for send" — callers should
  wait for `'drain'` after a `false` return (checking `closed`, since a
  closed transport also reports `false` and will never drain).

### Fixed

- WebSocket heartbeat: terminating a dead peer now removes the connection
  from all internal collections synchronously, and the ping loop guards
  against stale entries instead of throwing inside `setInterval`; the ping
  timer is `unref()`ed so an idle server no longer keeps the process alive.
- `Connection.sendPing`/`sendPong` now return uniform booleans ("accepted
  for send") regardless of whether a payload is passed; `sendPing` is
  consistently refused (`false`) while the connection is closing. Pong is
  intentionally still allowed during the close handshake (RFC 6455 5.5.3).
- `WrpcClientProxy.open()` checked a non-existent `connected` flag on the
  underlying client; it now checks `active`, so an already-open connection
  is no longer redundantly re-opened.
- HTTP requests outside `/api` now receive an honest `404` response
  (previously the request was left hanging with no reply).
- `Server.listen()` no longer requires `options.timeouts` — the bind-retry
  delay defaults to 2000 ms.
- `Client.emit()` now returns a `Promise`, matching the base `Emitter`
  contract (it previously returned `undefined` despite the declared type).
- `index.d.ts` drift: `Connection.sendClose` is `void` (was declared
  `boolean`); transport subclasses (`ClientTransport`,
  `ServerHttpTransport`, `ServerWsTransport`, `ServerEventTransport`) are
  now type-only exports since they were never runtime exports; the real
  `proxy` client option is declared while the phantom
  `packetHandler`/`binaryHandler` options are removed; `Server` extends
  `Emitter`; `WrpcReadable.stop/pull/checkStreamLimits/waitEvent` and
  `WrpcWritable.init` are declared; the phantom `Options.kind`,
  `Options.ports`, and `ApplicationContext.static` are removed.

### Changed

- Removed the `metautil` dependency introduced while porting the protocol
  from `metarhia/metacom`: the handful of functions actually used
  (`Emitter`, `jsonParse`, `generateUUID`, and a few single-file helpers)
  are now copied directly into `src/`, keeping the package
  zero-dependency. `generateUUID` is split into `src/runtime/node.js` /
  `src/runtime/browser.js`, swapped via `package.json#browser`, matching
  the existing `chunks.js`/`chunks.browser.js` split.

This package has not been published to npm yet — there is no `[1.0.0]`
release section until the first publish.
