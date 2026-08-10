# Changelog

All notable changes to **`@alexify/wrpc`** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
