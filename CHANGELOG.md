# Changelog

All notable changes to **`@alexify/wrpc`** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- WebSocket engine hardening (Ф1):
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
  Autobahn Testsuite harness (`scripts/autobahn/`), a 1 GiB stream memory
  guard (`pnpm test:perf`), and a nightly CI workflow running both.
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
