# Changelog

All notable changes to **`@alexify/wrpc`** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
