# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

wrpc (`@alexify/wrpc`) is a fast, low-overhead, zero-dependency, WebSocket-based RPC protocol for Node.js and browsers.

**Protocol architecture.** The wire protocol is JSON packets (`{ type: 'call' | 'callback' | 'event' | 'stream', ... }`) over WebSocket or HTTP, plus a small binary framing (`chunkEncode`/`chunkDecode`) for streamed uploads/downloads. Module layout under `src/`:

- `src/client.js` — `WrpcClient`/`WrpcClientProxy`: the RPC client (WS/HTTP/ServiceWorker-event transports, call scaffolding via `#scaffold`, reconnect handling).
- `src/rpc/` — the engine-agnostic server core: `router.js` (`defineRouter`/`procedure`, versions as `'unit.ver'` keys, input/output validators, timeout/queue), `core.js` (`RpcServer` with `attachSocket`/`attachPort`/`handleHttpCall`, plus `Client`/`Context`), `dispatcher.js` (packet routing over a `Router`), `sessions.js` (`SessionManager`, structural `SessionStore`, `MemorySessionStore`, cookie building).
- `src/server.js` — the batteries-included `Server` shell: `node:http(s)` + a WebSocket engine (default `createNodeEngine()`) composed around `RpcServer`. Constructor takes ONE options object with `router` — the old `(application, options)`/`getMethod` API is gone.
- `src/engine/` — the replaceable server-side engine port (`WrpcSocket` contract + `createNodeEngine`), published as the `@alexify/wrpc/engine` subpath; the shared contract test suite lives in `tests/engine/engineContract.js`.
- `src/transport.js` — `ServerTransport` and its http/ws/event subclasses; per-request CORS headers (`buildHeaders(cors, origin)`), cookies. `ServerHttpTransport` wraps an abstract `{ method, url, headers, body, respond }` call (net-free — the seam for framework adapters); `ServerWsTransport` is `(connection, meta)`.
- `src/streams.js` — `WrpcReadable`/`WrpcWritable`: chunked binary stream classes shared by client and server.
- `src/chunks.js` / `src/chunks.browser.js` — binary chunk framing (Node `Buffer` vs `TextEncoder`/`TextDecoder`), swapped via `package.json#browser`.
- `src/websocket/` — a from-scratch WebSocket server implementation (handshake, framing, `WebsocketServer`, `Connection`, `SegmentQueue` O(n) receive buffering in `segments.js`, RFC 7692 permessage-deflate in `permessageDeflate.js`) used by `src/transport.js`; Node-only, never bundled for the browser. Published as the `@alexify/wrpc/ws` subpath (root `ws.js` shim + hand-maintained `ws.d.ts`) — engine internals are NOT exported from the main barrel.
- `src/utils.js` — cross-platform primitives ported from `metautil` (`Emitter`, `jsonParse`) — see "No runtime dependencies" below.
- `src/runtime/node.js` / `src/runtime/browser.js` — the one platform split needed beyond chunks: `generateUUID` (`node:crypto` vs `globalThis.crypto`), swapped via `package.json#browser`.
- `src/index.js` — the RPC-level barrel (client + server + streams + utils; websocket engine lives in the `./ws` subpath). `src/index.browser.js` — a slimmer barrel excluding server/transport (anything touching `node:http`/`node:https`).

**No runtime dependencies.** An earlier pass ported code structurally from `metarhia/metacom`, which depends on `metautil`. Rather than install `metautil` (breaking the zero-dependency guarantee), the handful of functions actually used (`Emitter`, `jsonParse`, `generateUUID`, plus a few single-file-use helpers like `parseCookies`/`parseHost`/`receiveBody`/`split`/`parseParams`/`isError`) were copied in directly and adapted. `package.json` has no `dependencies` field at all — keep it that way; anything reused from another package must be copied in, not installed.

Package manager is **pnpm** (`packageManager: pnpm@10.34.5`). CommonJS throughout (`require`/`module.exports`), no build/transpile step — `src/` ships as-is (same philosophy as the sibling `@alexify/kerberos` project).

## Commands

```bash
pnpm test              # run all tests: node --test (recursive discovery over tests/)
node --test tests/smoke.test.js            # run a single test file
node --test --test-name-pattern="..."      # filter tests by name
pnpm test:types        # type-check tests/*.test-d.ts against index.d.ts via tsd
pnpm test:coverage     # c8 coverage over src/ (thresholds: 95% lines/statements, 90% branches, 95% functions)
pnpm test:perf         # 1 GiB stream memory guard (tests/perf/, non-.test.js so node --test skips it)
pnpm lint               # oxlint src tests scripts bench
pnpm format             # oxfmt src tests scripts bench (format:check for CI)
pnpm bench              # runs every script in bench/ (bench/run-all.js), not just bench.js
pnpm size                # bundle-size report (scripts/size.js), also run in CI's lint job
pnpm docs:dev            # VitePress dev server for docs/ (docs:build / docs:preview too)
```

Linting/formatting is **oxlint/oxfmt** (`.oxlintrc.json`, `.oxfmtrc.json`; the `correctness` category is intentionally off, and `no-void` is intentionally not enabled — this codebase follows the Metarhia style of `return void fn()` for explicit fire-and-forget) — their native bindings require Node ≥20.19. CI (`.github/workflows/ci.yml`) runs three jobs: `lint` (Node 22 — lint, format:check, size), `test` (Node 22/24 matrix — test:coverage, test:types), `docs` (Node 22 — docs:build). Lint/format deliberately target `src tests scripts bench` only, so `docs/` is not covered by them.

Style: 2-space indent, single quotes, semicolons, 120-char lines (see `.editorconfig`, `.oxfmtrc.json`).

## Package layout conventions

Mirrors the `@alexify/kerberos` entry-point pattern:

- `index.js` → `require('./src/index.js')` — the Node/default entry.
- `ws.js` → `require('./src/websocket/ws.js')` + `ws.d.ts` — the `./ws` subpath (WebSocket engine). New subpaths follow the same pattern: root shim + hand-written root `<name>.d.ts` + `exports` entry + `files` additions + `tests/<name>.test-d.ts`.
- `browser.js` → `require('./src/index.browser.js')`; resolved via the `package.json#browser` field map and the `browser` condition in `exports`. Do not deduplicate the two files even once they look similar — the browser entry intentionally excludes server-only modules.
- `index.d.ts` — hand-maintained types (not generated), covering the full `src/index.js` surface.
- `src/index.js` — the full barrel; `src/index.browser.js` — the browser-safe subset (see above).
- Platform-specific code lives in matched pairs swapped via `package.json#browser`: `src/chunks.js`/`src/chunks.browser.js` and `src/runtime/node.js`/`src/runtime/browser.js`. Add new pairs the same way if another Node-only API needs a browser equivalent.

## Testing

Tests live in `tests/` (not `test/` — this differs from `kerberos`), run via Node's built-in `node --test` runner (bare `node --test`, which recursively discovers `**/*.test.js` under the cwd — no vitest/jest config). Non-`.test.js` files under `tests/` (`tests/websocket/mockSocket.js`, `tests/websocket/protocolClient.js`) are shared test helpers, not test files themselves. The `#ws` subpath import (`package.json#imports`) lets tests require the websocket barrel without a relative path. Type tests use `tsd` (`tsd.directory: "tests"` in `package.json`), currently `tests/index.test-d.ts`.

## Documentation site (`docs/`)

VitePress site deployed to Vercel (`vercel.json` pins the build command/output dir); `docs/` is never published to npm (the `files` field in `package.json` is an explicit allowlist that omits it). Currently a minimal skeleton (`docs/.vitepress/config.mts`, a `theme/` extending `DefaultTheme`, `docs/index.md` home page, one `docs/guide/getting-started.md` page) — expand the nav/sidebar in `config.mts` as more guide/API pages are added, following the fuller structure in `kerberos/docs/.vitepress/config.mts` as a reference.
