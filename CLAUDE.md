# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## How this guidance is laid out

It is split by folder, the way the sibling `syncom` does it: this file holds what is true of the whole repository, and each folder's `CLAUDE.md` holds what is true of that folder — read when a file of the folder is. Before working in a folder, read its file; before working across two, read both.

| Folder | Its `CLAUDE.md` holds |
| ------ | --------------------- |
| `src/` | the layers, the byte budgets, the platform pairs, and the files of `src/` itself (`streams.js`, `transport.js`, `wire.js`, `attachments.js`, `server.js`, …) |
| `src/rpc/` | the server core: router, dispatcher, sessions, rooms, cluster, subscriptions, the dictionary |
| `src/client/` | `WrpcClient`, `WrpcClientProxy`, the transports of the base entry, the handshake carriers |
| `src/websocket/` | the built-in WebSocket engine (`./ws`) |
| `src/webrtc/` | peers, links, the mesh, signaling, trust assertions (`./webrtc`) |
| `src/webtransport/` | the channel both ends run on, the stream mux, the server half (`./wt`) |
| `src/broker/` | the capability model, sealing, the scenarios, the four adapters (`./broker`, `./broker/<name>`) |
| `src/encryption/` | primitives, envelopes, the sealed store, Noise sessions, HPKE requests, end-to-end helpers (`./encryption`) |
| `src/compression/` | per-message codecs and their negotiation; HTTP/SSE content coding |
| `src/deflate/` | the pure-JS DEFLATE codec (`./deflate`) |
| `src/adapters/` | uWebSockets.js, fastify, express (`./uws`, `./fastify`, `./express`) |
| `src/sse/` | Server-Sent Events as a transport (`./sse`) |
| `src/scaling/`, `src/engine/`, `src/telemetry/`, `src/query/`, `src/auth/`, `src/cli/` | one short file each |
| `tests/` | the runner, the shared helpers and contract suites, the warnings, which devDependency is whose |
| `bench/` | the performance conventions and the measurements behind them — read before touching a hot path |
| `docs/` | the VitePress site: sidebar, charts, diagrams, branding, analytics |

**A rule lives in ONE file** — the one of the folder it is about; a rule about everything lives here. When a convention changes, change it there, not in a second place. None of these files ships: `files` in `package.json` ends with `!**/CLAUDE.md`, and a guard in `tests/package/consistency.test.js` reads the packing list. The same guard keeps this file and `src/CLAUDE.md` small (they are read for every task), so new detail goes into a folder's file.

References in these files name a SYMBOL in a file, never a line number: line numbers drifted within a week every time they were refreshed (half of them pointed at the wrong line), and a guard in `tests/package/consistency.test.js` now refuses one — and checks that every path written in any of them exists and every "`symbol` in `file`" still holds.

## Project

wRPC (`@alexify/wrpc`, Web RPC) is a fast, zero-dependency RPC protocol for Node.js and browsers (WebSocket, HTTP, SSE, WebTransport, WebRTC, worker ports, message brokers). In prose the product is written **wRPC**; the package, the `wrpc` CLI, every wire name (`wrpc.v2`, `x-wrpc-*`) and the classes stay as they are.

**Protocol architecture.** The wire protocol is JSON packets (`{ type: 'call' | 'callback' | 'event' | 'stream' | 'ping' | 'pong', ... }`) over WebSocket or HTTP, plus a small binary framing (`chunkEncode`/`chunkDecode`) for streamed uploads/downloads. It is documented in `docs/reference/protocol.md`, which is updated per phase — it is at **revision 2** (`wrpc.v2`): revision 1 (what 1.0 speaks) plus the framed messages, i.e. a packet whose bytes travel as bytes. A 2.x peer speaks BOTH and the revision is negotiated per connection, so a frame is sent only where the peer said it reads one and a 1.0 peer is spoken to as 1.0 was: the ws subprotocol (`wrpc.v2, wrpc.v1` offered, the newest selected — `negotiateProtocol` in `src/websocket/upgrade.js`, the one reading both engines share), `wrpc-version` on an HTTP response plus the request's `Accept` (`readsFrames` in `src/transport.js` — never a `wrpc-version` request header: CORS), `v` on a worker port's first ping/pong (`negotiate` in `src/transport.js`), nothing on SSE (the `ready` frame's `channel` is the whole `<id>.<secret>` reference, opaque to a 1.0 client) and always 2 on the carriers 1.0 never had. There is NO revision option: `attachments: false` or a packet codec means "this end speaks revision 1" (`revisionProtocols` in `src/adapters/common.js` narrows a shell's engine to `wrpc.v1`), and `revision` is a read-only getter on `WrpcClient`, the server `Client` and `RpcServer`. The backplane (rooms/cluster) has no handshake and stays on flags. **A change a 1.0 peer cannot ignore belongs behind the revision**, and `tests/interop/` runs the PUBLISHED 1.0 (devDependency alias `wrpc-v1`) against the tree, both ways, to keep that true — and the PUBLISHED 2.0.1 (`wrpc-v2`, `tests/interop/v2.test.js`), the peer the next sentence is about. The revision cannot tell a 2.0.1 peer from a later one, though — both speak `wrpc.v2` — so an addition made AFTER 2.0 that an older 2.x peer cannot ignore is asked for by an additive field instead (the stream's `cancel: true`, `src/rpc/CLAUDE.md`). Every `##` section 1.0 did not have carries a `since 2.0` badge, and `## Changes since 1.0` tables what an older peer does with each addition (a guard in `tests/package/consistency.test.js` keeps both).

**No runtime dependencies.** An earlier pass ported code structurally from `metarhia/metacom`, which depends on `metautil`. Rather than install `metautil` (breaking the zero-dependency guarantee), the handful of functions actually used (`Emitter`, `jsonParse`, `generateUUID`, plus a few single-file-use helpers like `parseCookies`/`parseHost`/`receiveBody`/`split`/`parseParams`/`isError`) were copied in directly and adapted. `package.json` has no `dependencies` field at all — keep it that way; anything reused from another package must be copied in, not installed.

This rule extends to the adapters: **`uWebSockets.js`, `fastify`, `fastify-uws` and `express` are devDependencies used only by the adapter tests, and must never become runtime dependencies (not even `peerDependencies`/`optionalDependencies`).** The rule is the list's point, not the list: **every entry of `devDependencies` is test-, bench- or docs-only, and `package.json` has no `dependencies`, `peerDependencies` or `optionalDependencies` field** — a package named here that is missing from `package.json`, or the reverse, changes nothing about it. Every adapter takes its host by injection — `createUwsEngine({ uws: require('uWebSockets.js') })`, `wrpcFastify` receives the fastify instance from `register()`, `createWrpc()` returns middleware the app mounts — and validates that injection structurally (duck typing, e.g. `isEngine`, `isTemplatedApp`) rather than by `instanceof` against an imported class. Nothing under `src/` may `require()` a framework. Which devDependency is used by which suite is listed in `tests/CLAUDE.md`.

Package manager is **pnpm** (`packageManager: pnpm@10.34.5`). CommonJS throughout (`require`/`module.exports`), no build/transpile step — `src/` ships as-is (same philosophy as the sibling `@alexify/kerberos` project).

## Commands

```bash
pnpm test              # run all tests: node --test (recursive discovery over tests/)
node --test tests/smoke.test.js            # run a single test file
node --test --test-name-pattern="..."      # filter tests by name
node --test tests/adapters/*.test.js       # adapter suites (node --test takes files, not dirs)
pnpm test:types        # type-check tests/*.test-d.ts against index.d.ts via tsd
pnpm test:coverage     # c8 coverage over src/ (thresholds: 95% lines/statements, 90% branches, 95% functions)
pnpm test:ci           # test:coverage + --test-timeout=300000 --test-force-exit — what CI's test job runs; never the local default (a leak must stay visible)
pnpm test:coverage:floors  # per-FILE floors, a directory at a time (scripts/coverage-floors.js); reads coverage/tmp, so run it right after test:coverage. A floor only moves up
pnpm test:perf         # 1 GiB stream memory guard (tests/perf/, non-.test.js so node --test skips it); the manual WebRTC/WebTransport suites are in tests/CLAUDE.md
pnpm lint               # oxlint src tests scripts bench bin examples (a stale oxlint-disable is an error)
pnpm run prepublishOnly # every gate CI's always-on jobs run, chained; `pnpm publish` runs it too
pnpm format             # oxfmt src tests scripts bench bin examples (format:check for CI)
pnpm bench              # every script in bench/ (bench/run-all.js); the comparisons over real stacks are in bench/CLAUDE.md
pnpm size                # bundle-size report (scripts/size.js), also run in CI's lint job; browser-reachable entries carry a min+gzip budget and exceeding one FAILS the run
pnpm docs:dev            # VitePress dev server for docs/ (docs:build / docs:preview too)
```

oxlint/oxfmt's native bindings require Node ≥20.19. CI's jobs are in CONTRIBUTING.md ("CI"): lint, the test matrix from the exact `engines` floor (22.0.0) to 26, installed with `--ignore-scripts`, docs, the four broker services, and `ci-success`, which fails on a failed, cancelled or skipped job. Autobahn (`node scripts/autobahn/run.js`, `--slices` a case group at a time) and `pnpm test:perf` are deliberately not in CI. Lint and format target `src tests scripts bench bin examples` — not `docs/`, nor the examples' `public/` and READMEs; `tests/examples.test.js` compiles every example and checks each name it takes from a root entry, never running one.

## Package layout conventions

The layout of the sibling `mailer`: the root holds the main entry only (`index.js`, one `require()` of `src/index.js`, and `index.d.ts`); every subpath resolves straight to its entry file under `src/`, with a hand-written `.d.ts` (never generated) BESIDE it under the same name. A new subpath brings the entry and its `.d.ts`, an `exports` entry (`types` first), the matching `typesVersions` line (what TypeScript under `moduleResolution: node10` reads instead of `exports`), a `scripts/size.js` row and `tests/<name>.test-d.ts`. Guards in `tests/package/consistency.test.js` refuse a root shim, an exports target outside `src/` and a `typesVersions` that differs from the `types` conditions.

- `.` — `index.js` → `src/index.js` (the full barrel); the `browser` condition and the `package.json#browser` map point at `src/index.browser.js` (the browser-safe subset). Do not deduplicate the two even once they look similar: the browser entry intentionally excludes server-only modules. Types: `index.d.ts` (the full surface), `src/client.d.ts`, `src/index.browser.d.ts`.
- `src/rpc.d.ts` — the node-free server-core types (routers, subscriptions, sessions, rooms, `Client`, `Context`, `ClientHost`), re-exported by `index.d.ts` and imported by `src/webrtc/browser.d.ts`. It has no runtime file of its own; `src/client.d.ts` types the `src/client.js` barrel. `Context.server`/`Client.server` are typed as `ClientHost` (what an `RpcServer` and a `PeerHost` both satisfy), never `RpcServer`.
- `./ws` → `src/websocket/ws.js`; `./engine` → `src/engine/index.js`; `./uws`, `./fastify`, `./express` → `src/adapters/<name>.js`; `./scaling` → `src/scaling/index.js`; `./wt` → `src/webtransport/index.js` (the SERVER half only, Node-only, no `browser` condition: the client transport ships in the base entry); `./broker` → `src/broker/index.js`, `./broker/<name>` → `src/broker/<name>/index.js`.
- Three subpaths have a `browser` condition: `./sse` (→ `src/sse/client.js` — a browser needs the client transport, never the channel registry), `./webrtc` (→ `src/webrtc/browser.js` — no signaling unit; it re-exports `defineRouter`/`procedure`/`tracked`/`createEventLog`, which the main browser entry cannot afford) and `./encryption` (→ `src/encryption/browser.js`).
- `./query`, `./auth`, `./deflate` are browser-reachable with NO `browser` condition: one file serves both platforms, and `scripts/size.js` bundles each with `platform: 'browser'` under its own budget to keep that true. `./deflate` and `./encryption` are deliberately outside every other entry — only a page that injects them pays for them.
- `bin/wrpc.js` → `src/cli/types.js` — the `wrpc` command (`package.json#bin`). A shim on purpose: the logic lives under `src/` so the c8 thresholds cover it. Needs the shebang, the executable bit, and `bin/` in both `files` and the lint/format targets.
- Platform-specific code lives in matched pairs swapped via `package.json#browser`; the list is in `src/CLAUDE.md`.

## Conventions

The style is `js-conventions` of alexiskills, its Library profile, and errors follow `error-handling`. Here is what wRPC adds or does otherwise; **this file wins**.

- **Kept:** `'use strict'` first in every `src/` file; 120 columns; options in ms and bytes, unsuffixed (`callTimeout`).
- **Lint** (`.oxlintrc.json`): the reference of alexiskills (`correctness` and `suspicious` as errors, strict `eqeqeq`, the array-method ban in `src/`, …) plus `no-empty` with `allowEmptyCatch: false` and `no-labels`. Four rules of those categories are off, each for a reason: `promise/always-return` (a `.then` here is a continuation whose value nobody reads), `promise/no-promise-in-callback` (an `emit(…).catch` in a callback is the observer rule), `import/no-unassigned-import` (a subpath registers its transport by being required) and `unicorn/require-post-message-target-origin` (wRPC posts to workers and ports, which take no origin).
- **Loops:** `bench/CLAUDE.md` holds the two spellings a benchmark keeps (`new Array(n)`, `for...in`). Spreading into a call, `switch` dispatch, `__proto__: null` tables and peer-grown queues with caps are the skill's rules; the sites are guarded (`HEAD_READS`/`CALL_SPREADS`).
- **Kept by name:** `String(value)` where the value may be a symbol or is input of unchecked type; `delete` on dictionaries, header bags and peer data whose absent key means absent; `let x = undefined` for what user code returned. A targeted `oxlint-disable` names its reason (an option named `filter` or `map`, uWS's `forEach`, promise false positives, the CLI's console).
- **Errors:** a bad option, definition or injected host throws `ConfigError` (`ALX_ERR_CONFIG`), a bad argument of a running call `ArgumentError` (`ALX_ERR_ARGUMENT`) — `src/errors.js`, both `TypeError`s, the message prefixed with what is configured; `CliError` is `ALX_ERR_CLI`. Every code is in `ErrorCode` (`src/errors.d.ts`) and the table of `docs/reference/errors.md`, held equal by `tests/package/error-codes.test.js`, which also lists the bare `TypeError`s that stay (a peer's bytes, not the caller's mistake). **The numeric `error.code` is wrpc's wire status, the exception to `ALX_ERR_`**: what a handler or hook throws with a number is what the peer is answered. **Temporary exception:** the unprefixed string codes 2.x throws (`FramingError`, `DeflateError`, `ParseError`, `AssertionError`, `OpenError`, `EncryptionRefusedError`, `backpressure` — the second table of that page) keep their spelling until 3.0 renames them to `ALX_ERR_*`.
- **Logging:** `logger: false` silences all a class with a `logger` option prints, its `emitter.listeners` warning included (`emitterWarning` in `src/logging.js`).

## Release process

The `npm-publish` skill walks through it (only as `/npm-publish`); the "Release checklist" of CONTRIBUTING.md is the list and wins. What wRPC adds to the skill's steps:

- **Gates:** `prepublishOnly` (which `pnpm publish` runs) on Node 22.0.0, 22, 24 and 26, then by hand what CI does not run: Autobahn in both modes, `pnpm test:perf`, the WebRTC, WebTransport and broker integration suites.
- **CHANGELOG:** `### Changed (breaking)` stays under `[Unreleased]` even empty and alone holds a `**Breaking` entry; a non-empty one comes with `### Upgrading from X.Y`; the new version gets its link (`tests/package/consistency.test.js` holds all three).
- **Hand-synced:** the `nav` label and `keywords` of `docs/.vitepress/config.mts` (tested); the size figures in prose (not).
- **Package:** the tarball smoke installs into a temporary consumer with npm, the one place npm is used. No release workflow, provenance or dependency bot; the owner commits `chore(release): X.Y.Z`, tags, pushes and publishes.

## Skills

The skills are [alexiskills](https://alexiskills.vercel.app), `@alexify/metaskills` 1.1, a devDependency: all 37, through one symlink, `.claude/skills` → `node_modules/@alexify/metaskills/skills`, made by `prepare` (`alexiskills claude`) on every `pnpm install` and left out by `.gitignore`. What the six metarhia 1.0.5 skills kept here until 2.3 said of wRPC alone is in "Conventions" and "Release process".

- **The skills are not edited here**: a change goes to `Alexis-Technologies/metaskills` and comes back with a release. A skill of our own beside them turns the symlink into a directory of links; one named like a package skill hides it.
- **A review reads the skills first**: the review that ran without them missed what `data-structures` says in one line (never a hot `Array.shift`).

**This file wins** where a skill and it disagree.
