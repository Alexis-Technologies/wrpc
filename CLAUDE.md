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

**Protocol architecture.** The wire protocol is JSON packets (`{ type: 'call' | 'callback' | 'event' | 'stream' | 'ping' | 'pong', ... }`) over WebSocket or HTTP, plus a small binary framing (`chunkEncode`/`chunkDecode`) for streamed uploads/downloads. It is documented in `docs/reference/protocol.md`, which is updated per phase — it is at **revision 2** (`wrpc.v2`): revision 1 (what 1.0 speaks) plus the framed messages, i.e. a packet whose bytes travel as bytes. A 2.x peer speaks BOTH and the revision is negotiated per connection, so a frame is sent only where the peer said it reads one and a 1.0 peer is spoken to as 1.0 was: the ws subprotocol (`wrpc.v2, wrpc.v1` offered, the newest selected — `negotiateProtocol` in `src/websocket/upgrade.js`, the one reading both engines share), `wrpc-version` on an HTTP response plus the request's `Accept` (`readsFrames` in `src/transport.js` — never a `wrpc-version` request header: CORS), `v` on a worker port's first ping/pong (`negotiate` in `src/transport.js`), nothing on SSE (the `ready` frame's `channel` is the whole `<id>.<secret>` reference, opaque to a 1.0 client) and always 2 on the carriers 1.0 never had. There is NO revision option: `attachments: false` or a packet codec means "this end speaks revision 1" (`revisionProtocols` in `src/adapters/common.js` narrows a shell's engine to `wrpc.v1`), and `revision` is a read-only getter on `WrpcClient`, the server `Client` and `RpcServer`. The backplane (rooms/cluster) has no handshake and stays on flags. **A change a 1.0 peer cannot ignore belongs behind the revision**, and `tests/interop/` runs the PUBLISHED 1.0 (devDependency alias `wrpc-v1`) against the tree, both ways, to keep that true — and the PUBLISHED 2.0.1 (`wrpc-v2`, `tests/interop/v2.test.js`), the peer the next sentence is about. The revision cannot tell a 2.0.1 peer from a later one, though — both speak `wrpc.v2` — so an addition made AFTER 2.0 that an older 2.x peer cannot ignore is asked for by an additive field instead: a binary stream's receiver says that its reader let go (`tellSender` in `src/rpc/dispatcher.js`, the `onCancel` hook of `WrpcReadable`) only to a sender whose announcement carried `cancel: true`, which a `WrpcWritable` sets when its owner routes answers to it by id (`cancelStream` in `src/rpc/client.js`, `#unmatched` in `src/client/core.js`). Every `##` section 1.0 did not have carries a `since 2.0` badge, and `## Changes since 1.0` tables what an older peer does with each addition (a guard in `tests/package/consistency.test.js` keeps both).

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
pnpm test:perf         # 1 GiB stream memory guard (tests/perf/, non-.test.js so node --test skips it)
WRPC_RTC=node-datachannel node --test tests/webrtc/node-datachannel.integration.test.js   # WebRTC port contract against a real implementation (manual, like Redis)
WRPC_WT=fails node --test tests/wt/fails.integration.test.js   # WebTransport over @fails-components/webtransport (native binary) — manual, not in CI
WRPC_WT=quico node --test tests/wt/quico.integration.test.js   # …and over the pure-JS quico — manual, not in CI
pnpm lint               # oxlint src tests scripts bench bin examples
pnpm format             # oxfmt src tests scripts bench bin examples (format:check for CI)
pnpm bench              # runs every script in bench/ (bench/run-all.js), not just bench.js
node bench/http-comparison.js   # wrpc's HTTP paths vs fastify/express/tRPC/node:http under autocannon (fastify/benchmarks' settings; WRPC_BENCH_DURATION seconds per stack)
WRPC_WT=fails WRPC_RTC=node-datachannel node bench/transports.js   # wrpc over ws/WebTransport/WebRTC on real stacks (bench/support/real-stacks.js; needs node scripts/wt-cert.js certs) — rows skip without the variables; the same gate adds the wt/webrtc rows to bench/rpc-comparison.js
WRPC_WT=fails pnpm bench:browser transports   # the same from Chrome (bench/browser/transports.js); without WRPC_WT the WebTransport row is skipped
pnpm size                # bundle-size report (scripts/size.js), also run in CI's lint job; browser-reachable entries carry a min+gzip budget and exceeding one FAILS the run
pnpm docs:dev            # VitePress dev server for docs/ (docs:build / docs:preview too)
```

Linting/formatting is **oxlint/oxfmt** (`.oxlintrc.json`, `.oxfmtrc.json`; the `correctness` category is intentionally off, and `no-void` is intentionally not enabled — this codebase follows the Metarhia style of `return void fn()` for explicit fire-and-forget) — their native bindings require Node ≥20.19. CI (`.github/workflows/ci.yml`) runs three always-on jobs — `lint` (Node 22 — lint, format:check, size), `test` (Node 22/24/26 matrix, installed with `--ignore-scripts` — test:ci, test:coverage:floors, test:types; bare `node --test` recursively discovers `tests/adapters/**` too, so the adapter suites run here rather than in a separate job), `docs` (Node 22 — docs:build) — plus the four broker service jobs (`redis`, `nats`, `rabbitmq`, `kafka` — `tests/CLAUDE.md`). The Autobahn Testsuite (`node scripts/autobahn/run.js`) and the 1 GiB stream memory guard (`pnpm test:perf`) are deliberately **not** wired into CI — run them by hand. Where a sliced run is needed (an emulated client image that loses the server half-way), `node scripts/autobahn/run.js --slices` runs it a case group at a time. Lint/format target `src tests scripts bench bin examples` (the examples' scripts — `examples/**/public/**` and their READMEs are ignored in `.oxlintrc.json`/`.oxfmtrc.json`), so `docs/` is not covered by them; `tests/examples.test.js` compiles every example script and checks each name it destructures from a root entry against that entry's exports — the examples are never executed by the suite.

Style: 2-space indent, single quotes, semicolons, 120-char lines (see `.editorconfig`, `.oxfmtrc.json`).

## Package layout conventions

The layout of the sibling `mailer`: the root holds the main entry only (`index.js`, one `require()` of `src/index.js`, and `index.d.ts`); every subpath resolves straight to its entry file under `src/`, with a hand-written `.d.ts` (never generated) BESIDE it under the same name. A new subpath brings the entry and its `.d.ts`, an `exports` entry (`types` first), the matching `typesVersions` line (what TypeScript under `moduleResolution: node10` reads instead of `exports`), a `scripts/size.js` row and `tests/<name>.test-d.ts`. Guards in `tests/package/consistency.test.js` refuse a root shim, an exports target outside `src/` and a `typesVersions` that differs from the `types` conditions.

- `.` — `index.js` → `src/index.js` (the full barrel); the `browser` condition and the `package.json#browser` map point at `src/index.browser.js` (the browser-safe subset). Do not deduplicate the two even once they look similar: the browser entry intentionally excludes server-only modules. Types: `index.d.ts` (the full surface), `src/client.d.ts`, `src/index.browser.d.ts`.
- `src/rpc.d.ts` — the node-free server-core types (routers, subscriptions, sessions, rooms, `Client`, `Context`, `ClientHost`), re-exported by `index.d.ts` and imported by `src/webrtc/browser.d.ts`. It has no runtime file of its own; `src/client.d.ts` types the `src/client.js` barrel. `Context.server`/`Client.server` are typed as `ClientHost` (what an `RpcServer` and a `PeerHost` both satisfy), never `RpcServer`.
- `./ws` → `src/websocket/ws.js`; `./engine` → `src/engine/index.js`; `./uws`, `./fastify`, `./express` → `src/adapters/<name>.js`; `./scaling` → `src/scaling/index.js`; `./wt` → `src/webtransport/index.js` (the SERVER half only, Node-only, no `browser` condition: the client transport ships in the base entry); `./broker` → `src/broker/index.js`, `./broker/<name>` → `src/broker/<name>/index.js`.
- Three subpaths have a `browser` condition: `./sse` (→ `src/sse/client.js` — a browser needs the client transport, never the channel registry), `./webrtc` (→ `src/webrtc/browser.js` — no signaling unit; it re-exports `defineRouter`/`procedure`/`tracked`/`createEventLog`, which the main browser entry cannot afford) and `./encryption` (→ `src/encryption/browser.js`).
- `./query`, `./auth`, `./deflate` are browser-reachable with NO `browser` condition: one file serves both platforms, and `scripts/size.js` bundles each with `platform: 'browser'` under its own budget to keep that true. `./deflate` and `./encryption` are deliberately outside every other entry — only a page that injects them pays for them.
- `bin/wrpc.js` → `src/cli/types.js` — the `wrpc` command (`package.json#bin`). A shim on purpose: the logic lives under `src/` so the c8 thresholds cover it. Needs the shebang, the executable bit, and `bin/` in both `files` and the lint/format targets.
- Platform-specific code lives in matched pairs swapped via `package.json#browser`; the list is in `src/CLAUDE.md`.

## Skills

The skills are [alexiskills](https://alexiskills.vercel.app), `@alexify/metaskills` 1.1, a devDependency: all 37 of them, through one symlink, `.claude/skills` → `node_modules/@alexify/metaskills/skills`, made by the `prepare` script (`alexiskills claude`) on every `pnpm install`, a worktree's included, and left out by `.gitignore`. Until 2.3 the directory held six skills of metarhia/metaskills 1.0.5, two of them adapted to wRPC; what they said of wRPC alone is in "Conventions" and "Release process" now.

- **The skills are not edited here.** A change goes to `Alexis-Technologies/metaskills` and comes back with a release of the package. A skill of this repository's own beside them would turn the symlink into a directory of links (the CLI's pack mode); one named like a skill of the package hides it.
- `js-conventions` (its Library profile) and `npm-publish` are the Alexis ones; `npm-publish` runs only as `/npm-publish`.
- **A review reads the skills first**: the review that ran without them missed what `data-structures` says in one line (never a hot `Array.shift`).

**This file wins** where a skill and it disagree.
