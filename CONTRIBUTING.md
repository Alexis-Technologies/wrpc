# Contributing

Thanks for looking. This file covers how to work on `@alexify/wrpc`, the house
rules that are not obvious from the code, and the release checklist.

## Setup

```bash
pnpm install
pnpm test
```

Requires **Node.js ≥ 22** and **pnpm** (`packageManager` pins the version).
There is no build step: `src/` ships as-is.

## Commands

```bash
pnpm test              # node --test, recursive over tests/
pnpm test:coverage     # c8 over src/ — thresholds 95 lines / 95 statements / 90 branches / 95 functions
pnpm test:ci           # test:coverage as CI runs it: a 5 min per-test timeout and a forced exit, so a leak fails instead of hanging
pnpm test:coverage:floors  # per-file floors, a directory at a time — run right after test:coverage, it reads what that left
pnpm test:types        # tsd against the .d.ts files
pnpm check:dts         # every hand-written .d.ts through the repo's own tsc (7.x)
pnpm test:perf         # the 1 GiB stream memory guard
pnpm lint              # oxlint
pnpm format            # oxfmt (format:check in CI)
pnpm size              # bundle-size report; browser budgets fail the run
pnpm bench             # every script in bench/
pnpm docs:dev          # the VitePress site (docs:build / docs:preview too)
```

Run a single file with `node --test tests/smoke.test.js`, or filter by name
with `--test-name-pattern`. `node --test` takes **files**, not directories — a
targeted run of a folder needs a glob (`node --test tests/adapters/*.test.js`).

The `wrpc` command of this tree is `node bin/wrpc.js`. Inside the repository
`npx wrpc` and `node_modules/.bin/wrpc` run a **published** CLI, not this
tree's: the interop suite's aliases install `@alexify/wrpc@1.0.0` (`wrpc-v1`)
and `@alexify/wrpc@2.0.1` (`wrpc-v2`), bins included.

## House rules

**Zero runtime dependencies.** `package.json` has no `dependencies` field, and
no `peerDependencies` or `optionalDependencies` either. Anything reused from
another package is copied in and adapted, not installed. Optional integrations
(uWebSockets.js, Redis, fastify, express, TanStack) are **injected by the
caller** and validated structurally — duck typing, never `instanceof` against
an imported class. Nothing under `src/` may `require()` a framework.

**A `.d.ts` change and its `tsd` test land together.** The declarations are
hand-maintained, one file per subpath, and they drift silently otherwise. Every
new subpath brings its entry under `src/` with a `.d.ts` beside it, an
`exports` entry (`types` first), its `typesVersions` line, a `scripts/size.js`
row and a `tests/<name>.test-d.ts` — the package root holds the main entry
(`index.js`, `index.d.ts`) and nothing else.

**`tsd` type-checks with `@tsd/typescript` (5.9), not the repo's own
`typescript`.** A type relying on newer inference has to be checked against
both.

**Wire-protocol changes are documented in
[`docs/reference/protocol.md`](./docs/reference/protocol.md)**, which names
its revision and what each of its tiers promises: additive, optional fields
within a major version; anything else is a major, with a row in the page's
"Changes since" table saying what an older peer does with it and what to set
until every peer is upgraded. A `##` section the previous major did not have
carries a `since N.0` badge (`tests/package/consistency.test.js` holds the
list of the sections that need none).

**Style** is the `js-conventions` skill of
[alexiskills](https://alexiskills.vercel.app) (a devDependency, linked into
`.claude/skills` by `pnpm install`), with what wRPC does differently in the
"Conventions" of `CLAUDE.md`. It is enforced by oxlint/oxfmt: 2-space indent,
single quotes, semicolons, 120-column lines; the `correctness` and
`suspicious` categories are errors, `src/` iterates with loops (no
`map`/`filter`/`forEach`…), and `no-void` is off — `return void fn()` is the
idiom for explicit fire-and-forget. Tests are `describe`/`it('should …')`
with `node:assert/strict`.

### Tests

Tests live in `tests/`, run by Node's built-in runner. Files that are **not**
`*.test.js` are shared helpers, not tests: `tests/websocket/mockSocket.js`,
`tests/websocket/protocolClient.js`, `tests/engine/engineContract.js` (the
engine port contract suite), and the swap-test pair `tests/adapters/boots.js` +
`tests/adapters/spec.js` — one behavioural spec replayed against every way of
standing wrpc up.

Adapter tests **skip** (never fail) when a framework is missing, so a machine
without a working `uWebSockets.js` binary can still run the suite. CI has them
all.

Coverage has two gates. `test:coverage` checks the global thresholds — an
average, behind which one thin file hides. So every file of the source
directories — `src/rpc`, `src/client`, `src/websocket`, `src/sse`,
`src/adapters`, and the ones new in 2.0: `src/broker`, `src/webtransport`,
`src/encryption`, `src/webrtc`, `src/compression`, `src/deflate` — is also
held **per file** by `test:coverage:floors`, each directory to a floor of its
own (`scripts/coverage-floors.js` has the numbers and the one exception). A
floor moves up when its directory's weakest file improves — never down to let
a change in; cover the new branch instead.

> **A leaked uws engine hangs `node --test` forever.** A standalone engine holds
> a native listen socket; if a test fails an assertion before its cleanup line
> runs, the socket is never released and the whole run wedges instead of
> reporting a failure. Register teardown with `t.after(...)` **immediately after
> booting**, never as a trailing `await close()` at the end of the test body.

## CI

Three always-on jobs and four service jobs (`.github/workflows/ci.yml`):

- **lint** (Node 22) — `lint`, `format:check`, `check:dts`, `size`
- **test** (Node 22.0.0 — the floor of `engines` exactly — 22, 24 and 26,
  installed without build scripts) —
  `test:ci` (`test:coverage` with
  `--test-timeout=300000 --test-force-exit`: a file that outlives its tests
  fails in minutes instead of hanging to the job's timeout),
  `test:coverage:floors`, `test:types`
- **docs** (Node 22) — `docs:build`, which fails on dead links, after the
  install `vercel.json` runs for the deploy (`--ignore-scripts`: the site
  needs no native build)
- **redis**, **nats**, **rabbitmq**, **kafka** — the integration suites
  against a real server each (Redis 7.4, NATS 2.11 with JetStream — started
  through `compose.yaml`, since a GitHub service container cannot be given
  the `-js` flag — RabbitMQ 4.1, Kafka 3.9.1), under
  `WRPC_INTEGRATION_STRICT=1`: there a missing client package fails the job
  instead of skipping it green. Locally, `pnpm brokers:up` starts the same
  four and each suite skips itself without its env var.
- **ci-success** — the one job branch protection requires: it fails when any
  job above failed, was cancelled or was **skipped** (a skipped job is not a
  passed one).

Lint and format target `src tests scripts bench bin examples` — the examples'
scripts, not their pages or READMEs — and deliberately not `docs/`. The
examples are not run by the suite (they need servers and a browser);
`tests/examples.test.js` checks that each compiles and that every name it
takes from a root entry is one that entry exports.

Checks deliberately **not** in CI — run them by hand:

- `node scripts/autobahn/run.js` — the RFC 6455/7692 conformance suite against
  the engine in `src/websocket/`, and again with `AUTOBAHN_DEFLATE=takeover`
  for the queued, asynchronous compression paths. Needs docker; several
  minutes for 517 cases. `FAILED` and `WRONG CODE` fail the run, `NON-STRICT`
  and `INFORMATIONAL` do not — and so does a run the fuzzing client cut
  short (it gives up on the suite when one connect times out). Run it on an
  otherwise idle Docker: with the four broker containers up, the emulated
  client image loses the server half-way. Where it still does — an emulated
  amd64 image on an arm64 host — `node scripts/autobahn/run.js --slices` runs
  the suite a case group at a time, one container each, and repeats a group
  whose client lost the server instead of failing the whole run; the verdict
  is the sum and fails if the groups add up to fewer than the 517 cases.
- `pnpm test:perf` — the 1 GiB stream memory guard.
- `WRPC_RTC=node-datachannel node --test tests/webrtc/node-datachannel.integration.test.js`
  — the WebRTC port contract against a real implementation (the fake in
  `tests/webrtc/fakeRtc.js` is what `pnpm test` runs); needs the native
  `node-datachannel` build.
- `WRPC_WT=fails node --test tests/wt/fails.integration.test.js` and
  `WRPC_WT=quico node --test tests/wt/quico.integration.test.js` — the
  WebTransport server half against a real HTTP/3 stack
  (`@fails-components/webtransport` with its native `-transport-http3-quiche`
  binary, or the pure-JS `quico`). `pnpm test` covers the same code through
  the in-memory fake (`tests/wt/fakeWebTransport.js`); both files skip
  themselves without `WRPC_WT`. `node scripts/wt-cert.js` makes the
  short-lived ECDSA certificate they and a browser need.

## Stability and deprecation

The published surface is stable under semver, with five carve-outs marked
`@experimental` in the `.d.ts` files (the consumer-facing list is
[docs/reference/stability.md](./docs/reference/stability.md#experimental-carve-outs);
`tests/package/consistency.test.js` keeps the two counts equal):

- the **telemetry** writer shapes and metric set (`telemetry` option) — the
  signals will keep improving in minors;
- the **engine port** internals beyond the documented `WrpcSocket` contract
  (`capabilities` in particular);
- **WebTransport**, whole — the `wt` client transport, the `@alexify/wrpc/wt`
  subpath and the control-stream framing — until Node has a WebTransport of
  its own to settle the carrier against;
- the **message-broker family**, whole — `@alexify/wrpc/broker` and the
  `@alexify/wrpc/broker/*` adapters, their capability contracts, the broker
  metrics, the `broker` client transport and the broker binding — until every
  adapter has shipped;
- **application-level encryption**, whole — `@alexify/wrpc/encryption` and
  every `encryption` option it feeds, `Client.encryption`,
  `encryptionKey()`/`encryptionRequired`, `attach({ encrypted })`, and the
  session-encryption, sealed-request and broker-sealing wire formats — until
  the formats have been reviewed against real deployments.

An `@experimental` API may change in a minor release, with the change
described in the CHANGELOG. Everything else follows the usual rule: removal
or breaking change of a stable API needs (1) a deprecation note in the
CHANGELOG and the docs for at least one minor release, and (2) a major
version to actually remove it. The wire protocol has its own, stronger
promise — see [protocol.md](./docs/reference/protocol.md#stability): packet
shapes never break inside a major, and the subprotocol names the core's
revision on the wire — `wrpc.v2` since 2.0 (framed messages), negotiated
down to `wrpc.v1` with a 1.0 peer. A change a 1.0 peer cannot ignore belongs
behind the revision, and `tests/interop/` runs the published 1.0 against the
tree to keep that true.

## Release checklist

Releases are **manual**. Nothing in the tooling bumps a version, tags, or
publishes.

1. **Green everything** on the release commit — what CI's three always-on
   jobs run, chained so a gate that fails stops the chain. It is the
   `prepublishOnly` script, which `pnpm publish` runs again by itself:
   ```bash
   pnpm run prepublishOnly
   ```
   (`pnpm lint && pnpm format:check && pnpm test:ci && pnpm test:coverage:floors && pnpm test:types && pnpm check:dts && pnpm size && pnpm docs:build`).
   `test:coverage:floors` reads what `test:ci` left in `coverage/tmp`, so it
   runs right after it. CI's `test` job is a matrix of Node 22.0.0 (the floor
   of `engines`, exactly), 22, 24 and 26, and they differ (Node 22's runner cancels a file whose event loop goes empty):
   run the suite on each before calling it green. Then the suites CI
   deliberately does not run — the list under [Tests](#tests): Autobahn in
   both modes, `pnpm test:perf`, the WebRTC and WebTransport integration
   suites, the four broker ones.
   `tests/package/consistency.test.js` already fails this step if
   `docs/.vitepress/config.mts`'s keywords or nav version label drift from
   `package.json` — there is no separate manual sync step for either.
2. **Move `[Unreleased]` to the new version** in `CHANGELOG.md`, with the date,
   and open a fresh empty `[Unreleased]` above it — with an empty
   `### Changed (breaking)` section, which `tests/package/consistency.test.js`
   requires to exist. **Read that section before choosing the bump**: a
   non-empty one is a major, whatever the other sections say, and the same
   test fails the build if an entry marked `**Breaking` sits anywhere else
   (an "Added" feature that a 1.x peer cannot ignore is breaking too). A
   non-empty breaking section also comes with an `### Upgrading from <version>`
   section — the order of upgrade and the flags that keep an old peer working.
   The headings are Keep a Changelog's six plus `Changed (breaking)`,
   `Documentation`, `Upgrading from X.Y` and `Tooling`, and the new version
   gets its line in the link block at the bottom (`[X.Y.Z]: …/compare/vA...vX.Y.Z`,
   `[Unreleased]` moved on to compare from the new tag); the same test holds
   both.
3. **Bump `version` in `package.json`, and confirm it differs from what npm
   already has.** Semver against the **JavaScript API**; the wire protocol has
   [its own promise](./docs/reference/protocol.md#stability). Run
   `pnpm view @alexify/wrpc version` and make sure the bumped value doesn't
   match it (`pnpm view @alexify/wrpc versions` lists every one, 1.0.0
   included) — publishing an already-used version is a rejected
   `pnpm publish`, not a warning.
4. **Check what would ship.** `files` in `package.json` is an explicit
   allowlist — the main entry, `bin/`, `src/` and the four documents — so a
   root file that was not added to it silently disappears from the tarball:
   ```bash
   pnpm pack --dry-run
   ```
   Verify the file list contains `index.js`, `index.d.ts`, `bin/` and `src/`
   with every `.d.ts` in it, **no** `CLAUDE.md` (`!**/CLAUDE.md` keeps the
   agents' files out; a test asks pnpm's own packing list), and **not**
   `docs/`, `tests/`, `bench/`, `scripts/`, `examples/` or `coverage/`.
5. **Verify the tarball resolves**, CommonJS and ESM alike. Pack it in the
   repository with `pnpm pack`, then install it into a temporary consumer in
   a scratch directory — the one place npm is used, as a consumer would:
   ```bash
   npm install /path/to/alexify-wrpc-<version>.tgz
   node -e "require('@alexify/wrpc'); require('@alexify/wrpc/engine'); require('@alexify/wrpc/query')"
   node --input-type=module -e "import { Server, connect } from '@alexify/wrpc'; import { createNodeEngine } from '@alexify/wrpc/engine'; void Server; void connect; void createNodeEngine;"
   npx wrpc --version
   ```
   and that a bundler-resolution consumer type-checks against the shipped
   `.d.ts` files (`tsd` only ever checks `tests/`, never the installed
   tarball):
   ```bash
   npm install typescript @types/node
   printf "import { connect } from '@alexify/wrpc';\nvoid connect;\n" > smoke.ts
   npx tsc --moduleResolution bundler --module esnext --types node --noEmit smoke.ts
   ```
   `--types node` is not optional: the typings import `node:http` and name
   `Buffer`, and since TypeScript 6 a compiler run with no `tsconfig.json`
   includes no `@types` package unless told to — without the flag the smoke
   fails on the first line of `src/engine/index.d.ts`, whatever the tarball
   holds. TypeScript 7 has no `moduleResolution: node10` any more; the
   `typesVersions` that serves it on TypeScript ≤ 6 is checked in every mode
   by `tests/package/resolution.test.js`, with tsd's TypeScript (5.9).
6. **Commit, tag, push**: `git tag v<version> && git push --follow-tags`.
7. **Publish**: `pnpm publish` (`publishConfig.access` is already `public`),
   which runs `prepublishOnly` — step 1's gates — before anything leaves.
   The release commit is `chore(release): X.Y.Z`.
8. **Release notes** on GitHub from the CHANGELOG entry.
9. **The docs site deploys from `main` through Vercel** (`vercel.json` pins
   the build command and output directory) — no separate step, but confirm it
   actually landed before announcing anything:
   ```bash
   curl -o /dev/null -sw '%{http_code}\n' https://wrpc.vercel.app/
   curl -o /dev/null -sw '%{http_code}\n' https://wrpc.vercel.app/reference/protocol
   ```
   Both must answer `200` — the site has previously 404'd on everything while
   the deploy was pointed at a branch without the docs on it.
10. **Refresh the size numbers quoted in prose.** README's bundle-size table,
    `docs/guide/browser.md`, and any "under N KB" claim in `docs/index.md` or
    `docs/guide/client.md` are hand-copied from the `pnpm size` output already
    produced in step 1 — there is no guard test for prose, so they drift the
    moment a budget changes and nothing catches it until someone notices.

Releases stay a person running these commands by hand, on purpose: no
GitHub Actions release workflow, no publish provenance step.
