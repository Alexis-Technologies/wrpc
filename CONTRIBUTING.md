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
pnpm test:types        # tsd against the .d.ts files
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

## House rules

**Zero runtime dependencies.** `package.json` has no `dependencies` field, and
no `peerDependencies` or `optionalDependencies` either. Anything reused from
another package is copied in and adapted, not installed. Optional integrations
(uWebSockets.js, Redis, fastify, express, TanStack) are **injected by the
caller** and validated structurally — duck typing, never `instanceof` against
an imported class. Nothing under `src/` may `require()` a framework.

**A `.d.ts` change and its `tsd` test land together.** The declarations are
hand-maintained, one file per subpath, and they drift silently otherwise. Every
new subpath brings a root shim, a root `.d.ts`, an `exports` entry, a `files`
entry and a `tests/<name>.test-d.ts`.

**`tsd` type-checks with `@tsd/typescript` (5.9), not the repo's own
`typescript`.** A type relying on newer inference has to be checked against
both.

**Wire-protocol changes are documented in
[`docs/reference/protocol.md`](./docs/reference/protocol.md)**, which is frozen
at 1.0: additive, optional fields within a major version; anything else is a
major.

**Style** is enforced by oxlint/oxfmt: 2-space indent, single quotes,
semicolons, 120-column lines. The `correctness` category is intentionally off,
and `no-void` is intentionally not enabled — `return void fn()` is the idiom
this codebase uses for explicit fire-and-forget.

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

> **A leaked uws engine hangs `node --test` forever.** A standalone engine holds
> a native listen socket; if a test fails an assertion before its cleanup line
> runs, the socket is never released and the whole run wedges instead of
> reporting a failure. Register teardown with `t.after(...)` **immediately after
> booting**, never as a trailing `await close()` at the end of the test body.

## CI

Three jobs (`.github/workflows/ci.yml`):

- **lint** (Node 22) — `lint`, `format:check`, `size`
- **test** (Node 22 and 24) — `test:coverage`, `test:types`
- **docs** (Node 22) — `docs:build`, which fails on dead links

Lint and format deliberately target `src tests scripts bench bin` only, so
`docs/` is not covered by them.

Three checks are deliberately **not** in CI — run them by hand:

- `node scripts/autobahn/run.js` — the RFC 6455/7692 conformance suite against
  the engine in `src/websocket/`. Needs docker; several minutes for 500+
  cases. `FAILED` and `WRONG CODE` fail the run, `NON-STRICT` and
  `INFORMATIONAL` do not.
- `pnpm test:perf` — the 1 GiB stream memory guard.
- `REDIS_URL=redis://127.0.0.1:6379 node --test tests/scaling/redis.integration.test.js`
  — the scaling backplane against a real Redis. `pnpm test` already covers the
  adapter's contract through an in-repo ioredis-shaped fake
  (`tests/scaling/redis.test.js`); this is only useful when you want to check
  a live server, and the file skips itself without `REDIS_URL`.

## Stability and deprecation

The published surface is stable under semver, with two carve-outs marked
`@experimental` in the `.d.ts` files:

- the **telemetry** writer shapes and metric set (`telemetry` option) — the
  signals will keep improving in minors;
- the **engine port** internals beyond the documented `WrpcSocket` contract
  (`capabilities` in particular).

An `@experimental` API may change in a minor release, with the change
described in the CHANGELOG. Everything else follows the usual rule: removal
or breaking change of a stable API needs (1) a deprecation note in the
CHANGELOG and the docs for at least one minor release, and (2) a major
version to actually remove it. The wire protocol has its own, stronger
promise — see [protocol.md](./docs/reference/protocol.md#stability): packet
shapes never break inside a major, and the `wrpc.v1` subprotocol names the
revision on the wire.

## Release checklist

Releases are **manual**. Nothing in the tooling bumps a version, tags, or
publishes.

1. **Green everything** on the release commit:
   ```bash
   pnpm lint && pnpm format:check && pnpm test:coverage && pnpm test:types && pnpm size && pnpm docs:build
   ```
   `tests/package/consistency.test.js` already fails this step if
   `docs/.vitepress/config.mts`'s keywords or nav version label drift from
   `package.json` — there is no separate manual sync step for either.
2. **Move `[Unreleased]` to the new version** in `CHANGELOG.md`, with the date,
   and open a fresh empty `[Unreleased]` above it. Before the first publish,
   `[Unreleased]` carries a "not published to npm yet" note right under its
   heading — delete that note as part of this move; it does not apply again
   after v1.0.0 ships.
3. **Bump `version` in `package.json`, and confirm it differs from what npm
   already has.** Semver against the **JavaScript API**; the wire protocol has
   [its own promise](./docs/reference/protocol.md#stability). Before the first
   publish there is nothing on the registry to compare against, so this half
   of the step is a no-op; from the second release on, run
   `npm view @alexify/wrpc version` and make sure the bumped value doesn't
   match it — publishing an already-used version is a rejected `npm publish`,
   not a warning.
4. **Check what would ship.** `files` in `package.json` is an explicit
   allowlist, so a new root shim or `.d.ts` that was not added to it silently
   disappears from the tarball:
   ```bash
   npm publish --dry-run
   ```
   Verify the file list contains every root shim, every `.d.ts`, `bin/`, `src/`,
   and **not** `docs/`, `tests/`, `bench/`, `scripts/`, `examples/` or
   `coverage/`.
5. **Verify the tarball resolves**, CommonJS and ESM alike. In a scratch
   directory:
   ```bash
   npm pack
   npm install ./alexify-wrpc-<version>.tgz
   node -e "require('@alexify/wrpc'); require('@alexify/wrpc/engine'); require('@alexify/wrpc/query')"
   node --input-type=module -e "import { Server, connect } from '@alexify/wrpc'; import { createNodeEngine } from '@alexify/wrpc/engine'; void Server; void connect; void createNodeEngine;"
   npx wrpc --version
   ```
   and that a bundler-resolution consumer type-checks against the shipped
   `.d.ts` files (`tsd` only ever checks `tests/`, never the installed
   tarball):
   ```bash
   printf "import { connect } from '@alexify/wrpc';\nvoid connect;\n" > /tmp/wrpc-esm-smoke.ts
   npx tsc --moduleResolution bundler --module esnext --noEmit /tmp/wrpc-esm-smoke.ts
   ```
6. **Commit, tag, push**: `git tag v<version> && git push --follow-tags`.
7. **Publish**: `npm publish` (`publishConfig.access` is already `public`).
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
