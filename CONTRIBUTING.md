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

## Release checklist

Releases are **manual**. Nothing in the tooling bumps a version, tags, or
publishes.

1. **Green everything** on the release commit:
   ```bash
   pnpm lint && pnpm format:check && pnpm test:coverage && pnpm test:types && pnpm size && pnpm docs:build
   ```
2. **Move `[Unreleased]` to the new version** in `CHANGELOG.md`, with the date,
   and open a fresh empty `[Unreleased]` above it.
3. **Bump `version` in `package.json`.** Semver against the **JavaScript API**;
   the wire protocol has [its own promise](./docs/reference/protocol.md#stability).
4. **Sync the version in `docs/.vitepress/config.mts`** — the nav dropdown label
   is hand-maintained on purpose, so it is a checklist item rather than a
   surprise.
5. **Check what would ship.** `files` in `package.json` is an explicit
   allowlist, so a new root shim or `.d.ts` that was not added to it silently
   disappears from the tarball:
   ```bash
   npm publish --dry-run
   ```
   Verify the file list contains every root shim, every `.d.ts`, `bin/`, `src/`,
   and **not** `docs/`, `tests/`, `bench/`, `scripts/` or `coverage/`.
6. **Verify the tarball resolves.** In a scratch directory:
   ```bash
   npm pack
   npm install ./alexify-wrpc-<version>.tgz
   node -e "require('@alexify/wrpc'); require('@alexify/wrpc/engine'); require('@alexify/wrpc/query')"
   npx wrpc --version
   ```
7. **Commit, tag, push**: `git tag v<version> && git push --follow-tags`.
8. **Publish**: `npm publish` (`publishConfig.access` is already `public`).
9. **Release notes** on GitHub from the CHANGELOG entry.
10. The docs site deploys from `main` through Vercel (`vercel.json` pins the
    build command and output directory) — no separate step.
