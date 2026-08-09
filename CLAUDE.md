# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

wrpc (`@alexify/wrpc`) is intended to be a fast, low-overhead, zero-dependency, WebSocket-based RPC protocol for Node.js and browsers.

**Protocol architecture — TODO.** The protocol itself has not been implemented yet (`src/index.js` is a placeholder). This section will be filled in once the design lands: wire format/framing, request/response and streaming semantics, error handling, and the module layout under `src/`.

Package manager is **pnpm** (`packageManager: pnpm@10.34.5`). CommonJS throughout (`require`/`module.exports`), no build/transpile step — `src/` ships as-is (same philosophy as the sibling `@alexify/kerberos` project).

## Commands

```bash
pnpm test              # run all tests: node --test tests/*.test.js
node --test tests/smoke.test.js            # run a single test file
node --test --test-name-pattern="..."      # filter tests by name
pnpm test:types        # type-check tests/*.test-d.ts against index.d.ts via tsd
pnpm test:coverage     # c8 coverage over src/ (thresholds: 95% lines/statements, 90% branches, 95% functions)
pnpm lint               # oxlint src tests scripts bench
pnpm format             # oxfmt src tests scripts bench (format:check for CI)
pnpm bench              # ops/sec benchmark harness (bench/bench.js)
pnpm size                # bundle-size report (scripts/size.js), also run in CI's lint job
pnpm docs:dev            # VitePress dev server for docs/ (docs:build / docs:preview too)
```

Linting/formatting is **oxlint/oxfmt** (`.oxlintrc.json`, `.oxfmtrc.json`; the `correctness` category is intentionally off) — their native bindings require Node ≥20.19. CI (`.github/workflows/ci.yml`) runs three jobs: `lint` (Node 22 — lint, format:check, size), `test` (Node 22/24 matrix — test:coverage, test:types), `docs` (Node 22 — docs:build). Lint/format deliberately target `src tests scripts bench` only, so `docs/` is not covered by them.

Style: 2-space indent, single quotes, semicolons, 120-char lines (see `.editorconfig`, `.oxfmtrc.json`).

## Package layout conventions

Mirrors the `@alexify/kerberos` entry-point pattern:

- `index.js` → `require('./src/index.js')` — the Node/default entry.
- `browser.js` → mirrors `index.js`; resolved via the `package.json#browser` field map and the `browser` condition in `exports`. Do not deduplicate the two files even once they're identical — this is where a future platform split (see below) would diverge them.
- `index.d.ts` — hand-maintained types (not generated); currently a placeholder.
- `src/index.js` — the single barrel that will assemble the package's public surface as modules are added.
- Future platform-specific code (if any) belongs in `src/runtime/node.js` / `src/runtime/browser.js`, swapped via the `browser` field — re-add that mapping to `package.json#browser` only once those files exist.

## Testing

Tests live in `tests/` (not `test/` — this differs from `kerberos`), run via Node's built-in `node --test` runner, no vitest/jest config. `tests/smoke.test.js` is currently the only test — it just asserts the package loads and exports an object; replace/extend it as the protocol is implemented. Type tests use `tsd` (`tsd.directory: "tests"` in `package.json`), currently `tests/index.test-d.ts`.

## Documentation site (`docs/`)

VitePress site deployed to Vercel (`vercel.json` pins the build command/output dir); `docs/` is never published to npm (the `files` field in `package.json` is an explicit allowlist that omits it). Currently a minimal skeleton (`docs/.vitepress/config.mts`, a `theme/` extending `DefaultTheme`, `docs/index.md` home page, one `docs/guide/getting-started.md` page) — expand the nav/sidebar in `config.mts` as more guide/API pages are added, following the fuller structure in `kerberos/docs/.vitepress/config.mts` as a reference.
