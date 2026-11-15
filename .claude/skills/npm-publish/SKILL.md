---
name: npm-publish
description: Prepare an @alexify package for publishing with pnpm. Handles version bump, CHANGELOG update, typings check, quality gates, README and docs refresh, package.json audit, and pnpm pack verification. Use when the user asks to prepare a release, bump version, publish a package, or do release prep.
---

<!-- Adapted from metaskills v1.0.5 (skills/npm-publish) for wRPC: pnpm instead of npm for
     everything but the pack and the publish, the gates CI runs plus the suites it deliberately
     does not, Keep a Changelog headings as used in CHANGELOG.md (with its "Changed (breaking)"
     guard), and the hand-run "Release checklist" of CONTRIBUTING.md, which wins if the two ever
     disagree. -->

# npm Publish Preparation

Systematic workflow for preparing `@alexify/wrpc` for a new release. Every step must pass before proceeding to the next. The package manager is pnpm (pinned in `packageManager`); never create `package-lock.json`. Releases are run by hand on purpose: there is no release workflow, no publish provenance step and no dependency bot, and none is to be proposed. **This skill never commits, tags, pushes or publishes** — the bump commit, the tag, `npm publish` and the merge into `main` are the owner's.

The package is published (`npm view @alexify/wrpc versions` lists every version, 1.0.0 included). Semver here versions the **JavaScript API**; the wire protocol has its own, narrower promise (`docs/reference/protocol.md#stability`).

## Step 0: Gather Release Intent

Ask the user (use AskUserQuestion if available):

1. **Version bump type**: patch / minor / major / explicit version / prerelease tag
2. **Prerelease label** (if any): e.g. `alpha`, `beta`, `rc`
3. **Confirm changelog entries**: ask if the `[Unreleased]` section in CHANGELOG is complete, or if the user wants to draft entries from recent commits

If the user already specified intent (e.g. "prepare 2.1.0 release"), skip the question and proceed.

**Read `### Changed (breaking)` under `[Unreleased]` before proposing a bump:** a non-empty one is a major, whatever the other sections say.

## Step 1: Pre-flight Checks

Run, chained with `&&` (a gate that fails must stop the chain), and read the WHOLE output of `pnpm size` — never a grep for one row:

```bash
pnpm install --frozen-lockfile   # install deps exactly as locked
pnpm lint                        # oxlint over src tests scripts bench bin examples
pnpm format:check                # oxfmt
pnpm test:ci                     # the suite under the c8 gate (95 lines / 95 statements / 90 branches / 95 functions)
pnpm test:coverage:floors        # per-file floors; reads coverage/tmp, so right after the line above
pnpm test:types                  # tsd against the .d.ts files
pnpm size                        # the bundle report; a browser budget that is exceeded fails the run
pnpm docs:build                  # the docs site; a dead link fails it
```

These are CI's three always-on jobs (lint, test, docs). CI's `test` job runs on Node 22 **and** 24, and the two differ (Node 22's runner cancels a file whose event loop goes empty): run the suite on both before calling it green. Do not lower a coverage threshold or a floor, and do not raise a size budget to get green; a budget is a ratchet raised deliberately in `scripts/size.js`, in the change that earns the bytes, with the reason in a comment next to the number.

The suites CI deliberately does not run — run them by hand for a release:

```bash
pnpm test:perf                                                                   # the 1 GiB stream memory guard
node scripts/autobahn/run.js                                                     # Autobahn, both modes; on an otherwise idle Docker
WRPC_RTC=node-datachannel node --test tests/webrtc/node-datachannel.integration.test.js
node scripts/wt-cert.js certs                                                    # the certificate lives 14 days at most
WRPC_WT=fails node --test tests/wt/fails.integration.test.js
WRPC_WT=quico node --test tests/wt/quico.integration.test.js
pnpm brokers:up                                                                  # then each tests/broker/*.integration.test.js with its env var
```

Then in parallel: `pnpm outdated` and `pnpm audit` (report to the user; devDependencies only — the package has no `dependencies`, `peerDependencies` or `optionalDependencies` field, and must not gain one).

If any gate fails, stop and fix issues before continuing.

## Step 2: Version Bump

### Read current version from `package.json`

Compute the new version based on user intent and semver rules:

- **patch**: `1.2.3` → `1.2.4`
- **minor**: `1.2.3` → `1.3.0`
- **major**: `1.2.3` → `2.0.0`
- **prerelease**: `1.2.3` → `1.2.4-<label>.0`, or `1.2.4-beta.0` → `1.2.4-beta.1`
- **Removing prerelease tag**: `4.0.3-prerelease` → `4.0.3` (just strip the suffix)

Confirm the new value differs from what npm already has (`npm view @alexify/wrpc version`): publishing a used version is a rejected `npm publish`, not a warning.

Update `"version"` in `package.json` with the Edit tool, not `pnpm version`: the bump is a plain edit, and the commit, tag and changelog are handled by hand.

Change the nav label in `docs/.vitepress/config.mts` (`text: 'v<version>'`) in the same edit: it is hand-synced, and `tests/package/consistency.test.js` fails when the two differ.

## Step 3: Update CHANGELOG.md

The changelog follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/):

- **Sections**: `## [Unreleased]` at top; `## [X.Y.Z] - YYYY-MM-DD` for releases (date in ISO 8601)
- **Order**: newest first; every version gets an entry
- **Types of changes**: `Changed (breaking)`, `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`. Omit empty subsections — except `### Changed (breaking)` under `[Unreleased]`, which must exist even when empty (`tests/package/consistency.test.js` requires it)
- **Entries**: bullets starting with a **bold lead-in**, then the explanation; human-readable and curated (no raw git diffs)
- **Breaking changes**: an entry marked `**Breaking` sits in `Changed (breaking)` and nowhere else (the same test fails the build otherwise — an "Added" feature a 1.x peer cannot ignore is breaking too), and a non-empty breaking section carries a `#### Migrating from <version>` block: the order of upgrade and the flags that keep an old peer working
- **Links**: the file has no reference-link block at the bottom; do not add one

### 3a: Detect changelog structure

Read the full `CHANGELOG.md`. Identify the `[Unreleased]` section, the previous release heading (the comparison base) and whether `Changed (breaking)` has entries. Release tags are `vX.Y.Z`.

### 3b: Create the new version entry

**Always** draft entries from `git log` since the previous release tag:

```bash
git log $(git describe --tags --abbrev=0)..HEAD --oneline
```

Commits follow Conventional Commits: map `feat` → Added, `fix` → Fixed, `refactor!`/`feat!`/`fix!` → breaking, `ci`/`chore` → only when user-visible. Merge with the existing `[Unreleased]` content; it is usually already written.

Turn `[Unreleased]` into the release and open a fresh one above it, with its empty breaking section:

```markdown
## [Unreleased]

### Changed (breaking)

## [NEW_VERSION] - YYYY-MM-DD

### Added

- **Feature name.** What it does and why it matters.
```

## Step 4: TypeScript Declarations

The types are hand-written, never generated: `index.d.ts` (the Node surface), `client.d.ts` and `rpc.d.ts` (the node-free halves `index.d.ts` re-exports — neither has a `.js` at runtime), `browser.d.ts`, and one root `.d.ts` per published subpath (`*.browser.d.ts` for the subpaths with a `browser` condition):

1. `pnpm test:types` (tsd, `tests/*.test-d.ts` and `tests/broker/*.test-d.ts`) must pass with no errors
2. Read `src/index.js`, `src/index.browser.js` and every subpath's entry (`package.json#exports`) and confirm every exported class, function, type and method is declared, and no private (`#`) field leaks into the declarations
3. Every published subpath has its own tsd file — the guard `every published subpath has a tsd file` in `tests/package/consistency.test.js` is the list
4. If the release includes API changes, update the typings and the `.test-d.ts` together. `tsd` type-checks with `@tsd/typescript` (5.9), not the repo's own `typescript` (7.x): a type that leans on newer inference is checked against both

## Step 5: README, Docs & License Review

**Update the year** of the copyright line in `LICENSE` if the release is in a later year than the one it carries.

Scan for staleness:

1. **README.md usage examples**: do they match the current API? Run them if in doubt
2. **Bundle-size numbers in prose**: README's table, `docs/guide/browser.md`, and any "under N KB" claim in `docs/index.md` or `docs/guide/client.md` are hand-copied from `pnpm size` — refresh them from the output of Step 1
3. **docs/reference/protocol.md**: every `##` section 1.0 did not have carries a `since 2.0` badge, and `## Changes since 1.0` says what an older peer does with each addition (the consistency test keeps both). An addition made after 2.0 that an older 2.x peer cannot ignore is asked for by an additive field, never by the revision
4. **docs/**: guide and reference pages for anything the release changed; a new page needs its sidebar entry in `docs/.vitepress/config.mts`. `pnpm docs:build` must pass
5. **Benchmark charts**: a re-run benchmark updates both the table on its page and `docs/.vitepress/theme/benchmarks.json`
6. **docs keywords**: the `keywords` array in `docs/.vitepress/config.mts` mirrors `package.json#keywords` (the consistency test checks it)
7. **docs/public/llms.txt** and **CLAUDE.md**: key facts (exports, Node.js version, sizes, the paths and symbols CLAUDE.md names — its guard refuses a path that does not exist)
8. **SECURITY.md**: the supported-versions table, on a major release

Only edit if something is factually wrong or outdated. Don't rewrite style or add unsolicited content.

## Step 6: Package Audit

### 6a: Verify `package.json` fields

Check these fields are correct:

- `"main"` / `"exports"` / `"browser"` / `"bin"` — point to existing files; every subpath lists `types` first, the subpaths with a `browser` condition resolve to their `*.browser.js` half, and the `browser` map covers every platform pair
- `"types"` — points to `index.d.ts`
- `"files"` — the explicit allowlist includes every root shim, every root `.d.ts`, `bin/`, `src/`, `LICENSE`, `README.md`, `CHANGELOG.md`, `SECURITY.md` — and nothing else. A new shim or `.d.ts` that is missing from it silently disappears from the tarball
- `"engines"` — `node >=22`, matches the CI matrix (`.github/workflows/ci.yml`)
- `"dependencies"`, `"peerDependencies"`, `"optionalDependencies"` — absent, all three; every entry of `devDependencies` is test-, bench- or docs-only
- `"keywords"` — no typos, relevant terms
- `"license"`, `"repository"`, `"homepage"`, `"bugs"`, `"publishConfig"` (`access: public`) — valid

`tests/package/consistency.test.js` is the machine-checked half of this list. If it goes red after a change to an entry point, update `exports`, `files`, the `browser` map, `scripts/size.js` `ENTRIES` and the docs keywords; the test is right.

### 6b: Dry-run pack

```bash
npm publish --dry-run
```

Review the file list:

- Every root shim, every `.d.ts`, `bin/` and `src/` are included
- `docs/`, `tests/`, `bench/`, `scripts/`, `examples/`, `coverage/`, `certs/`, `.claude/` and CI files are excluded
- No unexpected large files

Then pack for real into a scratch directory and smoke the tarball, CommonJS and ESM alike:

```bash
npm pack
npm install ./alexify-wrpc-<version>.tgz
node -e "require('@alexify/wrpc'); require('@alexify/wrpc/engine'); require('@alexify/wrpc/query')"
node --input-type=module -e "import { Server, connect } from '@alexify/wrpc'; import { createNodeEngine } from '@alexify/wrpc/engine'; void Server; void connect; void createNodeEngine;"
npx wrpc --version
npx tsc --moduleResolution bundler --module esnext --noEmit <a file that imports { connect } from '@alexify/wrpc'>
```

(`tsd` only ever checks `tests/`, never the installed tarball.) Report the file count and the bundle sizes from `pnpm size` to the user.

## Step 7: Final Verification

Run the gates one more time after all edits:

```bash
pnpm lint && pnpm format:check && pnpm test:ci && pnpm test:coverage:floors && pnpm test:types && pnpm size && pnpm docs:build
```

Must exit 0. If it fails, fix and re-run.

## Step 8: Summary & Next Steps

Present a summary to the user:

```
Release preparation complete:
  Package:  <name>
  Version:  <old> → <new>
  Tests:    ✓ passing on Node 22 and 24 (coverage gate and floors met)
  Types:    ✓ valid
  Packed:   <N> files, <min+gzip of the browser entries>
```

Then list the steps that are the owner's — none of them is run by this skill:

1. Commit the release (`chore(release): X.Y.Z`) on the release branch
2. Merge the branch into `main`
3. `git tag v<version> && git push --follow-tags`
4. `npm publish` from that commit (for prereleases: `npm publish --tag <label>`)
5. Write the GitHub release notes from the changelog section
6. Verify the docs deploy — both must answer `200`:
   `curl -o /dev/null -sw '%{http_code}\n' https://wrpc.vercel.app/` and
   `curl -o /dev/null -sw '%{http_code}\n' https://wrpc.vercel.app/reference/protocol`

Do NOT run `npm publish`, `pnpm publish`, `git tag` or `git push` — the owner does.

## Step 9: Lockfile

Run `pnpm install` only if dependencies changed during the release; the updated `pnpm-lock.yaml` goes with the release. Never generate or commit `package-lock.json`.
