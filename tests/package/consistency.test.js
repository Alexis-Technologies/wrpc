'use strict';

// The machine-checked version of the release checklist's "check what would
// ship" step. `files` is an explicit allowlist, so a new root shim or .d.ts
// that was not added to it silently disappears from the tarball; an exports
// entry whose file does not exist fails only at a consumer's first import.
// This suite makes every one of those a red test instead.

const path = require('node:path');
const { existsSync } = require('node:fs');
const { test } = require('node:test');
const assert = require('node:assert');

const ROOT = path.join(__dirname, '..', '..');
const pkg = require('../../package.json');

// Every file an exports condition can resolve to, flattened.
const exportTargets = (value, out = []) => {
  if (typeof value === 'string') out.push(value);
  else if (value && typeof value === 'object') {
    for (const nested of Object.values(value)) exportTargets(nested, out);
  }
  return out;
};

const shipped = (relative) => {
  // The allowlist ships whole directories (src/, bin/) and named root files.
  const clean = relative.replace(/^\.\//, '');
  return pkg.files.some((entry) => (entry.endsWith('/') ? clean.startsWith(entry) : clean === entry));
};

test('every exports target exists and is in the files allowlist', () => {
  for (const [key, value] of Object.entries(pkg.exports)) {
    for (const target of exportTargets(value)) {
      assert.ok(existsSync(path.join(ROOT, target)), `${key} -> ${target} does not exist`);
      assert.ok(shipped(target), `${key} -> ${target} is not in the files allowlist`);
    }
  }
});

test('every subpath has a types condition wherever it has a runtime one', () => {
  for (const [key, value] of Object.entries(pkg.exports)) {
    if (key === './package.json') continue;
    const check = (conditions, label) => {
      assert.ok(typeof conditions === 'object', `${label} must be a conditions object`);
      assert.ok(conditions.types, `${label} has no types condition`);
      assert.ok(conditions.default, `${label} has no default condition`);
      // exports conditions match top-down: a `types` AFTER `default` is dead.
      const keys = Object.keys(conditions);
      assert.ok(keys.indexOf('types') < keys.indexOf('default'), `${label}: types must come before default`);
      if (conditions.browser && typeof conditions.browser === 'object') check(conditions.browser, `${label}.browser`);
    };
    check(value, key);
  }
});

test('every root shim requires a file that exists', () => {
  for (const [key, value] of Object.entries(pkg.exports)) {
    for (const target of exportTargets(value)) {
      if (!target.endsWith('.js')) continue;
      const source = require('node:fs').readFileSync(path.join(ROOT, target), 'utf8');
      const match = source.match(/require\('([^']+)'\)/);
      if (!match) continue; // not a shim
      const required = path.join(ROOT, path.dirname(target), match[1]);
      assert.ok(existsSync(required), `${key} shim ${target} requires missing ${match[1]}`);
    }
  }
});

test('every published subpath has a tsd file', () => {
  for (const key of Object.keys(pkg.exports)) {
    if (key === './package.json') continue;
    const name = key === '.' ? 'index' : key.slice(2);
    const candidates = [
      path.join(ROOT, 'tests', `${name}.test-d.ts`),
      path.join(ROOT, 'tests', name, 'index.test-d.ts'),
    ];
    assert.ok(
      candidates.some((candidate) => existsSync(candidate)),
      `${key} has no tests/${name}.test-d.ts`,
    );
  }
});

test('the browser field map points at files that ship', () => {
  for (const [from, to] of Object.entries(pkg.browser)) {
    for (const file of [from, to]) {
      assert.ok(existsSync(path.join(ROOT, file)), `browser map: ${file} does not exist`);
      assert.ok(shipped(file), `browser map: ${file} is not in the files allowlist`);
    }
  }
});

test('the bin shim exists, ships, and requires a shipped file', () => {
  for (const [name, target] of Object.entries(pkg.bin)) {
    assert.ok(existsSync(path.join(ROOT, target)), `bin ${name} -> ${target} does not exist`);
    assert.ok(shipped(target), `bin ${name} -> ${target} is not in the files allowlist`);
  }
});

test('every file the allowlist names exists', () => {
  for (const entry of pkg.files) {
    assert.ok(existsSync(path.join(ROOT, entry.replace(/\/$/, ''))), `files entry ${entry} does not exist`);
  }
});

// ---------------------------------------------------------------------------
// The hand-synced pairs CLAUDE.md names (and two the review found unnamed):
// every one of these used to be a release-checklist memory item, and two had
// already drifted by the time the guard below was written.

const read = (relative) => require('node:fs').readFileSync(path.join(ROOT, relative), 'utf8');

test('docs/.vitepress/config.mts mirrors package.json keywords and version', () => {
  const config = read('docs/.vitepress/config.mts');
  const start = config.indexOf('const keywords = [');
  assert.ok(start >= 0, 'config.mts must keep its keywords array');
  const block = config.slice(start, config.indexOf(']', start));
  const mirrored = [...block.matchAll(/'([^']+)'/g)].map((match) => match[1]);
  assert.deepStrictEqual(mirrored, pkg.keywords, 'config.mts keywords must mirror package.json exactly');
  assert.ok(config.includes(`'v${pkg.version}'`), `the nav version label must read v${pkg.version}`);
});

test('RPC_OPTION_KEYS matches the RpcServer constructor destructure', () => {
  const core = read('src/rpc/core.js');
  const keysStart = core.indexOf('const RPC_OPTION_KEYS = [');
  const keys = [...core.slice(keysStart, core.indexOf(']', keysStart)).matchAll(/'([^']+)'/g)].map((m) => m[1]);
  assert.ok(keys.length > 0);
  const ctorStart = core.indexOf('constructor(options = {})');
  const destructure = core.slice(core.indexOf('const {', ctorStart), core.indexOf('} = options;', ctorStart));
  const names = [...destructure.matchAll(/^\s{6}(\w+)/gm)].map((m) => m[1]);
  // Set equality both ways: a key the shells would drop (in the constructor
  // but not the list) and a stale key (listed but no longer destructured)
  // are both silent bugs.
  assert.deepStrictEqual([...names].sort(), [...keys].sort());
});

test('every exports subpath has a bundle-size row in scripts/size.js', () => {
  const size = read('scripts/size.js');
  const entries = [...size.matchAll(/entry: '([^']+)'/g)].map((m) => m[1]);
  for (const [key, value] of Object.entries(pkg.exports)) {
    if (key === './package.json') continue;
    const targets = exportTargets(value).map((target) => target.replace(/^\.\//, ''));
    // .d.ts targets are types conditions, not bundles.
    const runtime = targets.filter((target) => target.endsWith('.js'));
    assert.ok(
      runtime.some((target) => entries.includes(target)),
      `${key} has no ENTRIES row in scripts/size.js (targets: ${runtime.join(', ')})`,
    );
  }
});

// The value declarations of a root d.ts — class, function, const, let —
// with its re-exports followed (`export * from './x.js'` brings x's values;
// `export { a, b } from './x.js'` the named ones x declares as values).
// Interfaces and `export type` are not values and are not counted.
const declaredValues = (file, seen = new Set()) => {
  const names = new Set();
  if (seen.has(file) || !existsSync(path.join(ROOT, file))) return names;
  seen.add(file);
  const text = read(file);
  for (const match of text.matchAll(/^export (?:declare )?(?:abstract )?(?:class|function|const|let)\s+(\w+)/gm)) {
    names.add(match[1]);
  }
  for (const match of text.matchAll(/^export \* from '\.\/([\w./-]+)\.js';/gm)) {
    for (const name of declaredValues(`${match[1]}.d.ts`, seen)) names.add(name);
  }
  for (const match of text.matchAll(/^export \{([^}]*)\} from '\.\/([\w./-]+)\.js';/gm)) {
    const target = declaredValues(`${match[2]}.d.ts`, seen);
    for (const entry of match[1].split(',')) {
      const name = entry
        .trim()
        .split(/\s+as\s+/)
        .at(-1);
      if (name && target.has(name)) names.add(name);
    }
  }
  return names;
};

// Every barrel of every subpath, under both conditions, against the d.ts its
// `types` condition names — in BOTH directions: a runtime export the types
// never declare is an untyped API, and a value the types declare that the
// barrel never exports is a lie a user only discovers at runtime. One-way,
// over six barrels, this test missed both for a release.
test('runtime barrel exports and the hand-written types agree, both ways, on every subpath', () => {
  const cases = [];
  for (const [key, value] of Object.entries(pkg.exports)) {
    if (key === './package.json') continue;
    const conditions = [value];
    if (value.browser) conditions.push(value.browser);
    for (const condition of conditions) {
      if (typeof condition !== 'object' || !condition.default || !condition.types) continue;
      cases.push([condition.default.replace(/^\.\//, ''), condition.types.replace(/^\.\//, '')]);
    }
  }
  assert.ok(cases.length >= 22, `every subpath, both conditions: ${cases.length}`);
  for (const [barrel, dts] of cases) {
    let runtime;
    try {
      runtime = Object.keys(require(path.join(ROOT, barrel)));
    } catch (error) {
      // An adapter barrel whose framework is not installed here: nothing to
      // compare, and not this machine's failure (optional(), as the
      // adapter tests do).
      if (error.code === 'MODULE_NOT_FOUND') continue;
      throw error;
    }
    const declared = declaredValues(dts);
    for (const name of runtime) {
      assert.ok(declared.has(name), `${barrel} exports '${name}' but ${dts} never declares it`);
    }
    for (const name of declared) {
      assert.ok(runtime.includes(name), `${dts} declares '${name}' as a value but ${barrel} does not export it`);
    }
  }
});

// The size tables in README.md and docs/guide/browser.md are hand-pasted
// from `pnpm size`; the numbers go stale by design (the release checklist
// refreshes them), but a BUDGET that differs from scripts/size.js is a lie
// about what CI enforces — so every budget the tables show must be the
// script's, and every budgeted entry of the script must be in README's.
test('the budget column of the size tables in README and the browser guide matches scripts/size.js', () => {
  // scripts/size.js: every budgeted entry, as (subpath, half) -> budget,
  // from the entry FILE (browser.js is the main entry's browser half,
  // sse.browser.js the sse subpath's, query.js the query subpath's, ...).
  const script = read('scripts/size.js');
  const budgets = new Map();
  for (const match of script.matchAll(/entry: '([\w.]+)\.js',\s*platform: 'browser',\s*budget: (\d+)/g)) {
    // A budgeted entry IS a browser half (only browser-reachable entries carry
    // a budget), whatever the file is called: query.js, auth.js and
    // deflate.js are one file for both platforms.
    const [name] = match[1].split('.');
    const subpath = name === 'browser' ? '' : `/${name}`;
    budgets.set(`${subpath}|browser`, Number(match[2]));
  }
  assert.ok(budgets.size >= 7, `budgeted entries in scripts/size.js: ${budgets.size}`);
  // A table row that shows a budget: its subpath is the first backticked
  // token, its half is "browser" unless the row says "node".
  const shown = (text) => {
    const rows = new Map();
    const line = /^\| (\S[^|]*?) \| [\d.]+ KB \| \*\*[\d.]+ KB\*\* \| ([\d.]+) KB \|$/gm;
    for (const match of text.matchAll(line)) {
      const token = match[1].match(/@alexify\/wrpc(\/[\w/]+)?/);
      if (!token) continue;
      const half = / — node/.test(match[1]) ? 'node' : 'browser';
      rows.set(`${token[1] ?? ''}|${half}`, Number(match[2]));
    }
    return rows;
  };
  for (const file of ['README.md', 'docs/guide/browser.md']) {
    const rows = shown(read(file));
    assert.ok(rows.size >= 7, `${file}: budgeted rows found: ${rows.size}`);
    for (const [key, budget] of rows) {
      assert.ok(budgets.has(key), `${file}: ${key} shows a budget but scripts/size.js has none for it`);
      assert.strictEqual(
        budget,
        budgets.get(key),
        `${file}: ${key} shows ${budget} KB; scripts/size.js says ${budgets.get(key)}`,
      );
    }
    if (file === 'README.md') {
      for (const key of budgets.keys()) assert.ok(rows.has(key), `README.md has no row for the budgeted entry ${key}`);
    }
  }
});

// SECURITY.md is a release-checklist item: its version table must name the
// current major line (never a "pre-first-publish" that shipped), and its
// scope must name every directory that parses hostile bytes or holds keys.
test('SECURITY.md names the supported major, no pre-publish placeholder, and every hostile-input surface', () => {
  const text = read('SECURITY.md');
  const major = Math.max(2, Number(pkg.version.split('.')[0]));
  assert.ok(text.includes(`| \`${major}.x\``), `the supported row names ${major}.x`);
  assert.ok(!text.includes('pre-first-publish'), 'the pre-publish placeholder shipped once; never again');
  for (const surface of [
    'src/websocket/',
    'src/rpc/',
    'src/transport.js',
    'src/adapters/',
    'src/sse/',
    'src/auth/',
    'src/rpc/cluster.js',
    'src/cli/',
    'src/encryption/',
    'src/webtransport/',
    'src/deflate/inflate.js',
    'src/attachments.js',
    'src/webrtc/assertions.js',
    'src/broker/sealing.js',
  ]) {
    assert.ok(text.includes(surface), `SECURITY.md scope names ${surface}`);
  }
  assert.ok(text.includes('what-it-does-not-protect'), 'the documented limits are linked');
});

test('every ./x.js reference inside a shipped root d.ts resolves to a shipped x.d.ts', () => {
  const roots = pkg.files.filter((entry) => entry.endsWith('.d.ts'));
  for (const file of roots) {
    const text = read(file);
    const refs = [
      ...[...text.matchAll(/from '\.\/([\w.-]+)\.js'/g)].map((m) => m[1]),
      ...[...text.matchAll(/import\('\.\/([\w.-]+)\.js'\)/g)].map((m) => m[1]),
    ];
    for (const ref of refs) {
      const dts = `${ref}.d.ts`;
      assert.ok(existsSync(path.join(ROOT, dts)), `${file} references ./${ref}.js but ${dts} does not exist`);
      assert.ok(shipped(dts), `${file} references ./${ref}.js but ${dts} is not in the files allowlist`);
    }
  }
});

// The `[Unreleased]` section of the CHANGELOG, split by its `### ` headings:
// a map of heading -> the lines under it. The release checklist reads the
// breaking section to decide the bump, so an entry marked **Breaking that
// sits under `### Changed` or `### Added` is a semver lie waiting to ship.
const unreleasedSections = () => {
  const changelog = read('CHANGELOG.md');
  const start = changelog.indexOf('\n## [Unreleased]');
  assert.ok(start >= 0, 'CHANGELOG.md must keep an [Unreleased] section');
  const next = changelog.indexOf('\n## [', start + 1);
  const lines = changelog.slice(start, next < 0 ? undefined : next).split('\n');
  const sections = new Map();
  let heading = '';
  for (const line of lines) {
    if (line.startsWith('### ')) heading = line.slice(4).trim();
    else if (heading) sections.get(heading)?.push(line) ?? sections.set(heading, [line]);
  }
  return sections;
};

test('every **Breaking entry of [Unreleased] sits under ### Changed (breaking)', () => {
  const sections = unreleasedSections();
  const breaking = sections.get('Changed (breaking)');
  assert.ok(breaking, '[Unreleased] must keep a "### Changed (breaking)" section (empty is fine)');
  for (const [heading, lines] of sections) {
    if (heading === 'Changed (breaking)') continue;
    const marked = lines.filter((line) => line.includes('**Breaking'));
    assert.deepStrictEqual(
      marked,
      [],
      `"### ${heading}" holds a **Breaking entry; move it to "### Changed (breaking)"`,
    );
  }
  // The migration block belongs to the breaking section: a reader who lands
  // on "what broke" finds "what to do" right under it.
  assert.ok(
    breaking.some((line) => line.startsWith('#### Migrating from ')),
    'the breaking section must carry a "#### Migrating from <version>" block',
  );
});

// The 1.x deprecation notes promised that "2.0 makes it a TypeError"; 2.0
// kept the promise, so the sentence has nothing left to announce. A copy
// of it surviving in the source, the types or the docs would describe a
// fallback that no longer exists.
test('no text still promises what 2.0 already did', () => {
  const { readdirSync, readFileSync } = require('node:fs');
  const STALE = /2\.0 (?:will make|makes) (?:it|all of them)/;
  const walk = (dir, out) => {
    for (const entry of readdirSync(path.join(ROOT, dir), { withFileTypes: true }) ?? []) {
      const relative = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(relative, out);
      else if (/\.(?:js|md|ts)$/.test(entry.name)) out.push(relative);
    }
    return out;
  };
  const files = [...walk('src', []), ...walk('docs/guide', []), ...walk('docs/reference', [])];
  for (const name of readdirSync(ROOT)) if (name.endsWith('.d.ts')) files.push(name);
  const stale = files.filter((file) => STALE.test(readFileSync(path.join(ROOT, file), 'utf8')));
  assert.deepStrictEqual(stale, [], 'these files still promise what 2.0 already did');
});

const NUMBER_WORDS = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine'];
const subpaths = Object.keys(pkg.exports)
  .filter((key) => key !== '.' && key !== './package.json')
  .map((key) => key.slice(1));

// The stability page is where a consumer learns what the semver promise
// covers, llms.txt where a model does; a subpath missing from either is a
// public API nobody was told about.
test('docs/reference/stability.md and llms.txt name every exports subpath', () => {
  const stability = read('docs/reference/stability.md');
  const llms = read('docs/public/llms.txt');
  for (const subpath of subpaths) {
    assert.ok(stability.includes(`\`${subpath}\``), `stability.md must list ${subpath}`);
    assert.ok(llms.includes(`\`${subpath}\``), `llms.txt must list ${subpath}`);
  }
});

// "Four areas are marked @experimental" went stale twice — the brokers and
// then encryption were added under it. The count is derived from the list.
test('the @experimental carve-out counts match their lists', () => {
  const bullets = (text, from, to, label) => {
    const start = text.indexOf(from);
    assert.ok(start >= 0, `${label} must keep its "${from}" section`);
    const end = text.indexOf(to, start + from.length);
    const block = text.slice(start, end < 0 ? undefined : end);
    return block.split('\n').filter((line) => line.startsWith('- ')).length;
  };
  const stability = read('docs/reference/stability.md');
  const count = bullets(stability, '## `@experimental` carve-outs', '\n## ', 'stability.md');
  assert.ok(count > 0 && count < NUMBER_WORDS.length, `an unexpected carve-out count: ${count}`);
  const word = NUMBER_WORDS[count];
  const capitalized = word[0].toUpperCase() + word.slice(1);
  assert.ok(stability.includes(`${capitalized} areas are marked`), `stability.md must say "${capitalized} areas"`);
  const contributing = read('CONTRIBUTING.md');
  const mirrored = bullets(contributing, '## Stability and deprecation', '\nAn `@experimental` API', 'CONTRIBUTING.md');
  assert.strictEqual(mirrored, count, 'CONTRIBUTING.md must list the same carve-outs as stability.md');
  assert.ok(contributing.includes(`with ${word} carve-outs`), `CONTRIBUTING.md must say "${word} carve-outs"`);
});
