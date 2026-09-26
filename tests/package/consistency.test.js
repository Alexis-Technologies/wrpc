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

test('runtime barrel exports are all declared in the hand-written types', () => {
  const declared = (files) => {
    const names = new Set();
    for (const file of files) {
      const text = read(file);
      for (const match of text.matchAll(/^export (?:declare )?(?:abstract )?(?:class|function|const|let)\s+(\w+)/gm)) {
        names.add(match[1]);
      }
    }
    return names;
  };
  const cases = [
    ['index.js', declared(['index.d.ts', 'rpc.d.ts', 'client.d.ts'])],
    ['browser.js', declared(['browser.d.ts', 'client.d.ts'])],
    ['webrtc.js', declared(['webrtc.d.ts', 'webrtc.browser.d.ts', 'rpc.d.ts'])],
    ['webrtc.browser.js', declared(['webrtc.browser.d.ts', 'rpc.d.ts'])],
    ['wt.js', declared(['wt.d.ts', 'rpc.d.ts'])],
    ['broker.js', declared(['broker.d.ts'])],
  ];
  for (const [barrel, names] of cases) {
    const runtime = Object.keys(require(path.join(ROOT, barrel)));
    for (const name of runtime) {
      assert.ok(names.has(name), `${barrel} exports '${name}' but the d.ts pair never declares it`);
    }
  }
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
