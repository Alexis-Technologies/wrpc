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
    ['index.js', declared(['index.d.ts', 'client.d.ts'])],
    ['browser.js', declared(['browser.d.ts', 'client.d.ts'])],
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
