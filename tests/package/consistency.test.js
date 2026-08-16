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
