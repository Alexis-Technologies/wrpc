'use strict';

// The examples are what a reader copies first, and nothing ran them: a
// rename in a root entry (`webrtc.js`, `wt.js`, `auth.js` — all three were
// reshaped on the way to 2.0) left an example that throws at its first line,
// found by whoever tried it. They need servers, ports and a browser, so they
// are not EXECUTED here either; two cheap checks catch what a rename breaks:
//
//   - every example script compiles (a syntax error, a stray await);
//   - every name an example destructures from a root entry of this package
//     is one that entry really exports.
//
// `public/` is skipped: it holds the page and the bundle build.js writes.

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const Module = require('node:module');

const ROOT = path.join(__dirname, '..');
const EXAMPLES = path.join(ROOT, 'examples');

const scripts = (dir, out = []) => {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === 'public' || entry.name === 'node_modules') continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) scripts(full, out);
    else if (entry.name.endsWith('.js')) out.push(full);
  }
  return out;
};

const files = scripts(EXAMPLES);

test('examples: there is something to check', () => {
  assert.ok(files.length >= 7, `found ${files.length} example scripts`);
});

test('examples: every script compiles', () => {
  for (const file of files) {
    const source = fs.readFileSync(file, 'utf8');
    assert.doesNotThrow(
      () => new vm.Script(Module.wrap(source), { filename: file }),
      `${path.relative(ROOT, file)} does not compile`,
    );
  }
});

// `const { a, b: c, d = 1 } = require('../../<entry>.js')` and
// `...require('../../<entry>.js')`: the two ways an example reaches the
// package. A relative path that leaves examples/ is a root entry.
const DESTRUCTURED = /const\s*\{([^}]*)\}\s*=\s*require\('(\.\.\/\.\.\/[^']+)'\)/g;
const REQUIRED = /require\('(\.\.\/\.\.\/[^']+)'\)/g;

test('examples: every root entry an example requires exists, and exports the names taken from it', () => {
  let checked = 0;
  for (const file of files) {
    const source = fs.readFileSync(file, 'utf8');
    const label = path.relative(ROOT, file);
    for (const match of source.matchAll(REQUIRED)) {
      const target = path.resolve(path.dirname(file), match[1]);
      assert.ok(fs.existsSync(target), `${label} requires ${match[1]}, which does not exist`);
    }
    for (const match of source.matchAll(DESTRUCTURED)) {
      const target = path.resolve(path.dirname(file), match[2]);
      const entry = require(target);
      for (const part of match[1].split(',')) {
        const name = part.split(/[:=]/)[0].trim();
        if (name === '' || name.startsWith('...')) continue;
        assert.ok(Object.hasOwn(entry, name), `${label} takes \`${name}\` from ${match[2]}, which does not export it`);
        checked++;
      }
    }
  }
  assert.ok(checked >= 8, `only ${checked} imported names were checked — the pattern stopped matching`);
});
