'use strict';

// The examples are what a reader copies first, and nothing ran them: a
// rename in an entry (`webrtc`, `wt`, `auth` — all three were reshaped on
// the way to 2.0) left an example that throws at its first line,
// found by whoever tried it. They need servers, ports and a browser, so they
// are not EXECUTED here either; two cheap checks catch what a rename breaks:
//
//   - every example script compiles (a syntax error, a stray await);
//   - every name an example destructures from an entry of this package is
//     one that entry really exports.
//
// An example reaches the package by its NAME, the way a reader's own code
// does (`require('@alexify/wrpc/wt')`); here that name resolves to this
// tree through the package's exports — a self-reference.
//
// `public/` is skipped: it holds the page and the bundle build.js writes.

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
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

describe('examples', () => {
  it('should find something to check', () => {
    assert.ok(files.length >= 7, `found ${files.length} example scripts`);
  });

  it('should compile every script', () => {
    for (const file of files) {
      const source = fs.readFileSync(file, 'utf8');
      assert.doesNotThrow(
        () => new vm.Script(Module.wrap(source), { filename: file }),
        `${path.relative(ROOT, file)} does not compile`,
      );
    }
  });

  // `const { a, b: c, d = 1 } = require('@alexify/wrpc/<entry>')` and
  // `...require('@alexify/wrpc/<entry>')`: the two ways an example reaches
  // the package. An example may not reach into the tree by a relative path.
  const NAME = String.raw`@alexify\/wrpc(?:\/[\w/]+)?`;
  const DESTRUCTURED = new RegExp(String.raw`const\s*\{([^}]*)\}\s*=\s*require\('(${NAME})'\)`, 'g');
  const REQUIRED = new RegExp(String.raw`require\('(${NAME})'\)`, 'g');
  const RELATIVE = /require\('(\.\.\/[^']+)'\)/g;

  it('should require only entries that exist and that export the names taken from them', () => {
    let checked = 0;
    for (const file of files) {
      const source = fs.readFileSync(file, 'utf8');
      const label = path.relative(ROOT, file);
      for (const match of source.matchAll(RELATIVE)) {
        assert.fail(`${label} requires ${match[1]}; an example reaches the package by its name`);
      }
      for (const match of source.matchAll(REQUIRED)) {
        assert.doesNotThrow(() => require.resolve(match[1]), `${label} requires ${match[1]}, which does not resolve`);
      }
      for (const match of source.matchAll(DESTRUCTURED)) {
        const entry = require(match[2]);
        for (const part of match[1].split(',')) {
          const name = part.split(/[:=]/)[0].trim();
          if (name === '' || name.startsWith('...')) continue;
          assert.ok(
            Object.hasOwn(entry, name),
            `${label} takes \`${name}\` from ${match[2]}, which does not export it`,
          );
          checked++;
        }
      }
    }
    assert.ok(checked >= 8, `only ${checked} imported names were checked — the pattern stopped matching`);
  });
});
