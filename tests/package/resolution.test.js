'use strict';

// Every subpath resolves straight into src/ — no root shim, no root d.ts per
// subpath — so what finds a subpath's types is package.json alone: `exports`
// for a resolver that reads it, `typesVersions` for TypeScript under
// `moduleResolution: node10` (the default for `module: commonjs`), which
// never does. This asks the resolvers themselves, as a consumer would meet
// them: TypeScript in each mode against an installed copy of the package (a
// node_modules link to this tree), and Node at runtime by the package's own
// name.

const path = require('node:path');
const fs = require('node:fs');
const { tmpdir } = require('node:os');
const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const ROOT = path.join(__dirname, '..', '..');
const pkg = require('../../package.json');
const ts = require(require.resolve('@tsd/typescript', { paths: [path.dirname(require.resolve('tsd'))] }));

const subpaths = Object.keys(pkg.exports).filter((key) => key !== './package.json');
const specifier = (key) => (key === '.' ? pkg.name : `${pkg.name}/${key.slice(2)}`);
const absolute = (target) => path.join(ROOT, target);

// A consumer project whose node_modules holds the package as a link.
const consumer = () => {
  const dir = fs.mkdtempSync(path.join(tmpdir(), 'wrpc-resolution-'));
  fs.mkdirSync(path.join(dir, 'node_modules', '@alexify'), { recursive: true });
  fs.symlinkSync(ROOT, path.join(dir, 'node_modules', '@alexify', 'wrpc'), 'dir');
  const file = path.join(dir, 'consumer.ts');
  fs.writeFileSync(file, 'export {};\n');
  return { dir, file };
};

const MODES = {
  node10: { module: ts.ModuleKind.CommonJS, moduleResolution: ts.ModuleResolutionKind.Node10 },
  node16: { module: ts.ModuleKind.Node16, moduleResolution: ts.ModuleResolutionKind.Node16 },
  bundler: { module: ts.ModuleKind.ESNext, moduleResolution: ts.ModuleResolutionKind.Bundler },
  'bundler + browser': {
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    customConditions: ['browser'],
  },
};

describe('package resolution', () => {
  it('should resolve the types of every subpath under node10, node16 and bundler, and the browser half under browser', (t) => {
    const { dir, file } = consumer();
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    let checked = 0;
    for (const [mode, options] of Object.entries(MODES)) {
      for (const key of subpaths) {
        const entry = pkg.exports[key];
        const expected = mode === 'bundler + browser' && entry.browser ? entry.browser.types : entry.types;
        const { resolvedModule } = ts.resolveModuleName(specifier(key), file, { ...options, noEmit: true }, ts.sys);
        assert.ok(resolvedModule, `${mode}: ${specifier(key)} does not resolve`);
        assert.strictEqual(
          fs.realpathSync(resolvedModule.resolvedFileName),
          fs.realpathSync(absolute(expected)),
          `${mode}: ${specifier(key)}`,
        );
        checked++;
      }
    }
    assert.strictEqual(checked, subpaths.length * Object.keys(MODES).length);
  });

  it('should resolve every subpath at runtime, by the package name, to its entry under src/', () => {
    for (const key of subpaths) {
      const resolved = require.resolve(specifier(key));
      assert.strictEqual(resolved, absolute(pkg.exports[key].default), specifier(key));
    }
  });
});
