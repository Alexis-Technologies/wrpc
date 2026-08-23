'use strict';

// wrpc has no build step of its own and ships CommonJS; a browser page still
// needs a bundler to consume it (docs/guide/browser.md), so this is that
// bundler for the example only — esbuild is already a root devDependency.
const path = require('node:path');
const esbuild = require(require.resolve('esbuild', { paths: [path.join(__dirname, '..', '..')] }));

esbuild.buildSync({
  entryPoints: [path.join(__dirname, '..', '..', 'browser.js')],
  bundle: true,
  format: 'esm',
  platform: 'browser',
  outfile: path.join(__dirname, 'public', 'wrpc.browser.js'),
});

console.log('built public/wrpc.browser.js');
