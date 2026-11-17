'use strict';

// Bundles the browser client for the page — the base entry, which already
// carries the `wt` transport. Same shape as examples/chat/build.js.

const path = require('node:path');
const esbuild = require('esbuild');

esbuild
  .build({
    // The package's own name, resolved through its exports' `browser`
    // condition — what a page of your own would bundle.
    entryPoints: ['@alexify/wrpc'],
    absWorkingDir: __dirname,
    bundle: true,
    format: 'iife',
    globalName: 'wrpc',
    platform: 'browser',
    minify: true,
    outfile: path.join(__dirname, 'public/wrpc.browser.js'),
    logLevel: 'info',
  })
  .catch(() => process.exit(1));
