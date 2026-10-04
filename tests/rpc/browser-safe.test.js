'use strict';

// The server-side dispatcher must bundle for a browser with NO externals:
// the peer-to-peer WebRTC transport runs it inside a page. `selfContained`
// in scripts/size.js guards the published entries the same way; this pins
// the internal module before it has an entry of its own. The main browser
// entry is pinned by tests/index.browser.test.js and the size budget.

const { test } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');

const esbuild = require('esbuild');

const ROOT = path.join(__dirname, '..', '..');

const bundles = async (entry) => {
  const result = await esbuild.build({
    entryPoints: [path.join(ROOT, entry)],
    bundle: true,
    platform: 'browser',
    conditions: ['browser'],
    write: false,
    logLevel: 'silent',
    // Any bare or node: specifier fails resolution instead of being marked
    // external — the same trap scripts/size.js sets for browser entries.
    plugins: [
      {
        name: 'self-contained',
        setup(build) {
          build.onResolve({ filter: /^[^./]/ }, (args) => ({
            errors: [{ text: `${entry} must not import '${args.path}'` }],
          }));
        },
      },
    ],
  });
  return result.outputFiles[0].text;
};

test('browser-safe: the dispatcher bundles without node built-ins', async () => {
  const text = await bundles('src/rpc/dispatcher.js');
  assert.ok(text.length > 0);
});

test('browser-safe: the per-connection Client bundles without node built-ins', async () => {
  const text = await bundles('src/rpc/client.js');
  assert.ok(text.length > 0);
});
