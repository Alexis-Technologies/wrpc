/**
 * Honest bundle-size measurement for the README "Bundle size" table.
 *
 * Every entry from package.json#exports is bundled the way a consumer's
 * bundler would (browser condition for the main entry, plain Node
 * resolution for the server-only subpaths), then reports:
 *   - raw:      bundled, un-minified
 *   - min:      fully minified (whitespace + identifier mangling + syntax,
 *               comments stripped)
 *   - min+gzip: the number that matters for shipping to browsers
 *
 * The Node-only subpaths (ws/engine/uws/fastify/express) never ship to a
 * browser — they are reported for visibility into how much each one pulls
 * in, not because their gzip size is a real shipping cost.
 *
 * Run with `pnpm size`.
 */

const { gzipSync } = require('node:zlib');
const path = require('node:path');
const esbuild = require('esbuild');

const ROOT = path.join(__dirname, '..');

// One row per package.json#exports entry. `platform: 'browser'` is the only
// one that actually ships to a browser bundle (via the `browser` condition);
// everything else is Node-only and bundled with `platform: 'node'` so
// `require('node:...')` stays a real require instead of failing to resolve.
const ENTRIES = [
  { label: 'main entry — browser (@alexify/wrpc)', entry: 'browser.js', platform: 'browser' },
  { label: 'main entry — node (@alexify/wrpc)', entry: 'index.js', platform: 'node' },
  { label: 'websocket engine (@alexify/wrpc/ws)', entry: 'ws.js', platform: 'node' },
  { label: 'engine port (@alexify/wrpc/engine)', entry: 'engine.js', platform: 'node' },
  { label: 'uWebSockets.js adapter (@alexify/wrpc/uws)', entry: 'uws.js', platform: 'node' },
  { label: 'fastify adapter (@alexify/wrpc/fastify)', entry: 'fastify.js', platform: 'node' },
  { label: 'express adapter (@alexify/wrpc/express)', entry: 'express.js', platform: 'node' },
];

async function bundle(entry, platform, minify) {
  const result = await esbuild.build({
    entryPoints: [path.join(ROOT, entry)],
    bundle: true,
    minify,
    platform,
    conditions: platform === 'browser' ? ['browser'] : [],
    write: false,
    logLevel: 'silent',
  });
  return Buffer.from(result.outputFiles[0].contents);
}

function kb(bytes) {
  return `${(bytes / 1024).toFixed(1)} KB`;
}

async function main() {
  const rows = [];
  for (const { label, entry, platform } of ENTRIES) {
    const raw = await bundle(entry, platform, false);
    const min = await bundle(entry, platform, true);
    // Only the browser bundle must be builtin-free: the Node subpaths are
    // expected to pull in node:http et al., that is the whole point of them.
    if (platform === 'browser' && (raw.includes('node:crypto') || raw.includes('node:perf_hooks'))) {
      throw new Error(`${entry}: browser bundle unexpectedly contains Node builtins`);
    }
    rows.push({
      label,
      raw: kb(raw.length),
      min: kb(min.length),
      gzip: kb(gzipSync(min, { level: 9 }).length),
    });
  }

  console.log('| Entry | raw | min | min+gzip |');
  console.log('| ----- | ---:| ---:| --------:|');
  for (const row of rows) {
    console.log(`| ${row.label} | ${row.raw} | ${row.min} | ${row.gzip} |`);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
