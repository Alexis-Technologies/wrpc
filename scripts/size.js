/**
 * Honest bundle-size measurement for the README "Bundle size" table.
 *
 * Bundles the BROWSER variant of each entry the way a consumer's bundler
 * would, then reports:
 *   - raw:      bundled, un-minified
 *   - min:      fully minified (whitespace + identifier mangling + syntax,
 *               comments stripped)
 *   - min+gzip: the number that matters for shipping to browsers
 *
 * Run with `pnpm size`.
 */

const { gzipSync } = require('node:zlib');
const path = require('node:path');
const esbuild = require('esbuild');

const ROOT = path.join(__dirname, '..');

const ENTRIES = [{ label: 'main entry (@alexify/wrpc)', entry: 'browser.js' }];

async function bundle(entry, minify) {
  const result = await esbuild.build({
    entryPoints: [path.join(ROOT, entry)],
    bundle: true,
    minify,
    platform: 'browser',
    conditions: ['browser'],
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
  for (const { label, entry } of ENTRIES) {
    const raw = await bundle(entry, false);
    const min = await bundle(entry, true);
    if (raw.includes('node:crypto') || raw.includes('node:perf_hooks')) {
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
