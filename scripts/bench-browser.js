'use strict';

// Runs bench/browser/calls.js in a real Chrome: the client-side call path
// measured where it ships. Bundles the browser entry with esbuild (a
// devDependency, as in scripts/size.js), drives the installed Google Chrome
// through playwright-core (devDependency; no browser download — `channel:
// 'chrome'`), and prints ops/sec per scenario.
//
//   pnpm bench:browser
//   WRPC_ROOT=/path/to/another/checkout pnpm bench:browser   # a before/after
//
// Nothing in CI runs it: a browser benchmark on a laptop is a shape, not a
// gate (docs/guide/performance.md).

const path = require('node:path');
const fs = require('node:fs');

const esbuild = require('esbuild');
const { chromium } = require('playwright-core');

const HERE = path.resolve(__dirname, '..');
const ROOT = process.env.WRPC_ROOT ? path.resolve(process.env.WRPC_ROOT) : HERE;
const PAGE_SCRIPT = path.join(HERE, 'bench', 'browser', 'calls.js');

const bundle = async () => {
  const result = await esbuild.build({
    entryPoints: [path.join(ROOT, 'browser.js')],
    bundle: true,
    platform: 'browser',
    conditions: ['browser'],
    format: 'iife',
    globalName: 'wrpc',
    write: false,
    logLevel: 'silent',
  });
  return result.outputFiles[0].text;
};

const main = async () => {
  const [library, page] = [await bundle(), fs.readFileSync(PAGE_SCRIPT, 'utf8')];
  const browser = await chromium.launch({ channel: process.env.WRPC_BROWSER_CHANNEL ?? 'chrome', headless: true });
  try {
    const tab = await browser.newPage();
    tab.on('console', (message) => {
      if (message.type() === 'error') console.error(`[page] ${message.text()}`);
    });
    await tab.setContent('<!doctype html><title>wrpc bench</title>');
    await tab.addScriptTag({ content: library });
    await tab.addScriptTag({ content: page });
    const version = await browser.version();
    console.log(`Browser call-path benchmark — Chrome ${version}, entry ${path.relative(HERE, ROOT) || '.'}\n`);
    const results = await tab.evaluate(() => globalThis.__wrpcBench());
    for (const { name, opsPerSec } of results) {
      console.log(`  ${name.padEnd(46)}${opsPerSec.toLocaleString('en-US').padStart(14)} ops/sec`);
    }
    console.log();
  } finally {
    await browser.close();
  }
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
