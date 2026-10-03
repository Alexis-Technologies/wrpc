'use strict';

// Runs a browser benchmark in a real Chrome. Bundles the browser entry with
// esbuild (a devDependency, as in scripts/size.js), drives the installed
// Google Chrome through playwright-core (devDependency; no browser download —
// `channel: 'chrome'`), and prints what the page measured. Two suites:
//
//   pnpm bench:browser              bench/browser/calls.js — the client's
//                                   call path over an in-page echo
//   pnpm bench:browser transports   bench/browser/transports.js — the client
//                                   over Chrome's own WebSocket, WebTransport
//                                   and WebRTC, against a Node Server
//
//   WRPC_ROOT=/path/to/another/checkout pnpm bench:browser   # a before/after
//
// The transports suite serves its page from http://127.0.0.1 (WebTransport
// needs a secure context; a setContent page is about:blank). Its WebTransport
// row needs the HTTP/3 host the integration tests use — `node
// scripts/wt-cert.js certs` and WRPC_WT=fails — and is skipped without it;
// the WebSocket and WebRTC rows need nothing.
//
// Nothing in CI runs it: a browser benchmark on a laptop is a shape, not a
// gate (docs/guide/performance.md).

const http = require('node:http');
const path = require('node:path');
const fs = require('node:fs');

const esbuild = require('esbuild');
const { chromium } = require('playwright-core');

const HERE = path.resolve(__dirname, '..');
const ROOT = process.env.WRPC_ROOT ? path.resolve(process.env.WRPC_ROOT) : HERE;
const SUITE = process.argv[2] ?? 'calls';

const bundle = async (stdin) => {
  const result = await esbuild.build({
    ...(stdin
      ? { stdin: { contents: stdin, resolveDir: ROOT, sourcefile: 'bench-entry.js' } }
      : { entryPoints: [path.join(ROOT, 'browser.js')] }),
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

const launch = () => chromium.launch({ channel: process.env.WRPC_BROWSER_CHANNEL ?? 'chrome', headless: true });

const forwardErrors = (tab) =>
  tab.on('console', (message) => {
    if (message.type() === 'error') console.error(`[page] ${message.text()}`);
  });

const calls = async () => {
  const [library, page] = [await bundle(), fs.readFileSync(path.join(HERE, 'bench', 'browser', 'calls.js'), 'utf8')];
  const browser = await launch();
  try {
    const tab = await browser.newPage();
    forwardErrors(tab);
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

const transports = async () => {
  const { createWrpcServer } = require('../bench/support/wrpc-echo.js');
  const { api } = require('../bench/support/transport-api.js');
  const real = require('../bench/support/real-stacks.js');
  // One bundle, so the WebRTC barrel registers its transport into the same
  // WrpcClient the page connects with.
  const library = await bundle(
    "module.exports = { ...require('./browser.js'), webrtc: require('./webrtc.browser.js') };",
  );
  const page = fs.readFileSync(path.join(HERE, 'bench', 'browser', 'transports.js'), 'utf8');

  const { server, port } = await createWrpcServer(api);
  const wtSkipped = real.wtSkip();
  const wt = wtSkipped ? null : await real.bootWt(server);
  const files = {
    '/': [
      'text/html',
      '<!doctype html><title>wrpc transports bench</title><script src="/wrpc.js"></script><script src="/page.js"></script>',
    ],
    '/wrpc.js': ['text/javascript', library],
    '/page.js': ['text/javascript', page],
  };
  const site = http.createServer((req, res) => {
    const file = files[req.url];
    if (!file) return void res.writeHead(404).end();
    res.writeHead(200, { 'content-type': file[0] }).end(file[1]);
  });
  await new Promise((resolve) => site.listen(0, '127.0.0.1', resolve));

  const browser = await launch();
  try {
    const tab = await browser.newPage();
    forwardErrors(tab);
    await tab.goto(`http://127.0.0.1:${site.address().port}/`);
    const version = await browser.version();
    console.log(
      `Browser transport benchmark — Chrome ${version}, Node ${process.version}, entry ${path.relative(HERE, ROOT) || '.'}\n`,
    );
    if (wtSkipped) console.log(`wt: skipped — ${wtSkipped}\n`);
    const config = {
      // WRPC_BENCH_KINDS=ws,webrtc narrows the run.
      kinds: (process.env.WRPC_BENCH_KINDS?.split(',') ?? ['ws', 'wt', 'webrtc']).filter((kind) => kind !== 'wt' || wt),
      wsUrl: `ws://127.0.0.1:${port}/`,
      wtUrl: wt?.url,
      wtHash: wt ? JSON.parse(fs.readFileSync(path.join(HERE, 'certs', 'wt-cert.json'), 'utf8')).hash : null,
    };
    const results = await tab.evaluate((given) => globalThis.__wrpcTransports(given), config);
    const header = [
      'transport',
      'first call',
      'small',
      '10 KB',
      'small ×64',
      'stream MiB/s',
      'loaded p50',
      'loaded p99',
    ];
    const rows = [];
    for (const r of results) {
      if (r.error) {
        console.error(`${r.kind}: ${r.error}`);
        continue;
      }
      rows.push([
        r.kind,
        `${r.firstCall} ms`,
        r.small.toLocaleString('en-US'),
        r.large.toLocaleString('en-US'),
        r.pipelined.toLocaleString('en-US'),
        String(r.streamMiBps),
        `${r.loaded.p50} ms`,
        `${r.loaded.p99} ms`,
      ]);
    }
    const widths = header.map((h, i) => Math.max(h.length, ...rows.map((row) => row[i].length)));
    const line = (cells) => cells.map((cell, i) => cell.padStart(widths[i])).join('  ');
    console.log(`  ${line(header)}`);
    for (const row of rows) console.log(`  ${line(row)}`);
    console.log('\n  calls in ops/sec; "loaded" is a small call while 16 MiB uploads keep the connection busy\n');
  } finally {
    await browser.close();
    site.close();
    await wt?.stop();
    await server.close();
  }
};

const SUITES = { calls, transports };

if (!Object.hasOwn(SUITES, SUITE)) {
  console.error(`unknown suite "${SUITE}" — one of: ${Object.keys(SUITES).join(', ')}`);
  process.exitCode = 1;
} else {
  SUITES[SUITE]()
    .then(() => {
      // An HTTP/3 host can hold a native thread past its stop.
      setTimeout(() => process.exit(), 2000).unref();
    })
    .catch((error) => {
      console.error(error);
      process.exitCode = 1;
      setTimeout(() => process.exit(1), 2000).unref();
    });
}
