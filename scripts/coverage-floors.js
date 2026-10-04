'use strict';

// Per-file coverage floors for the directories that are new in 2.0.
//
// `pnpm test:coverage` checks the GLOBAL thresholds (95 lines, 90 branches,
// 95 functions), and a global threshold is an average: a new adapter at 80%
// branches hides behind three hundred well-covered files. The directories
// below are held per FILE instead — each file in them must stay at or above
// its directory's floor.
//
// Only the new directories: the 1.0 files that sit below these numbers
// (client/proxy.js, adapters/express.js, …) predate the rule, and a floor
// low enough to admit them would hold nothing. The floors start a little
// under what each directory's weakest file measured when they were set
// (the margin is run-to-run noise in timing-dependent branches) and move
// one way: UP, as the weakest file improves — never down to let a change in.
//
// Reads the coverage the last c8 run left in coverage/tmp and reruns
// nothing, so it goes right after test:coverage / test:ci:
//
//   pnpm test:coverage && pnpm test:coverage:floors

const { spawnSync } = require('node:child_process');

// Measured 2026-10-01 (weakest file per directory, lines / branches / functions):
//   broker        96.9 / 82.2 / 86.7   (rpc/client.js)
//   webtransport  96.4 / 90.8 / 88.9   (index.js, socket.js)
//   encryption    97.4 / 89.9 / 85.2   (httpServer.js, http.js, server.js)
//   webrtc        96.7 / 89.2 / 94.4   (host.js, mesh.js)
//   compression   99.0 / 94.7 / 100    (index.js, dictionary.js)
//   deflate       98.8 / 91.0 / 94.1   (inflate.js, deflate.js)
const FLOORS = [
  { include: 'src/broker/**', lines: 95, branches: 80, functions: 85 },
  {
    include: 'src/webtransport/**',
    // The one exception: the adapter over the optional pure-JS QUIC stack is
    // exercised against a fake and, by hand, against the real package
    // (76% branches) — its failure paths are the library's to produce.
    exclude: ['src/webtransport/quico.js'],
    lines: 95,
    branches: 89,
    functions: 87,
  },
  { include: 'src/encryption/**', lines: 96, branches: 88, functions: 84 },
  { include: 'src/webrtc/**', lines: 95, branches: 87, functions: 93 },
  { include: 'src/compression/**', lines: 98, branches: 93, functions: 95 },
  { include: 'src/deflate/**', lines: 97, branches: 89, functions: 92 },
];

const c8 = require.resolve('c8/bin/c8.js');

let failed = false;
for (const { include, exclude = [], lines, branches, functions } of FLOORS) {
  const args = [c8, 'check-coverage', '--per-file', '--all', '--include', include];
  for (const pattern of exclude) args.push('--exclude', pattern);
  // Statements follow lines in this codebase (one statement per line, by
  // the formatter), so they share a floor.
  args.push('--lines', String(lines), '--statements', String(lines));
  args.push('--branches', String(branches), '--functions', String(functions));
  const result = spawnSync(process.execPath, args, { encoding: 'utf8' });
  const ok = result.status === 0;
  console.log(
    `${ok ? 'ok  ' : 'FAIL'} ${include.padEnd(24)} lines ${lines}  branches ${branches}  functions ${functions}`,
  );
  if (!ok) {
    failed = true;
    process.stdout.write(`${result.stdout}${result.stderr}`);
  }
}
if (failed) process.exitCode = 1;
