/**
 * Compares wrpc's full RPC round-trip against an equivalent minimal
 * "echo RPC" (same {id, method, args} envelope, correlated by id) built
 * directly on top of ws, uWebSockets.js, @fastify/websocket, and
 * fastify-uws — so the numbers isolate transport overhead rather than
 * protocol-design differences. See bench/support/rpc-stacks.js.
 *
 * Each stack runs in its own child process (spawned here) because mixing
 * raw uWebSockets.js with fastify-uws's bundled copy of it in one process
 * segfaults on exit.
 *
 * These libraries are devDependencies used only by this benchmark; wrpc
 * itself stays zero-dependency (see CLAUDE.md).
 *
 * Run: pnpm bench (or: node bench/rpc-comparison.js)
 */
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const STACK_ORDER = ['wrpc', 'ws', 'uws', 'fastify-websocket', 'fastify-uws'];
const WORKER = path.join(__dirname, 'support', 'rpc-stack-worker.js');

function runStack(key) {
  const result = spawnSync(process.execPath, [WORKER, key], { encoding: 'utf8' });
  if (result.status !== 0) {
    console.error(result.stdout);
    console.error(result.stderr);
    throw new Error(`rpc-comparison stack "${key}" exited with status ${result.status}`);
  }
  const lines = result.stdout.trim().split('\n');
  const resultLine = lines.find((line) => line.startsWith('RESULT_JSON:'));
  for (const line of lines) {
    if (line !== resultLine) console.log(line);
  }
  return JSON.parse(resultLine.slice('RESULT_JSON:'.length));
}

function main() {
  console.log(`Node ${process.version} | ${new Date().toISOString()}\n`);

  const byStack = new Map();
  for (const key of STACK_ORDER) byStack.set(key, runStack(key));

  console.log('\nSummary (ops/sec, relative to wrpc):\n');
  for (const size of ['small payload', '10KB payload']) {
    const baseline = byStack.get('wrpc').find((r) => r.name.endsWith(size)).opsPerSec;
    console.log(`  ${size}:`);
    for (const key of STACK_ORDER) {
      const result = byStack.get(key).find((r) => r.name.endsWith(size));
      const ratio = (result.opsPerSec / baseline).toFixed(2);
      const label = result.name.slice(0, result.name.indexOf(' — '));
      console.log(`    ${label.padEnd(38)} ${String(result.opsPerSec).padStart(10)} ops/sec  (${ratio}x)`);
    }
    console.log();
  }
}

main();
