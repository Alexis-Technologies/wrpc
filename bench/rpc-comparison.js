/**
 * Compares wrpc's full RPC round-trip against two kinds of neighbour:
 *
 *  - RAW TRANSPORTS (ws, uWebSockets.js, @fastify/websocket, fastify-uws)
 *    driving an equivalent minimal "echo RPC" — the same {id, method, args}
 *    envelope correlated by id, so those numbers isolate transport overhead
 *    rather than protocol-design differences. They are a floor, not a rival:
 *    none of them implements sessions, rooms, streams or subscriptions.
 *  - RPC FRAMEWORKS (socket.io, tRPC over wsLink) doing the job wrpc does,
 *    envelope and bookkeeping included.
 *
 * See bench/support/rpc-stacks.js. Each measurement runs twice: one call at
 * a time (latency) and 64 in flight (throughput) — a stack with a fixed
 * per-call delay reads very differently under the two.
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

const STACK_ORDER = ['wrpc', 'ws', 'uws', 'fastify-websocket', 'fastify-uws', 'socket.io', 'trpc-ws'];
// Matched by suffix against the names rpc-stack-worker.js prints. The
// pipelined row keeps its own suffix (`… ×64`), so the match stays unambiguous.
const MEASUREMENTS = ['small payload', '10KB payload', 'small payload ×64'];
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
  for (const size of MEASUREMENTS) {
    const baseline = byStack.get('wrpc').find((r) => r.name.endsWith(size)).opsPerSec;
    console.log(`  ${size}:`);
    for (const key of STACK_ORDER) {
      const result = byStack.get(key).find((r) => r.name.endsWith(size));
      const ratio = (result.opsPerSec / baseline).toFixed(2);
      const label = result.name.slice(0, result.name.indexOf(' — '));
      console.log(`    ${label.padEnd(44)} ${String(result.opsPerSec).padStart(10)} ops/sec  (${ratio}x)`);
    }
    console.log();
  }
}

main();
