/**
 * Zero-dependency ops/sec benchmark harness for wrpc hot paths.
 *
 * Run: pnpm bench (or: node bench/bench.js)
 *
 * TODO: replace the placeholder scenario below with real client/server
 * round-trip benchmarks once the wRPC protocol is implemented, following
 * the shape of @alexify/kerberos's bench/bench.js.
 */
const { performance } = require('node:perf_hooks');
const wrpc = require('../src/index.js');

const WARMUP_ITERATIONS = 2_000;
const MEASURE_MS = 1_000;

async function bench(name, fn) {
  for (let i = 0; i < WARMUP_ITERATIONS; i++) await fn();

  let iterations = 0;
  const start = performance.now();
  while (performance.now() - start < MEASURE_MS) {
    await fn();
    iterations += 1;
  }
  const elapsed = performance.now() - start;
  const opsPerSec = Math.round((iterations / elapsed) * 1000);
  console.log(`${name.padEnd(52)} ${opsPerSec.toLocaleString('en-US').padStart(12)} ops/sec`);
  return { name, opsPerSec };
}

async function main() {
  console.log(`Node ${process.version} | ${new Date().toISOString()}\n`);
  const results = [];

  results.push(await bench('placeholder — module load', () => Promise.resolve(wrpc)));

  return results;
}

main();
