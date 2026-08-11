'use strict';

const { performance } = require('node:perf_hooks');

const WARMUP_ITERATIONS = 200;
const MEASURE_MS = 1_000;

async function bench(name, fn, options = {}) {
  const warmup = options.warmup ?? WARMUP_ITERATIONS;
  const measureMs = options.measureMs ?? MEASURE_MS;
  // A pipelined fn issues several calls per iteration, so ops and iterations
  // stop being the same number.
  const opsPerIteration = options.opsPerIteration ?? 1;

  for (let i = 0; i < warmup; i++) await fn();

  let iterations = 0;
  const start = performance.now();
  while (performance.now() - start < measureMs) {
    await fn();
    iterations += 1;
  }
  const elapsed = performance.now() - start;
  const opsPerSec = Math.round(((iterations * opsPerIteration) / elapsed) * 1000);
  console.log(`${name.padEnd(60)} ${opsPerSec.toLocaleString('en-US').padStart(12)} ops/sec`);
  return { name, opsPerSec };
}

module.exports = { bench, WARMUP_ITERATIONS, MEASURE_MS };
