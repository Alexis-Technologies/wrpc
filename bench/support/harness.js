'use strict';

const { performance } = require('node:perf_hooks');

const WARMUP_ITERATIONS = 200;
const MEASURE_MS = 1_000;
const FIRST_CALL_MS = 10_000;

// A bench whose fn awaits something that never settles — a renamed method, an
// event that stopped going on the wire — wedges the whole run with no output
// at all, which is far harder to diagnose than a thrown error. The first
// warmup call is the canary: racing ONLY that one keeps the guard out of the
// measured loop, where a per-iteration timer would allocate and skew the very
// numbers this file exists to report.
async function firstCall(name, fn) {
  let timer;
  const stalled = new Promise((_resolve, reject) => {
    timer = setTimeout(
      () => reject(new Error(`bench "${name}" did not settle within ${FIRST_CALL_MS} ms`)),
      FIRST_CALL_MS,
    );
  });
  try {
    await Promise.race([fn(), stalled]);
  } finally {
    clearTimeout(timer);
  }
}

async function bench(name, fn, options = {}) {
  const warmup = options.warmup ?? WARMUP_ITERATIONS;
  const measureMs = options.measureMs ?? MEASURE_MS;
  // A pipelined fn issues several calls per iteration, so ops and iterations
  // stop being the same number.
  const opsPerIteration = options.opsPerIteration ?? 1;

  if (warmup > 0) await firstCall(name, fn);
  for (let i = 1; i < warmup; i++) await fn();

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
