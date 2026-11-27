'use strict';

const { performance } = require('node:perf_hooks');

const { selected } = require('./isolate.js');

const WARMUP_ITERATIONS = 200;
const MEASURE_MS = 1_000;
const FIRST_CALL_MS = 10_000;
// The measure time is cut into windows, and a row reports their median and
// spread: one window's mean hid a GC pause or a deopt in one number.
const WINDOWS = 5;

// What a case answers is kept here, so V8 cannot drop work whose result
// nobody reads; read at exit, printed never.
let sink = 0;
process.on('exit', () => {
  if (sink === -1) console.log(sink);
});

// A synchronous case is called, not awaited: awaiting a plain value costs a
// promise and a microtask per iteration, which a case measured in
// nanoseconds then reports as its own. A promise is awaited as it is — a
// `.then` on it would add one more of each.
const isThenable = (value) => value !== null && typeof value === 'object' && typeof value.then === 'function';

const keep = (value) => {
  sink += value === undefined ? 0 : 1;
};

const median = (values) => {
  const sorted = values.toSorted((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
};

// A bench whose fn awaits something that never settles — a renamed method, an
// event that stopped going on the wire — wedges the whole run with no output
// at all, which is far harder to diagnose than a thrown error. The first
// warmup call is the canary: racing ONLY that one keeps the guard out of the
// measured loop, where a per-iteration timer would allocate and skew the very
// numbers this file exists to report.
const firstCall = async (name, fn) => {
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
};

const bench = async (name, fn, options = {}) => {
  // Under isolate(): only the case this process was started for.
  if (!selected()) return { name, opsPerSec: 0, skipped: true };
  const warmup = options.warmup ?? WARMUP_ITERATIONS;
  const measureMs = options.measureMs ?? MEASURE_MS;
  // A pipelined fn issues several calls per iteration, so ops and iterations
  // stop being the same number.
  const opsPerIteration = options.opsPerIteration ?? 1;

  if (warmup > 0) await firstCall(name, fn);
  for (let i = 1; i < warmup; i++) {
    const result = fn();
    keep(isThenable(result) ? await result : result);
  }

  const rates = [];
  const windowMs = measureMs / WINDOWS;
  for (let window = 0; window < WINDOWS; window++) {
    let iterations = 0;
    const start = performance.now();
    while (performance.now() - start < windowMs) {
      const result = fn();
      keep(isThenable(result) ? await result : result);
      iterations += 1;
    }
    const elapsed = performance.now() - start;
    rates.push(((iterations * opsPerIteration) / elapsed) * 1000);
  }
  const opsPerSec = Math.round(median(rates));
  const spread = ((Math.max(...rates) - Math.min(...rates)) / opsPerSec) * 50;
  const shown = `${opsPerSec.toLocaleString('en-US').padStart(12)} ops/sec ±${spread.toFixed(1)}%`;
  console.log(`${name.padEnd(60)} ${shown}`);
  return { name, opsPerSec };
};

module.exports = { bench, WARMUP_ITERATIONS, MEASURE_MS };
