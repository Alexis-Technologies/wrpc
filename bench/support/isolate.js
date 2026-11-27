'use strict';

// One case per process. A process that times several cases one after the
// other makes the harness's call site megamorphic and leaves the first case's
// garbage and JIT state to the next, so every row after the first is skewed
// (node-performance-profiling, "Run each case in its own process"). A bench
// opts in with `if (!isolate(__filename)) main();`: in the parent that runs
// the file once per case and answers true; in a child it answers false, the
// file runs, and `selected()` — which the harness and the hand-written
// timers call once per case — lets only that child's case through.
//
// Not a *.test.js and not a top-level bench: bench/run-all.js runs bench/*.js.

const { spawnSync } = require('node:child_process');

const NO_SUCH_CASE = 3;
const CASE = process.env.WRPC_BENCH_CASE;
const target = CASE === undefined ? null : Number(CASE);

let calls = 0;
let ran = false;

// Whether the next case of this process is the one to measure.
const selected = () => {
  const index = calls++;
  const isMine = target === null || index === target;
  if (isMine) ran = true;
  return isMine;
};

// The first process of a run, or a run that is not isolated: the one that
// prints a bench's heading.
const isLeader = target === null || target === 0;

// A child asked for a case past the last one says so by its exit code: that
// is how the parent learns it has run them all.
process.on('exit', (code) => {
  if (target !== null && !ran && code === 0) process.exitCode = NO_SUCH_CASE;
});

const isolate = (file) => {
  if (target !== null) return false;
  for (let index = 0; ; index++) {
    const env = { ...process.env, WRPC_BENCH_CASE: String(index) };
    const child = spawnSync(process.execPath, [...process.execArgv, file], { stdio: 'inherit', env });
    if (child.status === NO_SUCH_CASE) return true;
    if (child.status !== 0) {
      process.exitCode = child.status ?? 1;
      return true;
    }
  }
};

module.exports = { isolate, selected, isLeader };
