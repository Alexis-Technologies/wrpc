/**
 * The waiters of a `Semaphore` — the `queue` of a procedure, filled by peers
 * up to its `size`. A waiter leaves from the head when a slot frees, and from
 * anywhere when it gives up: its caller's signal aborts, its deadline or the
 * queue's `timeout` passes.
 *
 * It used to be an array: `indexOf` + `splice` for a waiter that leaves from
 * the middle. A timeout fires for every waiter queued at about the same time,
 * so they leave together, each one scanning and shifting what is left —
 * N² / 2 moves for a queue of N. A `Set` deletes in O(1) and still answers
 * its oldest member first.
 *
 * The rows below are the two spellings side by side, filled to N and emptied
 * the way a timeout storm empties them (in arrival order, each removal by
 * identity), in waiters per second. The last row is the semaphore itself,
 * through its abort path.
 *
 * Run with `node bench/semaphore.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');
const { Semaphore } = require('../src/utils.js');

let sink = 0;

const withArray = (n) => {
  const queue = [];
  const waiters = [];
  for (let i = 0; i < n; i++) {
    const waiter = { id: i };
    waiters.push(waiter);
    queue.push(waiter);
  }
  for (let i = 0; i < n; i++) {
    const index = queue.indexOf(waiters[i]);
    if (index > -1) queue.splice(index, 1);
  }
  sink += queue.length;
};

const withSet = (n) => {
  const queue = new Set();
  const waiters = [];
  for (let i = 0; i < n; i++) {
    const waiter = { id: i };
    waiters.push(waiter);
    queue.add(waiter);
  }
  for (let i = 0; i < n; i++) queue.delete(waiters[i]);
  sink += queue.size;
};

// One signal per call, as a dispatcher hands every call its own — aborted
// in arrival order, the way a burst of disconnects or deadlines empties it.
const storm = async (n) => {
  const semaphore = new Semaphore({ concurrency: 0, size: n });
  const controllers = [];
  const entered = [];
  for (let i = 0; i < n; i++) {
    const controller = new AbortController();
    controllers.push(controller);
    entered.push(semaphore.enter(controller.signal).catch(() => 0));
  }
  for (let i = 0; i < n; i++) controllers[i].abort();
  const settled = await Promise.all(entered);
  sink += settled.length;
};

const main = async () => {
  console.log(`Semaphore benchmark — Node ${process.version}\n`);
  for (const n of [100, 1_000, 10_000]) {
    await bench(`array indexOf + splice — ${n} waiters leave`, () => withArray(n), { opsPerIteration: n });
    await bench(`Set delete — ${n} waiters leave`, () => withSet(n), { opsPerIteration: n });
  }
  await bench('Semaphore — 10,000 queued waiters, each aborted', () => storm(10_000), {
    warmup: 5,
    opsPerIteration: 10_000,
  });
  if (sink < 0) console.log(sink);
};

main();
