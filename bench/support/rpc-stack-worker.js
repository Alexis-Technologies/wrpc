'use strict';

/**
 * Runs a single rpc-comparison stack (see rpc-stacks.js) in its own process
 * and prints its results as a `RESULT_JSON:` line for the orchestrator
 * (rpc-comparison.js) to parse. Isolation is required, not just tidy: mixing
 * raw uWebSockets.js with fastify-uws (which bundles a different build of
 * the same native addon) in one process segfaults on exit.
 */
const { bench } = require('./harness.js');
const stacks = require('./rpc-stacks.js');

const smallPayload = { name: 'Ada' };
const largePayload = { text: 'x'.repeat(10_000) };

// One call at a time measures LATENCY: a stack with a fixed per-call delay
// looks catastrophic here however cheap its per-call work is. PIPELINE calls
// in flight at once measures THROUGHPUT, where that delay overlaps away. Both
// are reported because a stack can be fast at one and slow at the other.
const PIPELINE = 64;

const pipelined = (call, payload) => () => Promise.all(Array.from({ length: PIPELINE }, () => call(payload)));

async function main() {
  const key = process.argv[2];
  const stack = stacks[key];
  if (!stack) {
    console.error(`Unknown rpc-comparison stack: ${key}`);
    process.exitCode = 1;
    return;
  }

  const handle = await stack.start();
  const results = [];
  results.push(await bench(`${stack.label} — small payload`, () => handle.call(smallPayload)));
  results.push(await bench(`${stack.label} — 10KB payload`, () => handle.call(largePayload)));
  results.push(
    await bench(`${stack.label} — small payload ×${PIPELINE}`, pipelined(handle.call, smallPayload), {
      warmup: 4,
      opsPerIteration: PIPELINE,
    }),
  );
  await handle.stop();

  console.log(`RESULT_JSON:${JSON.stringify(results)}`);
}

main();
