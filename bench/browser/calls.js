'use strict';

// The page half of the browser benchmark (scripts/bench-browser.js injects
// the browser bundle as `wrpc`, then this file, then evaluates
// `__wrpcBench()`). It measures the CLIENT's per-call cost in a real
// browser — the id, the packet, the pending-call record, the deadline
// bookkeeping, the callback settle — over an in-page echo server on a
// MessageChannel, so no network and no server code is in the number.
//
// Not discovered by bench/run-all.js (top-level files only): run it with
// `pnpm bench:browser`.

/* global wrpc */

const PIPELINES = [1, 64, 1024];
const WARMUP_MS = 300;
const MEASURE_MS = 1000;

// Answers wrpc packets on the port it is handed in the `wrpc:connect`
// message: every call is echoed back as its callback, a ping as a pong.
const fakeWorker = {
  postMessage(message, transfer) {
    if (message?.type !== 'wrpc:connect') return;
    const port = transfer[0];
    port.onmessage = ({ data }) => {
      const packet = JSON.parse(data);
      const answer = (one) => {
        if (one.type === 'call') return { type: 'callback', id: one.id, result: one.args };
        if (one.type === 'ping') return { type: 'pong' };
        return null;
      };
      if (Array.isArray(packet)) {
        port.postMessage(JSON.stringify(packet.map(answer).filter(Boolean)));
      } else {
        const reply = answer(packet);
        if (reply) port.postMessage(JSON.stringify(reply));
      }
    };
  },
};

const runFor = async (fn, ms) => {
  const end = performance.now() + ms;
  let iterations = 0;
  while (performance.now() < end) {
    await fn();
    iterations++;
  }
  return iterations;
};

const measure = async (name, fn, opsPerIteration) => {
  await runFor(fn, WARMUP_MS);
  const started = performance.now();
  const iterations = await runFor(fn, MEASURE_MS);
  const elapsed = performance.now() - started;
  return { name, opsPerSec: Math.round((iterations * opsPerIteration * 1000) / elapsed) };
};

globalThis.__wrpcBench = async () => {
  const client = await wrpc.connect('event://bench', { transport: 'event', worker: fakeWorker, heartbeat: false });
  client.use({ bench: { echo: { access: 'public' } } });
  const echo = client.api.bench.echo;
  const payload = { name: 'Ada' };
  const results = [];
  for (const depth of PIPELINES) {
    const fn =
      depth === 1
        ? () => echo(payload)
        : () => {
            const calls = new Array(depth);
            for (let i = 0; i < depth; i++) calls[i] = echo(payload);
            return Promise.all(calls);
          };
    results.push(await measure(`call echo ×${depth} in flight`, fn, depth));
  }
  // A per-call timeout puts the calls in their own deadline lane.
  results.push(
    await measure(
      'call echo ×64 in flight, per-call timeout',
      () => {
        const calls = new Array(64);
        for (let i = 0; i < 64; i++) calls[i] = echo(payload, { timeout: 5000 });
        return Promise.all(calls);
      },
      64,
    ),
  );
  client.close();
  return results;
};
