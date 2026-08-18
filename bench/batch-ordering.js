/**
 * `ServerHttpTransport#ordered` — the HTTP batch reordering step.
 *
 * A batch frame gets ONE response carrying every answer, emitted in the order
 * the packets were sent so a caller can zip requests to responses positionally.
 * Answers arrive in completion order, so they have to be put back.
 *
 * The obvious complaint about the original is that it is O(n^2): a findIndex
 * scan per batch slot. The obvious fix — index the answers by id — is a
 * pessimization at the sizes that actually occur, because building the Map
 * costs more than the quadratic it removes until n gets large. This bench is
 * what sets the threshold in src/transport.js, so re-run it before changing it.
 *
 * Run with `node bench/batch-ordering.js` or `pnpm bench`.
 */

'use strict';

const SIZES = [4, 8, 16, 24, 32, 64, 128];
const MEASURE_MS = 300;

const scan = (batch, collected) => {
  const pending = collected.slice();
  const answers = [];
  for (const id of batch) {
    const index = pending.findIndex((packet) => packet.id === id);
    if (index < 0) continue;
    answers.push(pending[index]);
    pending.splice(index, 1);
  }
  for (let i = 0; i < pending.length; i++) answers.push(pending[i]);
  return answers;
};

const indexed = (batch, collected) => {
  // Buckets, not single packets: two answers may legitimately share an id.
  const byId = new Map();
  for (let i = 0; i < collected.length; i++) {
    const packet = collected[i];
    const bucket = byId.get(packet.id);
    if (bucket === undefined) byId.set(packet.id, [i]);
    else bucket.push(i);
  }
  const answers = [];
  const taken = new Uint8Array(collected.length);
  for (const id of batch) {
    const bucket = byId.get(id);
    if (bucket === undefined || bucket.length === 0) continue;
    const index = bucket.shift();
    taken[index] = 1;
    answers.push(collected[index]);
  }
  // Leftovers in COLLECTION order, matching the scan's trailing push.
  for (let i = 0; i < collected.length; i++) if (!taken[i]) answers.push(collected[i]);
  return answers;
};

const fixture = (n) => {
  const batch = new Array(n);
  for (let i = 0; i < n; i++) batch[i] = `id-${i}`;
  // Completion order is not send order: reverse is the worst realistic case.
  const collected = new Array(n);
  for (let i = 0; i < n; i++) collected[i] = { id: `id-${n - 1 - i}`, type: 'callback', result: i };
  return { batch, collected };
};

const measure = (fn, batch, collected) => {
  for (let i = 0; i < 2000; i++) fn(batch, collected);
  let iterations = 0;
  const start = process.hrtime.bigint();
  const deadline = start + BigInt(MEASURE_MS) * 1_000_000n;
  do {
    for (let i = 0; i < 200; i++) fn(batch, collected);
    iterations += 200;
  } while (process.hrtime.bigint() < deadline);
  const ns = Number(process.hrtime.bigint() - start);
  return iterations / (ns / 1e9);
};

console.log(`Batch ordering benchmark — Node ${process.version}\n`);
console.log(
  `  ${'batch size'.padEnd(12)}${'linear scan'.padStart(16)}${'id-indexed'.padStart(16)}${'winner'.padStart(14)}`,
);

for (const n of SIZES) {
  const { batch, collected } = fixture(n);
  const a = JSON.stringify(scan(batch, collected));
  const b = JSON.stringify(indexed(batch, collected));
  if (a !== b) throw new Error(`ordering mismatch at n=${n}`);
  const scanRate = measure(scan, batch, collected);
  const indexRate = measure(indexed, batch, collected);
  const winner =
    indexRate > scanRate
      ? `indexed ${(indexRate / scanRate).toFixed(2)}x`
      : `scan ${(scanRate / indexRate).toFixed(2)}x`;
  console.log(
    `  ${String(n).padEnd(12)}${Math.round(scanRate).toLocaleString('en-US').padStart(13)}/s` +
      `${Math.round(indexRate).toLocaleString('en-US').padStart(13)}/s${winner.padStart(14)}`,
  );
}
