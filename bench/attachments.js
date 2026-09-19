'use strict';

// Binary attachments (src/attachments.js), priced: `hasBytes` is the walk
// every outbound packet pays once the feature is on (the reason
// `attachments: false` exists), and the frame is what a packet with bytes
// costs against the two alternatives it replaces — JSON's own view of a
// typed array (nine times the bytes, and a plain object on arrival) and
// base64 in a string (a third more, plus the decode).

const { performance } = require('node:perf_hooks');

const { hasBytes, encodeAttachments, decodeAttachments } = require('../src/attachments.js');

const report = (name, count, elapsedMs, extra = '') =>
  console.log(
    `  ${name.padEnd(52)}${Math.round((count / elapsedMs) * 1000)
      .toLocaleString('en-US')
      .padStart(12)}/sec${extra}`,
  );

const time = (label, fn, iterations, extra = '') => {
  let sink = 0;
  const started = performance.now();
  for (let i = 0; i < iterations; i++) sink += fn() ? 1 : 0;
  const elapsed = performance.now() - started;
  const ns = ((elapsed * 1e6) / iterations).toFixed(0);
  report(label, iterations, elapsed, `   ${ns} ns${extra}`);
  return sink;
};

const rows = (n) => ({
  type: 'callback',
  id: 'c1',
  result: Array.from({ length: n }, (_, i) => ({ id: i, name: `row-${i}`, tags: ['a', 'b'], score: i * 1.5 })),
});

const main = () => {
  const small = rows(3); // ~233 B
  const medium = rows(40); // ~3 KB
  const large = rows(400); // ~30 KB
  console.log('hasBytes: the per-packet walk (against JSON.stringify of the same packet)');
  time('233 B callback, walk', () => hasBytes(small), 500_000);
  time('233 B callback, JSON.stringify', () => JSON.stringify(small).length > 0, 500_000);
  time('3 KB callback, walk', () => hasBytes(medium), 100_000);
  time('3 KB callback, JSON.stringify', () => JSON.stringify(medium).length > 0, 100_000);
  time('30 KB callback, walk', () => hasBytes(large), 10_000);
  time('30 KB callback, JSON.stringify', () => JSON.stringify(large).length > 0, 10_000);

  console.log('a packet with a 1 KB and a 64 KB attachment: the frame against the alternatives');
  for (const size of [1024, 65536]) {
    const body = new Uint8Array(size).map((_, i) => i & 255);
    const packet = { type: 'call', id: 'c1', method: 'files/put', args: { name: 'a.bin', body } };
    const frame = encodeAttachments(packet);
    const jsonSize = JSON.stringify(packet).length;
    const base64Size = JSON.stringify({
      ...packet,
      args: { ...packet.args, body: Buffer.from(body).toString('base64') },
    }).length;
    console.log(`  ${size} B attachment: frame ${frame.length} B, JSON ${jsonSize} B, base64 ${base64Size} B`);
    time(`${size} B: encodeAttachments`, () => encodeAttachments(packet).length > 0, size > 4096 ? 5_000 : 100_000);
    time(
      `${size} B: decodeAttachments (copies the bytes)`,
      () => decodeAttachments(frame) !== null,
      size > 4096 ? 5_000 : 100_000,
    );
    time(
      `${size} B: JSON.stringify (what it used to do, wrongly)`,
      () => JSON.stringify(packet).length > 0,
      size > 4096 ? 500 : 20_000,
    );
  }
};

main();
