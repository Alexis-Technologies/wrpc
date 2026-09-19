'use strict';

// zlib's synchronous convenience calls against its threadpool ones, on the
// per-message codecs' own shapes (src/compression/native.js,
// src/compression/dictionary.js): what a hand-off costs on a message that
// takes microseconds, and where it starts paying for itself. Serial async
// is one call at a time — the latency a single message sees; ×16 keeps
// sixteen in flight — the throughput a burst gets from the four threadpool
// threads. Inflate is measured too, because it is the half that never
// earns the hand-off: eight times faster than deflate, so the fixed cost
// dominates it at every size a message can be.

const zlib = require('node:zlib');
const { performance } = require('node:perf_hooks');

const rows = (n) =>
  Buffer.from(
    JSON.stringify({
      type: 'callback',
      id: 'c1',
      result: Array.from({ length: n }, (_, i) => ({
        id: i,
        name: `row-${i}`,
        tags: ['a', 'b'],
        score: i * 1.5,
        ts: '2026-09-19T09:50:45.014Z',
      })),
    }),
  );

const call = (fn, input) =>
  new Promise((resolve, reject) => fn(input, (error, out) => (error ? reject(error) : resolve(out))));

const serial = async (fn, input, iterations) => {
  const started = performance.now();
  for (let i = 0; i < iterations; i++) await call(fn, input);
  return ((performance.now() - started) * 1000) / iterations;
};

const concurrent = (fn, input, iterations, width = 16) =>
  new Promise((resolve) => {
    const started = performance.now();
    let inflight = 0;
    let done = 0;
    const pump = () => {
      while (inflight < width && done + inflight < iterations) {
        inflight++;
        fn(input, () => {
          inflight--;
          done++;
          if (done === iterations) resolve(((performance.now() - started) * 1000) / iterations);
          else pump();
        });
      }
    };
    pump();
  });

const sync = (fn, input, iterations) => {
  const started = performance.now();
  for (let i = 0; i < iterations; i++) fn(input);
  return ((performance.now() - started) * 1000) / iterations;
};

const us = (value) => `${value.toFixed(1).padStart(8)} µs`;

const main = async () => {
  console.log('deflate-raw, µs per message: sync | async one at a time | async ×16 in flight  ·  inflate sync | async');
  for (const n of [3, 30, 300, 3000, 12000]) {
    const input = rows(n);
    const iterations = input.length > 200_000 ? 100 : input.length > 20_000 ? 1_000 : 10_000;
    const deflated = zlib.deflateRawSync(input);
    const line = [
      `${String(input.length).padStart(9)} B`,
      us(sync(zlib.deflateRawSync, input, iterations)),
      us(await serial(zlib.deflateRaw, input, iterations)),
      us(await concurrent(zlib.deflateRaw, input, iterations)),
      ' · ',
      us(sync(zlib.inflateRawSync, deflated, iterations)),
      us(await serial(zlib.inflateRaw, deflated, iterations)),
    ];
    console.log(`  ${line.join('  ')}`);
  }
};

main();
