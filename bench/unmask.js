'use strict';

/**
 * XOR unmask strategy benchmark for the WebSocket receive path.
 *
 * Compares three ways to unmask a client frame payload in place:
 *   (a) naive byte-wise loop — inline copy of the old implementation;
 *   (b) applyMask — the production word-wise XOR from src/websocket/frame.js;
 *   (c) Uint32Array-view variant, included for comparison.
 *
 * Masking is an involution (applying it twice restores the input), so each
 * iteration reuses the same buffer without copies. Iteration counts scale
 * down with payload size so the whole script finishes quickly.
 *
 * Run: node bench/unmask.js (or as part of `pnpm bench`)
 */

const { performance } = require('node:perf_hooks');
const crypto = require('node:crypto');
const { applyMask } = require('../src/websocket/frame.js');

const KIB = 1024;
const MIB = 1024 * 1024;

const SIZES = [
  { label: '64 B', size: 64, iterations: 1_000_000 },
  { label: '1 KiB', size: KIB, iterations: 400_000 },
  { label: '64 KiB', size: 64 * KIB, iterations: 6_000 },
  { label: '1 MiB', size: MIB, iterations: 384 },
];

// (a) Naive byte-wise loop — the pre-optimization implementation.
const naiveMask = (payload, mask) => {
  for (let i = 0; i < payload.length; i++) {
    payload[i] ^= mask[i & 3];
  }
};

// (c) Uint32Array view over the aligned middle, byte-wise head and tail.
// Word composition assumes a little-endian host, like the production code.
const uint32Mask = (payload, mask) => {
  const length = payload.length;
  let i = 0;
  const misalign = payload.byteOffset & 3;
  if (misalign !== 0) {
    const prefix = Math.min(4 - misalign, length);
    for (; i < prefix; i++) payload[i] ^= mask[i & 3];
  }
  const words = (length - i) >> 2;
  if (words > 0) {
    const view = new Uint32Array(payload.buffer, payload.byteOffset + i, words);
    const mask32 =
      (mask[i & 3] | (mask[(i + 1) & 3] << 8) | (mask[(i + 2) & 3] << 16) | (mask[(i + 3) & 3] << 24)) >>> 0;
    for (let w = 0; w < words; w++) view[w] ^= mask32;
    i += words << 2;
  }
  for (; i < length; i++) payload[i] ^= mask[i & 3];
};

const STRATEGIES = [
  { name: 'naive byte-wise', fn: naiveMask },
  { name: 'applyMask (prod)', fn: applyMask },
  { name: 'Uint32Array view', fn: uint32Mask },
];

// Every strategy must produce byte-identical output across lengths (odd
// tails included) and all four byteOffset alignments before being timed.
const verifyStrategies = () => {
  let ok = true;
  const lengths = [0, 1, 2, 3, 4, 5, 7, 8, 63, 64, 65, 1027];
  for (const length of lengths) {
    const mask = crypto.randomBytes(4);
    const data = crypto.randomBytes(length);
    const expected = Buffer.from(data);
    naiveMask(expected, mask);
    for (let offset = 0; offset < 4; offset++) {
      for (const { name, fn } of STRATEGIES) {
        const buffer = Buffer.from(new ArrayBuffer(length + offset), offset, length);
        data.copy(buffer);
        fn(buffer, mask);
        if (!buffer.equals(expected)) {
          ok = false;
          console.error(`FAIL: ${name} mismatch at length ${length}, byteOffset ${offset}`);
        }
      }
    }
  }
  return ok;
};

const bench = (fn, payload, mask, iterations) => {
  const warmup = Math.max(1, iterations >> 3);
  for (let i = 0; i < warmup; i++) fn(payload, mask);
  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn(payload, mask);
  const seconds = (performance.now() - start) / 1000;
  return {
    opsPerSec: iterations / seconds,
    mbPerSec: (payload.length * iterations) / MIB / seconds,
  };
};

const formatNumber = (num) => num.toLocaleString('en-US', { maximumFractionDigits: 2 });

const formatOps = (ops) => {
  if (ops >= 1_000_000) return `${formatNumber(ops / 1_000_000)}M`;
  if (ops >= 1000) return `${formatNumber(ops / 1000)}K`;
  return formatNumber(ops);
};

const main = () => {
  console.log(`XOR unmask benchmark — Node ${process.version}`);
  if (!verifyStrategies()) {
    process.exitCode = 1;
    return;
  }
  console.log();
  for (const { label, size, iterations } of SIZES) {
    const payload = crypto.randomFillSync(Buffer.allocUnsafe(size));
    const mask = crypto.randomBytes(4);
    console.log(`Payload ${label} — ${formatNumber(iterations)} iterations`);
    console.log(`  ${'strategy'.padEnd(18)}${'ops/sec'.padStart(12)}${'MB/s'.padStart(14)}`);
    for (const { name, fn } of STRATEGIES) {
      const { opsPerSec, mbPerSec } = bench(fn, payload, mask, iterations);
      console.log(`  ${name.padEnd(18)}${formatOps(opsPerSec).padStart(12)}${formatNumber(mbPerSec).padStart(14)}`);
    }
    console.log();
  }
};

main();
