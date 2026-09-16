'use strict';

/**
 * WebTransport stream framing benchmark (src/webtransport/framing.js).
 *
 * Every wrpc packet and every stream chunk on a WebTransport session goes
 * through frame()/frameText() on the way out and StreamParser on the way
 * in, so the two choices the file makes are measured here rather than
 * assumed:
 *
 *   frame, text:   (a) TextEncoder.encode() then copy into an exact frame
 *                  (b) encodeInto() straight into a worst-case-sized frame — production
 *                      under INLINE_TEXT (one allocation, up to 2/3 unused)
 *   parse:         (c) one read per message — the packet path
 *                  (d) a message split over 16 KiB reads — the chunk path
 *                      (head index instead of shift(): what keeps it linear)
 *
 * Run: node bench/wt-framing.js (or as part of `pnpm bench`)
 */

const { performance } = require('node:perf_hooks');
const crypto = require('node:crypto');

const {
  frame,
  frameText,
  StreamParser,
  KIND_TEXT,
  KIND_BINARY,
  INLINE_TEXT,
} = require('../src/webtransport/framing.js');

const KIB = 1024;
const MIB = 1024 * 1024;

const SIZES = [
  { label: '64 B', size: 64, iterations: 400_000 },
  { label: '1 KiB', size: KIB, iterations: 200_000 },
  { label: '16 KiB', size: 16 * KIB, iterations: 20_000 },
  { label: '1 MiB', size: MIB, iterations: 256 },
];

const TEXT_ENCODER = new TextEncoder();

// (a) encode-then-copy — the alternative production rejected under INLINE_TEXT.
const frameTextViaBytes = (text) => frame(KIND_TEXT, TEXT_ENCODER.encode(text));

let sunk = 0;
const sink = (bytes) => {
  sunk += bytes.length;
};

const bench = (fn, iterations) => {
  const warmup = Math.max(1, iterations >> 3);
  for (let i = 0; i < warmup; i++) fn();
  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn();
  return iterations / ((performance.now() - start) / 1000);
};

const formatNumber = (num) => num.toLocaleString('en-US', { maximumFractionDigits: 2 });
const formatOps = (ops) => {
  if (ops >= 1_000_000) return `${formatNumber(ops / 1_000_000)}M`;
  if (ops >= 1000) return `${formatNumber(ops / 1000)}K`;
  return formatNumber(ops);
};

const row = (name, ops) => console.log(`  ${name.padEnd(48)}${formatOps(ops).padStart(12)} ops/sec`);

// Every strategy must round-trip byte-identically before being timed.
const verify = () => {
  for (const size of [0, 1, 4095, 4096, 4097, 70_000]) {
    const text = crypto.randomBytes(size).toString('base64').slice(0, size);
    let out = null;
    const parser = new StreamParser({ onMessage: (_kind, data) => (out = data) });
    for (const framed of [frameText(text), frameTextViaBytes(text)]) {
      out = null;
      parser.push(framed);
      if (out !== text) throw new Error(`text round-trip failed at ${size}`);
    }
    const bytes = crypto.randomBytes(size);
    out = null;
    const framed = frame(KIND_BINARY, bytes);
    for (let offset = 0; offset < framed.length; offset += 7) parser.push(framed.subarray(offset, offset + 7));
    if (out === null || Buffer.compare(Buffer.from(out), bytes) !== 0) {
      throw new Error(`bytes round-trip failed at ${size}`);
    }
  }
};

const main = () => {
  console.log(`WebTransport stream framing benchmark — Node ${process.version} (INLINE_TEXT ${INLINE_TEXT})`);
  verify();
  for (const { label, size, iterations } of SIZES) {
    const text = crypto
      .randomBytes(Math.ceil(size / 2))
      .toString('hex')
      .slice(0, size);
    const bytes = crypto.randomBytes(size);
    console.log(`\n-- ${label} (${formatNumber(iterations)} iterations)`);
    row(
      '(a) text: encode() then copy, exact frame',
      bench(() => sink(frameTextViaBytes(text)), iterations),
    );
    row(
      `(b) text: encodeInto() worst-case frame${size <= INLINE_TEXT ? ' [prod]' : ''}`,
      bench(() => {
        const out = new Uint8Array(5 + text.length * 3);
        const { written } = TEXT_ENCODER.encodeInto(text, out.subarray(5));
        sink(out.subarray(0, 5 + written));
      }, iterations),
    );
    const parser = new StreamParser({ onMessage: sink });
    const whole = frame(KIND_BINARY, bytes);
    row(
      '(c) parse: one read per message',
      bench(() => parser.push(whole), iterations),
    );
    const reads = [];
    for (let offset = 0; offset < whole.length; offset += 16 * KIB) {
      reads.push(whole.subarray(offset, offset + 16 * KIB));
    }
    row(
      `(d) parse: split over 16 KiB reads (${reads.length})`,
      bench(() => {
        for (let i = 0; i < reads.length; i++) parser.push(reads[i]);
      }, iterations),
    );
  }
  if (sunk < 0) console.log(sunk); // keep the sink observable
};

main();
