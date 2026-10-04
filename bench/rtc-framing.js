'use strict';

/**
 * Data-channel framing benchmark (src/webrtc/framing.js).
 *
 * Every wrpc packet and every stream chunk on a WebRTC link goes through
 * FrameEncoder on the way out and FrameDecoder on the way in, so the two
 * choices the file makes are measured here rather than assumed:
 *
 *   encode, bytes:  (a) a fresh Uint8Array per fragment
 *                   (b) one reused scratch buffer per encoder — production
 *                       (the sink must copy synchronously, as a channel's
 *                       send() does; the 3x is the allocation, not the copy)
 *   encode, text:   (c) TextEncoder.encode() then copy into a fresh frame
 *                   (d) encodeInto() straight into the scratch frame — production
 *   decode:         (e) subarray over the received buffer — production
 *                   (f) slice (a defensive copy)
 *
 * Run: node bench/rtc-framing.js (or as part of `pnpm bench`)
 */

const { performance } = require('node:perf_hooks');
const crypto = require('node:crypto');

const {
  FrameEncoder,
  FrameDecoder,
  KIND_BINARY,
  KIND_TEXT,
  FLAG_FIN,
  HEADER_BYTES,
} = require('../src/webrtc/framing.js');

const KIB = 1024;
const MIB = 1024 * 1024;

const SIZES = [
  { label: '64 B', size: 64, iterations: 400_000 },
  { label: '1 KiB', size: KIB, iterations: 200_000 },
  { label: '64 KiB', size: 64 * KIB, iterations: 4_000 },
  { label: '1 MiB', size: MIB, iterations: 256 },
];
const LIMITS = [16 * KIB, 256 * KIB];

const TEXT_ENCODER = new TextEncoder();

// (a) a fresh frame per fragment — the alternative production rejected.
class FreshEncoder {
  constructor(maxMessageSize) {
    this.room = maxMessageSize - HEADER_BYTES;
  }
  encode(kind, bytes, sink) {
    const total = bytes.length;
    const room = this.room;
    let offset = 0;
    let count = 0;
    do {
      const size = total - offset < room ? total - offset : room;
      const frame = new Uint8Array(size + HEADER_BYTES);
      frame[0] = offset + size === total ? kind | FLAG_FIN : kind;
      frame.set(bytes.subarray(offset, offset + size), HEADER_BYTES);
      sink(frame);
      offset += size;
      count++;
    } while (offset < total);
    return count;
  }
}

// (c) encode-then-copy text into fresh frames — the alternative production rejected.
const encodeTextViaBytes = (encoder, text, sink) => encoder.encode(KIND_TEXT, TEXT_ENCODER.encode(text), sink);

// A sink shaped like RTCDataChannel.send: touches the frame, keeps nothing.
let sunk = 0;
const sink = (frame) => {
  sunk += frame.length;
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

const row = (name, ops) => console.log(`  ${name.padEnd(44)}${formatOps(ops).padStart(12)} ops/sec`);

// Every strategy must round-trip byte-identically before being timed.
const verify = () => {
  for (const limit of LIMITS) {
    const prod = new FrameEncoder(limit);
    const fresh = new FreshEncoder(limit);
    for (const size of [0, 1, limit - 2, limit - 1, limit, 2 * (limit - 1) + 1, 3 * limit + 7]) {
      const bytes = crypto.randomBytes(size);
      for (const encoder of [prod, fresh]) {
        const decoder = new FrameDecoder();
        let out = null;
        encoder.encode(KIND_BINARY, bytes, (frame) => {
          const result = decoder.push(Uint8Array.from(frame));
          if (result) out = result.data;
        });
        if (!out || Buffer.compare(Buffer.from(out), bytes) !== 0) throw new Error(`round-trip failed at ${size}`);
      }
      const text = 'x'.repeat(size);
      const decoder = new FrameDecoder();
      let out = null;
      prod.encodeText(text, (frame) => {
        const result = decoder.push(Uint8Array.from(frame));
        if (result) out = result.data;
      });
      if (out !== text) throw new Error(`text round-trip failed at ${size}`);
    }
  }
};

const main = () => {
  console.log(`Data-channel framing benchmark — Node ${process.version}`);
  verify();
  for (const limit of LIMITS) {
    console.log(`\nmaxMessageSize ${limit / KIB} KiB`);
    const prod = new FrameEncoder(limit);
    const fresh = new FreshEncoder(limit);
    for (const { label, size, iterations } of SIZES) {
      const bytes = crypto.randomBytes(size);
      const text = crypto
        .randomBytes(Math.ceil(size / 2))
        .toString('hex')
        .slice(0, size);
      console.log(`  -- ${label} (${formatNumber(iterations)} iterations)`);
      row(
        '(a) bytes: fresh frame per fragment',
        bench(() => fresh.encode(KIND_BINARY, bytes, sink), iterations),
      );
      row(
        '(b) bytes: scratch buffer reuse [prod]',
        bench(() => prod.encode(KIND_BINARY, bytes, sink), iterations),
      );
      row(
        '(c) text: encode() then copy, fresh',
        bench(() => encodeTextViaBytes(fresh, text, sink), iterations),
      );
      row(
        '(d) text: encodeInto() scratch [prod]',
        bench(() => prod.encodeText(text, sink), iterations),
      );
      // Decode: the single-fragment fast path is what a packet takes; the
      // reassembly path is what a chunk over the limit takes.
      const frames = [];
      prod.encode(KIND_BINARY, bytes, (frame) => frames.push(Uint8Array.from(frame)));
      const decoder = new FrameDecoder();
      const plural = frames.length === 1 ? '' : 's';
      row(
        `(e) decode: subarray [prod] (${frames.length} fragment${plural})`,
        bench(() => {
          for (let i = 0; i < frames.length; i++) decoder.push(frames[i]);
        }, iterations),
      );
      row(
        '(f) decode: slice copy',
        bench(() => {
          for (let i = 0; i < frames.length; i++) {
            const result = decoder.push(frames[i]);
            if (result) sunk += result.data.slice().length;
          }
        }, iterations),
      );
    }
  }
  if (sunk < 0) console.log(sunk); // keep the sink observable
};

main();
