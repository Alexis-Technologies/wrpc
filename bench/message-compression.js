'use strict';

// Per-message compression (src/compression) on the transports that have
// nothing under them — WebRTC data channels and WebTransport streams —
// priced at the sizes RPC messages come in: the codec itself (node:zlib
// one-shot raw deflate, what a Node peer uses; a browser's
// CompressionStream where this Node has one, what a page uses), and the
// Sequencer that keeps an asynchronous codec's output in order. Off by
// default; this is what turning it on costs and buys, and where the
// thresholds (1 KiB on Node, 4 KiB in a browser) come from.

const { performance } = require('node:perf_hooks');

const { nativeCompressor, Sequencer } = require('../src/compression/index.js');
const browser = require('../src/compression/native.browser.js');

const report = (name, count, elapsedMs, extra = '') =>
  console.log(
    `  ${name.padEnd(52)}${Math.round((count / elapsedMs) * 1000)
      .toLocaleString('en-US')
      .padStart(12)}/sec${extra}`,
  );

const event = (i) =>
  JSON.stringify({
    type: 'event',
    name: 'market/tick',
    data: { symbol: 'BTC-USD', bid: 42000 + (i % 100), ask: 42001 + (i % 100), ts: 1726500000000 + i },
  });

const callback = (rows) =>
  JSON.stringify({
    type: 'callback',
    id: 'c1',
    result: Array.from({ length: rows }, (_, i) => ({
      id: i,
      name: `row-${i}`,
      email: `user${i}@example.com`,
      createdAt: '2026-09-19T10:00:00.000Z',
      tags: ['alpha', 'beta'],
    })),
  });

const encoder = new TextEncoder();

const syncRound = (label, codec, text, iterations) => {
  const bytes = encoder.encode(text);
  let out = 0;
  let started = performance.now();
  for (let i = 0; i < iterations; i++) out += codec.encode(bytes).length;
  const encodeMs = performance.now() - started;
  const encoded = codec.encode(bytes);
  started = performance.now();
  for (let i = 0; i < iterations; i++) codec.decode(encoded, bytes.length);
  const decodeMs = performance.now() - started;
  const ratio = (bytes.length / (out / iterations)).toFixed(1);
  report(`${label} ${bytes.length.toLocaleString('en-US')} B, encode`, iterations, encodeMs, `   ${ratio}x`);
  report(`${label} ${bytes.length.toLocaleString('en-US')} B, decode`, iterations, decodeMs);
};

const asyncRound = async (label, codec, text, iterations) => {
  const bytes = encoder.encode(text);
  let started = performance.now();
  let out = 0;
  for (let i = 0; i < iterations; i++) out += (await codec.encode(bytes)).length;
  const encodeMs = performance.now() - started;
  const encoded = await codec.encode(bytes);
  started = performance.now();
  for (let i = 0; i < iterations; i++) await codec.decode(encoded, bytes.length);
  const decodeMs = performance.now() - started;
  const ratio = (bytes.length / (out / iterations)).toFixed(1);
  report(`${label} ${bytes.length.toLocaleString('en-US')} B, encode`, iterations, encodeMs, `   ${ratio}x`);
  report(`${label} ${bytes.length.toLocaleString('en-US')} B, decode`, iterations, decodeMs);
};

const sequencer = async () => {
  const count = 200_000;
  let delivered = 0;
  const deliver = () => {
    delivered++;
  };
  let queue = new Sequencer();
  let started = performance.now();
  for (let i = 0; i < count; i++) queue.push(i, deliver);
  report('Sequencer, plain values (the synchronous path)', count, performance.now() - started);
  queue = new Sequencer();
  delivered = 0;
  started = performance.now();
  for (let i = 0; i < count; i++) queue.push(Promise.resolve(i), deliver);
  await new Promise((resolve) => {
    const check = () => (delivered === count ? resolve() : setImmediate(check));
    check();
  });
  report('Sequencer, resolved promises (the async path)', count, performance.now() - started);
};

const main = async () => {
  const node = nativeCompressor();
  console.log('node:zlib raw deflate, one message at a time');
  syncRound('event', node, event(1), 50_000);
  syncRound('callback', node, callback(12), 20_000);
  syncRound('callback', node, callback(200), 2_000);
  syncRound('callback', node, callback(4000), 100);

  const page = browser.nativeCompressor();
  if (page !== null) {
    console.log('CompressionStream (deflate-raw), one message at a time — what a page pays');
    await asyncRound('event', page, event(1), 5_000);
    await asyncRound('callback', page, callback(12), 5_000);
    await asyncRound('callback', page, callback(200), 1_000);
    await asyncRound('callback', page, callback(4000), 50);
  }

  console.log('Order keeping');
  await sequencer();
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
