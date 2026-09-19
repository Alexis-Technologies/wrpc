'use strict';

// The pure-JS DEFLATE codec (@alexify/wrpc/deflate) against node:zlib and,
// where this Node has one, the platform's CompressionStream — at the sizes
// RPC messages come in, with and without the router dictionary. Two
// things this bench decides: that fixed Huffman costs nothing on a small
// message against a dictionary (so the encoder needs no dynamic trees), and
// where the hybrid codec should hand a large message to the platform
// (`nativeAbove`), since fixed codes fall behind dynamic ones there.

const zlib = require('node:zlib');
const { performance } = require('node:perf_hooks');

const { defineRouter, procedure } = require('../src/rpc/router.js');
const { buildDictionary } = require('../src/rpc/dictionary.js');
const { inflateRaw, deflateRaw } = require('../src/deflate/index.js');

const router = defineRouter({
  market: {
    quote: procedure({
      access: 'public',
      signature: { args: { symbol: 'string' }, returns: { bid: 'number', ask: 'number', ts: 'number' } },
      handler: async () => ({}),
    }),
    orders: procedure({
      access: 'public',
      signature: { returns: [{ id: 'string', side: 'string', price: 'number', size: 'number', createdAt: 'string' }] },
      handler: async () => [],
    }),
    emits: { tick: { data: { symbol: 'string', bid: 'number', ask: 'number', ts: 'number' } } },
  },
});
const dictionary = buildDictionary(router);

const encoder = new TextEncoder();
const tick = () =>
  encoder.encode(
    JSON.stringify({
      type: 'event',
      name: 'market/tick',
      data: { symbol: 'BTC-USD', bid: 42000.5, ask: 42001, ts: 1726500000000 },
    }),
  );
const orders = (rows) =>
  encoder.encode(
    JSON.stringify({
      type: 'callback',
      id: 'c1',
      result: Array.from({ length: rows }, (_, i) => ({
        id: `o-${i}`,
        side: i % 2 ? 'buy' : 'sell',
        price: 42000 + i,
        size: (i % 7) + 1,
        createdAt: '2026-09-19T10:00:00.000Z',
      })),
    }),
  );

const report = (name, count, elapsedMs, extra = '') =>
  console.log(
    `  ${name.padEnd(48)}${Math.round((count / elapsedMs) * 1000)
      .toLocaleString('en-US')
      .padStart(12)}/sec${extra}`,
  );

const timeSync = (label, fn, iterations, size) => {
  let out = 0;
  const started = performance.now();
  for (let i = 0; i < iterations; i++) out += fn().length;
  const elapsed = performance.now() - started;
  report(label, iterations, elapsed, size === undefined ? '' : `   ${size} -> ${Math.round(out / iterations)} B`);
};

const timeAsync = async (label, fn, iterations, size) => {
  let out = 0;
  const started = performance.now();
  for (let i = 0; i < iterations; i++) out += (await fn()).length;
  const elapsed = performance.now() - started;
  report(label, iterations, elapsed, size === undefined ? '' : `   ${size} -> ${Math.round(out / iterations)} B`);
};

const through = async (stream, bytes) => {
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader();
  const written = writer.write(bytes).then(() => writer.close());
  const chunks = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  await written;
  return chunks.length === 1 ? chunks[0] : Buffer.concat(chunks.map((c) => Buffer.from(c)));
};

const main = async () => {
  console.log(`dictionary: ${dictionary.length} B from the router`);
  const cases = [
    ['event 108 B', tick(), 20_000],
    ['callback 2 KB', orders(20), 5_000],
    ['callback 28 KB', orders(300), 300],
  ];
  for (const [label, bytes, n] of cases) {
    console.log(`\n${label}`);
    const size = bytes.length;
    timeSync('own deflate, dictionary (fixed Huffman)', () => deflateRaw(bytes, { dictionary }), n, size);
    timeSync(
      'zlib, dictionary, Z_FIXED',
      () => zlib.deflateRawSync(bytes, { dictionary, strategy: zlib.constants.Z_FIXED }),
      n,
      size,
    );
    timeSync('zlib, dictionary (dynamic)', () => zlib.deflateRawSync(bytes, { dictionary }), n, size);
    timeSync('own deflate, no dictionary', () => deflateRaw(bytes), n, size);
    timeSync('zlib, no dictionary (dynamic)', () => zlib.deflateRawSync(bytes), n, size);
    if (typeof CompressionStream === 'function') {
      await timeAsync(
        'CompressionStream, no dictionary',
        () => through(new CompressionStream('deflate-raw'), bytes),
        Math.max(50, n / 4),
        size,
      );
    }
    const encoded = zlib.deflateRawSync(bytes, { dictionary });
    timeSync('own inflate, dictionary', () => inflateRaw(encoded, { dictionary }), n);
    timeSync('zlib inflate, dictionary', () => zlib.inflateRawSync(encoded, { dictionary }), n);
    const plainEncoded = zlib.deflateRawSync(bytes);
    timeSync('own inflate, no dictionary', () => inflateRaw(plainEncoded), n);
    if (typeof DecompressionStream === 'function') {
      await timeAsync(
        'DecompressionStream, no dictionary',
        () => through(new DecompressionStream('deflate-raw'), plainEncoded),
        Math.max(50, n / 4),
      );
    }
  }
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
