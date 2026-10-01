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
const { inflateRaw, deflateRaw, createDeflateCodec } = require('../src/deflate/index.js');

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

// A skewed byte distribution — one value nearly always, the rest spread
// thin — is what gives a dynamic Huffman tree its longest codes (zlib caps
// them at 15 bits): the shape that costs a table-driven inflater the most.
// Deterministic (an LCG), so every run measures the same bytes.
const skewed = (size, seed = 7) => {
  const out = new Uint8Array(size);
  let x = seed >>> 0;
  for (let i = 0; i < size; i++) {
    x = (Math.imul(x, 1664525) + 1013904223) >>> 0;
    out[i] = x % 100 < 92 ? 0x20 : 1 + ((x >>> 8) % 255);
  }
  return out;
};

// One raw stream of many dynamic blocks: zlib fragments that end in a sync
// flush (no final bit) concatenated, the last one finished. Every block
// carries its own tree, so the per-block cost of the inflater is what this
// input measures.
const manyBlocks = (count, chunk) => {
  const parts = [];
  const plain = [];
  for (let i = 0; i < count; i++) {
    const bytes = skewed(chunk, i + 1);
    plain.push(bytes);
    const last = i === count - 1;
    parts.push(zlib.deflateRawSync(bytes, last ? {} : { finishFlush: zlib.constants.Z_SYNC_FLUSH }));
  }
  return { encoded: Buffer.concat(parts), plain: Buffer.concat(plain) };
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
  {
    // The encoder re-hashes the dictionary on every call, so a message's
    // cost grows with the dictionary it is compressed against — a 4 KB
    // one (a large router) and the 32 KiB zlib looks at, on the same
    // 2 KB callback; zlib beside it, which does the same work.
    const sample = Buffer.concat(Array.from({ length: 64 }, (_, i) => orders(20 + (i % 7))));
    const bytes = orders(20);
    console.log('\ndictionary size: 2 KB callback against a 4 KB and a 32 KiB dictionary');
    for (const size of [4096, 32768]) {
      const dict = sample.subarray(0, size);
      timeSync(
        `own deflate, ${size >> 10} KB dictionary`,
        () => deflateRaw(bytes, { dictionary: dict }),
        5_000,
        bytes.length,
      );
      // What a codec pays: the dictionary's hash chains are built once.
      const prepared = createDeflateCodec({ dictionary: dict, native: false });
      timeSync(
        `codec.encode, ${size >> 10} KB dictionary (prepared)`,
        () => prepared.encode(bytes),
        5_000,
        bytes.length,
      );
      timeSync(
        `zlib, ${size >> 10} KB dictionary (dynamic)`,
        () => zlib.deflateRawSync(bytes, { dictionary: dict }),
        5_000,
        bytes.length,
      );
    }
  }
  {
    const { encoded, plain } = manyBlocks(200, 4096);
    console.log(`\nmany dynamic blocks: 200 × 4 KB skewed, ${encoded.length} B encoded, ${plain.length} B plain`);
    timeSync('own inflate', () => inflateRaw(encoded), 50);
    timeSync('zlib inflate', () => zlib.inflateRawSync(encoded), 50);
  }
  {
    // Huffman-only over the skewed alphabet: the common byte gets a one-bit
    // code, every rare byte one of zlib's longest — the input on which the
    // inflater's sub-table path (codes past the root table) runs for every
    // rare symbol, so this row prices that path.
    const plain = skewed(65536);
    const encoded = zlib.deflateRawSync(plain, { strategy: zlib.constants.Z_HUFFMAN_ONLY });
    let rare = 0;
    for (const byte of plain) if (byte !== 0x20) rare++;
    console.log(`\nhuffman-only 64 KB skewed: ${rare} rare symbols on long codes, ${encoded.length} B encoded`);
    timeSync('own inflate', () => inflateRaw(encoded), 500);
    timeSync('zlib inflate', () => zlib.inflateRawSync(encoded), 500);
  }
  const cases = [
    ['event 108 B', tick(), 20_000],
    ['callback 2 KB', orders(20), 5_000],
    ['callback 28 KB', orders(300), 300],
  ];
  // The codec as a carrier holds it: one per process, its dictionary state
  // prepared on the first message (`native: false` — the pure-JS half).
  const codec = createDeflateCodec({ dictionary, native: false });
  const bare = createDeflateCodec({ native: false });
  for (const [label, bytes, n] of cases) {
    console.log(`\n${label}`);
    const size = bytes.length;
    timeSync('own deflate, dictionary (fixed Huffman)', () => deflateRaw(bytes, { dictionary }), n, size);
    timeSync('codec.encode, dictionary (prepared)', () => codec.encode(bytes), n, size);
    timeSync(
      'zlib, dictionary, Z_FIXED',
      () => zlib.deflateRawSync(bytes, { dictionary, strategy: zlib.constants.Z_FIXED }),
      n,
      size,
    );
    timeSync('zlib, dictionary (dynamic)', () => zlib.deflateRawSync(bytes, { dictionary }), n, size);
    timeSync('own deflate, no dictionary', () => deflateRaw(bytes), n, size);
    timeSync('codec.encode, no dictionary (prepared)', () => bare.encode(bytes), n, size);
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
