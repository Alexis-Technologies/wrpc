'use strict';

// `Content-Encoding` on the two HTTP-shaped transports, priced: a one-shot
// gzip of a packet/REST answer at the sizes RPC answers actually come in,
// the SSE path — one gzip member per response, sync-flushed after every
// event — against the same events sent plain, and the Accept-Encoding scan
// that picks the coding (src/contentEncoding.js pickEncoding) against the
// split-and-map it is written to avoid. Both options are off by default;
// this is what turning them on costs and buys. Which coding, at which
// level, is bench/algorithms.js.

const zlib = require('node:zlib');
const { performance } = require('node:perf_hooks');

const { encodedWriter, normalizeCompression, pickEncoding } = require('../src/contentEncoding.js');

const report = (name, count, elapsedMs, extra = '') =>
  console.log(
    `  ${name.padEnd(52)}${Math.round((count / elapsedMs) * 1000)
      .toLocaleString('en-US')
      .padStart(12)}/sec${extra}`,
  );

const answer = (rows) =>
  JSON.stringify({
    type: 'callback',
    id: 'c1',
    result: Array.from({ length: rows }, (_, i) => ({
      id: i,
      name: `row-${i}`,
      email: `user${i}@example.com`,
      createdAt: '2026-09-19T10:00:00.000Z',
      tags: ['alpha', 'beta'],
      balance: (i * 37) % 1000,
    })),
  });

const oneShot = (label, text, iterations) => {
  const body = Buffer.from(text);
  let bytes = 0;
  const started = performance.now();
  for (let i = 0; i < iterations; i++) bytes += zlib.gzipSync(body).length;
  const elapsed = performance.now() - started;
  const ratio = (body.length / (bytes / iterations)).toFixed(1);
  report(`${label} (${body.length.toLocaleString('en-US')} B)`, iterations, elapsed, `   ${ratio}x`);
};

const event = (i) =>
  `id: ${i}\ndata: ${JSON.stringify({
    type: 'event',
    name: 'market/tick',
    data: { symbol: 'BTC-USD', bid: 42000 + (i % 100), ask: 42001 + (i % 100), ts: 1726500000000 + i },
  })}\n\n`;

// The SSE member: every event flushed the moment it is written, so each one
// is its own (tiny) deflate block against the stream's history.
const sseStream = (count) =>
  new Promise((resolve) => {
    let wire = 0;
    let plain = 0;
    let done = 0;
    const sink = {
      write: (chunk) => {
        wire += chunk.length;
        return true;
      },
      end: () => resolve({ wire, plain, elapsed: performance.now() - started }),
      onClose: () => {},
      onDrain: () => {},
    };
    const writer = encodedWriter(sink, normalizeCompression(true, 'bench').encoders[0]);
    const started = performance.now();
    for (let i = 0; i < count; i++) {
      const text = event(i);
      plain += text.length;
      writer.write(text);
      done++;
    }
    if (done === count) writer.end();
  });

// The idiomatic spelling of the same choice: split, map, find.
const pickBySplit = (header, encoders) => {
  const accepted = new Map(
    header.split(',').map((part) => {
      const [token, ...params] = part.split(';');
      const zero = params.some((param) => /^\s*q\s*=\s*0(?:\.0{0,3})?\s*$/i.test(param));
      return [token.trim().toLowerCase(), !zero];
    }),
  );
  return encoders.find((encoder) => accepted.get(encoder.token) ?? accepted.get('*') ?? false) ?? null;
};

const negotiation = () => {
  const { encoders } = normalizeCompression({ encodings: ['br', 'gzip'] }, 'bench');
  const headers = ['gzip, deflate, br, zstd', 'gzip, deflate', 'br;q=1.0, gzip;q=0.8, *;q=0.1'];
  const iterations = 1_000_000;
  for (const [name, pick] of [
    ['pickEncoding (one scan)', pickEncoding],
    ['split + map + find', pickBySplit],
  ]) {
    let hits = 0;
    const started = performance.now();
    for (let i = 0; i < iterations; i++) if (pick(headers[i % 3], encoders) !== null) hits++;
    report(name, iterations, performance.now() - started, hits === iterations ? '' : '   MISMATCH');
  }
};

const main = async () => {
  console.log('HTTP: choosing the coding from Accept-Encoding');
  negotiation();

  console.log('HTTP: one-shot gzip of an answer (packet mode / REST)');
  oneShot('callback, 2 rows', answer(2), 20_000);
  oneShot('callback, 12 rows', answer(12), 20_000);
  oneShot('callback, 64 rows', answer(64), 5_000);
  oneShot('callback, 1000 rows', answer(1000), 300);

  console.log('SSE: one gzip member per response, flushed per event');
  const count = 20_000;
  const { wire, plain, elapsed } = await sseStream(count);
  report('repeated 90 B event through the member', count, elapsed, `   ${(plain / wire).toFixed(1)}x`);
  console.log(
    `  ${''.padEnd(52)}${(plain / count).toFixed(0).padStart(6)} B plain -> ${(wire / count).toFixed(0)} B on the wire per event`,
  );
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
