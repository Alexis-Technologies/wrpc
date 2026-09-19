'use strict';

// Which algorithm, at which level — the measurement behind every default in
// src/compression/native.js and src/contentEncoding.js. Three families
// node:zlib ships (deflate, Brotli, Zstandard where this Node has it), at
// the levels a per-message codec can afford, over the sizes RPC messages
// come in; then the persistent-stream shape an SSE response uses (one
// context, flushed after every event), with what a thousand open streams
// cost in memory — a context is per connection there, not per message.
//
// What it decides:
//   - `compression: true` stays deflate: level with zstd in bytes and CPU
//     up to ~2 KB, where RPC messages live, and the only format every
//     CompressionStream and every Node has.
//   - the platform deflate's default level, 3: zlib's levels 1–3 are its
//     fast strategy and 4+ the lazy one, and 3 is the knee — at 27 KB it
//     costs under half of level 6 for ~5% more bytes, and level 4 is both
//     slower AND larger than it.
//   - the default Brotli quality (4) and zstd level (1, with the source
//     size pledged) — and that zlib's own Brotli default, quality 11, is
//     never an option on a message path: milliseconds where the rest take
//     microseconds.
//   - SSE stays gzip: a flushed Brotli or zstd stream saves no bytes over
//     it on small events and holds 2–3x the memory per open response,
//     which a smaller window does not give back (the tables are the cost).

const { execFileSync } = require('node:child_process');
const { performance } = require('node:perf_hooks');
const zlib = require('node:zlib');

const c = zlib.constants;
const hasZstd = typeof zlib.zstdCompressSync === 'function';

const callback = (rows) =>
  Buffer.from(
    JSON.stringify({
      type: 'callback',
      id: 42,
      result: {
        items: Array.from({ length: rows }, (_, i) => ({
          id: 1000 + i,
          name: `User ${i}`,
          email: `user${i}@example.com`,
          createdAt: new Date(1.7e12 + i * 86400000).toISOString(),
          roles: i % 3 ? ['member'] : ['admin', 'member'],
          active: i % 2 === 0,
          score: Math.round(Math.sin(i) * 1e4) / 100,
        })),
        total: rows,
        cursor: `c${rows}`,
      },
    }),
  );

const event = (i) =>
  `data: ${JSON.stringify({
    type: 'event',
    name: 'chat/message',
    data: {
      id: 9000 + i,
      room: 'general',
      from: `user${i % 7}`,
      text: `hello there number ${i} how are you`,
      ts: 1.7e12 + i * 1000,
    },
  })}\n\n`;

const brotli = (quality) => (bytes) =>
  zlib.brotliCompressSync(bytes, {
    params: {
      [c.BROTLI_PARAM_QUALITY]: quality,
      [c.BROTLI_PARAM_MODE]: c.BROTLI_MODE_TEXT,
      [c.BROTLI_PARAM_SIZE_HINT]: bytes.length,
    },
  });

const zstd = (level) => (bytes) =>
  zlib.zstdCompressSync(bytes, { params: { [c.ZSTD_c_compressionLevel]: level }, pledgedSrcSize: bytes.length });

const codecs = [
  ['deflate-raw L1', (bytes) => zlib.deflateRawSync(bytes, { level: 1 }), (bytes) => zlib.inflateRawSync(bytes)],
  ['deflate-raw L3', (bytes) => zlib.deflateRawSync(bytes, { level: 3 }), (bytes) => zlib.inflateRawSync(bytes)],
  ['deflate-raw L4', (bytes) => zlib.deflateRawSync(bytes, { level: 4 }), (bytes) => zlib.inflateRawSync(bytes)],
  ['deflate-raw L6', (bytes) => zlib.deflateRawSync(bytes), (bytes) => zlib.inflateRawSync(bytes)],
  ['gzip L6', (bytes) => zlib.gzipSync(bytes), (bytes) => zlib.gunzipSync(bytes)],
  ['brotli q1', brotli(1), (bytes) => zlib.brotliDecompressSync(bytes)],
  ['brotli q4', brotli(4), (bytes) => zlib.brotliDecompressSync(bytes)],
  ['brotli q5', brotli(5), (bytes) => zlib.brotliDecompressSync(bytes)],
  ['brotli q11 (zlib default)', brotli(11), (bytes) => zlib.brotliDecompressSync(bytes)],
];
if (hasZstd) {
  codecs.push(
    ['zstd L1', zstd(1), (bytes) => zlib.zstdDecompressSync(bytes)],
    ['zstd L3', zstd(3), (bytes) => zlib.zstdDecompressSync(bytes)],
  );
}

// Microseconds per call: a fixed warm-up, then batches until the window is
// spent — quality 11 on 255 KB takes a third of a second a call, so a fixed
// iteration count would either starve the fast rows or stall on the slow one.
const time = (fn, input, windowMs = 300) => {
  const warm = Math.min(50, Math.max(2, Math.floor(2e6 / input.length)));
  for (let i = 0; i < warm; i++) fn(input);
  let count = 0;
  const started = performance.now();
  let elapsed = 0;
  do {
    fn(input);
    count++;
    elapsed = performance.now() - started;
  } while (elapsed < windowMs);
  return (elapsed * 1000) / count;
};

const micros = (value) => value.toFixed(1).padStart(10);

const oneShot = () => {
  for (const rows of [1, 2, 12, 180, 1700]) {
    const plain = callback(rows);
    console.log(`callback, ${plain.length.toLocaleString('en-US')} B plain`);
    for (const [name, encode, decode] of codecs) {
      const encoded = encode(plain);
      const share = ((encoded.length / plain.length) * 100).toFixed(1).padStart(5);
      console.log(
        `  ${name.padEnd(28)}${encoded.length.toLocaleString('en-US').padStart(9)} B ${share}%` +
          `   encode${micros(time(encode, plain))} µs   decode${micros(time(decode, encoded))} µs`,
      );
    }
  }
};

const streams = [
  ['gzip L6', () => zlib.createGzip(), c.Z_SYNC_FLUSH],
  ['gzip L6, windowBits 12', () => zlib.createGzip({ windowBits: 12, memLevel: 5 }), c.Z_SYNC_FLUSH],
  ['brotli q4', () => zlib.createBrotliCompress({ params: { [c.BROTLI_PARAM_QUALITY]: 4 } }), c.BROTLI_OPERATION_FLUSH],
  [
    'brotli q4, lgwin 16',
    () => zlib.createBrotliCompress({ params: { [c.BROTLI_PARAM_QUALITY]: 4, [c.BROTLI_PARAM_LGWIN]: 16 } }),
    c.BROTLI_OPERATION_FLUSH,
  ],
];
if (hasZstd) {
  streams.push(
    ['zstd L3', () => zlib.createZstdCompress(), c.ZSTD_e_flush],
    [
      'zstd L3, windowLog 16',
      () => zlib.createZstdCompress({ params: { [c.ZSTD_c_compressionLevel]: 3, [c.ZSTD_c_windowLog]: 16 } }),
      c.ZSTD_e_flush,
    ],
  );
}

// One persistent stream, every event flushed before the next is written —
// the SSE writer's shape. The time includes the flush round trip through
// the stream, which is what an event really waits for.
const flushed = (create, flush, events) =>
  new Promise((resolve, reject) => {
    const stream = create();
    let bytes = 0;
    stream.on('data', (chunk) => {
      bytes += chunk.length;
    });
    stream.once('error', reject);
    let i = 0;
    const started = performance.now();
    const step = () => {
      if (i === events) {
        const elapsed = performance.now() - started;
        stream.destroy();
        return void resolve({ perEvent: bytes / events, micros: (elapsed * 1000) / events });
      }
      stream.write(event(i++));
      stream.flush(flush, step);
    };
    step();
  });

// What N open streams hold on to: each has written one event and flushed,
// so its window and hash tables are allocated, as on a live connection.
// Measured in a child process per row — in one process the memory a
// destroyed row gave back is what the next row allocates from, and the
// later rows read as free.
const resident = async (index, count) => {
  const [, create, flush] = streams[index];
  const before = process.memoryUsage().rss;
  const open = [];
  for (let i = 0; i < count; i++) {
    const stream = create();
    stream.on('data', () => {});
    open.push(stream);
    await new Promise((resolve) => {
      stream.write(event(i));
      stream.flush(flush, resolve);
    });
  }
  return (process.memoryUsage().rss - before) / count;
};

const residentInChild = (index, count) =>
  Number(execFileSync(process.execPath, [__filename, 'resident', String(index), String(count)], { encoding: 'utf8' }));

const persistent = async () => {
  console.log(`one stream, flushed per event (${event(1).length} B plain)`);
  for (let index = 0; index < streams.length; index++) {
    const [name, create, flush] = streams[index];
    const { perEvent, micros: perFlush } = await flushed(create, flush, 2000);
    const each = residentInChild(index, 1000);
    console.log(
      `  ${name.padEnd(28)}${perEvent.toFixed(1).padStart(9)} B/event${micros(perFlush)} µs/event` +
        `${Math.round(each / 1024)
          .toLocaleString('en-US')
          .padStart(8)} KB resident/stream`,
    );
  }
};

const main = async () => {
  if (process.argv[2] === 'resident') {
    const each = await resident(Number(process.argv[3]), Number(process.argv[4]));
    process.stdout.write(String(each));
    return void process.exit(0);
  }
  if (!hasZstd) console.log('(this Node has no zstd in node:zlib — those rows are skipped)');
  oneShot();
  await persistent();
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
