'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const zlib = require('node:zlib');

const { normalizeCompression, nativeCompressor } = require('../../src/compression/index.js');
const { normalizeSyncCompression } = require('../../src/compression/sync.js');
const { hasZstd, ALGORITHMS } = require('../../src/compression/native.js');
const browser = require('../../src/compression/native.browser.js');
const { deflateCompressor, brotliCompressor, zstdCompressor } = require('../../src/compression/codecs.js');
const wrpc = require('../../index.js');

const zstdHere = hasZstd(zlib);
const text = (n) => new TextEncoder().encode(JSON.stringify({ rows: Array.from({ length: n }, (_, i) => ({ i })) }));
const EMPTY = new Uint8Array(0);

// A zlib with no Zstandard, as node:zlib is before 22.15 — the seam that
// covers both branches of the detection on whatever Node runs the suite.
const withoutZstd = () => {
  const lib = { ...zlib };
  delete lib.zstdCompressSync;
  delete lib.zstdDecompressSync;
  return lib;
};

test('the platform algorithms are the CompressionStream format names', () => {
  assert.deepStrictEqual(ALGORITHMS, ['deflate-raw', 'brotli', 'zstd']);
  assert.strictEqual(nativeCompressor().id, 'deflate-raw', 'deflate stays what no name means');
});

for (const algorithm of ['deflate-raw', 'brotli', 'zstd']) {
  test(`${algorithm}: round trip, the empty message, and the cap bounds an inflate`, (t) => {
    if (algorithm === 'zstd' && !zstdHere) return void t.skip('no zstd in this node:zlib');
    const codec = nativeCompressor({ algorithm });
    assert.strictEqual(codec.id, algorithm);
    assert.strictEqual(codec.threshold, 1024);
    assert.strictEqual(codec.async, null);
    const input = text(300);
    const encoded = codec.encode(input);
    assert.ok(encoded instanceof Uint8Array, 'synchronous');
    assert.ok(encoded.length < input.length / 4, `${input.length} -> ${encoded.length}`);
    assert.deepStrictEqual(new Uint8Array(codec.decode(encoded, input.length)), input);
    // The sync carriers probe a codec with an empty message at construction.
    assert.strictEqual(codec.decode(codec.encode(EMPTY), 16).length, 0);
    assert.throws(() => codec.decode(encoded, 64), /ERR_BUFFER_TOO_LARGE|maxOutputLength|exceed/i);
    assert.ok(normalizeSyncCompression({ codec: algorithm }, 'x'), 'usable on a Node↔Node carrier');
  });

  test(`${algorithm}: async hands only a message past the threshold to the threadpool`, async (t) => {
    if (algorithm === 'zstd' && !zstdHere) return void t.skip('no zstd in this node:zlib');
    const codec = nativeCompressor({ algorithm, async: 2048 });
    const small = text(20);
    const large = text(600);
    assert.ok(codec.encode(small) instanceof Uint8Array);
    const pending = codec.encode(large);
    assert.strictEqual(typeof pending.then, 'function');
    assert.deepStrictEqual(new Uint8Array(codec.decode(await pending, large.length)), large);
    assert.throws(() => normalizeSyncCompression({ codec }, 'x'), /declares async/);
  });
}

test('the default levels are the measured ones, not zlib’s', () => {
  const input = text(2000);
  assert.deepStrictEqual(nativeCompressor().encode(input), zlib.deflateRawSync(input, { level: 3 }));
  const quality = (q) =>
    zlib.brotliCompressSync(input, {
      params: {
        [zlib.constants.BROTLI_PARAM_QUALITY]: q,
        [zlib.constants.BROTLI_PARAM_MODE]: zlib.constants.BROTLI_MODE_TEXT,
        [zlib.constants.BROTLI_PARAM_SIZE_HINT]: input.length,
      },
    });
  assert.deepStrictEqual(nativeCompressor({ algorithm: 'brotli' }).encode(input), quality(4));
  assert.notDeepStrictEqual(quality(4), quality(11), 'and quality 11 would have been another stream');
});

test('nativeCompressor: a closed set of names, a level inside the family’s range', () => {
  assert.throws(() => nativeCompressor({ algorithm: 'gzip' }), /unknown algorithm "gzip"/);
  assert.throws(() => nativeCompressor({ algorithm: 'toString' }), /unknown algorithm/);
  assert.throws(() => nativeCompressor({ algorithm: 7 }), /unknown algorithm/);
  assert.throws(() => nativeCompressor({ level: 10 }), /deflate-raw level must be an integer from -1 to 9/);
  assert.throws(() => nativeCompressor({ algorithm: 'brotli', level: 12 }), /brotli level .* 0 to 11/);
  assert.throws(() => nativeCompressor({ algorithm: 'brotli', level: 1.5 }), /brotli level/);
  assert.throws(() => nativeCompressor({ algorithm: 'zstd', level: 0 }), /zstd level .* 1 to 22/);
});

test('zstd: detected, not assumed — a TypeError where node:zlib has none, null under `optional`', () => {
  assert.strictEqual(hasZstd(zlib), typeof zlib.zstdCompressSync === 'function');
  const old = withoutZstd();
  assert.strictEqual(hasZstd(old), false);
  assert.throws(() => nativeCompressor({ algorithm: 'zstd', zlib: old }), /no zstd in node:zlib \(22\.15\+/);
  assert.strictEqual(nativeCompressor({ algorithm: 'zstd', zlib: old, optional: true }), null);
  // The other two never depend on it.
  assert.strictEqual(nativeCompressor({ algorithm: 'brotli', zlib: old }).id, 'brotli');
  assert.strictEqual(nativeCompressor({ zlib: old }).id, 'deflate-raw');
  const fake = { ...zlib, zstdCompressSync: () => EMPTY, zstdDecompressSync: () => EMPTY };
  assert.strictEqual(nativeCompressor({ algorithm: 'zstd', zlib: fake }).id, 'zstd');
});

test('normalizeCompression: `codec` takes a platform algorithm by name, with `async`', () => {
  const brotli = normalizeCompression({ codec: 'brotli' }, 'x');
  assert.strictEqual(brotli.id, 'brotli');
  assert.strictEqual(brotli.threshold, 1024);
  assert.strictEqual(normalizeCompression({ codec: 'brotli', threshold: 64 }, 'x').threshold, 64);
  assert.strictEqual(normalizeCompression({ codec: 'brotli', async: true }, 'x').codec.async, 256 * 1024);
  assert.strictEqual(normalizeCompression({ codec: 'deflate-raw' }, 'x').id, 'deflate-raw');
  assert.throws(() => normalizeCompression({ codec: 'lz4' }, 'x'), /unknown algorithm "lz4"/);
  assert.throws(() => normalizeCompression({ codec: 7 }, 'x'), /must provide an id, .* or name an algorithm/);
  // One spelling: the name lives under `codec`, never in place of the options.
  assert.throws(() => normalizeCompression('zstd', 'x'), /must be true, false or an options object/);
});

test('the factories: a level, a threshold and async per codec — and the same ids', async () => {
  assert.strictEqual(wrpc.brotliCompressor, brotliCompressor, 'exported from the main entry');
  assert.strictEqual(wrpc.deflateCompressor, deflateCompressor);
  assert.strictEqual(wrpc.zstdCompressor, zstdCompressor);
  const input = text(2000);
  const fast = deflateCompressor({ level: 1 });
  assert.strictEqual(fast.id, 'deflate-raw', 'the id names the format, never the level');
  assert.deepStrictEqual(fast.encode(input), zlib.deflateRawSync(input, { level: 1 }));
  assert.deepStrictEqual(new Uint8Array(nativeCompressor().decode(fast.encode(input), input.length)), input);
  const small = brotliCompressor({ quality: 5, threshold: 128 });
  assert.strictEqual(small.id, 'brotli');
  assert.strictEqual(small.threshold, 128);
  assert.deepStrictEqual(new Uint8Array(brotliCompressor().decode(small.encode(input), input.length)), input);
  assert.strictEqual(normalizeCompression({ codec: small }, 'x').threshold, 128, 'the codec names its own threshold');
  const pooled = deflateCompressor({ async: { threshold: 1024 } });
  assert.strictEqual(pooled.async, 1024);
  assert.deepStrictEqual(new Uint8Array(pooled.decode(await pooled.encode(input), input.length)), input);
  assert.throws(() => deflateCompressor({ level: 11 }), /level/);
  assert.throws(() => brotliCompressor({ quality: -1 }), /brotli level/);
  assert.throws(
    () => brotliCompressor({ threshold: -1 }),
    /brotliCompressor: threshold must be a non-negative integer/,
  );
  assert.throws(() => deflateCompressor({ async: 'yes' }), /deflateCompressor: async must be/);
  if (zstdHere) {
    const zstd = zstdCompressor({ level: 3 });
    assert.strictEqual(zstd.id, 'zstd');
    assert.deepStrictEqual(new Uint8Array(zstd.decode(zstd.encode(input), input.length)), input);
  } else {
    assert.throws(() => zstdCompressor(), /no zstd/);
  }
});

test('native (browser): a format by name, null for one this CompressionStream lacks, zlib reads what it writes', async (t) => {
  if (browser.nativeCompressor() === null) return void t.skip('no CompressionStream here');
  assert.throws(() => browser.nativeCompressor({ algorithm: 'gzip' }), /unknown algorithm "gzip"/);
  const input = text(300);
  const decoders = { brotli: zlib.brotliDecompressSync, zstd: zlib.zstdDecompressSync };
  for (const algorithm of ['brotli', 'zstd']) {
    const codec = browser.nativeCompressor({ algorithm });
    let supported = true;
    try {
      void new CompressionStream(algorithm);
    } catch {
      supported = false;
    }
    if (!supported) {
      assert.strictEqual(codec, null, `${algorithm}: not a format here, so no codec — never a throw`);
      continue;
    }
    assert.strictEqual(codec.id, algorithm, 'the same id as the node half');
    const encoded = await codec.encode(input);
    assert.deepStrictEqual(new Uint8Array(await codec.decode(encoded, input.length)), input);
    if (typeof decoders[algorithm] === 'function') {
      assert.deepStrictEqual(new Uint8Array(decoders[algorithm](encoded)), input, 'zlib reads the page’s bytes');
      const fromNode = nativeCompressor({ algorithm }).encode(input);
      assert.deepStrictEqual(new Uint8Array(await codec.decode(fromNode, input.length)), input, 'and the page zlib’s');
    }
    await assert.rejects(codec.decode(encoded, 64), /exceeds the cap/);
  }
});

test('native (browser): no CompressionStream at all is no codec', () => {
  const saved = globalThis.CompressionStream;
  globalThis.CompressionStream = undefined;
  try {
    assert.strictEqual(browser.nativeCompressor(), null);
  } finally {
    globalThis.CompressionStream = saved;
  }
});
