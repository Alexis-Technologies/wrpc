'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const zlib = require('node:zlib');
const timers = require('node:timers/promises');

const {
  isCompressor,
  normalizeCompression,
  negotiate,
  nativeCompressor,
  Sequencer,
  DEFAULT_THRESHOLD,
} = require('../../src/compression/index.js');
const browser = require('../../src/compression/native.browser.js');
const { normalizeSyncCompression } = require('../../src/compression/sync.js');
const { DEFAULT_ASYNC_THRESHOLD, normalizeAsync } = require('../../src/compression/index.js');

const text = (n) => new TextEncoder().encode(JSON.stringify({ rows: Array.from({ length: n }, (_, i) => ({ i })) }));

test('isCompressor: structural — id, encode and decode', () => {
  const codec = { id: 'x', encode: (b) => b, decode: (b) => b };
  assert.strictEqual(isCompressor(codec), true);
  assert.strictEqual(isCompressor({ ...codec, id: '' }), false);
  assert.strictEqual(isCompressor({ ...codec, id: 1 }), false);
  assert.strictEqual(isCompressor({ id: 'x', encode: (b) => b }), false);
  assert.strictEqual(isCompressor(null), false);
  assert.strictEqual(isCompressor('deflate'), false);
});

test('normalizeCompression: off in every spelling of off, the native codec by default, strict on what is given', () => {
  for (const off of [undefined, null, false]) assert.strictEqual(normalizeCompression(off, 'x'), null);
  const on = normalizeCompression(true, 'x');
  assert.strictEqual(on.id, 'deflate-raw');
  assert.strictEqual(on.threshold, 1024, 'the node codec names its own threshold');
  assert.strictEqual(Object.isFrozen(on), true);
  const codec = { id: 'mine', encode: (b) => b, decode: (b) => b };
  assert.strictEqual(normalizeCompression({ codec }, 'x').threshold, DEFAULT_THRESHOLD);
  assert.strictEqual(normalizeCompression({ codec: { ...codec, threshold: 7 } }, 'x').threshold, 7);
  assert.strictEqual(normalizeCompression({ codec, threshold: 0 }, 'x').threshold, 0, 'an explicit threshold wins');
  assert.throws(() => normalizeCompression('gzip', 'x'), /must be true, false or an options object/);
  assert.throws(() => normalizeCompression([], 'x'), /must be true, false or an options object/);
  assert.throws(() => normalizeCompression({ codec: {} }, 'x'), /compression\.codec must provide an id/);
  assert.throws(() => normalizeCompression({ threshold: -1 }, 'x'), /threshold must be a non-negative integer/);
  assert.throws(() => normalizeCompression({ threshold: 1.5 }, 'x'), /threshold/);
});

test('normalizeCompression: async — the platform codec goes to the threadpool past a threshold, off by default', () => {
  assert.strictEqual(normalizeCompression(true, 'x').codec.async, null, 'synchronous unless asked');
  assert.strictEqual(normalizeCompression({ async: true }, 'x').codec.async, DEFAULT_ASYNC_THRESHOLD);
  assert.strictEqual(DEFAULT_ASYNC_THRESHOLD, 256 * 1024, 'the same default as perMessageDeflate.async');
  assert.strictEqual(normalizeCompression({ async: { threshold: 4096 } }, 'x').codec.async, 4096);
  for (const off of [undefined, null, false]) assert.strictEqual(normalizeAsync(off, 'x'), null);
  assert.throws(() => normalizeCompression({ async: 'yes' }, 'x'), /async must be true, false or \{ threshold \}/);
  assert.throws(() => normalizeCompression({ async: { threshold: 0 } }, 'x'), /async\.threshold must be a positive/);
  assert.throws(() => normalizeCompression({ async: { threshold: 1.5 } }, 'x'), /async\.threshold/);
  // An injected codec decides that for itself: the option is refused, not silently dropped.
  const codec = { id: 'mine', encode: (b) => b, decode: (b) => b };
  assert.throws(() => normalizeCompression({ codec, async: true }, 'x'), /applies to the platform codec/);
  assert.strictEqual(normalizeCompression({ codec, async: false }, 'x').codec, codec, 'off is not a request');
});

test('native (node): with async, a message under the threshold deflates on the loop and one past it on the threadpool', async () => {
  const codec = nativeCompressor({ async: 2048 });
  assert.strictEqual(codec.async, 2048);
  const small = text(20);
  const large = text(600);
  assert.ok(small.length < 2048 && large.length >= 2048, `${small.length} / ${large.length}`);
  const sync = codec.encode(small);
  assert.ok(sync instanceof Uint8Array, 'synchronous under the threshold');
  const pending = codec.encode(large);
  assert.strictEqual(typeof pending.then, 'function', 'a promise past it');
  const encoded = await pending;
  assert.deepStrictEqual(zlib.inflateRawSync(encoded), Buffer.from(large), 'the same raw deflate zlib reads');
  // decode never goes to the threadpool: an inflate is cheaper than the hand-off at every size.
  assert.deepStrictEqual(new Uint8Array(codec.decode(encoded, large.length)), large);
  assert.deepStrictEqual(new Uint8Array(codec.decode(sync, small.length)), small);
  assert.strictEqual(nativeCompressor().async, null);
});

test('normalizeSyncCompression: the Node↔Node carriers refuse a codec that declares async, and one that answers a promise', () => {
  assert.strictEqual(normalizeSyncCompression(true, 'x').codec.async, null);
  assert.throws(
    () => normalizeSyncCompression({ async: true }, 'x'),
    /declares async — this carrier has no ordering queue/,
  );
  const hybrid = nativeCompressor({ async: 1 });
  assert.throws(() => normalizeSyncCompression({ codec: hybrid }, 'x'), /declares async/);
  const promising = { id: 'p', encode: async (b) => b, decode: (b) => b };
  assert.throws(() => normalizeSyncCompression({ codec: promising }, 'x'), /must answer synchronously/);
  assert.strictEqual(normalizeSyncCompression(false, 'x'), null);
});

const fake = (id, extra = {}) => ({ id, encode: (b) => b, decode: (b) => b, ...extra });

test('normalizeCompression: `codec` is one codec or a list of them, names and injected alike, in order', () => {
  const mine = fake('mine', { threshold: 7 });
  const local = normalizeCompression({ codec: ['brotli', mine, 'deflate-raw'] }, 'x');
  assert.deepStrictEqual(local.ids, ['brotli', 'mine', 'deflate-raw']);
  assert.strictEqual(Object.isFrozen(local) && Object.isFrozen(local.codecs) && Object.isFrozen(local.ids), true);
  assert.strictEqual(local.id, 'brotli', 'the head is what a carrier with no negotiation encodes with');
  assert.strictEqual(local.codec, local.codecs[0].codec);
  assert.deepStrictEqual(
    local.codecs.map((entry) => entry.threshold),
    [1024, 7, 1024],
    'each codec its own threshold',
  );
  const flat = normalizeCompression({ codec: ['brotli', mine], threshold: 32 }, 'x');
  assert.deepStrictEqual(
    flat.codecs.map((entry) => entry.threshold),
    [32, 32],
    'an explicit threshold is every codec’s',
  );
  assert.deepStrictEqual(normalizeCompression(true, 'x').ids, ['deflate-raw'], 'one codec is a list of one');
  assert.throws(() => normalizeCompression({ codec: [] }, 'x'), /must not be an empty list/);
  assert.throws(() => normalizeCompression({ codec: ['brotli', 'brotli'] }, 'x'), /names brotli twice/);
  assert.throws(() => normalizeCompression({ codec: [mine, fake('mine')] }, 'x'), /names mine twice/);
  assert.throws(() => normalizeCompression({ codec: ['brotli', {}] }, 'x'), /compression\.codec must provide an id/);
  assert.throws(() => normalizeCompression({ codec: ['lz4', 'brotli'] }, 'x'), /unknown algorithm "lz4"/);
  assert.throws(() => normalizeCompression({ codec: fake('a,b') }, 'x'), /must not contain a comma or whitespace/);
  assert.throws(() => normalizeCompression({ codec: fake('a b') }, 'x'), /comma or whitespace/);
  // async shapes the named codecs of a list and leaves the injected ones be.
  const mixed = normalizeCompression({ codec: [mine, 'deflate-raw'], async: true }, 'x');
  assert.strictEqual(mixed.codecs[1].codec.async, DEFAULT_ASYNC_THRESHOLD);
  assert.throws(() => normalizeCompression({ codec: [mine], async: true }, 'x'), /applies to the platform codec/);
});

test('normalizeSyncCompression: every codec of the list is probed, not only the head', () => {
  const promising = { id: 'p', encode: async (b) => b, decode: (b) => b };
  assert.throws(
    () => normalizeSyncCompression({ codec: ['deflate-raw', promising] }, 'x'),
    /must answer synchronously/,
  );
  const hybrid = nativeCompressor({ async: 1 });
  assert.throws(() => normalizeSyncCompression({ codec: ['brotli', hybrid] }, 'x'), /declares async/);
  assert.deepStrictEqual(normalizeSyncCompression({ codec: ['brotli', 'deflate-raw'] }, 'x').ids, [
    'brotli',
    'deflate-raw',
  ]);
});

test('negotiate: each side sends with the first of ITS list the peer announced', () => {
  const a = normalizeCompression({ codec: [fake('zstd'), fake('brotli'), fake('deflate-raw')] }, 'a');
  const b = normalizeCompression({ codec: [fake('deflate-raw'), fake('brotli')] }, 'b');
  const atA = negotiate(a, b.ids);
  const atB = negotiate(b, a.ids);
  assert.strictEqual(atA.encode.id, 'brotli', 'A’s zstd is not on B’s list; brotli is its next');
  assert.strictEqual(atB.encode.id, 'deflate-raw', 'B’s own first choice, which A holds');
  // The two directions choose independently, and each end knows the other's choice.
  assert.strictEqual(atA.decode.id, atB.encode.id);
  assert.strictEqual(atB.decode.id, atA.encode.id);
  assert.strictEqual(atA.encode, a.codecs[1], 'an entry of the local list, threshold and all');
  // One id is a list of one: a peer of the single-codec days.
  const single = negotiate(a, 'deflate-raw');
  assert.deepStrictEqual([single.encode.id, single.decode.id], ['deflate-raw', 'deflate-raw']);
  // Nothing in common, nothing announced, nothing configured: plain.
  assert.strictEqual(negotiate(a, ['lz4']), null);
  assert.strictEqual(negotiate(a, []), null);
  assert.strictEqual(negotiate(a, undefined), null);
  assert.strictEqual(negotiate(a, true), null, 'a boolean names nothing');
  assert.strictEqual(negotiate(a, { length: 1, 0: 'zstd' }), null, 'an array-like is not a list');
  assert.strictEqual(negotiate(null, ['deflate-raw']), null);
});

test('negotiate: a peer’s list is peer-controlled — junk is skipped, the length is bounded, ids are only compared', () => {
  const local = normalizeCompression({ codec: [fake('zstd'), fake('deflate-raw')] }, 'x');
  const junk = negotiate(local, [1, null, {}, ['zstd'], 'deflate-raw']);
  assert.deepStrictEqual([junk.encode.id, junk.decode.id], ['deflate-raw', 'deflate-raw']);
  for (const id of ['__proto__', 'constructor', 'toString', 'hasOwnProperty']) {
    assert.strictEqual(negotiate(local, [id]), null, `${id} names no codec`);
  }
  // Only the first 16 entries are looked at, whatever the peer sent.
  const flood = Array.from({ length: 1000 }, (_, i) => `x${i}`);
  assert.strictEqual(negotiate(local, [...flood, 'zstd']), null);
  const near = [...flood.slice(0, 15), 'zstd'];
  assert.strictEqual(negotiate(local, near).encode.id, 'zstd');
});

test('native (node): raw deflate through zlib, and the cap bounds an inflate', () => {
  const codec = nativeCompressor();
  const input = text(200);
  const encoded = codec.encode(input);
  assert.ok(encoded.length < input.length / 4, `${input.length} -> ${encoded.length}`);
  assert.deepStrictEqual(new Uint8Array(codec.decode(encoded, input.length)), input);
  assert.throws(() => codec.decode(encoded, 64), /ERR_BUFFER_TOO_LARGE|maxOutputLength|exceed/i);
});

test('native (browser): CompressionStream round trip, interop with zlib both ways, and the cap', async (t) => {
  const codec = browser.nativeCompressor();
  if (codec === null) return void t.skip('no CompressionStream here');
  try {
    assert.ok(new CompressionStream('deflate-raw'));
  } catch {
    return void t.skip('deflate-raw is not a format here');
  }
  assert.strictEqual(codec.id, 'deflate-raw', 'the same id as the node half, so the two negotiate');
  assert.strictEqual(codec.threshold, 4096);
  const input = text(300);
  const encoded = await codec.encode(input);
  assert.ok(encoded instanceof Uint8Array);
  assert.ok(encoded.length < input.length / 4);
  assert.deepStrictEqual(new Uint8Array(await codec.decode(encoded, input.length)), input);
  // What a browser produces, zlib reads; what zlib produces, a browser reads.
  assert.deepStrictEqual(new Uint8Array(zlib.inflateRawSync(encoded)), input);
  const fromZlib = zlib.deflateRawSync(input);
  assert.deepStrictEqual(new Uint8Array(await codec.decode(fromZlib, input.length)), input);
  await assert.rejects(codec.decode(encoded, 64), /exceeds the cap/);
  // Larger than one transform chunk: the collected output is joined once.
  const big = new Uint8Array(300_000).map((_, i) => i % 251);
  const bigEncoded = await codec.encode(big);
  assert.deepStrictEqual(new Uint8Array(await codec.decode(bigEncoded, big.length)), big);
});

test('Sequencer: a plain push with nothing in flight delivers synchronously', () => {
  const queue = new Sequencer();
  const seen = [];
  queue.push('a', (v) => seen.push(v));
  assert.deepStrictEqual(seen, ['a']);
  assert.strictEqual(queue.pending, 0);
});

test('Sequencer: everything behind a promise waits its turn, plain values included', async () => {
  const queue = new Sequencer();
  const seen = [];
  let release;
  const slow = new Promise((resolve) => {
    release = resolve;
  });
  queue.push(slow, (v) => seen.push(v));
  queue.push('b', (v) => seen.push(v));
  queue.push(Promise.resolve('c'), (v) => seen.push(v));
  queue.push('d', (v) => seen.push(v));
  assert.deepStrictEqual(seen, [], 'nothing overtakes the promise');
  assert.strictEqual(queue.pending, 4);
  release('a');
  await timers.setImmediate();
  assert.deepStrictEqual(seen, ['a', 'b', 'c', 'd']);
  assert.strictEqual(queue.pending, 0);
  // Drained: back to the synchronous path.
  queue.push('e', (v) => seen.push(v));
  assert.deepStrictEqual(seen, ['a', 'b', 'c', 'd', 'e']);
});

test('Sequencer: a rejected promise runs recover in its slot, and the queue goes on', async () => {
  const queue = new Sequencer();
  const seen = [];
  queue.push(
    Promise.reject(new Error('boom')),
    (v) => seen.push(v),
    (error) => seen.push(`recovered:${error.message}`),
  );
  queue.push('next', (v) => seen.push(v));
  await timers.setImmediate();
  assert.deepStrictEqual(seen, ['recovered:boom', 'next']);
  assert.strictEqual(queue.pending, 0);
});

test('Sequencer: a deliver that throws is reported, not swallowed, and does not stall the queue', async () => {
  const errors = [];
  const queue = new Sequencer((error) => errors.push(error.message));
  const seen = [];
  queue.push(Promise.resolve('a'), () => {
    throw new Error('deliver failed');
  });
  queue.push('b', (v) => seen.push(v));
  await timers.setImmediate();
  assert.deepStrictEqual(errors, ['deliver failed']);
  assert.deepStrictEqual(seen, ['b']);
  assert.strictEqual(queue.pending, 0);
});
