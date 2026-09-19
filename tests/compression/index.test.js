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

test('negotiate: on only when the peer named the same codec', () => {
  const local = normalizeCompression(true, 'x');
  assert.strictEqual(negotiate(local, 'deflate-raw'), local);
  assert.strictEqual(negotiate(local, 'brotli'), null);
  assert.strictEqual(negotiate(local, undefined), null);
  assert.strictEqual(negotiate(local, true), null, 'a boolean names nothing');
  assert.strictEqual(negotiate(null, 'deflate-raw'), null);
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
