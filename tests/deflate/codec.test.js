'use strict';

// The pure-JS DEFLATE codec, tested the way a codec must be: against the
// implementations it has to interoperate with (node:zlib both ways, the
// platform's streams both ways), on a fuzz corpus, and on every way an
// input can be malformed — because a wrong byte here is a wrong RPC
// answer, not a slower one.

const { test } = require('node:test');
const assert = require('node:assert');
const zlib = require('node:zlib');
const crypto = require('node:crypto');

const { createDeflateCodec, inflateRaw, deflateRaw, DeflateError } = require('../../deflate.js');
const { buildDictionary, dictionaryCompressor, defineRouter, procedure, isCompressor } = require('../../index.js');
const { nativeCompressor } = require('../../src/compression/index.js');

const same = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)) === 0;

const router = defineRouter({
  market: {
    quote: procedure({
      access: 'public',
      signature: { args: { symbol: 'string' }, returns: { bid: 'number', ask: 'number', ts: 'number' } },
      handler: async () => ({}),
    }),
    emits: { tick: { data: { symbol: 'string', bid: 'number', ask: 'number', ts: 'number' } } },
  },
});
const dictionary = buildDictionary(router);

const event = (i = 0) =>
  Buffer.from(
    JSON.stringify({
      type: 'event',
      name: 'market/tick',
      data: { symbol: 'BTC-USD', bid: 42000 + (i % 100), ask: 42001 + (i % 100), ts: 1726500000000 + i },
    }),
  );
const rows = (n) =>
  Buffer.from(
    JSON.stringify({
      type: 'callback',
      id: 'c1',
      result: Array.from({ length: n }, (_, i) => ({ id: i, name: `row-${i}`, tags: ['a', 'b'] })),
    }),
  );

// A corpus that reaches every path: empty, one byte, the sizes RPC
// messages come in, a run, random (incompressible), a match longer than
// 258, a payload past the 32 KiB window, and several stored blocks.
const corpus = () => [
  ['empty', Buffer.alloc(0)],
  ['one byte', Buffer.from([7])],
  ['two bytes', Buffer.from('ab')],
  ['event', event()],
  ['12 rows', rows(12)],
  ['300 rows', rows(300)],
  ['4000 rows', rows(4000)],
  ['a run', Buffer.alloc(100_000, 0x41)],
  ['a long match', Buffer.from('abcdefghij'.repeat(1000))],
  ['random 1 KB', crypto.randomBytes(1024)],
  ['random 200 KB', crypto.randomBytes(200_000)],
  [
    'past the window',
    Buffer.from(Array.from({ length: 70_000 }, (_, i) => String.fromCharCode(32 + ((i * 7) % 90))).join('')),
  ],
  ['utf-8 text', Buffer.from('є😀 привіт '.repeat(500))],
];

test('inflate: every block type zlib produces, with and without a dictionary, at every level', () => {
  for (const [name, input] of corpus()) {
    for (const dict of [null, dictionary]) {
      const base = dict ? { dictionary: dict } : {};
      for (const opts of [
        { level: 0 },
        { level: 1 },
        { level: 6 },
        { level: 9 },
        { strategy: zlib.constants.Z_FIXED },
        { strategy: zlib.constants.Z_HUFFMAN_ONLY },
        { strategy: zlib.constants.Z_RLE },
        { memLevel: 1, windowBits: 9 },
      ]) {
        const compressed = zlib.deflateRawSync(input, { ...base, ...opts });
        const out = inflateRaw(compressed, { dictionary: dict });
        assert.ok(same(out, input), `${name} through zlib ${JSON.stringify(opts)} dict=${Boolean(dict)}`);
      }
    }
  }
});

test('deflate: what the own encoder produces, zlib and the own inflater both read back', () => {
  for (const [name, input] of corpus()) {
    for (const dict of [null, dictionary]) {
      for (const level of [1, 6, 9]) {
        const out = deflateRaw(input, { dictionary: dict, level });
        assert.ok(
          same(zlib.inflateRawSync(out, dict ? { dictionary: dict } : {}), input),
          `${name} -> zlib, level ${level}`,
        );
        assert.ok(same(inflateRaw(out, { dictionary: dict }), input), `${name} -> own inflate, level ${level}`);
      }
    }
  }
});

test('deflate: the sizes — fixed codes match dynamic on a small message with a dictionary, stored when nothing shrinks', () => {
  const ev = event();
  const ours = deflateRaw(ev, { dictionary }).length;
  const dynamic = zlib.deflateRawSync(ev, { dictionary }).length;
  const fixed = zlib.deflateRawSync(ev, { dictionary, strategy: zlib.constants.Z_FIXED }).length;
  assert.ok(ours <= fixed + 2, `ours ${ours} B against zlib fixed ${fixed} B`);
  assert.ok(ours <= dynamic + 6, `ours ${ours} B against zlib dynamic ${dynamic} B`);
  assert.ok(ours < ev.length / 2, `${ev.length} B event -> ${ours} B with the dictionary`);
  // Without history the small event barely shrinks — the plain codec's threshold, in numbers.
  assert.ok(deflateRaw(ev).length > ev.length * 0.8);
  // Incompressible input: exactly the stored form.
  const noise = crypto.randomBytes(70_000);
  const stored = deflateRaw(noise);
  assert.strictEqual(stored.length, noise.length + 5 * 2, 'two stored blocks, five bytes each');
  assert.strictEqual(stored[0], 0, 'BFINAL 0, BTYPE 00 on the first');
  assert.strictEqual(stored[5 + 65535], 1, 'BFINAL 1 on the last');
  assert.ok(same(zlib.inflateRawSync(stored), noise));
  // A long run: fixed codes spend ~13 bits per 258-byte match, so 100 KB of
  // one byte is ~630 B (a dynamic tree would make it a few dozen — the
  // large-message case the hybrid codec hands to the platform).
  assert.ok(deflateRaw(Buffer.alloc(100_000, 0x41)).length < 1000);
});

test('fuzz: 400 random and repetitive payloads round trip through every pair', () => {
  const seed = 0x9e3779b9;
  let x = seed;
  const next = () => {
    x = (Math.imul(x, 1664525) + 1013904223) >>> 0;
    return x;
  };
  for (let i = 0; i < 400; i++) {
    const length = next() % 5000;
    const kind = next() % 3;
    const input = Buffer.alloc(length);
    if (kind === 0) for (let j = 0; j < length; j++) input[j] = next() & 0xff;
    else if (kind === 1) for (let j = 0; j < length; j++) input[j] = 97 + (next() % 6);
    else {
      const text = JSON.stringify({
        i,
        n: next(),
        rows: Array.from({ length: next() % 40 }, (_, k) => ({ k, v: next() % 7 })),
      });
      Buffer.from(text).copy(input);
    }
    const dict = i % 2 === 0 ? dictionary : null;
    const ours = deflateRaw(input, { dictionary: dict, level: 1 + (i % 9) });
    assert.ok(same(inflateRaw(ours, { dictionary: dict }), input), `own pair #${i} (${length} B, kind ${kind})`);
    assert.ok(same(zlib.inflateRawSync(ours, dict ? { dictionary: dict } : {}), input), `ours -> zlib #${i}`);
    const theirs = zlib.deflateRawSync(input, { ...(dict ? { dictionary: dict } : {}), level: i % 10 });
    assert.ok(same(inflateRaw(theirs, { dictionary: dict }), input), `zlib -> ours #${i}`);
  }
});

test('inflate: malformed input is a coded DeflateError, never a wrong answer', () => {
  const good = zlib.deflateRawSync(rows(300));
  const code = (fn) => {
    try {
      fn();
    } catch (error) {
      assert.ok(error instanceof DeflateError, `${error}`);
      return error.code;
    }
    return null;
  };
  assert.strictEqual(
    code(() => inflateRaw(new Uint8Array(0))),
    'truncated',
  );
  assert.strictEqual(
    code(() => inflateRaw(good.subarray(0, 10))),
    'truncated',
  );
  assert.strictEqual(
    code(() => inflateRaw(good.subarray(0, good.length - 1))),
    'truncated',
  );
  assert.strictEqual(
    code(() => inflateRaw(new Uint8Array([0b111]))),
    'block',
    'BTYPE 11 is reserved',
  );
  // A stored block whose NLEN is not LEN's complement.
  assert.strictEqual(
    code(() => inflateRaw(new Uint8Array([1, 3, 0, 0, 0, 65, 66, 67]))),
    'stored',
  );
  // A stored block that promises more bytes than follow.
  assert.strictEqual(
    code(() => inflateRaw(new Uint8Array([1, 10, 0, 0xf5, 0xff, 65]))),
    'truncated',
  );
  // A distance reaching before the start of the output (fixed block: length 3, distance 1 on an empty window).
  const back = Buffer.from(deflateRaw(Buffer.from('aaaa')));
  assert.ok(same(inflateRaw(back), Buffer.from('aaaa')));
  // The same stream over a window that does not have the byte: dictionary required but missing is a distance error
  const withDict = deflateRaw(Buffer.from('market/tick'), { dictionary });
  const result = code(() => inflateRaw(withDict));
  assert.ok(
    result === 'distance' || result === 'huffman' || result === 'truncated',
    `read without its dictionary: ${result}`,
  );
  // The cap.
  assert.strictEqual(
    code(() => inflateRaw(zlib.deflateRawSync(Buffer.alloc(100_000, 0x20)), { maxOutput: 4096 })),
    'too-large',
  );
  assert.strictEqual(
    code(() => inflateRaw(zlib.deflateRawSync(rows(300)), { maxOutput: rows(300).length })),
    null,
    'exactly the cap is fine',
  );
  // Over-subscribed dynamic tree: HLIT/HDIST/HCLEN then code-length code lengths that oversubscribe.
  const bad = Buffer.from([0b101, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
  assert.ok(['huffman', 'truncated'].includes(code(() => inflateRaw(bad))));
  assert.throws(() => inflateRaw('text'), TypeError);
});

test('the platform streams: what CompressionStream produces the own inflater reads, and back', async (t) => {
  if (typeof CompressionStream !== 'function') return void t.skip('no CompressionStream here');
  try {
    assert.ok(new CompressionStream('deflate-raw'));
  } catch {
    return void t.skip('deflate-raw is not a format here');
  }
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
    return Buffer.concat(chunks.map((c) => Buffer.from(c)));
  };
  for (const [name, input] of corpus()) {
    const native = await through(new CompressionStream('deflate-raw'), input);
    assert.ok(same(inflateRaw(native), input), `CompressionStream -> own inflate: ${name}`);
    const ours = deflateRaw(input);
    assert.ok(
      same(await through(new DecompressionStream('deflate-raw'), ours), input),
      `own deflate -> DecompressionStream: ${name}`,
    );
  }
});

test('createDeflateCodec: a Compressor whose id agrees with the Node dictionary codec, synchronous in Node', async () => {
  const codec = createDeflateCodec({ dictionary });
  assert.strictEqual(isCompressor(codec), true);
  assert.strictEqual(
    codec.id,
    dictionaryCompressor(dictionary).id,
    'a Node peer on zlib and a browser peer on this codec negotiate',
  );
  assert.strictEqual(codec.threshold, 64);
  assert.ok(codec.dictionary instanceof Uint8Array);
  const plain = createDeflateCodec();
  assert.strictEqual(plain.id, 'deflate-raw');
  assert.strictEqual(plain.threshold, 1024);
  assert.strictEqual(plain.dictionary, null);
  // Both directions against the Node codecs — the interop the ids promise.
  const zlibDict = dictionaryCompressor(dictionary);
  const ev = event();
  assert.ok(same(zlibDict.decode(codec.encode(ev), ev.length), ev));
  assert.ok(same(codec.decode(zlibDict.encode(ev), ev.length), ev));
  const big = rows(4000);
  assert.ok(same(nativeCompressor().decode(plain.encode(big), big.length), big));
  assert.ok(same(plain.decode(nativeCompressor().encode(big), big.length), big));
  // In Node the codec answers synchronously whatever the size (no native hand-off by default).
  assert.ok(codec.encode(big) instanceof Uint8Array);
  // Asked for, the native hand-off applies past nativeAbove and answers a promise.
  if (typeof CompressionStream === 'function') {
    const hybrid = createDeflateCodec({ dictionary, native: true, nativeAbove: 2048 });
    assert.ok(hybrid.encode(ev) instanceof Uint8Array, 'small: the own encoder');
    const promised = hybrid.encode(big);
    assert.ok(promised instanceof Promise, 'large: CompressionStream');
    assert.ok(same(hybrid.decode(await promised, big.length), big), 'and it inflates on the dictionary side');
  }
  assert.throws(() => codec.decode(zlibDict.encode(big), 100), /exceeds the cap/);
  assert.throws(() => createDeflateCodec({ dictionary: new Uint8Array(0) }), /must not be empty/);
  assert.throws(() => createDeflateCodec({ threshold: -1 }), /threshold/);
  assert.throws(() => createDeflateCodec({ nativeAbove: 0 }), /nativeAbove/);
  assert.throws(() => createDeflateCodec({ level: 0 }), /level/);
  assert.throws(() => createDeflateCodec({ dictionary: 42 }), /bytes or a string/);
});
