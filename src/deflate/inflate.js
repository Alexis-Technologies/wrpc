'use strict';

// RFC 1951 inflate in plain JavaScript — the half of @alexify/wrpc/deflate
// that has to be complete: whatever sits on the other end (node:zlib, a
// browser's CompressionStream, this codec) picks its own block types, so
// stored, fixed and dynamic Huffman blocks are all read here. What the
// platform's DecompressionStream cannot do is the reason this exists — take
// a preset dictionary, and answer synchronously.
//
// Decoding tables are built per tree, sized 1 << (longest code) and indexed
// by the bit-reversed code, so a symbol is one table read on the peeked
// bits rather than a bit-by-bit walk; the fixed tables are built once. The
// output buffer starts with the dictionary's tail in it, so a distance
// that reaches back into the dictionary is an ordinary copy, and grows by
// doubling up to `maxOutput` — the cap that bounds a compression bomb to
// the size a plain message is bounded at. Browser-budgeted: manual loops,
// typed arrays, no closures on the hot path.

class DeflateError extends Error {
  constructor(message, code) {
    super(message);
    this.name = 'DeflateError';
    this.code = code;
  }
}

const WINDOW = 32768;
const MAX_BITS = 15;

// Length codes 257..285: base length and extra bits (RFC 1951 3.2.5).
const LENGTH_BASE = new Uint16Array([
  3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258,
]);
const LENGTH_EXTRA = new Uint8Array([
  0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
]);
// Distance codes 0..29.
const DIST_BASE = new Uint16Array([
  1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145,
  8193, 12289, 16385, 24577,
]);
const DIST_EXTRA = new Uint8Array([
  0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13,
]);
// The order code-length code lengths are transmitted in.
const CODE_LENGTH_ORDER = new Uint8Array([16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15]);

/**
 * A decoding table over canonical code lengths: entry = (length << 16) |
 * symbol at every index whose low `length` bits are the bit-reversed code,
 * or 0 where no code lands. Over-subscribed lengths are refused;
 * incomplete sets are allowed (a single-code distance tree is legal).
 */
const buildTable = (lengths, count) => {
  let maxLen = 0;
  for (let i = 0; i < count; i++) if (lengths[i] > maxLen) maxLen = lengths[i];
  if (maxLen === 0) return { table: null, maxLen: 0 };
  const blCount = new Uint16Array(MAX_BITS + 1);
  for (let i = 0; i < count; i++) blCount[lengths[i]]++;
  blCount[0] = 0;
  let left = 1;
  for (let len = 1; len <= MAX_BITS; len++) {
    left = (left << 1) - blCount[len];
    if (left < 0) throw new DeflateError('over-subscribed Huffman code', 'huffman');
  }
  const nextCode = new Uint16Array(MAX_BITS + 2);
  let code = 0;
  for (let len = 1; len <= MAX_BITS; len++) {
    code = (code + blCount[len - 1]) << 1;
    nextCode[len] = code;
  }
  const size = 1 << maxLen;
  const table = new Uint32Array(size);
  for (let sym = 0; sym < count; sym++) {
    const len = lengths[sym];
    if (len === 0) continue;
    let c = nextCode[len]++;
    let reversed = 0;
    for (let i = 0; i < len; i++) {
      reversed = (reversed << 1) | (c & 1);
      c >>= 1;
    }
    const entry = (len << 16) | sym;
    const step = 1 << len;
    for (let j = reversed; j < size; j += step) table[j] = entry;
  }
  return { table, maxLen };
};

// The fixed trees (RFC 1951 3.2.6), built once.
const FIXED_LIT = (() => {
  const lengths = new Uint8Array(288);
  let i = 0;
  for (; i < 144; i++) lengths[i] = 8;
  for (; i < 256; i++) lengths[i] = 9;
  for (; i < 280; i++) lengths[i] = 7;
  for (; i < 288; i++) lengths[i] = 8;
  return buildTable(lengths, 288);
})();
const FIXED_DIST = (() => {
  const lengths = new Uint8Array(30);
  lengths.fill(5);
  return buildTable(lengths, 30);
})();

class Inflater {
  #input;
  #pos = 0;
  #bitBuf = 0;
  #bitCnt = 0;
  #out;
  #outLen;
  #start; // where the message begins in #out (after the dictionary)
  #limit; // #start + maxOutput

  constructor(input, dictionary, maxOutput) {
    this.#input = input;
    const dictLen = dictionary === null ? 0 : Math.min(dictionary.length, WINDOW);
    // Sized for the common case — a small message that inflates to a few
    // times its input — and doubled as needed, never past the cap.
    const guess = Math.min(maxOutput, Math.max(input.length * 4, 256));
    this.#out = new Uint8Array(dictLen + guess);
    if (dictLen > 0) this.#out.set(dictionary.subarray(dictionary.length - dictLen), 0);
    this.#outLen = dictLen;
    this.#start = dictLen;
    this.#limit = dictLen + maxOutput;
  }

  // ---- bits

  #fill(n) {
    const input = this.#input;
    while (this.#bitCnt < n && this.#pos < input.length) {
      this.#bitBuf |= input[this.#pos++] << this.#bitCnt;
      this.#bitCnt += 8;
    }
  }

  #bits(n) {
    if (this.#bitCnt < n) {
      this.#fill(n);
      if (this.#bitCnt < n) throw new DeflateError('unexpected end of data', 'truncated');
    }
    const value = this.#bitBuf & ((1 << n) - 1);
    this.#bitBuf >>>= n;
    this.#bitCnt -= n;
    return value;
  }

  // One symbol from a table: peek the longest code's worth of bits (zero
  // padded at the very end of the input), read the entry, consume its
  // length — which has to fit in what was actually there.
  #symbol(tree) {
    const { table, maxLen } = tree;
    if (table === null) throw new DeflateError('symbol from an empty code', 'huffman');
    if (this.#bitCnt < maxLen) this.#fill(maxLen);
    const entry = table[this.#bitBuf & ((1 << maxLen) - 1)];
    if (entry === 0) throw new DeflateError('invalid Huffman code', 'huffman');
    const len = entry >>> 16;
    if (len > this.#bitCnt) throw new DeflateError('unexpected end of data', 'truncated');
    this.#bitBuf >>>= len;
    this.#bitCnt -= len;
    return entry & 0xffff;
  }

  // ---- output

  #room(n) {
    const needed = this.#outLen + n;
    if (needed > this.#limit) throw new DeflateError('inflated message exceeds the cap', 'too-large');
    if (needed <= this.#out.length) return;
    let size = this.#out.length * 2;
    while (size < needed) size *= 2;
    if (size > this.#limit) size = this.#limit;
    const grown = new Uint8Array(size);
    grown.set(this.#out.subarray(0, this.#outLen));
    this.#out = grown;
  }

  // ---- blocks

  #stored() {
    // Drop the rest of the current byte, then LEN / NLEN as whole bytes.
    const drop = this.#bitCnt & 7;
    this.#bitBuf >>>= drop;
    this.#bitCnt -= drop;
    const len = this.#bits(16);
    const nlen = this.#bits(16);
    if ((len ^ 0xffff) !== nlen) throw new DeflateError('stored block length mismatch', 'stored');
    // Whatever whole bytes the bit buffer still holds come first.
    this.#room(len);
    let remaining = len;
    while (remaining > 0 && this.#bitCnt >= 8) {
      this.#out[this.#outLen++] = this.#bitBuf & 0xff;
      this.#bitBuf >>>= 8;
      this.#bitCnt -= 8;
      remaining--;
    }
    if (this.#pos + remaining > this.#input.length) throw new DeflateError('unexpected end of data', 'truncated');
    this.#out.set(this.#input.subarray(this.#pos, this.#pos + remaining), this.#outLen);
    this.#pos += remaining;
    this.#outLen += remaining;
  }

  #dynamicTrees() {
    const hlit = this.#bits(5) + 257;
    const hdist = this.#bits(5) + 1;
    const hclen = this.#bits(4) + 4;
    if (hlit > 286 || hdist > 30) throw new DeflateError('too many codes', 'huffman');
    const clLengths = new Uint8Array(19);
    for (let i = 0; i < hclen; i++) clLengths[CODE_LENGTH_ORDER[i]] = this.#bits(3);
    const clTree = buildTable(clLengths, 19);
    const lengths = new Uint8Array(hlit + hdist);
    let i = 0;
    while (i < hlit + hdist) {
      const sym = this.#symbol(clTree);
      if (sym < 16) {
        lengths[i++] = sym;
        continue;
      }
      let repeat;
      let value = 0;
      if (sym === 16) {
        if (i === 0) throw new DeflateError('repeat with no previous length', 'huffman');
        value = lengths[i - 1];
        repeat = 3 + this.#bits(2);
      } else if (sym === 17) {
        repeat = 3 + this.#bits(3);
      } else {
        repeat = 11 + this.#bits(7);
      }
      if (i + repeat > hlit + hdist) throw new DeflateError('too many code lengths', 'huffman');
      while (repeat-- > 0) lengths[i++] = value;
    }
    if (lengths[256] === 0) throw new DeflateError('no end-of-block code', 'huffman');
    const lit = buildTable(lengths.subarray(0, hlit), hlit);
    const dist = buildTable(lengths.subarray(hlit), hdist);
    return { lit, dist };
  }

  #codes(lit, dist) {
    for (;;) {
      const sym = this.#symbol(lit);
      if (sym < 256) {
        this.#room(1);
        this.#out[this.#outLen++] = sym;
        continue;
      }
      if (sym === 256) return;
      const li = sym - 257;
      if (li >= 29) throw new DeflateError('invalid length code', 'huffman');
      const length = LENGTH_BASE[li] + this.#bits(LENGTH_EXTRA[li]);
      const di = this.#symbol(dist);
      if (di >= 30) throw new DeflateError('invalid distance code', 'huffman');
      const distance = DIST_BASE[di] + this.#bits(DIST_EXTRA[di]);
      if (distance > this.#outLen) throw new DeflateError('distance too far back', 'distance');
      this.#room(length);
      const out = this.#out;
      let from = this.#outLen - distance;
      let to = this.#outLen;
      // Overlapping copies are the point (a run), so byte by byte.
      for (let n = 0; n < length; n++) out[to++] = out[from++];
      this.#outLen = to;
    }
  }

  run() {
    let final;
    do {
      final = this.#bits(1);
      const type = this.#bits(2);
      if (type === 0) this.#stored();
      else if (type === 1) this.#codes(FIXED_LIT, FIXED_DIST);
      else if (type === 2) {
        const { lit, dist } = this.#dynamicTrees();
        this.#codes(lit, dist);
      } else throw new DeflateError('reserved block type', 'block');
    } while (!final);
    return this.#out.subarray(this.#start, this.#outLen);
  }
}

const toBytes = (input) => {
  if (input instanceof Uint8Array) return input;
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  throw new TypeError('inflate: expected bytes');
};

/**
 * Raw DEFLATE → bytes, synchronously. `dictionary` preloads the window (its
 * last 32 KiB); `maxOutput` is the inflated-size cap — past it a
 * DeflateError('too-large') rather than a bigger allocation. Every
 * malformed input is a DeflateError with a code, never a wrong answer.
 */
const inflateRaw = (input, { dictionary = null, maxOutput = Infinity } = {}) =>
  new Inflater(toBytes(input), dictionary === null ? null : toBytes(dictionary), maxOutput).run();

module.exports = { inflateRaw, DeflateError, buildTable, LENGTH_BASE, LENGTH_EXTRA, DIST_BASE, DIST_EXTRA, WINDOW };
