'use strict';

// RFC 1951 deflate in plain JavaScript, the small half of
// @alexify/wrpc/deflate: LZ77 over hash chains against a preset dictionary,
// written as ONE fixed-Huffman block. Measured before it was written
// (bench/deflate-js.js): on the messages this exists for — a few hundred
// bytes against a dictionary — fixed codes produce the same size as
// dynamic ones (58 B against 58 B on a 161 B event), because the dynamic
// tree costs more than it saves on so little input; past a couple of
// kilobytes dynamic wins by 26–39%, and that is where the codec hands the
// message to the platform's CompressionStream instead (index.js). So the
// encoder needs no tree construction at all, which is what keeps it ~200
// lines. Falls back to stored blocks when fixed codes would not shrink the
// input, as zlib does.
//
// Browser-budgeted and per-message: the hash chains are allocated per call,
// sized to the dictionary plus the input, which for a small message is a
// few kilobytes of typed arrays.

const { WINDOW, LENGTH_BASE, LENGTH_EXTRA, DIST_BASE, DIST_EXTRA } = require('./inflate.js');

const MIN_MATCH = 3;
const MAX_MATCH = 258;
const HASH_BITS = 15;
const HASH_SIZE = 1 << HASH_BITS;
const HASH_MASK = HASH_SIZE - 1;
const STORED_MAX = 65535;
// How far down a hash chain a match is looked for, by level: 1 is a quick
// pass, 9 exhaustive, the default what pays on wrpc-sized messages.
const CHAIN_BY_LEVEL = [4, 4, 8, 16, 24, 32, 32, 64, 128, 256];

const reverse = (code, len) => {
  let r = 0;
  for (let i = 0; i < len; i++) {
    r = (r << 1) | (code & 1);
    code >>= 1;
  }
  return r;
};

// The fixed literal/length codes, bit-reversed for LSB-first writing:
// entry = (reversed code << 4) | length.
const LIT_CODES = (() => {
  const codes = new Uint32Array(288);
  for (let sym = 0; sym < 288; sym++) {
    let code;
    let len;
    if (sym < 144) {
      code = 0x30 + sym;
      len = 8;
    } else if (sym < 256) {
      code = 0x190 + (sym - 144);
      len = 9;
    } else if (sym < 280) {
      code = sym - 256;
      len = 7;
    } else {
      code = 0xc0 + (sym - 280);
      len = 8;
    }
    codes[sym] = (reverse(code, len) << 4) | len;
  }
  return codes;
})();
const DIST_CODES = (() => {
  const codes = new Uint32Array(30);
  for (let sym = 0; sym < 30; sym++) codes[sym] = (reverse(sym, 5) << 4) | 5;
  return codes;
})();

// length (3..258) -> code index 0..28, distance (1..32768) -> code 0..29:
// direct tables, built once, so emitting a match is two reads.
const LENGTH_CODE = (() => {
  const table = new Uint8Array(MAX_MATCH + 1);
  for (let i = 0; i < 29; i++) {
    const base = LENGTH_BASE[i];
    const next = i === 28 ? MAX_MATCH + 1 : LENGTH_BASE[i + 1];
    for (let len = base; len < next; len++) table[len] = i;
  }
  table[MAX_MATCH] = 28;
  return table;
})();
const distCode = (distance) => {
  // 30 codes: a short scan from the top is cheaper than a 32 KiB table.
  let i = 29;
  while (DIST_BASE[i] > distance) i--;
  return i;
};

class BitWriter {
  #out;
  #len = 0;
  #buf = 0;
  #cnt = 0;

  constructor(size) {
    this.#out = new Uint8Array(size);
  }

  write(value, bits) {
    this.#buf |= value << this.#cnt;
    this.#cnt += bits;
    while (this.#cnt >= 8) {
      if (this.#len === this.#out.length) this.#grow();
      this.#out[this.#len++] = this.#buf & 0xff;
      this.#buf >>>= 8;
      this.#cnt -= 8;
    }
  }

  #grow() {
    const grown = new Uint8Array(this.#out.length * 2);
    grown.set(this.#out);
    this.#out = grown;
  }

  finish() {
    if (this.#cnt > 0) {
      if (this.#len === this.#out.length) this.#grow();
      this.#out[this.#len++] = this.#buf & 0xff;
    }
    return this.#out.subarray(0, this.#len);
  }

  get length() {
    return this.#len;
  }
}

// Stored blocks: the honest fallback for input fixed codes would not shrink.
const stored = (input) => {
  const blocks = Math.max(1, Math.ceil(input.length / STORED_MAX));
  const out = new Uint8Array(input.length + blocks * 5);
  let offset = 0;
  let pos = 0;
  for (let b = 0; b < blocks; b++) {
    const len = Math.min(STORED_MAX, input.length - pos);
    out[offset++] = b === blocks - 1 ? 1 : 0;
    out[offset++] = len & 0xff;
    out[offset++] = len >>> 8;
    out[offset++] = ~len & 0xff;
    out[offset++] = (~len >>> 8) & 0xff;
    out.set(input.subarray(pos, pos + len), offset);
    offset += len;
    pos += len;
  }
  return out;
};

const hashAt = (win, i) => ((win[i] << 10) ^ (win[i + 1] << 5) ^ win[i + 2]) & HASH_MASK;

const toBytes = (input) => {
  if (input instanceof Uint8Array) return input;
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  throw new TypeError('deflate: expected bytes');
};

/**
 * Bytes → raw DEFLATE, synchronously, as one fixed-Huffman block (or
 * stored blocks when that is smaller). `dictionary` preloads the window
 * (its last 32 KiB) exactly as zlib's `dictionary` option does, so the
 * output inflates on any inflater given the same bytes; `level` 1–9 is the
 * hash-chain depth.
 */
const deflateRaw = (input, { dictionary = null, level = 6 } = {}) => {
  const data = toBytes(input);
  const dict = dictionary === null ? null : toBytes(dictionary);
  const dictLen = dict === null ? 0 : Math.min(dict.length, WINDOW);
  const total = dictLen + data.length;
  const win = new Uint8Array(total);
  if (dictLen > 0) win.set(dict.subarray(dict.length - dictLen), 0);
  win.set(data, dictLen);
  const chain = CHAIN_BY_LEVEL[Math.max(1, Math.min(9, level | 0))];
  const head = new Int32Array(HASH_SIZE).fill(-1);
  const prev = new Int32Array(total);
  const writer = new BitWriter(Math.max(32, data.length >>> 1));
  // BFINAL = 1, BTYPE = 01 (fixed Huffman).
  writer.write(1, 1);
  writer.write(1, 2);
  // The dictionary's positions go into the chains first, so a match may
  // reach back into it from the first input byte.
  const insert = (i) => {
    const h = hashAt(win, i);
    prev[i] = head[h];
    head[h] = i;
  };
  for (let i = 0; i + MIN_MATCH <= dictLen; i++) insert(i);
  let pos = dictLen;
  const end = total;
  while (pos < end) {
    let bestLen = 0;
    let bestDist = 0;
    if (pos + MIN_MATCH <= end) {
      const h = hashAt(win, pos);
      let cand = head[h];
      let depth = chain;
      const maxLen = Math.min(MAX_MATCH, end - pos);
      while (cand >= 0 && depth-- > 0) {
        const dist = pos - cand;
        if (dist > WINDOW) break;
        if (win[cand + bestLen] === win[pos + bestLen] && win[cand] === win[pos]) {
          let len = 0;
          while (len < maxLen && win[cand + len] === win[pos + len]) len++;
          if (len > bestLen) {
            bestLen = len;
            bestDist = dist;
            if (len === maxLen) break;
          }
        }
        cand = prev[cand];
      }
      prev[pos] = head[h];
      head[h] = pos;
    }
    if (bestLen >= MIN_MATCH) {
      const li = LENGTH_CODE[bestLen];
      const lit = LIT_CODES[257 + li];
      writer.write(lit >>> 4, lit & 15);
      if (LENGTH_EXTRA[li] > 0) writer.write(bestLen - LENGTH_BASE[li], LENGTH_EXTRA[li]);
      const di = distCode(bestDist);
      const dc = DIST_CODES[di];
      writer.write(dc >>> 4, 5);
      if (DIST_EXTRA[di] > 0) writer.write(bestDist - DIST_BASE[di], DIST_EXTRA[di]);
      // The bytes inside the match join the chains too, or a later match
      // could not start on them.
      for (let i = 1; i < bestLen && pos + i + MIN_MATCH <= end; i++) insert(pos + i);
      pos += bestLen;
    } else {
      const lit = LIT_CODES[win[pos]];
      writer.write(lit >>> 4, lit & 15);
      pos++;
    }
  }
  const eob = LIT_CODES[256];
  writer.write(eob >>> 4, eob & 15);
  const out = writer.finish();
  // Fixed codes did not pay: stored blocks are exactly the input plus five
  // bytes per 64 KiB.
  return out.length >= data.length + 5 * Math.max(1, Math.ceil(data.length / STORED_MAX)) ? stored(data) : out;
};

module.exports = { deflateRaw };
