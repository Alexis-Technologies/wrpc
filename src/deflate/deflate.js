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
// Browser-budgeted. `deflateRaw` is the one-shot form: it allocates its
// window and hash chains per call and hashes the dictionary again each
// time, so its cost grows with the dictionary, not only with the message.
// A codec does that work ONCE — `createDeflater` below keeps the window,
// the dictionary's chains and a scratch region, and a message costs what
// the message costs (bench/deflate-js.js, the "codec" rows).

const { WINDOW, LENGTH_BASE, LENGTH_EXTRA, DIST_BASE, DIST_EXTRA, toBytes } = require('./inflate.js');

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

// The dictionary's positions into the chains, so a match may reach back
// into it from the first input byte. The last two are left out — hashing
// them needs the bytes that follow, which are the message's — and that is
// what makes the result a property of the dictionary alone.
const hashDictionary = (win, head, prev, dictLen) => {
  for (let i = 0; i + MIN_MATCH <= dictLen; i++) {
    const h = hashAt(win, i);
    prev[i] = head[h];
    head[h] = i;
  }
};

// The LZ77 pass and the fixed-Huffman emission over `win[dictLen, end)`,
// with the dictionary's chains already in `head` and `prev`. ONE algorithm
// for both ways of preparing that state: per call (deflateRaw) and once per
// codec (createDeflater), which is why the two produce the same bytes.
// `touched`, when given, takes every hash slot this pass writes, so the
// caller can put `head` back; the number written is the pass's answer in
// `state.dirty`.
//
// What a prepared state relies on: `prev[pos]` for a message position is
// written before it is read (a position enters a chain only when it is
// inserted), and `win` is never read at or past `end` (`pos + MIN_MATCH <=
// end` guards the hash, `maxLen` the compare) — so nothing a previous
// message left behind is ever seen.
const pass = (state, dictLen, end, chain) => {
  const { win, head, prev, touched } = state;
  const writer = new BitWriter(Math.max(32, (end - dictLen) >>> 1));
  // BFINAL = 1, BTYPE = 01 (fixed Huffman).
  writer.write(1, 1);
  writer.write(1, 2);
  let dirty = 0;
  let pos = dictLen;
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
      if (touched !== null) touched[dirty++] = h;
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
      for (let i = pos + 1; i < pos + bestLen && i + MIN_MATCH <= end; i++) {
        const h = hashAt(win, i);
        prev[i] = head[h];
        head[h] = i;
        if (touched !== null) touched[dirty++] = h;
      }
      pos += bestLen;
    } else {
      const lit = LIT_CODES[win[pos]];
      writer.write(lit >>> 4, lit & 15);
      pos++;
    }
  }
  state.dirty = dirty;
  const eob = LIT_CODES[256];
  writer.write(eob >>> 4, eob & 15);
  return writer.finish();
};

// Fixed codes did not pay: stored blocks are exactly the input plus five
// bytes per 64 KiB.
const orStored = (out, data) =>
  out.length >= data.length + 5 * Math.max(1, Math.ceil(data.length / STORED_MAX)) ? stored(data) : out;

const chainOf = (level) => CHAIN_BY_LEVEL[Math.max(1, Math.min(9, level | 0))];

/**
 * Bytes → raw DEFLATE, synchronously, as one fixed-Huffman block (or
 * stored blocks when that is smaller). `dictionary` preloads the window
 * (its last 32 KiB) exactly as zlib's `dictionary` option does, so the
 * output inflates on any inflater given the same bytes; `level` 1–9 is the
 * hash-chain depth.
 */
const deflateRaw = (input, { dictionary = null, level = 6 } = {}) => {
  const data = toBytes(input, 'deflate');
  const dict = dictionary === null ? null : toBytes(dictionary, 'deflate');
  const dictLen = dict === null ? 0 : Math.min(dict.length, WINDOW);
  const total = dictLen + data.length;
  const win = new Uint8Array(total);
  if (dictLen > 0) win.set(dict.subarray(dict.length - dictLen), 0);
  win.set(data, dictLen);
  const head = new Int32Array(HASH_SIZE).fill(-1);
  const prev = new Int32Array(total);
  hashDictionary(win, head, prev, dictLen);
  return orStored(pass({ win, head, prev, touched: null, dirty: 0 }, dictLen, total, chainOf(level)), data);
};

// The largest message a prepared state takes; a larger one goes through
// deflateRaw. A browser hands 4 KiB and more to CompressionStream anyway.
const SCRATCH = 8192;

/**
 * `encode(bytes) -> raw DEFLATE`, the same bytes deflateRaw answers for the
 * same dictionary and level, without redoing the dictionary's share of the
 * work on every message: the window with the dictionary in it, the hash
 * heads and the dictionary's chains are built once — on the first message,
 * so a codec nobody uses holds nothing — and a call copies the message in
 * behind the dictionary, runs the pass, and remembers which hash slots it
 * wrote. The NEXT call puts those slots back first (at the start, not the
 * end: an exception in between then costs nothing), which is `head` exactly
 * as the dictionary left it.
 *
 * bench/deflate-js.js, the "codec" rows against the one-shot ones: a 108 B
 * event 10.9 → 1.4 µs (8.9 → 1.6 with no dictionary at all: most of a
 * small message's cost was filling 128 KB of hash heads), a 2 KB callback
 * against a 4 KB dictionary 32 → 13, against 32 KiB 116 → 13 — the
 * dictionary's size no longer shows.
 *
 * About 0.2 MB plus five bytes per dictionary byte, per codec: build one
 * for a process or a page, not one per connection. Not reentrant, and
 * synchronous by construction.
 */
const createDeflater = (dictionary = null, level = 6) => {
  const dict = dictionary === null ? null : toBytes(dictionary, 'deflate');
  const dictLen = dict === null ? 0 : Math.min(dict.length, WINDOW);
  const chain = chainOf(level);
  let state = null;
  // What `head` holds for the dictionary alone; -1 everywhere without one.
  let base = null;
  const prepare = () => {
    const win = new Uint8Array(dictLen + SCRATCH);
    const head = new Int32Array(HASH_SIZE).fill(-1);
    const prev = new Int32Array(dictLen + SCRATCH);
    if (dictLen > 0) {
      win.set(dict.subarray(dict.length - dictLen), 0);
      hashDictionary(win, head, prev, dictLen);
      base = head.slice();
    }
    return { win, head, prev, touched: new Int32Array(SCRATCH), dirty: 0 };
  };
  return (input) => {
    const data = toBytes(input, 'deflate');
    if (data.length > SCRATCH) return deflateRaw(data, { dictionary: dict, level });
    state ??= prepare();
    const { head, touched } = state;
    for (let i = 0; i < state.dirty; i++) head[touched[i]] = base === null ? -1 : base[touched[i]];
    state.dirty = 0;
    state.win.set(data, dictLen);
    return orStored(pass(state, dictLen, dictLen + data.length, chain), data);
  };
};

module.exports = { deflateRaw, createDeflater };
