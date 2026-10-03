'use strict';

// Binary attachments: raw bytes anywhere in a packet — a Uint8Array in a
// call's args, an ArrayBuffer in a result, a typed array in an event's
// data — carried as bytes, in one binary frame, instead of what
// JSON.stringify makes of them (a `{"0":1,"1":2,…}` object nine times the
// size that arrives as a plain object, silently). The packet's JSON is
// written with `null` at every byte leaf and an index of where the leaves
// were; the buffers follow, back to back:
//
//   0x00  0x01  u32 headerLen  JSON [packet, [[path, byteLength], …]]  buffers…
//
// The 0x00 first byte is what a stream chunk never has (its first byte is
// an id length, at least 1), and kind 0x01 is this frame's (src/wire.js).
// Paths — the Jupyter `buffer_paths` shape — name the leaf from the packet
// root, so nothing in the user's data can collide with a placeholder. On
// the way in every buffer is COPIED out of the frame: a WebSocket engine
// hands over zero-copy views into its socket segments (segments.js), and a
// value that lives in `args` past the next frame must own its bytes.
//
// Browser-budgeted: this file lands in the main entry through the client
// transport's send path. `hasBytes` is the per-packet cost of the feature
// — a walk of the packet on every send, measured in bench/attachments.js —
// and `attachments: false` on either end skips it.

const { FRAME_MARK, FRAME_ATTACHMENTS } = require('./wire.js');

const MAX_DEPTH = 32;
const HEADER_BYTES = 6;
const encoder = new TextEncoder();
// fatal: a header that is not UTF-8 is a malformed frame, not U+FFFD soup.
const decoder = new TextDecoder('utf-8', { fatal: true });

const isBytes = (value) => value instanceof Uint8Array || ArrayBuffer.isView(value) || value instanceof ArrayBuffer;

// An object with toJSON() IS its projection: JSON.stringify serializes what
// toJSON answers, never the fields — so a domain object keeping a Buffer
// behind toJSON() (a password hash, say) must not have the Buffer lifted
// out from under it and sent. Checked AFTER isBytes: a Buffer has a toJSON
// of its own. The projection is never called here (acks.test.js pins one
// call per fan-out); it travels as JSON, as it always did.
const isOpaque = (value) => typeof value.toJSON === 'function';

const NONE = 0;
const BYTES = 1;
const DEEP = 2;
const CYCLE = 3;

// The fast walk keeps no memory of what it visited — that is what keeps it
// at the cost bench/attachments.js measures (rows(400): 17 µs, 160 ns on
// a 233 B packet; the ancestor stack below is an `includes` per container)
// — and what makes a graph with back-references (parent <-> children)
// exponential in it: 8^16 paths before the depth cap. So it hands over the
// moment it reaches MAX_DEPTH: a tree that deep is rare, a graph with a
// cycle gets there at once, and either way the slow walk answers (the
// 8-child graph of the bench: 0.5 µs).
const fast = (value, depth) => {
  if (typeof value !== 'object' || value === null) return NONE;
  if (isBytes(value)) return BYTES;
  if (isOpaque(value)) return NONE;
  if (depth >= MAX_DEPTH) return DEEP;
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      const found = fast(value[i], depth + 1);
      if (found !== NONE) return found;
    }
    return NONE;
  }
  for (const key in value) {
    const found = fast(value[key], depth + 1);
    if (found !== NONE) return found;
  }
  return NONE;
};

// The slow walk keeps its ancestors: a value met again on the way down is
// a cycle, and the whole packet is then left to JSON.stringify, whose
// "Converting circular structure" TypeError is the one the caller saw
// before attachments existed. Bounded by depth × nodes, not paths.
const slow = (value, stack) => {
  if (typeof value !== 'object' || value === null) return NONE;
  if (isBytes(value)) return BYTES;
  if (isOpaque(value)) return NONE;
  if (stack.includes(value)) return CYCLE;
  if (stack.length >= MAX_DEPTH) return NONE;
  stack.push(value);
  let found = NONE;
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length && found === NONE; i++) found = slow(value[i], stack);
  } else {
    for (const key in value) {
      found = slow(value[key], stack);
      if (found !== NONE) break;
    }
  }
  stack.pop();
  return found;
};

/**
 * Whether `value` holds bytes anywhere (a typed array, a DataView, an
 * ArrayBuffer) within 32 levels — what decides between JSON and a frame.
 * An object with toJSON() is opaque (its projection is what travels), and
 * a circular packet answers false: JSON.stringify's own TypeError follows.
 */
const hasBytes = (value) => {
  const found = fast(value, 0);
  if (found !== DEEP) return found === BYTES;
  return slow(value, []) === BYTES;
};

const asUint8 = (value) => {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
};

/**
 * The frame for a packet (or a batch array) that holds bytes. The packet is
 * not mutated: containers on the way to a byte leaf are copied, everything
 * else is shared.
 */
const encodeAttachments = (packet, maxDepth = MAX_DEPTH) => {
  const index = [];
  const buffers = [];
  // ONE mutable path, copied only at a byte leaf — a `path.concat(key)`
  // per node allocated an array per container visited: 414 → 137 µs on
  // rows(400) + 1 KB, 1.57 → 1.11 µs on a tiny event (bench/attachments.js).
  // The copy at the leaf is load-bearing: the index would otherwise hold
  // one array, rewritten by every step after.
  const path = [];
  // The ancestors of the value being stripped: a cycle is JSON's own
  // TypeError here, not an exponential walk that throws it later.
  const stack = [];
  let total = 0;
  const strip = (value) => {
    if (typeof value !== 'object' || value === null) return value;
    if (isBytes(value)) {
      const bytes = asUint8(value);
      index.push([path.slice(), bytes.length]);
      buffers.push(bytes);
      total += bytes.length;
      return null;
    }
    if (isOpaque(value)) return value;
    if (stack.includes(value)) throw new TypeError('Converting circular structure to JSON');
    if (stack.length >= maxDepth) return value;
    stack.push(value);
    let copy = null;
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) {
        const item = value[i];
        path.push(i);
        const next = strip(item);
        path.pop();
        if (next !== item) {
          if (copy === null) copy = value.slice();
          copy[i] = next;
        }
      }
    } else {
      // Own keys only, as JSON.stringify serializes them.
      for (const key in value) {
        if (!Object.hasOwn(value, key)) continue;
        const item = value[key];
        path.push(key);
        const next = strip(item);
        path.pop();
        if (next !== item) {
          if (copy === null) copy = { ...value };
          copy[key] = next;
        }
      }
    }
    stack.pop();
    return copy === null ? value : copy;
  };
  const stripped = strip(packet);
  const header = encoder.encode(JSON.stringify([stripped, index]));
  const frame = new Uint8Array(HEADER_BYTES + header.length + total);
  frame[0] = FRAME_MARK;
  frame[1] = FRAME_ATTACHMENTS;
  frame[2] = header.length >>> 24;
  frame[3] = (header.length >>> 16) & 255;
  frame[4] = (header.length >>> 8) & 255;
  frame[5] = header.length & 255;
  frame.set(header, HEADER_BYTES);
  let offset = HEADER_BYTES + header.length;
  for (let i = 0; i < buffers.length; i++) {
    frame.set(buffers[i], offset);
    offset += buffers[i].length;
  }
  return frame;
};

/** Whether `bytes` is an attachments frame by its first two bytes. */
const isAttachmentsFrame = (bytes) =>
  bytes.length >= HEADER_BYTES && bytes[0] === FRAME_MARK && bytes[1] === FRAME_ATTACHMENTS;

const malformed = (what) => new TypeError(`attachments: ${what}`);

// A path key: an array index, or an object key. `__proto__` is refused
// outright; every other key is written only where the parsed packet
// already holds an own `null` placeholder (Object.hasOwn below), which is
// what keeps a frame from reaching a prototype through `constructor` or
// anything else — the placeholder must have been created by JSON.parse.
const validKey = (key) =>
  typeof key === 'number' ? Number.isInteger(key) && key >= 0 : typeof key === 'string' && key !== '__proto__';

/**
 * The packet (or batch array) a frame carries, its byte leaves restored as
 * fresh Uint8Arrays — copies, never views into `bytes`. Every malformed
 * frame is a TypeError: a header that is not `[packet, index]`, a path
 * that names a forbidden key, walks through a non-container or lands on
 * anything but the `null` placeholder, or byte lengths that do not add up
 * to exactly what follows the header.
 */
const decodeAttachments = (bytes, maxDepth = MAX_DEPTH) => {
  if (!isAttachmentsFrame(bytes)) throw malformed('not an attachments frame');
  const headerLen = ((bytes[2] << 24) >>> 0) + (bytes[3] << 16) + (bytes[4] << 8) + bytes[5];
  if (HEADER_BYTES + headerLen > bytes.length) throw malformed('header past the end of the frame');
  let header;
  try {
    header = JSON.parse(decoder.decode(bytes.subarray(HEADER_BYTES, HEADER_BYTES + headerLen)));
  } catch {
    throw malformed('header is not JSON');
  }
  if (!Array.isArray(header) || header.length !== 2) throw malformed('header is not [packet, index]');
  const packet = header[0];
  const index = header[1];
  if (!Array.isArray(index)) throw malformed('index is not an array');
  let offset = HEADER_BYTES + headerLen;
  for (let i = 0; i < index.length; i++) {
    const entry = index[i];
    if (!Array.isArray(entry) || entry.length !== 2) throw malformed('index entry is not [path, length]');
    const path = entry[0];
    const length = entry[1];
    if (!Array.isArray(path) || path.length === 0 || path.length > maxDepth) throw malformed('bad path');
    if (!Number.isInteger(length) || length < 0 || offset + length > bytes.length) throw malformed('bad length');
    let target = packet;
    for (let j = 0; j < path.length - 1; j++) {
      const key = path[j];
      if (!validKey(key)) throw malformed('forbidden key in path');
      if (typeof target !== 'object' || target === null) throw malformed('path through a non-container');
      if (!Object.hasOwn(target, key)) throw malformed('path names a missing key');
      target = target[key];
    }
    const last = path[path.length - 1];
    if (!validKey(last)) throw malformed('forbidden key in path');
    if (typeof target !== 'object' || target === null) throw malformed('path through a non-container');
    if (!Object.hasOwn(target, last)) throw malformed('path names a missing key');
    if (target[last] !== null) throw malformed('placeholder is not null');
    // A copy, by construction: the constructor over a typed array
    // allocates — the frame may be a view into a socket buffer.
    target[last] = new Uint8Array(bytes.subarray(offset, offset + length));
    offset += length;
  }
  if (offset !== bytes.length) throw malformed('byte length mismatch');
  return packet;
};

// A backplane envelope wraps the packet — or a command's args — at most two
// levels deeper than a packet holds its data: its frame reaches that much
// further, so bytes a direct send delivers are delivered across instances
// too (at 31 levels they arrived as `{"0":…}` there).
const ENVELOPE_DEPTH = MAX_DEPTH + 2;

module.exports = {
  hasBytes,
  encodeAttachments,
  decodeAttachments,
  isAttachmentsFrame,
  FRAME_ATTACHMENTS,
  ENVELOPE_DEPTH,
};
