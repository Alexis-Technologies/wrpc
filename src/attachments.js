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

/**
 * Whether `value` holds bytes anywhere (a typed array, a DataView, an
 * ArrayBuffer) within 32 levels — what decides between JSON and a frame.
 */
const hasBytes = (value, depth = 0) => {
  if (typeof value !== 'object' || value === null) return false;
  if (isBytes(value)) return true;
  if (depth >= MAX_DEPTH) return false;
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) if (hasBytes(value[i], depth + 1)) return true;
    return false;
  }
  for (const key in value) if (hasBytes(value[key], depth + 1)) return true;
  return false;
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
const encodeAttachments = (packet) => {
  const index = [];
  const buffers = [];
  let total = 0;
  const strip = (value, path, depth) => {
    if (typeof value !== 'object' || value === null) return value;
    if (isBytes(value)) {
      const bytes = asUint8(value);
      index.push([path, bytes.length]);
      buffers.push(bytes);
      total += bytes.length;
      return null;
    }
    if (depth >= MAX_DEPTH) return value;
    if (Array.isArray(value)) {
      let copy = null;
      for (let i = 0; i < value.length; i++) {
        const item = value[i];
        const next = strip(item, path.concat(i), depth + 1);
        if (next !== item) {
          if (copy === null) copy = value.slice();
          copy[i] = next;
        }
      }
      return copy === null ? value : copy;
    }
    let copy = null;
    for (const key in value) {
      const item = value[key];
      const next = strip(item, path.concat(key), depth + 1);
      if (next !== item) {
        if (copy === null) copy = { ...value };
        copy[key] = next;
      }
    }
    return copy === null ? value : copy;
  };
  const stripped = strip(packet, [], 0);
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
const decodeAttachments = (bytes) => {
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
    if (!Array.isArray(path) || path.length === 0 || path.length > MAX_DEPTH) throw malformed('bad path');
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

module.exports = { hasBytes, encodeAttachments, decodeAttachments, isAttachmentsFrame, FRAME_ATTACHMENTS };
