'use strict';

const EMPTY_BUFFER = Buffer.alloc(0);

// FIFO byte queue over a list of socket segments. Bytes are copied at most
// once on consume (and only when a range spans segments), replacing the
// per-segment Buffer.concat accumulation that made large-message receive
// O(n^2) in the number of TCP segments.
class SegmentQueue {
  #segments = [];
  #offset = 0; // read cursor within #segments[0]
  #length = 0;

  get length() {
    return this.#length;
  }

  push(segment) {
    if (segment.length === 0) return;
    this.#segments.push(segment);
    this.#length += segment.length;
  }

  // Returns up to `size` leading bytes without consuming them.
  // Zero-copy when the range lies within the first segment.
  peek(size) {
    if (size > this.#length) size = this.#length;
    if (size === 0) return EMPTY_BUFFER;
    const first = this.#segments[0];
    const available = first.length - this.#offset;
    if (available >= size) return first.subarray(this.#offset, this.#offset + size);
    const out = Buffer.allocUnsafe(size);
    let copied = first.copy(out, 0, this.#offset);
    for (let i = 1; copied < size; i++) {
      const segment = this.#segments[i];
      copied += segment.copy(out, copied, 0, Math.min(segment.length, size - copied));
    }
    return out;
  }

  // Consumes and returns exactly `size` leading bytes. The result is a
  // zero-copy view into the source segment when the range does not span
  // segments — see the payload ownership contract in the changelog: callers
  // retaining the bytes long-term should copy them.
  consume(size) {
    if (size === 0) return EMPTY_BUFFER;
    if (size > this.#length) throw new RangeError('Cannot consume more bytes than buffered');
    const first = this.#segments[0];
    const available = first.length - this.#offset;
    let out;
    if (available > size) {
      out = first.subarray(this.#offset, this.#offset + size);
      this.#offset += size;
    } else if (available === size) {
      out = first.subarray(this.#offset);
      this.#segments.shift();
      this.#offset = 0;
    } else {
      out = Buffer.allocUnsafe(size);
      let copied = first.copy(out, 0, this.#offset);
      this.#segments.shift();
      this.#offset = 0;
      while (copied < size) {
        const segment = this.#segments[0];
        const take = Math.min(segment.length, size - copied);
        copied += segment.copy(out, copied, 0, take);
        if (take === segment.length) this.#segments.shift();
        else this.#offset = take;
      }
    }
    this.#length -= size;
    return out;
  }

  clear() {
    this.#segments.length = 0;
    this.#offset = 0;
    this.#length = 0;
  }
}

module.exports = { SegmentQueue };
