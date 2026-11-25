'use strict';

const EMPTY_BUFFER = Buffer.alloc(0);

// How far the head of the segment list runs before what is left is moved
// down to the front of its array (see #advance).
const COMPACT_AT = 1024;

// FIFO byte queue over a list of socket segments. Bytes are copied at most
// once on consume (and only when a range spans segments), replacing the
// per-segment Buffer.concat accumulation that made large-message receive
// O(n^2) in the number of TCP segments.
//
// The list is read from `#head`, never with `shift()`. How many segments a
// frame arrives in is the PEER's choice — it may write its payload a byte at
// a time — and past a few thousand elements every `shift()` copies the rest
// of the array: consuming a frame of N segments cost N² / 2 moves, in one
// synchronous call. A 300,000-byte frame sent in one-byte writes, twelve
// megabytes of packets over two seconds, stalled the event loop for 12.8
// seconds — before any authentication. 5.9 s for 256,000 one-byte segments
// against a few milliseconds (bench/segment-queue.js).
class SegmentQueue {
  #segments = [];
  #head = 0; // the first segment not consumed yet
  #offset = 0; // read cursor within that segment
  #length = 0;

  get length() {
    return this.#length;
  }

  push(segment) {
    if (segment.length === 0) return;
    this.#segments.push(segment);
    this.#length += segment.length;
  }

  // Lets go of the head segment. The array is emptied when the head reaches
  // its end — what a connection that is read as it arrives does at once —
  // and what is left is moved down once the head has passed half of a long
  // list, so a queue that never runs dry does not grow by what was read.
  #advance() {
    const segments = this.#segments;
    segments[this.#head++] = undefined;
    this.#offset = 0;
    if (this.#head === segments.length) {
      segments.length = 0;
      this.#head = 0;
    } else if (this.#head >= COMPACT_AT && this.#head * 2 >= segments.length) {
      segments.copyWithin(0, this.#head);
      segments.length -= this.#head;
      this.#head = 0;
    }
  }

  // Returns up to `size` leading bytes without consuming them.
  // Zero-copy when the range lies within the first segment.
  peek(size) {
    if (size > this.#length) size = this.#length;
    if (size === 0) return EMPTY_BUFFER;
    const first = this.#segments[this.#head];
    const available = first.length - this.#offset;
    if (available >= size) return first.subarray(this.#offset, this.#offset + size);
    const out = Buffer.allocUnsafe(size);
    let copied = first.copy(out, 0, this.#offset);
    for (let i = this.#head + 1; copied < size; i++) {
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
    const first = this.#segments[this.#head];
    const available = first.length - this.#offset;
    let out = null;
    if (available > size) {
      out = first.subarray(this.#offset, this.#offset + size);
      this.#offset += size;
    } else if (available === size) {
      out = first.subarray(this.#offset);
      this.#advance();
    } else {
      out = Buffer.allocUnsafe(size);
      let copied = first.copy(out, 0, this.#offset);
      this.#advance();
      while (copied < size) {
        const segment = this.#segments[this.#head];
        const take = Math.min(segment.length, size - copied);
        copied += segment.copy(out, copied, 0, take);
        if (take === segment.length) this.#advance();
        else this.#offset = take;
      }
    }
    this.#length -= size;
    return out;
  }

  clear() {
    this.#segments.length = 0;
    this.#head = 0;
    this.#offset = 0;
    this.#length = 0;
  }
}

module.exports = { SegmentQueue };
