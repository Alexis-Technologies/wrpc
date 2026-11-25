'use strict';

// A queue that is read from its head: appended at the tail, taken from the
// head, and — where a taker hands something back — put back AT the head. A
// ring whose capacity is a power of two, indexed through a mask: both ends
// are O(1), and it doubles when full and halves at a quarter, so a backlog
// that was drained does not keep its slots.
//
// The package's answer wherever an array was read with `shift()`: past a few
// thousand elements each `shift` moves the rest, and how long such a queue
// gets is somebody else's choice — a producer's, a peer's. A MemoryBroker
// backlog of 160,000 was produced and consumed at 10,621 messages a second
// over an array, against 2.1 million over this ring (bench/broker-backlog.js);
// a WebSocket connection's queue of 256,000 frames waiting behind a deflate
// cost 150 µs a frame against 30 (bench/ws-outbox.js).
//
// Node-only by where it is required from (the WebSocket engine and the
// brokers): no browser entry carries it. A readable's own queue keeps its
// head cursor (`pull` in src/streams.js) — that file is on a byte budget.

const MIN_CAPACITY = 16;

// The ring's slots, preallocated and written by index through the mask —
// the shape bench/broker-backlog.js and bench/ws-outbox.js measure.
// oxlint-disable-next-line unicorn/no-new-array
const slotsOf = (capacity) => new Array(capacity);

class Deque {
  #slots = slotsOf(MIN_CAPACITY);
  #mask = MIN_CAPACITY - 1;
  #head = 0;
  #length = 0;

  get length() {
    return this.#length;
  }

  push(value) {
    if (this.#length > this.#mask) this.#resize(this.#slots.length * 2);
    this.#slots[(this.#head + this.#length) & this.#mask] = value;
    this.#length++;
  }

  unshift(value) {
    if (this.#length > this.#mask) this.#resize(this.#slots.length * 2);
    this.#head = (this.#head - 1) & this.#mask;
    this.#slots[this.#head] = value;
    this.#length++;
  }

  /** The head, removed — `undefined` when there is none, as an array's is. */
  shift() {
    if (this.#length === 0) return undefined;
    const value = this.#slots[this.#head];
    this.#slots[this.#head] = undefined;
    this.#head = (this.#head + 1) & this.#mask;
    this.#length--;
    if (this.#mask >= MIN_CAPACITY && this.#length * 4 <= this.#mask) this.#resize(this.#slots.length / 2);
    return value;
  }

  /** The head, left where it is — `undefined` when there is none. */
  peek() {
    return this.#length === 0 ? undefined : this.#slots[this.#head];
  }

  // Into fresh slots rather than a walk over the old ones: what was queued
  // is let go of at once, and a caller that clears from inside a loop over
  // this queue (a listener that terminates its connection) finds it empty
  // on its next look, whatever it had read before.
  clear() {
    this.#slots = slotsOf(MIN_CAPACITY);
    this.#mask = MIN_CAPACITY - 1;
    this.#head = 0;
    this.#length = 0;
  }

  // Into a fresh array, from its start: what wrapped around the end of the
  // old one is in order again.
  #resize(capacity) {
    const slots = slotsOf(capacity);
    for (let i = 0; i < this.#length; i++) slots[i] = this.#slots[(this.#head + i) & this.#mask];
    this.#slots = slots;
    this.#mask = capacity - 1;
    this.#head = 0;
  }
}

module.exports = { Deque };
