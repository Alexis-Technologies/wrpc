'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { SegmentQueue } = require('../../src/websocket/segments.js');

describe('SegmentQueue', () => {
  it('should push/length/peek within a single segment', () => {
    const queue = new SegmentQueue();
    assert.strictEqual(queue.length, 0);
    assert.strictEqual(queue.peek(10).length, 0);

    queue.push(Buffer.from('hello world'));
    assert.strictEqual(queue.length, 11);
    assert.strictEqual(queue.peek(5).toString(), 'hello');
    // peek does not consume
    assert.strictEqual(queue.length, 11);
    // peek beyond the buffered length is clamped
    assert.strictEqual(queue.peek(100).toString(), 'hello world');
  });

  it('should ignore empty pushes', () => {
    const queue = new SegmentQueue();
    queue.push(Buffer.alloc(0));
    assert.strictEqual(queue.length, 0);
  });

  it('should stitch bytes across segments in peek', () => {
    const queue = new SegmentQueue();
    queue.push(Buffer.from('ab'));
    queue.push(Buffer.from('cd'));
    queue.push(Buffer.from('ef'));
    assert.strictEqual(queue.peek(5).toString(), 'abcde');
    assert.strictEqual(queue.length, 6);
  });

  it('should advance the cursor on a consume within one segment', () => {
    const queue = new SegmentQueue();
    queue.push(Buffer.from('abcdef'));
    assert.strictEqual(queue.consume(2).toString(), 'ab');
    assert.strictEqual(queue.consume(2).toString(), 'cd');
    assert.strictEqual(queue.length, 2);
    assert.strictEqual(queue.peek(2).toString(), 'ef');
  });

  it('should copy once on a consume across segments', () => {
    const queue = new SegmentQueue();
    queue.push(Buffer.from('ab'));
    queue.push(Buffer.from('cd'));
    queue.push(Buffer.from('efgh'));
    assert.strictEqual(queue.consume(5).toString(), 'abcde');
    assert.strictEqual(queue.length, 3);
    assert.strictEqual(queue.consume(3).toString(), 'fgh');
    assert.strictEqual(queue.length, 0);
  });

  it('should consume exactly to a segment boundary', () => {
    const queue = new SegmentQueue();
    queue.push(Buffer.from('abc'));
    queue.push(Buffer.from('def'));
    assert.strictEqual(queue.consume(3).toString(), 'abc');
    assert.strictEqual(queue.consume(3).toString(), 'def');
    assert.strictEqual(queue.length, 0);
  });

  it('should return a zero-copy view on a single-segment consume', () => {
    const queue = new SegmentQueue();
    const source = Buffer.from('abcdef');
    queue.push(source);
    const view = queue.consume(3);
    source[0] = 0x7a; // 'z'
    assert.strictEqual(view.toString(), 'zbc');
  });

  it('should handle consume(0) and over-consume', () => {
    const queue = new SegmentQueue();
    queue.push(Buffer.from('ab'));
    assert.strictEqual(queue.consume(0).length, 0);
    assert.throws(() => queue.consume(3), RangeError);
  });

  it('should reset state on clear', () => {
    const queue = new SegmentQueue();
    queue.push(Buffer.from('abc'));
    queue.consume(1);
    queue.clear();
    assert.strictEqual(queue.length, 0);
    assert.strictEqual(queue.peek(3).length, 0);
    queue.push(Buffer.from('xy'));
    assert.strictEqual(queue.consume(2).toString(), 'xy');
  });

  // How many segments a frame arrives in is the peer's choice — it may write
  // its payload a byte at a time — and the queue dropped each consumed segment
  // with `shift()`, which copies the rest of a long array: one consume() of
  // 256,000 one-byte segments took 5.9 seconds, synchronously. A ratio, not a
  // time: both sizes run on the same machine under the same load, and the
  // quadratic consume was twenty times slower per segment at the larger one.
  it('should consume a frame in many tiny segments in linear time', () => {
    const one = Buffer.alloc(1);
    const perSegment = (count) => {
      const queue = new SegmentQueue();
      for (let i = 0; i < count; i++) queue.push(one);
      const started = process.hrtime.bigint();
      assert.strictEqual(queue.consume(count).length, count);
      const elapsed = Number(process.hrtime.bigint() - started);
      assert.strictEqual(queue.length, 0);
      return elapsed / count;
    };
    perSegment(4000);
    const short = perSegment(8000);
    const long = perSegment(256_000);
    assert.ok(long < short * 8, `${long.toFixed(0)} ns a segment at 256,000 against ${short.toFixed(0)} at 8,000`);
  });

  it('should keep the bytes in order across the moves that keep the list short', () => {
    const queue = new SegmentQueue();
    const expected = [];
    let value = 0;
    const push = (count, size) => {
      for (let i = 0; i < count; i++) {
        const segment = Buffer.alloc(size);
        for (let at = 0; at < size; at++) {
          segment[at] = value & 0xff;
          expected.push(value & 0xff);
          value++;
        }
        queue.push(segment);
      }
    };
    const read = [];
    const take = (size) => {
      for (const byte of queue.consume(size)) read.push(byte);
    };
    push(5000, 3);
    // Whole segments, parts of one, and ranges that span many — past the
    // head's run and past half of the list, so what is left moves down.
    take(3);
    take(2);
    take(1);
    take(4000);
    take(7);
    // A peek across segments from a head that has moved, with an offset.
    const ahead = queue.peek(10);
    assert.deepStrictEqual(Array.from(ahead), expected.slice(read.length, read.length + 10));
    take(6000);
    push(3000, 2);
    take(queue.length - 5);
    take(5);
    assert.strictEqual(queue.length, 0);
    assert.deepStrictEqual(read, expected);
    // And it starts again from empty.
    queue.push(Buffer.from('xy'));
    assert.strictEqual(queue.consume(2).toString(), 'xy');
  });
});
