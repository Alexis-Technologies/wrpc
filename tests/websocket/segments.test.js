'use strict';

const test = require('node:test');
const assert = require('node:assert');

const { SegmentQueue } = require('../../src/websocket/segments.js');

test('SegmentQueue: push/length/peek within a single segment', () => {
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

test('SegmentQueue: empty pushes are ignored', () => {
  const queue = new SegmentQueue();
  queue.push(Buffer.alloc(0));
  assert.strictEqual(queue.length, 0);
});

test('SegmentQueue: peek stitches bytes across segments', () => {
  const queue = new SegmentQueue();
  queue.push(Buffer.from('ab'));
  queue.push(Buffer.from('cd'));
  queue.push(Buffer.from('ef'));
  assert.strictEqual(queue.peek(5).toString(), 'abcde');
  assert.strictEqual(queue.length, 6);
});

test('SegmentQueue: consume within one segment advances the cursor', () => {
  const queue = new SegmentQueue();
  queue.push(Buffer.from('abcdef'));
  assert.strictEqual(queue.consume(2).toString(), 'ab');
  assert.strictEqual(queue.consume(2).toString(), 'cd');
  assert.strictEqual(queue.length, 2);
  assert.strictEqual(queue.peek(2).toString(), 'ef');
});

test('SegmentQueue: consume across segments copies once', () => {
  const queue = new SegmentQueue();
  queue.push(Buffer.from('ab'));
  queue.push(Buffer.from('cd'));
  queue.push(Buffer.from('efgh'));
  assert.strictEqual(queue.consume(5).toString(), 'abcde');
  assert.strictEqual(queue.length, 3);
  assert.strictEqual(queue.consume(3).toString(), 'fgh');
  assert.strictEqual(queue.length, 0);
});

test('SegmentQueue: consume exactly to a segment boundary', () => {
  const queue = new SegmentQueue();
  queue.push(Buffer.from('abc'));
  queue.push(Buffer.from('def'));
  assert.strictEqual(queue.consume(3).toString(), 'abc');
  assert.strictEqual(queue.consume(3).toString(), 'def');
  assert.strictEqual(queue.length, 0);
});

test('SegmentQueue: single-segment consume is a zero-copy view', () => {
  const queue = new SegmentQueue();
  const source = Buffer.from('abcdef');
  queue.push(source);
  const view = queue.consume(3);
  source[0] = 0x7a; // 'z'
  assert.strictEqual(view.toString(), 'zbc');
});

test('SegmentQueue: consume(0) and over-consume', () => {
  const queue = new SegmentQueue();
  queue.push(Buffer.from('ab'));
  assert.strictEqual(queue.consume(0).length, 0);
  assert.throws(() => queue.consume(3), RangeError);
});

test('SegmentQueue: clear resets state', () => {
  const queue = new SegmentQueue();
  queue.push(Buffer.from('abc'));
  queue.consume(1);
  queue.clear();
  assert.strictEqual(queue.length, 0);
  assert.strictEqual(queue.peek(3).length, 0);
  queue.push(Buffer.from('xy'));
  assert.strictEqual(queue.consume(2).toString(), 'xy');
});
