'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { Emitter } = require('../src/utils.js');
const { WrpcReadable } = require('../src/streams.js');

class FakeSink extends Emitter {
  chunks = [];
  ended = false;

  write(chunk) {
    this.chunks.push(Buffer.from(chunk));
    return true;
  }

  end() {
    this.ended = true;
    queueMicrotask(() => this.emit('close'));
  }

  waitEvent(event) {
    return new Promise((resolve) => this.once(event, resolve));
  }

  removeListener(event, listener) {
    this.off(event, listener);
  }
}

test('WrpcReadable.finalize/pipe drains into a writable-like sink', async () => {
  const readable = new WrpcReadable('id', 'name', 6);
  const sink = new FakeSink();
  const piped = readable.pipe(sink);
  assert.strictEqual(piped, sink);

  await readable.push(Buffer.from('abc'));
  await readable.push(Buffer.from('def'));
  await readable.close();

  await sink.waitEvent('close').then(() => {});
  await new Promise((resolve) => setImmediate(resolve));

  assert.strictEqual(Buffer.concat(sink.chunks).toString(), 'abcdef');
  assert.strictEqual(sink.ended, true);
  assert.strictEqual(readable.status, 'closed');
});

test('WrpcReadable.finalize waits for drain when the sink reports backpressure', async () => {
  const readable = new WrpcReadable('id', 'name', 3);
  const sink = new FakeSink();
  sink.write = (chunk) => {
    sink.chunks.push(Buffer.from(chunk));
    return false; // signal backpressure on every write
  };

  readable.pipe(sink);
  const pushed = readable.push(Buffer.from('abc'));
  await new Promise((resolve) => setImmediate(resolve));
  sink.emit('drain');
  await pushed;
  await readable.close();
  await new Promise((resolve) => setImmediate(resolve));

  assert.strictEqual(Buffer.concat(sink.chunks).toString(), 'abc');
});

test('WrpcReadable.pipe surfaces finalize() rejections as an error event', async () => {
  const readable = new WrpcReadable('id', 'name', 3);
  const sink = new FakeSink();
  sink.write = () => {
    throw new Error('sink exploded');
  };
  const errorPromise = new Promise((resolve) => readable.once('error', resolve));
  readable.pipe(sink);
  await readable.push(Buffer.from('abc'));
  const error = await errorPromise;
  assert.match(error.message, /sink exploded/);
});

test('WrpcReadable.push applies backpressure once a consumer attached and the high water mark is exceeded', async () => {
  const readable = new WrpcReadable('id', 'name', 10, { highWaterMark: 1 });
  // The high-water mark only applies after the first read: the consumer
  // attaches, and PULL_EVENTs become possible.
  await readable.push(Buffer.from('a'));
  assert.strictEqual((await readable.read()).toString(), 'a');

  await readable.push(Buffer.from('b'));
  await readable.push(Buffer.from('c'));

  let fourthResolved = false;
  const fourth = readable.push(Buffer.from('d')).then(() => (fourthResolved = true));

  await new Promise((resolve) => setImmediate(resolve));
  assert.strictEqual(fourthResolved, false);

  assert.strictEqual((await readable.read()).toString(), 'b');
  await fourth;
  assert.strictEqual(fourthResolved, true);
  assert.strictEqual((await readable.read()).toString(), 'c');
  assert.strictEqual((await readable.read()).toString(), 'd');
});

test('WrpcReadable.push buffers freely before any consumer attaches', async () => {
  const readable = new WrpcReadable('id', 'name', 10, { highWaterMark: 1 });
  // No consumer yet: pushes far beyond the high-water mark must resolve
  // immediately — blocking here would deadlock the upload-then-call wire
  // pattern (chunks arrive before the call that starts the consumer).
  for (let i = 0; i < 40; i++) {
    await readable.push(Buffer.from(String(i)));
  }
  assert.strictEqual(readable.queue.length, 40);
  assert.strictEqual((await readable.read()).toString(), '0');
});

test('WrpcReadable.terminate marks the stream terminated without waiting for all bytes', async () => {
  const readable = new WrpcReadable('id', 'name', 1000);
  await readable.terminate();
  assert.strictEqual(readable.status, 'terminated');
  assert.strictEqual(readable.streaming, false);
});

test('WrpcReadable.checkStreamLimits grows the high water mark under listener pressure', async () => {
  const readable = new WrpcReadable('id', 'name', 100, { highWaterMark: 1 });
  await readable.push(Buffer.from('0'));
  assert.strictEqual((await readable.read()).toString(), '0'); // attach a consumer
  await readable.push(Buffer.from('a'));
  await readable.push(Buffer.from('b'));

  // 10 concurrent stalled pushes fill PULL_EVENT's listener count to the
  // MAX_LISTENERS(10) threshold that checkStreamLimits watches for.
  const stalled = [];
  for (let i = 0; i < 10; i++) {
    stalled.push(readable.push(Buffer.from(String(i))));
  }
  await new Promise((resolve) => setImmediate(resolve));

  // An 11th waiter trips checkStreamLimits' threshold (raising highWaterMark).
  // Since F2 the Emitter's listener cap only warns instead of throwing, so
  // the push stalls like the others rather than rejecting.
  const warnings = [];
  const originalWarn = globalThis.console.warn;
  globalThis.console.warn = (message) => warnings.push(message);
  const eleventh = readable.push(Buffer.from('x'));
  await new Promise((resolve) => setImmediate(resolve));
  globalThis.console.warn = originalWarn;

  assert.strictEqual(readable.highWaterMark, 2);
  assert.ok(warnings.some((message) => /MaxListenersExceededWarning/.test(message)));

  for (let i = 0; i < 13; i++) await readable.read();
  await Promise.all([...stalled, eleventh]);
});
