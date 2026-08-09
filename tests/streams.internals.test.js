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

test('WrpcReadable.push applies backpressure once the high water mark is exceeded', async () => {
  const readable = new WrpcReadable('id', 'name', 10, { highWaterMark: 1 });
  await readable.push(Buffer.from('a'));
  await readable.push(Buffer.from('b'));

  let thirdResolved = false;
  const third = readable.push(Buffer.from('c')).then(() => (thirdResolved = true));

  await new Promise((resolve) => setImmediate(resolve));
  assert.strictEqual(thirdResolved, false);

  assert.strictEqual((await readable.read()).toString(), 'a');
  await third;
  assert.strictEqual(thirdResolved, true);
  assert.strictEqual((await readable.read()).toString(), 'b');
  assert.strictEqual((await readable.read()).toString(), 'c');
});

test('WrpcReadable.terminate marks the stream terminated without waiting for all bytes', async () => {
  const readable = new WrpcReadable('id', 'name', 1000);
  await readable.terminate();
  assert.strictEqual(readable.status, 'terminated');
  assert.strictEqual(readable.streaming, false);
});

test('WrpcReadable.checkStreamLimits grows the high water mark under listener pressure', async () => {
  const readable = new WrpcReadable('id', 'name', 100, { highWaterMark: 1 });
  await readable.push(Buffer.from('a'));
  await readable.push(Buffer.from('b'));

  // 10 concurrent stalled pushes fill PULL_EVENT's listener count to the
  // MAX_LISTENERS(10) threshold that checkStreamLimits watches for.
  const stalled = [];
  for (let i = 0; i < 10; i++) {
    stalled.push(readable.push(Buffer.from(String(i))));
  }
  await new Promise((resolve) => setImmediate(resolve));

  // An 11th waiter trips checkStreamLimits' threshold (raising highWaterMark)
  // and then immediately hits the Emitter's own identical default maxListeners
  // cap when it tries to register itself, rejecting this specific push().
  const eleventh = await readable.push(Buffer.from('x')).catch((error) => error);
  assert.strictEqual(readable.highWaterMark, 2);
  assert.match(eleventh.message, /MaxListenersExceededWarning/);

  for (let i = 0; i < 12; i++) await readable.read();
  await Promise.all(stalled);
});
