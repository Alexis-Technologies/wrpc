'use strict';

const test = require('node:test');
const assert = require('node:assert');
const { Readable } = require('node:stream');

const { receiveBody, normalizeBody, statusLine, eachHeader } = require('../../src/adapters/common.js');

const streamOf = (...chunks) => Readable.from(chunks.map((chunk) => Buffer.from(chunk)));

test('receiveBody', async (t) => {
  await t.test('an empty stream reads as no body', async () => {
    assert.strictEqual(await receiveBody(streamOf()), null);
  });

  await t.test('a single chunk is passed through without a copy', async () => {
    assert.strictEqual((await receiveBody(streamOf('one'))).toString(), 'one');
  });

  await t.test('multiple chunks are concatenated in order', async () => {
    assert.strictEqual((await receiveBody(streamOf('a', 'b', 'c'))).toString(), 'abc');
  });

  await t.test('the size limit is enforced across chunks, not per chunk', async () => {
    await assert.rejects(receiveBody(streamOf('aaa', 'bbb', 'ccc'), 8), /Body size limit exceeded/);
  });

  await t.test('a nonsensical limit is refused instead of silently disabling the guard', async () => {
    await assert.rejects(receiveBody(streamOf('x'), -1), TypeError);
    await assert.rejects(receiveBody(streamOf('x'), 1.5), TypeError);
    await assert.rejects(receiveBody(streamOf('x'), Number.MAX_SAFE_INTEGER + 10), TypeError);
  });

  // The outcomes the `for await` loop had, kept by the listeners that
  // replaced it (bench/http-call.js).
  const live = () => new Readable({ read() {} });

  // Settled, the body leaves one listener: the error sink a late error
  // lands in instead of becoming an uncaught exception.
  const settledListeners = (stream) => {
    for (const event of ['data', 'end', 'close']) assert.strictEqual(stream.listenerCount(event), 0, event);
    assert.strictEqual(stream.listenerCount('error'), 1);
    stream.emit('error', new Error('late'));
  };

  await t.test('past the limit the stream is destroyed and the body refused', async () => {
    const stream = live();
    const pending = receiveBody(stream, 4);
    stream.push(Buffer.from('12345'));
    await assert.rejects(pending, /Body size limit exceeded/);
    assert.strictEqual(stream.destroyed, true);
    settledListeners(stream);
  });

  await t.test("the stream's own error is the rejection", async () => {
    const stream = live();
    const pending = receiveBody(stream);
    stream.push(Buffer.from('half'));
    stream.destroy(new Error('aborted'));
    await assert.rejects(pending, /aborted/);
  });

  await t.test('a stream closed before its end is a premature close', async () => {
    const stream = live();
    const pending = receiveBody(stream);
    stream.push(Buffer.from('half'));
    stream.destroy();
    await assert.rejects(pending, { code: 'ERR_STREAM_PREMATURE_CLOSE' });
  });

  await t.test('a stream already read to its end reads as no body; a dead one is refused', async () => {
    const ended = streamOf('gone');
    ended.resume();
    await new Promise((resolve) => ended.once('end', resolve));
    assert.strictEqual(await receiveBody(ended), null);
    const errored = live();
    errored.on('error', () => {});
    errored.destroy(new Error('reset'));
    await assert.rejects(receiveBody(errored), /reset/);
    const closed = live();
    closed.destroy();
    await assert.rejects(receiveBody(closed), { code: 'ERR_STREAM_PREMATURE_CLOSE' });
  });

  await t.test('a stream paused upstream is read all the same', async () => {
    const stream = streamOf('paused', ' body');
    stream.pause();
    assert.strictEqual((await receiveBody(stream)).toString(), 'paused body');
  });

  await t.test('a body read to its end leaves only the error sink behind', async () => {
    const stream = streamOf('a', 'b');
    assert.strictEqual((await receiveBody(stream)).toString(), 'ab');
    settledListeners(stream);
  });
});

test('normalizeBody', async (t) => {
  await t.test('absent bodies become null', () => {
    assert.strictEqual(normalizeBody(undefined), null);
    assert.strictEqual(normalizeBody(null), null);
  });

  await t.test('strings and buffers pass through untouched', () => {
    const buffer = Buffer.from('{"a":1}');
    assert.strictEqual(normalizeBody('{"a":1}'), '{"a":1}');
    assert.strictEqual(normalizeBody(buffer), buffer);
  });

  await t.test('a typed-array view is wrapped without copying its bytes', () => {
    const view = new Uint8Array([123, 125]); // '{}'
    assert.strictEqual(normalizeBody(view).toString(), '{}');
  });

  await t.test('a framework-parsed object is re-serialized for the core to re-parse', () => {
    assert.strictEqual(normalizeBody({ type: 'call', id: '1' }), '{"type":"call","id":"1"}');
  });

  await t.test('a non-serializable body reads as no body instead of throwing', () => {
    const circular = { self: null };
    circular.self = circular;
    assert.strictEqual(normalizeBody(circular), null);
  });
});

test('statusLine renders a uws status string, including unknown codes', () => {
  assert.strictEqual(statusLine(200), '200 OK');
  assert.strictEqual(statusLine(404), '404 Not Found');
  assert.strictEqual(statusLine(599), '599 Unknown');
});

test('eachHeader expands arrays into repeated header lines', () => {
  const seen = [];
  eachHeader(
    { 'Content-Type': 'application/json', 'Content-Length': 12, 'Set-Cookie': ['a=1', 'b=2'] },
    (name, value) => seen.push([name, value]),
  );
  assert.deepStrictEqual(seen, [
    ['Content-Type', 'application/json'],
    // numbers are stringified: frameworks reject non-string header values
    ['Content-Length', '12'],
    ['Set-Cookie', 'a=1'],
    ['Set-Cookie', 'b=2'],
  ]);
});
