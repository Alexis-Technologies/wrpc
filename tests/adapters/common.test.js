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
