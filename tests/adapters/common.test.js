'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { Readable } = require('node:stream');

const { receiveBody, normalizeBody, statusLine, eachHeader } = require('../../src/adapters/common.js');

const streamOf = (...chunks) => Readable.from(chunks.map((chunk) => Buffer.from(chunk)));

describe('adapters/common', () => {
  describe('receiveBody', () => {
    it('should read an empty stream as no body', async () => {
      assert.strictEqual(await receiveBody(streamOf()), null);
    });

    it('should pass a single chunk through without a copy', async () => {
      assert.strictEqual((await receiveBody(streamOf('one'))).toString(), 'one');
    });

    it('should concatenate multiple chunks in order', async () => {
      assert.strictEqual((await receiveBody(streamOf('a', 'b', 'c'))).toString(), 'abc');
    });

    it('should enforce the size limit across chunks, not per chunk', async () => {
      await assert.rejects(receiveBody(streamOf('aaa', 'bbb', 'ccc'), 8), /Body size limit exceeded/);
    });

    it('should refuse a nonsensical limit instead of silently disabling the guard', async () => {
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

    it('should destroy the stream past the limit and refuse the body', async () => {
      const stream = live();
      const pending = receiveBody(stream, 4);
      stream.push(Buffer.from('12345'));
      await assert.rejects(pending, /Body size limit exceeded/);
      assert.strictEqual(stream.destroyed, true);
      settledListeners(stream);
    });

    it("should reject with the stream's own error", async () => {
      const stream = live();
      const pending = receiveBody(stream);
      stream.push(Buffer.from('half'));
      stream.destroy(new Error('aborted'));
      await assert.rejects(pending, /aborted/);
    });

    it('should treat a stream closed before its end as a premature close', async () => {
      const stream = live();
      const pending = receiveBody(stream);
      stream.push(Buffer.from('half'));
      stream.destroy();
      await assert.rejects(pending, { code: 'ERR_STREAM_PREMATURE_CLOSE' });
    });

    it('should read a stream already read to its end as no body, and refuse a dead one', async () => {
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

    it('should read a stream paused upstream all the same', async () => {
      const stream = streamOf('paused', ' body');
      stream.pause();
      assert.strictEqual((await receiveBody(stream)).toString(), 'paused body');
    });

    it('should leave only the error sink behind for a body read to its end', async () => {
      const stream = streamOf('a', 'b');
      assert.strictEqual((await receiveBody(stream)).toString(), 'ab');
      settledListeners(stream);
    });
  });

  describe('normalizeBody', () => {
    it('should turn absent bodies into null', () => {
      assert.strictEqual(normalizeBody(undefined), null);
      assert.strictEqual(normalizeBody(null), null);
    });

    it('should pass strings and buffers through untouched', () => {
      const buffer = Buffer.from('{"a":1}');
      assert.strictEqual(normalizeBody('{"a":1}'), '{"a":1}');
      assert.strictEqual(normalizeBody(buffer), buffer);
    });

    it('should wrap a typed-array view without copying its bytes', () => {
      const view = new Uint8Array([123, 125]); // '{}'
      assert.strictEqual(normalizeBody(view).toString(), '{}');
    });

    it('should re-serialize a framework-parsed object for the core to re-parse', () => {
      assert.strictEqual(normalizeBody({ type: 'call', id: '1' }), '{"type":"call","id":"1"}');
    });

    it('should read a non-serializable body as no body instead of throwing', () => {
      const circular = { self: null };
      circular.self = circular;
      assert.strictEqual(normalizeBody(circular), null);
    });
  });

  it('should render a uws status string with statusLine, including unknown codes', () => {
    assert.strictEqual(statusLine(200), '200 OK');
    assert.strictEqual(statusLine(404), '404 Not Found');
    assert.strictEqual(statusLine(599), '599 Unknown');
  });

  it('should expand arrays into repeated header lines with eachHeader', () => {
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
});
