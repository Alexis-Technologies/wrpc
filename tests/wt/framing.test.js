'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');

const {
  HEADER_BYTES,
  KIND_TEXT,
  KIND_BINARY,
  INLINE_TEXT,
  FramingError,
  frame,
  frameText,
  StreamParser,
} = require('../../src/webtransport/framing.js');

const collect = (options = {}) => {
  const messages = [];
  const parser = new StreamParser({ ...options, onMessage: (kind, data) => messages.push({ kind, data }) });
  return { parser, messages };
};

const header = (bytes) => ({
  length: ((bytes[0] << 24) >>> 0) + (bytes[1] << 16) + (bytes[2] << 8) + bytes[3],
  kind: bytes[4],
});

describe('wt framing', () => {
  it('should frame a packet as KIND 0 over its UTF-8, and a chunk as KIND 1 over its bytes', () => {
    const text = frameText('{"type":"ping"} é');
    assert.strictEqual(text.length, HEADER_BYTES + Buffer.byteLength('{"type":"ping"} é'));
    assert.deepStrictEqual(header(text), { length: text.length - HEADER_BYTES, kind: KIND_TEXT });
    assert.strictEqual(Buffer.from(text.subarray(HEADER_BYTES)).toString(), '{"type":"ping"} é');
    const bytes = crypto.randomBytes(300);
    const binary = frame(KIND_BINARY, bytes);
    assert.deepStrictEqual(header(binary), { length: 300, kind: KIND_BINARY });
    assert.deepStrictEqual(Buffer.from(binary.subarray(HEADER_BYTES)), bytes);
    // The inline path over-allocates and hands back a view; the encode path
    // is exact. Both decode to the same text.
    const long = 'x'.repeat(INLINE_TEXT + 1);
    assert.strictEqual(frameText(long).length, HEADER_BYTES + long.length);
    assert.strictEqual(frameText(long).byteOffset, 0);
  });

  it('should deliver messages whole from the parser, however the bytes were split', () => {
    const { parser, messages } = collect();
    const packets = ['{"type":"ping"}', '{"type":"call","id":"1"}', '', 'ünïcödé'];
    const chunk = crypto.randomBytes(70_000);
    const wire = Buffer.concat([...packets.map((p) => frameText(p)), frame(KIND_BINARY, chunk), frameText('tail')]);
    // Every split size from single bytes to one big read.
    for (const size of [1, 2, 3, 4, 5, 6, 7, 13, 64, 4096, 65_536, wire.length]) {
      messages.length = 0;
      for (let offset = 0; offset < wire.length; offset += size) {
        parser.push(new Uint8Array(wire.subarray(offset, offset + size)));
      }
      assert.strictEqual(parser.pending, 0, `split ${size}: nothing left over`);
      assert.strictEqual(messages.length, packets.length + 2, `split ${size}: message count`);
      for (let i = 0; i < packets.length; i++) {
        assert.strictEqual(messages[i].kind, KIND_TEXT);
        assert.strictEqual(messages[i].data, packets[i]);
      }
      assert.strictEqual(messages[packets.length].kind, KIND_BINARY);
      assert.ok(messages[packets.length].data instanceof Uint8Array);
      assert.deepStrictEqual(Buffer.from(messages[packets.length].data), chunk);
      assert.strictEqual(messages[packets.length + 1].data, 'tail');
    }
  });

  it('should hand out a message contained in one read as a view over it, a spanning one as a copy', () => {
    const { parser, messages } = collect();
    const one = frame(KIND_BINARY, new Uint8Array([1, 2, 3]));
    parser.push(one);
    assert.strictEqual(messages[0].data.buffer, one.buffer);
    const two = frame(KIND_BINARY, new Uint8Array([4, 5, 6, 7]));
    parser.push(two.subarray(0, 6));
    parser.push(two.subarray(6));
    assert.notStrictEqual(messages[1].data.buffer, two.buffer);
    assert.deepStrictEqual(Array.from(messages[1].data), [4, 5, 6, 7]);
    // ArrayBuffer and other views are accepted as reads.
    parser.push(frameText('a').buffer.slice(0, HEADER_BYTES + 1));
    parser.push(new DataView(frameText('b').slice().buffer));
    assert.deepStrictEqual(
      messages.slice(2).map((m) => m.data),
      ['a', 'b'],
    );
  });

  it('should throw a FramingError on a bad header and reset the parser', () => {
    const { parser, messages } = collect({ maxMessage: 1024 });
    const unknownKind = frame(7, new Uint8Array(2));
    assert.throws(
      () => parser.push(unknownKind),
      (error) => error instanceof FramingError && error.code === 'kind',
    );
    assert.strictEqual(parser.pending, 0);
    const tooLarge = frame(KIND_BINARY, new Uint8Array(1025));
    assert.throws(
      () => parser.push(tooLarge.subarray(0, HEADER_BYTES)),
      (error) => error instanceof FramingError && error.code === 'too-large',
    );
    assert.throws(
      () => parser.push(frame(KIND_TEXT, new Uint8Array([0xff, 0xfe]))),
      (error) => error instanceof FramingError && error.code === 'utf8',
    );
    // Usable again after a reset: the next message parses.
    parser.push(frameText('ok'));
    assert.deepStrictEqual(messages, [{ kind: KIND_TEXT, data: 'ok' }]);
    assert.throws(() => new StreamParser({ maxMessage: 0, onMessage() {} }), TypeError);
    assert.throws(() => new StreamParser({}), TypeError);
    assert.throws(() => parser.push('text'), TypeError);
  });
});
