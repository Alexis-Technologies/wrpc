'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const crypto = require('node:crypto');

const {
  KIND_TEXT,
  KIND_BINARY,
  FLAG_FIN,
  HEADER_BYTES,
  MIN_MESSAGE_SIZE,
  MAX_MESSAGE_SIZE,
  DEFAULT_MAX_REASSEMBLY,
  FramingError,
  negotiateMessageSize,
  FrameEncoder,
  FrameDecoder,
} = require('../../src/webrtc/framing.js');

// The sink contract: a frame is only valid inside the call, so a test that
// wants to look at frames later copies them the way a channel's send() does.
const collect = () => {
  const frames = [];
  return { frames, sink: (frame) => frames.push(Uint8Array.from(frame)) };
};

const roundTrip = (limit, kind, input) => {
  const encoder = new FrameEncoder(limit);
  const decoder = new FrameDecoder();
  const { frames, sink } = collect();
  const count = kind === KIND_TEXT ? encoder.encodeText(input, sink) : encoder.encode(kind, input, sink);
  assert.strictEqual(count, frames.length);
  let out = null;
  for (let i = 0; i < frames.length; i++) {
    const result = decoder.push(frames[i].buffer);
    if (i < frames.length - 1) assert.strictEqual(result, null, `fragment ${i} must not complete the message`);
    else out = result;
  }
  assert.strictEqual(decoder.pending, 0);
  return { frames, out };
};

test('framing: header layout', () => {
  assert.strictEqual(HEADER_BYTES, 1);
  assert.strictEqual(KIND_TEXT, 0);
  assert.strictEqual(KIND_BINARY, 1);
  assert.strictEqual(FLAG_FIN, 0b10);
  const { frames } = collect();
  const encoder = new FrameEncoder(8);
  encoder.encode(KIND_BINARY, new Uint8Array([9, 9]), frames.push.bind(frames));
  assert.deepStrictEqual(Array.from(frames[0]), [KIND_BINARY | FLAG_FIN, 9, 9]);
  encoder.encodeText('ab', (frame) => frames.push(Uint8Array.from(frame)));
  assert.deepStrictEqual(Array.from(frames[1]), [KIND_TEXT | FLAG_FIN, 0x61, 0x62]);
});

test('framing: negotiateMessageSize', () => {
  assert.strictEqual(negotiateMessageSize(null), MIN_MESSAGE_SIZE);
  assert.strictEqual(negotiateMessageSize(undefined), MIN_MESSAGE_SIZE);
  assert.strictEqual(negotiateMessageSize({ maxMessageSize: 0 }), MIN_MESSAGE_SIZE);
  assert.strictEqual(negotiateMessageSize({ maxMessageSize: Infinity }), MIN_MESSAGE_SIZE);
  assert.strictEqual(negotiateMessageSize({ maxMessageSize: NaN }), MIN_MESSAGE_SIZE);
  assert.strictEqual(negotiateMessageSize({ maxMessageSize: 1 }), MIN_MESSAGE_SIZE, 'no room for a payload byte');
  assert.strictEqual(negotiateMessageSize({ maxMessageSize: 65536 }), 65536);
  assert.strictEqual(negotiateMessageSize({ maxMessageSize: 1024 }), 1024, 'under the floor is honoured, not raised');
  assert.strictEqual(negotiateMessageSize({ maxMessageSize: 1024 * 1024 * 1024 }), MAX_MESSAGE_SIZE, 'capped');
  assert.strictEqual(negotiateMessageSize({ maxMessageSize: 1024 * 1024 }, 64 * 1024), 64 * 1024, 'custom ceiling');
  assert.strictEqual(negotiateMessageSize(null, 8 * 1024), 8 * 1024, 'the floor never exceeds the ceiling');
});

test('framing: constructors validate', () => {
  assert.throws(() => new FrameEncoder(1), TypeError);
  assert.throws(() => new FrameEncoder(16.5), TypeError);
  assert.throws(() => new FrameDecoder({ maxReassembly: 0 }), TypeError);
  assert.strictEqual(new FrameEncoder(2).maxMessageSize, 2);
  assert.strictEqual(DEFAULT_MAX_REASSEMBLY, 16 * 1024 * 1024);
});

test('framing: binary round-trips at every boundary, at 16 KiB and 256 KiB', () => {
  for (const limit of [16 * 1024, 256 * 1024]) {
    const room = limit - HEADER_BYTES;
    for (const size of [0, 1, room - 1, room, room + 1, limit, 2 * room, 2 * room + 1, 3 * room + 7]) {
      const bytes = crypto.randomBytes(size);
      const { frames, out } = roundTrip(limit, KIND_BINARY, bytes);
      assert.strictEqual(frames.length, Math.max(1, Math.ceil(size / room)), `fragments for ${size} @ ${limit}`);
      for (const frame of frames) assert.ok(frame.length <= limit, 'every fragment fits the limit');
      assert.strictEqual(out.kind, KIND_BINARY);
      assert.ok(out.data instanceof Uint8Array);
      assert.strictEqual(Buffer.compare(Buffer.from(out.data), bytes), 0, `bytes for ${size} @ ${limit}`);
    }
  }
});

test('framing: text round-trips, multi-byte UTF-8 included, and the empty string', () => {
  const limit = 64;
  for (const text of ['', 'x', '{"type":"ping"}', 'ї'.repeat(20), '😀'.repeat(30), 'a'.repeat(1000)]) {
    const { out } = roundTrip(limit, KIND_TEXT, text);
    assert.strictEqual(out.kind, KIND_TEXT);
    assert.strictEqual(out.data, text);
  }
  // A string whose worst-case UTF-8 size exceeds the room but whose real
  // size fits still arrives as one fragment through the fallback path.
  const { frames, out } = roundTrip(32, KIND_TEXT, 'a'.repeat(20));
  assert.strictEqual(frames.length, 1);
  assert.strictEqual(out.data, 'a'.repeat(20));
});

test('framing: the single-fragment binary fast path is a view, not a copy', () => {
  const decoder = new FrameDecoder();
  const frame = new Uint8Array([KIND_BINARY | FLAG_FIN, 1, 2, 3]);
  const { data } = decoder.push(frame);
  assert.strictEqual(data.buffer, frame.buffer);
  assert.strictEqual(data.byteOffset, 1);
  assert.deepStrictEqual(Array.from(data), [1, 2, 3]);
  // Any view is accepted, as is a bare ArrayBuffer.
  const dv = new DataView(frame.buffer);
  assert.deepStrictEqual(Array.from(decoder.push(dv).data), [1, 2, 3]);
  assert.throws(() => decoder.push('nope'), TypeError);
  assert.throws(() => decoder.push(null), TypeError);
});

test('framing: the encoder reuses one scratch buffer — a retained frame is overwritten', () => {
  const encoder = new FrameEncoder(4);
  const retained = [];
  encoder.encode(KIND_BINARY, new Uint8Array([1, 2, 3, 4, 5, 6]), (frame) => retained.push(frame));
  assert.strictEqual(retained.length, 2);
  // Both retained views alias the same scratch memory: the last fragment wins.
  assert.strictEqual(retained[0].buffer, retained[1].buffer);
  assert.deepStrictEqual(Array.from(retained[1]), [KIND_BINARY | FLAG_FIN, 4, 5, 6]);
});

test('framing: malformed frames throw a coded FramingError and reset the decoder', () => {
  const decoder = new FrameDecoder({ maxReassembly: 8 });
  const expectError = (frame, code) => {
    assert.throws(
      () => decoder.push(frame),
      (error) => error instanceof FramingError && error.name === 'FramingError' && error.code === code,
      `expected code ${code}`,
    );
    assert.strictEqual(decoder.pending, 0, 'reset after an error');
  };
  expectError(new Uint8Array(0), 'empty');
  expectError(new Uint8Array([0b100 | FLAG_FIN, 1]), 'reserved');
  expectError(new Uint8Array([0b10000000, 1]), 'reserved');
  // A continuation whose KIND differs from the open message.
  assert.strictEqual(decoder.push(new Uint8Array([KIND_BINARY, 1, 2])), null);
  assert.strictEqual(decoder.pending, 2);
  expectError(new Uint8Array([KIND_TEXT | FLAG_FIN, 3]), 'kind');
  // Reassembly over the cap — on the first fragment and on a later one.
  expectError(new Uint8Array([KIND_BINARY, 1, 2, 3, 4, 5, 6, 7, 8, 9]), 'too-large');
  assert.strictEqual(decoder.push(new Uint8Array([KIND_BINARY, 1, 2, 3, 4, 5])), null);
  expectError(new Uint8Array([KIND_BINARY, 6, 7, 8, 9]), 'too-large');
  // Invalid UTF-8 in a text frame, single and reassembled.
  expectError(new Uint8Array([KIND_TEXT | FLAG_FIN, 0xff, 0xfe]), 'utf8');
  assert.strictEqual(decoder.push(new Uint8Array([KIND_TEXT, 0xc3])), null);
  expectError(new Uint8Array([KIND_TEXT | FLAG_FIN, 0x28]), 'utf8');
  // Still usable afterwards.
  assert.deepStrictEqual(decoder.push(new Uint8Array([KIND_TEXT | FLAG_FIN, 0x6f, 0x6b])), {
    kind: KIND_TEXT,
    data: 'ok',
    compressed: false,
  });
  // reset() drops a half message on request too.
  decoder.push(new Uint8Array([KIND_BINARY, 1]));
  decoder.reset();
  assert.strictEqual(decoder.pending, 0);
});

test('framing: a multi-byte character split across fragments reassembles', () => {
  const encoder = new FrameEncoder(3); // 2 payload bytes per fragment
  const decoder = new FrameDecoder();
  const { frames, sink } = collect();
  encoder.encodeText('є😀', sink); // 2 + 4 bytes -> 3 fragments
  assert.strictEqual(frames.length, 3);
  let out = null;
  for (const frame of frames) out = decoder.push(frame) ?? out;
  assert.deepStrictEqual(out, { kind: KIND_TEXT, data: 'є😀', compressed: false });
});
