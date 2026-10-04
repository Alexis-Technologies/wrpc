'use strict';

// The behavioural contract of a WebTransport channel end — what WtSocket
// (the server) and ClientWtTransport (the client) both promise, run over
// the fake by a harness each supplies (socket.test.js, client.test.js).
// The two are mirror copies of one another's send path, which is how a fix
// used to land on one end and skip the other. Not a *.test.js.
//
//   harness.open(sub, options) -> { world, end, peer }
//     end   { session, send(data, options?) -> boolean, stream(packet),
//             on(event, listener), bufferedAmount, compression, terminate() }
//     peer  peerEnd(session, stream): the other end of the control stream,
//           by hand — `received` (kinds and data past the capabilities),
//           `caps(text)` (its capabilities message), `session`.

const assert = require('node:assert');
const timers = require('node:timers/promises');
const zlib = require('node:zlib');

const {
  StreamParser,
  frame,
  frameCaps,
  KIND_CAPS,
  KIND_TEXT,
  KIND_TEXT_COMPRESSED,
} = require('../../src/webtransport/framing.js');
const { chunkEncode } = require('../../src/chunks.js');
const { waitFor } = require('../helpers/server.js');

const DEFLATE = 'deflate-raw';

// A codec that answers later: a CompressionStream's shape, which is what
// puts a message "in flight" and everything behind it in the queue.
const slowCodec = (delay = 5) => ({
  id: DEFLATE,
  threshold: 64,
  encode: async (bytes) => {
    await timers.setTimeout(delay);
    return zlib.deflateRawSync(bytes);
  },
  decode: async (bytes, max) => {
    await timers.setTimeout(delay);
    return zlib.inflateRawSync(bytes, { maxOutputLength: max });
  },
});

/** The peer end of a control stream: what the end under test sends, and a writer to talk back. */
const peerEnd = (session, stream) => {
  const received = [];
  const parser = new StreamParser({
    onMessage: (kind, data) => {
      if (kind !== KIND_CAPS) received.push({ kind, data });
    },
  });
  // Both compressed kinds are read here whatever was negotiated: the
  // contract checks what arrived, not what the parser would refuse.
  parser.compressed = true;
  void (async () => {
    const reader = stream.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) return;
        parser.push(value);
      }
    } catch {
      // The session closed under the read.
    }
  })();
  const writer = stream.writable.getWriter();
  return {
    session,
    received,
    /** Announces the peer's capabilities and waits for the end to have read them. */
    async caps(text) {
      await writer.write(frameCaps(text));
      await timers.setTimeout(5);
    },
    /** One frame of `kind` from the peer. */
    send: (kind, bytes) => writer.write(frame(kind, bytes)),
    /** The peer's FIN on the control stream — its graceful end. */
    finish: () => writer.close(),
  };
};

const text = (entry) => (entry.kind === KIND_TEXT_COMPRESSED ? zlib.inflateRawSync(entry.data).toString() : entry.data);

const runChannelContract = async (t, name, harness) => {
  const marks = { highWaterMark: 100, lowWaterMark: 20 };
  const drainsOf = (end) => {
    const drains = [];
    end.on('drain', () => drains.push(1));
    return drains;
  };

  await t.test(`${name}: a false on the control stream is followed by exactly one drain`, async (sub) => {
    const { world, end, peer } = await harness.open(sub, marks);
    const release = world.hold();
    const drains = drainsOf(end);
    assert.strictEqual(end.send('x'.repeat(50)), true);
    assert.strictEqual(end.send('y'.repeat(50)), false, 'past the high-water mark');
    assert.strictEqual(end.send('z'), false, 'still past it');
    assert.ok(end.bufferedAmount > 100);
    await timers.setImmediate();
    assert.deepStrictEqual(drains, []);
    release();
    await waitFor(() => drains.length === 1, 'drain');
    assert.strictEqual(end.bufferedAmount, 0);
    await waitFor(() => peer.received.length === 3, 'delivered');
    await timers.setTimeout(10);
    assert.strictEqual(drains.length, 1, 'one drain, not one per settled write');
  });

  await t.test(`${name}: a false from a side stream is followed by a drain`, async (sub) => {
    const { world, end, peer } = await harness.open(sub, marks);
    await peer.caps('{"streams":true}');
    end.stream({ type: 'stream', id: 'up', name: 'blob', size: 300 });
    await waitFor(() => end.session.uniOpened === 1, 'the chunks have a stream of their own');
    const release = world.hold();
    const drains = drainsOf(end);
    assert.strictEqual(end.send(chunkEncode('up', new Uint8Array(60))), true);
    assert.strictEqual(end.send(chunkEncode('up', new Uint8Array(60))), false, 'past the mark, on the side stream');
    assert.ok(end.bufferedAmount > 100, 'side-stream bytes count');
    await timers.setImmediate();
    assert.deepStrictEqual(drains, []);
    release();
    await waitFor(() => drains.length === 1, 'drain after a side-stream false');
    assert.strictEqual(end.bufferedAmount, 0);
  });

  await t.test(`${name}: chunks held for a stream still opening count, and drain once it flushes`, async (sub) => {
    const { world, end, peer } = await harness.open(sub, marks);
    await peer.caps('{"streams":true}');
    world.uniQuota = 'hang';
    end.stream({ type: 'stream', id: 'held', name: 'blob', size: 300 });
    // The open packet itself is taken by the session before the chunks are counted.
    await waitFor(() => end.bufferedAmount === 0, 'the open packet left');
    const drains = drainsOf(end);
    assert.strictEqual(end.send(chunkEncode('held', new Uint8Array(60))), true);
    assert.strictEqual(end.send(chunkEncode('held', new Uint8Array(60))), false, 'held chunks are buffered bytes');
    assert.strictEqual(end.bufferedAmount, 120);
    await timers.setTimeout(10);
    assert.deepStrictEqual(drains, [], 'nothing flushed while the open is parked');
    world.grant();
    await waitFor(() => drains.length === 1, 'drain once the stream opened and took them');
    assert.strictEqual(end.bufferedAmount, 0, 'counted once, not again when routed');
  });

  await t.test(
    `${name}: a compress in flight and a frame queued behind it answer false, then one drain`,
    async (sub) => {
      const { end, peer } = await harness.open(sub, { ...marks, compression: { codec: slowCodec(5) } });
      await peer.caps(JSON.stringify({ streams: true, enc: [DEFLATE] }));
      await waitFor(() => end.compression !== null, 'negotiated');
      const drains = drainsOf(end);
      const big = 'a'.repeat(2000);
      assert.strictEqual(end.send(big), false, 'in flight past the mark');
      assert.strictEqual(end.send('{"type":"ping"}'), false, 'queued behind it');
      await waitFor(() => peer.received.length === 2, 'both delivered');
      await waitFor(() => drains.length === 1, 'drain');
      await timers.setTimeout(10);
      assert.strictEqual(drains.length, 1);
      assert.strictEqual(end.bufferedAmount, 0);
      assert.deepStrictEqual(
        peer.received.map((entry) => entry.kind),
        [KIND_TEXT_COMPRESSED, KIND_TEXT],
      );
    },
  );

  await t.test(`${name}: an asynchronous codec keeps the wire in order`, async (sub) => {
    const { end, peer } = await harness.open(sub, { compression: { codec: slowCodec(3) } });
    await peer.caps(JSON.stringify({ streams: true, enc: [DEFLATE] }));
    await waitFor(() => end.compression !== null, 'negotiated');
    const sent = ['b'.repeat(500), '{"n":1}', 'c'.repeat(500), '{"n":2}', 'd'.repeat(500)];
    for (const message of sent) end.send(message);
    await waitFor(() => peer.received.length === sent.length, 'delivered');
    assert.deepStrictEqual(
      peer.received.map((entry) => entry.kind),
      [KIND_TEXT_COMPRESSED, KIND_TEXT, KIND_TEXT_COMPRESSED, KIND_TEXT, KIND_TEXT_COMPRESSED],
    );
    assert.deepStrictEqual(peer.received.map(text), sent);
  });

  await t.test(`${name}: what the peer sent before its FIN arrives, inflated by an asynchronous codec`, async (sub) => {
    const { end, peer } = await harness.open(sub, { compression: { codec: slowCodec(15) } });
    const received = [];
    end.on('message', (data, isBinary) => {
      if (isBinary !== true) received.push(String(data));
    });
    await peer.caps(JSON.stringify({ streams: true, enc: [DEFLATE] }));
    await waitFor(() => end.compression !== null, 'negotiated');
    const sent = Array.from({ length: 20 }, (_, i) =>
      JSON.stringify({ type: 'event', name: 'u/e', data: { i, pad: 'x'.repeat(200) } }),
    );
    for (const message of sent) await peer.send(KIND_TEXT_COMPRESSED, zlib.deflateRawSync(Buffer.from(message)));
    // The FIN right behind them, and the session closed — a server's
    // graceful close: the end used to shut its channel under every message
    // still inflating, and 0 of 20 arrived.
    await peer.finish();
    // A graceful close: the stream's data is read before the session goes
    // (closing a session resets its streams), as WtChannel's own close does.
    await timers.setTimeout(5);
    peer.session.close({ closeCode: 1000, reason: '' });
    await waitFor(() => received.length === sent.length, 'every message sent before the FIN');
    assert.deepStrictEqual(received, sent);
  });

  await t.test(
    `${name}: past maxBackpressure the session is terminated; one frame past the cap on an empty queue is not`,
    async (sub) => {
      const { world, end, peer } = await harness.open(sub, { ...marks, maxBackpressure: 200 });
      const release = world.hold();
      const errors = [];
      const closes = [];
      end.on('error', (error) => errors.push(error));
      end.on('close', () => closes.push(1));
      // Checked before the frame is queued, as the WebSocket engine counts
      // it: the first frame goes whatever its size, the next finds the queue
      // past the cap.
      assert.strictEqual(end.send('x'.repeat(300)), false, 'past the high mark, and sent');
      assert.strictEqual(closes.length, 0, 'one frame past the cap on an empty queue is not a fault');
      assert.strictEqual(end.send('y'), false, 'the queue is past the cap: terminated');
      await waitFor(() => closes.length === 1, 'terminated');
      await timers.setImmediate();
      assert.strictEqual(errors.length, 1);
      assert.strictEqual(errors[0].code, 'backpressure');
      assert.match(errors[0].message, /Backpressure limit exceeded/);
      assert.strictEqual(end.bufferedAmount, 0);
      release();
      await peer.session.closed;
      // Off: nothing is ever terminated for its queue.
      const open = await harness.open(sub, { ...marks, maxBackpressure: 0 });
      const hold = open.world.hold();
      const ended = [];
      open.end.on('close', () => ended.push(1));
      for (let i = 0; i < 20; i++) open.end.send('z'.repeat(100));
      assert.strictEqual(ended.length, 0);
      hold();
    },
  );

  await t.test(`${name}: maxBackpressure is validated at construction`, async (sub) => {
    for (const maxBackpressure of [-1, 1.5, '64mb']) {
      await assert.rejects(harness.open(sub, { maxBackpressure }), /maxBackpressure must be a non-negative integer/);
    }
  });

  await t.test(
    `${name}: bufferedAmount is 0 after terminate(), and a write settling late does not take it below`,
    async (sub) => {
      const { world, end, peer } = await harness.open(sub, marks);
      const release = world.hold();
      const drains = drainsOf(end);
      const closes = [];
      end.on('close', () => closes.push(1));
      assert.strictEqual(end.send('x'.repeat(50)), true);
      assert.ok(end.bufferedAmount >= 50, 'the frame is counted while the write is held');
      end.terminate();
      assert.strictEqual(end.bufferedAmount, 0);
      release();
      await timers.setTimeout(10);
      assert.strictEqual(end.bufferedAmount, 0, 'never negative');
      assert.deepStrictEqual(drains, [], 'no drain after the end');
      assert.strictEqual(closes.length, 1, 'one close');
      await peer.session.closed;
    },
  );
};

module.exports = { runChannelContract, peerEnd, slowCodec };
