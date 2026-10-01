'use strict';

/**
 * The per-chunk paths of the WebTransport stream mux and the socket's idle
 * timer (src/webtransport/streams.js, socket.js) — what bench/wt-framing.js,
 * which stops at frame()/StreamParser, does not cover. Every binary stream
 * chunk on a session with side streams takes them, so a change there is
 * measured here before it is made:
 *
 *   outbound   (a) StreamMux.chunk(): the frame of a stream with its own
 *                  WebTransport stream, routed to it — the whole call
 *              (b) what reading the chunk header a second time costs in
 *                  it: readId() against `1 + frame[0]`
 *   inbound    (c) chunkEncode(id, bytes): what a read off a side stream is
 *                  re-framed with, per read — production
 *              (d) the same frame from a header cached per stream
 *   idle       (e) clearTimeout + setTimeout + unref on every read —
 *                  production
 *              (f) a timestamp per read and one timer that re-arms itself
 *
 * (b), (d) and (f) are the alternatives; the header says which of them the
 * code took and by how much they won on THIS machine when it was written:
 *
 *   (b) taken: chunk() decoded the id, and #route decoded it again for the
 *       offset alone — about 130 ns of a 1.1 µs call; the offset is one
 *       byte read. (a) went 867K -> 980K ops/sec at 1 KiB.
 *   (d) not taken: 13 % of the re-frame at 1 KiB, 7 % at 16 KiB — the
 *       payload copy is what the read costs, and removing THAT needs a seam
 *       in the dispatcher (a chunk handed over as id + payload), not a
 *       cache here.
 *   (f) not taken: about 95 ns a read against 40, on a path where the read
 *       itself is microseconds, behind an option that is off by default.
 *
 * (The harness times every call: about 70 ns of each row is its own.)
 *
 * Run: node bench/wt-streams.js (or as part of `pnpm bench`)
 */

const crypto = require('node:crypto');

const { bench } = require('./support/harness.js');
const { StreamMux, idHeader, readId } = require('../src/webtransport/streams.js');
const { chunkEncode } = require('../src/chunks.js');

const ID = 'c0ffee00-1234-4abc-8def-0123456789ab';
const SIZES = [
  ['1 KiB', 1024],
  ['16 KiB', 16 * 1024],
];

// A session whose unidirectional streams take a write at once: the mux's own
// work is what is timed, not a sink.
const session = () => ({
  createUnidirectionalStream: async () => ({
    getWriter: () => ({ write: async () => {}, close: async () => {}, abort: async () => {} }),
  }),
  incomingUnidirectionalStreams: { getReader() {} },
});

const openedMux = async () => {
  const mux = new StreamMux(session(), { emitPacket() {}, emitChunk() {}, onQueued() {}, onSent() {} });
  mux.peerCaps('{"streams":true}');
  mux.control({ type: 'stream', id: ID, name: 'blob', size: 1 });
  // The side stream opens on a later turn.
  await new Promise((resolve) => setImmediate(resolve));
  return mux;
};

let sunk = 0;

async function main() {
  const mux = await openedMux();
  console.log('outbound: one chunk frame routed to its side stream');
  for (const [label, size] of SIZES) {
    const frame = chunkEncode(ID, crypto.randomBytes(size));
    await bench(`  (a) mux.chunk(frame), ${label}`, () => {
      if (!mux.chunk(frame)) throw new Error('not routed');
    });
  }
  const frame = chunkEncode(ID, crypto.randomBytes(1024));
  await bench('  (b) readId(frame) — the header read again', () => {
    sunk += readId(frame).offset;
  });
  await bench('  (b) 1 + frame[0] — the offset alone', () => {
    sunk += 1 + frame[0];
  });

  console.log('\ninbound: one read off a side stream, re-framed for the dispatcher');
  const header = idHeader(ID);
  for (const [label, size] of SIZES) {
    const bytes = crypto.randomBytes(size);
    await bench(`  (c) chunkEncode(id, bytes), ${label} — production`, () => {
      sunk += chunkEncode(ID, bytes).length;
    });
    await bench(`  (d) cached header + payload, ${label}`, () => {
      const out = new Uint8Array(header.length + bytes.length);
      out.set(header, 0);
      out.set(bytes, header.length);
      sunk += out.length;
    });
  }

  console.log('\nidle timer: one read re-arming it');
  let timer = null;
  const fire = () => {};
  await bench('  (e) clearTimeout + setTimeout + unref — production', () => {
    clearTimeout(timer);
    timer = setTimeout(fire, 60_000);
    timer.unref();
  });
  clearTimeout(timer);
  let seen = 0;
  await bench('  (f) performance.now() into a field', () => {
    seen = performance.now();
  });
  sunk += seen;
  if (sunk < 0) throw new Error('unreachable');
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
