/**
 * `overCap` — the rule that admits a chunk of a binary stream under
 * `maxStreamBuffer` (src/streams.js), called once per chunk by a server's
 * dispatcher and by a client.
 *
 * What it costs depends on who reads the stream. A reader that keeps up is
 * the hot case and takes no sum at all: two property reads and a return.
 * The sum over the connection's open streams (`heldBytes`) is taken only for
 * a chunk of a stream nobody reads yet, or — where the carrier cannot pause —
 * of one whose reader is behind; it is one addition per open stream, and a
 * connection has `maxStreams` of them at most (256 by default).
 *
 * The alternative was a count kept in step: every readable reporting its
 * reads and its end back to its owner, on every chunk of every stream, to
 * save the rows at the bottom of this table — which are the rows of a peer
 * that opened 256 streams and read none.
 *
 * Run with `node bench/stream-buffer.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');
const { WrpcReadable, overCap, heldBytes } = require('../src/streams.js');

const CAP = 64 * 1024 * 1024;
const CHUNK = 64 * 1024;

// `count` open streams with a chunk in each, the first of them returned.
const connection = async (count, read) => {
  const streams = new Map();
  for (let i = 0; i < count; i++) {
    const stream = new WrpcReadable(`s${i}`, 'bench', Number.MAX_SAFE_INTEGER);
    await stream.push(new Uint8Array(16));
    await stream.push(new Uint8Array(16));
    // A reader that took one chunk and has one waiting: keeping up.
    if (read) await stream.read();
    streams.set(stream.id, stream);
  }
  return { streams, stream: streams.get('s0') };
};

let sink = 0;

const main = async () => {
  console.log(`Stream buffer benchmark — Node ${process.version}\n`);
  {
    const { streams, stream } = await connection(1, true);
    await bench('overCap — a stream being read, 1 open (paced)', () => {
      if (overCap(streams, stream, CHUNK, CAP, true) !== null) sink++;
    });
    await bench('overCap — a stream being read, 1 open (carrier cannot pause)', () => {
      if (overCap(streams, stream, CHUNK, CAP, false) !== null) sink++;
    });
  }
  {
    const { streams, stream } = await connection(256, true);
    await bench('overCap — a stream being read, 256 open', () => {
      if (overCap(streams, stream, CHUNK, CAP, true) !== null) sink++;
    });
  }
  for (const count of [1, 16, 256]) {
    const { streams, stream } = await connection(count, false);
    await bench(`overCap — a stream nobody reads, ${count} open`, () => {
      if (overCap(streams, stream, CHUNK, CAP, true) !== null) sink++;
    });
  }
  {
    const { streams } = await connection(256, false);
    await bench('heldBytes — 256 open streams, summed', () => {
      sink += heldBytes(streams, false);
    });
  }
  if (sink < 0) console.log(sink);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
