'use strict';

/**
 * WebRTC fan-out benchmark: `PeerHost.to(room).emit` over N data-channel
 * links — what `Mesh.broadcast` is.
 *
 * A WebSocket fan-out builds its frame once for every recipient
 * (bench/send-path.js). A data channel cannot share a frame — each link has
 * its own message size, so its own fragmentation — but everything BEFORE
 * the fragments is the same for every recipient: the JSON (already one per
 * emit), its UTF-8, and under compression the deflated body. This measures
 * an emit to 8 and 32 links, plain and compressed, at three payload sizes,
 * so the per-link share of that common work is a number.
 *
 * When ChannelCodec.sendShared was written (one machine; the ratios are what
 * carries over), an emit to 32 links cost, per recipient:
 *
 *                       512 B      4 KB       16 KB
 *   plain, before       0.51 µs    2.73 µs    10.3 µs   utf8 + frame per link
 *   plain, after        0.14 µs    0.53 µs    1.7 µs    utf8 once
 *   compressed, before  9.1 µs     15.9 µs    37.7 µs   one deflate per link
 *   compressed, after   0.44 µs    0.87 µs    2.2 µs    one deflate per emit
 *
 * (To 8 links: 0.67 -> 0.35, 3.5 -> 1.6, 13.2 -> 5.8 plain; 9.4 -> 1.5,
 * 16.6 -> 3.1, 40.5 -> 8.7 compressed — the once-per-emit work is spread
 * over fewer recipients. An emit to ONE link pays 0.2 µs for the sharing.)
 *
 * Run: node bench/rtc-fanout.js (or as part of `pnpm bench`)
 */

const { performance } = require('node:perf_hooks');

const { PeerHost } = require('../src/webrtc/host.js');
const { RtcPeerTransport } = require('../src/webrtc/transport.js');
const { defineRouter, procedure } = require('../src/rpc/router.js');
const { deflateCompressor } = require('../src/compression/codecs.js');

// A data channel that takes every message at once: the host's own work is
// what is timed, not a network.
class SinkChannel {
  id = 1;
  label = 'bench';
  readyState = 'open';
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;
  binaryType = 'arraybuffer';
  frames = 0;
  bytes = 0;

  send(data) {
    this.frames++;
    this.bytes += typeof data === 'string' ? data.length : data.byteLength;
  }

  close() {
    this.readyState = 'closed';
  }

  addEventListener() {}

  removeEventListener() {}
}

const payloadOf = (size) => {
  const rows = [];
  let n = 0;
  while (JSON.stringify(rows).length < size) {
    rows.push({
      id: n,
      name: `row-${n}`,
      email: `user${n}@example.com`,
      at: '2026-10-01T10:00:00.000Z',
      ok: n % 2 === 0,
    });
    n++;
  }
  return { rows };
};

const fanout = async (links, compression, label, size) => {
  const router = defineRouter({ noop: { ping: procedure({ access: 'public', handler: async () => null }) } });
  const host = new PeerHost({ router, logger: false });
  const channels = [];
  for (let i = 0; i < links; i++) {
    const channel = new SinkChannel();
    // The two sizes a mesh really meets: a browser's and a Node peer's.
    const transport = new RtcPeerTransport(channel, {
      peer: `peer-${i}`,
      maxMessageSize: i % 2 === 0 ? 65536 : 262144,
      maxBackpressure: 0,
      compression,
    });
    const client = host.attach(transport, { peer: `peer-${i}` });
    client.join('mesh:bench');
    channels.push(channel);
  }
  const data = payloadOf(size);
  const emit = () => host.to('mesh:bench').emit('chat/state', data);
  for (let i = 0; i < 200; i++) emit();
  let emits = 0;
  const started = performance.now();
  while (performance.now() - started < 700) {
    for (let i = 0; i < 20; i++) emit();
    emits += 20;
  }
  const elapsed = performance.now() - started;
  const perRecipient = (elapsed * 1000) / (emits * links);
  const wire = channels[0].bytes / channels[0].frames;
  console.log(
    `  ${`x${links} ${compression ? 'compressed' : 'plain'}, ${label}`.padEnd(30)}` +
      `${Math.round((emits / elapsed) * 1000)
        .toLocaleString('en-US')
        .padStart(10)} emits/sec` +
      `${perRecipient.toFixed(2).padStart(9)} µs/recipient` +
      `${Math.round(wire).toLocaleString('en-US').padStart(9)} B/frame`,
  );
  host.close?.();
};

const main = async () => {
  console.log(`WebRTC fan-out benchmark — Node ${process.version}\n`);
  for (const compression of [null, { codec: deflateCompressor(), threshold: 256 }]) {
    for (const links of [8, 32]) {
      for (const [label, size] of [
        ['512 B', 512],
        ['4 KB', 4096],
        ['16 KB', 16384],
      ]) {
        await fanout(links, compression, label, size);
      }
    }
    console.log();
  }
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
