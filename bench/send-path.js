'use strict';

// The send-side benchmarks the receive-side suite never had: frame writing,
// permessage-deflate, and — the one that matters for scale-out — room
// fan-out, where the encode-once work is graded. Everything runs in-process
// over sink sockets: what is measured is wrpc's own serialization and
// framing cost, not the kernel's.

const { EventEmitter } = require('node:events');

const { Connection } = require('../src/websocket/ws.js');
const { compress, decompress } = require('../src/websocket/permessageDeflate.js');
const { RpcServer } = require('../src/rpc/core.js');
const { defineRouter, procedure } = require('../src/rpc/router.js');

// A socket that swallows writes and counts bytes: the null sink every
// send-side measurement drains into.
class SinkSocket extends EventEmitter {
  writableLength = 0;
  destroyed = false;
  bytes = 0;
  writes = 0;

  write(data) {
    this.bytes += data.length;
    this.writes++;
    return true;
  }

  cork() {}
  uncork() {}
  pause() {}
  resume() {}
  end() {}
  destroy() {
    this.destroyed = true;
  }
}

const mb = (bytes) => bytes / (1024 * 1024);

const report = (name, count, elapsedMs, bytes) => {
  const perSec = Math.round((count / elapsedMs) * 1000);
  const throughput = bytes === undefined ? '' : `${((mb(bytes) / elapsedMs) * 1000).toFixed(0).padStart(8)} MB/s`;
  console.log(`  ${name.padEnd(46)}${perSec.toLocaleString('en-US').padStart(14)}/sec${throughput}`);
};

const benchSendText = (label, payload, options = {}) => {
  const socket = new SinkSocket();
  const conn = new Connection(socket, Buffer.alloc(0), { maxBackpressure: 0, ...options });
  const iterations = 200_000;
  const started = performance.now();
  for (let i = 0; i < iterations; i++) conn.sendText(payload);
  const elapsed = performance.now() - started;
  conn.terminate();
  report(label, iterations, elapsed, socket.bytes);
};

const benchDeflate = () => {
  const json = JSON.stringify({ type: 'event', name: 'chat/message', data: { text: 'x'.repeat(2048) } });
  const payload = Buffer.from(json);
  const iterations = 20_000;

  let started = performance.now();
  for (let i = 0; i < iterations; i++) compress(payload);
  report(
    'permessage-deflate compress (2 KB json)',
    iterations,
    performance.now() - started,
    payload.length * iterations,
  );

  const compressed = compress(payload);
  started = performance.now();
  for (let i = 0; i < iterations; i++) decompress(compressed, 1024 * 1024);
  report(
    'permessage-deflate decompress (2 KB json)',
    iterations,
    performance.now() - started,
    payload.length * iterations,
  );
};

// Room fan-out over sink sockets. THE baseline for encode-once: today every
// recipient pays its own JSON.stringify + utf8 encode (+ deflate when on),
// so the per-recipient cost is what this number exposes.
const benchFanout = async (clients, deflate) => {
  const router = defineRouter({ noop: { ping: procedure({ access: 'public', handler: async () => null }) } });
  const rpc = new RpcServer({ router, logger: false });
  const sockets = [];
  for (let i = 0; i < clients; i++) {
    const socket = new SinkSocket();
    const conn = new Connection(socket, Buffer.alloc(0), {
      maxBackpressure: 0,
      ...(deflate ? { deflate: { threshold: 1, windowBits: 15 } } : {}),
    });
    const client = rpc.attachSocket(conn, { remoteAddress: `10.0.0.${i}` });
    client.join('load');
    sockets.push(socket);
  }
  const data = { text: 'x'.repeat(512), n: 42 };
  const iterations = deflate ? 500 : 5_000;
  const started = performance.now();
  for (let i = 0; i < iterations; i++) rpc.to('load').emit('chat/message', data);
  const elapsed = performance.now() - started;
  const bytes = sockets.reduce((sum, socket) => sum + socket.bytes, 0);
  report(`room fan-out x${clients}${deflate ? ' +deflate' : ''} (512 B)`, iterations, elapsed, bytes);
  await rpc.close();
};

const main = async () => {
  console.log(`Send-path benchmark — Node ${process.version}\n`);
  console.log(`  ${'scenario'.padEnd(46)}${'rate'.padStart(18)}${'throughput'.padStart(12)}`);
  benchSendText('sendText 200 B', 'x'.repeat(200));
  benchSendText('sendText 4 KB', 'x'.repeat(4096));
  benchSendText('sendText 200 B +deflate', 'x'.repeat(200), { deflate: { threshold: 1, windowBits: 15 } });
  benchDeflate();
  await benchFanout(50, false);
  await benchFanout(200, false);
  await benchFanout(50, true);
  console.log();
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
