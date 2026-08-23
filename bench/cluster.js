'use strict';

// The cluster layer's costs, each against the guarantee it must keep:
//   - presence reads (count/presence) are local sums — nanoseconds, no I/O;
//   - presence deltas ride every join/leave — the overhead a clustered
//     registry adds to the hot membership path;
//   - plain emit() must NOT have paid for ask()'s existence: same
//     encode-once fan-out as before;
//   - broadcast ask() pays one stringify plus a per-recipient id suffix.
// Two RpcServers share a MemoryBackplane, so the numbers include the real
// envelope serialization, just not a broker's network.

const { EventEmitter } = require('node:events');

const { Connection } = require('../src/websocket/ws.js');
const { RpcServer } = require('../src/rpc/core.js');
const { defineRouter, procedure } = require('../src/rpc/router.js');
const { MemoryBackplane } = require('../src/scaling/index.js');

class SinkSocket extends EventEmitter {
  writableLength = 0;
  destroyed = false;
  bytes = 0;

  write(data) {
    this.bytes += data.length;
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

const report = (name, count, elapsedMs) => {
  const perSec = Math.round((count / elapsedMs) * 1000);
  console.log(`  ${name.padEnd(52)}${perSec.toLocaleString('en-US').padStart(14)}/sec`);
};

const router = () => defineRouter({ noop: { ping: procedure({ access: 'public', handler: async () => null }) } });

const attachMany = (rpc, count, room) => {
  const clients = [];
  for (let i = 0; i < count; i++) {
    const conn = new Connection(new SinkSocket(), Buffer.alloc(0), { maxBackpressure: 0 });
    const client = rpc.attachSocket(conn, { remoteAddress: `10.0.0.${i}` });
    if (room) client.join(room);
    clients.push(client);
  }
  return clients;
};

const settle = async (turns = 8) => {
  for (let i = 0; i < turns; i++) await Promise.resolve();
};

const main = async () => {
  console.log(`Cluster benchmark — Node ${process.version}\n`);
  console.log(`  ${'scenario'.padEnd(52)}${'rate'.padStart(18)}`);

  const backplane = new MemoryBackplane();
  const a = new RpcServer({ router: router(), logger: false, backplane, instanceId: 'a' });
  const b = new RpcServer({ router: router(), logger: false, backplane, instanceId: 'b' });
  attachMany(a, 100, 'load');
  attachMany(b, 100, 'load');
  await settle();

  // Presence reads: the numbers application code calls per render/handler.
  {
    const iterations = 2_000_000;
    let sink = 0;
    let started = performance.now();
    for (let i = 0; i < iterations; i++) sink += a.cluster.count('load');
    report('cluster.count() — 2 instances', iterations, performance.now() - started);
    started = performance.now();
    for (let i = 0; i < iterations / 10; i++) sink += a.cluster.presence('load').total;
    report('cluster.presence() — 2 instances', iterations / 10, performance.now() - started);
    if (sink < 0) throw new Error('unreachable');
  }

  // The membership hot path with deltas attached to it.
  {
    const [client] = attachMany(a, 1, null);
    const iterations = 200_000;
    const started = performance.now();
    for (let i = 0; i < iterations; i++) {
      client.join('churn');
      client.leave('churn');
    }
    report('join+leave with presence deltas', iterations, performance.now() - started);
  }

  // Plain emit() after ask() landed: the encode-once regression number,
  // directly comparable to send-path.js's fan-out rows.
  {
    const data = { text: 'x'.repeat(512), n: 42 };
    const iterations = 5_000;
    const started = performance.now();
    for (let i = 0; i < iterations; i++) a.to('load').emit('chat/message', data);
    report('room fan-out x100 emit (512 B)', iterations, performance.now() - started);
  }

  // The ask fan-out itself: one stringify, per-recipient id suffix, one
  // pending-answer slot each. Answers never arrive (sink sockets), so the
  // measured cost is the SEND side; the tiny timeout keeps slots bounded.
  {
    const data = { text: 'x'.repeat(512), n: 42 };
    const iterations = 2_000;
    const started = performance.now();
    for (let i = 0; i < iterations; i++) {
      a.to('load')
        .local()
        .ask('chat/poll', data, { timeout: 1 })
        .catch(() => {});
    }
    report('room fan-out x100 ask send-side (512 B)', iterations, performance.now() - started);
    // Let the 1 ms timeouts drain before closing.
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  await a.close();
  await b.close();
  backplane.close();
  console.log();
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
