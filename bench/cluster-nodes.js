'use strict';

// The multi-node stand the paper's single-machine measurements lacked: N
// wrpc processes (bench/support/cluster-node-worker.js) over ONE Redis —
// backplane and session store — and M clients spread across them, so what
// is measured is a real fan-out through a real broker between real
// processes. Redis comes from `pnpm redis:up` (compose.yaml).
//
//   REDIS_URL=redis://127.0.0.1:6379 node bench/cluster-nodes.js
//
// Without REDIS_URL it prints one line and exits 0: bench/run-all.js runs
// every file here and must stay self-contained.
//
// Scenarios:
//   1. cross-instance emit — one instance emits to a room whose members sit
//      on every instance; rate, and the delivery latency distribution seen
//      by the remotest member (the timestamp rides the payload);
//   2. presence convergence — every client joins then leaves a room; how
//      long until the cluster's count on one instance agrees;
//   3. broadcast ask across instances;
//   4. an instance dies — its clients reconnect elsewhere with their
//      session token (no sticky routing), presence converges, and the loss
//      detector reports how many envelopes the survivors never saw.

const { fork } = require('node:child_process');
const path = require('node:path');

const { WrpcClient } = require('../src/client.js');
const { bearerAuth, memoryStore } = require('../src/auth/index.js');

const { REDIS_URL } = process.env;
const WORKER = path.join(__dirname, 'support', 'cluster-node-worker.js');
const NODES = Number(process.env.WRPC_NODES ?? 4);
const CLIENTS = Number(process.env.WRPC_CLIENTS ?? 200);
const MEASURE_MS = 1000;

const report = (name, count, elapsedMs, extra = '') =>
  console.log(
    `  ${name.padEnd(48)}${Math.round((count / elapsedMs) * 1000)
      .toLocaleString('en-US')
      .padStart(12)}/sec${extra}`,
  );

const percentile = (values, p) => {
  if (values.length === 0) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
};

// Every worker this run forked, killed on ANY exit — an orchestrator that
// died mid-run used to leave instances behind on the shared Redis, and a
// later run's same-named instances then argued with them over presence.
const children = new Set();
process.on('exit', () => {
  for (const child of children) child.kill('SIGKILL');
});
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => process.exit(1));

// Instance ids unique per run: two runs (or a leftover) must never share one.
const RUN = process.pid.toString(36);

const startNode = (instance) =>
  new Promise((resolve, reject) => {
    const child = fork(WORKER, [], { env: { ...process.env, WRPC_INSTANCE: instance }, stdio: 'inherit' });
    children.add(child);
    child.once('exit', () => children.delete(child));
    child.once('message', (message) => {
      if (message.type === 'ready') resolve({ child, port: message.port, instance });
      else reject(new Error(message.message ?? 'worker failed'));
    });
    child.once('exit', (code) => reject(new Error(`worker ${instance} exited with ${code}`)));
  });

const stopNode = (node) =>
  new Promise((resolve) => {
    node.child.once('exit', resolve);
    node.child.send({ type: 'stop' });
  });

const connect = async (node, user) => {
  const store = memoryStore();
  const client = await WrpcClient.connect(`ws://127.0.0.1:${node.port}/`, {
    heartbeat: false,
    reconnect: false,
    ...bearerAuth({ store, signIn: (c) => c.call('bench/login', { user }) }),
  });
  await client.load('bench');
  client.respond('bench/q', () => 1);
  return { client, store, node, user };
};

// `check` returns true when converged, or a value to report if it never does.
const waitUntil = async (check, timeoutMs = 10_000, label = 'convergence') => {
  const started = performance.now();
  let last;
  while (performance.now() - started < timeoutMs) {
    last = await check();
    if (last === true) return performance.now() - started;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error(`timed out waiting for ${label} (last observed: ${JSON.stringify(last)})`);
};

// A count probe: true at the target, the observed count otherwise.
const countIs = (client, room, target) => async () => {
  const count = await client.api.bench.count({ room });
  return count === target ? true : count;
};

const main = async () => {
  if (!REDIS_URL) {
    console.log('cluster-nodes: skipped (set REDIS_URL, see compose.yaml)');
    return;
  }
  console.log(
    `Multi-node benchmark — Node ${process.version}, ${NODES} instances, ${CLIENTS} clients, Redis at ${REDIS_URL}\n`,
  );
  console.log(`  ${'scenario'.padEnd(48)}${'rate'.padStart(16)}`);

  const nodes = [];
  for (let i = 0; i < NODES; i++) nodes.push(await startNode(`${RUN}-n${i}`));
  const peers = [];
  for (let i = 0; i < CLIENTS; i++) peers.push(await connect(nodes[i % NODES], `user-${i}`));
  const room = 'load';
  for (const peer of peers) await peer.client.api.bench.join({ room });
  // Presence needs a moment to replicate before the first count agrees.
  await waitUntil(countIs(peers[0].client, room, CLIENTS), 10_000, 'initial presence');

  // 1. Cross-instance emit: latency is measured at the LAST client of the
  // last instance — the longest hop through the broker.
  {
    const far = peers[peers.length - 1].client;
    const latencies = [];
    let received = 0;
    far.api.bench.on('tick', (data) => {
      received++;
      latencies.push(Date.now() - data.t);
    });
    let emits = 0;
    const started = performance.now();
    while (performance.now() - started < MEASURE_MS) {
      const before = received;
      await peers[0].client.api.bench.fanout({ room, data: { t: Date.now() } });
      emits++;
      await waitUntil(() => received > before || received, 2000, 'remote delivery');
    }
    const elapsed = performance.now() - started;
    report(
      `cross-instance emit ×${NODES} instances, ${CLIENTS} members`,
      emits,
      elapsed,
      ` latency p50 ${percentile(latencies, 50)} ms, p99 ${percentile(latencies, 99)} ms`,
    );
  }

  // 2. Presence convergence after a join/leave storm.
  {
    const storm = 'storm';
    const started = performance.now();
    await Promise.all(peers.map((peer) => peer.client.api.bench.join({ room: storm })));
    const joined = await waitUntil(countIs(peers[0].client, storm, CLIENTS), 15_000, 'presence after joins');
    await Promise.all(peers.map((peer) => peer.client.api.bench.leave({ room: storm })));
    const left = await waitUntil(countIs(peers[0].client, storm, 0), 15_000, 'presence after leaves');
    report(
      `presence: ${CLIENTS} joins + leaves across ${NODES} instances`,
      CLIENTS * 2,
      performance.now() - started,
      ` converged after ${joined.toFixed(0)} ms / ${left.toFixed(0)} ms`,
    );
  }

  // 3. Broadcast ask across instances.
  {
    let asks = 0;
    let answered = 0;
    let expected = 0;
    const started = performance.now();
    while (performance.now() - started < MEASURE_MS) {
      const result = await peers[0].client.api.bench.inquire({ room });
      answered += result.answers;
      expected += result.expected;
      asks++;
    }
    report(
      `broadcast ask across ${NODES} instances`,
      asks,
      performance.now() - started,
      ` ${(answered / asks).toFixed(1)} of ${(expected / asks).toFixed(1)} expected answers per ask`,
    );
  }

  // 4. An instance dies: its clients reconnect elsewhere with their
  // session token, presence converges, the loss detector speaks.
  {
    const victim = nodes[NODES - 1];
    const orphans = peers.filter((peer) => peer.node === victim);
    // Sessions were created on the victim; whoami on another instance is
    // the shared-store proof.
    const started = performance.now();
    await stopNode(victim);
    for (const peer of orphans) peer.client.close();
    const survivors = nodes.slice(0, NODES - 1);
    let restored = 0;
    await Promise.all(
      orphans.map(async (peer, i) => {
        const node = survivors[i % survivors.length];
        const client = await WrpcClient.connect(`ws://127.0.0.1:${node.port}/`, {
          heartbeat: false,
          reconnect: false,
          ...bearerAuth({ store: peer.store, signIn: () => null }),
        });
        await client.load('bench');
        if ((await client.api.bench.whoami()) === peer.user) restored++;
        await client.api.bench.join({ room });
        peer.client = client;
        peer.node = node;
      }),
    );
    const converged = await waitUntil(countIs(peers[0].client, room, CLIENTS), 15_000, 'presence after rehoming');
    const elapsed = performance.now() - started;
    let gaps = 0;
    for (const node of survivors) {
      const probe = peers.find((peer) => peer.node === node);
      gaps += await probe.client.api.bench.gaps();
    }
    report(
      `instance loss: ${orphans.length} clients rehomed`,
      orphans.length,
      elapsed,
      ` sessions restored ${restored}/${orphans.length}, presence converged after ${converged.toFixed(0)} ms, backplane gaps ${gaps}`,
    );
  }

  for (const peer of peers) peer.client.close();
  for (const node of nodes.slice(0, NODES - 1)) await stopNode(node);
  console.log();
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
