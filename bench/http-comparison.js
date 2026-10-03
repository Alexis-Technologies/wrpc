'use strict';
/**
 * wrpc over plain HTTP next to the frameworks an HTTP API is usually built
 * on — fastify, express, tRPC's standalone adapter, and node:http with no
 * framework as the ceiling — the way fastify/benchmarks compares Node
 * frameworks: autocannon, 100 connections, 10 pipelined requests each, one
 * server process per stack, the load generator in this process.
 *
 * Two wrpc rows: a procedure with a declared REST route (`http: { method,
 * path }` — a real endpoint, plain JSON in and out) and packet mode (the call
 * packet a WebSocket carries, as one POST). Every server echoes the same
 * `{ name: 'Ada' }`; see bench/support/http-servers.js.
 *
 *   node bench/http-comparison.js
 *   WRPC_BENCH_DURATION=30 node bench/http-comparison.js   # seconds per stack (default 10)
 *
 * autocannon, fastify, express and @trpc/server are devDependencies used
 * only by the benchmarks; wrpc itself stays zero-dependency.
 */
const path = require('node:path');
const { spawn } = require('node:child_process');

const autocannon = require('autocannon');

const ORDER = ['wrpc-rest', 'wrpc-packet', 'fastify', 'express', 'trpc', 'node-http'];
const WORKER = path.join(__dirname, 'support', 'http-server-worker.js');
const DURATION = Number(process.env.WRPC_BENCH_DURATION ?? 10);
const CONNECTIONS = 100;
const PIPELINING = 10;

const boot = (key) =>
  new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [WORKER, key], { stdio: ['pipe', 'pipe', 'inherit'] });
    let out = '';
    child.stdout.on('data', (chunk) => {
      out += chunk;
      const line = out.split('\n').find((candidate) => candidate.startsWith('READY_JSON:'));
      if (line) resolve({ child, ...JSON.parse(line.slice('READY_JSON:'.length)) });
    });
    child.once('exit', (code) => reject(new Error(`http stack "${key}" exited with ${code} before it was ready`)));
  });

const load = ({ port, path: route, body }) =>
  autocannon({
    url: `http://127.0.0.1:${port}${route}`,
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    connections: CONNECTIONS,
    pipelining: PIPELINING,
    duration: DURATION,
  });

async function main() {
  console.log(
    `Node ${process.version} | ${new Date().toISOString()} | ` +
      `${CONNECTIONS} connections, ${PIPELINING} pipelined, ${DURATION} s per stack\n`,
  );
  const servers = require('./support/http-servers.js');
  const rows = [];
  for (const key of ORDER) {
    const server = await boot(key);
    try {
      await load({ ...server, duration: 2 }); // warm-up, not counted
      const result = await load(server);
      if (result.non2xx > 0 || result.errors > 0) {
        throw new Error(`"${key}" answered ${result.non2xx} non-2xx and ${result.errors} errors`);
      }
      const row = {
        key,
        label: servers[key].label,
        requests: Math.round(result.requests.average),
        latency: result.latency.average,
        p99: result.latency.p99,
        throughput: result.throughput.average / (1024 * 1024),
      };
      rows.push(row);
      console.log(
        `${row.label.padEnd(32)} ${row.requests.toLocaleString('en-US').padStart(9)} req/s  ` +
          `latency ${row.latency.toFixed(2)} ms (p99 ${row.p99} ms)  ${row.throughput.toFixed(1)} MiB/s`,
      );
    } finally {
      server.child.stdin.end();
      await new Promise((resolve) => server.child.once('exit', resolve));
    }
  }
  console.log(`\nRESULT_JSON:${JSON.stringify(rows)}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
