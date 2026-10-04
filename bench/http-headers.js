'use strict';
/**
 * What wRPC's default response headers cost `node:http` itself — the ceiling
 * row of bench/http-comparison.js, answering the same JSON echo three ways:
 *
 *   bare           Content-Type and Content-Length, as that row does
 *   wrpc defaults  the block every wRPC HTTP answer carries (buildHeaders:
 *                  CORS, strict-transport-security, x-content-type-options,
 *                  wrpc-version)
 *   no preflight   the same block without Access-Control-Allow-Methods and
 *                  -Allow-Headers, which a browser reads only on a
 *                  preflight's answer
 *
 * Building the block is ~20 ns (bench/cors-headers.js); what costs is node
 * validating and writing every header of every answer, and the bytes. Same
 * load as bench/http-comparison.js: autocannon, 100 connections × 10
 * pipelined, one server process per variant, rounds interleaved so drift
 * shows as spread rather than as a difference.
 *
 *   node bench/http-headers.js
 *   WRPC_BENCH_DURATION=30 node bench/http-headers.js   # seconds per variant (default 10)
 */
const http = require('node:http');
const { fork } = require('node:child_process');

const { buildHeaders } = require('../src/transport.js');

const DEFAULTS = buildHeaders(undefined, undefined, 2);
const WITHOUT_PREFLIGHT = {};
for (const name in DEFAULTS) {
  if (name !== 'Access-Control-Allow-Methods' && name !== 'Access-Control-Allow-Headers') {
    WITHOUT_PREFLIGHT[name] = DEFAULTS[name];
  }
}
const VARIANTS = {
  bare: { label: 'bare (as node:http in the comparison)', headers: { 'Content-Type': 'application/json' } },
  defaults: { label: "wRPC's default headers", headers: DEFAULTS },
  'no-preflight': { label: 'defaults minus the preflight-only two', headers: WITHOUT_PREFLIGHT },
};

const DURATION = Number(process.env.WRPC_BENCH_DURATION ?? 10);
const ROUNDS = 2;

const serve = (key) => {
  const base = VARIANTS[key].headers;
  const server = http.createServer((request, response) => {
    const chunks = [];
    request.on('data', (chunk) => chunks.push(chunk));
    request.on('end', () => {
      const body = JSON.stringify(JSON.parse(Buffer.concat(chunks).toString()));
      response.writeHead(200, { ...base, 'Content-Length': Buffer.byteLength(body) });
      response.end(body);
    });
  });
  server.listen(0, '127.0.0.1', () => process.send(server.address().port));
  process.on('disconnect', () => process.exit(0));
};

async function main() {
  const autocannon = require('autocannon');
  console.log(`Node ${process.version} | 100 connections, 10 pipelined, ${DURATION} s per variant\n`);
  for (let round = 1; round <= ROUNDS; round++) {
    for (const key of Object.keys(VARIANTS)) {
      const child = fork(__filename, ['serve', key]);
      const port = await new Promise((resolve) => child.once('message', resolve));
      const options = {
        url: `http://127.0.0.1:${port}/echo`,
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ name: 'Ada' }),
        connections: 100,
        pipelining: 10,
      };
      try {
        await autocannon({ ...options, duration: 2 }); // warm-up, not counted
        const result = await autocannon({ ...options, duration: DURATION });
        const requests = Math.round(result.requests.average);
        const bytes = Math.round(result.throughput.average / result.requests.average);
        console.log(
          `round ${round}  ${VARIANTS[key].label.padEnd(40)} ${requests.toLocaleString('en-US').padStart(9)} req/s  ` +
            `${bytes} B per answer`,
        );
      } finally {
        child.disconnect();
        await new Promise((resolve) => child.once('exit', resolve));
      }
    }
  }
}

if (process.argv[2] === 'serve') {
  serve(process.argv[3]);
} else {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
