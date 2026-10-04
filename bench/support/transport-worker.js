'use strict';
/**
 * Runs bench/transports.js's scenarios for ONE transport kind — 'ws', 'wt'
 * or 'webrtc' — in its own process, and prints its results as a
 * `RESULT_JSON:` line for the orchestrator. One process per kind for the
 * reason rpc-stack-worker.js gives: a native stack (libquiche,
 * libdatachannel) is loaded only where it is measured, and a thread it
 * holds past its close cannot keep the run alive.
 *
 * Every kind reaches the SAME Server (the wrpc-echo one) through the same
 * client; only the transport between them differs.
 */
const { performance } = require('node:perf_hooks');

const { createWrpcServer } = require('./wrpc-echo.js');
const { api, pump, CHUNK } = require('./transport-api.js');
const real = require('./real-stacks.js');

const WARM_OPENS = 3;
const OPENS = 20;
const STREAM_BYTES = 16 * 1024 * 1024;
const LOAD_MS = 2000;
const STREAM_RUNS = 3;
const IDLE_CALLS = 500;

const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
const round = (value, digits = 2) => Number(value.toFixed(digits));

async function main() {
  const kind = process.argv[2];
  const skipped = kind === 'wt' ? real.wtSkip() : kind === 'webrtc' ? real.rtcSkip() : null;
  if (skipped) {
    console.log(`RESULT_JSON:${JSON.stringify({ kind, skipped })}`);
    return;
  }
  const { server, port } = await createWrpcServer(api);
  const wt = kind === 'wt' ? await real.bootWt(server) : null;
  const open = () => real.connectOver(kind, server, { wt, port });
  const result = { kind };

  // 1. Open, load, first call — what a page pays before its first answer.
  // For webrtc it includes the pair's own offer/answer and ICE on loopback,
  // the part a signaling server would relay in a real deployment.
  {
    const times = [];
    for (let i = 0; i < WARM_OPENS + OPENS; i++) {
      const start = performance.now();
      const connection = await open();
      await connection.client.load('bench');
      await connection.client.api.bench.echo({ i });
      if (i >= WARM_OPENS) times.push(performance.now() - start);
      connection.close();
    }
    times.sort((a, b) => a - b);
    result.firstCall = { p50: round(percentile(times, 50)), p95: round(percentile(times, 95)) };
    console.log(`${kind}: open + load + first call  p50 ${result.firstCall.p50} ms  p95 ${result.firstCall.p95} ms`);
    if (kind === 'webrtc') {
      // How much of that is the pair itself: offer/answer, ICE and DTLS on
      // loopback, with no wrpc on it yet.
      const pairs = [];
      for (let i = 0; i < OPENS; i++) {
        const start = performance.now();
        const pair = await real.rtcPair();
        pairs.push(performance.now() - start);
        pair.close();
      }
      pairs.sort((a, b) => a - b);
      result.pairOnly = { p50: round(percentile(pairs, 50)), p95: round(percentile(pairs, 95)) };
      console.log(`${kind}: of which the pair alone   p50 ${result.pairOnly.p50} ms  p95 ${result.pairOnly.p95} ms`);
    }
  }

  const connection = await open();
  const { client } = connection;
  await client.load('bench');

  // 2. A 16 MiB upload read back as a 16 MiB download.
  {
    const rates = [];
    for (let run = 0; run < STREAM_RUNS; run++) {
      const start = performance.now();
      const upload = client.createStream('blob', STREAM_BYTES);
      const answer = client.api.bench.echoStream({ stream: upload.id });
      await pump(upload, STREAM_BYTES);
      const backId = await answer;
      let received = 0;
      for await (const chunk of client.getStream(backId)) received += chunk.length;
      if (received !== STREAM_BYTES) throw new Error(`${kind}: ${received} bytes came back`);
      rates.push((2 * STREAM_BYTES) / (1024 * 1024) / ((performance.now() - start) / 1000));
    }
    rates.sort((a, b) => a - b);
    result.streamMiBps = round(rates[Math.floor(rates.length / 2)], 1);
    // The same bytes over the bare stack, no wrpc: the stack's own ceiling.
    const raw = [];
    for (let run = 0; run < STREAM_RUNS; run++) {
      const elapsed = await real.rawRoundTrip(kind, STREAM_BYTES, CHUNK);
      raw.push((2 * STREAM_BYTES) / (1024 * 1024) / (elapsed / 1000));
    }
    raw.sort((a, b) => a - b);
    result.rawStreamMiBps = round(raw[Math.floor(raw.length / 2)], 1);
    console.log(
      `${kind}: 16 MiB up + 16 MiB down  ${result.streamMiBps} MiB/s ` +
        `(the bare stack: ${result.rawStreamMiBps} MiB/s; median of ${STREAM_RUNS})`,
    );
  }

  // 3. A small call's latency, idle and while uploads keep the connection
  // busy for LOAD_MS — 16 MiB uploads back to back, each paced by 'drain'.
  // Over one ordered byte stream (a WebSocket's TCP) a call queues behind
  // whatever chunks are already buffered; WebTransport gives each binary
  // stream a QUIC stream of its own.
  {
    const sample = async (count, until) => {
      const times = [];
      while (until ? !until() : times.length < count) {
        const start = performance.now();
        await client.api.bench.echo({ n: times.length });
        times.push(performance.now() - start);
      }
      return times.sort((a, b) => a - b);
    };
    const idle = await sample(IDLE_CALLS);
    let done = false;
    let uploaded = 0;
    const load = (async () => {
      const deadline = performance.now() + LOAD_MS;
      while (performance.now() < deadline) {
        const upload = client.createStream('load', STREAM_BYTES);
        const sunk = client.api.bench.sink({ stream: upload.id });
        await pump(upload, STREAM_BYTES);
        if ((await sunk) !== STREAM_BYTES) throw new Error(`${kind}: the sink saw a short upload`);
        uploaded += STREAM_BYTES;
      }
    })().finally(() => {
      done = true;
    });
    const loaded = await sample(0, () => done);
    await load;
    result.latency = {
      idle: { p50: round(percentile(idle, 50), 3), p99: round(percentile(idle, 99), 3) },
      loaded: { p50: round(percentile(loaded, 50), 3), p99: round(percentile(loaded, 99), 3), calls: loaded.length },
      uploadMiB: uploaded / (1024 * 1024),
    };
    console.log(
      `${kind}: small call  idle p50 ${result.latency.idle.p50} ms p99 ${result.latency.idle.p99} ms | ` +
        `under upload load p50 ${result.latency.loaded.p50} ms p99 ${result.latency.loaded.p99} ms ` +
        `(${loaded.length} calls while ${result.latency.uploadMiB} MiB went up)`,
    );
  }

  connection.close();
  await wt?.stop();
  await server.close();
  if (kind === 'webrtc') real.rtcCleanup();
  console.log(`RESULT_JSON:${JSON.stringify(result)}`);
  setTimeout(() => process.exit(), 2000).unref();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
