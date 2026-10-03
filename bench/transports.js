'use strict';
/**
 * wrpc over each transport, end to end: the same Server and the same client
 * over a WebSocket (wrpc's own engine), WebTransport (a real HTTP/3 session
 * — @fails-components/webtransport, libquiche on both ends) and a WebRTC
 * data channel (node-datachannel — libdatachannel on both peers of a
 * loopback pair). What bench/rpc-comparison.js's echo rows do not show:
 *
 *   open + load + first call   what a client pays before its first answer
 *                              (for webrtc: the pair's own ICE and DTLS too,
 *                              timed alone as well)
 *   16 MiB up + 16 MiB down    a binary stream round trip, next to the same
 *                              bytes over the bare stack with no wrpc on it
 *   small call under load      a call's latency idle, and while 16 MiB
 *                              uploads keep the connection busy for 2 s
 *
 * The rows over a native stack run only when asked for — the gate the
 * integration tests use (bench/support/real-stacks.js):
 *
 *   node scripts/wt-cert.js certs
 *   WRPC_WT=fails WRPC_RTC=node-datachannel node bench/transports.js
 *
 * Without them those rows say why they were skipped and the run still
 * exits 0 (bench/run-all.js counts any other exit as a broken benchmark).
 * Loopback has no packet loss, so what QUIC does about loss is not in these
 * numbers; read them as shapes on one machine, as everything in bench/.
 */
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const KINDS = ['ws', 'wt', 'webrtc'];
const WORKER = path.join(__dirname, 'support', 'transport-worker.js');

function run(kind) {
  const result = spawnSync(process.execPath, [WORKER, kind], { encoding: 'utf8', timeout: 10 * 60 * 1000 });
  // A native stack's warnings repeat once per session; say each one once.
  const warnings = new Map();
  for (const line of (result.stderr ?? '').split('\n')) {
    if (line.trim()) warnings.set(line, (warnings.get(line) ?? 0) + 1);
  }
  for (const [line, count] of warnings) console.error(`  [${kind}] ${line}${count > 1 ? `  (×${count})` : ''}`);
  if (result.status !== 0) {
    console.error(result.stdout);
    throw new Error(`transport "${kind}" exited with status ${result.status ?? result.signal}`);
  }
  const lines = result.stdout.trim().split('\n');
  const resultLine = lines.find((line) => line.startsWith('RESULT_JSON:'));
  for (const line of lines) if (line !== resultLine) console.log(line);
  return JSON.parse(resultLine.slice('RESULT_JSON:'.length));
}

function main() {
  console.log(`Node ${process.version} | ${new Date().toISOString()}\n`);
  const results = [];
  for (const kind of KINDS) {
    const result = run(kind);
    if (result.skipped) console.log(`${kind}: skipped — ${result.skipped}`);
    else results.push(result);
  }
  if (results.length === 0) return;
  console.log('\nSummary:\n');
  const header = ['transport', 'first call p50', 'stream MiB/s', 'bare stack', 'idle p50', 'loaded p50', 'loaded p99'];
  const rows = results.map((r) => [
    r.kind,
    `${r.firstCall.p50} ms`,
    String(r.streamMiBps),
    String(r.rawStreamMiBps),
    `${r.latency.idle.p50} ms`,
    `${r.latency.loaded.p50} ms`,
    `${r.latency.loaded.p99} ms`,
  ]);
  const widths = header.map((h, i) => Math.max(h.length, ...rows.map((row) => row[i].length)));
  const line = (cells) => cells.map((cell, i) => cell.padStart(widths[i])).join('  ');
  console.log(`  ${line(header)}`);
  for (const row of rows) console.log(`  ${line(row)}`);
  console.log();
}

main();
