'use strict';

// The page half of the browser transport benchmark (`pnpm bench:browser
// transports` — scripts/bench-browser.js serves this page from
// http://127.0.0.1, a secure context, with one bundle as `wrpc`: the
// browser entry plus the WebRTC browser barrel as `wrpc.webrtc`, so both
// share one transport registry). wrpc's client in Chrome over:
//
//   ws      the browser's WebSocket to the Node Server
//   wt      the browser's WebTransport to the same Server's HTTP/3 host
//           (@fails-components/webtransport; only when the runner booted it)
//   webrtc  a data channel between two RTCPeerConnections in this page —
//           Chrome's own SCTP/DTLS on loopback, a PeerHost on the far end.
//           Both peers share this renderer, so the row pays both ends.
//
// Not discovered by bench/run-all.js (top-level files only).

/* global wrpc */

const WARMUP_MS = 300;
const MEASURE_MS = 1000;
const OPENS = 10;
const STREAM_BYTES = 16 * 1024 * 1024;
const LOAD_MS = 2000;
const CHUNK = 64 * 1024;
const BLOCK = new Uint8Array(CHUNK).fill(7);

const runFor = async (fn, ms) => {
  const end = performance.now() + ms;
  let iterations = 0;
  while (performance.now() < end) {
    await fn();
    iterations++;
  }
  return iterations;
};

const opsPerSec = async (fn, opsPerIteration = 1) => {
  await runFor(fn, WARMUP_MS);
  const started = performance.now();
  const iterations = await runFor(fn, MEASURE_MS);
  return Math.round((iterations * opsPerIteration * 1000) / (performance.now() - started));
};

const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
const round = (value, digits = 2) => Number(value.toFixed(digits));

const pump = async (writable, size) => {
  for (let sent = 0; sent < size; sent += CHUNK) {
    const piece = size - sent >= CHUNK ? BLOCK : BLOCK.subarray(0, size - sent);
    if (!writable.write(piece)) {
      if (writable.closed) throw new Error('the stream closed mid-upload');
      await new Promise((resolve) => writable.once('drain', resolve));
    }
  }
  writable.end();
};

const drain = async (readable) => {
  let bytes = 0;
  for await (const chunk of readable) bytes += chunk.length;
  return bytes;
};

// The in-page host the webrtc row calls: the same three procedures the Node
// Server answers (bench/support/transport-api.js), on a PeerHost.
const pageRouter = () => {
  const { defineRouter, procedure } = wrpc.webrtc;
  return defineRouter({
    bench: {
      echo: procedure({ access: 'public', handler: async (_context, args) => args }),
      echoStream: procedure({
        access: 'public',
        handler: async (context, { stream }) => {
          const bytes = await drain(context.client.getStream(stream));
          const back = context.client.createStream('back', bytes);
          pump(back, bytes);
          return back.id;
        },
      }),
      sink: procedure({
        access: 'public',
        handler: async (context, { stream }) => drain(context.client.getStream(stream)),
      }),
    },
  });
};

const opened = (channel) =>
  channel.readyState === 'open'
    ? Promise.resolve()
    : new Promise((resolve) => channel.addEventListener('open', resolve, { once: true }));

// Candidates from `from` reach `to` only once `to` has a remote description:
// addIceCandidate before one rejects, and a pair that lost every candidate
// on both sides never connects — it hung this benchmark now and then.
const trickle = (from, to) => {
  const held = [];
  let flushing = Promise.resolve();
  const flush = async () => {
    while (held.length > 0 && to.remoteDescription) await to.addIceCandidate(held.shift());
  };
  from.addEventListener('icecandidate', ({ candidate }) => {
    if (!candidate) return;
    held.push(candidate);
    flushing = flushing.then(flush);
  });
  return () => (flushing = flushing.then(flush));
};

const within = (promise, ms, what) =>
  Promise.race([
    promise,
    new Promise((_resolve, reject) => setTimeout(() => reject(new Error(`${what}: no answer in ${ms} ms`)), ms)),
  ]);

// Two peer connections in this page, one negotiated channel each, the
// offer/answer and candidates handed over by hand.
const rtcPair = async () => {
  const a = new RTCPeerConnection();
  const b = new RTCPeerConnection();
  const flushA = trickle(a, b);
  const flushB = trickle(b, a);
  const client = a.createDataChannel('wrpc', { negotiated: true, id: 0 });
  const host = b.createDataChannel('wrpc', { negotiated: true, id: 0 });
  await a.setLocalDescription();
  await b.setRemoteDescription(a.localDescription);
  await b.setLocalDescription();
  await a.setRemoteDescription(b.localDescription);
  await Promise.all([flushA(), flushB()]);
  await within(Promise.all([opened(client), opened(host)]), 10_000, 'the RTCPeerConnection pair');
  return {
    client,
    host,
    close: () => {
      a.close();
      b.close();
    },
  };
};

const connector = (kind, config) => {
  if (kind === 'ws') {
    return async () => ({ client: await wrpc.connect(config.wsUrl, { heartbeat: false }), close() {} });
  }
  if (kind === 'wt') {
    const value = Uint8Array.from(atob(config.wtHash), (c) => c.charCodeAt(0));
    const wt = { serverCertificateHashes: [{ algorithm: 'sha-256', value }] };
    return async () => ({
      client: await wrpc.connect(config.wtUrl, { transport: 'wt', wt, heartbeat: false }),
      close() {},
    });
  }
  const host = new wrpc.webrtc.PeerHost({ router: pageRouter() });
  let n = 0;
  return async () => {
    const pair = await rtcPair();
    const peer = `peer-${n++}`;
    host.attach(new wrpc.webrtc.RtcPeerTransport(pair.host, { peer }), { peer });
    const client = await wrpc.connect('webrtc:host', {
      transport: 'webrtc',
      channel: pair.client,
      reconnect: false,
      heartbeat: false,
    });
    return { client, close: pair.close };
  };
};

const measureKind = async (kind, config) => {
  const open = connector(kind, config);
  const result = { kind };

  // Open, load, first call.
  const opens = [];
  for (let i = 0; i < OPENS + 2; i++) {
    const start = performance.now();
    const connection = await open();
    await connection.client.load('bench');
    await connection.client.api.bench.echo({ i });
    if (i >= 2) opens.push(performance.now() - start);
    connection.client.close();
    connection.close();
  }
  opens.sort((a, b) => a - b);
  result.firstCall = round(percentile(opens, 50));

  const connection = await open();
  const { client } = connection;
  await client.load('bench');
  const echo = client.api.bench.echo;

  // Calls: the three columns of bench/rpc-comparison.js.
  const small = { name: 'Ada' };
  const large = { text: 'x'.repeat(10_000) };
  result.small = await opsPerSec(() => echo(small));
  result.large = await opsPerSec(() => echo(large));
  result.pipelined = await opsPerSec(() => {
    const calls = new Array(64);
    for (let i = 0; i < 64; i++) calls[i] = echo(small);
    return Promise.all(calls);
  }, 64);

  // 16 MiB up, 16 MiB back.
  {
    const start = performance.now();
    const upload = client.createStream('blob', STREAM_BYTES);
    const answer = client.api.bench.echoStream({ stream: upload.id });
    await pump(upload, STREAM_BYTES);
    const received = await drain(client.getStream(await answer));
    if (received !== STREAM_BYTES) throw new Error(`${kind}: ${received} bytes came back`);
    result.streamMiBps = round((2 * STREAM_BYTES) / (1024 * 1024) / ((performance.now() - start) / 1000), 1);
  }

  // A small call while uploads keep the connection busy.
  {
    const state = { done: false };
    const load = (async () => {
      const deadline = performance.now() + LOAD_MS;
      while (performance.now() < deadline) {
        const upload = client.createStream('load', STREAM_BYTES);
        const sunk = client.api.bench.sink({ stream: upload.id });
        await pump(upload, STREAM_BYTES);
        await sunk;
      }
    })().finally(() => {
      state.done = true;
    });
    const times = [];
    while (!state.done) {
      const start = performance.now();
      await echo(small);
      times.push(performance.now() - start);
    }
    await load;
    times.sort((a, b) => a - b);
    result.loaded = { p50: round(percentile(times, 50)), p99: round(percentile(times, 99)), calls: times.length };
  }

  client.close();
  connection.close();
  return result;
};

globalThis.__wrpcTransports = async (config) => {
  const results = [];
  for (const kind of config.kinds) {
    try {
      results.push(await measureKind(kind, config));
    } catch (error) {
      results.push({ kind, error: String(error?.stack ?? error) });
    }
  }
  return results;
};
