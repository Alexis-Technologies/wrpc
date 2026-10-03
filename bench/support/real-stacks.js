'use strict';

/**
 * The REAL stacks the transport benchmarks run wrpc over — the injections
 * tests/wt/fails.integration.test.js and
 * tests/webrtc/node-datachannel.integration.test.js make, gated the same way
 * so `pnpm bench` stays self-contained:
 *
 *   WRPC_WT=fails              @fails-components/webtransport — Google's
 *                              libquiche behind a native binding — for the
 *                              HTTP/3 host AND the Node client
 *   WRPC_RTC=node-datachannel  node-datachannel's W3C polyfill —
 *                              libdatachannel (usrsctp, libjuice) — for both
 *                              peers of a loopback pair
 *
 * Without the variable, or without a valid certificate, a row is SKIPPED
 * with its reason, never failed: bench/run-all.js counts a non-zero exit as
 * a broken benchmark. These packages are devDependencies used only by the
 * integration tests and these benchmarks; wrpc itself injects them.
 *
 *   node scripts/wt-cert.js certs   # the 14-day ECDSA certificate WT needs
 */

const fs = require('node:fs');
const path = require('node:path');
const { once } = require('node:events');
const { performance } = require('node:perf_hooks');

const { WrpcClient } = require('../../src/client.js');

const CERTS = process.env.WRPC_WT_CERTS
  ? path.resolve(process.env.WRPC_WT_CERTS)
  : path.join(__dirname, '..', '..', 'certs');

const wtSkip = () => {
  if (process.env.WRPC_WT !== 'fails') return 'set WRPC_WT=fails';
  const info = path.join(CERTS, 'wt-cert.json');
  if (!fs.existsSync(info)) return `no certificate: node scripts/wt-cert.js ${CERTS}`;
  const { notAfter } = JSON.parse(fs.readFileSync(info, 'utf8'));
  if (Date.parse(notAfter) < Date.now()) return `the certificate expired ${notAfter}: node scripts/wt-cert.js ${CERTS}`;
  return null;
};

const rtcSkip = () => (process.env.WRPC_RTC === 'node-datachannel' ? null : 'set WRPC_RTC=node-datachannel');

// A started HTTP/3 host on loopback and what a client needs to trust it.
const bootH3 = async () => {
  const { Http3Server, WebTransport, quicheLoaded } = await import('@fails-components/webtransport');
  await quicheLoaded;
  const { failsRequestCallback } = require('../../src/webtransport/index.js');
  const read = (name) => fs.readFileSync(path.join(CERTS, name), 'utf8');
  const info = JSON.parse(read('wt-cert.json'));
  const h3 = new Http3Server({
    port: 0,
    host: '127.0.0.1',
    secret: 'wrpc-bench',
    cert: read('wt-cert.pem'),
    privKey: read('wt-key.pem'),
  });
  h3.setRequestCallback(failsRequestCallback);
  h3.startServer();
  await h3.ready;
  const serverCertificateHashes = [{ algorithm: 'sha-256', value: Buffer.from(info.hash, 'base64') }];
  return { h3, port: h3.address().port, WebTransport, serverCertificateHashes, hash: info.hash };
};

// An HTTP/3 host whose sessions land in `server` — the same RpcServer the
// WebSocket clients of the same process reach. Returns what a client needs
// to dial it, and how to take it down.
const bootWt = async (server) => {
  const { acceptSessions } = require('../../src/webtransport/index.js');
  const { h3, port, WebTransport, serverCertificateHashes } = await bootH3();
  const acceptor = acceptSessions(server, h3.sessionStream('/api'));
  return {
    url: `https://127.0.0.1:${port}/api`,
    options: { transport: 'wt', wt: { WebTransport, serverCertificateHashes } },
    stop: async () => {
      await acceptor.stop();
      await h3.stopServer();
    },
  };
};

// Trickle candidates from `from` to `to`, held until `to` has a remote
// description — the helper tests/webrtc/portContract.js uses, for a pair
// whose signaling is two local variables.
const trickle = (from, to) => {
  const queue = [];
  let flushing = null;
  const flush = async () => {
    while (queue.length > 0 && to.remoteDescription) await to.addIceCandidate(queue.shift());
  };
  from.addEventListener('icecandidate', ({ candidate }) => {
    queue.push(candidate === null ? null : (candidate.toJSON?.() ?? candidate));
    flushing = (flushing ?? Promise.resolve()).then(flush);
  });
  return () => (flushing ?? Promise.resolve()).then(flush);
};

const opened = (channel) => (channel.readyState === 'open' ? Promise.resolve() : once(channel, 'open'));

// Two peer connections on loopback with one negotiated channel each,
// described to each other by hand: the raw-channel level of
// docs/guide/webrtc.md#your-own-connection. `client` goes to a WrpcClient,
// `host` to attachChannel().
const rtcPair = async () => {
  const { createW3cAdapter } = require('../../src/webrtc/port.js');
  const adapter = createW3cAdapter(require('node-datachannel/polyfill'));
  const a = adapter.createPeerConnection({ iceServers: [] });
  const b = adapter.createPeerConnection({ iceServers: [] });
  const client = a.createDataChannel('wrpc', { negotiated: true, id: 0 });
  const host = b.createDataChannel('wrpc', { negotiated: true, id: 0 });
  const flushA = trickle(a, b);
  const flushB = trickle(b, a);
  await a.setLocalDescription();
  await b.setRemoteDescription(a.localDescription);
  await b.setLocalDescription();
  await a.setRemoteDescription(b.localDescription);
  await flushA();
  await flushB();
  await Promise.all([opened(client), opened(host)]);
  return {
    client,
    host,
    close: () => {
      a.close();
      b.close();
    },
  };
};

// node-datachannel keeps a native thread alive until it is told to let go.
const rtcCleanup = () => {
  try {
    require('node-datachannel').cleanup();
  } catch {
    // never loaded
  }
};

// A WrpcClient into `server` over `kind` — 'ws', 'wt' or 'webrtc' — and
// how to take that one connection down. `wt` is a bootWt() result, shared
// by every connection of a run; a webrtc connection brings its own pair.
const connectOver = async (kind, server, { wt, port, ...options } = {}) => {
  if (kind === 'ws') {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`, options);
    return { client, close: () => client.close() };
  }
  if (kind === 'wt') {
    const client = await WrpcClient.connect(wt.url, { ...wt.options, ...options });
    return { client, close: () => client.close() };
  }
  if (kind === 'webrtc') {
    const { attachChannel } = require('../../src/webrtc/index.js');
    const pair = await rtcPair();
    attachChannel(server.rpc, pair.host);
    const client = await WrpcClient.connect('webrtc:server', {
      transport: 'webrtc',
      channel: pair.client,
      reconnect: false,
      ...options,
    });
    return {
      client,
      close: () => {
        client.close();
        pair.close();
      },
    };
  }
  throw new Error(`unknown transport kind ${kind}`);
};

// The same byte round trip with no wrpc at all — `bytes` up in `chunk`
// pieces, the same count back — over the bare stack, so a wrpc stream rate
// can be read against the stack's own. Resolves with the elapsed ms.
// ws: the `ws` package's own client and server (binary messages);
// wt: one bidirectional QUIC stream, written up, then answered down;
// webrtc: data-channel messages paced by bufferedAmountLowThreshold.
const rawRoundTrip = async (kind, bytes, chunk) => {
  const block = new Uint8Array(chunk).fill(7);
  if (kind === 'ws') {
    const http = require('node:http');
    const WebSocket = require('ws');
    const httpServer = http.createServer();
    const wss = new WebSocket.Server({ server: httpServer });
    wss.on('connection', (socket) => {
      let seen = 0;
      socket.on('message', async (data) => {
        seen += data.length;
        if (seen < bytes) return;
        for (let sent = 0; sent < bytes; sent += chunk) {
          socket.send(block);
          if (socket.bufferedAmount > 4 * 1024 * 1024) await new Promise((resolve) => setImmediate(resolve));
        }
      });
    });
    await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
    const socket = new WebSocket(`ws://127.0.0.1:${httpServer.address().port}/`);
    await once(socket, 'open');
    const start = performance.now();
    const back = new Promise((resolve) => {
      let got = 0;
      socket.on('message', (data) => {
        got += data.length;
        if (got >= bytes) resolve();
      });
    });
    for (let sent = 0; sent < bytes; sent += chunk) {
      socket.send(block);
      if (socket.bufferedAmount > 4 * 1024 * 1024) await new Promise((resolve) => setImmediate(resolve));
    }
    await back;
    const elapsed = performance.now() - start;
    socket.close();
    wss.close();
    httpServer.close();
    return elapsed;
  }
  if (kind === 'wt') {
    const { h3, port, WebTransport, serverCertificateHashes } = await bootH3();
    const sessions = h3.sessionStream('/raw').getReader();
    sessions.read().then(async ({ value: session }) => {
      await session.ready;
      const { value: stream } = await session.incomingBidirectionalStreams.getReader().read();
      const reader = stream.readable.getReader();
      for (let seen = 0; seen < bytes;) seen += (await reader.read()).value.byteLength;
      const writer = stream.writable.getWriter();
      for (let sent = 0; sent < bytes; sent += chunk) await writer.write(block);
      await writer.close();
    });
    const transport = new WebTransport(`https://127.0.0.1:${port}/raw`, { serverCertificateHashes });
    await transport.ready;
    const stream = await transport.createBidirectionalStream();
    const start = performance.now();
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();
    const back = (async () => {
      for (let got = 0; got < bytes;) got += (await reader.read()).value.byteLength;
    })();
    for (let sent = 0; sent < bytes; sent += chunk) await writer.write(block);
    await back;
    const elapsed = performance.now() - start;
    transport.close();
    await h3.stopServer();
    return elapsed;
  }
  if (kind === 'webrtc') {
    const pair = await rtcPair();
    const LOW = 1024 * 1024;
    const send = async (channel) => {
      channel.bufferedAmountLowThreshold = LOW;
      for (let sent = 0; sent < bytes; sent += chunk) {
        if (channel.bufferedAmount > 4 * LOW) await once(channel, 'bufferedamountlow');
        channel.send(block);
      }
    };
    const received = (channel) =>
      new Promise((resolve) => {
        let got = 0;
        channel.addEventListener('message', ({ data }) => {
          got += data.byteLength ?? data.length;
          if (got >= bytes) resolve();
        });
      });
    pair.host.binaryType = 'arraybuffer';
    pair.client.binaryType = 'arraybuffer';
    received(pair.host).then(() => send(pair.host));
    const back = received(pair.client);
    const start = performance.now();
    await send(pair.client);
    await back;
    const elapsed = performance.now() - start;
    pair.close();
    return elapsed;
  }
  throw new Error(`unknown transport kind ${kind}`);
};

module.exports = { wtSkip, rtcSkip, bootH3, bootWt, rtcPair, rtcCleanup, connectOver, rawRoundTrip };
