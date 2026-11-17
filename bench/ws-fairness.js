/**
 * How long ONE WebSocket connection can hold the event loop — what every
 * other connection, and every timer, waits behind (src/websocket/connection.js,
 * `turnBudget`).
 *
 * Node reads a socket up to 32 times in one iteration of the loop, 64 KiB
 * each; the engine parsed every frame of what was read and dispatched it in
 * that same iteration. The two rows a peer can produce with nothing but a
 * socket:
 *
 *   - 40,000 small calls written in one go (2.4 MiB), `perMessageDeflate`
 *     on, each answer over the deflate threshold and so deflated
 *     synchronously on the way out;
 *   - 100 binary frames of 16 KiB that inflate to 16 MiB each (1.6 MiB on
 *     the wire) — inflated synchronously whatever `async` says, its
 *     threshold being the compressed size.
 *
 * Each row prints, without the budget and with it: the worst lateness of a
 * 5 ms timer while the burst is served (the stall another peer sees), and
 * how long the whole burst took (what the budget costs the peer that sent
 * it). One run, on the machine these numbers were taken on:
 *
 *                              timer late by     burst served in
 *   pipelined calls, no budget        567 ms            727 ms
 *   pipelined calls, budget            14 ms            724 ms
 *   inflating frames, no budget       229 ms            240 ms
 *   inflating frames, budget            2 ms            213 ms
 *
 * Not ops/sec, so not the harness: the measure is a latency under load.
 *
 * Run with `node bench/ws-fairness.js` or `pnpm bench`.
 */

'use strict';

const net = require('node:net');
const crypto = require('node:crypto');

const { Server, defineRouter, procedure } = require('../index.js');
const { Connection, Frame, OPCODES, RSV1 } = require('../src/websocket/ws.js');
const { compress } = require('../src/websocket/permessageDeflate.js');

const CALLS = 40_000;
const BOMBS = 100;
const TICK = 5;

const maskedText = (text) => {
  const frame = Frame.text(text);
  frame.maskPayload();
  return frame.toBuffer();
};

// The worst lateness of a TICK ms timer between start() and stop().
const canary = () => {
  let worst = 0;
  let last = 0;
  let timer = null;
  return {
    start() {
      last = performance.now();
      timer = setInterval(() => {
        const now = performance.now();
        worst = Math.max(worst, now - last - TICK);
        last = now;
      }, TICK);
    },
    stop() {
      clearInterval(timer);
      return worst;
    },
  };
};

const pipelined = async (turnBudget) => {
  const body = crypto.randomBytes(900).toString('base64'); // 1.2 KB: over the 1 KiB deflate threshold
  const router = defineRouter({ demo: { get: procedure({ access: 'public', handler: async () => body }) } });
  const ws = { perMessageDeflate: true, turnBudget };
  const server = new Server({ host: '127.0.0.1', port: 0, protocol: 'http', logger: false, router, ws });
  await server.listen();
  const { port } = server.address();
  const socket = net.connect({ port, host: '127.0.0.1' });
  await new Promise((resolve) => socket.once('connect', resolve));
  socket.write(
    `GET ${server.rpc.basePath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
      `Sec-WebSocket-Key: ${crypto.randomBytes(16).toString('base64')}\r\nSec-WebSocket-Version: 13\r\n` +
      'Sec-WebSocket-Extensions: permessage-deflate\r\n\r\n',
  );
  await new Promise((resolve) => socket.once('data', resolve));
  const frames = [];
  for (let i = 0; i < CALLS; i++) {
    frames.push(maskedText(JSON.stringify({ type: 'call', id: String(i), method: 'demo/get', args: {} })));
  }
  const blob = Buffer.concat(frames);
  // The burst is served once that many frames came back. Server frames are
  // unmasked and these are short: a 7-bit length, or 126 and sixteen bits.
  let answered = 0;
  let pending = Buffer.alloc(0);
  const count = (chunk) => {
    pending = pending.length === 0 ? chunk : Buffer.concat([pending, chunk]);
    let at = 0;
    while (pending.length - at >= 4) {
      const short = pending[at + 1] & 0x7f;
      const size = short === 126 ? 4 + pending.readUInt16BE(at + 2) : 2 + short;
      if (pending.length - at < size) break;
      at += size;
      answered++;
    }
    pending = pending.subarray(at);
  };
  const watch = canary();
  const started = performance.now();
  const served = new Promise((resolve) => {
    socket.on('data', (chunk) => {
      count(chunk);
      if (answered >= CALLS) resolve(performance.now() - started);
    });
  });
  watch.start();
  socket.write(blob);
  const elapsed = await served;
  const late = watch.stop();
  socket.destroy();
  await server.close();
  return { late, elapsed };
};

// A socket that hands the connection what a peer wrote, one frame a write —
// what a real socket does with frames this size — all in one turn.
const inflating = async (turnBudget) => {
  const listeners = new Map();
  const socket = {
    destroyed: false,
    writableLength: 0,
    on: (event, listener) => void listeners.set(event, listener),
    write: () => true,
    cork() {},
    uncork() {},
    pause() {},
    resume() {},
    end() {},
    destroy() {
      this.destroyed = true;
    },
  };
  const deflate = { response: '', threshold: 1, windowBits: 15, async: { threshold: 256 * 1024 } };
  const connection = new Connection(socket, Buffer.alloc(0), { deflate, turnBudget });
  let delivered = 0;
  const bomb = compress(Buffer.alloc(16 * 1024 * 1024 - 1024, 0x20));
  const frame = new Frame(true, OPCODES.BINARY, false, bomb, null, RSV1);
  frame.maskPayload();
  const wire = frame.toBuffer();
  // The connection unmasks in place: a copy for every frame it is given.
  const segment = Buffer.concat(Array.from({ length: BOMBS }, () => wire));
  const watch = canary();
  const started = performance.now();
  const served = new Promise((resolve) => {
    connection.on('message', () => {
      if (++delivered === BOMBS) resolve(performance.now() - started);
    });
  });
  watch.start();
  await new Promise((resolve) => setTimeout(resolve, TICK * 2));
  listeners.get('data')(segment);
  const elapsed = await served;
  await new Promise((resolve) => setTimeout(resolve, TICK * 2));
  const late = watch.stop();
  connection.terminate();
  return { late, elapsed };
};

const row = (name, { late, elapsed }) =>
  void console.log(`${name.padEnd(34)} ${late.toFixed(0).padStart(8)} ms ${elapsed.toFixed(0).padStart(14)} ms`);

const main = async () => {
  console.log(`WebSocket fairness benchmark — Node ${process.version}\n`);
  console.log(`${''.padEnd(34)} timer late by   burst served in`);
  await pipelined(true); // warm-up: the JIT, zlib, the loopback
  row(`${CALLS} pipelined calls, no budget`, await pipelined(false));
  row(`${CALLS} pipelined calls, budget`, await pipelined(true));
  row(`${BOMBS} inflating frames, no budget`, await inflating(false));
  row(`${BOMBS} inflating frames, budget`, await inflating(true));
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
