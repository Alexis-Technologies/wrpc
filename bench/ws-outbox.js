/**
 * The outbound ordering queue of a WebSocket connection — the frames that
 * wait behind a deflate running off the event loop (src/websocket/connection.js,
 * `#outbox`). It exists under `contextTakeover` for what the server sends and
 * under `async`; every send issued while a compress is in flight queues
 * behind it, and the queue is read from its head as each deflate lands.
 *
 * How long it gets is not the engine's to say: `maxBackpressure` bounds its
 * BYTES (100 MiB by default), so a burst of small messages — a subscription
 * replayed to one client, the answers to a pipelined run of calls — is as
 * many entries as fit. It used to be an array read with `shift()`, which
 * past a few thousand elements moves the rest on every take.
 *
 * The first rows are that array, filled to N and taken from the head. The
 * rest are the engine itself over the ring (`Deque` in src/deque.js): N
 * messages of 40 bytes sent in one synchronous burst to a connection in each
 * mode, timed until the last frame is on the socket — in messages per
 * second, so a queue that costs the same per message at any length prints
 * the same number in every row. Over the array one run printed, on the
 * machine that now prints about 32,000 in every row of the first line and
 * 49,000 in every row of the second:
 *
 *   burst                  8,000     64,000    256,000
 *   context takeover      26,600     16,100      6,650
 *   async                 38,500     19,400      7,100
 *
 * — 256,000 messages took 38 seconds, 34 of them CPU, where 8,000 at the
 * same rate would have made it 7. (The floor is zlib's: one deflate per
 * message, off the loop, about 30 µs.)
 *
 * Run with `node bench/ws-outbox.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');
const { Connection } = require('../src/websocket/ws.js');

const MESSAGE = 'x'.repeat(40);

let sink = 0;

const withShift = (n) => {
  const outbox = [];
  for (let i = 0; i < n; i++) outbox.push(MESSAGE);
  while (outbox.length > 0) sink += outbox.shift().length;
};

// What a Connection needs of a socket, and a count of what reached it.
const createSocket = () => ({
  written: 0,
  writableLength: 0,
  destroyed: false,
  on() {},
  write() {
    this.written++;
    return true;
  },
  cork() {},
  uncork() {},
  pause() {},
  resume() {},
  end() {},
  destroy() {
    this.destroyed = true;
  },
});

const MODES = {
  'context takeover': { serverTakeover: true, clientTakeover: false, async: null },
  async: { serverTakeover: false, clientTakeover: false, async: { threshold: 16 } },
};

const burst = (mode, n) =>
  new Promise((resolve) => {
    const socket = createSocket();
    const deflate = { response: '', threshold: 1, windowBits: 15, ...MODES[mode] };
    const connection = new Connection(socket, Buffer.alloc(0), { deflate });
    for (let i = 0; i < n; i++) connection.sendText(MESSAGE);
    const poll = () => {
      if (socket.written < n) return void setImmediate(poll);
      sink += socket.written;
      connection.terminate();
      resolve();
    };
    poll();
  });

const optionsFor = (n) => ({ opsPerIteration: n, warmup: 2, measureMs: 2000 });

const main = async () => {
  console.log(`WebSocket outbox benchmark — Node ${process.version}\n`);
  for (const n of [1024, 16_384, 65_536]) {
    await bench(`array shift() — ${n} queued, taken from the head`, () => withShift(n), {
      opsPerIteration: n,
      warmup: n > 1024 ? 2 : 200,
      measureMs: n > 1024 ? 2000 : 1000,
    });
  }
  for (const mode of Object.keys(MODES)) {
    for (const n of [8_000, 64_000, 256_000]) {
      await bench(`${mode} — a burst of ${n} messages`, () => burst(mode, n), optionsFor(n));
    }
  }
  if (sink < 0) console.log(sink);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
