'use strict';

// permessage-deflate three ways — the stateless one-shot default, a live
// context (context takeover) and the async threadpool path — on the kind of
// traffic each is for: a repeated JSON event (where a context earns its
// memory) and a large payload (where async earns its hand-off). The last
// block measures what async exists for: event-loop delay while a stream of
// large messages is compressed.

const { monitorEventLoopDelay } = require('node:perf_hooks');

const { compress, compressAsync } = require('../src/websocket/permessageDeflate.js');
const { DeflateContext } = require('../src/websocket/deflateContext.js');

const event = (i) =>
  JSON.stringify({
    type: 'event',
    name: 'market/tick',
    data: { symbol: 'BTC-USD', bid: 42000 + (i % 100), ask: 42001 + (i % 100), ts: 1726500000000 + i },
  });

const report = (name, count, elapsedMs, extra = '') =>
  console.log(
    `  ${name.padEnd(52)}${Math.round((count / elapsedMs) * 1000)
      .toLocaleString('en-US')
      .padStart(12)}/sec${extra}`,
  );

const oneShot = (payloads) => {
  let bytes = 0;
  const started = performance.now();
  for (let i = 0; i < payloads.length; i++) bytes += compress(payloads[i]).length;
  return { elapsed: performance.now() - started, bytes };
};

const takeover = async (payloads) => {
  const context = new DeflateContext({ windowBits: 15 });
  let bytes = 0;
  const started = performance.now();
  for (let i = 0; i < payloads.length; i++) {
    bytes += await new Promise((resolve, reject) =>
      context.compress(payloads[i], (error, out) => (error ? reject(error) : resolve(out.length))),
    );
  }
  const elapsed = performance.now() - started;
  context.close();
  return { elapsed, bytes };
};

const async = async (payloads) => {
  let bytes = 0;
  const started = performance.now();
  for (let i = 0; i < payloads.length; i++) {
    bytes += await new Promise((resolve, reject) =>
      compressAsync(payloads[i], 15, (error, out) => (error ? reject(error) : resolve(out.length))),
    );
  }
  return { elapsed: performance.now() - started, bytes };
};

const ratio = (payloads, bytes) => {
  const raw = payloads.reduce((sum, p) => sum + p.length, 0);
  return ` ratio ${(raw / bytes).toFixed(1)}x`;
};

const main = async () => {
  console.log(`permessage-deflate modes — Node ${process.version}\n`);
  const events = Array.from({ length: 5_000 }, (_, i) => Buffer.from(event(i)));
  let r = oneShot(events);
  report('repeated JSON event, one-shot (default)', events.length, r.elapsed, ratio(events, r.bytes));
  r = await takeover(events);
  report('repeated JSON event, context takeover', events.length, r.elapsed, ratio(events, r.bytes));
  r = await async(events);
  report('repeated JSON event, async threadpool', events.length, r.elapsed, ratio(events, r.bytes));

  for (const size of [4, 32, 256]) {
    const payloads = Array.from({ length: size >= 256 ? 100 : 500 }, () =>
      Buffer.from(JSON.stringify({ rows: Array.from({ length: size * 8 }, (_, i) => ({ i, v: i * 7 })) })),
    );
    r = oneShot(payloads);
    report(`${size} KB-class JSON, one-shot`, payloads.length, r.elapsed, ratio(payloads, r.bytes));
    r = await async(payloads);
    report(`${size} KB-class JSON, async threadpool`, payloads.length, r.elapsed, ratio(payloads, r.bytes));
  }

  // Event-loop delay under a BURST of large messages — a fan-out's worth
  // issued in one turn, as a broadcast does. Synchronous deflate blocks the
  // loop for the whole burst; async hands it to the threadpool and the
  // loop keeps serving every other connection meanwhile.
  const big = Buffer.from(JSON.stringify({ rows: Array.from({ length: 12288 }, (_, i) => ({ i, v: i * 7 })) }));
  const burst = 100;
  for (const mode of ['one-shot', 'async']) {
    const histogram = monitorEventLoopDelay({ resolution: 1 });
    histogram.enable();
    await new Promise((resolve) => setTimeout(resolve, 20));
    const started = performance.now();
    if (mode === 'one-shot') {
      for (let i = 0; i < burst; i++) compress(big);
      await new Promise((resolve) => setImmediate(resolve));
    } else {
      await Promise.all(
        Array.from(
          { length: burst },
          () =>
            new Promise((resolve, reject) => compressAsync(big, 15, (error) => (error ? reject(error) : resolve()))),
        ),
      );
    }
    const elapsed = performance.now() - started;
    histogram.disable();
    report(
      `${(big.length / 1024).toFixed(0)} KB burst ×${burst}, ${mode}`,
      burst,
      elapsed,
      ` loop delay max ${(histogram.max / 1e6).toFixed(0)} ms`,
    );
  }
  console.log();
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
