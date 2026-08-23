/**
 * `Emitter.emit` — the hottest shared primitive in the project. It runs once
 * per inbound client message (src/client/transports.js), once per SSE event
 * (src/sse/client.js), and once per stream chunk (src/streams.js), and every
 * one of those sites has exactly ONE synchronous listener.
 *
 * The number that matters is the single-sync-listener row: it is what pays for
 * the hand-written fast path in src/utils.js, and it is what a future edit
 * would be giving up by collapsing that branch back into the generic path.
 *
 * The awaited rows measure dispatch + settle. The fire-and-forget row measures
 * what `void emitter.emit(...)` actually costs at a call site that never
 * awaits — the allocation, not the microtask drain — so it is batched inside a
 * single iteration with opsPerIteration, because the harness awaits fn().
 *
 * Run with `node bench/emitter.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');
const { Emitter } = require('../src/utils.js');

const BATCH = 1000;

let sink = 0;
const sync = (value) => {
  sink += value;
};

const main = async () => {
  console.log(`Emitter benchmark — Node ${process.version}\n`);

  {
    const emitter = new Emitter();
    let i = 0;
    await bench('emit awaited — 0 listeners', () => emitter.emit('m', i++));
  }
  {
    const emitter = new Emitter();
    emitter.on('m', sync);
    let i = 0;
    await bench('emit awaited — 1 sync listener', () => emitter.emit('m', i++));
  }
  {
    const emitter = new Emitter();
    emitter.on('m', async (value) => {
      sink += value;
    });
    let i = 0;
    await bench('emit awaited — 1 async listener', () => emitter.emit('m', i++));
  }
  {
    const emitter = new Emitter();
    emitter.on('m', sync);
    emitter.on('m', (value) => {
      sink -= value;
    });
    let i = 0;
    await bench('emit awaited — 2 sync listeners', () => emitter.emit('m', i++));
  }
  {
    // A lone `once` takes the slow path on purpose: the sweep may delete the
    // whole record, which the fast path is not allowed to do.
    let i = 0;
    await bench('emit awaited — lone once listener (re-armed)', () => {
      const emitter = new Emitter();
      emitter.once('m', sync);
      return emitter.emit('m', i++);
    });
  }
  {
    const emitter = new Emitter();
    emitter.on('m', () => {
      throw new Error('bench');
    });
    let i = 0;
    await bench('emit awaited — throwing listener (rejection path)', () =>
      emitter.emit('m', i++).then(
        () => {},
        () => {},
      ));
  }
  {
    // The `void emitter.emit(...)` shape: nothing awaits, so this is the
    // allocation cost alone.
    const emitter = new Emitter();
    emitter.on('m', sync);
    let i = 0;
    await bench(
      'emit fire-and-forget — 1 sync listener',
      () => {
        for (let n = 0; n < BATCH; n++) void emitter.emit('m', i++);
      },
      { opsPerIteration: BATCH, warmup: 20 },
    );
  }

  if (sink === Number.MIN_SAFE_INTEGER) console.log('unreachable', sink);
};

void main();
