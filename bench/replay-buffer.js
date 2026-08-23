'use strict';

// The ring buffers in EventLog (src/rpc/subscriptions.js) and SseChannel
// (src/sse/server.js): both replay buffers used to drop their oldest entry
// with Array#shift on a per-event path. This file is the measurement those
// comments cite: push+shift at steady state versus the preallocated
// slots+head+count ring, at the default capacity of 100 — where the shift
// cost is also NOT monotonic in size (V8's left-trimming threshold), so a
// config value silently changed the per-event cost.

const { bench } = require('./support/harness.js');

const CAPACITY = 100;
const BATCH = 1000;

const run = async () => {
  {
    // Steady state: the buffer is full, every push evicts.
    const entries = [];
    for (let i = 0; i < CAPACITY; i++) entries.push({ id: i, data: 'x' });
    let n = CAPACITY;
    await bench(
      'push + shift (old shape)',
      async () => {
        for (let i = 0; i < BATCH; i++) {
          entries.push({ id: n++, data: 'x' });
          if (entries.length > CAPACITY) entries.shift();
        }
        return entries.length;
      },
      { opsPerIteration: BATCH },
    );
  }

  {
    const slots = new Array(CAPACITY);
    let head = 0;
    let count = 0;
    let n = 0;
    await bench(
      'ring: slots + head + count (new shape)',
      async () => {
        for (let i = 0; i < BATCH; i++) {
          const entry = { id: n++, data: 'x' };
          if (count < CAPACITY) {
            slots[(head + count) % CAPACITY] = entry;
            count++;
          } else {
            slots[head] = entry;
            head = (head + 1) % CAPACITY;
          }
        }
        return count;
      },
      { opsPerIteration: BATCH },
    );
  }
};

run();
