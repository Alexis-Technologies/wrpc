'use strict';

// A preset dictionary built from the router (src/rpc/dictionary.js) against
// one-shot deflate with no history, at the sizes RPC messages come in. The
// small end is where a dictionary earns its keep — a repeated event shape
// compresses against field names and targets that are already in the
// window — and the numbers here are what set the dictionary codec's 64 B
// threshold against the plain codec's 1 KiB.

const { performance } = require('node:perf_hooks');

const { defineRouter, procedure } = require('../src/rpc/router.js');
const { buildDictionary } = require('../src/rpc/dictionary.js');
const { nativeCompressor } = require('../src/compression/index.js');
const { dictionaryCompressor } = require('../src/compression/dictionary.js');

const router = defineRouter({
  market: {
    quote: procedure({
      access: 'public',
      signature: { args: { symbol: 'string' }, returns: { bid: 'number', ask: 'number', ts: 'number' } },
      handler: async () => ({}),
    }),
    orders: procedure({
      access: 'public',
      signature: {
        args: { symbol: 'string', 'limit?': 'number' },
        returns: [{ id: 'string', side: 'string', price: 'number', size: 'number', createdAt: 'string' }],
      },
      handler: async () => [],
    }),
    emits: { tick: { data: { symbol: 'string', bid: 'number', ask: 'number', ts: 'number' } } },
  },
});

const encoder = new TextEncoder();
const tick = (i) =>
  JSON.stringify({
    type: 'event',
    name: 'market/tick',
    data: { symbol: 'BTC-USD', bid: 42000 + (i % 100), ask: 42001 + (i % 100), ts: 1726500000000 + i },
  });
const call = (i) => JSON.stringify({ type: 'call', id: `c${i}`, method: 'market/quote', args: { symbol: 'ETH-USD' } });
const orders = (rows) =>
  JSON.stringify({
    type: 'callback',
    id: 'c1',
    result: Array.from({ length: rows }, (_, i) => ({
      id: `o-${i}`,
      side: i % 2 ? 'buy' : 'sell',
      price: 42000 + i,
      size: (i % 7) + 1,
      createdAt: '2026-09-19T10:00:00.000Z',
    })),
  });

const report = (name, count, elapsedMs, extra = '') =>
  console.log(
    `  ${name.padEnd(46)}${Math.round((count / elapsedMs) * 1000)
      .toLocaleString('en-US')
      .padStart(12)}/sec${extra}`,
  );

const round = (label, codec, texts) => {
  const inputs = texts.map((t) => encoder.encode(t));
  let out = 0;
  let plainBytes = 0;
  const started = performance.now();
  for (let i = 0; i < inputs.length; i++) {
    out += codec.encode(inputs[i]).length;
    plainBytes += inputs[i].length;
  }
  const elapsed = performance.now() - started;
  const ratio = (plainBytes / out).toFixed(2);
  const per = `${Math.round(plainBytes / inputs.length)} -> ${Math.round(out / inputs.length)} B`;
  report(label, inputs.length, elapsed, `   ${ratio}x   ${per}`);
};

const main = () => {
  const dictionary = buildDictionary(router);
  console.log(`dictionary: ${dictionary.length} B from the router`);
  const plain = nativeCompressor();
  const dict = dictionaryCompressor(dictionary);
  const N = 20_000;
  const ticks = Array.from({ length: N }, (_, i) => tick(i));
  const calls = Array.from({ length: N }, (_, i) => call(i));
  console.log('one-shot deflate, no history');
  round('event, 108 B', plain, ticks);
  round('call, 84 B', plain, calls);
  round('callback, 12 rows', plain, [orders(12)].concat(Array(1999).fill(orders(12))));
  round('callback, 300 rows', plain, Array(200).fill(orders(300)));
  console.log('one-shot deflate with the router dictionary');
  round('event, 108 B', dict, ticks);
  round('call, 84 B', dict, calls);
  round('callback, 12 rows', dict, Array(2000).fill(orders(12)));
  round('callback, 300 rows', dict, Array(200).fill(orders(300)));
};

main();
