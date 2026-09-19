'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const zlib = require('node:zlib');

const {
  defineRouter,
  procedure,
  RpcServer,
  WrpcClient,
  buildDictionary,
  dictionaryCompressor,
} = require('../../index.js');
const { MAX_DICTIONARY } = require('../../src/rpc/dictionary.js');
const { dictionaryId, nativeCompressor, isCompressor } = require('../../src/compression/index.js');
const { MemoryBroker, attachBrokerRpc } = require('../../broker.js');
const { HEADER_ENC, HEADER_KIND, KIND } = require('../../src/broker/rpc/frames.js');
const { quiet } = require('../broker/support.js');

const decoder = new TextDecoder();

// `schema` on a procedure needs an ajv-shaped compiler injected; the
// dictionary reads the schema, never runs it.
const validation = { ajv: { compile: () => Object.assign(() => true, { errors: null }) } };

const appRouter = () =>
  defineRouter(
    {
      market: {
        quote: procedure({
          access: 'public',
          signature: {
            args: { symbol: 'string', 'depth?': 'number' },
            returns: { bid: 'number', ask: 'number', ts: 'number' },
          },
          handler: async () => ({ bid: 1, ask: 2, ts: 3 }),
        }),
        book: procedure({
          access: 'public',
          schema: {
            body: {
              type: 'object',
              properties: {
                symbol: { type: 'string' },
                levels: { type: 'array', items: { type: 'object', properties: { price: {}, size: {} } } },
              },
            },
          },
          handler: async () => [],
        }),
        ticks: procedure.subscription({
          access: 'public',
          signature: { data: { symbol: 'string', last: 'number' } },
          handler: async function* () {},
        }),
        on: {
          watch: procedure({ access: 'public', signature: { args: { symbols: 'string[]' } }, handler: async () => {} }),
        },
        emits: { tick: { data: { symbol: 'string', bid: 'number', ask: 'number', ts: 'number' } } },
      },
    },
    { validation },
  );

test('buildDictionary: deterministic, ordered least to most frequent, and made of what the router declares', () => {
  const a = buildDictionary(appRouter());
  const b = buildDictionary(appRouter());
  assert.deepStrictEqual(a, b, 'two instances of the same router build the same bytes');
  assert.strictEqual(dictionaryId(a), dictionaryId(b));
  const text = decoder.decode(a);
  // Field names from signatures, schemas, the inbound handler and the declared event.
  for (const key of ['"symbol":', '"depth":', '"bid":', '"levels":', '"price":', '"last":', '"symbols":']) {
    assert.ok(text.includes(key), `${key} declared somewhere in the router`);
  }
  // Targets and event names as they appear on the wire.
  assert.ok(text.includes('"method":"market/quote"'));
  assert.ok(text.includes('"method":"market/ticks"'));
  assert.ok(text.includes('"name":"market/tick"'), 'a declared outbound event');
  assert.ok(text.includes('"name":"market/watch"'), 'an inbound handler');
  // The skeletons close the dictionary: the most frequent bytes last.
  assert.ok(text.endsWith('{"type":"ping"}{"type":"pong"}'));
  assert.ok(text.indexOf('"symbol":') < text.indexOf('"method":"market/quote"'));
  assert.ok(text.indexOf('"method":"market/quote"') < text.indexOf('{"type":"callback","id":"'));
  assert.throws(() => buildDictionary({}), /introspect/);
  assert.throws(() => buildDictionary(appRouter(), { limit: 0 }), /limit/);
});

test('buildDictionary: past the cap the front is cut and the tail — the frequent end — kept', () => {
  const properties = {};
  for (let i = 0; i < 6000; i++) properties[`field_${i.toString(36).padStart(4, '0')}`] = { type: 'string' };
  const huge = defineRouter(
    { big: { thing: procedure({ access: 'public', schema: { body: { properties } }, handler: async () => 1 }) } },
    { validation },
  );
  const dictionary = buildDictionary(huge);
  assert.strictEqual(dictionary.length, MAX_DICTIONARY);
  const text = decoder.decode(dictionary);
  assert.ok(text.endsWith('{"type":"pong"}'));
  assert.ok(text.includes('"method":"big/thing"'));
  assert.ok(!text.includes('"field_0000":'), 'the earliest, least frequent keys are what went');
  assert.strictEqual(buildDictionary(huge, { limit: 512 }).length, 512);
});

test('dictionaryCompressor: a Compressor whose id carries the dictionary, interoperable with zlib', () => {
  const dictionary = buildDictionary(appRouter());
  const codec = dictionaryCompressor(dictionary);
  assert.strictEqual(isCompressor(codec), true);
  assert.strictEqual(codec.id, `deflate-raw+dict:${dictionaryId(dictionary)}`);
  assert.strictEqual(codec.threshold, 64);
  assert.strictEqual(dictionaryCompressor(dictionary, { threshold: 16, level: 9 }).threshold, 16);
  const event = Buffer.from(
    JSON.stringify({
      type: 'event',
      name: 'market/tick',
      data: { symbol: 'BTC-USD', bid: 42000.5, ask: 42001, ts: 1726500000000 },
    }),
  );
  const encoded = codec.encode(event);
  assert.deepStrictEqual(new Uint8Array(codec.decode(encoded, event.length)), new Uint8Array(event));
  // zlib with the same dictionary reads it, and the plain codec's output reads back through it.
  assert.deepStrictEqual(zlib.inflateRawSync(encoded, { dictionary }), event);
  const plain = nativeCompressor();
  assert.deepStrictEqual(new Uint8Array(codec.decode(plain.encode(event), event.length)), new Uint8Array(event));
  // The point: with the history preloaded, the event is far smaller than one-shot deflate makes it.
  const plainSize = plain.encode(event).length;
  assert.ok(
    encoded.length < plainSize * 0.6,
    `${event.length} B event: ${plainSize} B plain one-shot, ${encoded.length} B with the dictionary`,
  );
  assert.throws(() => codec.decode(encoded, 8), /larger than|ERR_BUFFER_TOO_LARGE|exceed/i);
  // Another dictionary is another id — and cannot read this one's output.
  const other = dictionaryCompressor(Buffer.from('nothing alike'));
  assert.notStrictEqual(other.id, codec.id);
  assert.throws(() => dictionaryCompressor(new Uint8Array(0)), /must not be empty/);
  assert.throws(() => dictionaryCompressor(dictionary, { threshold: -1 }), /threshold/);
  assert.throws(() => dictionaryCompressor(dictionary, { level: 12 }), /level/);
  assert.throws(() => dictionaryCompressor(42), /bytes or a string/);
});

test('dictionaryId: FNV-1a over the bytes, 16 hex characters, sensitive to every byte', () => {
  const a = dictionaryId(new Uint8Array([1, 2, 3]));
  assert.match(a, /^[0-9a-f]{16}$/);
  assert.strictEqual(a, dictionaryId(new Uint8Array([1, 2, 3])));
  assert.notStrictEqual(a, dictionaryId(new Uint8Array([1, 2, 4])));
  assert.notStrictEqual(a, dictionaryId(new Uint8Array([1, 2, 3, 0])));
});

// The dictionary on a real carrier: the broker binding, both ends with the
// same router, then with routers that differ.
const spied = (broker) => {
  const sent = [];
  const send = broker.direct.send;
  const direct = {
    ...broker.direct,
    send: (address, body, options) => {
      sent.push({ size: body.length, headers: options?.headers ?? {} });
      return send(address, body, options);
    },
  };
  return { sent, broker: { name: broker.name, direct, close: () => broker.close() } };
};

test('dictionary on the broker binding: negotiated by id — the same router on both ends compresses, a different one stays plain', async (t) => {
  const router = appRouter();
  const { broker, sent } = spied(new MemoryBroker({ logger: quiet }));
  const rpc = new RpcServer({ router, logger: false, sse: false });
  const codec = dictionaryCompressor(buildDictionary(router));
  const handle = await attachBrokerRpc(rpc, broker, { service: 'market', logger: quiet, compression: { codec } });
  t.after(async () => {
    await handle.stop();
    await rpc.close();
    broker.close();
  });
  const connect = (compression) =>
    WrpcClient.connect('broker://market', {
      transport: 'broker',
      broker,
      mode: 'session',
      compression,
      heartbeat: false,
      reconnect: false,
      logger: false,
    });

  const same = await connect({ codec: dictionaryCompressor(buildDictionary(appRouter())) });
  t.after(() => void same.close());
  const welcome = sent.find((m) => m.headers[HEADER_KIND] === KIND.WELCOME);
  assert.strictEqual(welcome.headers[HEADER_ENC], codec.id, 'the ids agree: the same router, the same bytes');
  await same.load('market');
  sent.length = 0;
  assert.deepStrictEqual(await same.api.market.quote({ symbol: 'BTC-USD' }), { bid: 1, ask: 2, ts: 3 });
  // Even a small callback travels compressed under the 64 B threshold.
  const answer = sent.find((m) => m.headers[HEADER_KIND] === KIND.PACKET && m.headers[HEADER_ENC] === codec.id);
  assert.ok(answer, 'the callback left with the dictionary codec');

  const differentRouter = defineRouter({ other: { thing: procedure({ access: 'public', handler: async () => 1 }) } });
  sent.length = 0;
  const different = await connect({ codec: dictionaryCompressor(buildDictionary(differentRouter)) });
  t.after(() => void different.close());
  const welcome2 = sent.find((m) => m.headers[HEADER_KIND] === KIND.WELCOME);
  assert.strictEqual(
    welcome2.headers[HEADER_ENC],
    undefined,
    'another dictionary: nothing agreed, the wire stays plain',
  );
  await different.load('market');
  assert.deepStrictEqual(await different.api.market.quote({ symbol: 'ETH-USD' }), { bid: 1, ask: 2, ts: 3 });
});
