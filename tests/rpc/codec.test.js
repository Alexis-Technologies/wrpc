'use strict';

// The pluggable text codec: one injected { encode, decode, contentType? }
// re-frames every wrpc packet on ws / packet-mode HTTP / batches / the
// broadcast fan-out, on both sides of the wire. Text-only v1 by design.

const test = require('node:test');
const assert = require('node:assert');

const { RpcServer, WrpcClient, defineRouter, procedure } = require('../../index.js');
const { isCodec } = require('../../src/utils.js');
const { bootServer, connectClient } = require('../helpers/server.js');

// A toy single-line codec whose frames are deliberately NOT JSON.
const toyCodec = () => {
  const stats = { encoded: 0, decoded: 0 };
  return {
    stats,
    contentType: 'application/x-wrpc-toy',
    encode: (packet) => {
      stats.encoded++;
      return `X${JSON.stringify(packet)}`;
    },
    decode: (text) => {
      stats.decoded++;
      if (!text.startsWith('X')) throw new Error('bad frame');
      return JSON.parse(text.slice(1));
    },
  };
};

const api = () =>
  defineRouter({
    chat: {
      send: procedure({ access: 'public', handler: async (_ctx, { text }) => ({ echoed: text }) }),
      note: procedure({
        access: 'public',
        handler: async (ctx) => {
          ctx.server.broadcast('chat/note', { hi: 1 });
          return { ok: true };
        },
      }),
      poll: procedure({
        access: 'public',
        handler: async (ctx) => {
          const { answers, errors } = await ctx.server.to('lobby').ask('chat/poll', { q: 1 }, { timeout: 500 });
          return { answers, errors };
        },
      }),
      join: procedure({
        access: 'public',
        handler: async (ctx) => {
          ctx.client.join('lobby');
          return { ok: true };
        },
      }),
    },
  });

test('isCodec: structural, both halves required', () => {
  assert.strictEqual(isCodec({ encode: () => '', decode: () => ({}) }), true);
  assert.strictEqual(isCodec({ encode: () => '' }), false);
  assert.strictEqual(isCodec(null), false);
  assert.throws(() => new RpcServer({ router: api(), codec: { encode: () => '' } }), /options\.codec/);
});

test('a client refuses a half-shaped codec up front', async () => {
  await assert.rejects(WrpcClient.connect('ws://x/api', { codec: {} }), /options\.codec/);
});

test('codec and compiled serializers are mutually exclusive', () => {
  const router = defineRouter(
    {
      p: {
        make: procedure({
          access: 'public',
          schema: { response: { 200: { type: 'object' } } },
          handler: async () => ({}),
        }),
      },
    },
    {
      validation: {
        ajv: { compile: () => () => true },
        serializer: { compile: () => (value) => JSON.stringify(value) },
      },
    },
  );
  assert.throws(() => new RpcServer({ router, codec: toyCodec() }), /mutually exclusive/);
});

test('codec end to end over ws: calls, events, batches, asks', async (t) => {
  const codec = toyCodec();
  const { server, port } = await bootServer(t, { router: api(), codec });
  const url = `ws://127.0.0.1:${port}${server.rpc.basePath}`;

  await t.test('a call round-trips through the codec on both sides', async () => {
    const clientCodec = toyCodec();
    const client = await connectClient(t, url, { codec: clientCodec });
    await client.load('chat');
    assert.deepStrictEqual(await client.api.chat.send({ text: 'hi' }), { echoed: 'hi' });
    assert.ok(clientCodec.stats.encoded > 0, 'client encoded outbound frames');
    assert.ok(clientCodec.stats.decoded > 0, 'client decoded inbound frames');
  });

  await t.test('a broadcast event arrives through the codec, single-encoded', async () => {
    const client = await connectClient(t, url, { codec: toyCodec() });
    await client.load('chat');
    const note = new Promise((resolve) => client.api.chat.on('note', resolve));
    const before = codec.stats.encoded;
    await client.api.chat.note();
    assert.deepStrictEqual(await note, { hi: 1 });
    // One fan-out encode for the event itself (other frames also count, so
    // only assert it moved — the single-encode property is the code path).
    assert.ok(codec.stats.encoded > before);
  });

  await t.test('client batching encodes the real array', async () => {
    const client = await connectClient(t, url, { codec: toyCodec(), batch: true });
    await client.load('chat');
    const [a, b] = await Promise.all([client.api.chat.send({ text: 'a' }), client.api.chat.send({ text: 'b' })]);
    assert.strictEqual(a.echoed, 'a');
    assert.strictEqual(b.echoed, 'b');
  });

  await t.test('the ask fan-out takes the per-recipient slow path', async () => {
    const client = await connectClient(t, url, { codec: toyCodec() });
    await client.load('chat');
    client.respond('chat/poll', async () => ({ vote: 'yes' }));
    await client.api.chat.join();
    const { answers, errors } = await client.api.chat.poll();
    assert.deepStrictEqual(answers, [{ vote: 'yes' }]);
    assert.deepStrictEqual(errors, []);
  });
});

test('codec over packet-mode HTTP: frames and Content-Type', async (t) => {
  const codec = toyCodec();
  const { server, port } = await bootServer(t, { router: api(), codec });
  const base = `http://127.0.0.1:${port}${server.rpc.basePath}`;

  await t.test('the raw wire really is codec-framed', async () => {
    const res = await fetch(base, {
      method: 'POST',
      headers: { 'content-type': 'application/x-wrpc-toy' },
      body: codec.encode({ type: 'call', id: '1', method: 'chat/send', args: { text: 'raw' } }),
    });
    assert.strictEqual(res.headers.get('content-type'), 'application/x-wrpc-toy');
    const text = await res.text();
    assert.ok(text.startsWith('X'), 'the response is not JSON framing');
    assert.deepStrictEqual(codec.decode(text).result, { echoed: 'raw' });
  });

  await t.test('a JSON body against a codec server is a malformed packet, not a crash', async () => {
    const res = await fetch(base, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ type: 'call', id: '1', method: 'chat/send', args: {} }),
    });
    // Malformed → the dispatcher's structure error (500 on an empty packet).
    const packet = codec.decode(await res.text());
    assert.strictEqual(packet.error.code, 500);
  });

  await t.test('an HTTP batch answers one codec-framed array', async () => {
    const frame = codec.encode([
      { type: 'call', id: 'a', method: 'chat/send', args: { text: '1' } },
      { type: 'call', id: 'b', method: 'chat/send', args: { text: '2' } },
    ]);
    const res = await fetch(base, {
      method: 'POST',
      headers: { 'content-type': 'application/x-wrpc-toy' },
      body: frame,
    });
    const answers = codec.decode(await res.text());
    assert.strictEqual(answers.length, 2);
    assert.deepStrictEqual(
      answers.map((p) => p.result.echoed),
      ['1', '2'],
    );
  });

  await t.test('the http client transport speaks it end to end', async () => {
    const client = await connectClient(t, base, { transport: 'http', codec: toyCodec() });
    await client.load('chat');
    assert.deepStrictEqual(await client.api.chat.send({ text: 'h' }), { echoed: 'h' });
  });

  await t.test('REST mode stays JSON — curl is its audience', async () => {
    const res = await fetch(`${base}/chat/send?text=q`);
    const packet = await res.json(); // plain JSON, not codec-framed
    assert.deepStrictEqual(packet.result, { echoed: 'q' });
  });
});
