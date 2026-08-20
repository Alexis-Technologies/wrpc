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

test('isCodec: structural — a packet half, a rest section, or both', () => {
  assert.strictEqual(isCodec({ encode: () => '', decode: () => ({}) }), true);
  assert.strictEqual(isCodec({ encode: () => '' }), false);
  assert.strictEqual(isCodec(null), false);
  assert.strictEqual(isCodec({ rest: { encode: () => '', decode: () => null } }), true, 'rest-only is a codec');
  assert.strictEqual(isCodec({ rest: { encode: () => '' } }), false, 'a declared rest section must be complete');
  assert.strictEqual(
    isCodec({ encode: () => '', decode: () => null, rest: { encode: () => '' } }),
    false,
    'a malformed rest section is refused even next to a valid packet half',
  );
  assert.strictEqual(isCodec({ rest: { encode: () => '', decode: () => null, contentType: 5 } }), false);
  assert.strictEqual(isCodec({ rest: { encode: () => '', decode: () => null, contentType: 'x/y' } }), true);
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

  await t.test('REST mode stays JSON without codec.rest — curl is its audience', async () => {
    const res = await fetch(`${base}/chat/send?text=q`);
    // The mode-blind Content-Type bug: the packet codec's type must never
    // ride on a REST body it did not frame.
    assert.notStrictEqual(res.headers.get('content-type'), 'application/x-wrpc-toy');
    const packet = await res.json(); // plain JSON, not codec-framed
    assert.deepStrictEqual(packet.result, { echoed: 'q' });
  });
});

// ---------------------------------------------------------------------------
// codec.rest: the opt-in BODY codec for REST mode. Values, not packets —
// and binary is fine here (whole HTTP bodies, no framing to collide with).

// A toy binary body codec: one prefix byte, then JSON bytes.
const toyRestCodec = () => {
  const stats = { encoded: 0, decoded: 0 };
  return {
    stats,
    rest: {
      contentType: 'application/x-wrpc-bin',
      encode: (value) => {
        stats.encoded++;
        return Buffer.concat([Buffer.from([0xab]), Buffer.from(JSON.stringify(value ?? null))]);
      },
      decode: (body) => {
        stats.decoded++;
        const buffer = Buffer.isBuffer(body) ? body : Buffer.from(body);
        if (buffer[0] !== 0xab) throw new Error('bad binary frame');
        return JSON.parse(buffer.subarray(1).toString());
      },
    },
  };
};

const restApi = () =>
  defineRouter({
    projects: {
      create: procedure({
        access: 'public',
        http: { method: 'POST', path: '/projects/:orgId', status: 201 },
        handler: async (_ctx, { params, body }) => ({ orgId: params.orgId, name: body?.name }),
      }),
      findById: procedure({
        access: 'public',
        http: { method: 'GET', path: '/projects/:id' },
        handler: async (_ctx, { params }) => ({ id: params.id }),
      }),
      boom: procedure({
        access: 'public',
        http: { method: 'GET', path: '/projects/:id/boom' },
        handler: async () => {
          const error = new Error('gone');
          error.code = 404;
          error.details = { why: 'archived' };
          throw error;
        },
      }),
      plain: procedure({ access: 'public', handler: async (_ctx, args) => ({ plain: true, args }) }),
    },
    misc: {
      // Deliberately outside every trie path: the conventional-mode probe.
      plain: procedure({ access: 'public', handler: async (_ctx, args) => ({ plain: true, args }) }),
    },
  });

test('codec.rest on the shell: binary bodies on both REST modes', async (t) => {
  const codec = toyRestCodec();
  const { server, port } = await bootServer(t, { router: restApi(), codec });
  const base = `http://127.0.0.1:${port}${server.rpc.basePath}`;
  const decodeRes = async (res) => codec.rest.decode(Buffer.from(await res.arrayBuffer()));

  await t.test('a declared route round-trips a binary request and response', async () => {
    const res = await fetch(`${base}/projects/42`, {
      method: 'POST',
      headers: { 'content-type': 'application/x-wrpc-bin' },
      body: codec.rest.encode({ name: 'Alpha' }),
    });
    assert.strictEqual(res.status, 201);
    assert.strictEqual(res.headers.get('content-type'), 'application/x-wrpc-bin');
    assert.deepStrictEqual(await decodeRes(res), { orgId: '42', name: 'Alpha' });
  });

  await t.test('an error body travels codec-framed, details included', async () => {
    const res = await fetch(`${base}/projects/1/boom`);
    assert.strictEqual(res.status, 404);
    assert.strictEqual(res.headers.get('content-type'), 'application/x-wrpc-bin');
    assert.deepStrictEqual(await decodeRes(res), { message: 'gone', code: 404, details: { why: 'archived' } });
  });

  await t.test('a 405 keeps its Allow header and frames its error body', async () => {
    const res = await fetch(`${base}/projects/42`, { method: 'PATCH' });
    assert.strictEqual(res.status, 405);
    assert.deepStrictEqual(res.headers.get('allow').split(', ').sort(), ['GET', 'POST']);
    assert.deepStrictEqual(await decodeRes(res), { message: 'Method Not Allowed', code: 405 });
  });

  await t.test('a malformed escape answers a framed 400', async () => {
    const res = await fetch(`${base}/projects/%ZZ`);
    assert.strictEqual(res.status, 400);
    assert.strictEqual((await decodeRes(res)).code, 400);
  });

  await t.test('a request body the codec cannot decode is the caller`s 400', async () => {
    const res = await fetch(`${base}/projects/42`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: 'not framed' }),
    });
    assert.strictEqual(res.status, 400);
  });

  await t.test('the conventional mode frames its callback envelope too', async () => {
    const res = await fetch(`${base}/misc/plain?x=1`);
    assert.strictEqual(res.headers.get('content-type'), 'application/x-wrpc-bin');
    const packet = await decodeRes(res);
    assert.strictEqual(packet.type, 'callback');
    assert.deepStrictEqual(packet.result, { plain: true, args: { x: '1' } });
  });

  await t.test('packet mode is untouched by a rest-only codec', async () => {
    const res = await fetch(base, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ type: 'call', id: '1', method: 'projects/plain', args: {} }),
    });
    assert.strictEqual(res.headers.get('content-type'), 'application/json');
    assert.deepStrictEqual((await res.json()).result, { plain: true, args: {} });
  });
});

test('codec.rest on the client: the REST leg speaks binary end to end', async (t) => {
  const { server, port } = await bootServer(t, { router: restApi(), codec: toyRestCodec() });
  const base = `http://127.0.0.1:${port}${server.rpc.basePath}`;

  await t.test('a mapped call round-trips through the client codec', async () => {
    const clientCodec = toyRestCodec();
    const client = await connectClient(t, base, { transport: 'http', codec: clientCodec });
    await client.load('projects');
    const created = await client.api.projects.create({ params: { orgId: '7' }, body: { name: 'B' } });
    assert.deepStrictEqual(created, { orgId: '7', name: 'B' });
    assert.ok(clientCodec.stats.encoded > 0, 'the request body went through encode');
    assert.ok(clientCodec.stats.decoded > 0, 'the response body went through decode');
  });

  await t.test('a wire error decodes into WrpcError with code and details', async () => {
    const client = await connectClient(t, base, { transport: 'http', codec: toyRestCodec() });
    await client.load('projects');
    await assert.rejects(client.api.projects.boom({ params: { id: '1' } }), (error) => {
      assert.strictEqual(error.code, 404);
      assert.strictEqual(error.message, 'gone');
      assert.deepStrictEqual(error.details, { why: 'archived' });
      return true;
    });
  });
});

test('the REST leg Content-Type: the packet codec never leaks; codec.rest owns it', async (t) => {
  const captured = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (_url, options) => {
    captured.push(options.headers);
    return { status: 200, text: async () => 'null', arrayBuffer: async () => new ArrayBuffer(0) };
  };
  t.after(() => void (globalThis.fetch = realFetch));
  const HttpTransport = WrpcClient.transport.http;
  const transport = new HttpTransport('http://x/api');
  // The mirror of the server's mode-blind bug: a PACKET codec on the
  // transport must not stamp its contentType onto a JSON REST-leg body.
  transport.codec = toyCodec();
  await transport.request('POST', 'http://x/api/things', '{"a":1}', undefined);
  assert.strictEqual(captured[0]['Content-Type'], 'application/json');
  const rest = { encode: () => new Uint8Array([1]), decode: () => null, contentType: 'application/x-bin' };
  await transport.request('POST', 'http://x/api/things', new Uint8Array([1]), undefined, rest);
  assert.strictEqual(captured[1]['Content-Type'], 'application/x-bin');
});
