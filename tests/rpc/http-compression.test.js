'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const zlib = require('node:zlib');

const { defineRouter, procedure, RpcServer } = require('../../index.js');
const { bootServer } = require('../helpers/server.js');
const { normalizeCompression, pickEncoding } = require('../../src/contentEncoding.js');
const { hasZstd } = require('../../src/compression/native.js');

// A result comfortably past the 1 KiB default threshold, and one under it.
const big = { rows: Array.from({ length: 64 }, (_, i) => ({ id: i, name: `row-${i}`, tags: ['a', 'b', 'c'] })) };
const small = { ok: true };

const router = defineRouter({
  data: {
    big: procedure({ access: 'public', handler: async () => big }),
    small: procedure({ access: 'public', handler: async () => small }),
    echo: procedure({ access: 'public', handler: async (_ctx, args) => args }),
    cached: procedure({
      access: 'public',
      http: { method: 'GET', path: '/cached', cache: { maxAge: 60 } },
      handler: async () => big,
    }),
    encoded: procedure({
      access: 'public',
      http: { method: 'GET', path: '/encoded', headers: { 'Content-Encoding': 'identity' } },
      handler: async () => big,
    }),
  },
});

const packet = (method, args = {}) => JSON.stringify({ type: 'call', id: 'c1', method, args });

// Raw `fetch` with explicit encodings, and the body as BYTES — node's fetch
// inflates a gzip body by itself, which is the point for users and exactly
// what these assertions must see through.
const post = async (origin, body, headers = {}) => {
  const res = await fetch(`${origin}/api`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body,
  });
  return { res, bytes: Buffer.from(await res.arrayBuffer()) };
};

// fetch inflates any `Content-Encoding` it recognizes, so the raw bytes are
// read through node:http wherever an assertion needs them.
const rawGet = (origin, path, headers) =>
  new Promise((resolve, reject) => {
    const http = require('node:http');
    http
      .get(`${origin}/api${path}`, { headers }, (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () => resolve({ res, bytes: Buffer.concat(chunks) }));
      })
      .on('error', reject);
  });

const rawPost = (origin, body, headers) =>
  new Promise((resolve, reject) => {
    const http = require('node:http');
    const req = http.request(
      `${origin}/api`,
      { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers } },
      (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () => resolve({ res, bytes: Buffer.concat(chunks) }));
      },
    );
    req.on('error', reject);
    req.end(body);
  });

test('http compression: off by default — a gzip-accepting request gets plain bytes', async (t) => {
  const { origin } = await bootServer(t, { router });
  const { res, bytes } = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': 'gzip' });
  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.headers['content-encoding'], undefined);
  assert.strictEqual(res.headers.vary, undefined);
  assert.strictEqual(JSON.parse(bytes.toString()).result.rows.length, 64);
});

test('http compression: enabled, a packet answer past the threshold is gzip with Vary', async (t) => {
  const { origin } = await bootServer(t, { router, http: { compression: true } });
  const { res, bytes } = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': 'gzip, deflate, br' });
  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.headers['content-encoding'], 'gzip');
  assert.strictEqual(res.headers.vary, 'Accept-Encoding');
  assert.strictEqual(Number(res.headers['content-length']), bytes.length, 'Content-Length is the encoded size');
  const plain = zlib.gunzipSync(bytes);
  assert.ok(bytes.length < plain.length / 3, `compressed ${bytes.length} B against ${plain.length} B plain`);
  assert.deepStrictEqual(JSON.parse(plain.toString()).result, big);
});

test('http compression: node fetch inflates by itself, so a user sees plain JSON', async (t) => {
  const { origin } = await bootServer(t, { router, http: { compression: true } });
  const { res, bytes } = await post(origin, packet('data/big'));
  assert.strictEqual(res.headers.get('content-encoding'), 'gzip');
  assert.deepStrictEqual(JSON.parse(bytes.toString()).result, big);
});

test('http compression: under the threshold, without Accept-Encoding, or refused by the filter — plain', async (t) => {
  const seen = [];
  const { origin } = await bootServer(t, {
    router,
    http: {
      compression: {
        filter: (call) => {
          seen.push(call.method);
          return call.headers['x-compress'] !== 'no';
        },
      },
    },
  });
  await t.test('a small answer', async () => {
    const { res } = await rawPost(origin, packet('data/small'), { 'Accept-Encoding': 'gzip' });
    assert.strictEqual(res.headers['content-encoding'], undefined);
  });
  await t.test('a peer that does not accept gzip', async () => {
    const { res, bytes } = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': 'identity' });
    assert.strictEqual(res.headers['content-encoding'], undefined);
    assert.strictEqual(JSON.parse(bytes.toString()).result.rows.length, 64);
  });
  await t.test('a peer that weights gzip at zero', async () => {
    const { res } = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': 'gzip;q=0, *;q=0.5' });
    assert.strictEqual(res.headers['content-encoding'], undefined);
  });
  await t.test('the filter says no', async () => {
    const before = seen.length;
    const { res } = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': 'gzip', 'x-compress': 'no' });
    assert.strictEqual(res.headers['content-encoding'], undefined);
    assert.strictEqual(seen.length, before + 1, 'the filter ran once, on the abstract call');
    assert.strictEqual(seen[seen.length - 1], 'POST');
  });
  await t.test('the filter says yes', async () => {
    const { res } = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': 'gzip' });
    assert.strictEqual(res.headers['content-encoding'], 'gzip');
  });
});

test('http compression: a batch frame is encoded once, as one body', async (t) => {
  const { origin } = await bootServer(t, { router, http: { compression: true } });
  const batch = JSON.stringify([
    { type: 'call', id: 'a', method: 'data/big', args: [] },
    { type: 'call', id: 'b', method: 'data/small', args: [] },
  ]);
  const { res, bytes } = await rawPost(origin, batch, { 'Accept-Encoding': 'gzip' });
  assert.strictEqual(res.headers['content-encoding'], 'gzip');
  const answers = JSON.parse(zlib.gunzipSync(bytes).toString());
  assert.deepStrictEqual(
    answers.map((a) => a.id),
    ['a', 'b'],
  );
});

test('http compression: REST answers — conventional, declared with a cache policy, and HEAD', async (t) => {
  const { origin } = await bootServer(t, { router, http: { compression: true } });
  await t.test('conventional GET /unit/method', async () => {
    const { res, bytes } = await rawGet(origin, '/data/big', { 'Accept-Encoding': 'gzip' });
    assert.strictEqual(res.headers['content-encoding'], 'gzip');
    assert.strictEqual(res.headers['content-type'], 'application/json');
    assert.deepStrictEqual(JSON.parse(zlib.gunzipSync(bytes).toString()).result, big);
  });
  await t.test('a declared route under http.cache keeps its ETag over the PLAIN body', async () => {
    const first = await rawGet(origin, '/cached', { 'Accept-Encoding': 'gzip' });
    assert.strictEqual(first.res.headers['content-encoding'], 'gzip');
    assert.match(first.res.headers['cache-control'], /max-age=60/);
    const etag = first.res.headers.etag;
    assert.match(etag, /^W\//);
    // The same representation asked for plain: same validator, the tag is
    // over the entity, not its encoding — weak by construction.
    const plain = await rawGet(origin, '/cached', { 'Accept-Encoding': 'identity' });
    assert.strictEqual(plain.res.headers['content-encoding'], undefined);
    assert.strictEqual(plain.res.headers.etag, etag);
    const revalidated = await rawGet(origin, '/cached', { 'Accept-Encoding': 'gzip', 'If-None-Match': etag });
    assert.strictEqual(revalidated.res.statusCode, 304);
    assert.strictEqual(revalidated.res.headers['content-encoding'], undefined, 'an empty 304 is never encoded');
    assert.strictEqual(revalidated.bytes.length, 0);
  });
  await t.test('a route that set Content-Encoding itself is left alone', async () => {
    const { res, bytes } = await rawGet(origin, '/encoded', { 'Accept-Encoding': 'gzip' });
    assert.strictEqual(res.headers['content-encoding'], 'identity');
    assert.deepStrictEqual(JSON.parse(bytes.toString()), big);
  });
  await t.test('HEAD carries the encoded headers and no body', async () => {
    const http = require('node:http');
    const res = await new Promise((resolve, reject) => {
      http
        .request(`${origin}/api/cached`, { method: 'HEAD', headers: { 'Accept-Encoding': 'gzip' } }, resolve)
        .on('error', reject)
        .end();
    });
    res.resume();
    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.headers['content-encoding'], 'gzip');
  });
});

test('http compression: Vary joins the CORS Vary: Origin', async (t) => {
  const { origin } = await bootServer(t, {
    router,
    cors: { origins: ['https://app.example'] },
    http: { compression: true },
  });
  const { res } = await rawPost(origin, packet('data/big'), {
    'Accept-Encoding': 'gzip',
    Origin: 'https://app.example',
  });
  assert.strictEqual(res.headers['content-encoding'], 'gzip');
  assert.strictEqual(res.headers.vary, 'Origin, Accept-Encoding');
});

test('http compression: async past its threshold answers from the threadpool, same bytes', async (t) => {
  const { origin } = await bootServer(t, {
    router,
    http: { compression: { async: { threshold: 1 } } },
  });
  const { res, bytes } = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': 'gzip' });
  assert.strictEqual(res.headers['content-encoding'], 'gzip');
  assert.deepStrictEqual(JSON.parse(zlib.gunzipSync(bytes).toString()).result, big);
  // The sync and async paths produce a gzip member zlib reads back the same.
  const sync = await rawPost(origin, packet('data/small'), { 'Accept-Encoding': 'gzip' });
  assert.strictEqual(sync.res.headers['content-encoding'], undefined, 'still under the size threshold');
});

test('http compression: a large REST body above the async threshold', async (t) => {
  const { origin } = await bootServer(t, {
    router,
    http: { compression: { async: { threshold: 4096 }, encodings: [{ encoding: 'gzip', level: 1 }] } },
  });
  const payload = { text: 'x'.repeat(200_000) };
  const { res, bytes } = await rawPost(origin, packet('data/echo', payload), { 'Accept-Encoding': 'gzip' });
  assert.strictEqual(res.headers['content-encoding'], 'gzip');
  assert.ok(bytes.length < 2048, `200 KB of one byte gzips to ${bytes.length} B`);
  assert.deepStrictEqual(JSON.parse(zlib.gunzipSync(bytes).toString()).result, payload);
});

test('http compression: the option is validated at construction', () => {
  assert.throws(() => new RpcServer({ router, http: null }), /options\.http must be an object/);
  assert.throws(() => new RpcServer({ router, http: { compression: 'yes' } }), /must be true, false or an options/);
  assert.throws(() => new RpcServer({ router, http: { compression: { threshold: -1 } } }), /threshold/);
  assert.throws(() => new RpcServer({ router, http: { compression: { filter: 'no' } } }), /filter must be a function/);
  assert.throws(() => new RpcServer({ router, http: { compression: { async: 'later' } } }), /async must be an object/);
  // Off in every spelling of off.
  for (const off of [undefined, null, false]) assert.strictEqual(normalizeCompression(off, 'x'), null);
  const on = normalizeCompression(true, 'x');
  assert.deepStrictEqual(
    { threshold: on.threshold, filter: on.filter, async: on.async, tokens: on.encoders.map((e) => e.token) },
    { threshold: 1024, filter: null, async: null, tokens: ['gzip'] },
    'gzip alone is what `true` means',
  );
  assert.strictEqual(Object.isFrozen(on) && Object.isFrozen(on.encoders), true);
  assert.strictEqual(normalizeCompression({ async: true }, 'x').async.threshold, 256 * 1024);
  assert.strictEqual(normalizeCompression({ async: { threshold: 10 } }, 'x').async.threshold, 10);
});

test('http compression: `encodings` is validated — a closed set of names, each coding its own knobs', () => {
  const of = (encodings, extra) => normalizeCompression({ encodings }, 'x', extra);
  assert.deepStrictEqual(
    of(['br', 'GZIP']).encoders.map((e) => e.token),
    ['br', 'gzip'],
  );
  assert.throws(() => of([]), /a list of one to eight encodings/);
  assert.throws(() => of('gzip'), /a list of one to eight encodings/);
  assert.throws(() => of(Array(9).fill('gzip')), /one to eight/);
  assert.throws(() => of(['deflate']), /unknown encoding "deflate"/);
  assert.throws(() => of(['gzip', 'gzip']), /names gzip twice/);
  assert.throws(() => of([7]), /an encoding is 'gzip', 'br', 'zstd' or an object/);
  assert.throws(() => of([{ encoding: 'g zip', encode: (b) => b }]), /must be an HTTP token/);
  assert.throws(() => of([{ encoding: 'identity', encode: (b) => b }]), /identity is not an encoding to apply/);
  assert.throws(() => of([{ encoding: 'gzip', level: 12 }]), /gzip level must be an integer from -1 to 9/);
  assert.throws(() => of([{ encoding: 'gzip', memLevel: 0 }]), /gzip memLevel/);
  assert.throws(() => of([{ encoding: 'br', quality: 12 }]), /br quality must be an integer from 0 to 11/);
  assert.throws(() => of([{ encoding: 'zstd', level: 0 }]), /zstd level must be an integer from 1 to 22/);
  assert.throws(() => of([{ encoding: 'x-mine', encode: 'no' }]), /must provide encode\(bytes\)/);
  assert.throws(
    () => of([{ encoding: 'x-mine', encode: (b) => b, createStream: 1 }]),
    /createStream must be a function/,
  );
  // The zlib knobs moved into the coding they belong to.
  assert.throws(() => normalizeCompression({ level: 6 }, 'x'), /belong to the coding/);
  assert.throws(() => normalizeCompression({ memLevel: 8 }, 'x'), /belong to the coding/);
  // zstd is detected, not assumed: a Node without it says so at construction.
  const old = { ...zlib };
  delete old.zstdCompressSync;
  assert.throws(() => of(['zstd', 'gzip'], { zlib: old }), /no zstd in node:zlib/);
  assert.strictEqual(of(['br', 'gzip'], { zlib: old }).encoders.length, 2);
  // An SSE response needs every coding to stream.
  const oneShot = { encoding: 'x-mine', encode: (b) => b };
  assert.strictEqual(of([oneShot]).encoders[0].token, 'x-mine');
  assert.throws(() => of([oneShot], { streaming: true }), /x-mine encoding has no createStream\(\)/);
});

const GZIP = normalizeCompression(true, 'x').encoders;
const acceptsGzip = (header) => pickEncoding(header, GZIP) !== null;

test('pickEncoding: the Accept-Encoding grammar', () => {
  assert.strictEqual(acceptsGzip('gzip'), true);
  assert.strictEqual(acceptsGzip('gzip, deflate, br'), true);
  assert.strictEqual(acceptsGzip('br;q=1.0, gzip;q=0.8, *;q=0.1'), true);
  assert.strictEqual(acceptsGzip('GZIP'), true);
  assert.strictEqual(acceptsGzip('x-gzip'), true);
  assert.strictEqual(acceptsGzip('*'), true);
  assert.strictEqual(acceptsGzip('br, *;q=0.5'), true);
  assert.strictEqual(acceptsGzip('identity'), false);
  assert.strictEqual(acceptsGzip('gzip;q=0'), false);
  assert.strictEqual(acceptsGzip('gzip;q=0.000'), false);
  assert.strictEqual(acceptsGzip('gzip;q=0, *'), false, 'an explicit zero beats the wildcard');
  assert.strictEqual(acceptsGzip('*;q=0'), false);
  assert.strictEqual(acceptsGzip(''), false);
  assert.strictEqual(acceptsGzip(undefined), false);
  assert.strictEqual(acceptsGzip('gzipped'), false);
  assert.strictEqual(acceptsGzip(' gzip ; q=0.5 '), true);
});

test('pickEncoding: the first coding of the SERVER’s list the request accepts', () => {
  const { encoders } = normalizeCompression(
    { encodings: ['br', { encoding: 'x-mine', encode: (b) => b }, 'gzip'] },
    'x',
  );
  const pick = (header) => pickEncoding(header, encoders)?.token ?? null;
  assert.strictEqual(pick('gzip, deflate, br'), 'br', 'the server’s order, not the header’s');
  assert.strictEqual(pick('gzip;q=1.0, br;q=0.1'), 'br', 'a weight says acceptable, not preferred');
  assert.strictEqual(pick('gzip'), 'gzip');
  assert.strictEqual(pick('gzip, x-mine'), 'x-mine');
  assert.strictEqual(pick('br;q=0, gzip'), 'gzip', 'a refused coding is passed over');
  assert.strictEqual(pick('br;q=0, *'), 'x-mine', 'the wildcard covers what was not named');
  assert.strictEqual(pick('br;q=0, x-mine;q=0, gzip;q=0, *'), null, 'and nothing that was refused');
  assert.strictEqual(pick('zstd, deflate'), null);
  assert.strictEqual(pick(`${'x,'.repeat(10_000)}br`), 'br', 'a long header is one scan');
});

const decoders = { gzip: zlib.gunzipSync, br: zlib.brotliDecompressSync, zstd: zlib.zstdDecompressSync };

test('http compression: `encodings` — Brotli, zstd and gzip by the server’s preference, Vary all the same', async (t) => {
  const encodings = hasZstd(zlib) ? ['zstd', 'br', 'gzip'] : ['br', 'gzip'];
  const { origin } = await bootServer(t, { router, http: { compression: { encodings } } });
  const cases = [
    ['gzip, deflate, br, zstd', encodings[0]],
    ['gzip, deflate, br', 'br'],
    ['gzip, deflate', 'gzip'],
    ['br;q=0, zstd;q=0, *', 'gzip'],
  ];
  for (const [accept, expected] of cases) {
    const { res, bytes } = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': accept });
    assert.strictEqual(res.headers['content-encoding'], expected, accept);
    assert.strictEqual(res.headers.vary, 'Accept-Encoding');
    assert.deepStrictEqual(JSON.parse(decoders[expected](bytes).toString()).result, big);
  }
  const none = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': 'deflate' });
  assert.strictEqual(none.res.headers['content-encoding'], undefined, 'nothing acceptable: plain');
  // fetch inflates Brotli by itself too: a user sees plain JSON.
  const { res, bytes } = await post(origin, packet('data/big'), { 'Accept-Encoding': 'br' });
  assert.strictEqual(res.headers.get('content-encoding'), 'br');
  assert.deepStrictEqual(JSON.parse(bytes.toString()).result, big);
});

test('http compression: Brotli past the async threshold answers from the threadpool', async (t) => {
  const { origin } = await bootServer(t, {
    router,
    http: { compression: { encodings: [{ encoding: 'br', quality: 5 }], async: { threshold: 4096 } } },
  });
  const payload = { text: 'x'.repeat(200_000) };
  const { res, bytes } = await rawPost(origin, packet('data/echo', payload), { 'Accept-Encoding': 'br' });
  assert.strictEqual(res.headers['content-encoding'], 'br');
  assert.deepStrictEqual(JSON.parse(zlib.brotliDecompressSync(bytes).toString()).result, payload);
});

test('http compression: an application’s own coding — synchronous, a promise, and one that fails', async (t) => {
  const reversed = (body) => Buffer.from(body).reverse();
  let mode = 'sync';
  const mine = {
    encoding: 'x-rev',
    encode: (body) => {
      if (mode === 'throw') throw new Error('no');
      if (mode === 'reject') return Promise.reject(new Error('no'));
      return mode === 'async' ? Promise.resolve(reversed(body)) : reversed(body);
    },
  };
  const { origin } = await bootServer(t, { router, http: { compression: { encodings: [mine, 'gzip'] } } });
  for (mode of ['sync', 'async']) {
    const { res, bytes } = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': 'x-rev, gzip' });
    assert.strictEqual(res.headers['content-encoding'], 'x-rev', mode);
    assert.strictEqual(Number(res.headers['content-length']), bytes.length);
    assert.deepStrictEqual(JSON.parse(reversed(bytes).toString()).result, big);
  }
  // A coding that fails answers the plain body, honestly labelled — never a broken one.
  for (mode of ['throw', 'reject']) {
    const { res, bytes } = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': 'x-rev' });
    assert.strictEqual(res.headers['content-encoding'], undefined, mode);
    assert.deepStrictEqual(JSON.parse(bytes.toString()).result, big);
  }
  mode = 'sync';
  const gz = await rawPost(origin, packet('data/big'), { 'Accept-Encoding': 'gzip' });
  assert.strictEqual(gz.res.headers['content-encoding'], 'gzip', 'a peer without it gets the next on the list');
});
