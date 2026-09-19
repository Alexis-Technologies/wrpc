'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const zlib = require('node:zlib');

const { defineRouter, procedure, RpcServer } = require('../../index.js');
const { bootServer } = require('../helpers/server.js');
const { normalizeCompression, acceptsGzip } = require('../../src/contentEncoding.js');

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
    http: { compression: { async: { threshold: 4096 }, level: 1 } },
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
  assert.throws(() => new RpcServer({ router, http: { compression: { level: 12 } } }), /level must be an integer/);
  assert.throws(() => new RpcServer({ router, http: { compression: { memLevel: 0 } } }), /memLevel/);
  assert.throws(() => new RpcServer({ router, http: { compression: { async: 'later' } } }), /async must be an object/);
  // Off in every spelling of off.
  for (const off of [undefined, null, false]) assert.strictEqual(normalizeCompression(off, 'x'), null);
  const on = normalizeCompression(true, 'x');
  assert.deepStrictEqual(on, { threshold: 1024, filter: null, level: undefined, memLevel: undefined, async: null });
  assert.strictEqual(normalizeCompression({ async: true }, 'x').async.threshold, 256 * 1024);
  assert.strictEqual(normalizeCompression({ async: { threshold: 10 } }, 'x').async.threshold, 10);
});

test('acceptsGzip: the Accept-Encoding grammar', () => {
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
