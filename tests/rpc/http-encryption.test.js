'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { defineRouter, procedure, WrpcClient } = require('../../index.js');
const { createEncryption, generateKey, fetchServerKey, createReplayCache } = require('../../encryption.js');
const { pack, unpack, isSealedType, sealedFetch, CONTENT_TYPE } = require('../../src/encryption/http.js');
const { createHpke, dhKem } = require('../../src/encryption/hpke.js');
const { aead } = require('../../src/encryption/aead.js');
const { x25519 } = require('../../src/encryption/dh.js');
const { createKdf } = require('../../src/encryption/hkdf.js');
const { parseBundle } = require('../../src/encryption/statics.js');
const { OpenError } = require('../../src/encryption/contracts.js');
const { buildBoots } = require('../adapters/boots.js');
const { bootServer } = require('../helpers/server.js');

const SECRET = 'a card number nobody on the path should read: 4111 1111 1111 1111';

const router = defineRouter({
  data: {
    echo: procedure({ access: 'public', handler: async (ctx, args) => ({ args, kind: ctx.client.transportKind }) }),
    boom: procedure({
      access: 'public',
      handler: async () => {
        const error = new Error('No such card');
        error.code = 404;
        throw error;
      },
    }),
    login: procedure({
      access: 'public',
      handler: async (ctx, { name }) => {
        ctx.client.startSession(undefined, { name });
        return true;
      },
    }),
    me: procedure({ handler: async (ctx) => ctx.client.session.state.name }),
    project: procedure({
      access: 'public',
      http: { method: 'GET', path: '/projects/:id' },
      handler: async (_ctx, { params }) => ({ id: params.id, note: SECRET }),
    }),
    // What the handler is told about the sender, header by header.
    facts: procedure({
      access: 'public',
      handler: async (ctx) => {
        const h = ctx.meta.headers;
        return {
          xff: h['x-forwarded-for'] ?? null,
          realIp: h['x-real-ip'] ?? null,
          cfIp: h['cf-connecting-ip'] ?? null,
          site: h['sec-fetch-site'] ?? null,
          host: h.host ?? null,
          origin: h.origin ?? null,
          via: h.via ?? null,
          app: h['x-app'] ?? null,
          cookie: h.cookie ?? null,
        };
      },
    }),
  },
});

// Every outer request and response body, as a proxy in the middle would see them.
const spyingFetch = () => {
  const seen = [];
  const fetchSpy = async (url, init) => {
    const response = await fetch(url, init);
    const body = Buffer.from(await response.clone().arrayBuffer());
    seen.push({ url: String(url), init, status: response.status, type: response.headers.get('content-type'), body });
    return response;
  };
  return { seen, fetch: fetchSpy };
};

const secure = async (t, encryption = {}) => {
  const booted = await bootServer(t, { router, encryption: { keys: generateKey(), ...encryption } });
  const endpoint = `${booted.origin}${booted.server.rpc.basePath}`;
  const serverKey = await booted.server.rpc.encryptionKey();
  const connect = async (options = {}) => {
    const client = await WrpcClient.connect(endpoint, {
      transport: 'http',
      encryption: createEncryption({ serverKey }),
      logger: false,
      ...options,
    });
    t.after(() => void client.close());
    return client;
  };
  return { ...booted, endpoint, serverKey, connect };
};

test('http encryption: calls, errors and REST routes — one opaque POST each, nothing readable either way', async (t) => {
  const { endpoint, connect } = await secure(t, { required: true });
  const spy = spyingFetch();
  const client = await connect({ fetch: spy.fetch });
  await client.load('data');
  assert.deepStrictEqual(await client.api.data.echo({ note: SECRET }), { args: { note: SECRET }, kind: 'http' });
  await assert.rejects(client.api.data.boom({}), { message: 'No such card', code: 404 });
  assert.deepStrictEqual(await client.api.data.project({ params: { id: '42' } }), { id: '42', note: SECRET });
  assert.ok(spy.seen.length >= 4);
  for (const { url, init, status, type, body } of spy.seen) {
    assert.strictEqual(url, `${endpoint}`.replace(/\/?$/, '') || endpoint, 'always the one endpoint');
    assert.strictEqual(init.method, 'POST', 'a GET route included');
    assert.deepStrictEqual(init.headers, { 'Content-Type': CONTENT_TYPE });
    assert.strictEqual(status, 200, 'the real status is inside');
    assert.ok(isSealedType(type));
    for (const needle of ['4111', 'echo', 'projects', 'No such card', 'callback']) {
      assert.ok(!Buffer.from(init.body).includes(needle), `request: ${needle}`);
      assert.ok(!body.includes(needle), `response: ${needle}`);
    }
  }
});

test('http encryption: required refuses plaintext; without it both are served', async (t) => {
  const strict = await secure(t, { required: true });
  const packet = JSON.stringify({ type: 'call', id: '1', method: 'data/echo', args: { n: 1 } });
  const post = (endpoint) =>
    fetch(endpoint, { method: 'POST', headers: { 'content-type': 'application/json' }, body: packet });
  assert.strictEqual((await post(strict.endpoint)).status, 426);
  assert.strictEqual((await fetch(`${strict.endpoint}/projects/1`)).status, 426);
  const lenient = await secure(t);
  assert.strictEqual((await post(lenient.endpoint)).status, 200);
  const client = await lenient.connect();
  await client.load('data');
  assert.strictEqual((await client.api.data.echo({})).kind, 'http');
});

test("http encryption: a session cookie still works — Set-Cookie is the outer response's", async (t) => {
  const { endpoint, connect } = await secure(t, { required: true });
  const jar = [];
  const cookies = async (url, init) => {
    const headers = jar.length > 0 ? { ...init.headers, cookie: jar.join('; ') } : init.headers;
    const response = await fetch(url, { ...init, headers });
    for (const line of response.headers.getSetCookie()) jar.push(line.split(';')[0]);
    return response;
  };
  const client = await connect({ fetch: cookies });
  await client.load('data');
  await client.api.data.login({ name: 'ada' });
  assert.strictEqual(jar.length, 1, 'set by the outer response, where a browser can store an HttpOnly cookie');
  assert.strictEqual(await client.api.data.me(), 'ada');
  assert.ok(endpoint);
});

test('http encryption: replayed, stale, altered or sealed to an unknown key — refused bare, never answered sealed', async (t) => {
  const warnings = [];
  const logger = {
    log() {},
    info() {},
    debug() {},
    error() {},
    warn: (entry) => warnings.push(entry),
    child: () => logger,
  };
  const { connect, endpoint } = await secure(t, { required: true, maxSkew: 60_000 });
  const stale = await bootServer(t, { router, logger, encryption: { keys: generateKey(), maxSkew: 60_000 } });
  const staleEndpoint = `${stale.origin}${stale.server.rpc.basePath}`;
  const staleKey = await stale.server.rpc.encryptionKey();

  // Replay: the very bytes, sent again by whoever sits in the middle
  const spy = spyingFetch();
  const client = await connect({ fetch: spy.fetch });
  await client.load('data');
  await client.api.data.echo({ n: 1 });
  const { init } = spy.seen.at(-1);
  const replayed = await fetch(endpoint, init);
  assert.strictEqual(replayed.status, 409);
  assert.ok(!isSealedType(replayed.headers.get('content-type')));

  // Stale: a sender whose clock is five minutes off, against a one-minute window
  const kdf = createKdf();
  const cipher = aead();
  const late = sealedFetch({
    hpke: createHpke({ kem: dhKem(x25519(), kdf), kdf, cipher }),
    kdf,
    cipher,
    serverKey: parseBundle(staleKey),
    now: () => Date.now() - 5 * 60_000,
  })(fetch, staleEndpoint);
  await assert.rejects(late(staleEndpoint, { method: 'POST', body: '{}' }), /answered in plaintext \(409\)/);

  // Altered in flight, truncated, sealed to a kid nobody holds, not the format at all
  const bodies = [
    (body) => Buffer.from(body).fill(0xff, body.length - 4),
    (body) => Buffer.from(body).subarray(0, 20),
    (body) => Buffer.concat([Buffer.from([1, 2, 4]), Buffer.from('nope'), Buffer.from(body).subarray(4)]),
    () => Buffer.from([9, 9, 9, 9, 9]),
    (body) => Buffer.concat([Buffer.from([1, 7]), Buffer.from(body).subarray(2)]),
  ];
  const statuses = [];
  for (const alter of bodies) {
    const response = await fetch(staleEndpoint, { ...init, body: alter(init.body) });
    statuses.push(response.status);
    assert.ok(!isSealedType(response.headers.get('content-type')));
  }
  assert.deepStrictEqual(statuses, [400, 400, 400, 400, 400]);
  assert.deepStrictEqual(
    warnings.filter((w) => w.event === 'encryption.refused').map((w) => w.reason),
    ['stale', 'open', 'format', 'kid', 'format', 'format'],
  );
});

test("http encryption: what the connection says about the sender is not the sender's to declare inside", async (t) => {
  const { connect, port } = await secure(t);
  // A page behind a proxy that reads x-forwarded-for, or a rate limit keyed
  // on cf-connecting-ip: the proxy sets those on the OUTER request, which it
  // can see; the inner one it cannot, and used to win.
  const lies = {
    'x-forwarded-for': '10.0.0.1',
    'x-real-ip': '10.0.0.2',
    'cf-connecting-ip': '10.0.0.3',
    'sec-fetch-site': 'same-origin',
    host: 'admin.internal',
    origin: 'https://admin.internal',
    via: '1.1 nobody',
    'x-app': 'v1',
    cookie: 'sid=declared',
  };
  const client = await connect({ headers: lies });
  await client.load('data');
  const { host, origin, ...facts } = await client.api.data.facts();
  assert.deepStrictEqual(
    facts,
    { xff: null, realIp: null, cfIp: null, site: null, via: null, app: 'v1', cookie: 'sid=declared' },
    "the ambient names are dropped whether or not the outer request carried them; the rest is the sender's",
  );
  assert.strictEqual(host, `127.0.0.1:${port}`, "the host is the outer request's");
  assert.notStrictEqual(origin, 'https://admin.internal');
  // A cookie the OUTER request carries — the HttpOnly one script cannot
  // set, sent by the browser — is not overridden from inside.
  const jar = (url, init) => fetch(url, { ...init, headers: { ...init.headers, cookie: 'sid=outer' } });
  const cookied = await connect({ headers: { cookie: 'sid=declared' }, fetch: jar });
  await cookied.load('data');
  assert.strictEqual((await cookied.api.data.facts()).cookie, 'sid=outer');
});

test('http encryption: the inner header is bounded before it is parsed', async (t) => {
  const warnings = [];
  const logger = {
    log() {},
    info() {},
    debug() {},
    error() {},
    warn: (entry) => warnings.push(entry),
    child: () => logger,
  };
  const booted = await bootServer(t, { router, logger, encryption: { keys: generateKey() } });
  const endpoint = `${booted.origin}${booted.server.rpc.basePath}`;
  const serverKey = await booted.server.rpc.encryptionKey();
  const client = await WrpcClient.connect(endpoint, {
    transport: 'http',
    encryption: createEncryption({ serverKey }),
    logger: false,
    headers: { 'x-app': 'x'.repeat(17 * 1024) },
  });
  t.after(() => void client.close());
  await assert.rejects(client.load('data'));
  assert.deepStrictEqual(
    warnings.filter((w) => w.event === 'encryption.refused').map((w) => w.reason),
    ['format'],
  );
});

test('http encryption: a client that encrypts does not read a plaintext answer, whatever its status', async (t) => {
  const bare = await bootServer(t, { router });
  const endpoint = `${bare.origin}${bare.server.rpc.basePath}`;
  const encryption = createEncryption({ serverKey: `0:${'A'.repeat(43)}:${'B'.repeat(43)}` });
  await assert.rejects(
    WrpcClient.connect(endpoint, { transport: 'http', encryption, logger: false }).then((client) =>
      client.load('data'),
    ),
    /./,
  );
  const sealed = encryption.fetch(fetch, endpoint);
  await assert.rejects(sealed(endpoint, { method: 'POST', body: '{}' }), /answered in plaintext/);
  // No serverKey, nothing to seal a request to
  const anonymous = createEncryption({ pattern: 'NN' });
  assert.strictEqual(anonymous.fetch, null);
  await assert.rejects(
    WrpcClient.connect(endpoint, { transport: 'http', encryption: anonymous, logger: false }),
    /has no serverKey to seal a request to/,
  );
});

test('http encryption: discovery publishes the bundle — and can be turned off', async (t) => {
  const { endpoint, serverKey, url } = await secure(t, { required: true });
  assert.strictEqual(await fetchServerKey(endpoint), serverKey);
  assert.strictEqual(await fetchServerKey(`${url}/`), serverKey, 'a ws url is its http one');
  const response = await fetch(`${endpoint}/encryption-key`);
  assert.strictEqual(response.headers.get('cache-control'), 'no-store');
  const hidden = await secure(t, { discovery: false, required: true });
  await assert.rejects(fetchServerKey(hidden.endpoint), /no key at .*\(426\)/);
  const bare = await bootServer(t, { router });
  await assert.rejects(fetchServerKey(`${bare.origin}${bare.server.rpc.basePath}`), /no key at/);
  await assert.rejects(
    fetchServerKey(endpoint, { fetch: async () => new Response('{"key":"not a bundle"}') }),
    /the discovered key must be a key bundle/,
  );
});

test('http encryption: the options are validated where the server is built', async (t) => {
  const keys = generateKey();
  const build = (encryption) => bootServer(t, { router, encryption: { keys, ...encryption } });
  await assert.rejects(build({ maxSkew: 0 }), /maxSkew must be a positive integer/);
  await assert.rejects(build({ replay: {} }), /replay must be \{ seen\(id, ttl\) \}/);
  await assert.rejects(build({ discovery: 'no' }), /discovery must be a boolean/);
  // An injected replay memory is asked, once per request, with a ttl of twice the window
  const asked = [];
  const { connect } = await secure(t, {
    maxSkew: 1000,
    replay: { seen: async (id, ttl) => asked.push([id.length, ttl]) > 1 },
  });
  const client = await connect();
  await client.load('data');
  assert.deepStrictEqual(asked[0], [43, 2000]);
  await assert.rejects(client.api.data.echo({}), /./, 'the second request was called a replay');
});

test('replay cache: once within the ttl, again after it, bounded', () => {
  let time = 1000;
  const cache = createReplayCache({ max: 3, now: () => time });
  assert.strictEqual(cache.seen('a', 100), false);
  assert.strictEqual(cache.seen('a', 100), true);
  time += 101;
  assert.strictEqual(cache.seen('a', 100), false, 'expired');
  assert.strictEqual(cache.seen('b', 100), false);
  assert.strictEqual(cache.seen('c', 100), false);
  assert.strictEqual(cache.size, 3);
  assert.strictEqual(cache.seen('d', 100), false);
  assert.ok(cache.size <= 3, 'the oldest made room');
  assert.strictEqual(createReplayCache().seen('x', 1), false);
});

test('sealed body format: a JSON header and bytes, and every malformed one is an OpenError', () => {
  const packed = pack({ m: 'POST', u: '/api' }, Uint8Array.of(1, 2, 3));
  const { header, body } = unpack(packed);
  assert.deepStrictEqual(header, { m: 'POST', u: '/api' });
  assert.deepStrictEqual([...body], [1, 2, 3]);
  assert.strictEqual(unpack(pack({}, new Uint8Array(0))).body.length, 0);
  const json = (text) => Buffer.concat([Buffer.from([0, 0, 0, text.length]), Buffer.from(text)]);
  for (const bad of [
    new Uint8Array(3),
    Uint8Array.of(0, 0, 0, 9, 1),
    json('[1]'),
    json('null'),
    json('{bad'),
    json('"x"'),
  ]) {
    assert.throws(() => unpack(bad), OpenError);
  }
  assert.strictEqual(isSealedType('Application/WRPC-Sealed; charset=binary'), true);
  assert.strictEqual(isSealedType('application/json'), false);
  assert.strictEqual(isSealedType(undefined), false);
});

// Every host the core runs under hands a sealed body over as bytes.
for (const entry of buildBoots()) {
  test(`http encryption: ${entry.name} — a sealed call through the host`, { skip: entry.skip }, async (t) => {
    const instance = await entry.boot({ router, encryption: { keys: generateKey(), required: true } });
    t.after(() => instance.close());
    const endpoint = `http://127.0.0.1:${instance.port}/api`;
    const serverKey = await fetchServerKey(endpoint);
    const client = await WrpcClient.connect(endpoint, {
      transport: 'http',
      encryption: createEncryption({ serverKey }),
      logger: false,
    });
    t.after(() => void client.close());
    await client.load('data');
    assert.deepStrictEqual((await client.api.data.echo({ note: SECRET })).args, { note: SECRET });
    // `required` on every surface the host serves: the fastify boots mount
    // the REST routes natively, outside handleHttpCall, and used to answer
    // them in the clear — with a client added for each.
    const plain = await fetch(`${endpoint}/projects/1`);
    assert.strictEqual(plain.status, 426, `${entry.name}: a plaintext REST route`);
    assert.ok(!(await plain.text()).includes(SECRET));
    const posted = await fetch(endpoint, { method: 'POST', body: '{"type":"ping"}' });
    assert.strictEqual(posted.status, 426, `${entry.name}: a plaintext packet`);
    assert.strictEqual(instance.rpc.clients.size, 0, `${entry.name}: no client was added for the refused request`);
    // The sealed client still works after the refusals.
    assert.deepStrictEqual((await client.api.data.echo({ n: 2 })).args, { n: 2 });
  });
}
