'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { readHandshake } = require('../../index.js');
const { carriedBags, sanitizeDeclared, RESERVED_DECLARED } = require('../../src/rpc/handshake.js');

// What a browser client offers: base64url JSON without padding, the only
// spelling a subprotocol token admits.
const b64u = (value) => Buffer.from(JSON.stringify(value), 'utf8').toString('base64url');
const offerH = (value) => `wrpc.h.${b64u(value)}`;
const offerM = (value) => `wrpc.m.${b64u(value)}`;
const query = (name, value) => `${name}=${encodeURIComponent(JSON.stringify(value))}`;

const warnings = () => {
  const records = [];
  return { records, log: { warn: (record) => void records.push(record) } };
};

test('readHandshake: the subprotocol carrier lands declared headers UNDER the observed ones', () => {
  const declared = {
    'X-App-Version': '1.2.3',
    xTenant: 'acme', // camelCase -> kebab
    'x-thing': 'declared', // the observed value must win
    cookie: 'token=forged',
    origin: 'https://evil.example',
    'x-wrpc-channel': 'forged',
    secFetchSite: 'same-origin', // reserved AFTER the kebab transform
    num: 5,
    nested: { a: 1 },
    __proto__: { polluted: 1 },
  };
  const { headers, meta } = readHandshake({
    url: '/api',
    headers: {
      cookie: 'token=real',
      'x-thing': 'observed',
      'sec-websocket-protocol': `wrpc.v1, ${offerH(declared)}, ${offerM({ userId: 7, tags: ['a'] })}`,
    },
  });
  assert.strictEqual(headers['x-app-version'], '1.2.3');
  assert.strictEqual(headers['x-tenant'], 'acme');
  assert.strictEqual(headers['x-thing'], 'observed');
  assert.strictEqual(headers.cookie, 'token=real');
  assert.strictEqual(headers.origin, undefined);
  assert.strictEqual(headers['x-wrpc-channel'], undefined);
  assert.strictEqual(headers['sec-fetch-site'], undefined);
  assert.strictEqual(headers.num, undefined);
  assert.strictEqual(headers.nested, undefined);
  assert.strictEqual({}.polluted, undefined, 'Object.prototype survived');
  // Type-faithful, kebab-keyed, frozen — the same bag every other carrier gives.
  assert.deepStrictEqual({ ...meta }, { 'user-id': 7, tags: ['a'] });
  assert.ok(Object.isFrozen(meta));
});

test('readHandshake: carrier tokens leave the sec-websocket-protocol an application sees', () => {
  const both = readHandshake({
    headers: { 'sec-websocket-protocol': `wrpc.v1,${offerH({ a: '1' })},  wrpc.bearer.tok9 , chat` },
  });
  assert.strictEqual(both.headers['sec-websocket-protocol'], 'wrpc.v1, chat');
  assert.strictEqual(both.headers.a, '1');
  // Nothing but carriers: the header goes away rather than staying empty.
  const only = readHandshake({ headers: { 'sec-websocket-protocol': 'wrpc.bearer.tok9', 'x-obs': 'kept' } });
  assert.deepStrictEqual(Object.keys(only.headers), ['x-obs']);
  // No wrpc token at all: the observed bag is handed back untouched.
  const observed = { 'sec-websocket-protocol': 'chat, superchat' };
  assert.strictEqual(readHandshake({ headers: observed }).headers, observed);
});

test('readHandshake: a bag survives UTF-8 through base64url', () => {
  const { meta } = readHandshake({ headers: { 'sec-websocket-protocol': offerM({ city: 'Київ', mark: '✓' }) } });
  assert.deepStrictEqual({ ...meta }, { city: 'Київ', mark: '✓' });
});

test('readHandshake: a carrier is chosen, never merged — an offered token silences the query', () => {
  const url = `/api?${query('wrpc_h', { 'x-from': 'query' })}&${query('wrpc_meta', { from: 'query' })}`;
  // The query alone still works: carrier 'query', and WebTransport.
  const old = readHandshake({ url, headers: {} });
  assert.strictEqual(old.headers['x-from'], 'query');
  assert.deepStrictEqual({ ...old.meta }, { from: 'query' });
  // Each bag picks its carrier independently.
  const mixed = readHandshake({ url, headers: { 'sec-websocket-protocol': offerH({ 'x-from': 'offer' }) } });
  assert.strictEqual(mixed.headers['x-from'], 'offer');
  assert.deepStrictEqual({ ...mixed.meta }, { from: 'query' });
  // A REFUSED token still silences the query: no downgrade by sending garbage.
  const refused = readHandshake({ url, headers: { 'sec-websocket-protocol': 'wrpc.h.%%%, wrpc.m.bm90LWpzb24' } });
  assert.strictEqual(refused.headers['x-from'], undefined);
  assert.deepStrictEqual({ ...refused.meta }, {});
  // A real x-wrpc-meta header (only a non-browser peer can send one) wins over the offer.
  const real = readHandshake({
    headers: {
      'x-wrpc-meta': encodeURIComponent(JSON.stringify({ from: 'header' })),
      'sec-websocket-protocol': offerM({ from: 'offer' }),
    },
  });
  assert.deepStrictEqual({ ...real.meta }, { from: 'header' });
});

test('readHandshake: malformed or oversize offers are refused, never thrown', () => {
  const cases = [
    'wrpc.h.',
    'wrpc.h.!!!',
    `wrpc.h.${Buffer.from('not json').toString('base64url')}`,
    offerH(['a', 'b']),
    offerH(7),
    offerH(null),
    `${offerH({ a: '1' }).slice(0, -3)}`,
  ];
  for (const offer of cases) {
    const { headers, meta } = readHandshake({ headers: { 'x-obs': 'kept', 'sec-websocket-protocol': offer } });
    assert.deepStrictEqual(Object.keys(headers), ['x-obs'], offer);
    assert.deepStrictEqual({ ...meta }, {}, offer);
  }
  // No logger handed in: an oversize label is still a quiet refusal.
  const silent = readHandshake(
    { headers: { 'sec-websocket-protocol': offerH({ pad: 'x'.repeat(99) }) } },
    { metaMaxBytes: 8 },
  );
  assert.deepStrictEqual(Object.keys(silent.headers), []);
  assert.deepStrictEqual(readHandshake(undefined), { headers: {}, meta: {} });
  assert.deepStrictEqual(readHandshake({ headers: { 'sec-websocket-protocol': ['a', 'b'] } }).meta, {});
});

test('readHandshake: ONE budget covers both tokens, headers first, measured before decoding', () => {
  const { records, log } = warnings();
  const h = offerH({ pad: 'h'.repeat(60) });
  const m = offerM({ pad: 'm'.repeat(60) });
  const metaMaxBytes = h.length + m.length - 1;
  const over = readHandshake({ headers: { 'sec-websocket-protocol': `wrpc.v1, ${h}, ${m}` } }, { metaMaxBytes, log });
  assert.strictEqual(over.headers.pad, 'h'.repeat(60));
  assert.deepStrictEqual({ ...over.meta }, {}, 'the second token no longer fit');
  assert.deepStrictEqual(records, [{ event: 'meta.oversize', carrier: 'protocol', bytes: m.length }]);
  const fits = readHandshake(
    { headers: { 'sec-websocket-protocol': `${h}, ${m}` } },
    { metaMaxBytes: metaMaxBytes + 1, log },
  );
  assert.strictEqual(fits.meta.pad, 'm'.repeat(60));
  // The query keeps its own whole-string measure.
  const long = `/api?${query('wrpc_h', { pad: 'x'.repeat(200) })}`;
  assert.deepStrictEqual(Object.keys(readHandshake({ url: long, headers: {} }, { metaMaxBytes: 64, log }).headers), []);
  assert.strictEqual(records.at(-1).carrier, 'query');
});

test('carriedBags: the first token of a kind wins, and a Bearer token is left to the auth strategy', () => {
  assert.strictEqual(carriedBags(undefined, 2048, null), null);
  assert.strictEqual(carriedBags('chat, superchat', 2048, null), null, 'no wrpc token: not even split');
  const carried = carriedBags(`${offerH({ a: '1' })}, ${offerH({ a: '2' })}, wrpc.bearer.tok, wrpc.v1`, 2048, null);
  assert.strictEqual(carried.headers, '{"a":"1"}');
  assert.strictEqual(carried.meta, undefined);
  assert.deepStrictEqual(carried.rest, ['wrpc.v1']);
});

test('the deny list covers what a hostile page could forge, on the name that is kept', () => {
  const forged = {
    'X-Forwarded-For': '10.0.0.1',
    xForwardedProto: 'https',
    'x-real-ip': '10.0.0.1',
    forwarded: 'for=10.0.0.1',
    via: '1.1 forged',
    'true-client-ip': '10.0.0.1',
    'cf-connecting-ip': '10.0.0.1',
    'x-client-ip': '10.0.0.1',
    host: 'internal',
    'proxy-authorization': 'Basic x',
    'content-length': '0',
    // Not reserved: a name that merely resembles one, and the one declared
    // name that is a credential — the bearer transport reads it.
    'x-forwarded': 'kept',
    'x-via': 'kept',
    authorization: 'Bearer kept',
  };
  assert.deepStrictEqual(
    { ...sanitizeDeclared(forged) },
    { 'x-forwarded': 'kept', 'x-via': 'kept', authorization: 'Bearer kept' },
  );
  assert.strictEqual(sanitizeDeclared({ cookie: 'a' }), null, 'nothing kept is null, not an empty bag');
  assert.ok(RESERVED_DECLARED.test('x-forwarded-for'));
});
