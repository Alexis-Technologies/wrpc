'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const node = require('../../src/client/wsHandshake.js');
const browser = require('../../src/client/wsHandshake.browser.js');
const { readHandshake } = require('../../index.js');

// A constructor that records what it was handed. `strict` behaves like a
// browser (and Deno): anything but a string or an array of tokens as the
// second argument is a SyntaxError, thrown synchronously.
const TOKEN = /^[!#$%&'*+.^_`|~A-Za-z0-9-]+$/;
const fakeSocket = ({ strict = false } = {}) => {
  const calls = [];
  class FakeWebSocket {
    constructor(...args) {
      if (strict && args.length > 1) {
        const list = Array.isArray(args[1]) ? args[1] : [String(args[1])];
        for (const name of list) {
          if (!TOKEN.test(name)) throw new SyntaxError(`The subprotocol '${name}' is invalid.`);
        }
      }
      calls.push(args);
    }
  }
  return { FakeWebSocket, calls };
};

const recorder = () => {
  const records = [];
  return { records, log: { warn: (record) => void records.push(record) } };
};

// What the server would make of a recorded browser-style call.
const received = ([url, protocols]) =>
  readHandshake({ url, headers: protocols ? { 'sec-websocket-protocol': protocols.join(', ') } : {} });

const URL = 'ws://host/api';

test('node: declared headers and meta leave as REAL headers — no query, no tokens, no Bearer lift', () => {
  const { FakeWebSocket, calls } = fakeSocket();
  const options = { headers: { authorization: 'Bearer tok', 'x-tenant': 'acme' }, meta: { 'user-id': 7 } };
  node.openSocket(FakeWebSocket, URL, ['wrpc.v1'], options, null);
  assert.deepStrictEqual(calls, [
    [
      URL,
      {
        protocols: ['wrpc.v1'],
        headers: {
          authorization: 'Bearer tok',
          'x-tenant': 'acme',
          'x-wrpc-meta': encodeURIComponent(JSON.stringify({ 'user-id': 7 })),
        },
      },
    ],
  ]);
});

test('node: metaFormat prefixed finally means something on ws, and an empty offer is no obstacle', () => {
  const { FakeWebSocket, calls } = fakeSocket();
  node.openSocket(FakeWebSocket, URL, [], { meta: { 'user-id': '7' }, metaPrefixed: true }, null);
  assert.deepStrictEqual(calls[0], [URL, { protocols: [], headers: { 'x-wrpc-meta-user-id': '7' } }]);
});

test('node: nothing declared is the plain two-argument constructor', () => {
  const { FakeWebSocket, calls } = fakeSocket();
  node.openSocket(FakeWebSocket, URL, ['wrpc.v1'], {}, null);
  node.openSocket(FakeWebSocket, URL, [], { headers: null, meta: null }, null);
  assert.deepStrictEqual(calls, [[URL, ['wrpc.v1']], [URL]]);
});

test('node: names the handshake owns are refused, an oversize meta block is dropped, both loudly', () => {
  const { FakeWebSocket, calls } = fakeSocket();
  const { records, log } = recorder();
  const headers = { host: 'evil', 'sec-websocket-key': 'x', upgrade: 'h2c', 'content-length': '9', 'x-ok': '1' };
  node.openSocket(FakeWebSocket, URL, ['wrpc.v1'], { headers, meta: { pad: 'x'.repeat(3000) } }, log);
  assert.deepStrictEqual(calls[0][1].headers, { 'x-ok': '1' });
  assert.deepStrictEqual(records.map((record) => record.event).sort(), [
    'declared.unsendable',
    'declared.unsendable',
    'declared.unsendable',
    'declared.unsendable',
    'meta.oversize',
  ]);
});

test('node: a constructor without an init bag falls back to the carrier tokens', () => {
  const { FakeWebSocket, calls } = fakeSocket({ strict: true });
  const { records, log } = recorder();
  node.openSocket(FakeWebSocket, URL, ['wrpc.v1'], { headers: { 'x-tenant': 'acme' }, meta: { n: 1 } }, log);
  assert.strictEqual(records[0].event, 'handshake.fallback');
  assert.ok(records[0].err instanceof SyntaxError);
  assert.strictEqual(calls.length, 1);
  assert.strictEqual(calls[0][0], URL, 'the url stays clean');
  const { headers, meta } = received(calls[0]);
  assert.strictEqual(headers['x-tenant'], 'acme');
  assert.deepStrictEqual({ ...meta }, { n: 1 });
  // A second throw is the caller's: open() rejects with it.
  assert.throws(() => node.openSocket(FakeWebSocket, URL, ['not a token'], { headers: { a: '1' } }, log), SyntaxError);
});

test("node: carrier 'protocol' and 'query' go straight to the browser half", () => {
  const { FakeWebSocket, calls } = fakeSocket();
  node.openSocket(FakeWebSocket, URL, ['wrpc.v1'], { carrier: 'protocol', headers: { a: '1' } }, null);
  node.openSocket(FakeWebSocket, URL, ['wrpc.v1'], { carrier: 'query', headers: { a: '1' } }, null);
  assert.strictEqual(calls[0][0], URL);
  assert.strictEqual(calls[0][1].length, 2);
  assert.ok(calls[1][0].includes('?wrpc_h='));
  assert.deepStrictEqual(calls[1][1], ['wrpc.v1']);
});

test('browser: the bags ride the offer as tokens a strict constructor accepts, and the server reads them back', () => {
  const { FakeWebSocket, calls } = fakeSocket({ strict: true });
  const options = {
    headers: { 'x-tenant': 'acme', 'x-note': 'a b/c=d?e' },
    meta: { city: 'Київ', mark: '✓', nested: { deep: [1, 2] }, pad: '>>>???' },
  };
  browser.openSocket(FakeWebSocket, `${URL}?room=1`, ['wrpc.v1'], options, null);
  const [url, offer] = calls[0];
  assert.strictEqual(url, `${URL}?room=1`, "the application's own query is untouched");
  assert.strictEqual(offer[0], 'wrpc.v1');
  assert.ok(offer[1].startsWith('wrpc.h.') && offer[2].startsWith('wrpc.m.'));
  for (const token of offer) assert.ok(!/[=+/]/.test(token), `base64url without padding: ${token}`);
  const { headers, meta } = received(calls[0]);
  assert.strictEqual(headers['x-tenant'], 'acme');
  assert.strictEqual(headers['x-note'], 'a b/c=d?e');
  assert.deepStrictEqual(JSON.parse(JSON.stringify(meta)), options.meta);
});

test('browser: a Bearer credential rides bare and outside the budget; anything else rides inside the bag', () => {
  const { FakeWebSocket, calls } = fakeSocket({ strict: true });
  const jwt = `${'h'.repeat(40)}.${'p'.repeat(2400)}.sig-_`;
  browser.openSocket(FakeWebSocket, URL, ['wrpc.v1'], { headers: { authorization: `Bearer ${jwt}`, a: '1' } }, null);
  assert.strictEqual(calls[0][1][1], `wrpc.bearer.${jwt}`);
  assert.strictEqual(received(calls[0]).headers.a, '1');
  assert.strictEqual(received(calls[0]).headers.authorization, undefined);
  // Alone in the bag: lifted, and no empty `wrpc.h.` token follows it.
  browser.openSocket(FakeWebSocket, URL, ['wrpc.v1'], { headers: { authorization: 'Bearer tok' } }, null);
  assert.deepStrictEqual(calls[1], [URL, ['wrpc.v1', 'wrpc.bearer.tok']]);
  // Not a token (or not Bearer): it stays a declared header, base64url-safe.
  for (const authorization of ['Bearer a/b+c==', 'Basic dXNlcjpwYXNz', 'Bearer ']) {
    browser.openSocket(FakeWebSocket, URL, ['wrpc.v1'], { headers: { authorization } }, null);
    const call = calls.at(-1);
    assert.strictEqual(call[1].length, 2);
    assert.strictEqual(received(call).headers.authorization, authorization);
  }
});

test('browser: ONE budget for both tokens, headers first — a refused bag is loud, never fatal', () => {
  const { FakeWebSocket, calls } = fakeSocket({ strict: true });
  const { records, log } = recorder();
  const big = { pad: 'x'.repeat(1200) };
  browser.openSocket(FakeWebSocket, URL, ['wrpc.v1'], { headers: big, meta: big }, log);
  assert.strictEqual(calls[0][1].length, 2, 'the second token no longer fit');
  assert.strictEqual(received(calls[0]).headers.pad, big.pad);
  assert.deepStrictEqual({ ...received(calls[0]).meta }, {});
  assert.strictEqual(records.length, 1);
  assert.strictEqual(records[0].event, 'meta.oversize');
  assert.strictEqual(records[0].carrier, 'protocol');
  // Far past the budget: refused on the text, never encoded (nor spread).
  browser.openSocket(FakeWebSocket, URL, ['wrpc.v1'], { meta: { pad: 'x'.repeat(1_000_000) } }, log);
  assert.deepStrictEqual(calls[1], [URL, ['wrpc.v1']]);
  // Whatever left the client fits the server's default budget as well.
  const total = calls[0][1].slice(1).reduce((sum, token) => sum + token.length, 0);
  assert.ok(total <= 2048);
});

test('browser: an empty offer means the query for everything — tokens need a protocol the server can answer', () => {
  const { FakeWebSocket, calls } = fakeSocket({ strict: true });
  const { records, log } = recorder();
  const options = { headers: { authorization: 'Bearer tok', a: '1' }, meta: { n: 1 } };
  browser.openSocket(FakeWebSocket, URL, [], options, log);
  assert.strictEqual(calls[0].length, 1, 'nothing offered');
  const { headers, meta } = received(calls[0]);
  assert.strictEqual(headers.authorization, 'Bearer tok');
  assert.strictEqual(headers.a, '1');
  assert.deepStrictEqual({ ...meta }, { n: 1 });
  assert.deepStrictEqual(records, [{ event: 'declared.exposed', key: 'authorization', carrier: 'query' }]);
});

test("browser: carrier 'query' keeps the url carrier and still lifts the Bearer token out of it", () => {
  const { FakeWebSocket, calls } = fakeSocket({ strict: true });
  const { records, log } = recorder();
  const options = { carrier: 'query', headers: { authorization: 'Bearer tok', a: '1' }, meta: { n: 1 } };
  browser.openSocket(FakeWebSocket, URL, ['wrpc.v1'], options, log);
  const [url, offer] = calls[0];
  assert.deepStrictEqual(offer, ['wrpc.v1', 'wrpc.bearer.tok']);
  assert.ok(url.includes('wrpc_h=') && url.includes('wrpc_meta=') && !url.includes('tok'));
  assert.deepStrictEqual(records, []);
  // Nothing declared: the url and the offer are the caller's, untouched.
  browser.openSocket(FakeWebSocket, URL, ['wrpc.v1'], {}, log);
  assert.deepStrictEqual(calls[1], [URL, ['wrpc.v1']]);
});

test('the `carrier` option is validated at construction', () => {
  const { WrpcClient } = require('../../index.js');
  const WsTransport = WrpcClient.transport.ws;
  const build = (options) => new WrpcClient(URL, new WsTransport(URL), options);
  for (const carrier of ['auto', 'protocol', 'query']) assert.ok(build({ carrier }));
  assert.throws(() => build({ carrier: 'headers' }), /options\.carrier must be 'auto', 'protocol' or 'query'/);
  // A token needs an offer the server can answer next to it.
  assert.throws(() => build({ carrier: 'protocol', protocols: [] }), /cannot ride an empty `protocols` offer/);
  assert.ok(build({ carrier: 'query', protocols: [] }));
});
