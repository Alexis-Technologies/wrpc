'use strict';

// The one redial of the ws transport (protocol.md#connection-metadata): a
// handshake that offered `wrpc.v2`, carried the declared bags as subprotocol
// tokens and was answered `wrpc.v1` may have reached a 1.0 server, which
// reads them from the connect URL only — so the transport dials again with
// the query. tests/interop/ proves it against the published 1.0 with the
// browser build; this drives the same branch in the tree's own modules, with
// a WebSocket that — like a page's — takes no init bag.

const { test } = require('node:test');
const assert = require('node:assert');

class PageSocket {
  static opened = [];
  // What the mocked server selects from an offer.
  static select = () => '';

  constructor(url, protocols) {
    // A browser's constructor reads a second argument as subprotocols; an
    // init bag is a SyntaxError there, which is what sends the Node half of
    // the handshake to the carrier tokens.
    if (protocols !== undefined && !Array.isArray(protocols)) throw new SyntaxError('no init bag');
    this.url = url;
    this.offer = protocols ?? [];
    this.protocol = PageSocket.select(this.offer);
    this.listeners = new Map();
    this.closed = false;
    PageSocket.opened.push(this);
    queueMicrotask(() => this.emit('open'));
  }

  addEventListener(type, fn) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(fn);
  }

  emit(type, payload) {
    for (const fn of this.listeners.get(type) ?? []) fn(payload);
  }

  send() {}

  close() {
    if (this.closed) return;
    this.closed = true;
    queueMicrotask(() => this.emit('close'));
  }
}

globalThis.WebSocket = PageSocket;

const { WrpcClient } = require('../../src/client.js');

const recorder = () => {
  const lines = [];
  return { lines, logger: { debug() {}, info() {}, error() {}, warn: (line) => lines.push(line) } };
};

const open = async (t, select, options = {}) => {
  PageSocket.opened = [];
  PageSocket.select = select;
  const { lines, logger } = recorder();
  const WsTransport = WrpcClient.transport.ws;
  const client = new WrpcClient('ws://page.test/api', new WsTransport('ws://page.test/api'), {
    heartbeat: false,
    reconnect: false,
    logger,
    headers: { 'x-tenant': 'acme' },
    meta: { userId: 7 },
    ...options,
  });
  t.after(() => void client.close());
  await client.open();
  const requeries = lines.filter((line) => line.event === 'handshake.requery');
  return { client, sockets: PageSocket.opened, requeries, lines };
};

const newest = (offer) => (offer.includes('wrpc.v2') ? 'wrpc.v2' : offer.includes('wrpc.v1') ? 'wrpc.v1' : '');
const legacy = (offer) => (offer.includes('wrpc.v1') ? 'wrpc.v1' : '');

test('requery: answered wrpc.v1 with tokens on the offer — dialled once more, with the query', async (t) => {
  const { client, sockets, requeries } = await open(t, legacy);
  assert.strictEqual(sockets.length, 2);
  const [first, second] = sockets;
  assert.deepStrictEqual(first.offer.slice(0, 2), ['wrpc.v2', 'wrpc.v1']);
  assert.ok(
    first.offer.some((name) => name.startsWith('wrpc.h.')),
    'the tokens rode the first offer',
  );
  assert.ok(!first.url.includes('wrpc_'), 'and its url was clean');
  assert.strictEqual(first.closed, true, 'the first socket is abandoned');
  assert.deepStrictEqual(second.offer, ['wrpc.v2', 'wrpc.v1'], 'no token the second time');
  assert.ok(
    second.url.includes('wrpc_h=') && second.url.includes('wrpc_meta='),
    `the query carries both (${second.url})`,
  );
  assert.strictEqual(requeries.length, 1);
  assert.strictEqual(client.revision, 1);
  assert.strictEqual(client.active, true);
  // The transport remembers: its next open goes straight to the query.
  client.close();
  await new Promise((resolve) => setImmediate(resolve));
  await client.open();
  assert.strictEqual(sockets.length, 3, 'one handshake, not two');
  assert.ok(sockets[2].url.includes('wrpc_h='));
  assert.strictEqual(requeries.length, 1, 'said once');
});

test('requery: answered wrpc.v2 — one handshake, the tokens were read', async (t) => {
  const { client, sockets, requeries } = await open(t, newest);
  assert.strictEqual(sockets.length, 1);
  assert.strictEqual(requeries.length, 0);
  assert.strictEqual(client.revision, 2);
});

test('requery: never for an application that chose its carrier, its offer, or declared nothing', async (t) => {
  // `carrier: 'protocol'` — the query is forbidden.
  const forced = await open(t, legacy, { carrier: 'protocol' });
  assert.strictEqual(forced.sockets.length, 1);
  // `attachments: false` offers wrpc.v1 alone: v1 is this end's own choice,
  // and says nothing about the server.
  const own = await open(t, legacy, { attachments: false });
  assert.strictEqual(own.sockets.length, 1);
  assert.deepStrictEqual(own.sockets[0].offer.slice(0, 1), ['wrpc.v1']);
  // An application's own protocols.
  const custom = await open(t, legacy, { protocols: ['wrpc.v1'] });
  assert.strictEqual(custom.sockets.length, 1);
  // Nothing declared: no token rode the offer, so nothing went unread.
  const bare = await open(t, legacy, { headers: undefined, meta: undefined });
  assert.strictEqual(bare.sockets.length, 1);
  assert.strictEqual(bare.requeries.length, 0);
});

test('requery: the redial names every declared header it put in the URL, not only a credential', async (t) => {
  const { lines } = await open(t, legacy, { headers: { 'x-tenant': 'acme', 'x-api-key': 'k' } });
  const exposed = lines.filter((line) => line.event === 'declared.exposed');
  assert.deepStrictEqual(exposed, [{ event: 'declared.exposed', keys: ['x-tenant', 'x-api-key'], carrier: 'query' }]);
});

test('requery: an offer of wrpc.v1 alone answered wrpc.v1 is ambiguous — no redial, said once', async (t) => {
  // `attachments: false` on the page: a 2.x server without frames read the
  // tokens, a 1.0 server did not, and both answer wrpc.v1.
  const { client, sockets, lines } = await open(t, legacy, { attachments: false });
  assert.strictEqual(sockets.length, 1, 'no redial');
  const said = () => lines.filter((line) => line.event === 'handshake.ambiguous').length;
  assert.strictEqual(said(), 1);
  client.close();
  await new Promise((resolve) => setImmediate(resolve));
  await client.open();
  assert.strictEqual(said(), 1, 'once per transport');
  // Nothing to say without tokens, nor for an application that chose its carrier.
  const bare = await open(t, legacy, { attachments: false, headers: undefined, meta: undefined });
  assert.strictEqual(bare.lines.filter((line) => line.event === 'handshake.ambiguous').length, 0);
  const chosen = await open(t, legacy, { attachments: false, carrier: 'protocol' });
  assert.strictEqual(chosen.lines.filter((line) => line.event === 'handshake.ambiguous').length, 0);
});
