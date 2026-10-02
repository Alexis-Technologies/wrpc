'use strict';

// A 1.0 peer and this tree, talking to each other for real: the published
// 1.0 package on one end, the working tree on the other, in both directions
// and over every transport 1.0 had. What the protocol page promises about a
// mixed deployment (protocol.md#changes-since-1-0) is checked here against
// the 1.0 that shipped, not against a description of it.

const { test } = require('node:test');
const assert = require('node:assert');

const { next, legacy, boot, connect, browserBuild } = require('./peers.js');
const { waitFor } = require('../helpers/wait.js');

const skip = legacy ? false : 'wrpc-v1 (the published 1.0) is not installed';

// Each row: who serves, who calls. The suite runs every case both ways.
const PAIRS = legacy
  ? [
      { name: '1.0 client → this server', server: next, client: legacy },
      { name: 'this client → 1.0 server', server: legacy, client: next },
    ]
  : [];

test('interop: the published 1.0 is what the alias resolves to', { skip }, () => {
  assert.strictEqual(require('wrpc-v1/package.json').version, '1.0.0');
  assert.notStrictEqual(legacy.WrpcClient, next.WrpcClient, 'two packages, not one module twice');
});

for (const pair of PAIRS) {
  test(`interop ws: a call and its answer — ${pair.name}`, { skip }, async (t) => {
    const { ws } = await boot(t, pair.server);
    const client = await connect(t, pair.client, ws);
    assert.deepStrictEqual(await client.api.echo.say({ text: 'hello', n: [1, 2, 3] }), { text: 'hello', n: [1, 2, 3] });
  });

  test(`interop ws: a server event reaches the client — ${pair.name}`, { skip }, async (t) => {
    const { ws } = await boot(t, pair.server);
    const client = await connect(t, pair.client, ws);
    const seen = [];
    client.api.echo.on('poke', (data) => seen.push(data));
    assert.strictEqual(await client.api.echo.nudge({ at: 7 }), true);
    await waitFor(() => seen.length === 1, 'the event arrives');
    assert.deepStrictEqual(seen, [{ at: 7 }]);
  });

  test(`interop http: a call and its answer — ${pair.name}`, { skip }, async (t) => {
    const { http } = await boot(t, pair.server);
    const client = await connect(t, pair.client, http);
    assert.deepStrictEqual(await client.api.echo.say({ text: 'over http' }), { text: 'over http' });
  });
}

test('interop sse: this client against a 1.0 server — a call and an event', { skip }, async (t) => {
  const { http } = await boot(t, legacy);
  const client = await connect(t, next, http, { transport: 'sse' });
  const seen = [];
  client.api.echo.on('poke', (data) => seen.push(data));
  assert.deepStrictEqual(await client.api.echo.say({ text: 'sse' }), { text: 'sse' });
  assert.strictEqual(await client.api.echo.nudge({ at: 1 }), true);
  await waitFor(() => seen.length === 1, 'the event arrives on the stream');
  assert.deepStrictEqual(seen, [{ at: 1 }]);
});

test('interop ws: a 1.0 client is served under wrpc.v1, the revision it offered', { skip }, async (t) => {
  const { server, ws } = await boot(t, next);
  await connect(t, legacy, ws);
  const protocols = [...server.wsServer.connections].map((connection) => connection.protocol);
  assert.deepStrictEqual(protocols, ['wrpc.v1']);
});

// ---- revision 2 is negotiated: bytes with a 1.0 peer --------------------
//
// A 1.0 peer reads no framed message. Before the revision was negotiated a
// result holding bytes reached a 1.0 client as a frame it threw on (the call
// timed out after 7 s) and a 1.0 server answered a call holding bytes with
// an id-less 400. Now the subprotocol says who the peer is, and it is sent
// the JSON 1.0 itself made of a Uint8Array.

const BYTES = Uint8Array.of(1, 2, 3);
const JSON_BYTES = { 0: 1, 1: 2, 2: 3 };

for (const pair of PAIRS) {
  test(`interop ws: bytes in a call and in its answer travel as 1.0 JSON — ${pair.name}`, { skip }, async (t) => {
    const { ws } = await boot(t, pair.server);
    const client = await connect(t, pair.client, ws, { callTimeout: 2000 });
    assert.deepStrictEqual(await client.api.echo.say({ blob: BYTES }), { blob: JSON_BYTES });
  });

  test(`interop ws: an event holding bytes arrives as 1.0 JSON — ${pair.name}`, { skip }, async (t) => {
    const { ws } = await boot(t, pair.server);
    const client = await connect(t, pair.client, ws, { callTimeout: 2000 });
    const seen = [];
    client.api.echo.on('poke', (data) => seen.push(data));
    assert.strictEqual(await client.api.echo.nudge({ blob: BYTES }), true);
    await waitFor(() => seen.length === 1, 'the event arrives');
    assert.deepStrictEqual(seen, [{ blob: JSON_BYTES }]);
  });
}

for (const pair of PAIRS) {
  test(`interop http: bytes in a call and in its answer travel as 1.0 JSON — ${pair.name}`, { skip }, async (t) => {
    const { http } = await boot(t, pair.server);
    const client = await connect(t, pair.client, http, { callTimeout: 2000 });
    assert.deepStrictEqual(await client.api.echo.say({ blob: BYTES }), { blob: JSON_BYTES });
  });

  test(`interop http: a batch with bytes in one answer loses none of its calls — ${pair.name}`, { skip }, async (t) => {
    const { http } = await boot(t, pair.server);
    const client = await connect(t, pair.client, http, { callTimeout: 2000, batch: true });
    const answers = await Promise.all([
      client.api.echo.say({ n: 1 }),
      client.api.echo.say({ blob: BYTES }),
      client.api.echo.say({ n: 3 }),
    ]);
    assert.deepStrictEqual(answers, [{ n: 1 }, { blob: JSON_BYTES }, { n: 3 }]);
  });
}

test('interop http: this client never raises its revision against a 1.0 server', { skip }, async (t) => {
  const { http } = await boot(t, legacy);
  const client = await connect(t, next, http);
  await client.api.echo.say({ n: 1 });
  assert.strictEqual(client.revision, 1);
});

test('interop ws: this client knows a 1.0 server by the revision it selected', { skip }, async (t) => {
  const { ws } = await boot(t, legacy);
  const client = await connect(t, next, ws);
  assert.strictEqual(client.revision, 1);
});

test(
  'interop ws: a room broadcast with bytes reaches a 1.0 client and a 2.x client, each in its own form',
  { skip },
  async (t) => {
    const { server, ws } = await boot(t, next);
    const old = await connect(t, legacy, ws);
    const modern = await connect(t, next, ws);
    const got = { old: [], modern: [] };
    old.api.echo.on('poke', (data) => got.old.push(data));
    modern.api.echo.on('poke', (data) => got.modern.push(data));
    assert.strictEqual(server.rpc.broadcast('echo/poke', { blob: BYTES }), 2);
    await waitFor(() => got.old.length === 1 && got.modern.length === 1, 'both hear the broadcast');
    assert.deepStrictEqual(got.old, [{ blob: JSON_BYTES }]);
    assert.deepStrictEqual(got.modern, [{ blob: BYTES }]);
  },
);

// ---- a page's labels against a 1.0 server --------------------------------
//
// A browser cannot set a handshake header, so a 2.x page declares `headers`
// and `meta` as `wrpc.h.`/`wrpc.m.` subprotocol tokens — which 1.0 never
// read: it took them from the connect URL. A server that answers `wrpc.v1`
// to an offer of `wrpc.v2` may be that server, so the client dials again
// with the query.

const recorder = () => {
  const lines = [];
  return { lines, logger: { debug() {}, info() {}, error() {}, warn: (line) => lines.push(line) } };
};

const LABELS = { headers: { 'x-tenant': 'acme' }, meta: { userId: 7 } };

test(
  'interop ws: a page’s declared headers and meta reach a 1.0 server — redialled once with the query',
  { skip },
  async (t) => {
    const page = await browserBuild();
    const { server, ws } = await boot(t, legacy);
    let handshakes = 0;
    server.wsServer.on('connection', () => handshakes++);
    const { lines, logger } = recorder();
    const client = await connect(t, page, ws, { ...LABELS, logger });
    const seen = await client.api.echo.seen();
    assert.strictEqual(seen.tenant, 'acme');
    assert.deepStrictEqual(seen.data, { 'user-id': 7 });
    assert.ok(seen.url.includes('wrpc_h='), `the query carrier 1.0 reads (url was '${seen.url}')`);
    assert.strictEqual(handshakes, 2, 'the tokens, then the query');
    // The transport remembers: a later open goes straight to the query.
    client.close();
    await waitFor(() => !client.active, 'closed');
    await client.open();
    assert.strictEqual((await client.api.echo.seen()).tenant, 'acme');
    assert.strictEqual(handshakes, 3, 'one handshake for the reopen, not two');
    assert.deepStrictEqual(
      lines.filter((line) => line.event === 'handshake.requery'),
      [{ event: 'handshake.requery' }],
      'said once',
    );
    assert.strictEqual(client.revision, 1);
  },
);

test(
  'interop ws: against this server the same page keeps its tokens — no second handshake, a clean URL',
  { skip },
  async (t) => {
    const page = await browserBuild();
    const { ws } = await boot(t, next);
    const { lines, logger } = recorder();
    const client = await connect(t, page, ws, { ...LABELS, logger });
    const seen = await client.api.echo.seen();
    assert.strictEqual(seen.tenant, 'acme');
    assert.deepStrictEqual(seen.data, { 'user-id': 7 });
    assert.ok(!seen.url.includes('wrpc_'), `nothing rides the url (was '${seen.url}')`);
    assert.deepStrictEqual(lines, []);
    assert.strictEqual(client.revision, 2);
  },
);

test(
  'interop ws: `carrier: "protocol"` is the application’s choice — never redialled, the labels are lost on 1.0',
  { skip },
  async (t) => {
    const page = await browserBuild();
    const { ws } = await boot(t, legacy);
    const { lines, logger } = recorder();
    const client = await connect(t, page, ws, { ...LABELS, carrier: 'protocol', logger });
    const seen = await client.api.echo.seen();
    assert.strictEqual(seen.tenant, null);
    assert.deepStrictEqual(seen.data, {});
    assert.deepStrictEqual(lines, []);
  },
);

test('interop ws: a page with nothing to declare connects to a 1.0 server in one handshake', { skip }, async (t) => {
  const page = await browserBuild();
  const { server, ws } = await boot(t, legacy);
  let handshakes = 0;
  server.wsServer.on('connection', () => handshakes++);
  const { lines, logger } = recorder();
  const client = await connect(t, page, ws, { logger });
  assert.deepStrictEqual(await client.api.echo.say({ ok: true }), { ok: true });
  assert.strictEqual(handshakes, 1);
  assert.deepStrictEqual(lines, []);
});
