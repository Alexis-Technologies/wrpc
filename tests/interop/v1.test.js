'use strict';

// A 1.0 peer and this tree, talking to each other for real: the published
// 1.0 package on one end, the working tree on the other, in both directions
// and over every transport 1.0 had. What the protocol page promises about a
// mixed deployment (protocol.md#changes-since-1-0) is checked here against
// the 1.0 that shipped, not against a description of it.

const { test } = require('node:test');
const assert = require('node:assert');

const { next, legacy, boot, connect } = require('./peers.js');
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
