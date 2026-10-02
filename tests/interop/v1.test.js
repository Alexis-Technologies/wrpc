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
