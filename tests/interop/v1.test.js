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

// SSE speaks revision 1 to every client: a 2.x server answered a 1.0 SSE
// client's call holding bytes in its result with 501 and dropped such an
// event, with no flag to set on either end.
for (const pair of PAIRS) {
  test(
    `interop sse: bytes in a call, its answer and an event travel as 1.0 JSON — ${pair.name}`,
    { skip },
    async (t) => {
      const { http } = await boot(t, pair.server);
      const client = await connect(t, pair.client, http, { transport: 'sse', callTimeout: 2000 });
      assert.deepStrictEqual(await client.api.echo.say({ blob: BYTES }), { blob: JSON_BYTES }, "the client's bytes");
      assert.deepStrictEqual(await client.api.echo.make(), { blob: JSON_BYTES }, "the server's bytes in a result");
      const seen = [];
      client.api.echo.on('poke', (data) => seen.push(data));
      assert.strictEqual(await client.api.echo.nudge({ blob: BYTES }), true);
      assert.strictEqual(await client.api.echo.blast(), true);
      await waitFor(() => seen.length === 2, 'both events arrive on the stream');
      assert.deepStrictEqual(seen, [{ blob: JSON_BYTES }, { blob: JSON_BYTES }]);
    },
  );
}

test(
  'interop http: a 2.x and a 1.0 instance behind one address — a call holding bytes lands on either',
  { skip },
  async (t) => {
    const modern = await boot(t, next);
    const old = await boot(t, legacy);
    // A round-robin balancer: every other request goes to the 1.0 instance.
    let requests = 0;
    const balanced = (url, init) => {
      const base = requests++ % 2 === 0 ? modern.http : old.http;
      return fetch(base + String(url).slice(modern.http.length), init);
    };
    const client = await connect(t, next, modern.http, { fetch: balanced, callTimeout: 2000 });
    // The load was answered `wrpc-version: 2`, so the next call leaves as a
    // frame — and reaches the 1.0 instance, which refuses it unread. It used
    // to end in a 408; it is sent again as JSON.
    assert.strictEqual(client.revision, 2);
    for (let i = 0; i < 4; i++) {
      assert.deepStrictEqual(await client.api.echo.say({ blob: BYTES }), { blob: JSON_BYTES }, `call ${i}`);
    }
  },
);

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

// ---- SSE: the channel secret, with no revision to negotiate --------------
//
// A 2.x server guards an SSE channel with a secret, and a 1.0 client knows
// of none: it presents the `channel` of the `ready` frame, whatever it is.
// So the frame carries the whole reference there — `<id>.<secret>` — and a
// 1.0 client presents the secret without knowing it holds one. (Before, it
// presented the id alone, was answered 409 on every POST and re-opened the
// channel for ever.)

test(
  'interop sse: a 1.0 client against this server — calls, an event, and one channel for all of it',
  { skip },
  async (t) => {
    const { server, http } = await boot(t, next);
    const client = await connect(t, legacy, http, { transport: 'sse', callTimeout: 2000 });
    const seen = [];
    client.api.echo.on('poke', (data) => seen.push(data));
    assert.deepStrictEqual(await client.api.echo.say({ text: 'sse' }), { text: 'sse' });
    assert.strictEqual(await client.api.echo.nudge({ at: 1 }), true);
    await waitFor(() => seen.length === 1, 'the event arrives on the stream');
    assert.deepStrictEqual(seen, [{ at: 1 }]);
    assert.strictEqual(server.rpc.sse.size, 1, 'the channel it opened is the channel it kept');
  },
);

// ---- a worker port --------------------------------------------------------
//
// A page and the worker it talks to are deployed apart: a tab loaded before
// a release keeps its old bundle next to a new worker, and a Service Worker
// outlives the pages that installed it. A port has no handshake, so the
// revision rides the page's first ping — which a 1.0 end answers plainly.

const workerOf = (rpc) => ({
  attached: [],
  postMessage(message, transfer) {
    if (message.type === 'wrpc:connect') this.attached.push(rpc.attachPort(transfer[0]));
  },
});

for (const pair of PAIRS) {
  test(`interop port: bytes travel as 1.0 JSON, and the page closes cleanly — ${pair.name}`, { skip }, async (t) => {
    const { server } = await boot(t, pair.server);
    const unhandled = [];
    const onUnhandled = (error) => unhandled.push(error);
    process.on('unhandledRejection', onUnhandled);
    t.after(() => process.off('unhandledRejection', onUnhandled));
    const worker = workerOf(server.rpc);
    const client = await connect(t, pair.client, 'ws://unused.invalid/api', { worker, callTimeout: 2000 });
    assert.deepStrictEqual(await client.api.echo.say({ blob: BYTES }), { blob: JSON_BYTES });
    // A 1.0 server holds a port client for one call only (it became
    // persistent in 2.0), so an event down the port is this server's to send.
    if (pair.server === next) {
      const seen = [];
      client.api.echo.on('poke', (data) => seen.push(data));
      assert.strictEqual(await client.api.echo.nudge({ blob: BYTES }), true);
      await waitFor(() => seen.length === 1, 'the event arrives');
      assert.deepStrictEqual(seen, [{ blob: JSON_BYTES }]);
    }
    // This tree's page says goodbye on its port before closing it — to an
    // end that named a revision; a 1.0 end reads every message as a packet.
    // (A 1.0 client's own close is left to the teardown: it throws on a
    // second one.)
    if (pair.client === next) client.close();
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.deepStrictEqual(unhandled, [], 'nothing was posted that the other end throws on');
  });
}

// The worker PROXY — one socket for every tab — with a page of the other
// package in front of it. `self` is what a worker's global scope gives the
// proxy: a message bus, and the location its socket is opened against.
const workerScope = (t, endpoint) => {
  const listeners = [];
  globalThis.self = {
    addEventListener: (type, fn) => listeners.push({ type, fn }),
    // `ws://` + host is all a 1.0 proxy builds its URL from.
    location: { protocol: 'http:', host: endpoint.slice('ws://'.length) },
  };
  t.after(() => {
    delete globalThis.self;
  });
  // What the page holds as its `worker`.
  return {
    postMessage(message, transfer = []) {
      for (const { type, fn } of listeners) if (type === 'message') fn({ data: message, ports: transfer });
    },
  };
};

test(
  'interop proxy: this page behind a 1.0 worker proxy — bytes as 1.0 JSON, and no goodbye it would throw on',
  { skip },
  async (t) => {
    const { ws } = await boot(t, next);
    const unhandled = [];
    const onUnhandled = (error) => unhandled.push(error);
    process.on('unhandledRejection', onUnhandled);
    t.after(() => process.off('unhandledRejection', onUnhandled));
    const worker = workerScope(t, ws);
    const proxy = new legacy.WrpcClientProxy({ reconnect: false, heartbeat: false });
    t.after(() => proxy.close());
    const page = await connect(t, next, 'ws://unused.invalid/api', { worker, callTimeout: 2000 });
    assert.strictEqual(page.revision, 1, 'a 1.0 proxy answers the first ping plainly');
    assert.deepStrictEqual(await page.api.echo.say({ blob: BYTES }), { blob: JSON_BYTES });
    page.close();
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.deepStrictEqual(unhandled, []);
  },
);

test(
  'interop proxy: a 1.0 page behind this worker proxy — a frame from the server reaches it as JSON',
  { skip },
  async (t) => {
    const { ws } = await boot(t, next);
    const worker = workerScope(t, ws);
    const proxy = new next.WrpcClientProxy({ url: ws, reconnect: false, heartbeat: false });
    t.after(() => proxy.close());
    const page = await connect(t, legacy, 'ws://unused.invalid/api', { worker, callTimeout: 2000 });
    // Upstream is revision 2 (two 2.x ends): the server answers bytes as a
    // frame, which a 1.0 page would throw on — the proxy posts it the JSON.
    assert.deepStrictEqual(await page.api.echo.say({ blob: BYTES }), { blob: JSON_BYTES });
    const seen = [];
    page.api.echo.on('poke', (data) => seen.push(data));
    assert.strictEqual(await page.api.echo.nudge({ blob: BYTES }), true);
    await waitFor(() => seen.length === 1, 'the event arrives');
    assert.deepStrictEqual(seen, [{ blob: JSON_BYTES }]);
  },
);
