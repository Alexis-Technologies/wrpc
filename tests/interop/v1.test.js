'use strict';

// A 1.0 peer and this tree, talking to each other for real: the published
// 1.0 package on one end, the working tree on the other, in both directions
// and over every transport 1.0 had. What the protocol page promises about a
// mixed deployment (protocol.md#changes-since-1-0) is checked here against
// the 1.0 that shipped, not against a description of it.

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

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

// ---- revision 2 is negotiated: bytes with a 1.0 peer --------------------
//
// A 1.0 peer reads no framed message. Before the revision was negotiated a
// result holding bytes reached a 1.0 client as a frame it threw on (the call
// timed out after 7 s) and a 1.0 server answered a call holding bytes with
// an id-less 400. Now the subprotocol says who the peer is, and it is sent
// the JSON 1.0 itself made of a Uint8Array.

const BYTES = Uint8Array.of(1, 2, 3);
const JSON_BYTES = { 0: 1, 1: 2, 2: 3 };

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

describe('interop with the published 1.0', () => {
  it('should resolve the alias to the published 1.0', { skip }, () => {
    assert.strictEqual(require('wrpc-v1/package.json').version, '1.0.0');
    assert.notStrictEqual(legacy.WrpcClient, next.WrpcClient, 'two packages, not one module twice');
  });

  for (const pair of PAIRS) {
    it(`should carry a call and its answer over ws — ${pair.name}`, { skip }, async (t) => {
      const { ws } = await boot(t, pair.server);
      const client = await connect(t, pair.client, ws);
      assert.deepStrictEqual(await client.api.echo.say({ text: 'hello', n: [1, 2, 3] }), {
        text: 'hello',
        n: [1, 2, 3],
      });
    });

    it(`should bring a server event to the client over ws — ${pair.name}`, { skip }, async (t) => {
      const { ws } = await boot(t, pair.server);
      const client = await connect(t, pair.client, ws);
      const seen = [];
      client.api.echo.on('poke', (data) => seen.push(data));
      assert.strictEqual(await client.api.echo.nudge({ at: 7 }), true);
      await waitFor(() => seen.length === 1, 'the event arrives');
      assert.deepStrictEqual(seen, [{ at: 7 }]);
    });

    it(`should carry a call and its answer over http — ${pair.name}`, { skip }, async (t) => {
      const { http } = await boot(t, pair.server);
      const client = await connect(t, pair.client, http);
      assert.deepStrictEqual(await client.api.echo.say({ text: 'over http' }), { text: 'over http' });
    });
  }

  // The receiving half of a binary stream holds its sender to the size it
  // announced. Nothing changed on the wire, so a 1.0 sender — which
  // keeps its announcement like any other — is served as before, and a 1.0
  // receiver is sent what it always was.
  for (const pair of PAIRS) {
    it(`should carry a binary stream up and down over ws — ${pair.name}`, { skip }, async (t) => {
      const { ws } = await boot(t, pair.server);
      const client = await connect(t, pair.client, ws);
      const pieces = 8;
      const size = pieces * 64 * 1024;
      const upload = client.createStream('up.bin', size);
      const received = client.api.echo.upload({ id: upload.id });
      for (let i = 0; i < pieces; i++) upload.write(new Uint8Array(size / pieces).fill(i + 1));
      upload.end();
      assert.deepStrictEqual(await received, { name: 'up.bin', size });
      const id = await client.api.echo.download({ size, pieces });
      const readable = client.getStream(id);
      let downloaded = 0;
      for await (const chunk of readable) downloaded += chunk.length;
      assert.strictEqual(downloaded, size);
      assert.strictEqual(readable.bytesRead, size);
    });

    // The whole upload on the wire before the call that reads it: what a
    // readable has no reader for yet is held, under `maxStreamBuffer`.
    it(`should take an upload sent over ws ahead of the call that reads it — ${pair.name}`, { skip }, async (t) => {
      const { ws } = await boot(t, pair.server);
      const client = await connect(t, pair.client, ws);
      const size = 256 * 1024;
      const upload = client.createStream('ahead.bin', size);
      upload.write(new Uint8Array(size).fill(9));
      upload.end();
      assert.deepStrictEqual(await client.api.echo.upload({ id: upload.id }), { name: 'ahead.bin', size });
    });
  }

  // A receiver tells a sender that its stream is no longer read only where
  // the sender asked to hear (`cancel` on its announcement). A 1.0 sender
  // never asks, so it is never told — it would report a callback nobody
  // waited for — and a 1.0 receiver ignores the field and never tells: the
  // stream is sent whole, and dropped on arrival, as it always was.
  for (const pair of PAIRS) {
    it(`should send whole an upload its handler lets go of over ws — ${pair.name}`, { skip }, async (t) => {
      const { ws } = await boot(t, pair.server);
      const client = await connect(t, pair.client, ws);
      const escalated = [];
      client.on('error', (error) => escalated.push(error));
      const pieces = 8;
      const size = pieces * 16 * 1024;
      const upload = client.createStream('refused.bin', size);
      upload.write(new Uint8Array(size / pieces));
      assert.strictEqual(await client.api.echo.refuse({ id: upload.id }), true);
      // A round trip later the sender still has heard nothing.
      assert.deepStrictEqual(await client.api.echo.say({ at: 1 }), { at: 1 });
      assert.strictEqual(upload.closed, false, 'a sender facing 1.0 was told to stop');
      for (let i = 1; i < pieces; i++) assert.notStrictEqual(upload.write(new Uint8Array(size / pieces)), false);
      upload.end();
      assert.deepStrictEqual(await client.api.echo.say({ at: 2 }), { at: 2 });
      assert.deepStrictEqual(escalated, [], 'a reader letting go surfaced as an error on a 1.0 pairing');
    });

    it(`should send whole a download its reader lets go of over ws — ${pair.name}`, { skip }, async (t) => {
      const warned = [];
      const quiet = () => {};
      const logger = { log: quiet, debug: quiet, info: quiet, warn: (...args) => warned.push(args), error: quiet };
      const { ws } = await boot(t, pair.server, { logger });
      const client = await connect(t, pair.client, ws);
      const escalated = [];
      client.on('error', (error) => escalated.push(error));
      const size = 8 * 16 * 1024;
      const id = await client.api.echo.pour({ size });
      const readable = client.getStream(id);
      await readable.terminate();
      // The server writes all of it and ends the stream — which is what
      // makes the client forget it.
      await waitFor(() => {
        try {
          client.getStream(id);
          return false;
        } catch {
          return true;
        }
      }, 'the whole download, to its end');
      assert.deepStrictEqual(await client.api.echo.say({ at: 3 }), { at: 3 });
      // A 1.0 client has a race of its own here, which this server cannot
      // close: Node's WebSocket hands it every binary message as a Blob (1.0
      // never set `binaryType`), each chunk is read with an awaited
      // `blob.arrayBuffer()`, and the `end` behind it is handled without
      // waiting — once the reader terminated, that `end` forgets the stream at
      // once, and a chunk still being read finds it gone: `Stream <id> is not
      // initialized`. One CI run on a busy runner got one; a Blob read 5 ms
      // slower gets one for every chunk. It is the stream ending, not the
      // server telling a 1.0 client anything — which would be `Callback <id>
      // not found`, and stays an error. This tree's client counts a chunk in
      // the turn it arrived in, so the other direction keeps the strict check.
      const late = pair.client === legacy ? `Stream ${id} is not initialized` : null;
      assert.deepStrictEqual(
        escalated.filter((error) => error.message !== late),
        [],
      );
      assert.deepStrictEqual(warned, [], 'the server logged an answer nobody waited for');
    });
  }

  it('should carry a call and an event over sse from this client to a 1.0 server', { skip }, async (t) => {
    const { http } = await boot(t, legacy);
    const client = await connect(t, next, http, { transport: 'sse' });
    const seen = [];
    client.api.echo.on('poke', (data) => seen.push(data));
    assert.deepStrictEqual(await client.api.echo.say({ text: 'sse' }), { text: 'sse' });
    assert.strictEqual(await client.api.echo.nudge({ at: 1 }), true);
    await waitFor(() => seen.length === 1, 'the event arrives on the stream');
    assert.deepStrictEqual(seen, [{ at: 1 }]);
  });

  it('should serve a 1.0 client over ws under wrpc.v1, the revision it offered', { skip }, async (t) => {
    const { server, ws } = await boot(t, next);
    await connect(t, legacy, ws);
    const protocols = [...server.wsServer.connections].map((connection) => connection.protocol);
    assert.deepStrictEqual(protocols, ['wrpc.v1']);
  });

  for (const pair of PAIRS) {
    it(`should carry bytes in a call and in its answer over ws as 1.0 JSON — ${pair.name}`, { skip }, async (t) => {
      const { ws } = await boot(t, pair.server);
      const client = await connect(t, pair.client, ws, { callTimeout: 2000 });
      assert.deepStrictEqual(await client.api.echo.say({ blob: BYTES }), { blob: JSON_BYTES });
    });

    it(`should deliver an event holding bytes over ws as 1.0 JSON — ${pair.name}`, { skip }, async (t) => {
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
    it(`should carry bytes in a call and in its answer over http as 1.0 JSON — ${pair.name}`, { skip }, async (t) => {
      const { http } = await boot(t, pair.server);
      const client = await connect(t, pair.client, http, { callTimeout: 2000 });
      assert.deepStrictEqual(await client.api.echo.say({ blob: BYTES }), { blob: JSON_BYTES });
    });

    it(
      `should lose none of the calls of an http batch with bytes in one answer — ${pair.name}`,
      { skip },
      async (t) => {
        const { http } = await boot(t, pair.server);
        const client = await connect(t, pair.client, http, { callTimeout: 2000, batch: true });
        const answers = await Promise.all([
          client.api.echo.say({ n: 1 }),
          client.api.echo.say({ blob: BYTES }),
          client.api.echo.say({ n: 3 }),
        ]);
        assert.deepStrictEqual(answers, [{ n: 1 }, { blob: JSON_BYTES }, { n: 3 }]);
      },
    );
  }

  // SSE speaks revision 1 to every client: a 2.x server answered a 1.0 SSE
  // client's call holding bytes in its result with 501 and dropped such an
  // event, with no flag to set on either end.
  for (const pair of PAIRS) {
    it(
      `should carry bytes in a call, its answer and an event over sse as 1.0 JSON — ${pair.name}`,
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

  it(
    'should land a call holding bytes on either of a 2.x and a 1.0 instance behind one http address',
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

  it("should never raise this client's revision over http against a 1.0 server", { skip }, async (t) => {
    const { http } = await boot(t, legacy);
    const client = await connect(t, next, http);
    await client.api.echo.say({ n: 1 });
    assert.strictEqual(client.revision, 1);
  });

  it('should let this client know a 1.0 server over ws by the revision it selected', { skip }, async (t) => {
    const { ws } = await boot(t, legacy);
    const client = await connect(t, next, ws);
    assert.strictEqual(client.revision, 1);
  });

  it(
    'should bring a room broadcast with bytes to a 1.0 client and a 2.x client over ws, each in its own form',
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

  it(
    'should bring a page’s declared headers and meta to a 1.0 server over ws — redialled once with the query',
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

  it(
    'should let the same page keep its tokens against this server — no second handshake, a clean URL',
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

  it(
    'should take `carrier: "protocol"` as the application’s choice — never redialled, the labels are lost on 1.0',
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

  // A `wrpc.v1` answer is ambiguous: a 2.x server that sends no frames gives
  // it too. Both directions of that are said, since neither can be told apart.
  it(
    'should have a 2.x server without frames answer wrpc.v1 too — redialled for nothing, and the URL is named',
    { skip },
    async (t) => {
      const page = await browserBuild();
      const { server, ws } = await boot(t, next, { attachments: false });
      let handshakes = 0;
      server.wsServer.on('connection', () => handshakes++);
      const { lines, logger } = recorder();
      const client = await connect(t, page, ws, { headers: { 'x-tenant': 'acme', 'x-api-key': 'k' }, logger });
      assert.strictEqual((await client.api.echo.seen()).tenant, 'acme');
      assert.strictEqual(handshakes, 2, 'the tokens were read the first time; the redial cannot know');
      assert.deepStrictEqual(
        lines.filter((line) => line.event === 'declared.exposed'),
        [{ event: 'declared.exposed', keys: ['x-tenant', 'x-api-key'], carrier: 'query' }],
        'every header the redial put in the URL is named',
      );
    },
  );

  it(
    'should never redial a page that offers wrpc.v1 alone — against 1.0 its labels are lost, and said',
    { skip },
    async (t) => {
      const page = await browserBuild();
      const { ws } = await boot(t, legacy);
      const { lines, logger } = recorder();
      const client = await connect(t, page, ws, { ...LABELS, attachments: false, logger });
      assert.strictEqual((await client.api.echo.seen()).tenant, null, 'a 1.0 server read no token');
      assert.deepStrictEqual(
        lines.filter((line) => line.event === 'handshake.ambiguous'),
        [{ event: 'handshake.ambiguous' }],
      );
      // `carrier: 'query'` is the way to a 1.0 server.
      const queried = await connect(t, page, ws, { ...LABELS, attachments: false, carrier: 'query' });
      assert.strictEqual((await queried.api.echo.seen()).tenant, 'acme');
    },
  );

  it('should connect a page with nothing to declare to a 1.0 server over ws in one handshake', { skip }, async (t) => {
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

  it(
    'should serve a 1.0 client over sse from this server — calls, an event, and one channel for all of it',
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

  for (const pair of PAIRS) {
    it(`should carry bytes over a port as 1.0 JSON, and close the page cleanly — ${pair.name}`, { skip }, async (t) => {
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

    // Under a packet codec the port's first ping is the codec's: a JSON ping
    // was a malformed packet to a server that decodes with it.
    it(`should take the codec's ping over a port under a packet codec — ${pair.name}`, { skip }, async (t) => {
      const codec = {
        encode: (packet) => JSON.stringify({ wrapped: packet }),
        decode: (text) => {
          const outer = JSON.parse(text);
          return Object.hasOwn(outer, 'wrapped') ? outer.wrapped : null;
        },
      };
      const { server } = await boot(t, pair.server, { codec });
      const errors = [];
      const worker = workerOf(server.rpc);
      const client = await connect(t, pair.client, 'ws://unused.invalid/api', { worker, codec, callTimeout: 2000 });
      client.on('error', (error) => errors.push(error));
      assert.deepStrictEqual(await client.api.echo.say({ n: 1 }), { n: 1 });
      assert.deepStrictEqual(await client.api.echo.say({ blob: BYTES }), { blob: JSON_BYTES });
      assert.deepStrictEqual(errors, []);
    });
  }

  it(
    'should run this page behind a 1.0 worker proxy — bytes as 1.0 JSON, and no goodbye it would throw on',
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

  it('should bring a frame from the server to a 1.0 page behind this worker proxy as JSON', { skip }, async (t) => {
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
  });
});
