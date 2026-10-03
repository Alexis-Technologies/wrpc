'use strict';

// The protocol revision of a connection (protocol.md#versioning): what the
// two ends negotiate, and what each one sends once it is known. Revision 2
// is the framed messages — a packet whose bytes travel as bytes; a peer that
// negotiated revision 1 is sent the JSON 1.0 made of them. The published 1.0
// itself is the other end in tests/interop/; here a 2.x end plays it by
// offering, or selecting, `wrpc.v1`.

const { test } = require('node:test');
const assert = require('node:assert');
const { MessageChannel } = require('node:worker_threads');

const { RpcServer, defineRouter, procedure } = require('../../index.js');
const { WebsocketServer } = require('#ws');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');
const { recorder } = require('../helpers/recorder.js');

const BYTES = Uint8Array.of(1, 2, 3);
// What JSON.stringify makes of a Uint8Array — the form 1.0 sent and read.
const JSON_BYTES = { 0: 1, 1: 2, 2: 3 };

const boot = async (t, options = {}) => {
  const seen = { args: [], clients: [] };
  const router = defineRouter({
    files: {
      get: procedure({ access: 'public', handler: async () => ({ blob: BYTES }) }),
      put: procedure({
        access: 'public',
        handler: async (context, args) => {
          seen.args.push(args);
          seen.clients.push(context.client);
          return true;
        },
      }),
      join: procedure({
        access: 'public',
        handler: async (context) => {
          context.client.join('room');
          return context.client.revision;
        },
      }),
    },
  });
  const booted = await bootServer(t, { router, ...options });
  return { ...booted, seen };
};

const connect = async (t, url, options) => {
  const client = await connectClient(t, url, options);
  await client.load('files');
  return client;
};

test('revision: two 2.x ends negotiate wrpc.v2 — bytes travel as bytes, both ways', async (t) => {
  const { url, seen } = await boot(t);
  const client = await connect(t, url);
  assert.strictEqual(client.revision, 2);
  assert.strictEqual(await client.api.files.join(), 2, 'the server-side client says the same');
  assert.deepStrictEqual(await client.api.files.get(), { blob: BYTES });
  await client.api.files.put({ blob: BYTES });
  assert.ok(seen.args[0].blob instanceof Uint8Array);
});

test('revision: a client that offers wrpc.v1 alone is spoken to as 1.0 was', async (t) => {
  const { url, seen } = await boot(t);
  const client = await connect(t, url, { protocols: ['wrpc.v1'] });
  assert.strictEqual(client.revision, 1);
  assert.strictEqual(await client.api.files.join(), 1);
  // The result a 1.0 client could not have read as a frame arrives as JSON…
  assert.deepStrictEqual(await client.api.files.get(), { blob: JSON_BYTES });
  // …and what the client sends is JSON too: no frame leaves a revision-1 end.
  await client.api.files.put({ blob: BYTES });
  assert.deepStrictEqual(seen.args[0], { blob: JSON_BYTES });
});

test('revision: a peer that offers nothing negotiated nothing — revision 1', async (t) => {
  const { url } = await boot(t);
  const client = await connect(t, url, { protocols: [] });
  assert.strictEqual(client.revision, 1);
  assert.deepStrictEqual(await client.api.files.get(), { blob: JSON_BYTES });
});

test('revision: `attachments: false` on the server selects wrpc.v1 — a default client sends it no frame', async (t) => {
  const { server, url, seen } = await boot(t, { attachments: false });
  assert.strictEqual(server.rpc.revision, 1);
  const client = await connect(t, url);
  assert.strictEqual([...server.wsServer.connections][0].protocol, 'wrpc.v1');
  assert.strictEqual(client.revision, 1);
  // The call that used to be answered an id-less 500 and time out.
  assert.strictEqual(await client.api.files.put({ blob: BYTES }), true);
  assert.deepStrictEqual(seen.args[0], { blob: JSON_BYTES });
  assert.deepStrictEqual(await client.api.files.get(), { blob: JSON_BYTES });
});

test('revision: `attachments: false` on the client offers wrpc.v1 — a default server sends it no frame', async (t) => {
  const { server, url, seen } = await boot(t);
  assert.strictEqual(server.rpc.revision, 2);
  const client = await connect(t, url, { attachments: false });
  assert.strictEqual([...server.wsServer.connections][0].protocol, 'wrpc.v1');
  assert.strictEqual(client.revision, 1);
  // The result that used to arrive as a frame this client does not read.
  assert.deepStrictEqual(await client.api.files.get(), { blob: JSON_BYTES });
  await client.api.files.put({ blob: BYTES });
  assert.deepStrictEqual(seen.args[0], { blob: JSON_BYTES });
});

test('revision: an application that configured its own protocols owns the negotiation', async (t) => {
  // `attachments: false` narrows the engine only when the app did not.
  const { server, url } = await boot(t, { attachments: false, ws: { protocols: ['chat'] } });
  const client = await connect(t, url, { protocols: ['chat'] });
  assert.strictEqual([...server.wsServer.connections][0].protocol, 'chat');
  assert.strictEqual(client.revision, 1);
});

test('revision: a room of both revisions — one event, a frame for the 2.x client and JSON for the 1.0 one', async (t) => {
  const { server, url } = await boot(t);
  const modern = await connect(t, url);
  const old = await connect(t, url, { protocols: ['wrpc.v1'] });
  const second = await connect(t, url, { protocols: ['wrpc.v1'] });
  const got = { modern: [], old: [], second: [] };
  modern.api.files.on('changed', (data) => got.modern.push(data));
  old.api.files.on('changed', (data) => got.old.push(data));
  second.api.files.on('changed', (data) => got.second.push(data));
  await Promise.all([modern.api.files.join(), old.api.files.join(), second.api.files.join()]);
  assert.strictEqual(server.rpc.to('room').emit('files/changed', { blob: BYTES }), 3);
  await waitFor(() => got.modern.length === 1 && got.old.length === 1 && got.second.length === 1, 'all three hear it');
  assert.deepStrictEqual(got.modern, [{ blob: BYTES }]);
  assert.deepStrictEqual(got.old, [{ blob: JSON_BYTES }]);
  assert.deepStrictEqual(got.second, [{ blob: JSON_BYTES }]);
  // An event without bytes is the same text for everyone.
  server.rpc.to('room').emit('files/changed', { n: 1 });
  await waitFor(() => got.modern.length === 2 && got.old.length === 2, 'the plain event arrives');
  assert.deepStrictEqual([got.modern[1], got.old[1]], [{ n: 1 }, { n: 1 }]);
});

test('revision: a direct event to a revision-1 client is JSON', async (t) => {
  const { url, seen } = await boot(t);
  const old = await connect(t, url, { protocols: ['wrpc.v1'] });
  const got = [];
  old.api.files.on('changed', (data) => got.push(data));
  await old.api.files.put({});
  seen.clients[0].sendEvent('files/changed', { blob: BYTES });
  await waitFor(() => got.length === 1, 'the event arrives');
  assert.deepStrictEqual(got, [{ blob: JSON_BYTES }]);
});

test('revision: an engine composed by hand that selects wrpc.v2 for a frameless server is named in the log', async (t) => {
  // The shells narrow their engine to wrpc.v1 (revisionProtocols); a server
  // wired to a bare WebsocketServer has nobody to do it, and says so.
  const log = recorder();
  const rpc = new RpcServer({
    router: defineRouter({ unit: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } }),
    attachments: false,
    logger: log.writer,
  });
  const http = require('node:http').createServer();
  const wsServer = new WebsocketServer({ server: http, logger: false });
  wsServer.on('connection', (socket, req) => rpc.attachSocket(socket, { headers: req.headers, url: req.url }));
  await new Promise((resolve) => http.listen(0, '127.0.0.1', resolve));
  t.after(() => {
    wsServer.close();
    http.close();
    rpc.close();
  });
  const url = `ws://127.0.0.1:${http.address().port}/api`;
  const client = await connectClient(t, url);
  assert.strictEqual(client.revision, 2, 'the bare engine selected the newest revision offered');
  await waitFor(() => log.all('revision.mismatch').length === 1, 'the mismatch is logged');
  // A configuration error is the same on every connection: one warn, then debug.
  for (let i = 0; i < 4; i++) await connectClient(t, url);
  await waitFor(() => log.all('revision.mismatch').length === 5, 'every connection is still a line');
  assert.deepStrictEqual(
    log.all('revision.mismatch').map((entry) => entry.level),
    ['warn', 'debug', 'debug', 'debug', 'debug'],
  );
});

// ---- HTTP: no handshake, so the two directions say it in headers --------

const post = (url, packet, headers = {}) =>
  fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(packet),
  });

const FRAMES = { Accept: 'application/octet-stream, application/json' };

test('revision http: a server announces the revision it speaks, exposed to another origin', async (t) => {
  const on = await boot(t);
  const off = await boot(t, { attachments: false });
  const call = { type: 'call', id: '1', method: 'files/put', args: {} };
  const two = await post(`${on.origin}/api`, call);
  assert.strictEqual(two.headers.get('wrpc-version'), '2');
  assert.strictEqual(two.headers.get('access-control-expose-headers'), 'wrpc-version');
  const one = await post(`${off.origin}/api`, call);
  assert.strictEqual(one.headers.get('wrpc-version'), '1', '`attachments: false` speaks revision 1');
  const preflight = await fetch(`${on.origin}/api`, { method: 'OPTIONS' });
  assert.strictEqual(preflight.headers.get('wrpc-version'), '2');
});

test('revision http: a result holding bytes is a frame only for a caller that asked for one', async (t) => {
  const { origin } = await boot(t);
  const call = { type: 'call', id: '7', method: 'files/get', args: {} };
  // No Accept — a 1.0 client, curl: the JSON 1.0 answered.
  const plain = await post(`${origin}/api`, call);
  assert.strictEqual(plain.headers.get('content-type'), 'application/json');
  assert.deepStrictEqual(await plain.json(), { type: 'callback', id: '7', result: { blob: JSON_BYTES } });
  // Asked for: the frame.
  const framed = await post(`${origin}/api`, call, FRAMES);
  assert.strictEqual(framed.headers.get('content-type'), 'application/octet-stream');
  const bytes = new Uint8Array(await framed.arrayBuffer());
  assert.deepStrictEqual([bytes[0], bytes[1]], [0, 1], 'a framed message of kind 1');
});

test('revision http: an answer whose form follows Accept says so in Vary — a shared cache keys on it', async (t) => {
  const { origin } = await boot(t);
  const call = { type: 'call', id: '8', method: 'files/get', args: {} };
  const plain = await post(`${origin}/api`, call);
  const framed = await post(`${origin}/api`, call, FRAMES);
  assert.strictEqual(plain.headers.get('vary'), 'Accept');
  assert.strictEqual(framed.headers.get('vary'), 'Accept');
  const rest = await fetch(`${origin}/api/files/get`);
  assert.strictEqual(rest.headers.get('vary'), 'Accept', 'the conventional REST mode too');
  // Joined onto what is there: an origin allowlist's Vary: Origin.
  const listed = await boot(t, { cors: { origins: ['https://app.test'] } });
  const cross = await post(`${listed.origin}/api`, call, { origin: 'https://app.test' });
  assert.strictEqual(cross.headers.get('vary'), 'Origin, Accept');
  // A server that answers no frame does not vary on Accept.
  const off = await boot(t, { attachments: false });
  assert.strictEqual((await post(`${off.origin}/api`, call, FRAMES)).headers.get('vary'), null);
});

test('revision http: a batch and the conventional REST mode follow the same rule', async (t) => {
  const { origin } = await boot(t);
  const batch = [
    { type: 'call', id: 'a', method: 'files/get', args: {} },
    { type: 'call', id: 'b', method: 'files/put', args: {} },
  ];
  const plain = await post(`${origin}/api`, batch);
  assert.strictEqual(plain.headers.get('content-type'), 'application/json');
  assert.deepStrictEqual(await plain.json(), [
    { type: 'callback', id: 'a', result: { blob: JSON_BYTES } },
    { type: 'callback', id: 'b', result: true },
  ]);
  const framed = await post(`${origin}/api`, batch, FRAMES);
  assert.strictEqual(framed.headers.get('content-type'), 'application/octet-stream');
  // GET /api/files/get — what a browser's fetch or curl sends has no such
  // Accept, and reads JSON; the envelope is framed only on request.
  const rest = await fetch(`${origin}/api/files/get`);
  assert.strictEqual(rest.headers.get('content-type'), 'application/json');
  assert.deepStrictEqual((await rest.json()).result, { blob: JSON_BYTES });
  const restFramed = await fetch(`${origin}/api/files/get`, { headers: FRAMES });
  assert.strictEqual(restFramed.headers.get('content-type'), 'application/octet-stream');
});

test('revision http: the client sends a frame only after a response said 2', async (t) => {
  const { origin, seen } = await boot(t);
  const client = await connectClient(t, `${origin}/api`);
  assert.strictEqual(client.revision, 1, 'nothing heard yet');
  // Before any answer the server is unknown: bytes leave as 1.0 JSON.
  assert.strictEqual(await client.call('files/put', { blob: BYTES }), true);
  assert.deepStrictEqual(seen.args[0], { blob: JSON_BYTES });
  assert.strictEqual(client.revision, 2, 'the answer carried wrpc-version: 2');
  assert.strictEqual(await client.call('files/put', { blob: BYTES }), true);
  assert.ok(seen.args[1].blob instanceof Uint8Array);
  assert.deepStrictEqual(await client.call('files/get'), { blob: BYTES });
});

test('revision http: against a server that speaks revision 1 the client never sends a frame', async (t) => {
  const { origin, seen } = await boot(t, { attachments: false });
  const client = await connectClient(t, `${origin}/api`);
  await client.load('files');
  assert.strictEqual(client.revision, 1);
  assert.strictEqual(await client.api.files.put({ blob: BYTES }), true);
  assert.deepStrictEqual(seen.args[0], { blob: JSON_BYTES });
  assert.deepStrictEqual(await client.api.files.get(), { blob: JSON_BYTES });
});

test('revision http: a frame refused by a server that does not say 2 is sent again as JSON — a proxy page is not', async (t) => {
  // One address, two instances that disagree: the second reads no frames
  // (a 1.0 one refuses a frame the same way — tests/interop).
  const framed = await boot(t);
  const plain = await boot(t, { attachments: false });
  let requests = 0;
  const balanced = (url, init) => {
    const target = requests++ % 2 === 0 ? framed.origin : plain.origin;
    return fetch(target + String(url).slice(framed.origin.length), init);
  };
  const client = await connectClient(t, `${framed.origin}/api`, { fetch: balanced, callTimeout: 2000 });
  await client.load('files');
  assert.strictEqual(client.revision, 2);
  // The frame lands on the instance that reads none; it used to be a 408.
  assert.strictEqual(await client.api.files.put({ blob: BYTES }), true);
  assert.strictEqual(requests, 3, 'the load, the refused frame, the JSON');
  assert.deepStrictEqual(plain.seen.args, [], 'the frame was refused unread');
  assert.deepStrictEqual(framed.seen.args, [{ blob: JSON_BYTES }], 'the JSON went to the next instance');

  // An answer with no packet in it says nothing about the server behind it.
  const page = await boot(t);
  let fail = true;
  const flaky = async (url, init) => {
    if (typeof init.body !== 'string' && fail) {
      fail = false;
      return new Response('<html>502 Bad Gateway</html>', { status: 502, headers: { 'content-type': 'text/html' } });
    }
    return fetch(url, init);
  };
  const behindProxy = await connectClient(t, `${page.origin}/api`, { fetch: flaky, callTimeout: 2000 });
  await behindProxy.load('files');
  await assert.rejects(behindProxy.api.files.put({ blob: BYTES }), (error) => error.code === 502);
  assert.strictEqual(behindProxy.revision, 2, 'still 2');
});

test('revision http: `attachments: false` on the client asks for no frame', async (t) => {
  const { origin } = await boot(t);
  const client = await connectClient(t, `${origin}/api`, { attachments: false });
  await client.load('files');
  assert.deepStrictEqual(await client.api.files.get(), { blob: JSON_BYTES });
});

// ---- a worker port: no handshake, the revision rides the first ping ------

// What a worker does with a page's connect message: hands the port, and the
// message, to a server it holds. `forward` decides how much of the message.
const workerOf = (rpc, forward = () => null) => ({
  attached: [],
  postMessage(message, transfer) {
    if (message.type === 'wrpc:connect') this.attached.push(rpc.attachPort(transfer[0], forward(message)));
  },
});

const overPort = async (t, serverOptions = {}, clientOptions = {}) => {
  const { server, seen } = await boot(t, serverOptions);
  const worker = workerOf(server.rpc);
  const client = await connectClient(t, 'ws://unused.invalid/api', { worker, ...clientOptions });
  await client.load('files');
  return { client, seen, peer: worker.attached[0] };
};

test('revision port: a 2.x page and a 2.x server agree on 2 by the first ping — bytes travel as bytes', async (t) => {
  const { client, seen, peer } = await overPort(t);
  assert.strictEqual(client.revision, 2);
  assert.strictEqual(peer.revision, 2);
  await client.api.files.put({ blob: BYTES });
  assert.ok(seen.args[0].blob instanceof Uint8Array);
  assert.deepStrictEqual(await client.api.files.get(), { blob: BYTES });
});

test('revision port: either end that reads no frames keeps the port at revision 1', async (t) => {
  const server = await overPort(t, { attachments: false });
  assert.strictEqual(server.client.revision, 1);
  assert.strictEqual(server.peer.revision, 1);
  await server.client.api.files.put({ blob: BYTES });
  assert.deepStrictEqual(server.seen.args[0], { blob: JSON_BYTES });
  assert.deepStrictEqual(await server.client.api.files.get(), { blob: JSON_BYTES });

  const page = await overPort(t, {}, { attachments: false });
  assert.strictEqual(page.client.revision, 1);
  assert.strictEqual(page.peer.revision, 1);
  assert.deepStrictEqual(await page.client.api.files.get(), { blob: JSON_BYTES });
});

test("revision port: under a packet codec the ping and its pong are the codec's — and the goodbye still releases", async (t) => {
  // A server under a codec decodes everything a port carries with it: the
  // JSON ping was a malformed packet there, logged on every connect and
  // answered with an id-less 500 the page saw as an error.
  const codec = {
    encode: (packet) => JSON.stringify({ wrapped: packet }),
    // Like a binary codec: what it did not make is no packet at all.
    decode: (text) => {
      const outer = JSON.parse(text);
      return Object.hasOwn(outer, 'wrapped') ? outer.wrapped : null;
    },
  };
  const log = recorder();
  const errors = [];
  const { client, peer } = await overPort(t, { codec, logger: log.writer }, { codec });
  client.on('error', (error) => errors.push(error));
  assert.deepStrictEqual(await client.api.files.get(), { blob: JSON_BYTES });
  assert.strictEqual(client.revision, 1);
  assert.strictEqual(peer.revision, 1);
  assert.deepStrictEqual([...log.all('packet.malformed'), ...log.all('packet.unknown')], []);
  assert.deepStrictEqual(errors, []);
  // The pong named a revision, so the page knows a 2.x end and says goodbye.
  const closed = new Promise((resolve) => peer.once('close', resolve));
  client.close();
  await closed;
});

test('revision port: a page that names nothing — a 1.0 page — is answered a plain pong and sent no frame', async (t) => {
  const { server } = await boot(t);
  const { port1, port2 } = new MessageChannel();
  t.after(() => {
    port1.close();
    port2.close();
  });
  const peer = server.rpc.attachPort(port1);
  const replies = [];
  port2.on('message', (data) => replies.push(data));
  port2.postMessage('{"type":"ping"}');
  port2.postMessage(JSON.stringify({ type: 'call', id: 'c1', method: 'files/get', args: {} }));
  await waitFor(() => replies.length === 2, 'the pong and the answer');
  assert.strictEqual(replies[0], '{"type":"pong"}');
  assert.deepStrictEqual(JSON.parse(replies[1]), { type: 'callback', id: 'c1', result: { blob: JSON_BYTES } });
  assert.strictEqual(peer.revision, 1);
});

test('revision port: a consumer that hands the connect message over names the revision before any ping', async (t) => {
  const { server } = await boot(t);
  const { port1, port2 } = new MessageChannel();
  t.after(() => {
    port1.close();
    port2.close();
  });
  const replies = [];
  port2.on('message', (data) => replies.push(data));
  const peer = server.rpc.attachPort(port1, { v: 2 });
  assert.strictEqual(peer.revision, 2);
  // The server is the first to send: a frame already.
  peer.sendEvent('files/changed', { blob: BYTES });
  await waitFor(() => replies.length === 1, 'the event arrives');
  assert.ok(replies[0] instanceof Uint8Array, 'a framed message');
  // And a server that sends none is not talked into it.
  const off = await boot(t, { attachments: false });
  const other = new MessageChannel();
  t.after(() => {
    other.port1.close();
    other.port2.close();
  });
  assert.strictEqual(off.server.rpc.attachPort(other.port1, { v: 2 }).revision, 1);
});

test('revision: a ping that names a revision means nothing on a WebSocket — the subprotocol settled it', async (t) => {
  const { url } = await boot(t);
  const socket = new WebSocket(url, ['wrpc.v1']);
  t.after(() => socket.close());
  const frames = [];
  socket.addEventListener('message', ({ data }) => frames.push(data));
  await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));
  socket.send('{"type":"ping","v":2}');
  socket.send(JSON.stringify({ type: 'call', id: 'w1', method: 'files/get', args: {} }));
  await waitFor(() => frames.length === 2, 'the pong and the answer');
  assert.strictEqual(frames[0], '{"type":"pong"}');
  assert.deepStrictEqual(JSON.parse(frames[1]).result, { blob: JSON_BYTES });
});
