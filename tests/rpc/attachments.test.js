'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { MessageChannel } = require('node:worker_threads');

const { RpcServer, Server, defineRouter, procedure } = require('../../index.js');
require('../../sse.js');
const { hasBytes, encodeAttachments, decodeAttachments, isAttachmentsFrame } = require('../../src/attachments.js');
const { FRAME_MARK, FRAME_ATTACHMENTS } = require('../../src/wire.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const bytes = (n, fill) => new Uint8Array(n).fill(fill);
const same = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)) === 0;

// ---- the frame itself ------------------------------------------------

test('hasBytes: typed arrays, ArrayBuffers and DataViews anywhere within 32 levels', () => {
  assert.strictEqual(hasBytes(null), false);
  assert.strictEqual(hasBytes('x'), false);
  assert.strictEqual(hasBytes({ a: 1, b: [1, 2, { c: 'x' }] }), false);
  assert.strictEqual(hasBytes(new Uint8Array(1)), true);
  assert.strictEqual(hasBytes(new ArrayBuffer(1)), true);
  assert.strictEqual(hasBytes(new DataView(new ArrayBuffer(1))), true);
  assert.strictEqual(hasBytes(new Float32Array(2)), true);
  assert.strictEqual(hasBytes(Buffer.from('x')), true);
  assert.strictEqual(hasBytes({ a: { b: [{ c: new Uint8Array(1) }] } }), true);
  assert.strictEqual(hasBytes(new Map([['k', new Uint8Array(1)]])), false, 'a Map has no JSON keys');
  let deep = new Uint8Array(1);
  for (let i = 0; i < 40; i++) deep = { deep };
  assert.strictEqual(hasBytes(deep), false, 'past the depth cap, JSON would not reach it either');
});

test('encode/decode: every byte leaf travels as bytes, the packet is not mutated, and the copies own their bytes', () => {
  const chunk = bytes(1000, 7);
  const packet = {
    type: 'call',
    id: 'c1',
    method: 'files/put',
    args: {
      name: 'a.bin',
      body: chunk,
      parts: [bytes(3, 1), { nested: new ArrayBuffer(2) }],
      empty: new Uint8Array(0),
      // A DataView and a typed array over a larger buffer: byteOffset and
      // byteLength must be honoured, not the whole backing buffer sent.
      view: new DataView(new Uint8Array([9, 8, 7, 6]).buffer, 1, 2),
      shorts: new Uint16Array([258, 772]),
    },
  };
  const before = JSON.stringify(packet);
  const frame = encodeAttachments(packet);
  assert.strictEqual(JSON.stringify(packet), before, "the caller's packet is untouched");
  assert.ok(isAttachmentsFrame(frame));
  assert.strictEqual(frame[0], FRAME_MARK);
  assert.strictEqual(frame[1], FRAME_ATTACHMENTS);
  assert.ok(frame.length < 1000 + 3 + 2 + 2 + 4 + 350, `${frame.length} B: the bytes plus a small header, no base64`);
  const decoded = decodeAttachments(frame);
  assert.strictEqual(decoded.method, 'files/put');
  assert.ok(decoded.args.body instanceof Uint8Array && !Buffer.isBuffer(decoded.args.body));
  assert.ok(same(decoded.args.body, chunk));
  assert.ok(same(decoded.args.parts[0], bytes(3, 1)));
  assert.ok(decoded.args.parts[1].nested instanceof Uint8Array);
  assert.strictEqual(decoded.args.parts[1].nested.length, 2);
  assert.strictEqual(decoded.args.empty.length, 0);
  assert.ok(same(decoded.args.view, new Uint8Array([8, 7])));
  assert.ok(same(decoded.args.shorts, new Uint8Array(new Uint16Array([258, 772]).buffer)));
  // A copy, not a view: scribbling on the frame afterwards changes nothing.
  frame.fill(0xff, 6);
  assert.ok(same(decoded.args.body, chunk));
  // A batch array at the root works the same way.
  const batch = decodeAttachments(encodeAttachments([packet, { type: 'call', id: 'c2', method: 'x', args: {} }]));
  assert.strictEqual(batch.length, 2);
  assert.ok(same(batch[0].args.body, chunk));
});

test('decode: every malformed frame is a TypeError, never a wrong packet', () => {
  const frame = encodeAttachments({ type: 'event', name: 'x', data: { b: bytes(4, 1) } });
  const header = (json) => {
    const text = Buffer.from(json);
    const out = Buffer.alloc(6 + text.length);
    out[0] = FRAME_MARK;
    out[1] = FRAME_ATTACHMENTS;
    out.writeUInt32BE(text.length, 2);
    text.copy(out, 6);
    return out;
  };
  const refuses = (input, pattern) => assert.throws(() => decodeAttachments(input), pattern);
  refuses(new Uint8Array([1, 2, 3]), /not an attachments frame/);
  refuses(frame.subarray(0, 10), /past the end|byte length/);
  refuses(Buffer.concat([header('{'), Buffer.alloc(0)]), /not JSON/);
  refuses(header('{"a":1}'), /not \[packet, index\]/);
  refuses(header('[{}, 1]'), /index is not an array/);
  refuses(header('[{}, [1]]'), /index entry/);
  refuses(header('[{}, [[[], 0]]]'), /bad path/);
  refuses(header('[{"a":null}, [[["a"], 5]]]'), /bad length/);
  refuses(header('[{"a":null}, [[["__proto__"], 0]]]'), /forbidden key/);
  refuses(header('[{"a":1}, [[["a","b"], 0]]]'), /non-container/);
  refuses(header('[{"a":{}}, [[["a","b"], 0]]]'), /missing key/);
  refuses(header('[{"a":1}, [[["a"], 0]]]'), /placeholder is not null/);
  refuses(Buffer.concat([header('[{"a":null}, [[["a"], 0]]]'), Buffer.from([1])]), /byte length mismatch/);
  const deep = Array.from({ length: 33 }, (_, i) => `k${i}`);
  refuses(header(`[{}, [[${JSON.stringify(deep)}, 0]]]`), /bad path/);
  // A data key spelled `constructor` is an own placeholder like any other: it is restored, not refused.
  const ok = decodeAttachments(encodeAttachments({ type: 'event', name: 'x', data: { constructor: bytes(1, 9) } }));
  assert.ok(ok.data.constructor instanceof Uint8Array);
  assert.strictEqual(Object.getPrototypeOf(ok.data), Object.prototype, 'the prototype is untouched');
});

// ---- over the wire --------------------------------------------------

const received = [];
const router = defineRouter({
  files: {
    put: procedure({
      access: 'public',
      handler: async (_ctx, { name, body }) => {
        received.push({ name, body });
        return { name, size: body.length, kind: body.constructor.name, first: body[0] };
      },
    }),
    get: procedure({
      access: 'public',
      handler: async (_ctx, { size, fill }) => ({ body: bytes(size, fill), meta: { nested: [bytes(2, fill)] } }),
    }),
    plain: procedure({ access: 'public', handler: async (_ctx, { n }) => n * 2 }),
    notify: procedure({
      access: 'public',
      handler: async (ctx, { fill }) => void ctx.client.sendEvent('files/blob', { body: bytes(16, fill) }),
    }),
    shout: procedure({
      access: 'public',
      handler: async (ctx, { fill }) => ctx.server.to('room').emit('files/blob', { body: bytes(2048, fill) }),
    }),
    join: procedure({ access: 'public', handler: async (ctx) => void ctx.client.join('room') }),
    rest: procedure({
      access: 'public',
      http: { method: 'GET', path: '/blob' },
      handler: async () => ({ body: bytes(4, 1) }),
    }),
    on: {
      upload: procedure({ access: 'public', handler: async (_ctx, { body }) => void received.push({ inbound: body }) }),
    },
  },
});

test('ws: bytes in args and results travel as frames and arrive as Uint8Arrays, both ways', async (t) => {
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('files');
  received.length = 0;
  const answer = await client.api.files.put({ name: 'a', body: bytes(5000, 3) });
  assert.deepStrictEqual(answer, { name: 'a', size: 5000, kind: 'Uint8Array', first: 3 });
  assert.ok(received[0].body instanceof Uint8Array);
  const got = await client.api.files.get({ size: 3000, fill: 9 });
  assert.ok(got.body instanceof Uint8Array && got.body.length === 3000 && got.body[2999] === 9);
  assert.ok(got.meta.nested[0] instanceof Uint8Array);
  // A plain call on the same connection is still a text packet.
  assert.strictEqual(await client.api.files.plain({ n: 21 }), 42);
});

test('ws: the bytes a handler keeps are its own — the next frame does not overwrite them', async (t) => {
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('files');
  received.length = 0;
  // Two frames back to back: the engine hands over views into its socket
  // buffer, and the first value must survive the second read.
  await Promise.all([
    client.api.files.put({ name: 'first', body: bytes(70_000, 1) }),
    client.api.files.put({ name: 'second', body: bytes(70_000, 2) }),
  ]);
  const first = received.find((r) => r.name === 'first');
  const second = received.find((r) => r.name === 'second');
  assert.ok(
    first.body.every((b) => b === 1),
    'the first argument still holds its bytes',
  );
  assert.ok(second.body.every((b) => b === 2));
});

test('ws: events with bytes — to one client, to a room (one shared BINARY frame), and from the client', async (t) => {
  const { url } = await bootServer(t, { router });
  const a = await connectClient(t, url);
  const b = await connectClient(t, url);
  await a.load('files');
  await b.load('files');
  const seenA = [];
  const seenB = [];
  a.api.files.on('blob', (data) => seenA.push(data.body));
  b.api.files.on('blob', (data) => seenB.push(data.body));
  await a.api.files.notify({ fill: 4 });
  await waitFor(() => seenA.length === 1);
  assert.ok(seenA[0] instanceof Uint8Array && seenA[0][15] === 4);
  await a.api.files.join();
  await b.api.files.join();
  await a.api.files.shout({ fill: 6 });
  await waitFor(() => seenA.length === 2 && seenB.length === 1);
  assert.ok(seenB[0].length === 2048 && seenB[0][0] === 6);
  received.length = 0;
  a.sendEvent('files/upload', { body: bytes(10, 8) });
  await waitFor(() => received.length === 1);
  assert.ok(received[0].inbound instanceof Uint8Array && received[0].inbound[9] === 8);
});

test('ws: a batching client sends a call with bytes on its own frame, in order', async (t) => {
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url, { batch: true });
  await client.load('files');
  received.length = 0;
  const results = await Promise.all([
    client.api.files.plain({ n: 1 }),
    client.api.files.put({ name: 'b', body: bytes(8, 5) }),
    client.api.files.plain({ n: 2 }),
  ]);
  assert.deepStrictEqual(results, [2, { name: 'b', size: 8, kind: 'Uint8Array', first: 5 }, 4]);
});

test('http: a packet POST carries a frame up and a frame down; a batch too; REST refuses with 501', async (t) => {
  const { origin, port } = await bootServer(t, { router });
  const client = await connectClient(t, `http://127.0.0.1:${port}/api`);
  await client.load('files');
  const answer = await client.api.files.put({ name: 'h', body: bytes(300, 2) });
  assert.strictEqual(answer.kind, 'Uint8Array');
  const got = await client.api.files.get({ size: 100, fill: 1 });
  assert.ok(got.body instanceof Uint8Array && got.body.length === 100);
  const batched = await connectClient(t, `http://127.0.0.1:${port}/api`, { batch: true });
  await batched.load('files');
  const results = await Promise.all([batched.api.files.plain({ n: 3 }), batched.api.files.get({ size: 4, fill: 7 })]);
  assert.strictEqual(results[0], 6);
  assert.ok(results[1].body instanceof Uint8Array && results[1].body[3] === 7);
  const res = await fetch(`${origin}/api/blob`);
  assert.strictEqual(res.status, 501);
  assert.match((await res.json()).message, /codec\.rest/);
});

test('sse: refused explicitly — a TypeError on the client, a 501 for a result, a dropped event with a warning', async (t) => {
  const warnings = [];
  const logger = {
    log() {},
    info() {},
    debug() {},
    error() {},
    warn: (entry) => warnings.push(entry),
    child() {
      return logger;
    },
  };
  const { port } = await bootServer(t, { router, logger });
  const client = await connectClient(t, `http://127.0.0.1:${port}/api`, { transport: 'sse' });
  await client.load('files');
  await assert.rejects(client.api.files.put({ name: 's', body: bytes(4, 1) }), /binary attachments need a WebSocket/);
  await assert.rejects(client.api.files.get({ size: 4, fill: 1 }), (error) => error.code === 501);
  await client.api.files.notify({ fill: 1 });
  await waitFor(() => warnings.some((w) => w.event === 'sse.bytes'));
  assert.strictEqual(await client.api.files.plain({ n: 5 }), 10, 'the channel is fine');
});

test('attachments: false on both ends restores revision-1 JSON; a packet codec turns it off by itself', async (t) => {
  const { url } = await bootServer(t, { router, attachments: false });
  const client = await connectClient(t, url, { attachments: false });
  await client.load('files');
  received.length = 0;
  const answer = await client.api.files.put({ name: 'j', body: bytes(3, 1) });
  assert.strictEqual(answer.kind, 'Object', 'what JSON makes of a typed array');
  assert.strictEqual(received[0].body instanceof Uint8Array, false);
  const codec = { encode: (p) => JSON.stringify(p), decode: (t) => JSON.parse(t) };
  const coded = await bootServer(t, { router, codec });
  const codedClient = await connectClient(t, coded.url, { codec });
  await codedClient.load('files');
  assert.strictEqual((await codedClient.api.files.put({ name: 'c', body: bytes(3, 1) })).kind, 'Object');
});

test('port: attachPort carries frames both ways over a MessageChannel', async (t) => {
  const rpc = new RpcServer({ router, logger: false, sse: false });
  t.after(() => rpc.close());
  const { port1, port2 } = new MessageChannel();
  t.after(() => {
    port1.close();
    port2.close();
  });
  rpc.attachPort(port1);
  const replies = [];
  port2.on('message', (data) => replies.push(data));
  // Up: a call with bytes, posted as the frame a client would build.
  port2.postMessage(
    encodeAttachments({ type: 'call', id: 'p1', method: 'files/put', args: { name: 'p', body: bytes(6, 2) } }),
  );
  await waitFor(() => replies.length === 1);
  assert.deepStrictEqual(JSON.parse(replies[0]), {
    type: 'callback',
    id: 'p1',
    result: { name: 'p', size: 6, kind: 'Uint8Array', first: 2 },
  });
  // Down: a result with bytes arrives as a frame on the port.
  port2.postMessage(JSON.stringify({ type: 'call', id: 'p2', method: 'files/get', args: { size: 12, fill: 3 } }));
  await waitFor(() => replies.length === 2);
  assert.ok(replies[1] instanceof Uint8Array && isAttachmentsFrame(replies[1]));
  const got = decodeAttachments(replies[1]);
  assert.strictEqual(got.id, 'p2');
  assert.ok(got.result.body instanceof Uint8Array && got.result.body[11] === 3);
});

test('the option is validated only as a boolean switch, and Server forwards it', async (t) => {
  const server = new Server({
    router,
    protocol: 'http',
    host: '127.0.0.1',
    port: 0,
    logger: false,
    attachments: false,
  });
  await server.listen();
  t.after(() => server.close());
  const client = await connectClient(t, `ws://127.0.0.1:${server.address().port}/api`, { attachments: false });
  await client.load('files');
  assert.strictEqual((await client.api.files.put({ name: 'z', body: bytes(2, 1) })).kind, 'Object');
});
