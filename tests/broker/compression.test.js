'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const zlib = require('node:zlib');

const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure, WrpcClient, tracked } = require('../../index.js');
const { MemoryBroker, attachBrokerRpc } = require('../../broker.js');
const { HEADER_ENC, HEADER_KIND, KIND } = require('../../src/broker/rpc/frames.js');
const { quiet, waitFor } = require('./support.js');

const DEFLATE = 'deflate-raw';

const router = defineRouter({
  calc: {
    add: procedure({ access: 'public', handler: async (_ctx, { a, b }) => a + b }),
    big: procedure({
      access: 'public',
      handler: async (_ctx, { rows }) => Array.from({ length: rows }, (_, i) => ({ i, name: `row-${i}` })),
    }),
    echo: procedure({ access: 'public', handler: async (_ctx, args) => args }),
    notify: procedure({
      access: 'public',
      handler: async (ctx, { rows }) => void ctx.client.sendEvent('calc/note', { rows: Array(rows).fill('x') }),
    }),
    ticks: procedure.subscription({
      access: 'public',
      handler: async function* (_ctx, { count }) {
        for (let i = 0; i < count; i++) yield tracked(String(i), { i, pad: 'x'.repeat(2000) });
      },
    }),
    upload: procedure({
      access: 'public',
      handler: async (ctx, { stream }) => {
        let bytes = 0;
        for await (const chunk of ctx.client.getStream(stream)) bytes += chunk.length;
        return bytes;
      },
    }),
  },
});

// Every `direct.send` through the broker, with its headers and body size —
// the wire, since neither transport is reachable through a client. The
// capability object is frozen, so the spy is a broker-shaped copy.
const spied = (broker) => {
  const sent = [];
  const send = broker.direct.send;
  const direct = {
    ...broker.direct,
    send: (address, body, options) => {
      sent.push({ address, size: body.length, headers: options?.headers ?? {} });
      return send(address, body, options);
    },
  };
  return { sent, broker: { name: broker.name, direct, close: () => broker.close() } };
};

const boot = async (t, serverOptions = {}) => {
  const spy = spied(new MemoryBroker({ logger: quiet }));
  const { broker, sent } = spy;
  const rpc = new RpcServer({ router, logger: false, sse: false });
  const handle = await attachBrokerRpc(rpc, broker, { service: 'calc', logger: quiet, ...serverOptions });
  t.after(async () => {
    await handle.stop();
    await rpc.close();
    broker.close();
  });
  return { broker, rpc, handle, sent };
};

const connect = (t, broker, options = {}) =>
  WrpcClient.connect('broker://calc', {
    transport: 'broker',
    broker,
    heartbeat: false,
    reconnect: false,
    logger: false,
    ...options,
  }).then((client) => {
    t.after(() => void client.close());
    return client;
  });

const encodedFrames = (sent) => sent.filter((m) => m.headers[HEADER_ENC] !== undefined);

test('broker compression: off by default — no header anywhere, bodies as they are', async (t) => {
  const { broker, sent } = await boot(t);
  const client = await connect(t, broker, { mode: 'session' });
  await client.load('calc');
  const rows = await client.api.calc.big({ rows: 300 });
  assert.strictEqual(rows.length, 300);
  assert.strictEqual(encodedFrames(sent).length, 0);
  const stateless = await connect(t, broker);
  await stateless.load('calc');
  assert.strictEqual((await stateless.api.calc.big({ rows: 300 })).length, 300);
  assert.strictEqual(encodedFrames(sent).length, 0);
});

test('broker compression (session): negotiated on hello/welcome, applied past the threshold both ways', async (t) => {
  const { broker, sent } = await boot(t, { compression: true });
  const client = await connect(t, broker, { mode: 'session', compression: true });
  const hello = sent.find((m) => m.headers[HEADER_KIND] === KIND.HELLO);
  const welcome = sent.find((m) => m.headers[HEADER_KIND] === KIND.WELCOME);
  assert.strictEqual(hello.headers[HEADER_ENC], DEFLATE, 'the client names its codec');
  assert.strictEqual(welcome.headers[HEADER_ENC], DEFLATE, 'the server agrees');

  await t.test('a large answer comes back compressed, a small one plain', async () => {
    await client.load('calc');
    sent.length = 0;
    const rows = await client.api.calc.big({ rows: 400 });
    assert.strictEqual(rows.length, 400);
    const answer = sent.find((m) => m.headers[HEADER_KIND] === KIND.PACKET && m.headers[HEADER_ENC]);
    assert.ok(answer, 'the callback frame carried the header');
    assert.ok(answer.size < 2000, `${answer.size} B on the wire for a ~9 KB answer`);
    sent.length = 0;
    assert.strictEqual(await client.api.calc.add({ a: 1, b: 2 }), 3);
    assert.strictEqual(encodedFrames(sent).length, 0, 'under the threshold, plain');
  });

  await t.test('a large argument goes up compressed', async () => {
    sent.length = 0;
    const echoed = await client.api.calc.echo({ text: 'y'.repeat(20_000) });
    assert.strictEqual(echoed.text.length, 20_000);
    const up = sent.find((m) => m.headers[HEADER_KIND] === KIND.PACKET && m.headers[HEADER_ENC]);
    assert.ok(up && up.size < 1000, `${up?.size} B up for a 20 KB argument`);
  });

  await t.test('events, subscriptions and stream chunks ride the same session', async () => {
    const notes = [];
    client.api.calc.on('note', (data) => notes.push(data));
    await client.api.calc.notify({ rows: 3000 });
    await waitFor(() => notes.length === 1);
    assert.strictEqual(notes[0].rows.length, 3000);
    const seen = [];
    client.api.calc.ticks.subscribe({ count: 4 }, { onData: (v) => seen.push(v.i) });
    await waitFor(() => seen.length === 4);
    assert.deepStrictEqual(seen, [0, 1, 2, 3]);
    sent.length = 0;
    const up = client.createStream('blob', 30_000);
    const call = client.api.calc.upload({ stream: up.id });
    up.write(new Uint8Array(30_000).fill(7));
    up.end();
    assert.strictEqual(await call, 30_000);
    const chunk = sent.find((m) => m.headers[HEADER_KIND] === KIND.CHUNK);
    assert.ok(chunk && chunk.headers[HEADER_ENC] === DEFLATE && chunk.size < 500, `chunk ${chunk?.size} B`);
  });
});

test('broker compression (stateless): the request names the codec, only the answer is compressed', async (t) => {
  const { broker, sent } = await boot(t, { compression: true });
  const client = await connect(t, broker, { compression: true });
  await client.load('calc');
  sent.length = 0;
  const echoed = await client.api.calc.echo({ text: 'y'.repeat(20_000) });
  assert.strictEqual(echoed.text.length, 20_000);
  const request = sent.find((m) => m.headers[HEADER_KIND] === KIND.REQUEST);
  const response = sent.find((m) => m.headers[HEADER_KIND] === KIND.RESPONSE);
  assert.strictEqual(request.headers[HEADER_ENC], DEFLATE, 'accepts');
  assert.ok(request.size > 20_000, 'the request itself travels plain');
  assert.strictEqual(response.headers[HEADER_ENC], DEFLATE);
  assert.ok(response.size < 1000, `${response.size} B answer`);
});

test('broker compression: one side on, the other off — plain, and everything still answers', async (t) => {
  const { broker, sent } = await boot(t, { compression: true });
  const client = await connect(t, broker, { mode: 'session' });
  await client.load('calc');
  assert.strictEqual((await client.api.calc.big({ rows: 400 })).length, 400);
  assert.strictEqual(encodedFrames(sent).length, 0);
  const other = await boot(t);
  const eager = await connect(t, other.broker, { mode: 'session', compression: true });
  await eager.load('calc');
  assert.strictEqual((await eager.api.calc.big({ rows: 400 })).length, 400);
  const welcome = other.sent.find((m) => m.headers[HEADER_KIND] === KIND.WELCOME);
  assert.strictEqual(welcome.headers[HEADER_ENC], undefined, 'the server agreed to nothing');
  assert.strictEqual(encodedFrames(other.sent).filter((m) => m.headers[HEADER_KIND] !== KIND.HELLO).length, 0);
});

test('broker compression: an undecodable frame ends the session like a sequence gap', async (t) => {
  const { broker, rpc, handle } = await boot(t, { compression: true, maxMessage: 4096 });
  const client = await connect(t, broker, { mode: 'session', compression: true });
  await client.load('calc');
  assert.strictEqual(handle.sessions, 1);
  // A frame the server cannot inflate under its cap: 200 KB of one byte.
  const closed = new Promise((resolve) => client.once('close', resolve));
  await assert.rejects(client.api.calc.echo({ text: 'z'.repeat(200_000) }));
  await closed;
  await waitFor(() => handle.sessions === 0);
  assert.ok(rpc);
});

test('broker compression: the option is validated, and an asynchronous codec is refused here', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const rpc = new RpcServer({ router, logger: false, sse: false });
  t.after(() => rpc.close());
  await assert.rejects(
    attachBrokerRpc(rpc, broker, { service: 'x', compression: 'lz4' }),
    /compression must be true, false or an options object/,
  );
  const asyncCodec = { id: DEFLATE, encode: async (b) => b, decode: async (b) => b };
  await assert.rejects(
    attachBrokerRpc(rpc, broker, { service: 'x', compression: { codec: asyncCodec } }),
    /must answer synchronously/,
  );
  await assert.rejects(attachBrokerRpc(rpc, broker, { service: 'x', maxMessage: 0 }), /maxMessage/);
  await assert.rejects(
    WrpcClient.connect('broker://x', { transport: 'broker', broker, compression: { threshold: -1 }, logger: false }),
    /threshold/,
  );
  // A synchronous injected codec works, and it is the one named on the wire.
  const mine = {
    id: 'mine',
    threshold: 16,
    encode: (b) => zlib.deflateRawSync(b),
    decode: (b, max) => zlib.inflateRawSync(b, { maxOutputLength: max }),
  };
  const { sent, broker: spiedBroker } = spied(broker);
  const handle = await attachBrokerRpc(rpc, spiedBroker, { service: 'x', compression: { codec: mine }, logger: quiet });
  t.after(() => handle.stop());
  const client = await WrpcClient.connect('broker://x', {
    transport: 'broker',
    broker: spiedBroker,
    mode: 'session',
    compression: { codec: mine },
    heartbeat: false,
    reconnect: false,
    logger: false,
  });
  t.after(() => void client.close());
  await client.load('calc');
  assert.strictEqual((await client.api.calc.big({ rows: 100 })).length, 100);
  assert.ok(sent.some((m) => m.headers[HEADER_ENC] === 'mine'));
});
