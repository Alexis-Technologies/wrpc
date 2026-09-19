'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const zlib = require('node:zlib');
const timers = require('node:timers/promises');

const { defineRouter, procedure, tracked } = require('../../index.js');
const { attachSession, acceptSessions } = require('../../wt.js');
const { WtSocket } = require('../../src/webtransport/socket.js');
const { ClientWtTransport } = require('../../src/client/webtransport.js');
const {
  StreamParser,
  frame,
  frameText,
  frameCaps,
  KIND_TEXT,
  KIND_CAPS,
  KIND_TEXT_DEFLATE,
  KIND_BINARY_DEFLATE,
} = require('../../src/webtransport/framing.js');
const { chunkEncode } = require('../../src/chunks.js');
const { createFakeWt } = require('./fakeWebTransport.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const DEFLATE = 'deflate-raw';
const big = JSON.stringify({
  type: 'event',
  name: 'x',
  data: { rows: Array.from({ length: 120 }, (_, i) => ({ i, name: `row-${i}`, tags: ['a', 'b'] })) },
});
const small = JSON.stringify({ type: 'ping' });
const encoder = new TextEncoder();

// A codec that answers later, so order across compressed and plain
// messages is actually exercised — a CompressionStream's shape.
const slowCodec = (delay = 5) => ({
  id: DEFLATE,
  threshold: 64,
  encode: async (bytes) => {
    await timers.setTimeout(delay);
    return zlib.deflateRawSync(bytes);
  },
  decode: async (bytes, max) => {
    await timers.setTimeout(delay);
    return zlib.inflateRawSync(bytes, { maxOutputLength: max });
  },
});

// The peer end of a server socket, by hand: a parser over what the socket
// sends (caps kept apart), and a writer to talk to it.
const serverPair = async (t, options = {}) => {
  const world = createFakeWt();
  const client = new world.WebTransport('https://h/api');
  await client.ready;
  const session = await world.next();
  const stream = await client.createBidirectionalStream();
  const reader = session.incomingBidirectionalStreams.getReader();
  const { value: control } = await reader.read();
  reader.releaseLock();
  const socket = new WtSocket(session, control, options);
  t.after(() => socket.terminate());
  const received = [];
  const caps = [];
  const parser = new StreamParser({
    onMessage: (kind, data) => {
      if (kind === KIND_CAPS) caps.push(JSON.parse(data));
      else received.push({ kind, data });
    },
  });
  parser.deflate = true;
  void (async () => {
    const r = stream.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await r.read();
        if (done) return;
        parser.push(value);
      }
    } catch {
      // closed under the read
    }
  })();
  const writer = stream.writable.getWriter();
  const messages = [];
  socket.on('message', (data, isBinary) => messages.push({ data, isBinary }));
  return { world, client, session, socket, received, caps, writer, messages };
};

test('wt compression: off by default — nothing announced, a compressed kind from the peer is a 1002', async (t) => {
  const pair = await serverPair(t);
  await waitFor(() => pair.caps.length === 1);
  assert.strictEqual(pair.caps[0].deflate, undefined);
  const closed = new Promise((resolve) => pair.socket.once('close', (code) => resolve(code)));
  await pair.writer.write(frameCaps(JSON.stringify({ deflate: DEFLATE })));
  await pair.writer.write(frame(KIND_TEXT_DEFLATE, zlib.deflateRawSync(encoder.encode(big))));
  assert.strictEqual(await closed, 1002);
});

test('wt compression: announced, negotiated, and applied past the threshold in both directions', async (t) => {
  const pair = await serverPair(t, { compression: true });
  await waitFor(() => pair.caps.length === 1);
  assert.strictEqual(pair.caps[0].deflate, DEFLATE);
  assert.strictEqual(pair.socket.compression, null, 'not until the peer named it');

  await t.test('before the peer announced, everything goes plain', async () => {
    pair.socket.send(big);
    await waitFor(() => pair.received.length === 1);
    assert.strictEqual(pair.received[0].kind, KIND_TEXT);
    pair.received.length = 0;
  });

  await pair.writer.write(frameCaps(JSON.stringify({ streams: false, deflate: DEFLATE })));
  await waitFor(() => pair.socket.compression === DEFLATE);

  await t.test('a large packet leaves as KIND 3 and reads back; a small one stays KIND 0', async () => {
    pair.socket.send(big);
    pair.socket.send(small);
    await waitFor(() => pair.received.length === 2);
    assert.strictEqual(pair.received[0].kind, KIND_TEXT_DEFLATE);
    assert.ok(pair.received[0].data.length < big.length / 4, `${big.length} -> ${pair.received[0].data.length}`);
    assert.strictEqual(zlib.inflateRawSync(pair.received[0].data).toString(), big);
    assert.deepStrictEqual(pair.received[1], { kind: KIND_TEXT, data: small });
    pair.received.length = 0;
  });

  await t.test('a large chunk leaves as KIND 4 (the mux is off, the peer announced no streams)', async () => {
    const chunk = chunkEncode('s1', new Uint8Array(4096).fill(7));
    pair.socket.send(chunk);
    await waitFor(() => pair.received.length === 1);
    assert.strictEqual(pair.received[0].kind, KIND_BINARY_DEFLATE);
    assert.deepStrictEqual(new Uint8Array(zlib.inflateRawSync(pair.received[0].data)), chunk);
    pair.received.length = 0;
  });

  await t.test('compress: false on the message sends it plain whatever was negotiated', async () => {
    pair.socket.send(big, { compress: false });
    await waitFor(() => pair.received.length === 1);
    assert.strictEqual(pair.received[0].kind, KIND_TEXT);
    pair.received.length = 0;
  });

  await t.test('inbound KIND 3 and KIND 4 are inflated before delivery', async () => {
    await pair.writer.write(frame(KIND_TEXT_DEFLATE, zlib.deflateRawSync(encoder.encode(big))));
    const chunk = chunkEncode('s2', new Uint8Array(2048).fill(9));
    await pair.writer.write(frame(KIND_BINARY_DEFLATE, zlib.deflateRawSync(chunk)));
    await pair.writer.write(frameText(small));
    await waitFor(() => pair.messages.length === 3);
    assert.deepStrictEqual(pair.messages[0], { data: big, isBinary: false });
    assert.strictEqual(pair.messages[1].isBinary, true);
    assert.deepStrictEqual(new Uint8Array(pair.messages[1].data), chunk);
    assert.deepStrictEqual(pair.messages[2], { data: small, isBinary: false });
    pair.messages.length = 0;
  });

  await t.test('a packet that inflates past maxMessage is a 1002', async () => {
    const closed = new Promise((resolve) => pair.socket.once('close', (code) => resolve(code)));
    const bomb = zlib.deflateRawSync(Buffer.alloc(20 * 1024 * 1024, 0x20));
    await pair.writer.write(frame(KIND_TEXT_DEFLATE, bomb));
    assert.strictEqual(await closed, 1002);
  });
});

test('wt compression: a peer naming another codec, or none, keeps the wire plain', async (t) => {
  const pair = await serverPair(t, { compression: true });
  await pair.writer.write(frameCaps(JSON.stringify({ deflate: 'brotli' })));
  await timers.setTimeout(10);
  assert.strictEqual(pair.socket.compression, null);
  pair.socket.send(big);
  await waitFor(() => pair.received.length === 1);
  assert.strictEqual(pair.received[0].kind, KIND_TEXT);
  // And a compressed kind from such a peer is still a violation.
  const closed = new Promise((resolve) => pair.socket.once('close', (code) => resolve(code)));
  await pair.writer.write(frame(KIND_TEXT_DEFLATE, zlib.deflateRawSync(encoder.encode(big))));
  assert.strictEqual(await closed, 1002);
});

test('wt compression: an asynchronous codec keeps the wire in order, both ways', async (t) => {
  const pair = await serverPair(t, { compression: { codec: slowCodec(4) } });
  await pair.writer.write(frameCaps(JSON.stringify({ deflate: DEFLATE })));
  await waitFor(() => pair.socket.compression === DEFLATE);
  // Outbound: big (slow), small (would overtake), big, small, and a stream
  // control packet through the mux path — all in send order.
  const order = [];
  for (let i = 0; i < 4; i++) {
    const text = i % 2 === 0 ? big.replace('"i":0', `"i":${100 + i}`) : small.replace('ping', `p${i}`);
    order.push(text);
    pair.socket.send(text);
  }
  await waitFor(() => pair.received.length === 4);
  const sent = pair.received.map((m) =>
    m.kind === KIND_TEXT_DEFLATE ? zlib.inflateRawSync(m.data).toString() : m.data,
  );
  assert.deepStrictEqual(sent, order);
  assert.deepStrictEqual(
    pair.received.map((m) => m.kind),
    [KIND_TEXT_DEFLATE, KIND_TEXT, KIND_TEXT_DEFLATE, KIND_TEXT],
  );
  // Inbound: compressed, plain, compressed, plain — delivered in wire order.
  const inbound = ['a', 'b', 'c', 'd'].map((k) => big.replace('"i":0', `"${k}":0`));
  await pair.writer.write(frame(KIND_TEXT_DEFLATE, zlib.deflateRawSync(encoder.encode(inbound[0]))));
  await pair.writer.write(frameText(inbound[1]));
  await pair.writer.write(frame(KIND_TEXT_DEFLATE, zlib.deflateRawSync(encoder.encode(inbound[2]))));
  await pair.writer.write(frameText(inbound[3]));
  await waitFor(() => pair.messages.length === 4);
  assert.deepStrictEqual(
    pair.messages.map((m) => m.data),
    inbound,
  );
});

test('wt compression: a codec that fails or does not shrink the message sends it plain', async (t) => {
  let calls = 0;
  const codec = {
    id: DEFLATE,
    threshold: 16,
    encode: (bytes) => {
      calls++;
      if (calls === 1) throw new Error('encoder broke');
      if (calls === 2) return Promise.reject(new Error('encoder broke later'));
      return bytes; // "compressed" to the same size: not worth the kind
    },
    decode: (bytes) => bytes,
  };
  const pair = await serverPair(t, { compression: { codec } });
  await pair.writer.write(frameCaps(JSON.stringify({ deflate: DEFLATE })));
  await waitFor(() => pair.socket.compression === DEFLATE);
  pair.socket.send(big);
  pair.socket.send(big);
  pair.socket.send(big);
  await waitFor(() => pair.received.length === 3);
  assert.deepStrictEqual(
    pair.received.map((m) => m.kind),
    [KIND_TEXT, KIND_TEXT, KIND_TEXT],
  );
  assert.strictEqual(calls, 3);
});

test('wt compression: the option is validated at construction', async (t) => {
  const world = createFakeWt();
  const client = new world.WebTransport('https://h/api');
  await client.ready;
  const session = await world.next();
  await client.createBidirectionalStream();
  const reader = session.incomingBidirectionalStreams.getReader();
  const { value: control } = await reader.read();
  reader.releaseLock();
  t.after(() => client.close());
  assert.throws(() => new WtSocket(session, control, { compression: 'yes' }), /compression must be true, false/);
  assert.throws(() => new WtSocket(session, control, { compression: { codec: {} } }), /compression\.codec/);
});

// --- the client transport --------------------------------------------

const ENDPOINT = 'https://127.0.0.1:4433/api';

// The server side of a control stream by hand, deflate accepted.
const controlStream = async (session) => {
  const reader = session.incomingBidirectionalStreams.getReader();
  const { value: stream } = await reader.read();
  reader.releaseLock();
  const received = [];
  const caps = [];
  const parser = new StreamParser({
    onMessage: (kind, data) => {
      if (kind === KIND_CAPS) caps.push(JSON.parse(data));
      else received.push({ kind, data });
    },
  });
  parser.deflate = true;
  void (async () => {
    const streamReader = stream.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await streamReader.read();
        if (done) return;
        parser.push(value);
      }
    } catch {
      // The session closed under the read.
    }
  })();
  return { received, caps, writer: stream.writable.getWriter() };
};

test('wt compression (client): the wt bag or connect() names it; negotiated against the server caps', async (t) => {
  const world = createFakeWt();
  const transport = new ClientWtTransport(ENDPOINT, { WebTransport: world.WebTransport, compression: true });
  t.after(() => transport.close());
  const messages = [];
  transport.on('message', (data) => messages.push(data));
  await transport.open();
  const session = await world.next();
  const server = await controlStream(session);
  await waitFor(() => server.caps.length === 1);
  assert.deepStrictEqual(server.caps[0], { streams: true, deflate: DEFLATE });
  assert.strictEqual(transport.compression, null);
  await server.writer.write(frameCaps(JSON.stringify({ streams: true, deflate: DEFLATE })));
  await waitFor(() => transport.compression === DEFLATE);
  transport.write(big);
  transport.write(small);
  await waitFor(() => server.received.length === 2);
  assert.strictEqual(server.received[0].kind, KIND_TEXT_DEFLATE);
  assert.strictEqual(zlib.inflateRawSync(server.received[0].data).toString(), big);
  assert.strictEqual(server.received[1].kind, KIND_TEXT);
  await server.writer.write(frame(KIND_TEXT_DEFLATE, zlib.deflateRawSync(encoder.encode(big))));
  await waitFor(() => messages.length === 1);
  assert.strictEqual(messages[0], big);
  // A chunk of a stream the mux never opened (no `stream` packet preceded
  // it) falls back to the control stream — and is compressed there.
  const chunk = chunkEncode('up', new Uint8Array(4096));
  transport.write(chunk);
  await waitFor(() => server.received.length === 3);
  assert.strictEqual(server.received[2].kind, KIND_BINARY_DEFLATE);
  assert.deepStrictEqual(new Uint8Array(zlib.inflateRawSync(server.received[2].data)), chunk);
});

test('wt compression (client): connect()-level `compression` reaches the transport; a bad value throws at open', async (t) => {
  const world = createFakeWt();
  const transport = new ClientWtTransport(ENDPOINT, { WebTransport: world.WebTransport });
  t.after(() => transport.close());
  await assert.rejects(transport.open({ compression: 'zstd' }), /compression must be true, false/);
  await transport.open({ compression: { threshold: 32 } });
  const session = await world.next();
  const server = await controlStream(session);
  await waitFor(() => server.caps.length === 1);
  assert.strictEqual(server.caps[0].deflate, DEFLATE);
});

test('wt compression (client): a server that inflates past maxMessage, or sends a compressed kind unannounced, is hung up', async (t) => {
  const world = createFakeWt();
  const transport = new ClientWtTransport(ENDPOINT, {
    WebTransport: world.WebTransport,
    compression: true,
    maxMessage: 4096,
  });
  t.after(() => transport.close());
  const errors = [];
  transport.on('error', (error) => errors.push(error));
  await transport.open();
  const session = await world.next();
  const server = await controlStream(session);
  await server.writer.write(frameCaps(JSON.stringify({ deflate: DEFLATE })));
  await waitFor(() => transport.compression === DEFLATE);
  const closed = new Promise((resolve) => transport.once('close', resolve));
  await server.writer.write(frame(KIND_TEXT_DEFLATE, zlib.deflateRawSync(Buffer.alloc(64 * 1024, 0x20))));
  await closed;
  assert.strictEqual(transport.active, false);
  assert.ok(errors.length >= 1);
});

// --- end to end ------------------------------------------------------

// The fake's WebTransport with the control stream's writer spied on: the
// KIND byte of every frame the client sends, since neither side's
// transport is reachable through a WrpcClient or a server Client.
const spied = (world) => {
  const kinds = [];
  class SpiedWebTransport extends world.WebTransport {
    async createBidirectionalStream() {
      const stream = await super.createBidirectionalStream();
      return {
        readable: stream.readable,
        writable: {
          getWriter: () => {
            const writer = stream.writable.getWriter();
            return {
              write: (bytes) => {
                kinds.push(bytes[4]);
                return writer.write(bytes);
              },
              close: () => writer.close(),
              abort: (reason) => writer.abort(reason),
              releaseLock: () => writer.releaseLock(),
            };
          },
        },
      };
    }
  }
  return { WebTransport: SpiedWebTransport, kinds };
};

const router = () =>
  defineRouter({
    data: {
      big: procedure({
        access: 'public',
        handler: async (_ctx, { rows }) => Array.from({ length: rows }, (_, i) => ({ i, name: `row-${i}` })),
      }),
      echo: procedure({ access: 'public', handler: async (_ctx, args) => args }),
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

test('wt compression (e2e): a WrpcClient and an attached session, both on, do everything they did plain', async (t) => {
  const { server, url } = await bootServer(t, { router: router() });
  const world = createFakeWt();
  const acceptor = acceptSessions(server, world.sessions, {
    compression: true,
    onError: (error) => t.diagnostic(String(error)),
  });
  t.after(() => acceptor.stop());
  const spy = spied(world);
  const client = await connectClient(t, url, {
    transport: 'wt',
    wt: { WebTransport: spy.WebTransport },
    compression: true,
  });
  await client.load('data');
  const api = client.api.data;
  const rows = await api.big({ rows: 500 });
  assert.strictEqual(rows.length, 500);
  assert.strictEqual(rows[499].name, 'row-499');
  // A large argument goes UP compressed too: the frame left as KIND 3.
  const before = spy.kinds.length;
  const echoed = await api.echo({ text: 'y'.repeat(50_000) });
  assert.strictEqual(echoed.text.length, 50_000);
  assert.ok(spy.kinds.slice(before).includes(KIND_TEXT_DEFLATE), `kinds after: ${spy.kinds.slice(before)}`);
  const seen = [];
  api.ticks.subscribe({ count: 5 }, { onData: (v) => seen.push(v.i) });
  await waitFor(() => seen.length === 5);
  assert.deepStrictEqual(seen, [0, 1, 2, 3, 4]);
  const up = client.createStream('blob', 10_000);
  const call = api.upload({ stream: up.id });
  up.write(new Uint8Array(10_000).fill(1));
  up.end();
  assert.strictEqual(await call, 10_000);
});

test('wt compression (e2e): one side on, the other off — plain, and nothing breaks', async (t) => {
  const { server, url } = await bootServer(t, { router: router() });
  const world = createFakeWt();
  const acceptor = acceptSessions(server, world.sessions, { onError: (error) => t.diagnostic(String(error)) });
  t.after(() => acceptor.stop());
  const spy = spied(world);
  const client = await connectClient(t, url, {
    transport: 'wt',
    wt: { WebTransport: spy.WebTransport },
    compression: true,
  });
  await client.load('data');
  const rows = await client.api.data.big({ rows: 300 });
  assert.strictEqual(rows.length, 300);
  const echoed = await client.api.data.echo({ text: 'y'.repeat(50_000) });
  assert.strictEqual(echoed.text.length, 50_000);
  assert.ok(!spy.kinds.includes(KIND_TEXT_DEFLATE), 'the server named no codec, so nothing left compressed');
});

test('wt compression (e2e): attachSession takes the option too', async (t) => {
  const { server, url } = await bootServer(t, { router: router() });
  const world = createFakeWt();
  const pending = (async () => {
    const session = await world.next();
    return attachSession(server, session, { compression: true });
  })();
  const spy = spied(world);
  const client = await connectClient(t, url, {
    transport: 'wt',
    wt: { WebTransport: spy.WebTransport },
    compression: true,
  });
  const attachedClient = await pending;
  assert.ok(attachedClient);
  await client.load('data');
  assert.strictEqual((await client.api.data.big({ rows: 200 })).length, 200);
  const echoed = await client.api.data.echo({ text: 'y'.repeat(50_000) });
  assert.strictEqual(echoed.text.length, 50_000);
  assert.ok(spy.kinds.includes(KIND_TEXT_DEFLATE));
});
