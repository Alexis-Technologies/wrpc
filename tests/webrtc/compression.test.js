'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const zlib = require('node:zlib');
const timers = require('node:timers/promises');

const { WrpcClient } = require('../../src/client/core.js');
const { defineRouter, procedure, RpcServer } = require('../../index.js');
const { ClientRtcTransport, RtcPeerTransport } = require('../../src/webrtc/transport.js');
const { RtcLink } = require('../../src/webrtc/link.js');
const { WrpcPeer } = require('../../src/webrtc/peer.js');
const { attachChannel } = require('../../src/webrtc/index.js');
const {
  FrameEncoder,
  FrameDecoder,
  FramingError,
  KIND_TEXT,
  FLAG_FIN,
  FLAG_DEFLATE,
} = require('../../src/webrtc/framing.js');
const { chunkEncode } = require('../../src/chunks.js');
const { createFakeRtc } = require('./fakeRtc.js');
const { FakeSignalHub } = require('./fakeSignalHub.js');
const { rawChannelPair } = require('./rawChannel.js');
const { waitFor, within } = require('./portContract.js');

const DEFLATE = 'deflate-raw';
const quiet = {
  log() {},
  info() {},
  warn() {},
  error() {},
  debug() {},
  child() {
    return this;
  },
};

const big = JSON.stringify({
  type: 'event',
  name: 'x',
  data: { rows: Array.from({ length: 120 }, (_, i) => ({ i, name: `row-${i}`, tags: ['a', 'b'] })) },
});
const small = JSON.stringify({ type: 'ping' });

// A codec that answers later — a CompressionStream's shape — so order
// across compressed and plain messages is actually exercised.
const slowCodec = (delay = 4) => ({
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

// The header byte of every frame a channel sends, without touching the
// channel's shape (the structural check still sees a data channel).
const spy = (channel) => {
  const headers = [];
  const send = channel.send.bind(channel);
  channel.send = (frame) => {
    headers.push(frame[0]);
    return send(frame);
  };
  return headers;
};

// ---- framing ---------------------------------------------------------

test('rtc framing: bit 2 is the deflate flag — reserved until the decoder is told', () => {
  const encoder16 = new FrameEncoder(16 * 1024);
  const frames = [];
  encoder16.encode(KIND_TEXT | FLAG_DEFLATE, new Uint8Array(40_000), (frame) => frames.push(frame.slice()));
  assert.strictEqual(frames.length, 3);
  for (const frame of frames) assert.ok(frame[0] & FLAG_DEFLATE, 'the flag rides every fragment');
  assert.strictEqual(frames[2][0] & FLAG_FIN, FLAG_FIN);

  const strict = new FrameDecoder();
  assert.throws(
    () => strict.push(frames[0]),
    (error) => error instanceof FramingError && error.code === 'reserved',
  );

  const lenient = new FrameDecoder();
  lenient.deflate = true;
  assert.strictEqual(lenient.push(frames[0]), null);
  assert.strictEqual(lenient.push(frames[1]), null);
  const message = lenient.push(frames[2]);
  assert.deepStrictEqual(
    { kind: message.kind, deflated: message.deflated, length: message.data.length },
    {
      kind: KIND_TEXT,
      deflated: true,
      length: 40_000,
    },
  );
  assert.ok(message.data instanceof Uint8Array, 'a deflated packet is bytes until inflated');

  // A continuation that drops the flag is a mismatch, like a changed kind.
  lenient.push(frames[0]);
  const plain = frames[1].slice();
  plain[0] &= ~FLAG_DEFLATE;
  assert.throws(
    () => lenient.push(plain),
    (error) => error.code === 'kind',
  );

  // A plain single-fragment message still decodes as text, deflated: false.
  const single = new FrameEncoder(1024);
  let text;
  single.encodeText(small, (frame) => {
    text = lenient.push(frame);
  });
  assert.deepStrictEqual(text, { kind: KIND_TEXT, data: small, deflated: false });
  // Bits 3-7 stay reserved even with deflate on.
  assert.throws(
    () => lenient.push(new Uint8Array([0b1000 | FLAG_FIN, 1])),
    (error) => error.code === 'reserved',
  );
});

// ---- transports over a raw pair -------------------------------------

test('rtc compression (raw channel): off by default; on when both ends say so; the wire shows it', async (t) => {
  const pair = await rawChannelPair(t, { fake: { maxMessageSize: 4096 } });
  const sentByClient = spy(pair.a);
  const client = new ClientRtcTransport('webrtc:test', { channel: pair.a, maxMessageSize: 4096 });
  const host = new RtcPeerTransport(pair.b, { peer: 'a', maxMessageSize: 4096 });
  t.after(() => client.close());
  const packets = [];
  host.on('packet', (text) => packets.push(text));
  await client.open();
  assert.strictEqual(client.compression, null);
  assert.strictEqual(host.compression, null);
  client.write(big);
  await waitFor(() => packets.length === 1);
  assert.strictEqual(packets[0], big);
  assert.ok(
    sentByClient.every((h) => (h & FLAG_DEFLATE) === 0),
    'nothing compressed by default',
  );
});

test('rtc compression (raw channel): explicit on both sides — packets and chunks past the threshold, both ways', async (t) => {
  const pair = await rawChannelPair(t, { fake: { maxMessageSize: 4096 } });
  const sentByClient = spy(pair.a);
  const sentByHost = spy(pair.b);
  const client = new ClientRtcTransport('webrtc:test', { channel: pair.a, compression: true, maxMessageSize: 4096 });
  const host = new RtcPeerTransport(pair.b, { peer: 'a', compression: true, maxMessageSize: 4096 });
  t.after(() => client.close());
  const packets = [];
  const chunks = [];
  const inbound = [];
  host.on('packet', (text) => packets.push(text));
  host.on('chunk', (bytes) => chunks.push(bytes));
  client.on('message', (data) => inbound.push(data));
  await client.open();
  assert.strictEqual(client.compression, DEFLATE);
  assert.strictEqual(host.compression, DEFLATE);

  await t.test('client → host: a large packet compressed, a small one plain, order kept', async () => {
    client.write(big);
    client.write(small);
    await waitFor(() => packets.length === 2);
    assert.deepStrictEqual(packets, [big, small]);
    const flags = sentByClient.map((h) => h & FLAG_DEFLATE);
    assert.deepStrictEqual(flags, [FLAG_DEFLATE, 0]);
    sentByClient.length = 0;
  });

  await t.test('client → host: a chunk past the threshold, fragmented after compression', async () => {
    const chunk = chunkEncode(
      'up',
      new Uint8Array(50_000).map((_, i) => i % 7),
    );
    client.write(chunk);
    await waitFor(() => chunks.length === 1);
    assert.deepStrictEqual(new Uint8Array(chunks[0]), chunk);
    assert.ok(sentByClient.length >= 1 && sentByClient.length < 13, `${sentByClient.length} fragments for 50 KB`);
    assert.ok(sentByClient.every((h) => h & FLAG_DEFLATE));
    sentByClient.length = 0;
  });

  await t.test('host → client: the same, and compress:false per message through writeWith', async () => {
    host.write(big);
    host.writeWith(big, { compress: false });
    host.write(chunkEncode('down', new Uint8Array(8192).fill(3)));
    await waitFor(() => inbound.length === 3);
    assert.strictEqual(inbound[0], big);
    assert.strictEqual(inbound[1], big);
    assert.strictEqual(inbound[2].length, 8192 + 1 + 4);
    // The plain 4.6 KB packet is two fragments at this message size.
    assert.deepStrictEqual(
      sentByHost.map((h) => h & FLAG_DEFLATE),
      [FLAG_DEFLATE, 0, 0, FLAG_DEFLATE],
    );
  });
});

test('rtc compression (raw channel): one side on, the other off — the plain side hangs up on the first flagged frame', async (t) => {
  const pair = await rawChannelPair(t, { fake: { maxMessageSize: 4096 } });
  const client = new ClientRtcTransport('webrtc:test', { channel: pair.a, compression: true, maxMessageSize: 4096 });
  const errors = [];
  const host = new RtcPeerTransport(pair.b, {
    peer: 'a',
    maxMessageSize: 4096,
    onError: (error) => errors.push(error),
  });
  t.after(() => client.close());
  await client.open();
  const closed = new Promise((resolve) => host.once('close', resolve));
  client.write(big);
  await closed;
  assert.strictEqual(errors.length, 1);
  assert.strictEqual(errors[0].code, 'reserved');
});

test('rtc compression (raw channel): an asynchronous codec keeps both directions in order', async (t) => {
  const pair = await rawChannelPair(t, { fake: { maxMessageSize: 4096 } });
  const client = new ClientRtcTransport('webrtc:test', {
    channel: pair.a,
    compression: { codec: slowCodec() },
    maxMessageSize: 4096,
  });
  const host = new RtcPeerTransport(pair.b, { peer: 'a', compression: { codec: slowCodec() }, maxMessageSize: 4096 });
  t.after(() => client.close());
  const packets = [];
  const inbound = [];
  host.on('packet', (text) => packets.push(text));
  client.on('message', (data) => inbound.push(data));
  await client.open();
  const order = [];
  for (let i = 0; i < 6; i++) {
    const text = i % 2 === 0 ? big.replace('"i":0', `"i":${100 + i}`) : small.replace('ping', `p${i}`);
    order.push(text);
    client.write(text);
    host.write(text);
  }
  await waitFor(() => packets.length === 6 && inbound.length === 6);
  assert.deepStrictEqual(packets, order);
  assert.deepStrictEqual(inbound, order);
});

test('rtc compression (raw channel): an inflate past maxReassembly is a protocol error', async (t) => {
  const pair = await rawChannelPair(t, { fake: { maxMessageSize: 4096 } });
  const client = new ClientRtcTransport('webrtc:test', { channel: pair.a, compression: true, maxMessageSize: 4096 });
  const errors = [];
  const host = new RtcPeerTransport(pair.b, {
    peer: 'a',
    compression: true,
    maxMessageSize: 4096,
    framing: { maxReassembly: 16 * 1024 },
    onError: (error) => errors.push(error),
  });
  t.after(() => client.close());
  await client.open();
  const closed = new Promise((resolve) => host.once('close', resolve));
  // 200 KB of one byte: a few hundred bytes on the wire, far past the cap inflated.
  client.write(JSON.stringify({ pad: 'x'.repeat(200_000) }));
  await closed;
  assert.ok(errors.length >= 1);
  assert.strictEqual(errors[0].code, 'inflate');
  assert.match(String(errors[0].message), /ERR_BUFFER_TOO_LARGE|maxOutputLength|larger than|exceed/i);
});

test('rtc compression: the option is validated, and connect() carries it to the transport', async (t) => {
  assert.throws(() => new ClientRtcTransport('webrtc:x', { compression: 'gzip' }), /compression must be true, false/);
  const pair = await rawChannelPair(t, { fake: { maxMessageSize: 4096 } });
  assert.throws(() => new RtcPeerTransport(pair.b, { compression: { codec: {} } }), /compression\.codec/);
  const host = new RtcPeerTransport(pair.b, { peer: 'a', compression: true });
  const packets = [];
  host.on('packet', (text) => packets.push(text));
  const sent = spy(pair.a);
  const client = await WrpcClient.connect('webrtc:x', {
    transport: 'webrtc',
    channel: pair.a,
    compression: true,
    heartbeat: false,
    reconnect: false,
    logger: false,
  });
  t.after(() => void client.close());
  await client.call('data/echo', { pad: 'y'.repeat(5000) }).catch(() => {});
  await waitFor(() => packets.length >= 1);
  assert.ok(
    sent.some((h) => h & FLAG_DEFLATE),
    'the call packet left compressed',
  );
});

// ---- links: caps in the description signal --------------------------

const linkPair = async (t, capsA, capsB, options = {}) => {
  const fake = createFakeRtc({ maxMessageSize: 4096 });
  const links = {};
  const make = (localId, remoteId, caps) =>
    new RtcLink({
      localId,
      remoteId,
      adapter: fake.adapter,
      log: quiet,
      caps,
      ...options,
      signal: async (message) => {
        await timers.setImmediate();
        await links[remoteId]?.receive(message);
      },
    });
  links.a = make('a', 'b', capsA);
  links.b = make('b', 'a', capsB);
  t.after(() => {
    links.a.close();
    links.b.close();
    fake.world.close();
  });
  links.a.start();
  links.b.start();
  await within(Promise.all([links.a.waitOpen(), links.b.waitOpen()]), 'links open');
  return { a: links.a, b: links.b, fake };
};

test("rtc compression (link): each side learns the other's caps from the description before the channels open", async (t) => {
  const { a, b } = await linkPair(t, { deflate: DEFLATE }, { deflate: DEFLATE });
  assert.deepStrictEqual(a.peerCaps, { deflate: DEFLATE });
  assert.deepStrictEqual(b.peerCaps, { deflate: DEFLATE });
  const client = new ClientRtcTransport('webrtc:b', { link: a, compression: true });
  const host = new RtcPeerTransport(b, { peer: 'a', compression: true });
  t.after(() => client.close());
  await client.open();
  assert.strictEqual(client.compression, DEFLATE);
  assert.strictEqual(host.compression, DEFLATE);
  const sent = spy(a.clientChannel);
  const packets = [];
  host.on('packet', (text) => packets.push(text));
  client.write(big);
  await waitFor(() => packets.length === 1);
  assert.strictEqual(packets[0], big);
  assert.ok(sent[0] & FLAG_DEFLATE);
});

test('rtc compression (link): a peer that announced nothing, or another codec, is served plain — no hangup', async (t) => {
  const { a, b } = await linkPair(t, { deflate: DEFLATE }, null);
  assert.strictEqual(a.peerCaps, null);
  const client = new ClientRtcTransport('webrtc:b', { link: a, compression: true });
  const host = new RtcPeerTransport(b, { peer: 'a' });
  t.after(() => client.close());
  await client.open();
  assert.strictEqual(client.compression, null, 'on locally, off on the wire');
  const sent = spy(a.clientChannel);
  const packets = [];
  host.on('packet', (text) => packets.push(text));
  client.write(big);
  await waitFor(() => packets.length === 1);
  assert.strictEqual(sent[0] & FLAG_DEFLATE, 0);
  const other = await linkPair(t, { deflate: DEFLATE }, { deflate: 'brotli' });
  const client2 = new ClientRtcTransport('webrtc:b', { link: other.a, compression: true });
  t.after(() => client2.close());
  await client2.open();
  assert.strictEqual(client2.compression, null);
});

// ---- WrpcPeer end to end ---------------------------------------------

const routerOf = (name) =>
  defineRouter({
    calc: {
      who: procedure({ access: 'public', handler: async () => name }),
      big: procedure({
        access: 'public',
        handler: async (_ctx, { rows }) => Array.from({ length: rows }, (_, i) => ({ i, name: `row-${i}` })),
      }),
      echo: procedure({ access: 'public', handler: async (_ctx, args) => args }),
    },
  });

const peerPair = async (t, options = {}) => {
  const hub = new FakeSignalHub();
  const fake = createFakeRtc({ maxMessageSize: 16 * 1024 });
  const make = (id) =>
    new WrpcPeer({
      router: routerOf(id),
      signaler: hub.signaler(id),
      rtc: fake.adapter,
      logger: false,
      client: { heartbeat: false },
      ...options,
    });
  const a = make('a');
  const b = make('b');
  t.after(async () => {
    await a.close();
    await b.close();
    fake.world.close();
  });
  await a.start();
  await b.start();
  return { a, b, hub, fake };
};

test('rtc compression (peer): both peers on — calls with large payloads both ways, negotiated per link', async (t) => {
  const { a, b } = await peerPair(t, { compression: true });
  const linkToB = await within(a.connect('b'), 'a dials b');
  await linkToB.load('calc');
  const api = linkToB.api.calc;
  assert.strictEqual(await api.who(), 'b');
  const rows = await api.big({ rows: 400 });
  assert.strictEqual(rows.length, 400);
  const echoed = await api.echo({ pad: 'z'.repeat(40_000) });
  assert.strictEqual(echoed.pad.length, 40_000);
  assert.deepStrictEqual(linkToB.link.peerCaps, { deflate: DEFLATE });
  const linkToA = b.links.get('a');
  assert.deepStrictEqual(linkToA.link.peerCaps, { deflate: DEFLATE });
  await linkToA.load('calc');
  assert.strictEqual((await linkToA.api.calc.big({ rows: 300 })).length, 300);
});

test('rtc compression (peer): one peer on, the other off — plain, and every call still answers', async (t) => {
  const hub = new FakeSignalHub();
  const fake = createFakeRtc({ maxMessageSize: 16 * 1024 });
  const a = new WrpcPeer({
    router: routerOf('a'),
    signaler: hub.signaler('a'),
    rtc: fake.adapter,
    logger: false,
    client: { heartbeat: false },
    compression: true,
  });
  const b = new WrpcPeer({
    router: routerOf('b'),
    signaler: hub.signaler('b'),
    rtc: fake.adapter,
    logger: false,
    client: { heartbeat: false },
  });
  t.after(async () => {
    await a.close();
    await b.close();
    fake.world.close();
  });
  await a.start();
  await b.start();
  const link = await within(a.connect('b'), 'a dials b');
  assert.strictEqual(link.link.peerCaps, null);
  await link.load('calc');
  assert.strictEqual((await link.api.calc.big({ rows: 500 })).length, 500);
  assert.throws(
    () => new WrpcPeer({ signaler: hub.signaler('c'), rtc: fake.adapter, compression: 'lz' }),
    /compression/,
  );
});

// ---- attachChannel ---------------------------------------------------

test('rtc compression (attachChannel): the server half takes the option like every other', async (t) => {
  const rpc = new RpcServer({ router: routerOf('server'), logger: false });
  t.after(() => rpc.close());
  const pair = await rawChannelPair(t, { fake: { maxMessageSize: 4096 } });
  const sentByServer = spy(pair.b);
  const attached = attachChannel(rpc, pair.b, { peer: 'client', compression: true, maxMessageSize: 4096 });
  assert.ok(attached);
  const client = await WrpcClient.connect('webrtc:server', {
    transport: 'webrtc',
    channel: pair.a,
    compression: true,
    heartbeat: false,
    reconnect: false,
    logger: false,
  });
  t.after(() => void client.close());
  await client.load('calc');
  const rows = await client.api.calc.big({ rows: 600 });
  assert.strictEqual(rows.length, 600);
  assert.ok(
    sentByServer.some((h) => h & FLAG_DEFLATE),
    'the answer left compressed',
  );
});
