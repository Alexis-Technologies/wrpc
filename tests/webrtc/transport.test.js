'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { WrpcClient } = require('../../src/client/core.js');
const { ClientRtcTransport, RtcPeerTransport } = require('../../src/webrtc/transport.js');
const { RtcLink } = require('../../src/webrtc/link.js');
const { KIND_BINARY, FLAG_FIN } = require('../../src/webrtc/framing.js');
const { chunkEncode } = require('../../src/chunks.js');
const { createFakeRtc } = require('./fakeRtc.js');
const { waitFor, within } = require('./portContract.js');
const { runTransportContract } = require('../client/transportContract.js');

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

// A connected pair of links through an in-memory relay.
const linkPair = async (t, options = {}) => {
  const fake = createFakeRtc(options.fake);
  const links = {};
  const make = (localId, remoteId) =>
    new RtcLink({
      localId,
      remoteId,
      adapter: fake.adapter,
      log: quiet,
      restartTimeout: 50,
      ...options.link,
      signal: async (message) => {
        await timers.setImmediate();
        await links[remoteId]?.receive(message);
      },
    });
  links.a = make('a', 'b');
  links.b = make('b', 'a');
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

const onceEvent = (emitter, name) => new Promise((resolve) => emitter.once(name, resolve));

test('webrtc transport: registered and passes the shared client contract', async (t) => {
  assert.strictEqual(WrpcClient.transport.webrtc, ClientRtcTransport);
  await runTransportContract(t, 'webrtc', ClientRtcTransport);
  const bare = new ClientRtcTransport('webrtc:peer');
  assert.strictEqual(bare.link, null);
  assert.strictEqual(bare.heartbeat, true);
  assert.strictEqual(bare.persistent, true);
  await assert.rejects(bare.open(), /needs a link/);
  assert.throws(() => bare.write('x'), /Not connected/);
  bare.close(); // never opened: a no-op
  bare.terminate();
});

test('webrtc transport: open() waits for the link, announces open before resolving, is idempotent', async (t) => {
  const { a } = await linkPair(t);
  const transport = new ClientRtcTransport('webrtc:b', { link: a });
  const events = [];
  transport.on('open', () => events.push('open'));
  let resolvedBeforeOpen = null;
  const opening = transport.open().then(() => {
    resolvedBeforeOpen = events.length === 0;
  });
  const again = transport.open(); // the same in-flight promise
  await Promise.all([opening, again]);
  assert.strictEqual(resolvedBeforeOpen, false, "'open' was emitted before open() resolved");
  assert.strictEqual(transport.active, true);
  assert.deepStrictEqual(events, ['open']);
  await transport.open(); // active: no second open
  assert.deepStrictEqual(events, ['open']);
});

test('webrtc transport: text and binary cross to the peer host transport, in order, fragmented', async (t) => {
  const { a, b } = await linkPair(t, { fake: { maxMessageSize: 32 } });
  const client = new ClientRtcTransport('webrtc:b', { link: a });
  const host = new RtcPeerTransport(b, { peer: 'a' });
  const packets = [];
  const chunks = [];
  host.on('packet', (text) => packets.push(text));
  host.on('chunk', (bytes) => chunks.push(bytes));
  await client.open();
  assert.strictEqual(client.write('{"type":"ping"}'), true);
  const big = new Uint8Array(200).map((_, i) => i);
  client.write(chunkEncode('s1', big));
  client.write('{"type":"pong"}');
  await within(
    waitFor(() => packets.length === 2 && chunks.length === 1, 'delivery'),
    'delivery',
  );
  assert.deepStrictEqual(packets, ['{"type":"ping"}', '{"type":"pong"}']);
  assert.ok(chunks[0] instanceof Uint8Array);
  assert.deepStrictEqual(Array.from(chunks[0]), Array.from(chunkEncode('s1', big)));
  assert.strictEqual(host.kind, 'webrtc');
  assert.strictEqual(host.source, 'a');
  assert.strictEqual(host.connection, host, 'persistent');
  assert.strictEqual(host.link, b);
  assert.strictEqual(host.channel, b.hostChannel);
});

test('webrtc transport: the host half writes back on its channel; the client receives strings and bytes', async (t) => {
  const { a, b } = await linkPair(t);
  const client = new ClientRtcTransport('webrtc:b', { link: a });
  const host = new RtcPeerTransport(b, { peer: 'a' });
  const received = [];
  client.on('message', (data) => received.push(data));
  await client.open();
  assert.strictEqual(host.send({ type: 'callback', id: '1', result: 1 }), true);
  assert.strictEqual(host.write(new Uint8Array([7, 8])), true);
  assert.strictEqual(host.write(new Uint8Array([9]).buffer), true);
  await within(
    waitFor(() => received.length === 3, 'delivery'),
    'delivery',
  );
  assert.strictEqual(received[0], '{"type":"callback","id":"1","result":1}');
  assert.deepStrictEqual(Array.from(received[1]), [7, 8]);
  assert.deepStrictEqual(Array.from(received[2]), [9]);
  assert.throws(() => host.write(42), TypeError);
});

test('webrtc transport: backpressure — write() answers false above the high-water mark, then drain', async (t) => {
  const { a, b } = await linkPair(t, { fake: { latency: 2 } });
  const client = new ClientRtcTransport('webrtc:b', { link: a, highWaterMark: 100, lowWaterMark: 50 });
  const host = new RtcPeerTransport(b, { peer: 'a', highWaterMark: 100, lowWaterMark: 50 });
  await client.open();
  let drains = 0;
  client.on('drain', () => drains++);
  let hostDrains = 0;
  host.on('drain', () => hostDrains++);
  assert.strictEqual(client.write(new Uint8Array(150)), false);
  assert.strictEqual(a.clientChannel.bufferedAmountLowThreshold, 50);
  await within(
    waitFor(() => drains === 1, 'client drain'),
    'client drain',
  );
  assert.strictEqual(client.write(new Uint8Array(10)), true);
  assert.strictEqual(host.write(new Uint8Array(150)), false);
  await within(
    waitFor(() => hostDrains === 1, 'host drain'),
    'host drain',
  );
});

test('webrtc transport: terminate() is local — the link lives, the next open() reattaches', async (t) => {
  const { a, b } = await linkPair(t);
  const client = new ClientRtcTransport('webrtc:b', { link: a });
  const host = new RtcPeerTransport(b, { peer: 'a' });
  const packets = [];
  host.on('packet', (text) => packets.push(text));
  await client.open();
  let closes = 0;
  client.on('close', () => closes++);
  client.terminate();
  assert.strictEqual(client.active, false);
  assert.strictEqual(closes, 1);
  assert.strictEqual(a.state, 'connected', 'the link is untouched');
  assert.throws(() => client.write('x'), /Not connected/);
  client.terminate(); // idempotent
  assert.strictEqual(closes, 1);
  await client.open();
  assert.strictEqual(client.active, true);
  client.write('again');
  await within(
    waitFor(() => packets.includes('again'), 'delivery after reattach'),
    'delivery',
  );
});

test('webrtc transport: terminate() while open() is waiting on the link wins over the late open', async (t) => {
  const fake = createFakeRtc();
  t.after(() => fake.world.close());
  const links = {};
  const make = (localId, remoteId) =>
    new RtcLink({
      localId,
      remoteId,
      adapter: fake.adapter,
      log: quiet,
      signal: async (message) => {
        await timers.setImmediate();
        await links[remoteId]?.receive(message);
      },
    });
  links.a = make('a', 'b');
  links.b = make('b', 'a');
  t.after(() => {
    links.a.close();
    links.b.close();
  });
  const client = new ClientRtcTransport('webrtc:b', { link: links.a });
  let opened = 0;
  client.on('open', () => opened++);
  links.a.start();
  const pending = client.open(); // waits: b has not started
  client.terminate(); // the core gave up (connectTimeout)
  links.b.start();
  await assert.rejects(pending, /terminated/);
  await links.a.waitOpen();
  await timers.setTimeout(5);
  assert.strictEqual(opened, 0);
  assert.strictEqual(client.active, false);
  await client.open();
  assert.strictEqual(opened, 1);
});

test('webrtc transport: close() on either half ends the LINK — both halves go down, the peer is told', async (t) => {
  const { a, b } = await linkPair(t);
  const client = new ClientRtcTransport('webrtc:b', { link: a });
  const host = new RtcPeerTransport(b, { peer: 'a' });
  await client.open();
  const hostClosed = onceEvent(host, 'close');
  const clientClosed = onceEvent(client, 'close');
  client.close();
  assert.strictEqual(client.active, false);
  assert.strictEqual(a.state, 'closed');
  await within(Promise.all([hostClosed, clientClosed]), 'both closed');
  assert.strictEqual(b.state, 'closed', 'the peer received the goodbye');
  assert.strictEqual(host.write('late'), false, 'a down host transport answers false, never throws');
  host.close(); // idempotent on a closed link
});

test('webrtc transport: the server half closing (client.close() on the host) also ends the link', async (t) => {
  const { a, b } = await linkPair(t);
  const client = new ClientRtcTransport('webrtc:b', { link: a });
  const host = new RtcPeerTransport(b, { peer: 'a' });
  await client.open();
  const clientClosed = onceEvent(client, 'close');
  host.close();
  await within(clientClosed, 'client down');
  assert.strictEqual(b.state, 'closed');
  assert.strictEqual(a.state, 'closed');
});

test('webrtc transport: a link failure closes both halves; a redialled link is picked up by the next open()', async (t) => {
  const { a, b } = await linkPair(t);
  const client = new ClientRtcTransport('webrtc:b', { link: a });
  const host = new RtcPeerTransport(b, { peer: 'a' });
  await client.open();
  const firstChannel = a.clientChannel;
  const clientClosed = onceEvent(client, 'close');
  const hostClosed = onceEvent(host, 'close');
  // Kill the link for good: b's pc dies without a goodbye.
  b.pc.close();
  await within(Promise.all([clientClosed, hostClosed]), 'both down');
  assert.strictEqual(a.state, 'failed');
  // The owner redials; the transport's next open() follows the link.
  await within(
    waitFor(() => b.state === 'failed', 'b failed'),
    'b failed',
  );
  a.redial();
  b.redial();
  await client.open();
  assert.notStrictEqual(a.clientChannel, firstChannel);
  const host2 = new RtcPeerTransport(b, { peer: 'a' });
  const packets = [];
  host2.on('packet', (text) => packets.push(text));
  client.write('after redial');
  await within(
    waitFor(() => packets.includes('after redial'), 'delivery'),
    'delivery',
  );
});

test('webrtc transport: a framing error from the peer is an error event and closes the link', async (t) => {
  const { a, b } = await linkPair(t);
  const client = new ClientRtcTransport('webrtc:b', { link: a });
  const errors = [];
  client.on('error', (error) => errors.push(error));
  await client.open();
  // Raw garbage straight on the channel the client reads (b's HOST channel
  // is the pair of a's client channel): reserved bits set.
  b.hostChannel.send(new Uint8Array([0b11111111, 1, 2]));
  await within(
    waitFor(() => a.state === 'closed', 'link closed'),
    'link closed',
  );
  assert.strictEqual(errors.length, 1);
  assert.strictEqual(errors[0].name, 'FramingError');
  assert.strictEqual(client.active, false);
});

test('webrtc transport: the host half reports framing errors through onError and closes the link', async (t) => {
  const { a, b } = await linkPair(t);
  const errors = [];
  const host = new RtcPeerTransport(b, { peer: 'a', onError: (error) => errors.push(error) });
  const client = new ClientRtcTransport('webrtc:b', { link: a });
  await client.open();
  a.clientChannel.send(new Uint8Array([KIND_BINARY | FLAG_FIN | 0b100]));
  await within(
    waitFor(() => b.state === 'closed', 'link closed'),
    'link closed',
  );
  assert.strictEqual(errors[0].name, 'FramingError');
  assert.strictEqual(host.write('late'), false, 'the host half is down with the link');
});

test('webrtc transport: a channel error event reaches onError; without onError it is dropped', async (t) => {
  const { a, b } = await linkPair(t);
  const errors = [];
  const host = new RtcPeerTransport(b, { peer: 'a', onError: (error) => errors.push(error.message) });
  const quietHost = new RtcPeerTransport(a, { peer: 'b' });
  b.hostChannel.dispatchEvent(Object.assign(new Event('error'), { error: new Error('channel broke') }));
  b.hostChannel.dispatchEvent(new Event('error'));
  a.hostChannel.dispatchEvent(new Event('error'));
  assert.deepStrictEqual(errors, ['channel broke', 'data channel error']);
  assert.strictEqual(host.write('still up'), true);
  assert.strictEqual(quietHost.write('still up'), true);
});

test('webrtc transport: a link is accepted through connect() options, like the event transport takes worker', async (t) => {
  const { a } = await linkPair(t);
  const client = await WrpcClient.connect('webrtc:b', {
    transport: 'webrtc',
    link: a,
    heartbeat: false,
    reconnect: false,
  });
  t.after(() => client.close());
  assert.strictEqual(client.active, true);
  assert.strictEqual(client.url, 'webrtc:b');
});

test('webrtc transport: constructing the host half needs a host channel', async (t) => {
  const fake = createFakeRtc();
  t.after(() => fake.world.close());
  const link = new RtcLink({ localId: 'a', remoteId: 'b', adapter: fake.adapter, signal() {}, log: quiet });
  assert.throws(() => new RtcPeerTransport(link, { peer: 'b' }), /no host channel/);
});
