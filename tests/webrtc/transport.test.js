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
const { rawChannelPair } = require('./rawChannel.js');
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

// ---------------------------------------------------------------------------
// The raw-channel mode: no link, a data channel the application owns.

test('webrtc transport: raw channel — open() over an open channel announces open first and takes binaryType', async (t) => {
  const { a, b } = await rawChannelPair(t);
  assert.strictEqual(a.binaryType, 'blob', 'the browser default the transport must override');
  const client = new ClientRtcTransport('webrtc:host', { channel: a });
  assert.strictEqual(client.link, null);
  assert.strictEqual(client.channel, null, 'null before open()');
  const events = [];
  client.on('open', () => events.push('open'));
  await client.open();
  assert.deepStrictEqual(events, ['open']);
  assert.strictEqual(client.channel, a);
  assert.strictEqual(a.binaryType, 'arraybuffer');
  const host = new RtcPeerTransport(b);
  assert.strictEqual(host.link, null);
  assert.strictEqual(host.channel, b);
  assert.strictEqual(host.source, 'wrpc', 'the label is the default source');
  assert.strictEqual(host.kind, 'webrtc');
  assert.strictEqual(host.connection, host, 'persistent');
  assert.strictEqual(b.binaryType, 'arraybuffer');
  const packets = [];
  const received = [];
  host.on('packet', (text) => packets.push(text));
  client.on('message', (data) => received.push(data));
  assert.strictEqual(client.write('{"type":"ping"}'), true);
  assert.strictEqual(host.send({ type: 'pong' }), true);
  await within(
    waitFor(() => packets.length === 1 && received.length === 1, 'both directions'),
    'both directions',
  );
  assert.deepStrictEqual(packets, ['{"type":"ping"}']);
  assert.deepStrictEqual(received, ['{"type":"pong"}']);
});

test('webrtc transport: raw channel — both sides fragment at their own maxMessageSize', async (t) => {
  const { a, b } = await rawChannelPair(t, { fake: { maxMessageSize: 8192 } });
  const client = new ClientRtcTransport('webrtc:host', { channel: a, maxMessageSize: 1024 });
  const host = new RtcPeerTransport(b, { peer: 'client', maxMessageSize: 4096 });
  assert.strictEqual(host.source, 'client');
  const chunks = [];
  const received = [];
  host.on('chunk', (bytes) => chunks.push(bytes));
  client.on('message', (data) => received.push(data));
  await client.open();
  const up = new Uint8Array(5000).map((_, i) => i & 0xff);
  const down = new Uint8Array(7000).map((_, i) => (i * 7) & 0xff);
  client.write(chunkEncode('s1', up));
  host.write(down);
  await within(
    waitFor(() => chunks.length === 1 && received.length === 1, 'delivery'),
    'delivery',
  );
  assert.deepStrictEqual(Array.from(chunks[0]), Array.from(chunkEncode('s1', up)));
  assert.deepStrictEqual(Array.from(received[0]), Array.from(down));
  assert.ok(a.sent >= 5, `the client fragmented at 1 KiB: ${a.sent} frames`);
  assert.ok(b.sent >= 2, `the host fragmented at 4 KiB: ${b.sent} frames`);
});

test('webrtc transport: raw channel — open() waits for a connecting channel', async (t) => {
  const { a, b, connect } = await rawChannelPair(t, { deferred: true });
  assert.strictEqual(a.readyState, 'connecting');
  const client = new ClientRtcTransport('webrtc:host', { channel: a });
  let opened = false;
  client.on('open', () => (opened = true));
  const opening = client.open();
  await timers.setTimeout(10);
  assert.strictEqual(opened, false, 'still waiting');
  assert.strictEqual(client.channel, a, 'known while waiting (terminate() closes it)');
  await connect();
  await within(opening, 'open');
  assert.strictEqual(opened, true);
  assert.strictEqual(b.readyState, 'open');
});

test('webrtc transport: raw channel — a closed channel is refused with a pointer to the factory', async (t) => {
  const { a } = await rawChannelPair(t);
  a.close();
  await timers.setTimeout(5);
  const client = new ClientRtcTransport('webrtc:host', { channel: a });
  await assert.rejects(client.open(), /closed; pass a factory/);
  assert.strictEqual(client.active, false);
});

test('webrtc transport: raw channel — a connecting channel that closes before opening rejects open()', async (t) => {
  const { a } = await rawChannelPair(t, { deferred: true });
  const client = new ClientRtcTransport('webrtc:host', { channel: a });
  const opening = client.open();
  a.close();
  await assert.rejects(opening, /closed; pass a factory/);
});

test('webrtc transport: raw channel — something that is not a channel is a TypeError', async (t) => {
  const client = new ClientRtcTransport('webrtc:host', { channel: () => ({ send() {} }) });
  await assert.rejects(client.open(), TypeError);
  assert.throws(() => new ClientRtcTransport('webrtc:host', { link: {}, channel: {} }), /mutually exclusive/);
  assert.throws(() => new RtcPeerTransport({ send() {} }), /no host channel/);
});

test('webrtc transport: raw channel — close() closes the channel; the host half sees it', async (t) => {
  const { a, b } = await rawChannelPair(t);
  const client = new ClientRtcTransport('webrtc:host', { channel: a });
  const host = new RtcPeerTransport(b);
  await client.open();
  const hostClosed = onceEvent(host, 'close');
  let closes = 0;
  client.on('close', () => closes++);
  client.close();
  assert.strictEqual(client.active, false);
  assert.strictEqual(closes, 1);
  await within(hostClosed, 'host closed');
  assert.strictEqual(a.readyState, 'closed');
  assert.strictEqual(host.write('late'), false);
  client.close(); // idempotent
  assert.strictEqual(closes, 1);
});

test('webrtc transport: raw channel — the host half closing closes the channel; the client sees it', async (t) => {
  const { a, b } = await rawChannelPair(t);
  const client = new ClientRtcTransport('webrtc:host', { channel: a });
  const host = new RtcPeerTransport(b);
  await client.open();
  const clientClosed = onceEvent(client, 'close');
  host.close();
  await within(clientClosed, 'client closed');
  assert.strictEqual(b.readyState, 'closed');
  assert.strictEqual(client.channel, null);
  host.close(); // idempotent
});

test('webrtc transport: raw channel — terminate() closes the dead channel; a static channel cannot reopen', async (t) => {
  const { a, b } = await rawChannelPair(t);
  const client = new ClientRtcTransport('webrtc:host', { channel: a });
  const host = new RtcPeerTransport(b);
  await client.open();
  const hostClosed = onceEvent(host, 'close');
  client.terminate();
  assert.strictEqual(client.active, false);
  await within(hostClosed, 'host closed');
  assert.strictEqual(a.readyState, 'closed', 'nobody else owns a raw channel');
  await assert.rejects(client.open(), /closed; pass a factory/);
});

test('webrtc transport: raw channel — a factory is asked on every open(), and terminate() during it wins', async (t) => {
  const first = await rawChannelPair(t);
  const { world } = first;
  const hosts = [];
  let calls = 0;
  const factory = async () => {
    calls++;
    const pair = calls === 1 ? first : await rawChannelPair(t, { world });
    hosts.push(new RtcPeerTransport(pair.b));
    return pair.a;
  };
  const client = new ClientRtcTransport('webrtc:host', { channel: factory });
  await client.open();
  assert.strictEqual(calls, 1);
  assert.strictEqual(client.channel, first.a);
  // The channel dies: the next open() gets the factory's next channel.
  const closed = onceEvent(client, 'close');
  first.b.close();
  await within(closed, 'client down');
  await client.open();
  assert.strictEqual(calls, 2);
  assert.notStrictEqual(client.channel, first.a);
  const packets = [];
  hosts[1].on('packet', (text) => packets.push(text));
  client.write('on the second channel');
  await within(
    waitFor(() => packets.includes('on the second channel'), 'delivery'),
    'delivery',
  );
  // terminate() while the factory is still working: the late channel is
  // closed, not adopted.
  client.terminate();
  let release;
  const gate = new Promise((resolve) => (release = resolve));
  const slow = new ClientRtcTransport('webrtc:host', {
    channel: async () => {
      await gate;
      const pair = await rawChannelPair(t, { world });
      return pair.a;
    },
  });
  let opened = 0;
  slow.on('open', () => opened++);
  const pending = slow.open();
  slow.terminate();
  release();
  await assert.rejects(pending, /terminated/);
  assert.strictEqual(opened, 0);
  assert.strictEqual(slow.active, false);
});

test('webrtc transport: raw channel — a framing error closes the channel on both halves', async (t) => {
  const { a, b } = await rawChannelPair(t);
  const errors = [];
  const client = new ClientRtcTransport('webrtc:host', { channel: a });
  const host = new RtcPeerTransport(b, { onError: (error) => errors.push(error.name) });
  client.on('error', (error) => errors.push(error.name));
  await client.open();
  b.send(new Uint8Array([0b11111111, 1]));
  await within(
    waitFor(() => a.readyState === 'closed', 'closed'),
    'closed',
  );
  assert.deepStrictEqual(errors, ['FramingError']);
  assert.strictEqual(client.active, false);
  assert.strictEqual(host.write('late'), false);
});

test('webrtc transport: raw channel — channel and maxMessageSize arrive through connect() options', async (t) => {
  const { a, b } = await rawChannelPair(t);
  const host = new RtcPeerTransport(b);
  const packets = [];
  host.on('packet', (text) => packets.push(text));
  const client = await WrpcClient.connect('webrtc:host', {
    transport: 'webrtc',
    channel: a,
    maxMessageSize: 64,
    heartbeat: false,
    reconnect: false,
  });
  t.after(() => client.close());
  assert.strictEqual(client.active, true);
  client.write(`{"type":"event","name":"x","data":"${'y'.repeat(200)}"}`);
  await within(
    waitFor(() => packets.length === 1, 'delivery'),
    'delivery',
  );
  assert.ok(a.sent >= 4, `fragmented at 64 bytes: ${a.sent} frames`);
});
