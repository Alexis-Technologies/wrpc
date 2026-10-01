'use strict';

// A fan-out over data channels: what PeerHost.to(room).emit — Mesh.broadcast
// — does with one message and N links. The JSON was always one per emit;
// its UTF-8 and, under compression, the deflated body are too now
// (ChannelCodec.sendShared), and only the fragmenting is per link. These
// check that sharing: who compresses, how often, and that every recipient
// still reads exactly what a link of its own would have been sent.

const { test } = require('node:test');
const assert = require('node:assert');
const zlib = require('node:zlib');
const crypto = require('node:crypto');
const timers = require('node:timers/promises');

const { defineRouter, procedure } = require('../../index.js');
const { PeerHost } = require('../../src/webrtc/host.js');
const { RtcPeerTransport } = require('../../src/webrtc/transport.js');
const { FLAG_COMPRESSED, SharedFrames } = require('../../src/webrtc/framing.js');
const { PreparedFrames } = require('../../src/websocket/prepared.js');
const { decodeAttachments } = require('../../src/attachments.js');
const { rawChannelPair } = require('./rawChannel.js');
const { waitFor, within } = require('./portContract.js');

const router = defineRouter({ noop: { ping: procedure({ access: 'public', handler: async () => null }) } });

// A codec that counts — and, with `delay`, answers later, the way a
// CompressionStream does.
const counting = (id = 'deflate-raw', { delay = 0, fail = false } = {}) => {
  const codec = {
    id,
    threshold: 64,
    calls: 0,
    encode: (bytes) => {
      codec.calls++;
      if (fail) throw new Error('codec broke');
      const out = zlib.deflateRawSync(bytes);
      return delay > 0 ? timers.setTimeout(delay).then(() => out) : out;
    },
    decode: (bytes, max) => zlib.inflateRawSync(bytes, { maxOutputLength: max }),
  };
  return codec;
};

// A host with one link per spec — `{ size, compression }` — each over a raw
// channel pair whose far end is a transport of the same options, which
// reassembles and inflates for the assertions. `headers` is the first byte
// of every frame the host put on that link.
const meshOf = async (t, specs) => {
  const host = new PeerHost({ router, logger: false });
  const links = [];
  for (let i = 0; i < specs.length; i++) {
    const { size = 16384, compression = null, ...rest } = specs[i];
    const pair = await rawChannelPair(t);
    const headers = [];
    const send = pair.a.send.bind(pair.a);
    pair.a.send = (frame) => {
      headers.push(frame[0]);
      return send(frame);
    };
    const out = new RtcPeerTransport(pair.a, { peer: `p${i}`, maxMessageSize: size, compression, ...rest });
    const client = host.attach(out, { peer: `p${i}` });
    client.join('room');
    const far = new RtcPeerTransport(pair.b, { peer: 'host', maxMessageSize: size, compression });
    const packets = [];
    const chunks = [];
    far.on('packet', (text) => packets.push(JSON.parse(text)));
    far.on('chunk', (bytes) => chunks.push(bytes));
    links.push({ out, client, packets, chunks, headers, channel: pair.a });
  }
  return { host, links };
};

const rows = (n) => ({ rows: Array.from({ length: n }, (_, i) => ({ i, name: `row-${i}`, tags: ['a', 'b'] })) });
const compressed = (headers) => headers.filter((header) => (header & FLAG_COMPRESSED) !== 0).length;
const delivered = (links, count) =>
  within(
    waitFor(() => links.every((link) => link.packets.length >= count), `${count} packets on every link`),
    'delivery',
  );

test('rtc fan-out: one deflate for the whole emit, whatever the links’ message sizes', async (t) => {
  const codec = counting();
  const compression = { codec };
  const { host, links } = await meshOf(t, [
    { size: 16384, compression },
    { size: 65536, compression },
    { size: 16384, compression },
    { size: 262144, compression },
  ]);
  // Text a deflate barely shrinks, so the BODY is still several fragments on
  // the small links and one on the large.
  const data = { ...rows(20), noise: crypto.randomBytes(60_000).toString('base64') };
  assert.strictEqual(host.to('room').emit('chat/state', data), 4);
  await delivered(links, 1);
  assert.strictEqual(codec.calls, 1, 'compressed once, not once per link');
  for (const link of links) {
    assert.deepStrictEqual(link.packets[0], { type: 'event', name: 'chat/state', data });
    assert.strictEqual(compressed(link.headers), link.headers.length, 'every fragment carries the flag');
  }
  // The same body, fragmented to each link's own size.
  assert.ok(links[0].headers.length > links[1].headers.length);
  assert.strictEqual(links[3].headers.length, 1);
  // A second emit is a second message: nothing is kept across emits.
  host.to('room').emit('chat/state', data);
  await delivered(links, 2);
  assert.strictEqual(codec.calls, 2);
});

test('rtc fan-out: plain where it should be — under the threshold, compress: false, a link without the codec', async (t) => {
  const codec = counting();
  const { host, links } = await meshOf(t, [{ compression: { codec } }, { compression: { codec } }, {}]);
  host.to('room').emit('chat/tick', { n: 1 });
  await delivered(links, 1);
  assert.strictEqual(codec.calls, 0, 'a small event is not worth a deflate');
  const data = rows(60);
  host.to('room').emit('chat/state', data, { compress: false });
  await delivered(links, 2);
  assert.strictEqual(codec.calls, 0, 'the emit opted out');
  host.to('room').emit('chat/state', data);
  await delivered(links, 3);
  assert.strictEqual(codec.calls, 1);
  // Two links took the compressed body; the third has no codec and got the
  // same message plain — from the same shared bytes.
  assert.deepStrictEqual(
    links.map((link) => compressed(link.headers)),
    [1, 1, 0],
  );
  for (const link of links) assert.deepStrictEqual(link.packets[2].data, data);
});

test('rtc fan-out: a link on another codec gets a body of its own; a codec that throws sends plain', async (t) => {
  const deflate = counting('deflate-raw');
  const other = counting('x-other');
  const broken = counting('x-broken', { fail: true });
  const { host, links } = await meshOf(t, [
    { compression: { codec: deflate } },
    { compression: { codec: other } },
    { compression: { codec: deflate } },
    { compression: { codec: broken } },
    { compression: { codec: broken } },
  ]);
  const data = rows(60);
  host.to('room').emit('chat/state', data);
  await delivered(links, 1);
  assert.deepStrictEqual([deflate.calls, other.calls], [1, 1], 'one body per codec id');
  assert.strictEqual(broken.calls, 1, 'asked once; its answer — none — is remembered for the emit');
  assert.deepStrictEqual(
    links.map((link) => compressed(link.headers)),
    [1, 1, 1, 0, 0],
  );
  for (const link of links) assert.deepStrictEqual(link.packets[0].data, data);
});

test('rtc fan-out: an asynchronous codec is awaited once, and the order of a link is kept around it', async (t) => {
  const codec = counting('deflate-raw', { delay: 15 });
  const { host, links } = await meshOf(t, [{ compression: { codec } }, { compression: { codec } }]);
  const data = rows(60);
  host.to('room').emit('chat/state', data); // in the codec for 15 ms
  links[0].client.sendEvent('chat/after', { n: 1 }); // a direct send behind it, on one link
  host.to('room').emit('chat/tick', { n: 2 }); // and a small shared one behind both
  await delivered(links, 2);
  await within(
    waitFor(() => links[0].packets.length === 3, 'three on the first link'),
    'delivery',
  );
  assert.strictEqual(codec.calls, 1, 'both links waited on one promise');
  assert.deepStrictEqual(
    links[0].packets.map((packet) => packet.name),
    ['chat/state', 'chat/after', 'chat/tick'],
    'send order, not completion order',
  );
  assert.deepStrictEqual(
    links[1].packets.map((packet) => packet.name),
    ['chat/state', 'chat/tick'],
  );
});

test('rtc fan-out: bytes in the data are one attachments frame for every link', async (t) => {
  const codec = counting();
  const { host, links } = await meshOf(t, [{ compression: { codec } }, {}]);
  const blob = Uint8Array.from({ length: 5000 }, (_, i) => i % 7); // compressible
  host.to('room').emit('chat/file', { name: 'a.bin', blob });
  await within(
    waitFor(() => links.every((link) => link.chunks.length === 1), 'a binary message on every link'),
    'delivery',
  );
  assert.strictEqual(codec.calls, 1);
  for (const link of links) {
    const packet = decodeAttachments(link.chunks[0]);
    assert.strictEqual(packet.name, 'chat/file');
    assert.deepStrictEqual(Buffer.from(packet.data.blob), Buffer.from(blob));
  }
});

test('rtc fan-out: a slot another engine filled is left alone; a link over its cap is closed, the others are not', async (t) => {
  const faults = [];
  const { host, links } = await meshOf(t, [
    {},
    { maxBackpressure: 64, onError: (error) => faults.push(error.code) },
    {},
  ]);
  // A room mixing WebSocket clients and data channels: the first WebSocket
  // recipient fills the slot with ITS cache. The channel sends the text.
  const text = JSON.stringify({ type: 'event', name: 'chat/mixed', data: { n: 1 } });
  const foreign = { text, frames: new PreparedFrames(text), inner: null, compress: true };
  assert.strictEqual(links[0].out.writeShared(foreign), true);
  assert.ok(foreign.frames instanceof PreparedFrames, 'not replaced');
  await within(
    waitFor(() => links[0].packets.length === 1, 'the mixed-room packet'),
    'delivery',
  );
  assert.strictEqual(links[0].packets[0].name, 'chat/mixed');
  // Its own slot is a SharedFrames, made by the first link and reused.
  const own = { text, frames: null, inner: null, compress: true };
  links[0].out.writeShared(own);
  const made = own.frames;
  assert.ok(made instanceof SharedFrames);
  links[2].out.writeShared(own);
  assert.strictEqual(own.frames, made);
  // Past maxBackpressure: that channel is closed, as a direct send would do.
  host.to('room').emit('chat/state', rows(60));
  await within(
    waitFor(() => links[1].channel.readyState !== 'open', 'the capped link closed'),
    'close',
  );
  assert.deepStrictEqual(faults, ['backpressure']);
  assert.strictEqual(links[0].channel.readyState, 'open');
  assert.strictEqual(links[2].channel.readyState, 'open');
  // A transport that is down answers false and touches nothing.
  assert.strictEqual(links[1].out.writeShared({ text, frames: null, inner: null, compress: true }), false);
});
