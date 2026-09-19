'use strict';

const http = require('node:http');
const zlib = require('node:zlib');
const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { WrpcClient, defineRouter, procedure, tracked } = require('../../index.js');
const { SseParser, CHANNEL_HEADER } = require('../../sse.js');
const { bootServer, waitFor } = require('../helpers/server.js');

const bigEvent = { rows: Array.from({ length: 40 }, (_, i) => ({ id: i, name: `row-${i}` })) };

const router = defineRouter({
  feed: {
    ping: procedure({ access: 'public', handler: async () => 'pong' }),
    burst: procedure({
      access: 'public',
      handler: async (context, { count }) => {
        for (let i = 0; i < count; i++) context.client.sendEvent('feed/tick', { ...bigEvent, i });
        return count;
      },
    }),
    ticks: procedure.subscription({
      access: 'public',
      handler: async function* (_context, { count }) {
        for (let i = 0; i < count; i++) yield tracked(String(i), { ...bigEvent, i });
      },
    }),
  },
});

// Opens the events stream over node:http and hands back the response plus a
// parser fed with the INFLATED bytes (through a streaming gunzip when the
// response says so) — so the assertions see what a browser's fetch sees,
// while the raw chunk sizes stay observable.
const openStream = (origin, headers = {}) =>
  new Promise((resolve, reject) => {
    http
      .get(`${origin}/api/events`, { headers: { Accept: 'text/event-stream', ...headers } }, (res) => {
        const parser = new SseParser();
        const events = [];
        const raw = [];
        const encoded = res.headers['content-encoding'] === 'gzip';
        const source = encoded ? res.pipe(zlib.createGunzip()) : res;
        source.on('data', (chunk) => {
          for (const event of parser.push(chunk.toString())) events.push(event);
        });
        res.on('data', (chunk) => raw.push(chunk.length));
        resolve({ res, events, raw, encoded, close: () => res.destroy() });
      })
      .on('error', reject);
  });

const channelOf = (events) => JSON.parse(events.find((e) => e.event === 'ready').data).channel;

const postPacket = async (origin, channel, packet) => {
  const res = await fetch(`${origin}/api`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: channel },
    body: JSON.stringify(packet),
  });
  assert.strictEqual(res.status, 202);
};

test('sse compression: off by default, whatever the peer accepts', async (t) => {
  const { origin } = await bootServer(t, { router });
  const stream = await openStream(origin, { 'Accept-Encoding': 'gzip' });
  t.after(stream.close);
  assert.strictEqual(stream.res.headers['content-encoding'], undefined);
  await waitFor(() => stream.events.some((e) => e.event === 'ready'));
});

test('sse compression: on, the stream is one gzip member flushed per event', async (t) => {
  const { origin } = await bootServer(t, { router, sse: { compression: true } });
  const stream = await openStream(origin, { 'Accept-Encoding': 'gzip' });
  t.after(stream.close);
  assert.strictEqual(stream.res.headers['content-encoding'], 'gzip');
  assert.strictEqual(stream.res.headers.vary, 'Accept-Encoding');
  assert.strictEqual(stream.res.headers['content-type'], 'text/event-stream');

  await t.test('the ready frame arrives at once — nothing waits for a next event', async () => {
    await waitFor(() => stream.events.some((e) => e.event === 'ready'), 'ready never inflated');
  });

  const channel = channelOf(stream.events);

  await t.test('a callback comes back on the encoded stream', async () => {
    await postPacket(origin, channel, { type: 'call', id: 'p1', method: 'feed/ping', args: {} });
    await waitFor(() => stream.events.some((e) => e.data.includes('"id":"p1"')), 'callback never arrived');
    const packet = JSON.parse(stream.events.find((e) => e.data.includes('"id":"p1"')).data);
    assert.strictEqual(packet.result, 'pong');
  });

  await t.test('events are each their own flush, and repetition pays off on the wire', async () => {
    const before = stream.events.length;
    const rawBefore = stream.raw.reduce((a, b) => a + b, 0);
    await postPacket(origin, channel, { type: 'call', id: 'b1', method: 'feed/burst', args: { count: 20 } });
    await waitFor(() => stream.events.filter((e) => e.data.includes('feed/tick')).length === 20, 'ticks missing');
    const ticks = stream.events.slice(before).filter((e) => e.data.includes('feed/tick'));
    assert.strictEqual(ticks.length, 20);
    const plain = ticks.reduce((sum, e) => sum + e.data.length, 0);
    // Let the last chunk land before reading the raw total.
    await timers.setTimeout(20);
    const wire = stream.raw.reduce((a, b) => a + b, 0) - rawBefore;
    assert.ok(wire < plain / 8, `20 near-identical events: ${plain} B plain, ${wire} B on the wire`);
  });

  await t.test('a subscription streams through the same member', async () => {
    await postPacket(origin, channel, { type: 'subscribe', id: 's1', method: 'feed/ticks', args: { count: 5 } });
    await waitFor(() => stream.events.some((e) => e.data.includes('"type":"end"')), 'the end packet never came');
    const data = stream.events.filter((e) => e.data.includes('"type":"data"') && e.data.includes('"id":"s1"'));
    assert.strictEqual(data.length, 5);
  });
});

test('sse compression: the decision is per GET — accept, filter, re-attach', async (t) => {
  const { origin } = await bootServer(t, {
    router,
    sse: { compression: { filter: (call) => call.headers['x-compress'] !== 'no' } },
  });

  await t.test('a peer that does not accept gzip gets a plain stream', async () => {
    const stream = await openStream(origin, { 'Accept-Encoding': 'identity' });
    t.after(stream.close);
    assert.strictEqual(stream.res.headers['content-encoding'], undefined);
    await waitFor(() => stream.events.some((e) => e.event === 'ready'));
  });

  await t.test('the filter refuses one', async () => {
    const stream = await openStream(origin, { 'Accept-Encoding': 'gzip', 'x-compress': 'no' });
    t.after(stream.close);
    assert.strictEqual(stream.res.headers['content-encoding'], undefined);
    await waitFor(() => stream.events.some((e) => e.event === 'ready'));
  });

  await t.test('a re-attach with Last-Event-ID replays on a fresh member — or plain, if it asks', async () => {
    const first = await openStream(origin, { 'Accept-Encoding': 'gzip' });
    t.after(first.close);
    assert.strictEqual(first.res.headers['content-encoding'], 'gzip');
    await waitFor(() => first.events.some((e) => e.event === 'ready'));
    const channel = channelOf(first.events);
    await postPacket(origin, channel, { type: 'call', id: 'p1', method: 'feed/ping', args: {} });
    await waitFor(() => first.events.some((e) => e.id === '0'), 'first frame missing');
    first.close();
    // Encoded again, from a frame the peer says it did not see.
    const second = await openStream(origin, {
      'Accept-Encoding': 'gzip',
      [CHANNEL_HEADER]: channel,
      'last-event-id': '-1',
    });
    t.after(second.close);
    assert.strictEqual(second.res.headers['content-encoding'], 'gzip');
    await waitFor(() => second.events.some((e) => e.id === '0'), 'the replay never came through the new member');
    second.close();
    // And plain on the same channel, when this GET does not accept gzip.
    const third = await openStream(origin, {
      'Accept-Encoding': 'identity',
      [CHANNEL_HEADER]: channel,
      'last-event-id': '-1',
    });
    t.after(third.close);
    assert.strictEqual(third.res.headers['content-encoding'], undefined);
    await waitFor(() => third.events.some((e) => e.id === '0'), 'the plain replay never came');
  });
});

test('sse compression: the wrpc client needs no change — fetch inflates for it', async (t) => {
  const { origin } = await bootServer(t, { router, sse: { compression: true } });
  const client = await WrpcClient.connect(`${origin}/api`, { transport: 'sse', reconnect: false, heartbeat: false });
  t.after(() => void client.close());
  await client.load('feed');
  const api = client.api.feed;
  assert.strictEqual(await api.ping(), 'pong');
  const ticks = [];
  api.on('tick', (data) => ticks.push(data));
  await api.burst({ count: 3 });
  await waitFor(() => ticks.length === 3);
  const seen = [];
  api.ticks.subscribe({ count: 4 }, { onData: (value) => seen.push(value.i) });
  await waitFor(() => seen.length === 4);
  assert.deepStrictEqual(seen, [0, 1, 2, 3]);
});

test('sse compression: the option is validated', async () => {
  const { SseChannels } = require('../../sse.js');
  assert.throws(
    () => new SseChannels({ addClient: () => ({}), compression: 'gzip' }),
    /must be true, false or an options/,
  );
  assert.throws(() => new SseChannels({ addClient: () => ({}), compression: { level: 'max' } }), /level/);
  assert.doesNotThrow(() => new SseChannels({ addClient: () => ({}), compression: { level: 9, memLevel: 9 } }));
});
