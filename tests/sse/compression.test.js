'use strict';

const http = require('node:http');
const zlib = require('node:zlib');
const { Transform } = require('node:stream');
const timers = require('node:timers/promises');
const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

const { WrpcClient, defineRouter, procedure, tracked } = require('../../index.js');
const { SseParser, CHANNEL_HEADER } = require('../../src/sse/index.js');
const { bootServer, waitFor } = require('../helpers/server.js');
const { suiteScope } = require('../helpers/scope.js');

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
const INFLATERS = {
  gzip: () => zlib.createGunzip(),
  br: () => zlib.createBrotliDecompress(),
  zstd: () => zlib.createZstdDecompress(),
  // The test's own coding: every byte XORed, so a chunk decodes on its own.
  'x-xor': () =>
    new Transform({
      transform(chunk, _encoding, done) {
        done(
          null,
          Buffer.from(chunk).map((byte) => byte ^ 0x5a),
        );
      },
    }),
};

const openStream = (origin, headers = {}) =>
  new Promise((resolve, reject) => {
    http
      .get(`${origin}/api/events`, { headers: { Accept: 'text/event-stream', ...headers } }, (res) => {
        const parser = new SseParser();
        const events = [];
        const raw = [];
        const coding = res.headers['content-encoding'];
        const encoded = coding !== undefined;
        const source = encoded ? res.pipe(INFLATERS[coding]()) : res;
        source.on('data', (chunk) => {
          for (const event of parser.push(chunk.toString())) events.push(event);
        });
        res.on('data', (chunk) => raw.push(chunk.length));
        resolve({ res, events, raw, encoded, close: () => res.destroy() });
      })
      .on('error', reject);
  });

// What a request presents: the id and the secret the `ready` frame handed out.
const channelOf = (events) => {
  const ready = JSON.parse(events.find((e) => e.event === 'ready').data);
  return ready.channel;
};

const postPacket = async (origin, channel, packet) => {
  const res = await fetch(`${origin}/api`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: channel },
    body: JSON.stringify(packet),
  });
  assert.strictEqual(res.status, 202);
};

describe('sse compression', () => {
  it('should be off by default, whatever the peer accepts', async (t) => {
    const { origin } = await bootServer(t, { router });
    const stream = await openStream(origin, { 'Accept-Encoding': 'gzip' });
    t.after(stream.close);
    assert.strictEqual(stream.res.headers['content-encoding'], undefined);
    await waitFor(() => stream.events.some((e) => e.event === 'ready'));
  });

  describe('on, the stream is one gzip member flushed per event', () => {
    const suite = suiteScope();
    after(() => suite.close());
    let origin = '';
    let stream = null;
    let channel = null;
    before(async () => {
      ({ origin } = await bootServer(suite, { router, sse: { compression: true } }));
      stream = await openStream(origin, { 'Accept-Encoding': 'gzip' });
      suite.after(stream.close);
    });

    it('should answer the stream gzip-encoded, varied by Accept-Encoding, as text/event-stream', () => {
      assert.strictEqual(stream.res.headers['content-encoding'], 'gzip');
      assert.strictEqual(stream.res.headers.vary, 'Accept-Encoding');
      assert.strictEqual(stream.res.headers['content-type'], 'text/event-stream');
    });

    it('should deliver the ready frame at once — nothing waits for a next event', async () => {
      await waitFor(() => stream.events.some((e) => e.event === 'ready'), 'ready never inflated');
      channel = channelOf(stream.events);
    });

    it('should bring a callback back on the encoded stream', async () => {
      await postPacket(origin, channel, { type: 'call', id: 'p1', method: 'feed/ping', args: {} });
      await waitFor(() => stream.events.some((e) => e.data.includes('"id":"p1"')), 'callback never arrived');
      const packet = JSON.parse(stream.events.find((e) => e.data.includes('"id":"p1"')).data);
      assert.strictEqual(packet.result, 'pong');
    });

    it('should flush each event on its own, and repetition pays off on the wire', async () => {
      const seenBefore = stream.events.length;
      const rawBefore = stream.raw.reduce((a, b) => a + b, 0);
      await postPacket(origin, channel, { type: 'call', id: 'b1', method: 'feed/burst', args: { count: 20 } });
      await waitFor(() => stream.events.filter((e) => e.data.includes('feed/tick')).length === 20, 'ticks missing');
      const ticks = stream.events.slice(seenBefore).filter((e) => e.data.includes('feed/tick'));
      assert.strictEqual(ticks.length, 20);
      const plain = ticks.reduce((sum, e) => sum + e.data.length, 0);
      // Let the last chunk land before reading the raw total.
      await timers.setTimeout(20);
      const wire = stream.raw.reduce((a, b) => a + b, 0) - rawBefore;
      assert.ok(wire < plain / 8, `20 near-identical events: ${plain} B plain, ${wire} B on the wire`);
    });

    it('should stream a subscription through the same member', async () => {
      await postPacket(origin, channel, { type: 'subscribe', id: 's1', method: 'feed/ticks', args: { count: 5 } });
      await waitFor(() => stream.events.some((e) => e.data.includes('"type":"end"')), 'the end packet never came');
      const data = stream.events.filter((e) => e.data.includes('"type":"data"') && e.data.includes('"id":"s1"'));
      assert.strictEqual(data.length, 5);
    });
  });

  describe('the decision is per GET — accept, filter, re-attach', () => {
    const suite = suiteScope();
    after(() => suite.close());
    let origin = '';
    before(async () => {
      ({ origin } = await bootServer(suite, {
        router,
        sse: { compression: { filter: (call) => call.headers['x-compress'] !== 'no' } },
      }));
    });

    it('should give a peer that does not accept gzip a plain stream', async (t) => {
      const stream = await openStream(origin, { 'Accept-Encoding': 'identity' });
      t.after(stream.close);
      assert.strictEqual(stream.res.headers['content-encoding'], undefined);
      await waitFor(() => stream.events.some((e) => e.event === 'ready'));
    });

    it('should let the filter refuse one', async (t) => {
      const stream = await openStream(origin, { 'Accept-Encoding': 'gzip', 'x-compress': 'no' });
      t.after(stream.close);
      assert.strictEqual(stream.res.headers['content-encoding'], undefined);
      await waitFor(() => stream.events.some((e) => e.event === 'ready'));
    });

    it('should replay a re-attach with Last-Event-ID on a fresh member — or plain, if it asks', async (t) => {
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

  it('should need no change in the wrpc client — fetch inflates for it', async (t) => {
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

  for (const coding of ['br', 'zstd']) {
    it(`should serve ${coding} — the same stream, every event flushed the moment it is written`, async (t) => {
      const missing = coding === 'zstd' && typeof zlib.createZstdCompress !== 'function';
      if (missing) return void t.skip('no zstd in this node:zlib');
      const { origin } = await bootServer(t, { router, sse: { compression: { encodings: [coding, 'gzip'] } } });
      const stream = await openStream(origin, { 'Accept-Encoding': `gzip, ${coding}` });
      t.after(stream.close);
      assert.strictEqual(stream.res.headers['content-encoding'], coding, 'the server’s first choice the GET accepts');
      assert.strictEqual(stream.res.headers.vary, 'Accept-Encoding');
      // Nothing waits for a next event: ready, then a callback, each on its own flush.
      await waitFor(() => stream.events.some((e) => e.event === 'ready'), 'ready never inflated');
      const channel = channelOf(stream.events);
      await postPacket(origin, channel, { type: 'call', id: 'p1', method: 'feed/ping', args: {} });
      await waitFor(() => stream.events.some((e) => e.data.includes('"id":"p1"')), 'callback never arrived');
      const rawBefore = stream.raw.reduce((a, b) => a + b, 0);
      await postPacket(origin, channel, { type: 'call', id: 'b1', method: 'feed/burst', args: { count: 20 } });
      await waitFor(() => stream.events.filter((e) => e.data.includes('feed/tick')).length === 20, 'ticks missing');
      const plain = stream.events
        .filter((e) => e.data.includes('feed/tick'))
        .reduce((sum, e) => sum + e.data.length, 0);
      await timers.setTimeout(20);
      const wire = stream.raw.reduce((a, b) => a + b, 0) - rawBefore;
      assert.ok(wire < plain / 6, `20 near-identical events: ${plain} B plain, ${wire} B on the wire`);
      // A peer that only takes gzip gets gzip from the same server.
      const other = await openStream(origin, { 'Accept-Encoding': 'gzip' });
      t.after(other.close);
      assert.strictEqual(other.res.headers['content-encoding'], 'gzip');
    });
  }

  it('should take an application’s own streaming coding', async (t) => {
    const xor = {
      encoding: 'x-xor',
      encode: (body) => Buffer.from(body).map((byte) => byte ^ 0x5a),
      createStream: () => INFLATERS['x-xor'](),
    };
    const { origin } = await bootServer(t, { router, sse: { compression: { encodings: [xor] } } });
    const stream = await openStream(origin, { 'Accept-Encoding': 'x-xor' });
    t.after(stream.close);
    assert.strictEqual(stream.res.headers['content-encoding'], 'x-xor');
    await waitFor(() => stream.events.some((e) => e.event === 'ready'), 'ready never decoded');
    const channel = channelOf(stream.events);
    await postPacket(origin, channel, { type: 'call', id: 'p1', method: 'feed/ping', args: {} });
    await waitFor(() => stream.events.some((e) => e.data.includes('"id":"p1"')), 'callback never arrived');
  });

  it('should end the response on an encoder stream that fails, and say the failure once for the coding', async (t) => {
    const { recorder } = require('../helpers/recorder.js');
    const log = recorder();
    let streams = 0;
    const failing = {
      encoding: 'x-xor',
      encode: (body) => Buffer.from(body),
      // Passes everything up to the callback of the call below, and fails on it.
      createStream: () => {
        streams++;
        return new Transform({
          transform(chunk, _encoding, done) {
            if (String(chunk).includes('"id":"p1"')) return void done(new Error('encoder stream broke'));
            done(
              null,
              Buffer.from(chunk).map((byte) => byte ^ 0x5a),
            );
          },
        });
      },
    };
    const { origin } = await bootServer(t, {
      router,
      logger: log.writer,
      sse: { compression: { encodings: [failing] } },
    });
    for (let round = 0; round < 2; round++) {
      const stream = await openStream(origin, { 'Accept-Encoding': 'x-xor' });
      t.after(stream.close);
      await waitFor(() => stream.events.some((e) => e.event === 'ready'), 'ready never decoded');
      const ended = new Promise((resolve) => stream.res.once('end', resolve));
      await postPacket(origin, channelOf(stream.events), { type: 'call', id: 'p1', method: 'feed/ping', args: {} });
      await ended;
    }
    assert.strictEqual(streams, 2);
    const lines = log.all('compression.failed');
    assert.strictEqual(lines.length, 1, 'two streams failed; the coding is named once');
    assert.deepStrictEqual(
      [lines[0].level, lines[0].component, lines[0].carrier, lines[0].direction, lines[0].codec, lines[0].err.message],
      ['warn', 'sse', 'sse', 'encode', 'x-xor', 'encoder stream broke'],
    );
  });

  it('should validate the option', async () => {
    const { SseChannels } = require('../../src/sse/index.js');
    assert.throws(
      () => new SseChannels({ addClient: () => ({}), compression: 'gzip' }),
      /must be true, false or an options/,
    );
    const gzip = (extra) => ({ encodings: [{ encoding: 'gzip', ...extra }] });
    assert.throws(() => new SseChannels({ addClient: () => ({}), compression: gzip({ level: 'max' }) }), /level/);
    assert.doesNotThrow(() => new SseChannels({ addClient: () => ({}), compression: gzip({ level: 9, memLevel: 9 }) }));
    assert.throws(() => new SseChannels({ addClient: () => ({}), compression: { level: 9 } }), /belong to the coding/);
    // A stream needs a coding that can stream: a one-shot custom encoder is refused here, not at the first GET.
    const oneShot = { encoding: 'x-mine', encode: (b) => b };
    assert.throws(
      () => new SseChannels({ addClient: () => ({}), compression: { encodings: ['gzip', oneShot] } }),
      /x-mine encoding has no createStream\(\)/,
    );
  });
});
