'use strict';

const timers = require('node:timers/promises');
const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

const { Server, WrpcClient, defineRouter, procedure, createEventStream, tracked } = require('../../index.js');
const { SseParser, CHANNEL_HEADER } = require('../../src/sse/index.js');
const { waitFor } = require('../helpers/wait.js');
const { splitChannelRef } = require('../../src/sse/constants.js');

// The `ready` frame: ONE field, `channel`, holding the whole reference a
// client presents back — `<id>.<secret>`. Split here into the id the
// registry is keyed by and the secret, for the tests that present one
// without the other; `ref` is what a client actually holds.
const readyOf = (data) => {
  const frame = JSON.parse(data);
  assert.deepStrictEqual(Object.keys(frame), ['channel'], 'the frame names the reference and nothing else');
  const { id, secret } = splitChannelRef(frame.channel);
  return { channel: id, secret, ref: frame.channel };
};

// ---------------------------------------------------------------------------
// End to end over a real node:http server

const state = { log: [], live: new Set(), closed: 0 };

const router = defineRouter({
  test: {
    hello: procedure({ access: 'public', handler: async (_context, { name }) => `Hello, ${name}` }),
    peekMeta: procedure({
      access: 'public',
      handler: async (context) => ({
        url: context.meta.url,
        hasHeaders: Object.keys(context.meta.headers).length > 0,
        appVersion: context.meta.headers['x-app-version'] ?? null,
      }),
    }),
    fail: procedure({
      access: 'public',
      handler: async () => {
        const error = new Error('Boom');
        error.code = 418;
        throw error;
      },
    }),
    notify: procedure({
      access: 'public',
      handler: async (context) => {
        context.client.sendEvent('test/ping', { ping: true });
        return { ok: true };
      },
    }),
    upload: procedure({
      access: 'public',
      handler: async (context) => context.client.createStream('x', 1) && 'unreachable',
    }),
    messages: procedure.subscription({
      access: 'public',
      handler: async function* (_context, _args, { signal }) {
        const stream = createEventStream({ signal });
        state.live.add(stream);
        try {
          yield* stream;
        } finally {
          state.live.delete(stream);
          state.closed++;
        }
      },
    }),
  },
  auth: {
    // Signing in happens over a PLAIN POST — the one half of this transport
    // that can set a cookie, since a channel's transport looks persistent.
    signin: procedure({
      access: 'public',
      handler: async (context) => {
        context.client.startSession(undefined, { user: 'ada' });
        return { ok: true };
      },
    }),
    // `access` defaults to 'session': this is 403 without one.
    whoami: procedure({ handler: async (context) => context.session.state.user }),
  },
});

const publish = (text) => {
  const id = String(state.log.push(text) - 1);
  for (const stream of state.live) stream.push(tracked(id, text));
};

const createServer = async (options = {}) => {
  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 50 },
    ...options,
  });
  await server.listen();
  return { server, port: server.address().port };
};

// A reader that pumps one event stream into an array, for the tests that
// need to watch what came back rather than drive a whole client.
const readStream = async (url, headers = {}) => {
  const controller = new AbortController();
  const res = await fetch(url, { headers: { accept: 'text/event-stream', ...headers }, signal: controller.signal });
  const events = [];
  const pump = (async () => {
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    const parser = new SseParser();
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        events.push(...parser.push(decoder.decode(value, { stream: true })));
      }
    } catch {
      // aborted
    }
  })();
  return { res, controller, events, pump };
};

describe('SseParser', () => {
  // ---------------------------------------------------------------------------
  // The parser, on its own

  describe('the event-stream format', () => {
    it('should parse a simple event', () => {
      const parser = new SseParser();
      assert.deepStrictEqual(parser.push('data: hello\n\n'), [{ id: null, event: 'message', data: 'hello' }]);
    });

    it('should join fields split across chunks', () => {
      const parser = new SseParser();
      assert.deepStrictEqual(parser.push('id: 7\nda'), []);
      assert.deepStrictEqual(parser.push('ta: {"a":'), []);
      assert.deepStrictEqual(parser.push('1}\n\n'), [{ id: '7', event: 'message', data: '{"a":1}' }]);
    });

    it('should parse several events in one chunk', () => {
      const parser = new SseParser();
      const events = parser.push('data: one\n\ndata: two\n\n');
      assert.deepStrictEqual(
        events.map((event) => event.data),
        ['one', 'two'],
      );
    });

    it('should join multi-line data with newlines', () => {
      const parser = new SseParser();
      assert.deepStrictEqual(parser.push('data: a\ndata: b\n\n')[0].data, 'a\nb');
    });

    it('should ignore comments — that is the proxy heartbeat', () => {
      const parser = new SseParser();
      assert.deepStrictEqual(parser.push(': ping\n\n'), []);
      assert.deepStrictEqual(parser.push(': ping\ndata: real\n\n')[0].data, 'real');
    });

    it("should carry a named event's name", () => {
      const parser = new SseParser();
      assert.deepStrictEqual(parser.push('event: ready\ndata: {}\n\n'), [{ id: null, event: 'ready', data: '{}' }]);
    });

    it('should treat CRLF and bare CR as line endings too', () => {
      const parser = new SseParser();
      assert.deepStrictEqual(parser.push('data: crlf\r\n\r\n')[0].data, 'crlf');
      const split = new SseParser();
      assert.deepStrictEqual(split.push('data: wait\r'), [], 'a trailing CR might still be half of a CRLF');
      assert.deepStrictEqual(split.push('\n\r\n')[0].data, 'wait');
    });

    it('should keep the id until the next one', () => {
      const parser = new SseParser();
      assert.strictEqual(parser.push('id: 1\ndata: a\n\n')[0].id, '1');
      assert.strictEqual(parser.push('data: b\n\n')[0].id, '1', 'the last id stays current');
    });

    it('should ignore an id containing U+0000, per the spec', () => {
      const nul = String.fromCharCode(0);
      const parser = new SseParser();
      assert.strictEqual(parser.push(`id: b${nul}ad\ndata: a\n\n`)[0].id, null);
      assert.strictEqual(parser.push('id: has spaces\ndata: b\n\n')[0].id, 'has spaces', 'only NUL is special');
    });

    it('should handle a field with no value, and a dispatch with no data', () => {
      const parser = new SseParser();
      assert.deepStrictEqual(parser.push('data\n\n'), [{ id: null, event: 'message', data: '' }]);
      assert.deepStrictEqual(parser.push('\n'), [], 'a blank line with nothing buffered dispatches nothing');
      assert.deepStrictEqual(parser.push('event: lonely\n\n'), [], 'no data means no event');
    });

    it('should never throw on random noise', () => {
      const parser = new SseParser();
      const alphabet = ['\n', '\r', ':', ' ', 'a', 'data', 'id', 'event', '{', '}', 'é'];
      let seed = 12345;
      const next = () => {
        seed = (seed * 1103515245 + 12345) % 2147483648;
        return alphabet[seed % alphabet.length];
      };
      for (let i = 0; i < 2000; i++) {
        assert.doesNotThrow(() => parser.push(next()));
      }
    });
  });

  describe('a line that does not end is scanned once, and an event has a cap', () => {
    it('should cost an open line what it weighs, not its square', () => {
      // Each chunk used to be appended and the whole buffer scanned again from
      // its start: 2 MiB in 1 KiB chunks took 6.4 s. A ratio, not an absolute.
      const time = (kib) => {
        const parser = new SseParser();
        const chunk = 'x'.repeat(1024);
        const started = process.hrtime.bigint();
        for (let i = 0; i < kib; i++) parser.push(chunk);
        return Number(process.hrtime.bigint() - started);
      };
      time(64);
      const small = Math.max(time(64), 1);
      const large = time(4096);
      assert.ok(large / small < 64 * 8, `64× the bytes took ${(large / small).toFixed(0)}× the time`);
    });

    it('should make the pieces of a line split across many chunks one line', () => {
      const parser = new SseParser();
      const text = `data: ${'payload-'.repeat(500)}`;
      for (let i = 0; i < text.length; i += 7) assert.deepStrictEqual(parser.push(text.slice(i, i + 7)), []);
      assert.deepStrictEqual(parser.push('\n\n'), [{ id: null, event: 'message', data: 'payload-'.repeat(500) }]);
    });

    it('should end a line at a CR that ends a chunk, the LF behind it being the same line end', () => {
      const parser = new SseParser();
      assert.deepStrictEqual(parser.push('data: a\r'), []);
      assert.deepStrictEqual(parser.push('\ndata: b\r'), []);
      assert.deepStrictEqual(parser.push('\r'), [{ id: null, event: 'message', data: 'a\nb' }]);
      // A CR followed by something else is a line end of its own.
      assert.deepStrictEqual(parser.push('data: c\r'), []);
      assert.deepStrictEqual(parser.push('data: d\n\n')[0].data, 'c\nd');
    });

    it('should error on an event over maxMessage, whether it is one line or many', () => {
      const open = new SseParser({ maxMessage: 1000 });
      assert.throws(() => {
        for (let i = 0; i < 20; i++) open.push('x'.repeat(100));
      }, /SSE event exceeds maxMessage/);
      const lines = new SseParser({ maxMessage: 1000 });
      assert.throws(() => {
        for (let i = 0; i < 20; i++) lines.push(`data: ${'y'.repeat(100)}\n`);
      }, /SSE event exceeds maxMessage/);
      // What is dispatched is forgotten: many events under the cap are fine.
      const fine = new SseParser({ maxMessage: 1000 });
      for (let i = 0; i < 50; i++) assert.strictEqual(fine.push(`data: ${'z'.repeat(500)}\n\n`).length, 1);
    });
  });
});

describe('sse transport', () => {
  describe('a full RPC session over POST + event stream', () => {
    let server = null;
    let client = null;
    after(() => server?.close());
    after(() => void client?.close());
    before(async () => {
      let port = 0;
      ({ server, port } = await createServer());
      client = await WrpcClient.connect(`http://127.0.0.1:${port}/api`, {
        transport: 'sse',
        heartbeat: false,
        headers: { 'X-App-Version': '7.7' },
      });
    });

    it('should send load and call as POSTs and answer them on the stream', async () => {
      await client.load('test');
      assert.strictEqual(await client.api.test.hello({ name: 'SSE' }), 'Hello, SSE');
    });

    it('should keep the code of errors', async () => {
      await assert.rejects(client.api.test.fail(), (error) => error.code === 418);
    });

    it('should carry in the channel client the meta of the GET that opened it', async () => {
      const meta = await client.call('test/peekMeta');
      assert.ok(meta.url.includes('/api/events'), `meta.url is the opening GET's (got '${meta.url}')`);
      assert.strictEqual(meta.hasHeaders, true);
      assert.strictEqual(meta.appVersion, '7.7', 'declared headers ride the stream GET as real ones');
    });

    it('should bring server events to the stream', async () => {
      const ping = new Promise((resolve) => client.api.test.once('ping', resolve));
      assert.deepStrictEqual(await client.api.test.notify(), { ok: true });
      assert.deepStrictEqual(await ping, { ping: true });
    });

    it('should open subscriptions by a POST and deliver them on the stream', async () => {
      const seen = [];
      const handle = client.api.test.messages.subscribe({}, { onData: (data) => seen.push(data) });
      await waitFor(() => state.live.size === 1, 'the subscription never opened');
      publish('first');
      publish('second');
      await waitFor(() => seen.length === 2, `only ${seen.length} values arrived`);
      assert.deepStrictEqual(seen, ['first', 'second']);
      assert.strictEqual(handle.lastEventId, '1');
      handle.unsubscribe();
      await waitFor(() => state.live.size === 0, 'the unsubscribe never reached the server');
    });

    it('should refuse binary streams rather than corrupt them', async () => {
      await assert.rejects(client.api.test.upload(), (error) => /text-only/.test(error.message));
    });

    it('should round-trip the app-level heartbeat over POST and stream', async () => {
      // The ping goes out as a POST, the pong comes back on the stream — which
      // is the only liveness signal this transport has.
      const beat = await new Promise((resolve) => {
        const socketClient = client;
        socketClient.send({ type: 'ping' });
        setTimeout(() => resolve(socketClient.active), 60);
      });
      assert.strictEqual(beat, true);
    });
  });

  describe('the channel is what ties the two halves together', () => {
    let server = null;
    let base = '';
    after(() => server?.close());
    before(async () => {
      let port = 0;
      ({ server, port } = await createServer());
      base = `http://127.0.0.1:${port}/api`;
    });

    let channelId = null;
    let secret = null;

    it('should answer 409 to a POST with an unknown channel', async () => {
      const res = await fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: 'nope' },
        body: JSON.stringify({ type: 'call', id: '1', method: 'test/hello', args: { name: 'x' } }),
      });
      const body = await res.json();
      assert.strictEqual(res.status, 409);
      assert.strictEqual(body.error.code, 409);
    });

    it('should answer 409 to a client-proposed id too — the server mints them', async () => {
      const res = await fetch(`${base}/events?channel=probe`, { headers: { accept: 'text/event-stream' } });
      assert.strictEqual(res.status, 409);
      await res.body?.cancel?.();
    });

    it('should mint the channel id on the stream and set streaming headers', async (t) => {
      const controller = new AbortController();
      t.after(() => controller.abort());
      const res = await fetch(`${base}/events`, {
        headers: { accept: 'text/event-stream' },
        signal: controller.signal,
      });
      assert.strictEqual(res.status, 200);
      assert.strictEqual(res.headers.get('content-type'), 'text/event-stream');
      assert.strictEqual(res.headers.get('cache-control'), 'no-cache, no-transform');
      assert.strictEqual(res.headers.get('x-accel-buffering'), 'no');

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      const parser = new SseParser();
      const events = [];
      while (events.length === 0) {
        const { value } = await reader.read();
        events.push(...parser.push(decoder.decode(value, { stream: true })));
      }
      assert.strictEqual(events[0].event, 'ready');
      const ready = readyOf(events[0].data);
      channelId = ready.channel;
      secret = ready.secret;
      assert.match(channelId, /^[0-9a-f][0-9a-f-]{34}[0-9a-f]$/, 'the id comes from the server, not the request');
      assert.match(secret, /^[A-Za-z0-9_-]{24}$/, 'and so does the secret: 18 bytes, base64url');
      assert.notStrictEqual(secret, channelId);
      assert.strictEqual(server.rpc.sse.size, 1);
    });

    it('should answer 409 to the id alone — the secret is the credential, and its absence is not told apart', async () => {
      const post = (ref, id) =>
        fetch(base, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: ref },
          body: JSON.stringify({ type: 'call', id, method: 'test/hello', args: { name: 'x' } }),
        });
      assert.strictEqual((await post(channelId, 'n1')).status, 409, 'the id alone');
      assert.strictEqual((await post(`${channelId}.`, 'n2')).status, 409, 'an empty secret');
      assert.strictEqual((await post(`${channelId}.${'A'.repeat(24)}`, 'n3')).status, 409, 'a wrong one');
      assert.strictEqual((await post(`${channelId}.${secret.slice(1)}`, 'n4')).status, 409, 'a short one');
      const reattach = await fetch(`${base}/events?channel=${encodeURIComponent(channelId)}`, {
        headers: { accept: 'text/event-stream', 'last-event-id': '-1' },
      });
      assert.strictEqual(reattach.status, 409, 'a replay without the secret');
      await reattach.body?.cancel?.();
      assert.strictEqual(server.rpc.sse.size, 1, 'the channel is untouched');
    });

    it('should answer 409 to a secret as long in characters but not in bytes, on every path that presents one', async () => {
      // `é` is one character and two UTF-8 bytes: the comparison used to
      // measure characters and hand bytes to timingSafeEqual, which threw —
      // a rejection nobody caught and a request nobody answered.
      const wide = `${channelId}.${'é'.repeat(secret.length)}`;
      const byQuery = await fetch(`${base}/events?channel=${encodeURIComponent(wide)}`, {
        headers: { accept: 'text/event-stream' },
      });
      assert.strictEqual(byQuery.status, 409, 'the query');
      await byQuery.body?.cancel?.();
      const byHeader = await fetch(`${base}/events`, {
        headers: { accept: 'text/event-stream', [CHANNEL_HEADER]: wide },
      });
      assert.strictEqual(byHeader.status, 409, 'the header');
      await byHeader.body?.cancel?.();
      const byPost = await fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: wide },
        body: JSON.stringify({ type: 'call', id: 'w1', method: 'test/hello', args: { name: 'x' } }),
      });
      assert.strictEqual(byPost.status, 409, 'a channel POST');
      assert.strictEqual(server.rpc.sse.size, 1, 'the channel is untouched');
    });

    it('should answer 202 to a POST on that channel and reply on the stream', async () => {
      const res = await fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: `${channelId}.${secret}` },
        body: JSON.stringify({ type: 'call', id: 'c1', method: 'test/hello', args: { name: 'Channel' } }),
      });
      assert.strictEqual(res.status, 202);
      assert.strictEqual((await res.text()).length, 0, 'the answer travels on the stream, not here');
    });
  });

  it('should not let a guessable channel id hijack — the application counts, the server draws the secret', async (t) => {
    // A counter is what the production guide offers as a generateId; it
    // used to make every SSE channel opened without a cookie or a bearer the
    // next attacker's, together with the session it signed into.
    let n = 0;
    const { server, port } = await createServer({ generateId: () => `id-${++n}` });
    t.after(() => server.close());
    const base = `http://127.0.0.1:${port}/api`;
    const victim = await readStream(`${base}/events`);
    t.after(() => victim.controller.abort());
    await waitFor(() => victim.events.some((event) => event.event === 'ready'), 'the ready event never arrived');
    const ready = readyOf(victim.events.find((event) => event.event === 'ready').data);
    assert.match(ready.channel, /^id-\d+$/, "the id is the application's — a counter here");
    const post = (ref, id) =>
      fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: ref },
        body: JSON.stringify({ type: 'call', id, method: 'test/hello', args: { name: 'x' } }),
      });
    // The attacker counts too.
    assert.strictEqual((await post(ready.channel, 'a1')).status, 409);
    assert.strictEqual((await post(`${ready.channel}.${'B'.repeat(24)}`, 'a2')).status, 409);
    const replay = await fetch(`${base}/events?channel=${encodeURIComponent(ready.channel)}`, {
      headers: { accept: 'text/event-stream', 'last-event-id': '-1' },
    });
    assert.strictEqual(replay.status, 409);
    await replay.body?.cancel?.();
    assert.strictEqual((await post(`${ready.channel}.${ready.secret}`, 'v1')).status, 202, 'the holder');
    await waitFor(() => victim.events.some((event) => event.event === 'message'), 'the holder was not answered');
    assert.strictEqual(victim.events.filter((event) => event.event === 'message').length, 1, 'only the holder');
  });

  it('should replay with Last-Event-ID what the dropped stream missed', async (t) => {
    const { server, port } = await createServer({ sse: { heartbeat: 0, retention: 5000 } });
    t.after(() => server.close());
    const base = `http://127.0.0.1:${port}/api`;

    const open = async (channel = null, headers = {}) => {
      const controller = new AbortController();
      const query = channel ? `?channel=${encodeURIComponent(channel)}` : '';
      const res = await fetch(`${base}/events${query}`, {
        headers: { accept: 'text/event-stream', ...headers },
        signal: controller.signal,
      });
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      const parser = new SseParser();
      const events = [];
      const pump = (async () => {
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            events.push(...parser.push(decoder.decode(value, { stream: true })));
          }
        } catch {
          // aborted
        }
      })();
      return { controller, events, pump };
    };

    const first = await open();
    await waitFor(() => first.events.length >= 1, 'the ready event never arrived');
    const ready = readyOf(first.events[0].data);
    const keep = `${ready.channel}.${ready.secret}`;
    await fetch(base, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: keep },
      body: JSON.stringify({ type: 'call', id: 'r1', method: 'test/hello', args: { name: 'One' } }),
    });
    await waitFor(() => first.events.length >= 2, 'the answer never arrived');
    const answer = first.events.at(-1);
    assert.strictEqual(JSON.parse(answer.data).result, 'Hello, One');

    // The stream drops; the channel (and its client) is held for `retention`.
    first.controller.abort();
    await first.pump;
    await timers.setTimeout(30);
    assert.strictEqual(server.rpc.sse.size, 1, 'a blip must not destroy the channel');

    // A second answer produced while nobody is attached goes into the buffer.
    await fetch(base, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: keep },
      body: JSON.stringify({ type: 'call', id: 'r2', method: 'test/hello', args: { name: 'Two' } }),
    });
    await timers.setTimeout(30);

    const second = await open(keep, { 'last-event-id': answer.id });
    t.after(() => second.controller.abort());
    await waitFor(() => second.events.some((event) => event.data.includes('Two')), 'the replay never arrived');
    const replayed = second.events.filter((event) => event.event === 'message').map((event) => JSON.parse(event.data));
    assert.deepStrictEqual(
      replayed.map((packet) => packet.result),
      ['Hello, Two'],
      'only what was missed is replayed, not what was already seen',
    );
  });

  describe('a channel restores the session its GET arrived with', () => {
    let server = null;
    let base = '';
    let token = '';
    after(() => server?.close());
    before(async () => {
      let port = 0;
      ({ server, port } = await createServer());
      base = `http://127.0.0.1:${port}/api`;

      const login = await fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'call', id: 'in', method: 'auth/signin', args: {} }),
      });
      const [cookie] = login.headers.getSetCookie();
      token = cookie.match(/^token=([^;]+)/)[1];
    });

    const openChannel = async (headers = {}) => {
      const stream = await readStream(`${base}/events`, headers);
      await waitFor(() => stream.events.some((event) => event.event === 'ready'), 'the ready event never arrived');
      const ready = readyOf(stream.events.find((event) => event.event === 'ready').data);
      // What a request presents: the id and the secret, in one header.
      stream.channel = `${ready.channel}.${ready.secret}`;
      return stream;
    };

    const call = async (channel, id, method, headers = {}) => {
      const res = await fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: channel, ...headers },
        body: JSON.stringify({ type: 'call', id, method, args: {} }),
      });
      assert.strictEqual(res.status, 202);
    };

    it('should sign the channel in with a cookie on the stream', async (t) => {
      const stream = await openChannel({ cookie: `token=${token}` });
      t.after(() => stream.controller.abort());
      await call(stream.channel, 'w1', 'auth/whoami', { cookie: `token=${token}` });
      await waitFor(() => stream.events.some((event) => event.event === 'message'), 'nothing came back');
      const answer = JSON.parse(stream.events.find((event) => event.event === 'message').data);
      assert.strictEqual(answer.error, undefined, 'a valid cookie must not have to sign in again per channel');
      assert.strictEqual(answer.result, 'ada');
    });

    it('should leave the channel anonymous without one, and a session call is 403', async (t) => {
      const stream = await openChannel();
      t.after(() => stream.controller.abort());
      await call(stream.channel, 'w2', 'auth/whoami');
      await waitFor(() => stream.events.some((event) => event.event === 'message'), 'nothing came back');
      const answer = JSON.parse(stream.events.find((event) => event.event === 'message').data);
      assert.strictEqual(answer.error.code, 403);
    });

    it('should answer 403 to a POST without the binding cookie — the id is not a bearer token', async (t) => {
      const stream = await openChannel({ cookie: `token=${token}` });
      t.after(() => stream.controller.abort());
      const res = await fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: stream.channel },
        body: JSON.stringify({ type: 'call', id: 'w3', method: 'auth/whoami', args: {} }),
      });
      assert.strictEqual(res.status, 403);
      assert.strictEqual((await res.json()).error.code, 403);
    });

    it('should answer 403 to a re-attach without the binding cookie too', async () => {
      const stream = await openChannel({ cookie: `token=${token}` });
      stream.controller.abort();
      await stream.pump;
      const res = await fetch(`${base}/events?channel=${encodeURIComponent(stream.channel)}`, {
        headers: { accept: 'text/event-stream' },
      });
      assert.strictEqual(res.status, 403);
      await res.body?.cancel?.();
    });
  });

  describe('channel creation is capped', () => {
    it('should cap per address', async (t) => {
      const { server, port } = await createServer({ sse: { maxChannelsPerAddress: 2 } });
      t.after(() => server.close());
      const base = `http://127.0.0.1:${port}/api`;
      const first = await readStream(`${base}/events`);
      t.after(() => first.controller.abort());
      const second = await readStream(`${base}/events`);
      t.after(() => second.controller.abort());
      const res = await fetch(`${base}/events`, { headers: { accept: 'text/event-stream' } });
      assert.strictEqual(res.status, 429);
      await res.body?.cancel?.();
    });

    it('should cap globally', async (t) => {
      const { server, port } = await createServer({ sse: { maxChannels: 1 } });
      t.after(() => server.close());
      const base = `http://127.0.0.1:${port}/api`;
      const only = await readStream(`${base}/events`);
      t.after(() => only.controller.abort());
      const res = await fetch(`${base}/events`, { headers: { accept: 'text/event-stream' } });
      assert.strictEqual(res.status, 503);
      await res.body?.cancel?.();
    });
  });

  describe('a cross-origin channel is granted, not silently blocked', () => {
    const origin = 'https://app.example';
    const cors = { origins: [origin], credentials: true };
    let server = null;
    let base = '';
    after(() => server?.close());
    before(async () => {
      let port = 0;
      ({ server, port } = await createServer({ cors }));
      base = `http://127.0.0.1:${port}/api`;
    });

    it('should allow in the preflight the headers this transport actually sends', async () => {
      const res = await fetch(base, {
        method: 'OPTIONS',
        headers: { origin, 'access-control-request-method': 'POST', 'access-control-request-headers': CHANNEL_HEADER },
      });
      assert.strictEqual(res.status, 200);
      const allowed = res.headers.get('access-control-allow-headers').toLowerCase();
      // Neither is CORS-safelisted, so a browser never sends the request at all
      // unless the preflight names them.
      assert.match(allowed, /x-wrpc-channel/);
      assert.match(allowed, /last-event-id/);
      assert.strictEqual(res.headers.get('access-control-allow-origin'), origin);
    });

    it('should make the stream and the POST that feeds it both readable', async (t) => {
      const stream = await readStream(`${base}/events`, { origin });
      t.after(() => stream.controller.abort());
      assert.strictEqual(stream.res.headers.get('access-control-allow-origin'), origin);
      assert.strictEqual(stream.res.headers.get('access-control-allow-credentials'), 'true');
      await waitFor(() => stream.events.some((event) => event.event === 'ready'), 'the ready event never arrived');
      const ready = readyOf(stream.events.find((event) => event.event === 'ready').data);
      const channel = `${ready.channel}.${ready.secret}`;

      const res = await fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', origin, [CHANNEL_HEADER]: channel },
        body: JSON.stringify({ type: 'call', id: 'x1', method: 'test/hello', args: { name: 'Cross' } }),
      });
      assert.strictEqual(res.status, 202);
      assert.strictEqual(res.headers.get('access-control-allow-origin'), origin, 'the 202 is unreadable without this');
      assert.strictEqual(res.headers.get('access-control-allow-credentials'), 'true');
      await waitFor(() => stream.events.some((event) => event.event === 'message'), 'the answer never arrived');
    });

    it('should refuse an unknown channel in a way the page can read', async () => {
      const res = await fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', origin, [CHANNEL_HEADER]: 'gone' },
        body: JSON.stringify({ type: 'call', id: 'x2', method: 'test/hello', args: { name: 'x' } }),
      });
      assert.strictEqual(res.status, 409);
      assert.strictEqual(res.headers.get('access-control-allow-origin'), origin);
    });

    it('should refuse a disallowed origin outright, not merely deny it the headers', async () => {
      const res = await fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', origin: 'https://evil.example' },
        body: JSON.stringify({ type: 'call', id: 'x3', method: 'test/hello', args: { name: 'x' } }),
      });
      assert.strictEqual(res.status, 403, 'the call must not run for a disallowed origin');
      assert.strictEqual(res.headers.get('access-control-allow-origin'), null);
      assert.strictEqual(res.headers.get('vary'), 'Origin');
    });
  });

  it('should say so on a host that cannot stream', async (t) => {
    const { server } = await createServer();
    t.after(() => server.close());
    // The abstract call the core consumes; a host with no `stream` is exactly
    // what an adapter that cannot keep a response open looks like.
    const answers = [];
    await server.rpc.handleHttpCall({
      method: 'GET',
      url: '/api/events',
      headers: {},
      respond: (response) => answers.push(response),
    });
    assert.strictEqual(answers[0].status, 501);
    assert.match(JSON.parse(answers[0].body).error.message, /unsupported/i);
  });

  it('should give a resume past the replay buffer an honest gap frame', async (t) => {
    const { server, port } = await createServer({ sse: { heartbeat: 0, retention: 5000, replay: 2 } });
    t.after(() => server.close());
    const base = `http://127.0.0.1:${port}/api`;

    const first = await readStream(`${base}/events`);
    await waitFor(() => first.events.some((event) => event.event === 'ready'), 'ready never arrived');
    const ready = readyOf(first.events.find((event) => event.event === 'ready').data);
    const channel = `${ready.channel}.${ready.secret}`;

    const call = (id) =>
      fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: channel },
        body: JSON.stringify({ type: 'call', id, method: 'test/hello', args: { name: id } }),
      });
    await call('g1');
    await waitFor(() => first.events.some((event) => event.event === 'message'), 'the first answer never arrived');
    const seenId = first.events.find((event) => event.event === 'message').id;
    first.controller.abort();
    await first.pump;

    // Three more answers while nobody is attached: with replay=2 the first of
    // them falls out of the buffer, so resuming from `seenId` cannot be honest.
    await call('g2');
    await call('g3');
    await call('g4');
    await timers.setTimeout(30);

    const second = await readStream(`${base}/events?channel=${encodeURIComponent(channel)}`, {
      'last-event-id': seenId,
    });
    t.after(() => second.controller.abort());
    await waitFor(() => second.events.some((event) => event.event === 'gap'), 'the gap frame never arrived');
    const gap = second.events.find((event) => event.event === 'gap');
    assert.ok(JSON.parse(gap.data).oldest > Number(seenId), 'the gap names what IS still available');
    assert.ok(
      !second.events.some((event) => event.event === 'message'),
      'a resume that cannot be complete must not silently replay a truncated history',
    );
  });

  it('should cap the replay buffer by bytes, not only frames', async (t) => {
    const { server, port } = await createServer({ sse: { heartbeat: 0, replay: 100, replayBytes: 512 } });
    t.after(() => server.close());
    const base = `http://127.0.0.1:${port}/api`;

    const stream = await readStream(`${base}/events`);
    t.after(() => stream.controller.abort());
    await waitFor(() => stream.events.some((event) => event.event === 'ready'), 'ready never arrived');
    const ready = readyOf(stream.events.find((event) => event.event === 'ready').data);

    // Each answer is ~300 bytes; after several the byte budget must evict old
    // frames long before the 100-frame cap would.
    for (let i = 0; i < 5; i++) {
      await fetch(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: `${ready.channel}.${ready.secret}` },
        body: JSON.stringify({ type: 'call', id: `b${i}`, method: 'test/hello', args: { name: 'x'.repeat(200) } }),
      });
    }
    await waitFor(() => stream.events.filter((event) => event.event === 'message').length === 5, 'answers arrived');
    const held = server.rpc.sse.get(ready.channel);
    assert.ok(held.bytes <= 512, `the buffer holds ${held.bytes} bytes, over the 512 budget`);
    // `count` is the ring's live-entry counter (buffer.length is the ring's
    // preallocated capacity, constant by design).
    assert.ok(held.count < 5, 'older frames were evicted by the byte budget');
  });
});
