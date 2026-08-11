'use strict';

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { Server, WrpcClient, defineRouter, procedure, createEventStream, tracked } = require('../../index.js');
const { SseParser, CHANNEL_HEADER } = require('../../sse.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 300; i++) {
    if (predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

// ---------------------------------------------------------------------------
// The parser, on its own

test('SseParser: the event-stream format', async (t) => {
  await t.test('a simple event', () => {
    const parser = new SseParser();
    assert.deepStrictEqual(parser.push('data: hello\n\n'), [{ id: null, event: 'message', data: 'hello' }]);
  });

  await t.test('fields split across chunks', () => {
    const parser = new SseParser();
    assert.deepStrictEqual(parser.push('id: 7\nda'), []);
    assert.deepStrictEqual(parser.push('ta: {"a":'), []);
    assert.deepStrictEqual(parser.push('1}\n\n'), [{ id: '7', event: 'message', data: '{"a":1}' }]);
  });

  await t.test('several events in one chunk', () => {
    const parser = new SseParser();
    const events = parser.push('data: one\n\ndata: two\n\n');
    assert.deepStrictEqual(
      events.map((event) => event.data),
      ['one', 'two'],
    );
  });

  await t.test('multi-line data joins with newlines', () => {
    const parser = new SseParser();
    assert.deepStrictEqual(parser.push('data: a\ndata: b\n\n')[0].data, 'a\nb');
  });

  await t.test('comments are ignored — that is the proxy heartbeat', () => {
    const parser = new SseParser();
    assert.deepStrictEqual(parser.push(': ping\n\n'), []);
    assert.deepStrictEqual(parser.push(': ping\ndata: real\n\n')[0].data, 'real');
  });

  await t.test('a named event carries its name', () => {
    const parser = new SseParser();
    assert.deepStrictEqual(parser.push('event: ready\ndata: {}\n\n'), [{ id: null, event: 'ready', data: '{}' }]);
  });

  await t.test('CRLF and bare CR are line endings too', () => {
    const parser = new SseParser();
    assert.deepStrictEqual(parser.push('data: crlf\r\n\r\n')[0].data, 'crlf');
    const split = new SseParser();
    assert.deepStrictEqual(split.push('data: wait\r'), [], 'a trailing CR might still be half of a CRLF');
    assert.deepStrictEqual(split.push('\n\r\n')[0].data, 'wait');
  });

  await t.test('the id sticks until the next one', () => {
    const parser = new SseParser();
    assert.strictEqual(parser.push('id: 1\ndata: a\n\n')[0].id, '1');
    assert.strictEqual(parser.push('data: b\n\n')[0].id, '1', 'the last id stays current');
  });

  await t.test('an id containing U+0000 is ignored, per the spec', () => {
    const nul = String.fromCharCode(0);
    const parser = new SseParser();
    assert.strictEqual(parser.push(`id: b${nul}ad\ndata: a\n\n`)[0].id, null);
    assert.strictEqual(parser.push('id: has spaces\ndata: b\n\n')[0].id, 'has spaces', 'only NUL is special');
  });

  await t.test('a field with no value, and a dispatch with no data', () => {
    const parser = new SseParser();
    assert.deepStrictEqual(parser.push('data\n\n'), [{ id: null, event: 'message', data: '' }]);
    assert.deepStrictEqual(parser.push('\n'), [], 'a blank line with nothing buffered dispatches nothing');
    assert.deepStrictEqual(parser.push('event: lonely\n\n'), [], 'no data means no event');
  });

  await t.test('random noise never throws', () => {
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

// ---------------------------------------------------------------------------
// End to end over a real node:http server

const state = { log: [], live: new Set(), closed: 0 };

const router = defineRouter({
  test: {
    hello: procedure({ access: 'public', handler: async (_context, { name }) => `Hello, ${name}` }),
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
    console: quiet,
    timeouts: { bind: 50 },
    ...options,
  });
  await server.listen();
  return { server, port: server.address().port };
};

test('sse: a full RPC session over POST + event stream', async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());
  const client = await WrpcClient.connect(`http://127.0.0.1:${port}/api`, {
    transport: 'sse',
    heartbeat: false,
  });
  t.after(() => void client.close());

  await t.test('load and call go out as POSTs and answer on the stream', async () => {
    await client.load('test');
    assert.strictEqual(await client.api.test.hello({ name: 'SSE' }), 'Hello, SSE');
  });

  await t.test('errors keep their code', async () => {
    await assert.rejects(client.api.test.fail(), (error) => error.code === 418);
  });

  await t.test('server events reach the stream', async () => {
    const ping = new Promise((resolve) => client.api.test.once('ping', resolve));
    assert.deepStrictEqual(await client.api.test.notify(), { ok: true });
    assert.deepStrictEqual(await ping, { ping: true });
  });

  await t.test('subscriptions work: opened by a POST, delivered on the stream', async () => {
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

  await t.test('binary streams are refused rather than corrupted', async () => {
    await assert.rejects(client.api.test.upload(), (error) => /text-only/.test(error.message));
  });

  await t.test('the app-level heartbeat round-trips over POST and stream', async () => {
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

test('sse: the channel is what ties the two halves together', async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());
  const base = `http://127.0.0.1:${port}/api`;

  await t.test('a POST with an unknown channel is a 404', async () => {
    const res = await fetch(base, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: 'nope' },
      body: JSON.stringify({ type: 'call', id: '1', method: 'test/hello', args: { name: 'x' } }),
    });
    const body = await res.json();
    assert.strictEqual(res.status, 404);
    assert.strictEqual(body.error.code, 404);
  });

  await t.test('the stream announces its channel and sets streaming headers', async () => {
    const controller = new AbortController();
    t.after(() => controller.abort());
    const res = await fetch(`${base}/events?channel=probe`, {
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
    assert.deepStrictEqual(JSON.parse(events[0].data), { channel: 'probe' });
    assert.strictEqual(server.rpc.sse.size, 1);
  });

  await t.test('a POST on that channel answers 202 and replies on the stream', async () => {
    const res = await fetch(base, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: 'probe' },
      body: JSON.stringify({ type: 'call', id: 'c1', method: 'test/hello', args: { name: 'Channel' } }),
    });
    assert.strictEqual(res.status, 202);
    assert.strictEqual((await res.text()).length, 0, 'the answer travels on the stream, not here');
  });
});

test('sse: Last-Event-ID replays what the dropped stream missed', async (t) => {
  const { server, port } = await createServer({ sse: { heartbeat: 0, retention: 5000 } });
  t.after(() => server.close());
  const base = `http://127.0.0.1:${port}/api`;

  const open = async (headers = {}) => {
    const controller = new AbortController();
    const res = await fetch(`${base}/events?channel=keep`, {
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
  await fetch(base, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: 'keep' },
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
    headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: 'keep' },
    body: JSON.stringify({ type: 'call', id: 'r2', method: 'test/hello', args: { name: 'Two' } }),
  });
  await timers.setTimeout(30);

  const second = await open({ 'last-event-id': answer.id });
  t.after(() => second.controller.abort());
  await waitFor(() => second.events.some((event) => event.data.includes('Two')), 'the replay never arrived');
  const replayed = second.events.filter((event) => event.event === 'message').map((event) => JSON.parse(event.data));
  assert.deepStrictEqual(
    replayed.map((packet) => packet.result),
    ['Hello, Two'],
    'only what was missed is replayed, not what was already seen',
  );
});

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

test('sse: a channel restores the session its GET arrived with', async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());
  const base = `http://127.0.0.1:${port}/api`;

  const login = await fetch(base, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'call', id: 'in', method: 'auth/signin', args: {} }),
  });
  const [cookie] = login.headers.getSetCookie();
  const token = cookie.match(/^token=([^;]+)/)[1];

  const call = async (channel, id, method) => {
    const res = await fetch(base, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', [CHANNEL_HEADER]: channel },
      body: JSON.stringify({ type: 'call', id, method, args: {} }),
    });
    assert.strictEqual(res.status, 202);
  };

  await t.test('a cookie on the stream signs the channel in', async () => {
    const stream = await readStream(`${base}/events?channel=signed`, { cookie: `token=${token}` });
    t.after(() => stream.controller.abort());
    await call('signed', 'w1', 'auth/whoami');
    await waitFor(() => stream.events.some((event) => event.event === 'message'), 'nothing came back');
    const answer = JSON.parse(stream.events.find((event) => event.event === 'message').data);
    assert.strictEqual(answer.error, undefined, 'a valid cookie must not have to sign in again per channel');
    assert.strictEqual(answer.result, 'ada');
  });

  await t.test('without one the channel is anonymous and a session call is 403', async () => {
    const stream = await readStream(`${base}/events?channel=bare`);
    t.after(() => stream.controller.abort());
    await call('bare', 'w2', 'auth/whoami');
    await waitFor(() => stream.events.some((event) => event.event === 'message'), 'nothing came back');
    const answer = JSON.parse(stream.events.find((event) => event.event === 'message').data);
    assert.strictEqual(answer.error.code, 403);
  });
});

test('sse: a cross-origin channel is granted, not silently blocked', async (t) => {
  const origin = 'https://app.example';
  const cors = { origins: [origin], credentials: true };
  const { server, port } = await createServer({ cors });
  t.after(() => server.close());
  const base = `http://127.0.0.1:${port}/api`;

  await t.test('the preflight allows the headers this transport actually sends', async () => {
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

  await t.test('the stream and the POST that feeds it are both readable', async () => {
    const stream = await readStream(`${base}/events?channel=cross`, { origin });
    t.after(() => stream.controller.abort());
    assert.strictEqual(stream.res.headers.get('access-control-allow-origin'), origin);
    assert.strictEqual(stream.res.headers.get('access-control-allow-credentials'), 'true');

    const res = await fetch(base, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', origin, [CHANNEL_HEADER]: 'cross' },
      body: JSON.stringify({ type: 'call', id: 'x1', method: 'test/hello', args: { name: 'Cross' } }),
    });
    assert.strictEqual(res.status, 202);
    assert.strictEqual(res.headers.get('access-control-allow-origin'), origin, 'the 202 is unreadable without this');
    assert.strictEqual(res.headers.get('access-control-allow-credentials'), 'true');
    await waitFor(() => stream.events.some((event) => event.event === 'message'), 'the answer never arrived');
  });

  await t.test('an unknown channel refuses in a way the page can read', async () => {
    const res = await fetch(base, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', origin, [CHANNEL_HEADER]: 'gone' },
      body: JSON.stringify({ type: 'call', id: 'x2', method: 'test/hello', args: { name: 'x' } }),
    });
    assert.strictEqual(res.status, 404);
    assert.strictEqual(res.headers.get('access-control-allow-origin'), origin);
  });

  await t.test('a disallowed origin is still not granted one', async () => {
    const res = await fetch(base, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', origin: 'https://evil.example', [CHANNEL_HEADER]: 'cross' },
      body: JSON.stringify({ type: 'call', id: 'x3', method: 'test/hello', args: { name: 'x' } }),
    });
    assert.strictEqual(res.headers.get('access-control-allow-origin'), null);
    assert.strictEqual(res.headers.get('vary'), 'Origin');
  });
});

test('sse: a host that cannot stream says so', async (t) => {
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
