'use strict';

const http = require('node:http');
const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { WebsocketServer } = require('#ws');
const { Server, WrpcClient, defineRouter, procedure } = require('../index.js');
const { backoffDelay, jsonParse } = require('../src/utils.js');

const noop = () => {};

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 100; i++) {
    if (predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

const router = (extra = {}) =>
  defineRouter({
    test: {
      hello: procedure({ access: 'public', handler: async () => 'hi' }),
      notify: procedure({
        access: 'public',
        handler: async (context, { name = 'test/ping' } = {}) => {
          context.client.sendEvent(name, { ping: true });
          return { ok: true };
        },
      }),
      ...extra,
    },
  });

const createServer = async (definition, port = 0) => {
  const server = new Server({
    router: definition,
    host: '127.0.0.1',
    port,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 50 },
    retry: 20,
  });
  await server.listen();
  return { server, port: server.address().port };
};

// ---------------------------------------------------------------------------
// The backoff schedule, as a pure function

test('backoffDelay: truncated exponential growth with full jitter', async (t) => {
  const schedule = { minDelay: 100, maxDelay: 1000, factor: 2 };

  await t.test('without jitter the window itself is the delay', () => {
    const delays = [0, 1, 2, 3, 4].map((attempt) => backoffDelay({ ...schedule, attempt, jitter: false }));
    assert.deepStrictEqual(delays, [100, 200, 400, 800, 1000], 'growth is capped by maxDelay');
  });

  await t.test('full jitter spreads the delay over the whole window', () => {
    assert.strictEqual(backoffDelay({ ...schedule, attempt: 2, random: () => 0 }), 0);
    assert.strictEqual(backoffDelay({ ...schedule, attempt: 2, random: () => 1 }), 400);
    assert.strictEqual(backoffDelay({ ...schedule, attempt: 2, random: () => 0.5 }), 200);
  });

  await t.test('a real random source stays inside the window', () => {
    for (let attempt = 0; attempt < 8; attempt++) {
      const window = Math.min(schedule.maxDelay, schedule.minDelay * 2 ** attempt);
      for (let i = 0; i < 100; i++) {
        const delay = backoffDelay({ ...schedule, attempt });
        assert.ok(delay >= 0 && delay <= window, `delay ${delay} outside [0, ${window}]`);
      }
    }
  });

  await t.test('a huge attempt count cannot overflow past the cap', () => {
    assert.strictEqual(backoffDelay({ ...schedule, attempt: 5000, jitter: false }), 1000);
    assert.strictEqual(backoffDelay({ minDelay: 100, maxDelay: 1000, factor: 0, attempt: 3, jitter: false }), 100);
  });

  await t.test('defaults: factor 2 and jitter on', () => {
    const delay = backoffDelay({ minDelay: 100, maxDelay: 1000, attempt: 1, random: () => 1 });
    assert.strictEqual(delay, 200);
  });
});

test('reconnect: the reconnectTimeout shorthand keeps the delay it asked for', async (t) => {
  const { server, port } = await createServer(router());
  t.after(() => server.close());
  const url = `ws://127.0.0.1:${port}/api`;

  // A shorthand above the default 30 s cap used to be silently capped back
  // down to 30 s, reconnecting FASTER than asked.
  const shorthand = await WrpcClient.connect(url, { reconnectTimeout: 60_000, random: () => 1, heartbeat: false });
  const explicit = await WrpcClient.connect(url, {
    reconnect: { minDelay: 60_000 },
    random: () => 1,
    heartbeat: false,
  });
  t.after(() => {
    shorthand.close();
    explicit.close();
  });

  const first = new Promise((resolve) => shorthand.once('reconnecting', resolve));
  const second = new Promise((resolve) => explicit.once('reconnecting', resolve));
  await server.close();

  assert.strictEqual((await first).delay, 60_000);
  assert.strictEqual((await second).delay, 60_000, 'both spellings mean the same thing');
});

test('reconnect: a socket abandoned by terminate() cannot close its replacement', async (t) => {
  const { server, port } = await createServer(router());
  t.after(() => server.close());

  const { ws: WsTransport } = WrpcClient.transport;
  const transport = new WsTransport(`ws://127.0.0.1:${port}/api`);
  t.after(() => transport.close());
  await transport.open();

  const closes = [];
  transport.on('close', () => void closes.push(Date.now()));

  // terminate() reports the close and walks away from a still-live socket —
  // which is what a heartbeat timeout does. The reconnect then installs a new
  // socket while the old one is still finishing its close handshake.
  transport.terminate();
  assert.strictEqual(closes.length, 1);
  assert.strictEqual(transport.active, false);

  await transport.open();
  assert.strictEqual(transport.active, true);

  await timers.setTimeout(150); // the abandoned socket's own 'close' lands here
  assert.strictEqual(transport.active, true, 'the abandoned socket must not close its replacement');
  assert.strictEqual(closes.length, 1, 'and must not report a close it does not own');
});

// ---------------------------------------------------------------------------
// Reconnect

test('reconnect: the backoff schedule drives the retries and then gives up', async (t) => {
  const { server, port } = await createServer(router());
  const delays = [];
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    reconnect: { minDelay: 10, maxDelay: 40, factor: 2, retries: 4 },
    random: () => 1, // pin the jitter to the top of the window
    heartbeat: false,
  });
  t.after(() => void client.close());
  client.on('error', noop); // connection refusals are expected here
  client.on('reconnecting', ({ delay }) => delays.push(delay));
  const exhausted = new Promise((resolve) => client.once('reconnect-failed', resolve));

  await server.close();
  const failure = await exhausted;

  assert.deepStrictEqual(delays, [10, 20, 40, 40], 'exponential growth, capped at maxDelay');
  assert.strictEqual(failure.attempts, 4);
  assert.strictEqual(client.active, false);
});

test('reconnect: the api is rebuilt and its listeners survive', async (t) => {
  const legacy = { legacy: procedure({ access: 'public', handler: async () => 'old' }) };
  const first = await createServer(router(legacy));
  const { port } = first;
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    reconnect: { minDelay: 10, maxDelay: 20, jitter: false },
    heartbeat: false,
  });
  t.after(() => void client.close());
  client.on('error', noop);

  await client.load('test');
  assert.strictEqual(await client.api.test.hello(), 'hi');
  assert.strictEqual(await client.api.test.legacy(), 'old');

  const unit = client.api.test;
  const events = [];
  unit.on('ping', (data) => events.push(data));

  const reconnected = new Promise((resolve) => client.once('reconnect', resolve));
  await first.server.close();
  // The replacement drops the `legacy` method: a reload has to reflect that.
  const second = await createServer(router(), port);
  t.after(() => second.server.close());

  const info = await reconnected;
  assert.deepStrictEqual(info.units, ['test'], 'every loaded unit is reloaded');
  assert.ok(info.attempts >= 1, 'the payload reports how many attempts it took');

  assert.strictEqual(client.api.test, unit, 'the unit object is reused, not replaced');
  assert.strictEqual(await client.api.test.hello(), 'hi', 'the api works again after a restart');
  assert.strictEqual(client.api.test.legacy, undefined, 'a method the server dropped stops being callable');

  await client.api.test.notify();
  await timers.setTimeout(20);
  assert.deepStrictEqual(events, [{ ping: true }], 'listeners registered before the drop still fire');
});

test('reconnect: an explicit close is not a reconnect', async (t) => {
  const { server, port } = await createServer(router());
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
  await client.load('test');

  let reconnects = 0;
  client.on('reconnect', () => void reconnects++);
  const closed = new Promise((resolve) => client.once('close', resolve));
  client.close();
  await closed;
  await timers.setTimeout(30);

  assert.strictEqual(client.active, false);
  assert.strictEqual(reconnects, 0, 'a closed client does not reconnect on its own');

  const reopened = new Promise((resolve) => client.once('open', resolve));
  await client.open();
  await reopened;
  t.after(() => void client.close());
  await timers.setTimeout(20);
  assert.strictEqual(reconnects, 0, 'reopening by hand is a fresh start, not a reconnect');
});

// ---------------------------------------------------------------------------
// Heartbeat

test('heartbeat: the server answers an app-level ping', async (t) => {
  const { server, port } = await createServer(router());
  t.after(() => server.close());

  const socket = new WebSocket(`ws://127.0.0.1:${port}/api`);
  t.after(() => socket.close());
  await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));
  const answer = new Promise((resolve) => {
    socket.addEventListener('message', ({ data }) => resolve(jsonParse(data)), { once: true });
  });
  socket.send(JSON.stringify({ type: 'ping' }));
  assert.deepStrictEqual(await answer, { type: 'pong' });
});

test('heartbeat: an unanswered ping forces a reconnect', async (t) => {
  const httpServer = http.createServer();
  const wsServer = new WebsocketServer({ server: httpServer });
  const pings = [];
  let answer = true;
  wsServer.on('connection', (ws) => {
    ws.on('message', (raw) => {
      const packet = jsonParse(raw.toString()) || {};
      if (packet.type !== 'ping') return;
      pings.push(packet);
      if (answer) ws.send(JSON.stringify({ type: 'pong' }));
    });
  });
  await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
  const { port } = httpServer.address();
  t.after(() => void httpServer.close());

  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}`, {
    heartbeat: { interval: 15, timeout: 60 },
    reconnect: false,
  });
  t.after(() => void client.close());
  client.on('error', noop);

  await t.test('an answered heartbeat keeps the connection open', async () => {
    await timers.setTimeout(80);
    assert.ok(pings.length >= 2, `expected repeated pings, got ${pings.length}`);
    assert.strictEqual(client.active, true);
  });

  await t.test('a silent peer trips the liveness timer and closes', async () => {
    answer = false;
    const timedOut = new Promise((resolve) => client.once('heartbeat-timeout', resolve));
    const closed = new Promise((resolve) => client.once('close', resolve));
    await timedOut;
    await closed;
    assert.strictEqual(client.active, false);
  });
});

test('heartbeat: the client answers a server-initiated ping', async (t) => {
  const httpServer = http.createServer();
  const wsServer = new WebsocketServer({ server: httpServer });
  const packets = [];
  let peer = null;
  wsServer.on('connection', (ws) => {
    peer = ws;
    ws.on('message', (raw) => void packets.push(jsonParse(raw.toString())));
  });
  await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
  const { port } = httpServer.address();
  t.after(() => void httpServer.close());

  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}`, { heartbeat: false, reconnect: false });
  t.after(() => void client.close());

  await t.test('a ping is answered with a pong', async () => {
    peer.send(JSON.stringify({ type: 'ping' }));
    await waitFor(() => packets.length === 1);
    assert.deepStrictEqual(packets[0], { type: 'pong' });
  });

  await t.test('an unsolicited pong is ignored', async () => {
    peer.send(JSON.stringify({ type: 'pong' }));
    await timers.setTimeout(30);
    assert.strictEqual(client.active, true, 'nothing was waiting on it, so nothing happens');
    assert.strictEqual(packets.length, 1);
  });
});

test('errors: a background failure with no listener is logged, not thrown', async (t) => {
  const { server, port } = await createServer(router());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    reconnect: { minDelay: 5, maxDelay: 5, jitter: false, retries: 1 },
    heartbeat: false,
  });
  t.after(() => void client.close());

  const logged = [];
  const { error } = globalThis.console;
  globalThis.console.error = (...args) => void logged.push(args);
  t.after(() => {
    globalThis.console.error = error;
  });

  const exhausted = new Promise((resolve) => client.once('reconnect-failed', resolve));
  await server.close();
  await exhausted;

  assert.strictEqual(client.listenerCount('error'), 0, 'nothing is listening for errors');
  assert.ok(logged.length > 0, 'the connection failure reached the console instead of throwing');
});

test('heartbeat: only a transport that can die silently gets one', () => {
  const { ws: WsTransport, http: HttpTransport, event: EventTransport } = WrpcClient.transport;
  assert.strictEqual(new WsTransport('ws://127.0.0.1:1').heartbeat, true);
  assert.strictEqual(new HttpTransport('http://127.0.0.1:1').heartbeat, false);
  assert.strictEqual(EventTransport.getInstance('http://127.0.0.1:1').heartbeat, false);
});

// ---------------------------------------------------------------------------
// Unhandled events

test('events: one that reaches no listener surfaces as unhandled-event', async (t) => {
  const { server, port } = await createServer(router());
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
  t.after(() => void client.close());
  await client.load('test');

  await t.test('an event for a unit that was never loaded', async () => {
    const unhandled = new Promise((resolve) => client.once('unhandled-event', resolve));
    await client.api.test.notify({ name: 'other/ping' });
    assert.deepStrictEqual(await unhandled, { name: 'other/ping', data: { ping: true } });
  });

  await t.test('an event on a loaded unit nobody subscribed to', async () => {
    const unhandled = new Promise((resolve) => client.once('unhandled-event', resolve));
    await client.api.test.notify({ name: 'test/ping' });
    assert.deepStrictEqual(await unhandled, { name: 'test/ping', data: { ping: true } });
  });

  await t.test('a unit name from Object.prototype resolves to nothing', async () => {
    // `api` is a plain object, so 'constructor' and 'toString' used to
    // resolve up the prototype chain and be treated as loaded units.
    for (const name of ['constructor/ping', 'toString/ping', '__proto__/ping', 'hasOwnProperty/ping']) {
      const unhandled = new Promise((resolve) => client.once('unhandled-event', resolve));
      await client.api.test.notify({ name });
      assert.deepStrictEqual(await unhandled, { name, data: { ping: true } });
    }
    assert.strictEqual(await client.api.test.hello(), 'hi', 'the client survived every one');
  });

  await t.test('an event name with an empty event part is unhandled', async () => {
    const unhandled = new Promise((resolve) => client.once('unhandled-event', resolve));
    await client.api.test.notify({ name: 'test/' });
    assert.deepStrictEqual(await unhandled, { name: 'test/', data: { ping: true } });
  });

  await t.test('a listener takes precedence over unhandled-event', async () => {
    let unhandled = 0;
    client.on('unhandled-event', () => void unhandled++);
    const received = new Promise((resolve) => client.api.test.once('ping', resolve));
    await client.api.test.notify({ name: 'test/ping' });
    assert.deepStrictEqual(await received, { ping: true });
    await timers.setTimeout(20);
    assert.strictEqual(unhandled, 0);
  });
});

// ---------------------------------------------------------------------------
// Phase 4: the connection lifecycle settles everything deterministically.

test('in-flight calls are rejected with 503 the moment the connection dies', async (t) => {
  const definition = router({
    slow: procedure({ access: 'public', handler: async () => timers.setTimeout(5000, 'late') }),
  });
  const { server, port } = await createServer(definition);
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    heartbeat: false,
    reconnect: false,
    callTimeout: 30_000,
  });
  t.after(() => void client.close());
  await client.load('test');

  const started = Date.now();
  const pending = client.api.test.slow();
  await timers.setTimeout(20);
  // The server terminates this peer: the pending call must settle NOW, not
  // in 30 seconds.
  for (const peer of server.clients) peer.destroy();
  for (const connection of server.wsServer.connections) connection.terminate();
  const error = await pending.then(
    () => null,
    (failure) => failure,
  );
  assert.ok(error, 'the call must reject');
  assert.strictEqual(error.code, 503);
  assert.match(error.message, /Connection closed/);
  assert.ok(Date.now() - started < 5000, 'settled by the disconnect, not by the timeout');
});

test('an explicit close() rejects in-flight calls too', async (t) => {
  const definition = router({
    slow: procedure({ access: 'public', handler: async () => timers.setTimeout(5000, 'late') }),
  });
  const { server, port } = await createServer(definition);
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    heartbeat: false,
    reconnect: false,
    callTimeout: 30_000,
  });
  await client.load('test');
  const pending = client.api.test.slow();
  await timers.setTimeout(20);
  client.close();
  await assert.rejects(pending, (error) => error.code === 503);
});

test('a failed load() on reconnect does not kill the subscriptions silently', async (t) => {
  const state = { subscribes: 0 };
  const definition = defineRouter({
    feed: {
      ticks: procedure.subscription({
        access: 'public',
        handler: async function* (_context, _args, { signal }) {
          state.subscribes++;
          await timers.setTimeout(60_000, undefined, { signal }).catch(() => {});
        },
      }),
    },
  });
  const { server, port } = await createServer(definition);
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    heartbeat: false,
    reconnectTimeout: 20,
    logger: false,
  });
  t.after(() => void client.close());
  await client.load('feed');
  client.api.feed.ticks.subscribe({}, {});
  await waitFor(() => state.subscribes === 1, 'the subscription never opened');

  // Force a reconnect. The re-subscribe must happen even though restore also
  // reloads units — the coupling that used to kill subscriptions is gone.
  for (const connection of server.wsServer.connections) connection.terminate();
  await waitFor(() => state.subscribes === 2, 'the subscription was not restored after the reconnect');
});

test('close({ drain }) lets in-flight calls finish and refuses new ones with 503', async (t) => {
  const definition = router({
    slow: procedure({ access: 'public', handler: async () => timers.setTimeout(150, 'done') }),
  });
  const { server, port } = await createServer(definition);
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    heartbeat: false,
    reconnect: false,
  });
  t.after(() => void client.close());
  await client.load('test');

  const inFlight = client.api.test.slow();
  await timers.setTimeout(20);
  const closing = server.close({ drain: 2000 });
  await timers.setTimeout(20);
  // The drain window refuses NEW work...
  await assert.rejects(client.api.test.hello(), (error) => error.code === 503);
  // ...but finishes what it started.
  assert.strictEqual(await inFlight, 'done');
  await closing;
});

test('the queue is abort-aware and the timeout covers the queue wait', async (t) => {
  const definition = router({
    queued: procedure({
      access: 'public',
      timeout: 120,
      queue: { concurrency: 1, size: 10 },
      handler: async () => timers.setTimeout(80, 'ran'),
    }),
  });
  const { server, port } = await createServer(definition);
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false, reconnect: false });
  t.after(() => void client.close());
  await client.load('test');

  // First occupies the slot (~80ms); second waits ~80ms in the queue and
  // has ~40ms of budget left — its handler alone needs 80 more, so the
  // DEADLINE (not a fresh per-handler timeout) must fail it with 408.
  const first = client.api.test.queued();
  const second = client.api.test.queued();
  assert.strictEqual(await first, 'ran');
  await assert.rejects(second, (error) => error.code === 408);
});

test('an HTTP transport failure settles the exact calls it carried', async (t) => {
  // A server that answers 502 with an HTML body — the proxy failure shape
  // wrpc packets never travel in. Without the synthesized answers the
  // introspect call under load() would hang out its whole callTimeout.
  const proxy = http.createServer((req, res) => {
    res.writeHead(502, { 'Content-Type': 'text/html' });
    res.end('<html>Bad Gateway</html>');
  });
  await new Promise((resolve) => proxy.listen(0, '127.0.0.1', resolve));
  t.after(() => proxy.close());
  const { port } = proxy.address();

  const client = await WrpcClient.connect(`http://127.0.0.1:${port}/api`, {
    reconnect: false,
    callTimeout: 30_000,
  });
  t.after(() => void client.close());
  const started = Date.now();
  await assert.rejects(client.load('test'), (error) => error.code === 502);
  assert.ok(Date.now() - started < 5000, 'settled by the synthesized answer, not the timeout');
});
