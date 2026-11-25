'use strict';

const http = require('node:http');
const timers = require('node:timers/promises');
const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

const { WebsocketServer } = require('#ws');
const { Server, WrpcClient, defineRouter, procedure } = require('../index.js');
const { backoffDelay, jsonParse } = require('../src/utils.js');
const { waitFor } = require('./helpers/wait.js');

const noop = () => {};

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

describe('backoffDelay: truncated exponential growth with full jitter', () => {
  const schedule = { minDelay: 100, maxDelay: 1000, factor: 2 };

  it('should make the window itself the delay without jitter', () => {
    const delays = [0, 1, 2, 3, 4].map((attempt) => backoffDelay({ ...schedule, attempt, jitter: false }));
    assert.deepStrictEqual(delays, [100, 200, 400, 800, 1000], 'growth is capped by maxDelay');
  });

  it('should spread the delay over the whole window with full jitter', () => {
    assert.strictEqual(backoffDelay({ ...schedule, attempt: 2, random: () => 0 }), 0);
    assert.strictEqual(backoffDelay({ ...schedule, attempt: 2, random: () => 1 }), 400);
    assert.strictEqual(backoffDelay({ ...schedule, attempt: 2, random: () => 0.5 }), 200);
  });

  it('should stay inside the window with a real random source', () => {
    for (let attempt = 0; attempt < 8; attempt++) {
      const window = Math.min(schedule.maxDelay, schedule.minDelay * 2 ** attempt);
      for (let i = 0; i < 100; i++) {
        const delay = backoffDelay({ ...schedule, attempt });
        assert.ok(delay >= 0 && delay <= window, `delay ${delay} outside [0, ${window}]`);
      }
    }
  });

  it('should not overflow past the cap on a huge attempt count', () => {
    assert.strictEqual(backoffDelay({ ...schedule, attempt: 5000, jitter: false }), 1000);
    assert.strictEqual(backoffDelay({ minDelay: 100, maxDelay: 1000, factor: 0, attempt: 3, jitter: false }), 100);
  });

  it('should default to factor 2 and jitter on', () => {
    const delay = backoffDelay({ minDelay: 100, maxDelay: 1000, attempt: 1, random: () => 1 });
    assert.strictEqual(delay, 200);
  });
});

describe('reconnect', () => {
  it('should keep the delay the reconnectTimeout shorthand asked for', async (t) => {
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

  it('should not let a socket abandoned by terminate() close its replacement', async (t) => {
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

  it('should drive the retries by the backoff schedule and then give up', async (t) => {
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

  it('should rebuild the api, and its listeners should survive', async (t) => {
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

  it('should not take an explicit close for a reconnect', async (t) => {
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
});

// ---------------------------------------------------------------------------
// Heartbeat

describe('heartbeat', () => {
  it('should get an answer from the server to an app-level ping', async (t) => {
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

  describe('an unanswered ping forces a reconnect', () => {
    const pings = [];
    let answer = true;
    let httpServer = null;
    let client = null;

    before(async () => {
      httpServer = http.createServer();
      const wsServer = new WebsocketServer({ server: httpServer });
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

      client = await WrpcClient.connect(`ws://127.0.0.1:${port}`, {
        heartbeat: { interval: 15, timeout: 60 },
        reconnect: false,
      });
      client.on('error', noop);
    });

    after(() => void httpServer?.close());
    after(() => void client?.close());

    it('should keep the connection open while the heartbeat is answered', async () => {
      await timers.setTimeout(80);
      assert.ok(pings.length >= 2, `expected repeated pings, got ${pings.length}`);
      assert.strictEqual(client.active, true);
    });

    it('should trip the liveness timer and close on a silent peer', async () => {
      answer = false;
      const timedOut = new Promise((resolve) => client.once('heartbeat-timeout', resolve));
      const closed = new Promise((resolve) => client.once('close', resolve));
      await timedOut;
      await closed;
      assert.strictEqual(client.active, false);
    });
  });

  describe('the client answers a server-initiated ping', () => {
    const packets = [];
    let peer = null;
    let httpServer = null;
    let client = null;

    before(async () => {
      httpServer = http.createServer();
      const wsServer = new WebsocketServer({ server: httpServer });
      wsServer.on('connection', (ws) => {
        peer = ws;
        ws.on('message', (raw) => void packets.push(jsonParse(raw.toString())));
      });
      await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
      const { port } = httpServer.address();

      client = await WrpcClient.connect(`ws://127.0.0.1:${port}`, { heartbeat: false, reconnect: false });
    });

    after(() => void httpServer?.close());
    after(() => void client?.close());

    it('should answer a ping with a pong', async () => {
      peer.send(JSON.stringify({ type: 'ping' }));
      await waitFor(() => packets.length === 1);
      assert.deepStrictEqual(packets[0], { type: 'pong' });
    });

    it('should ignore an unsolicited pong', async () => {
      peer.send(JSON.stringify({ type: 'pong' }));
      await timers.setTimeout(30);
      assert.strictEqual(client.active, true, 'nothing was waiting on it, so nothing happens');
      assert.strictEqual(packets.length, 1);
    });
  });

  it('should log a background failure with no error listener, not throw it', async (t) => {
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

  it('should be given only to a transport that can die silently', () => {
    const { ws: WsTransport, http: HttpTransport, event: EventTransport } = WrpcClient.transport;
    assert.strictEqual(new WsTransport('ws://127.0.0.1:1').heartbeat, true);
    assert.strictEqual(new HttpTransport('http://127.0.0.1:1').heartbeat, false);
    assert.strictEqual(new EventTransport('http://127.0.0.1:1').heartbeat, false);
  });
});

// ---------------------------------------------------------------------------
// Unhandled events

describe('events: one that reaches no listener surfaces as unhandled-event', () => {
  let server = null;
  let client = null;

  before(async () => {
    const booted = await createServer(router());
    server = booted.server;
    client = await WrpcClient.connect(`ws://127.0.0.1:${booted.port}/api`, { heartbeat: false });
    await client.load('test');
  });

  after(() => server?.close());
  after(() => void client?.close());

  it('should surface an event for a unit that was never loaded', async () => {
    const unhandled = new Promise((resolve) => client.once('unhandled-event', resolve));
    await client.api.test.notify({ name: 'other/ping' });
    assert.deepStrictEqual(await unhandled, { name: 'other/ping', data: { ping: true } });
  });

  it('should surface an event on a loaded unit nobody subscribed to', async () => {
    const unhandled = new Promise((resolve) => client.once('unhandled-event', resolve));
    await client.api.test.notify({ name: 'test/ping' });
    assert.deepStrictEqual(await unhandled, { name: 'test/ping', data: { ping: true } });
  });

  it('should resolve a unit name from Object.prototype to nothing', async () => {
    // `api` is a plain object, so 'constructor' and 'toString' used to
    // resolve up the prototype chain and be treated as loaded units.
    for (const name of ['constructor/ping', 'toString/ping', '__proto__/ping', 'hasOwnProperty/ping']) {
      const unhandled = new Promise((resolve) => client.once('unhandled-event', resolve));
      await client.api.test.notify({ name });
      assert.deepStrictEqual(await unhandled, { name, data: { ping: true } });
    }
    assert.strictEqual(await client.api.test.hello(), 'hi', 'the client survived every one');
  });

  it('should surface an event name with an empty event part', async () => {
    const unhandled = new Promise((resolve) => client.once('unhandled-event', resolve));
    await client.api.test.notify({ name: 'test/' });
    assert.deepStrictEqual(await unhandled, { name: 'test/', data: { ping: true } });
  });

  it('should let a listener take precedence over unhandled-event', async () => {
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

describe('the connection lifecycle settles everything deterministically', () => {
  it('should reject in-flight calls with 503 the moment the connection dies', async (t) => {
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

  it('should reject in-flight calls on an explicit close() too', async (t) => {
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

  it('should not let a failed load() on reconnect kill the subscriptions silently', async (t) => {
    const state = { subscribes: 0 };
    const definition = defineRouter({
      feed: {
        ticks: procedure.subscription({
          access: 'public',
          // oxlint-disable-next-line require-yield -- a feed that yields nothing on purpose: it stays open until the client lets go
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

  // ---------------------------------------------------------------------------
  // Backoff correctness when the failure happens AFTER the socket opened: the
  // attempt counter is reset to 0 the moment 'open' fires, so a post-open
  // failure (restore, and later authenticate) must put it back before it
  // terminates — otherwise the cycle hammers minDelay forever.

  const sessionGatedBoot = async (t) => {
    const definition = defineRouter({
      auth: {
        login: procedure({
          access: 'public',
          handler: async (context) => void context.client.startSession(undefined, { user: 'a' }),
        }),
      },
      secret: {
        peek: procedure({ access: 'session', handler: async () => 'ok' }),
      },
    });
    const server = new Server({
      router: definition,
      host: '127.0.0.1',
      port: 0,
      protocol: 'http',
      logger: false,
      timeouts: { bind: 50 },
      introspection: 'session',
    });
    await server.listen();
    t.after(() => server.close());
    return { server, port: server.address().port };
  };

  it('should grow the backoff on a failing restore load() and eventually exhaust retries', async (t) => {
    const { server, port } = await sessionGatedBoot(t);
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: { minDelay: 10, maxDelay: 1000, factor: 2, jitter: false, retries: 3 },
    });
    t.after(() => void client.close());
    // introspection: 'session' + a ws login (which sets no cookie) is exactly
    // the scenario in which restore's load() fails on every new socket.
    client.use({ auth: { login: { access: 'public' } } });
    await client.api.auth.login();
    await client.load('secret');
    assert.strictEqual(await client.api.secret.peek(), 'ok');

    const delays = [];
    let failures = 0;
    client.on('reconnecting', ({ delay }) => void delays.push(delay));
    client.on('restore-failed', () => void failures++);
    const exhausted = new Promise((resolve) => client.on('reconnect-failed', resolve));

    for (const connection of server.wsServer.connections) connection.terminate();

    const result = await exhausted;
    // Before the fix the counter reset to 0 on every open, so the schedule was
    // [10, 10, 10, ...] forever and 'reconnect-failed' never fired at all.
    assert.deepStrictEqual(delays, [10, 20, 40]);
    assert.strictEqual(failures, 3);
    assert.strictEqual(result.attempts, 3);
  });

  it('should escalate a throwing restore-failed listener, not leave an unhandled rejection', async (t) => {
    const { server, port } = await sessionGatedBoot(t);
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: { minDelay: 10, maxDelay: 20, factor: 2, jitter: false, retries: 1 },
    });
    t.after(() => void client.close());
    client.use({ auth: { login: { access: 'public' } } });
    await client.api.auth.login();
    await client.load('secret');

    const escalated = [];
    client.on('error', (error) => void escalated.push(error.message));
    client.on('restore-failed', () => {
      throw new Error('listener boom');
    });
    const exhausted = new Promise((resolve) => client.on('reconnect-failed', resolve));
    for (const connection of server.wsServer.connections) connection.terminate();
    await exhausted;
    assert.ok(escalated.includes('listener boom'));
  });

  it('should let in-flight calls finish on close({ drain }) and refuse new ones with 503', async (t) => {
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

  it('should make the queue abort-aware and the timeout cover the queue wait', async (t) => {
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

  it('should settle the exact calls an HTTP transport failure carried', async (t) => {
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
});

// ---------------------------------------------------------------------------
// Transport fallback: `transport: [a, b, ...]` — candidates in order,
// retries per candidate, loud capability loss.

const { Emitter } = require('../src/utils.js');

// A fake client transport on the ClientTransport contract, registered under
// a throwaway name (the pattern the heartbeat test above set). `kill()`
// makes every further open() reject the way a dead socket does — an error
// plus a 'close' on the next microtask.
const registerFake = (name, { persistent = true } = {}) => {
  const instances = [];
  class Fake extends Emitter {
    constructor(url) {
      super();
      this.url = url;
      this.persistent = persistent;
      this.heartbeat = false;
      this.active = false;
      this.dead = false;
      instances.push(this);
    }

    async open(options = {}) {
      this.lastOpen = options; // what the client resolved for THIS open
      if (this.dead) {
        queueMicrotask(() => this.emit('close'));
        throw new Error(`open failed (${this.url})`);
      }
      this.active = true;
      this.emit('open');
    }

    kill() {
      this.dead = true;
      this.active = false;
      this.emit('close');
    }

    write() {
      return true;
    }

    send() {
      return true;
    }

    close() {
      this.active = false;
      this.emit('close');
    }

    terminate() {
      // The base ClientTransport contract: terminate reports the close, so
      // the reconnect cycle a post-open failure forces can continue.
      this.close();
    }

    online() {}

    offline() {}
  }
  WrpcClient.transport[name] = Fake;
  return { instances, teardown: () => delete WrpcClient.transport[name] };
};

describe('fallback', () => {
  it('should validate the candidate list up front', async () => {
    await assert.rejects(WrpcClient.connect('ws://x/api', { transport: [] }), /must not be empty/);
    await assert.rejects(WrpcClient.connect('ws://x/api', { transport: ['ws', 'nope'] }), /Unknown transport 'nope'/);
    await assert.rejects(WrpcClient.connect('ws://x/api', { transport: ['ws', 'event'] }), /cannot contain 'event'/);
  });

  it('should hand a dying candidate over to the next, and the old one goes inert', async (t) => {
    const first = registerFake('fake-a');
    const second = registerFake('fake-b');
    t.after(() => {
      first.teardown();
      second.teardown();
    });

    const client = await WrpcClient.connect('http://host/api', {
      transport: ['fake-a', 'fake-b'],
      reconnect: { retries: 2, minDelay: 1, maxDelay: 1, jitter: false },
      heartbeat: false,
    });
    t.after(() => void client.close());

    const fallbacks = [];
    client.on('transport-fallback', (info) => fallbacks.push(info));
    const failed = [];
    client.on('reconnect-failed', (info) => failed.push(info));

    const a = first.instances[0];
    a.kill();

    await waitFor(() => second.instances.length > 0 && second.instances[0].active, 'fake-b never took over');
    assert.deepStrictEqual(fallbacks, [{ from: 'fake-a', to: 'fake-b' }]);
    assert.strictEqual(failed.length, 0, 'reconnect-failed must wait for the LAST candidate');
    assert.strictEqual(client.active, true);
    // Retries reset per candidate: the swap arrived with a fresh counter.
    assert.strictEqual(client.attempt, 0);

    // A late 'close' from the abandoned transport must not disturb the live
    // candidate — its listeners came off at the swap.
    a.emit('close');
    await timers.setTimeout(10);
    assert.strictEqual(client.active, true);
  });

  it('should emit reconnect-failed only when the LAST candidate is exhausted', async (t) => {
    const first = registerFake('fake-a');
    const second = registerFake('fake-b');
    t.after(() => {
      first.teardown();
      second.teardown();
    });

    const client = await WrpcClient.connect('http://host/api', {
      transport: ['fake-a', 'fake-b'],
      reconnect: { retries: 1, minDelay: 1, maxDelay: 1, jitter: false },
      heartbeat: false,
    });
    t.after(() => void client.close());

    const fallbacks = [];
    client.on('transport-fallback', (info) => fallbacks.push(info));
    const exhausted = new Promise((resolve) => client.once('reconnect-failed', resolve));

    first.instances[0].kill();
    await waitFor(() => second.instances.length > 0, 'fake-b never constructed');
    // The second candidate is dead on arrival too.
    second.instances[0].dead = true;
    second.instances[0].close();
    await exhausted;
    assert.strictEqual(fallbacks.length, 1, 'no wrap-around: one pass over the list');
  });

  it('should re-spell the URL per candidate scheme', async (t) => {
    const first = registerFake('fake-a');
    t.after(() => first.teardown());
    const client = await WrpcClient.connect('wss://host/api', {
      transport: ['fake-a', 'http'],
      reconnect: { retries: 0 },
      heartbeat: false,
    });
    t.after(() => void client.close());
    // The fake saw the ws spelling (non-ws candidates get http/https).
    assert.strictEqual(first.instances[0].url, 'https://host/api');
  });

  it('should fail live subscriptions loudly when landing on a non-persistent transport', async (t) => {
    const feed = procedure.subscription({
      access: 'public',
      handler: async function* (_context, _args, { signal }) {
        yield 1;
        await new Promise((resolve) => {
          signal.addEventListener('abort', resolve, { once: true });
        });
      },
    });
    const { server, port } = await createServer(router({ feed }));
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      transport: ['ws', 'http'],
      reconnect: { retries: 0, minDelay: 1, maxDelay: 1, jitter: false },
      heartbeat: false,
    });
    t.after(() => void client.close());
    await client.load('test');

    const seen = [];
    const errors = [];
    client.api.test.feed.subscribe({}, { onData: (value) => seen.push(value), onError: (error) => errors.push(error) });
    await waitFor(() => seen.length > 0, 'the feed never produced');

    const fallbacks = [];
    client.on('transport-fallback', (info) => fallbacks.push(info));
    await server.close();

    await waitFor(() => errors.length > 0, 'the subscription never failed');
    assert.strictEqual(errors[0].code, 400);
    assert.match(errors[0].message, /cannot carry subscriptions/);
    assert.deepStrictEqual(fallbacks, [{ from: 'ws', to: 'http' }]);
    client.close();
  });
});

// ---------------------------------------------------------------------------
// The authenticate hook: awaited inside open() on the first connect, and on
// every reconnect BEFORE the subscriptions are re-opened and units reloaded.

const authBoot = async (t, { introspection = 'session' } = {}) => {
  const order = [];
  const state = { subscribes: 0, logins: 0 };
  const definition = defineRouter({
    auth: {
      login: procedure({
        access: 'public',
        handler: async (context) => {
          state.logins++;
          order.push('login');
          context.client.startSession(undefined, { user: 'ann' });
        },
      }),
    },
    feed: {
      ticks: procedure.subscription({
        access: 'session',
        // oxlint-disable-next-line require-yield -- a feed that yields nothing on purpose: it stays open until the client lets go
        handler: async function* (_context, _args, { signal }) {
          state.subscribes++;
          order.push('subscribe');
          await timers.setTimeout(60_000, undefined, { signal }).catch(() => {});
        },
      }),
    },
  });
  const server = new Server({
    router: definition,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 50 },
    introspection,
  });
  await server.listen();
  t.after(() => server.close());
  return { server, port: server.address().port, order, state };
};

describe('authenticate', () => {
  it('should be awaited on first connect, and before restore on reconnect', async (t) => {
    const { server, port, order, state } = await authBoot(t);
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnectTimeout: 10,
      authenticate: (c) => c.call('auth/login'),
    });
    t.after(() => void client.close());
    // connect() resolved authenticated: a session-gated load works right away
    // even though the server's introspection itself requires a session.
    assert.strictEqual(state.logins, 1);
    await client.load('feed');
    client.api.feed.ticks.subscribe({}, {});
    await waitFor(() => state.subscribes === 1, 'the subscription never opened');

    // Force a reconnect. The DECISIVE assertion is the arrival order on the
    // new socket: login (the credential) must precede the re-subscribe, which
    // an 'open' listener could never guarantee — and the session-gated
    // subscription resuming at all proves the credential arrived first.
    for (const connection of server.wsServer.connections) connection.terminate();
    await waitFor(() => state.subscribes === 2, 'the subscription was not restored after the reconnect');
    assert.deepStrictEqual(order, ['login', 'subscribe', 'login', 'subscribe']);
    assert.strictEqual(state.logins, 2);
  });

  it('should hand the hook { reconnected, attempts }', async (t) => {
    const { server, port } = await authBoot(t, { introspection: true });
    const infos = [];
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnectTimeout: 10,
      authenticate: (c, info) => void infos.push(info),
    });
    t.after(() => void client.close());
    assert.deepStrictEqual(infos, [{ reconnected: false, attempts: 0 }]);
    for (const connection of server.wsServer.connections) connection.terminate();
    await waitFor(() => infos.length === 2, 'the hook never re-ran on reconnect');
    assert.strictEqual(infos[1].reconnected, true);
    assert.ok(infos[1].attempts >= 1);
  });

  it('should walk the backoff on a failing hook and emit authenticate-failed', async (t) => {
    const { server, port } = await authBoot(t, { introspection: true });
    let allow = true;
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: { minDelay: 10, maxDelay: 1000, factor: 2, jitter: false, retries: 3 },
      authenticate: () => {
        if (!allow) throw new Error('credential rejected');
      },
    });
    t.after(() => void client.close());

    const delays = [];
    const failed = [];
    client.on('reconnecting', ({ delay }) => void delays.push(delay));
    client.on('authenticate-failed', (info) => void failed.push(info));
    const exhausted = new Promise((resolve) => client.on('reconnect-failed', resolve));

    allow = false;
    for (const connection of server.wsServer.connections) connection.terminate();
    await exhausted;
    // The same growth lock as the restore path: without the attempt-count
    // restoration this would be [10, 10, 10, ...] forever.
    assert.deepStrictEqual(delays, [10, 20, 40]);
    assert.strictEqual(failed.length, 3);
    assert.strictEqual(failed[0].error.message, 'credential rejected');
    assert.strictEqual(failed[0].reconnected, true);
    assert.ok(failed[0].attempts >= 1);
  });

  it('should never turn a refused connection stable, however late its close arrives', async (t) => {
    // A hook that throws synchronously runs inside the transport's 'open'
    // emit, where the ws transport is still settling its open: terminate()
    // there can only start a close handshake, and 'close' comes when the
    // server answers it. The stability window (stableAfter defaults to
    // minDelay) used to run out first on a slow runner and zero the attempt
    // count — the backoff started over: [10, 20, 10, 20, 40]. This transport
    // reports every forced close late, so the race is not left to the runner.
    const instances = [];
    class LateClose extends Emitter {
      constructor(url) {
        super();
        this.url = url;
        this.persistent = true;
        this.heartbeat = false;
        this.active = false;
        instances.push(this);
      }

      async open() {
        this.active = true;
        this.emit('open');
      }

      write() {
        return true;
      }

      send() {
        return true;
      }

      close() {
        this.terminate();
      }

      terminate() {
        if (!this.active) return;
        this.active = false;
        setTimeout(() => this.emit('close'), 40);
      }

      drop() {
        this.active = false;
        this.emit('close');
      }

      online() {}

      offline() {}
    }
    WrpcClient.transport.lateclose = LateClose;
    t.after(() => delete WrpcClient.transport.lateclose);

    let allow = true;
    const client = await WrpcClient.connect('fake://x', {
      transport: ['lateclose'],
      heartbeat: false,
      logger: false,
      reconnect: { minDelay: 10, maxDelay: 1000, factor: 2, jitter: false, retries: 3 },
      authenticate: () => {
        if (!allow) throw new Error('credential rejected');
      },
    });
    t.after(() => void client.close());

    const delays = [];
    let exhausted = false;
    client.on('reconnecting', ({ delay }) => void delays.push(delay));
    client.on('reconnect-failed', () => {
      exhausted = true;
    });

    allow = false;
    instances.at(-1).drop();
    // Bounded: a counter that keeps going back to zero never exhausts.
    await waitFor(() => exhausted || delays.length > 3, 'the reconnect cycle stalled');
    assert.deepStrictEqual(delays, [10, 20, 40]);
    assert.strictEqual(exhausted, true);
  });

  it('should reject connect() on a first-connect failure and leave no zombie', async (t) => {
    const { port } = await authBoot(t, { introspection: true });
    const connectionsBefore = WrpcClient.connections.size;
    await assert.rejects(
      WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
        heartbeat: false,
        logger: false,
        reconnectTimeout: 10,
        authenticate: () => {
          throw new Error('bad password');
        },
      }),
      (error) => error.message === 'bad password',
    );
    // open() had registered the client before the transport was reached; the
    // rejection must not leave it behind for WrpcClient.online() to revive.
    assert.strictEqual(WrpcClient.connections.size, connectionsBefore);
  });

  it('should stop the retry cycle when the hook closes the client', async (t) => {
    const { server, port } = await authBoot(t, { introspection: true });
    let fatal = false;
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: { minDelay: 5, maxDelay: 10, factor: 2, jitter: false, retries: 10 },
      authenticate: (c) => {
        if (!fatal) return;
        c.close(); // the documented "a bad password won't fix itself" escape
        throw new Error('give up');
      },
    });
    const reconnecting = [];
    client.on('reconnecting', (info) => void reconnecting.push(info));
    fatal = true;
    for (const connection of server.wsServer.connections) connection.terminate();
    await waitFor(() => !WrpcClient.connections.has(client), 'close() inside the hook never took effect');
    const settled = reconnecting.length;
    await timers.setTimeout(60);
    assert.strictEqual(reconnecting.length, settled, 'the cycle kept scheduling after close()');
  });

  it('should reject connect() when the hook refuses the FIRST candidate — not resolve unauthenticated on the next', async (t) => {
    const a = registerFake('authfirst-a');
    const b = registerFake('authfirst-b');
    t.after(() => {
      a.teardown();
      b.teardown();
    });
    const connectionsBefore = WrpcClient.connections.size;
    let calls = 0;
    await assert.rejects(
      WrpcClient.connect('fake://x', {
        transport: ['authfirst-a', 'authfirst-b'],
        heartbeat: false,
        logger: false,
        reconnect: { minDelay: 5, maxDelay: 10, jitter: false, retries: 2 },
        authenticate: () => {
          calls++;
          throw new Error('bad credentials');
        },
      }),
      /bad credentials/,
    );
    // The hook's verdict is the application's: no second candidate was
    // tried behind its back, nothing was kept, nothing reconnects.
    assert.strictEqual(calls, 1);
    assert.strictEqual(b.instances.length, 0, 'candidate B was never opened');
    assert.strictEqual(a.instances[0].active, false, 'candidate A was closed');
    assert.strictEqual(WrpcClient.connections.size, connectionsBefore);
    await timers.setTimeout(40);
    assert.strictEqual(a.instances.length, 1, 'no reconnect cycle survived the rejection');
    assert.strictEqual(b.instances.length, 0);

    // A candidate that never OPENS, on the other hand, hands over — and the
    // hook is awaited on the one that did before connect() resolves.
    const dead = registerFake('deadfirst-a');
    const live = registerFake('deadfirst-b');
    t.after(() => {
      dead.teardown();
      live.teardown();
    });
    WrpcClient.transport['deadfirst-a'].prototype.open = async function open() {
      throw new Error('unreachable');
    };
    let authenticated = false;
    const client = await WrpcClient.connect('fake://x', {
      transport: ['deadfirst-a', 'deadfirst-b'],
      heartbeat: false,
      logger: false,
      reconnect: { minDelay: 5, maxDelay: 10, jitter: false, retries: 1 },
      authenticate: async () => {
        await timers.setTimeout(5);
        authenticated = true;
      },
    });
    t.after(() => void client.close());
    assert.strictEqual(authenticated, true, 'connect() resolved authenticated, on candidate B');
    assert.strictEqual(client.active, true);
    assert.strictEqual(live.instances[0].active, true);
  });

  it('should fall through to the next transport candidate on exhausted retries', async (t) => {
    const a = registerFake('authfba');
    const b = registerFake('authfbb');
    t.after(() => {
      a.teardown();
      b.teardown();
    });
    let calls = 0;
    const client = await WrpcClient.connect('fake://x', {
      transport: ['authfba', 'authfbb'],
      heartbeat: false,
      logger: false,
      reconnect: { minDelay: 5, maxDelay: 10, factor: 2, jitter: false, retries: 1 },
      authenticate: () => {
        calls++;
        if (calls === 1) return; // the first connect succeeds on candidate A
        if (b.instances.some((i) => i.active)) return; // candidate B accepts
        throw new Error('still refused on A');
      },
    });
    t.after(() => void client.close());
    const fallbacks = [];
    client.on('transport-fallback', (info) => void fallbacks.push(info));
    // close(), not kill(): candidate A must keep OPENING successfully so the
    // failure that exhausts its retries is the authenticate hook, not the socket.
    a.instances.at(-1).close();
    await waitFor(() => b.instances.some((i) => i.active), 'the fallback candidate never opened');
    assert.deepStrictEqual(fallbacks, [{ from: 'authfba', to: 'authfbb' }]);
    assert.ok(calls >= 3, 'the hook must have been retried on A and re-run on B');
  });

  it('should fire open before reconnect without an authenticate hook, restore unchanged', async (t) => {
    const definition = router();
    const { server, port } = await createServer(definition);
    t.after(() => server.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnectTimeout: 10,
    });
    t.after(() => void client.close());
    await client.load('test');
    const events = [];
    client.on('open', () => void events.push('open'));
    client.on('reconnect', () => void events.push('reconnect'));
    for (const connection of server.wsServer.connections) connection.terminate();
    await waitFor(() => events.includes('reconnect'), 'never reconnected');
    assert.deepStrictEqual(events, ['open', 'reconnect']);
  });
});

// ---------------------------------------------------------------------------
// The refresh hook: single-flight, one-shot retry, original error surfaces.

const refreshBoot = async (t) => {
  const state = { ok: false, ids: [], hits: 0 };
  const definition = defineRouter({
    flaky: {
      get: procedure({
        access: 'public',
        handler: async (context) => {
          state.hits++;
          state.ids.push(context.uuid);
          if (!state.ok) {
            const error = new Error('expired');
            error.code = 401;
            error.expose = true;
            throw error;
          }
          return 'fresh';
        },
      }),
      teapot: procedure({
        access: 'public',
        handler: async () => {
          const error = new Error('teapot');
          error.code = 418;
          error.expose = true;
          throw error;
        },
      }),
    },
  });
  const { server, port } = await createServer(definition);
  t.after(() => server.close());
  return { server, port, state };
};

// A transport the test drives by hand. `sent` is every packet the client
// wrote (it writes synchronously, so they are all there the moment the calls
// are issued) and `deliver` feeds answers back.
//
// The single-flight tests below use it because the property they assert —
// refusals arriving while a run is open all join that one run — is a client
// invariant with a precondition no wire can guarantee. Over a real socket the
// refusals are both what STARTS the run and what has to arrive during it, so
// any delivery spread eats the window: emission can be forced simultaneous,
// but a loaded Linux box may still split one write burst across reads where
// loopback here coalesces it. Every clock-based window tried against this
// raced. Here the test delivers the refusals and holds the run open itself,
// so the precondition is established rather than hoped for. The real-socket
// refresh path stays covered by the server-backed tests around these.
const registerScripted = (name) => {
  const sent = [];
  let live = null;
  class Scripted extends Emitter {
    constructor(url) {
      super();
      this.url = url;
      this.persistent = true;
      this.heartbeat = false;
      this.active = false;
      live = this;
    }

    async open() {
      this.active = true;
      this.emit('open');
    }

    write(packet) {
      sent.push(packet);
      return true;
    }

    send(packet) {
      return this.write(packet);
    }

    close() {
      this.active = false;
      this.emit('close');
    }

    terminate() {
      this.close();
    }

    online() {}

    offline() {}
  }
  WrpcClient.transport[name] = Scripted;
  return {
    sent,
    calls: () => sent.filter((packet) => packet.type === 'call'),
    deliver: (packet) => void live.emit('message', JSON.stringify(packet)),
    refuse: (packet, code = 401) =>
      void live.emit(
        'message',
        JSON.stringify({ type: 'callback', id: packet.id, error: { message: 'expired', code } }),
      ),
    teardown: () => delete WrpcClient.transport[name],
  };
};

describe('refresh', () => {
  it('should refresh a 401 once and retry the call with a fresh packet', async (t) => {
    const { port, state } = await refreshBoot(t);
    let refreshes = 0;
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      refresh: () => {
        refreshes++;
        state.ok = true;
      },
    });
    t.after(() => void client.close());
    await client.load('flaky');
    assert.strictEqual(await client.api.flaky.get(), 'fresh');
    assert.strictEqual(refreshes, 1);
    // Two server-side invocations — the refusal and the retry — each its own
    // packet (context uuids differ), never a re-send of the same id.
    assert.strictEqual(state.hits, 2);
    assert.notStrictEqual(state.ids[0], state.ids[1]);
  });

  it('should produce exactly one refresh for ten concurrent 401s', async (t) => {
    const wire = registerScripted('scripted-join');
    t.after(wire.teardown);
    let refreshes = 0;
    let release;
    const held = new Promise((resolve) => (release = resolve));
    const client = await WrpcClient.connect('ws://scripted/api', {
      transport: 'scripted-join',
      heartbeat: false,
      logger: false,
      reconnect: false,
      refresh: async () => {
        refreshes++;
        await held;
      },
    });
    t.after(() => void client.close());

    // Issued before any run exists, so every one of them is allowed to join
    // (a call issued DURING a run is the deadlock guard's case instead).
    const calls = Array.from({ length: 10 }, () => client.call('flaky/get'));
    const issued = wire.calls();
    assert.strictEqual(issued.length, 10, 'the client writes synchronously: all ten are on the wire');

    // Refused in one turn, and the run cannot end until this test says so —
    // the window is a fact here, not a race.
    for (const packet of issued) wire.refuse(packet);
    await waitFor(() => refreshes === 1, 'the refresh never ran');
    release();

    // Each joined caller re-issues exactly once, under a fresh packet id.
    await waitFor(() => wire.calls().length === 20, 'the refused calls never re-issued');
    const retries = wire.calls().slice(10);
    assert.strictEqual(new Set(retries.map((packet) => packet.id)).size, 10, 'each retry has its own id');
    for (const packet of retries) wire.deliver({ type: 'callback', id: packet.id, result: 'fresh' });

    assert.deepStrictEqual(await Promise.all(calls), new Array(10).fill('fresh'));
    assert.strictEqual(refreshes, 1);
  });

  it('should surface the ORIGINAL error on a failing refresh, and never loop the retries', async (t) => {
    const { port, state } = await refreshBoot(t);
    let refreshes = 0;
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      refresh: () => {
        refreshes++;
        throw new Error('refresh broke');
      },
    });
    t.after(() => void client.close());
    await client.load('flaky');
    await assert.rejects(client.api.flaky.get(), (error) => error.code === 401 && error.message === 'expired');
    assert.strictEqual(refreshes, 1);

    // A refresh that "succeeds" without fixing anything: the retry's second
    // 401 surfaces as-is instead of triggering another refresh.
    const stubborn = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      refresh: () => void refreshes++,
    });
    t.after(() => void stubborn.close());
    refreshes = 0;
    state.ok = false;
    await stubborn.load('flaky');
    await assert.rejects(stubborn.api.flaky.get(), (error) => error.code === 401);
    assert.strictEqual(refreshes, 1);
  });

  it('should not be triggered by codes outside `on`, and `on` widens the trigger', async (t) => {
    const { port, state } = await refreshBoot(t);
    let refreshes = 0;
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      refresh: { on: [401, 403], handler: () => void refreshes++ },
    });
    t.after(() => void client.close());
    await client.load('flaky');
    await assert.rejects(client.api.flaky.teapot(), (error) => error.code === 418);
    assert.strictEqual(refreshes, 0);
    state.ok = true;
    assert.strictEqual(await client.api.flaky.get(), 'fresh');
  });

  it('should never fire for calls made inside the authenticate hook', async (t) => {
    const { port } = await refreshBoot(t);
    let refreshes = 0;
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      authenticate: async (c) => {
        // A 401 inside the hook is the hook's to handle — a refresh here
        // would recurse into the very flow that is authenticating.
        await c.call('flaky/get').catch(() => {});
      },
      refresh: () => void refreshes++,
    });
    t.after(() => void client.close());
    assert.strictEqual(refreshes, 0);
  });
});

describe('connection metadata: headers, meta, declared and metaFormat', () => {
  it('should re-evaluate the function form of headers on every open', async (t) => {
    const fake = registerFake('hdrfake');
    t.after(() => fake.teardown());
    let version = 0;
    const client = await WrpcClient.connect('fake://x', {
      transport: ['hdrfake'],
      heartbeat: false,
      logger: false,
      reconnect: { minDelay: 5, maxDelay: 10, factor: 2, jitter: false, retries: 3 },
      headers: () => ({ 'x-app-version': String(++version) }),
    });
    t.after(() => void client.close());
    const transport = fake.instances.at(-1);
    assert.deepStrictEqual(transport.lastOpen.headers, { 'x-app-version': '1' });
    // The reconnect resolves the function AGAIN — a rotated value is presented,
    // not the one frozen at construction.
    transport.close();
    await waitFor(() => transport.lastOpen.headers?.['x-app-version'] === '2', 'the reconnect kept the stale headers');
  });

  it('should carry the connection-phase meta option on open() alongside headers', async (t) => {
    const fake = registerFake('metafake');
    t.after(() => fake.teardown());
    let n = 0;
    const client = await WrpcClient.connect('fake://x', {
      transport: ['metafake'],
      heartbeat: false,
      logger: false,
      reconnect: { minDelay: 5, maxDelay: 10, factor: 2, jitter: false, retries: 3 },
      meta: () => ({ v: String(++n) }),
    });
    t.after(() => void client.close());
    const transport = fake.instances.at(-1);
    assert.deepStrictEqual(transport.lastOpen.meta, { v: '1' });
    transport.close();
    await waitFor(() => transport.lastOpen.meta?.v === '2', 'the reconnect kept the stale meta');
  });

  it('should throw loudly at construction on an invalid headers option', async () => {
    const WsTransport = WrpcClient.transport.ws;
    assert.throws(() => new WrpcClient('ws://x', new WsTransport('ws://x'), { headers: ['nope'] }), /options\.headers/);
  });

  it('should kebab-normalize declared keys before any transport sees them', async (t) => {
    const fake = registerFake('kebabfake');
    t.after(() => fake.teardown());
    const client = await WrpcClient.connect('fake://x', {
      transport: ['kebabfake'],
      heartbeat: false,
      logger: false,
      headers: { xAppVersion: '2', 'x-already': 'kebab', numeric: 7, nested: { a: 1 }, missing: null },
      meta: { userId: 1, traceId: 'abc' },
    });
    t.after(() => void client.close());
    const transport = fake.instances.at(-1);

    // One normalization point, upstream of every carrier — which is why ws and
    // the worker transport need no code of their own.
    assert.deepStrictEqual(transport.lastOpen.headers, {
      'x-app-version': '2',
      'x-already': 'kebab',
      numeric: '7', // stringified: over ws this used to be dropped outright
      nested: '{"a":1}',
      // `missing: null` is absent — a header cannot say "absent"
    });
    assert.strictEqual(Object.hasOwn(transport.lastOpen.headers, 'missing'), false);

    // Meta keeps its JSON types in the default carrier: only a header carrier
    // has to flatten, and flattening here would lose information for nothing.
    assert.deepStrictEqual(transport.lastOpen.meta, { 'user-id': 1, 'trace-id': 'abc' });
  });

  it('should drop an unsendable declared header value with a warning, not fail', async (t) => {
    const fake = registerFake('warnfake');
    t.after(() => fake.teardown());
    const warnings = [];
    const client = await WrpcClient.connect('fake://x', {
      transport: ['warnfake'],
      heartbeat: false,
      logger: {
        warn: (entry) => warnings.push(entry),
        info: () => {},
        error: () => {},
        debug: () => {},
        log: () => {},
      },
      headers: { userName: 'José', ok: 'plain' },
    });
    t.after(() => void client.close());
    const transport = fake.instances.at(-1);
    assert.deepStrictEqual(transport.lastOpen.headers, { ok: 'plain' });
    assert.ok(
      warnings.some((entry) => entry.event === 'declared.unsendable' && entry.key === 'user-name'),
      'the drop must be visible on the side that can fix it',
    );
  });

  it("should flatten values with metaFormat 'prefixed' on EVERY transport, not just the header legs", async (t) => {
    const fake = registerFake('pfxfake');
    t.after(() => fake.teardown());
    const client = await WrpcClient.connect('fake://x', {
      transport: ['pfxfake'],
      heartbeat: false,
      logger: false,
      metaFormat: 'prefixed',
      meta: { userId: 7, isRetry: true, nested: { a: 1 }, missing: null, plain: 'x' },
    });
    t.after(() => void client.close());
    const transport = fake.instances.at(-1);

    // The guarantee is about the bag the SERVER observes, not the wire: ws
    // still carries one JSON query parameter, but its values are already
    // flattened, so switching transports cannot change the shape.
    assert.deepStrictEqual(transport.lastOpen.meta, {
      'user-id': '7',
      'is-retry': 'true',
      nested: '{"a":1}',
      plain: 'x',
    });
    assert.strictEqual(transport.lastOpen.metaPrefixed, true);
  });

  it('should throw loudly at construction on an invalid metaFormat', async () => {
    const WsTransport = WrpcClient.transport.ws;
    assert.throws(
      () => new WrpcClient('ws://x', new WsTransport('ws://x'), { metaFormat: 'nope' }),
      /options\.metaFormat/,
    );
    // The default is untouched by the option's absence.
    const fine = new WrpcClient('ws://x', new WsTransport('ws://x'), { meta: { a: 1 } });
    assert.ok(fine);
  });
});

// ---------------------------------------------------------------------------
// The reconnect/refresh edges: refresh re-entry, subscription refresh,
// backoff stability, connect timeout, and the coded settlement of calls a
// dead transport strands.

const net = require('node:net');
const { ClientTransport } = require('../src/client.js');

describe('the reconnect/refresh edges', () => {
  it('should surface the refusal of a call made BY the refresh handler instead of deadlocking', async (t) => {
    const { port, state } = await refreshBoot(t);
    let inner = null;
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      refresh: async (c) => {
        // The handler's own call is refused too (state.ok is still false).
        // Before the joined-run guard this awaited the very promise that was
        // awaiting it — a permanent, client-wide wedge with no timer left to
        // break it.
        inner = await c.call('flaky/get', {}).then(
          () => 'resolved',
          (error) => error.code,
        );
        state.ok = true;
      },
    });
    t.after(() => void client.close());
    await client.load('flaky');
    assert.strictEqual(await client.api.flaky.get(), 'fresh');
    assert.strictEqual(inner, 401, "the handler's own call must surface its 401, not join the refresh");
  });

  const subscriptionRefreshBoot = async (t) => {
    const state = { ok: false, subscribes: 0 };
    const definition = defineRouter({
      feed: {
        ticks: procedure.subscription({
          access: 'public',
          handler: async function* (_context, _args, { signal }) {
            state.subscribes++;
            if (!state.ok) {
              const error = new Error('expired');
              error.code = 401;
              error.expose = true;
              throw error;
            }
            yield 'tick';
            await timers.setTimeout(60_000, undefined, { signal }).catch(() => {});
          },
        }),
      },
    });
    const { server, port } = await createServer(definition);
    t.after(() => server.close());
    return { server, port, state };
  };

  it('should run the refresh on a refused subscribe and re-open the feed', async (t) => {
    const { port, state } = await subscriptionRefreshBoot(t);
    let refreshes = 0;
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      refresh: () => {
        refreshes++;
        state.ok = true;
      },
    });
    t.after(() => void client.close());
    await client.load('feed');
    const values = [];
    const errors = [];
    client.api.feed.ticks.subscribe({}, { onData: (v) => void values.push(v), onError: (e) => void errors.push(e) });
    // The refused subscribe runs the single-flight refresh and re-opens: the
    // feed delivers instead of dying with a terminal 401 while plain calls
    // heal — the silent half-dead client this path used to produce.
    await waitFor(() => values.length === 1, 'the feed never delivered after the refresh');
    assert.deepStrictEqual(values, ['tick']);
    assert.strictEqual(refreshes, 1);
    assert.strictEqual(state.subscribes, 2);
    assert.deepStrictEqual(errors, []);
  });

  it('should make a subscribe refused AGAIN after the refresh terminal — no loop', async (t) => {
    const { port, state } = await subscriptionRefreshBoot(t);
    let refreshes = 0;
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      // "Succeeds" without fixing anything: the retry's refusal must surface.
      refresh: () => void refreshes++,
    });
    t.after(() => void client.close());
    await client.load('feed');
    const errors = [];
    client.api.feed.ticks.subscribe({}, { onData: noop, onError: (e) => void errors.push(e) });
    await waitFor(() => errors.length === 1, 'the second refusal never surfaced');
    assert.strictEqual(errors[0].code, 401);
    assert.strictEqual(refreshes, 1);
    assert.strictEqual(state.subscribes, 2);
    await timers.setTimeout(30);
    assert.strictEqual(state.subscribes, 2, 'a terminal refusal must not keep re-subscribing');
  });

  it('should climb the reconnect backoff for an accept-then-drop peer instead of pinning at minDelay', async (t) => {
    const fake = registerFake('acceptdrop');
    t.after(fake.teardown);
    const client = await WrpcClient.connect('fake://x', {
      transport: ['acceptdrop'],
      heartbeat: false,
      logger: false,
      reconnect: { minDelay: 10, maxDelay: 1000, factor: 2, jitter: false, retries: 3 },
    });
    t.after(() => void client.close());
    const transport = fake.instances.at(-1);
    const delays = [];
    client.on('reconnecting', ({ delay }) => void delays.push(delay));
    const exhausted = new Promise((resolve) => client.on('reconnect-failed', resolve));
    // From now on every open is accepted and immediately dropped by the peer
    // — the shape of a server shedding load mid-restart.
    const accept = transport.open.bind(transport);
    transport.open = async (options) => {
      await accept(options);
      queueMicrotask(() => transport.close());
    };
    transport.close();
    await exhausted;
    // Before the stability window this read [10, 10, 10, ...] forever: every
    // TCP open re-zeroed the counter, retries never exhausted and
    // 'reconnect-failed' never fired.
    assert.deepStrictEqual(delays, [10, 20, 40]);
  });

  it('should reject a handshake that never answers with a coded 408 (connectTimeout)', async (t) => {
    // A TCP listener that accepts and never speaks: the WebSocket handshake
    // neither opens nor errors, which used to park the ladder forever.
    const black = net.createServer(() => {});
    await new Promise((resolve) => black.listen(0, '127.0.0.1', resolve));
    t.after(() => black.close());
    const { port } = black.address();
    const connectionsBefore = WrpcClient.connections.size;
    const started = Date.now();
    await assert.rejects(
      WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
        heartbeat: false,
        logger: false,
        reconnect: false,
        connectTimeout: 50,
      }),
      (error) => error.code === 408 && /Connect timeout/.test(error.message),
    );
    assert.ok(Date.now() - started < 5000, 'settled by connectTimeout, not a transport default');
    assert.strictEqual(WrpcClient.connections.size, connectionsBefore, 'the failed connect must not leave a zombie');
  });

  it('should settle a batched call flushed onto a dead transport with a coded 503, not callTimeout', async (t) => {
    const { server, port } = await createServer(router());
    t.after(() => server.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      batch: true,
      callTimeout: 30_000,
    });
    t.after(() => void client.close());
    client.on('error', noop);
    await client.load('test');
    for (const connection of server.wsServer.connections) connection.terminate();
    await waitFor(() => !client.active, 'the drop never reached the client');
    const started = Date.now();
    // The frame was spliced out of #pending before write threw, so neither
    // #failCalls nor a 'close' could see these ids — the flush itself settles.
    await assert.rejects(client.api.test.hello({}), (error) => error.code === 503);
    assert.ok(Date.now() - started < 5000, 'settled by the flush failure, not the timeout');
  });

  it('should reject a call issued while the transport is down with a coded 503', async (t) => {
    const { server, port } = await createServer(router());
    t.after(() => server.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      callTimeout: 30_000,
    });
    t.after(() => void client.close());
    await client.load('test');
    for (const connection of server.wsServer.connections) connection.terminate();
    await waitFor(() => !client.active, 'the drop never reached the client');
    const started = Date.now();
    await assert.rejects(
      client.api.test.hello({}),
      (error) => error.code === 503 && /Not connected/.test(error.message),
    );
    assert.ok(Date.now() - started < 5000, 'rejected on write, not at the timeout');
  });

  it('should answer in failPackets a request that left as an attachments frame too', () => {
    const { encodeAttachments } = require('../src/attachments.js');
    const transport = new ClientTransport('x');
    const seen = [];
    transport.on('message', (text) => void seen.push(jsonParse(text)));
    const frame = encodeAttachments({ type: 'call', id: 'b', method: 'files/put', args: { body: new Uint8Array(3) } });
    transport.failPackets(frame, 503);
    assert.deepStrictEqual(seen, [
      { type: 'callback', id: 'b', error: { message: 'HTTP request failed (503)', code: 503 } },
    ]);
    // Bytes that are no frame, and a frame that does not decode, answer nothing.
    transport.failPackets(new Uint8Array([1, 2, 3]), 503);
    transport.failPackets(frame.subarray(0, 12), 503);
    assert.strictEqual(seen.length, 1);
  });

  it('should synthesize answers in failPackets for call packets only', () => {
    const transport = new ClientTransport('x');
    const seen = [];
    transport.on('message', (text) => void seen.push(jsonParse(text)));
    const frame = JSON.stringify([{ type: 'call', id: 'a' }, { type: 'subscribe', id: 's' }, { type: 'ping' }]);
    transport.failPackets(frame, 502);
    // The subscribe is NOT answered: its terminal signal is an `end` packet,
    // and a synthesized callback for it would throw in #settle as stale.
    assert.deepStrictEqual(seen, [
      [{ type: 'callback', id: 'a', error: { message: 'HTTP request failed (502)', code: 502 } }],
    ]);
  });

  it('should surface a throwing heartbeat-timeout listener through error, never as an unhandled rejection', async (t) => {
    const httpServer = http.createServer();
    const wsServer = new WebsocketServer({ server: httpServer });
    let answer = true;
    wsServer.on('connection', (ws) => {
      ws.on('message', (raw) => {
        const packet = jsonParse(raw.toString()) || {};
        if (packet.type === 'ping' && answer) ws.send(JSON.stringify({ type: 'pong' }));
      });
    });
    await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
    const { port } = httpServer.address();
    t.after(() => void httpServer.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}`, {
      heartbeat: { interval: 15, timeout: 30 },
      logger: false,
      reconnect: false,
    });
    t.after(() => void client.close());
    const escalated = new Promise((resolve) => client.on('error', resolve));
    client.on('heartbeat-timeout', () => {
      throw new Error('listener exploded');
    });
    answer = false;
    const error = await escalated;
    assert.strictEqual(error.message, 'listener exploded');
  });

  it('should log a failing refresh run and emit refresh-failed, once for all joined callers', async (t) => {
    const wire = registerScripted('scripted-failing');
    t.after(wire.teardown);
    const failures = [];
    let started = 0;
    let release;
    const held = new Promise((resolve) => (release = resolve));
    const client = await WrpcClient.connect('ws://scripted/api', {
      transport: 'scripted-failing',
      heartbeat: false,
      logger: false,
      reconnect: false,
      // Held open until this test releases it, so both refusals are inside the
      // run by construction. A handler that finished before the second refusal
      // landed would start a second run — which is the behaviour under test.
      refresh: async () => {
        started++;
        await held;
        throw new Error('refresh broke');
      },
    });
    t.after(() => void client.close());
    client.on('refresh-failed', (info) => void failures.push(info));

    // Two concurrent refusals join ONE refresh run: the callers surface their
    // original 401s, the run's own failure is reported exactly once.
    const calls = [client.call('flaky/get'), client.call('flaky/get')];
    const issued = wire.calls();
    assert.strictEqual(issued.length, 2);
    for (const packet of issued) wire.refuse(packet);
    await waitFor(() => started === 1, 'the refresh never started');
    release();

    const results = await Promise.allSettled(calls);
    assert.ok(results.every((r) => r.status === 'rejected' && r.reason.code === 401));
    assert.strictEqual(failures.length, 1);
    assert.strictEqual(failures[0].error.message, 'refresh broke');
    assert.strictEqual(failures[0].cause.code, 401);
  });

  it('should let the per-call deadline of CallOptions.timeout beat callTimeout and ride the packet', async (t) => {
    const definition = defineRouter({
      slow: {
        // The server-side budget check: a generous procedure timeout that the
        // caller's packet field must SHORTEN.
        nap: procedure({
          access: 'public',
          timeout: 30_000,
          handler: async (context) => {
            await timers.setTimeout(500, undefined, { signal: context.signal }).catch(() => {});
            return 'done';
          },
        }),
      },
    });
    const { server, port } = await createServer(definition);
    t.after(() => server.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      callTimeout: 30_000,
    });
    t.after(() => void client.close());
    await client.load('slow');
    const started = Date.now();
    await assert.rejects(client.api.slow.nap(undefined, { timeout: 60 }), (error) => error.code === 408);
    assert.ok(Date.now() - started < 5000, 'the per-call deadline, not callTimeout, fired');
  });

  it('should re-issue listed retry codes with fresh packet ids, and surface others as-is', async (t) => {
    const state = { hits: 0, ids: [] };
    const definition = defineRouter({
      flaky: {
        get: procedure({
          access: 'public',
          handler: async (context) => {
            state.hits++;
            state.ids.push(context.uuid);
            if (state.hits < 3) {
              const error = new Error('overloaded');
              error.code = 503;
              error.expose = true;
              throw error;
            }
            return 'finally';
          },
        }),
        teapot: procedure({
          access: 'public',
          handler: async () => {
            const error = new Error('teapot');
            error.code = 418;
            error.expose = true;
            throw error;
          },
        }),
      },
    });
    const { server, port } = await createServer(definition);
    t.after(() => server.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      logger: false,
      reconnect: false,
      retry: { attempts: 2, on: [503], minDelay: 5, maxDelay: 10, jitter: false },
    });
    t.after(() => void client.close());
    await client.load('flaky');
    assert.strictEqual(await client.api.flaky.get(), 'finally');
    assert.strictEqual(state.hits, 3, 'two retries after the first failure');
    assert.strictEqual(new Set(state.ids).size, 3, 'every attempt is its own packet');
    // A code outside the policy surfaces immediately.
    await assert.rejects(client.api.flaky.teapot(), (error) => error.code === 418);
    // Exhausted attempts surface the LAST refusal.
    state.hits = -10; // 503 forever
    await assert.rejects(client.api.flaky.get(), (error) => error.code === 503);
  });
});

// --- The connection's hooks are under connectTimeout ---------------------------

describe('connectTimeout covers the hooks the connection waits on', () => {
  let server = null;
  let url = '';
  const never = () => new Promise(noop);

  before(async () => {
    const booted = await createServer(router());
    server = booted.server;
    url = `ws://127.0.0.1:${booted.port}/api`;
  });

  after(() => server?.close());

  it('should reject connect() with 408 on a headers() that never settles', async () => {
    const started = Date.now();
    await assert.rejects(
      WrpcClient.connect(url, {
        heartbeat: false,
        reconnect: false,
        logger: false,
        connectTimeout: 100,
        headers: never,
      }),
      (error) => error.code === 408,
    );
    assert.ok(Date.now() - started < 2000, 'and does so at the deadline');
  });

  it('should reject connect() and close the connection on an authenticate() that never settles', async () => {
    await assert.rejects(
      WrpcClient.connect(url, {
        heartbeat: false,
        reconnect: false,
        logger: false,
        connectTimeout: 100,
        authenticate: never,
      }),
      (error) => error.code === 408,
    );
    await waitFor(() => server.rpc.clients.size === 0, 'the half-open connection is let go of');
  });

  it('should not make a reconnect whose authenticate() hangs the last one', async (t) => {
    let hang = false;
    let hooks = 0;
    const client = await WrpcClient.connect(url, {
      heartbeat: false,
      logger: false,
      connectTimeout: 100,
      reconnect: { minDelay: 10, maxDelay: 20, jitter: false },
      authenticate: () => {
        hooks++;
        return hang ? never() : undefined;
      },
    });
    t.after(() => void client.close());
    client.on('error', noop);
    hang = true;
    for (const peer of server.rpc.clients) peer.close();
    // It used to stop here for good: the hook of the first reconnect never
    // settled, and nothing was left to schedule a second.
    await waitFor(() => hooks >= 3, { message: 'the cycle goes on past a hung hook', timeout: 5000 });
    hang = false;
    await waitFor(() => client.active && server.rpc.clients.size === 1, 'and a later attempt connects');
  });

  it('should give the call its own refusal back on a refresh() that never settles', async (t) => {
    const guarded = defineRouter({
      unit: { secret: procedure({ handler: async () => 'never' }) },
    });
    const second = await createServer(guarded);
    t.after(() => second.server.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${second.port}/api`, {
      heartbeat: false,
      reconnect: false,
      logger: false,
      connectTimeout: 100,
      refresh: never,
    });
    t.after(() => void client.close());
    client.on('error', noop);
    await assert.rejects(client.call('unit/secret', {}), (error) => error.code === 403);
  });
});

// --- A timeout is a number a timer can hold -------------------------------------

describe("callTimeout and a call's timeout", () => {
  it('should be refused unless a timer can hold them', async (t) => {
    const { server, port } = await createServer(
      router({ hang: procedure({ access: 'public', handler: () => new Promise(noop) }) }),
    );
    t.after(() => server.close());
    const url = `ws://127.0.0.1:${port}/api`;
    // `'300'` is what an environment variable holds. It used to be added to
    // Date.now() as a string: the call never timed out, and a one-millisecond
    // timer was re-armed about eight hundred times a second.
    const quiet = { heartbeat: false, reconnect: false, logger: false };
    for (const callTimeout of ['300', Infinity, -5, 2 ** 31, {}]) {
      await assert.rejects(
        async () => WrpcClient.connect(url, { ...quiet, callTimeout }),
        /callTimeout must be a number of milliseconds/,
        `callTimeout: ${String(callTimeout)}`,
      );
    }
    // Nothing set: the default, as before.
    for (const callTimeout of [undefined, null, 0, false]) {
      const plain = await WrpcClient.connect(url, { ...quiet, callTimeout });
      await plain.close();
    }
    const client = await WrpcClient.connect(url, {
      heartbeat: false,
      reconnect: false,
      logger: false,
      callTimeout: 60,
    });
    t.after(() => void client.close());
    client.on('error', noop);
    await assert.rejects(client.call('test/hang', {}), (error) => error.code === 408);
    // A bad argument, thrown where the call is made.
    assert.throws(() => client.call('test/hang', {}, { timeout: '50' }), /timeout must be a number of milliseconds/);
    assert.throws(
      () => client.call('test/hang', {}, { timeout: Infinity }),
      /timeout must be a number of milliseconds/,
    );
    await assert.rejects(client.call('test/hang', {}, { timeout: 30 }), (error) => error.code === 408);
  });
});

// --- A call that timed out is cancelled on the server ---------------------------

describe('a call that times out', () => {
  it("should tell the server, and make its late answer nobody's error", async (t) => {
    let aborted = 0;
    const { server, port } = await createServer(
      router({
        slow: procedure({
          access: 'public',
          handler: (context) =>
            new Promise((resolve) => {
              context.signal.addEventListener('abort', () => aborted++);
              setTimeout(() => resolve('late'), 150);
            }),
        }),
      }),
    );
    t.after(() => server.close());
    const errors = [];
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      reconnect: false,
      logger: false,
      callTimeout: 40,
    });
    t.after(() => void client.close());
    client.on('error', (error) => errors.push(error.message));
    await assert.rejects(client.call('test/slow', {}), (error) => error.code === 408);
    // The server used to go on with the call: its handler never heard, and the
    // answer came back as `Callback <id> not found` on the 'error' event.
    await waitFor(() => aborted === 1, 'the handler hears the cancel');
    const [peer] = server.rpc.clients;
    assert.strictEqual(peer.calls.size, 0, 'the server holds no call for a caller that left');
    await timers.setTimeout(200);
    assert.deepStrictEqual(errors, []);
    assert.strictEqual(await client.call('test/hello', {}), 'hi', 'the connection is fine');
  });
});

// --- An ask responder's failure is not the asker's to read ----------------------

describe('a responder that throws', () => {
  it('should answer its ask without its internals', async (t) => {
    const { server, port } = await createServer(
      router({
        ask: procedure({
          access: 'public',
          handler: async (context, { name }) => {
            try {
              return await context.client.ask(name, {}, { timeout: 1000 });
            } catch (error) {
              return { failed: { message: error.message, code: error.code, details: error.details ?? null } };
            }
          },
        }),
      }),
    );
    t.after(() => server.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      reconnect: false,
      logger: false,
    });
    t.after(() => void client.close());
    client.on('error', noop);
    client.respond('bug', async () => {
      const config = null;
      return config.secretField;
    });
    client.respond('refusal', async () => {
      throw Object.assign(new Error('Not for you'), { code: 403, details: { why: 'role' } });
    });
    client.respond('exposed', async () => {
      throw Object.assign(new Error('Upstream is down'), { code: 502, expose: true });
    });
    // The TypeError's text used to travel to whoever asked.
    assert.deepStrictEqual(await client.call('test/ask', { name: 'bug' }), {
      failed: { message: 'Responder failed', code: 500, details: null },
    });
    // A 4xx is written for the asker; a 5xx that opts in is too.
    assert.deepStrictEqual(await client.call('test/ask', { name: 'refusal' }), {
      failed: { message: 'Not for you', code: 403, details: { why: 'role' } },
    });
    assert.strictEqual((await client.call('test/ask', { name: 'exposed' })).failed.message, 'Upstream is down');
  });
});
