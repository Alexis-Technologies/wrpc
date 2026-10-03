'use strict';

// End to end over the fake: a real Server (WebSocket engine on TCP), a
// WebTransport session attached beside it, and the ordinary WrpcClient on
// `transport: 'wt'` with the fake's constructor injected.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { WrpcClient, defineRouter, procedure, tracked, createEventLog } = require('../../index.js');
const { bearerAuth, bearerTransport, memoryStore } = require('../../auth.js');
const { attachSession, acceptSessions, fromFails, isWtSession } = require('../../wt.js');
const { createFakeWt } = require('./fakeWebTransport.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const router = () => {
  const log = createEventLog({ size: 16 });
  const positions = [];
  let seq = 0;
  const pub = (handler) => procedure({ access: 'public', handler });
  return defineRouter({
    chat: {
      hello: pub(async (ctx, { name }) => `hi ${name} over ${ctx.client.transportKind}`),
      meta: pub(async (ctx) => ({ headers: { ...ctx.meta.headers }, data: { ...ctx.meta.data } })),
      login: pub(async (ctx, { user }) => {
        ctx.client.startSession(`tok-${user}-${++seq}`, { user });
        return { access: ctx.session.token };
      }),
      whoami: pub(async (ctx) => ctx.session?.state?.user ?? null),
      join: pub(async (ctx, { room }) => void ctx.client.join(room)),
      // Server -> client unreliable delivery, per client and per room.
      nudge: pub(async (ctx, { text }) => void ctx.client.sendEvent('chat/nudge', text, { unreliable: true })),
      wave: pub(async (ctx, { room, text }) => ctx.server.to(room).emit('chat/wave', text, { unreliable: true })),
      // Client -> server unreliable delivery: the unit's inbound handler counts.
      on: {
        position: pub(async (ctx, data) => void positions.push({ kind: ctx.client.transportKind, data })),
      },
      positions: pub(async () => positions.slice()),
      reset: pub(async () => void positions.splice(0)),
      shout: pub(async (ctx, { room, text }) => void ctx.server.to(room).emit('chat/shout', text)),
      ticks: procedure.subscription({
        access: 'public',
        handler: async function* (_ctx, _args, { lastEventId }) {
          for (const event of log.since(lastEventId) ?? []) yield event;
          for (let i = 0; i < 3; i++) yield tracked(log.push({ i }), { i });
        },
      }),
      upload: pub(async (ctx, { stream }) => {
        const readable = ctx.client.getStream(stream);
        let bytes = 0;
        for await (const chunk of readable) bytes += chunk.length;
        return bytes;
      }),
      download: pub(async (ctx, { size }) => {
        const stream = ctx.client.createStream('blob', size);
        stream.write(new Uint8Array(size));
        stream.end();
        return stream.id;
      }),
    },
  });
};

// A world whose sessions are attached to `server` as they arrive.
const attachAll = (t, server, options = {}) => {
  const world = createFakeWt();
  const acceptor = acceptSessions(server, world.sessions, {
    onError: (error) => t.diagnostic(String(error)),
    ...options,
  });
  t.after(() => acceptor.stop());
  return world;
};

const wtClient = (t, world, url, options = {}) =>
  connectClient(t, url, { transport: 'wt', wt: { WebTransport: world.WebTransport }, ...options });

test('wt attach: the capabilities settle the revision — bytes as bytes only between two ends that read frames', async (t) => {
  const bytesRouter = defineRouter({
    b: {
      echo: procedure({
        access: 'public',
        handler: async (_ctx, { blob }) => ({ kind: blob.constructor.name, blob: Uint8Array.of(7, 8) }),
      }),
    },
  });
  // A server with `attachments: false` used to be sent the client's frame
  // anyway — WebTransport was "always revision 2" — refuse it unread, and
  // leave the call to time out.
  for (const [label, serverOptions, clientOptions, revision] of [
    ['both read frames', {}, {}, 2],
    ['the server reads none', { attachments: false }, {}, 1],
    ['the client reads none', {}, { attachments: false }, 1],
  ]) {
    const { server, url } = await bootServer(t, { router: bytesRouter, ...serverOptions });
    const world = attachAll(t, server);
    const client = await wtClient(t, world, url, { callTimeout: 2000, ...clientOptions });
    // Each end's capabilities are its first message: crossed before the load is answered.
    await client.load('b');
    const answer = await client.api.b.echo({ blob: Uint8Array.of(1, 2) });
    assert.strictEqual(client.revision, revision, label);
    const [peer] = server.rpc.clients;
    assert.strictEqual(peer.revision, revision, `${label}, on the server`);
    if (revision === 2) {
      assert.strictEqual(answer.kind, 'Uint8Array', label);
      assert.ok(answer.blob instanceof Uint8Array, label);
    } else {
      assert.strictEqual(answer.kind, 'Object', label);
      assert.deepStrictEqual(answer.blob, { 0: 7, 1: 8 }, label);
    }
  }
});

test('wt attach: a WebTransport client calls, receives events, subscribes and streams beside a ws client', async (t) => {
  const { server, url } = await bootServer(t, { router: router() });
  const world = attachAll(t, server);
  const client = await wtClient(t, world, url);
  await client.load('chat');
  assert.strictEqual(await client.api.chat.hello({ name: 'ann' }), 'hi ann over wt');
  await waitFor(() => server.rpc.clients.size === 1, 'one client');
  const [serverClient] = server.rpc.clients;
  assert.strictEqual(serverClient.transportKind, 'wt');
  assert.strictEqual(serverClient.persistent, true);

  // Rooms are shared with a ws client on the same server.
  const ws = await connectClient(t, url);
  await ws.load('chat');
  await ws.api.chat.join({ room: 'r' });
  await client.api.chat.join({ room: 'r' });
  const heard = [];
  client.api.chat.on('shout', (text) => heard.push(text));
  await ws.api.chat.shout({ room: 'r', text: 'from ws' });
  await waitFor(() => heard.length === 1, 'event over wt');
  assert.deepStrictEqual(heard, ['from ws']);

  // A subscription with tracked values.
  const seen = [];
  for await (const value of client.api.chat.ticks.iterate({})) {
    seen.push(value.i);
    if (seen.length === 3) break;
  }
  assert.deepStrictEqual(seen, [0, 1, 2]);

  // Binary streams in both directions.
  const upload = client.createStream('data', 70_000);
  const bytes = new Uint8Array(70_000);
  upload.write(bytes);
  upload.end();
  assert.strictEqual(await client.api.chat.upload({ stream: upload.id }), 70_000);
  const id = await client.api.chat.download({ size: 40_000 });
  const readable = client.getStream(id);
  let total = 0;
  for await (const chunk of readable) total += chunk.length;
  assert.strictEqual(total, 40_000);
});

test('wt attach: declared headers, meta and a bearer session ride the connect URL; cookies never do', async (t) => {
  const { server, url } = await bootServer(t, {
    router: router(),
    sessions: { transport: bearerTransport() },
  });
  const world = attachAll(t, server);
  const client = await wtClient(t, world, url, { headers: { 'x-device': 'tablet' }, meta: { tenant: 'acme' } });
  await client.load('chat');
  const meta = await client.api.chat.meta({});
  assert.strictEqual(meta.headers['x-device'], 'tablet');
  assert.strictEqual(meta.headers.origin, 'https://app.example');
  assert.deepStrictEqual(meta.data, { tenant: 'acme' });

  // bearerAuth signs in on the first connect and presents the token as a
  // Bearer credential on the next: bearerTransport reads
  // `declared.authorization`, which the wt transport put in wrpc_h — the
  // CONNECT request itself carries no cookie and no Authorization header.
  const store = memoryStore();
  const first = await wtClient(
    t,
    world,
    url,
    bearerAuth({ store, signIn: (c) => c.call('chat/login', { user: 'ann' }) }),
  );
  await first.load('chat');
  assert.strictEqual(await first.api.chat.whoami({}), 'ann');
  const stored = await store.get('tokens');
  assert.ok(stored.access.startsWith('tok-ann-'));
  const again = await wtClient(
    t,
    world,
    url,
    bearerAuth({ store, signIn: () => assert.fail('a stored token needs no sign-in') }),
  );
  await again.load('chat');
  assert.strictEqual(await again.api.chat.whoami({}), 'ann');
  const [, , third] = server.rpc.clients;
  assert.strictEqual(third.meta.headers.authorization, `Bearer ${stored.access}`);
});

test('wt attach: attachSession refuses on verify and on a silent client, and validates its inputs', async (t) => {
  const { server, url } = await bootServer(t, { router: router() });
  const world = createFakeWt();
  const client = new world.WebTransport(url);
  await client.ready;
  const session = await world.next();
  assert.strictEqual(isWtSession(session), true);
  const { recorder } = require('../helpers/recorder.js');
  const log = recorder();
  const refused = await attachSession(server, session, {
    ...fromFails(session),
    remoteAddress: '203.0.113.9',
    verify: () => false,
    logger: log.writer,
  });
  assert.strictEqual(refused, null);
  assert.deepStrictEqual(await client.closed, { closeCode: 403, reason: 'Forbidden' });

  const silent = new world.WebTransport(url);
  await silent.ready;
  const late = await world.next();
  const timedOut = await attachSession(server.rpc, late, { acceptTimeout: 20, logger: log.writer });
  assert.strictEqual(timedOut, null);
  assert.deepStrictEqual(await silent.closed, { closeCode: 408, reason: 'No control stream' });
  // Both refusals are lines, on a child bound to the peer where one is known.
  assert.deepStrictEqual(
    log.all('wt.refused').map((e) => [e.level, e.component, e.peer, e.status, e.reason]),
    [
      ['warn', 'wt', '203.0.113.9', 403, 'verify'],
      ['warn', 'wt', undefined, 408, 'timeout'],
    ],
  );

  // ONE deadline for the whole path: a session whose `ready` never settles,
  // or a verify that never answers, is a 408 too — it used to hold
  // attachSession forever, outside acceptTimeout.
  const never = () => new Promise(() => {});
  const hanging = (extra) => {
    const closes = [];
    const session = {
      incomingBidirectionalStreams: new ReadableStream(),
      createBidirectionalStream: never,
      close: (info) => closes.push(info),
      closed: never(),
      ...extra,
    };
    return { session, closes };
  };
  const notReady = hanging({ ready: never() });
  assert.strictEqual(await attachSession(server, notReady.session, { acceptTimeout: 20 }), null);
  assert.deepStrictEqual(notReady.closes, [{ closeCode: 408, reason: 'No control stream' }]);
  const stuckVerify = hanging({});
  assert.strictEqual(await attachSession(server, stuckVerify.session, { acceptTimeout: 20, verify: never }), null);
  assert.deepStrictEqual(stuckVerify.closes, [{ closeCode: 408, reason: 'No control stream' }]);
  // And a signal ends an accept still in its handshake with 1001.
  const controller = new AbortController();
  const stopped = hanging({});
  const attaching = attachSession(server, stopped.session, { acceptTimeout: 5_000, signal: controller.signal });
  await timers.setTimeout(5);
  controller.abort();
  assert.strictEqual(await attaching, null);
  assert.deepStrictEqual(stopped.closes, [{ closeCode: 1001, reason: 'Server is closing' }]);

  await assert.rejects(attachSession({}, session), TypeError);
  await assert.rejects(attachSession(server, {}), TypeError);
  await assert.rejects(attachSession(server, late, { acceptTimeout: 0 }), TypeError);
  await assert.rejects(attachSession(server, late, { maxBackpressure: -1 }), /maxBackpressure/);
  assert.throws(() => acceptSessions(server, 42), TypeError);
  assert.throws(() => acceptSessions(server, createFakeWt().sessions, { maxPending: 0 }), TypeError);
  assert.strictEqual(server.rpc.clients.size, 0);
});

test('wt attach: a silent session does not block the sessions behind it; stop() closes what is still handshaking', async (t) => {
  const { server, url } = await bootServer(t, { router: router() });
  const world = createFakeWt();
  const clients = [];
  const acceptor = acceptSessions(server, world.sessions, {
    acceptTimeout: 5_000,
    onClient: (client) => clients.push(client),
    onError: (error) => t.diagnostic(String(error)),
  });
  t.after(() => acceptor.stop());
  // A peer that connected and went silent: no control stream, ever.
  const silent = new world.WebTransport(url);
  await silent.ready;
  await timers.setTimeout(20);
  // The next client is attached at once — the silent one waits out its
  // acceptTimeout on its own, not in front of everybody.
  const started = Date.now();
  const client = await wtClient(t, world, url);
  assert.ok(Date.now() - started < 1000, `attached in ${Date.now() - started} ms`);
  await waitFor(() => clients.length === 1, 'accepted');
  assert.strictEqual(client.active, true);
  // stop(): the silent session is closed now, not at its timeout, and
  // `done` waits for it.
  const closed = silent.closed;
  await acceptor.stop();
  world.close();
  await acceptor.done;
  assert.deepStrictEqual(await closed, { closeCode: 1001, reason: 'Server is closing' });
});

test('wt attach: past maxPending a session is refused 503 at once, said once per episode', async (t) => {
  const { server, url } = await bootServer(t, { router: router() });
  const world = createFakeWt();
  const warnings = [];
  const logger = {
    log() {},
    info() {},
    debug() {},
    error() {},
    warn: (entry) => warnings.push(entry),
    child: () => logger,
  };
  const acceptor = acceptSessions(server, world.sessions, { acceptTimeout: 5_000, maxPending: 1, logger });
  t.after(() => acceptor.stop());
  const silent = new world.WebTransport(url);
  await silent.ready;
  await timers.setTimeout(20);
  for (let i = 0; i < 2; i++) {
    const refused = new world.WebTransport(url);
    await refused.ready;
    assert.deepStrictEqual(await refused.closed, { closeCode: 503, reason: 'Busy' });
  }
  assert.deepStrictEqual(
    warnings.filter((w) => w.event === 'wt.accept.saturated').map((w) => [w.pending, w.maxPending]),
    [[1, 1]],
  );
});

test('wt attach: acceptSessions over an iterable, meta hook, onClient, stop', async (t) => {
  const { server, url } = await bootServer(t, { router: router() });
  const world = createFakeWt();
  const clients = [];
  const errors = [];
  const sessions = [];
  const acceptor = acceptSessions(
    server,
    {
      async *[Symbol.asyncIterator]() {
        for (;;) {
          let session;
          try {
            session = await world.next();
          } catch {
            return; // the world closed
          }
          sessions.push(session);
          yield session;
        }
      },
    },
    {
      meta: (session) => ({ ...fromFails(session), remoteAddress: '10.0.0.7' }),
      onClient: (client) => clients.push(client),
      onError: (error) => errors.push(error),
    },
  );
  t.after(() => acceptor.stop());
  const client = await wtClient(t, world, url);
  await waitFor(() => clients.length === 1, 'accepted');
  assert.strictEqual(clients[0].transportKind, 'wt');
  assert.strictEqual(clients[0].meta.remoteAddress, '10.0.0.7');
  assert.strictEqual(client.active, true);
  await acceptor.stop();
  // The generator is suspended on world.next(); ending the world settles it.
  world.close();
  await acceptor.done;
  assert.deepStrictEqual(errors, []);
  assert.strictEqual(sessions.length, 1);

  // A source that throws ends the loop through onError, and done resolves.
  const thrown = [];
  const failing = acceptSessions(
    server,
    (async function* () {
      yield* [];
      throw new Error('source broke');
    })(),
    { onError: (error) => thrown.push(error.message) },
  );
  await failing.done;
  assert.deepStrictEqual(thrown, ['source broke']);

  // An onError that throws itself — closing the `null` a failed source is
  // reported with is the easy way — is a line in the server's log, never a
  // rejection of `done`: it is documented as always resolving, and nobody
  // awaits it with a catch.
  const { recorder } = require('../helpers/recorder.js');
  const log = recorder();
  const unhandled = [];
  const onUnhandled = (error) => unhandled.push(error);
  process.on('unhandledRejection', onUnhandled);
  t.after(() => process.off('unhandledRejection', onUnhandled));
  const careless = acceptSessions(
    server,
    (async function* () {
      yield* [];
      throw new Error('source broke again');
    })(),
    { logger: log.writer, onError: (_error, session) => session.close() },
  );
  await careless.done;
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepStrictEqual(unhandled, []);
  const line = log.find('wt.onError');
  assert.deepStrictEqual([line.level, line.component, line.err.name], ['error', 'wt', 'TypeError']);
});

test('wt attach: the server closing a client says goodbye with 1001, and the client reconnects over wt', async (t) => {
  const { server, url } = await bootServer(t, { router: router() });
  const sessions = [];
  const world = attachAll(t, server, { onClient: (_client, session) => sessions.push(session) });
  const client = await wtClient(t, world, url, {
    reconnect: { minDelay: 5, maxDelay: 10, jitter: false, retries: 3 },
  });
  await client.load('chat');
  const [serverClient] = server.rpc.clients;
  assert.strictEqual(serverClient.transportKind, 'wt');
  const reconnects = [];
  client.on('reconnect', () => reconnects.push(1));
  // The server-side transport's close is the 1001 goodbye a WebSocket gets;
  // the fake settles both ends' `closed` with it.
  const closed = sessions[0].closed;
  serverClient.close();
  assert.deepStrictEqual(await closed, { closeCode: 1001, reason: 'Server is closing' });
  await waitFor(() => reconnects.length === 1, 'reconnected over wt');
  assert.strictEqual(await client.api.chat.hello({ name: 'bob' }), 'hi bob over wt');
  assert.strictEqual(sessions.length, 2);
  await timers.setImmediate();
});

test('wt attach: WrpcClient.transport.wt is the one connect() constructs', () => {
  assert.strictEqual(typeof WrpcClient.transport.wt, 'function');
});

test('wt attach: unreliable events ride datagrams on wt, the control stream when too big, and the socket on ws', async (t) => {
  const { server, url } = await bootServer(t, { router: router() });
  const world = createFakeWt({ maxDatagramSize: 200 });
  const acceptor = acceptSessions(server, world.sessions);
  t.after(() => acceptor.stop());
  const client = await wtClient(t, world, url);
  await client.load('chat');
  const ws = await connectClient(t, url);
  await ws.load('chat');
  await client.api.chat.join({ room: 'r' });
  await ws.api.chat.join({ room: 'r' });

  // Server -> client: a datagram to the wt client, the socket to the ws one.
  const nudges = [];
  const waves = { wt: [], ws: [] };
  client.api.chat.on('nudge', (text) => nudges.push(text));
  client.api.chat.on('wave', (text) => waves.wt.push(text));
  ws.api.chat.on('wave', (text) => waves.ws.push(text));
  await client.api.chat.nudge({ text: 'small' });
  await client.api.chat.nudge({ text: 'x'.repeat(500) }); // does not fit: reliable
  await waitFor(() => nudges.length === 2, 'nudges');
  assert.deepStrictEqual(nudges, ['small', 'x'.repeat(500)]);
  const recipients = await ws.api.chat.wave({ room: 'r', text: 'hi' });
  assert.strictEqual(recipients, 2);
  await waitFor(() => waves.wt.length === 1 && waves.ws.length === 1, 'waves');

  // Client -> server: the wt client's datagram, the ws client's socket.
  client.sendEvent('chat/position', { x: 1 }, { unreliable: true });
  ws.sendEvent('chat/position', { x: 2 }, { unreliable: true });
  await waitFor(async () => (await client.api.chat.positions({})).length === 2, 'positions');
  assert.deepStrictEqual((await client.api.chat.positions({})).map((p) => p.kind).sort(), ['ws', 'wt']);
  await client.api.chat.reset({});

  // A lossy network loses some: the server's count is short, and nothing errors.
  world.lossy(1);
  await client.api.chat.nudge({ text: 'lost' });
  client.sendEvent('chat/position', { x: 3 }, { unreliable: true });
  await timers.setTimeout(20);
  assert.deepStrictEqual(nudges.length, 2);
  assert.deepStrictEqual(await client.api.chat.positions({}), []);
  world.lossy(0);

  // An ask can never be unreliable.
  const [attached] = server.rpc.clients;
  assert.throws(() => attached.ask('chat/x', {}, { unreliable: true }), TypeError);
});

test('wt attach: an upload into a slow handler is paced by the readable — the pause reaches the side stream', async (t) => {
  // 64 chunks of 64 KiB on a stream of their own, into a handler that
  // takes its time: the core pauses the socket while the readable is over
  // its high-water mark, and the side stream must stop too. It used to
  // keep flowing around the pause, and the readable's mark had to grow
  // (checkStreamLimits) to hold what arrived — an overflow in the making.
  let watched = null;
  const initial = { mark: 0, peak: 0 };
  const slow = defineRouter({
    files: {
      sink: procedure({
        access: 'public',
        handler: async (ctx, { stream }) => {
          const readable = ctx.client.getStream(stream);
          watched = readable;
          initial.mark = readable.highWaterMark;
          let bytes = 0;
          for await (const chunk of readable) {
            initial.peak = Math.max(initial.peak, readable.queue.length);
            await timers.setTimeout(2);
            bytes += chunk.length;
          }
          return bytes;
        },
      }),
    },
  });
  const { server, url } = await bootServer(t, { router: slow });
  const world = attachAll(t, server);
  const client = await wtClient(t, world, url);
  await client.load('files');
  const chunk = 64 * 1024;
  const count = 64;
  const upload = client.createStream('data', chunk * count);
  const result = client.api.files.sink({ stream: upload.id });
  for (let i = 0; i < count; i++) {
    if (!upload.write(new Uint8Array(chunk).fill(i)) && !upload.closed) {
      await new Promise((resolve) => upload.once('drain', resolve));
    }
  }
  upload.end();
  assert.strictEqual(await result, chunk * count);
  assert.ok(watched !== null);
  assert.strictEqual(watched.highWaterMark, initial.mark, 'the mark never had to grow');
  assert.ok(initial.peak <= initial.mark + 2, `the queue peaked at ${initial.peak} against a mark of ${initial.mark}`);
});
