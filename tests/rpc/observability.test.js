'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { RpcServer, WrpcClient, defineRouter, procedure } = require('../../index.js');
const { bootServer } = require('../helpers/server.js');
const { Server } = require('../../index.js');
const { MemoryBackplane } = require('../../scaling.js');

// A pino-shaped collector: `child` and `level` are what mark a sink
// structured, and the merged bindings are what this file is really testing —
// they are the only evidence that the per-component and per-connection
// children were built and handed down.
const collector = () => {
  const entries = [];
  const make = (bindings) => ({
    level: 'trace',
    bindings,
    child: (extra) => make({ ...bindings, ...extra }),
    log: (entry, message) => entries.push({ level: 'log', ...bindings, ...entry, message }),
    info: (entry, message) => entries.push({ level: 'info', ...bindings, ...entry, message }),
    debug: (entry, message) => entries.push({ level: 'debug', ...bindings, ...entry, message }),
    warn: (entry, message) => entries.push({ level: 'warn', ...bindings, ...entry, message }),
    error: (entry, message) => entries.push({ level: 'error', ...bindings, ...entry, message }),
  });
  return { logger: make({}), entries };
};

const createRouter = () =>
  defineRouter({
    probe: {
      echo: procedure({ access: 'public', handler: async (_context, args) => args }),
      boom: procedure({
        access: 'public',
        handler: async () => {
          throw new Error('handler exploded');
        },
      }),
    },
  });

// Delegates to the shared boot (tests/helpers/server.js); `logger` arrives
// through options, overriding the shared quiet default.
const boot = async (t, options) => {
  const { url } = await bootServer(t, { router: createRouter(), ...options });
  return url;
};

test('the logger reaches every component of a live server', async (t) => {
  const { logger, entries } = collector();
  const url = await boot(t, { logger });

  const client = await WrpcClient.connect(url, { reconnect: false });
  await client.load('probe');
  await client.api.probe.echo({ ok: 1 });
  await assert.rejects(client.api.probe.boom());
  await client.close();

  await t.test('the listen line carries its event', () => {
    const listen = entries.find((entry) => entry.event === 'listen');
    assert.ok(listen, 'a listen entry was written');
    assert.strictEqual(listen.level, 'info');
  });

  await t.test('a successful call is logged with its method and peer', () => {
    const ok = entries.find((entry) => entry.event === 'call.ok' && entry.method === 'probe/echo');
    assert.ok(ok, 'the probe/echo call was logged');
    assert.ok(typeof ok.peer === 'string' && ok.peer.length > 0, 'the connection binding is present');
    assert.strictEqual(ok.message, `${ok.peer}\tCALL\tprobe/echo\tOK`);
  });

  await t.test('a failing handler is logged as an error with its code', () => {
    const failure = entries.find((entry) => entry.event === 'rpc.error');
    assert.ok(failure, 'an rpc.error entry was written');
    assert.strictEqual(failure.code, 500);
    assert.ok(failure.err instanceof Error);
    assert.ok(typeof failure.peer === 'string', 'errors carry the connection binding too');
  });
});

test('component children are bound where the plan says they are', async (t) => {
  await t.test('the rooms child labels a broadcast serialization failure', async () => {
    const { logger, entries } = collector();
    const rpc = new RpcServer({ router: createRouter(), logger, backplane: new MemoryBackplane() });
    const circular = {};
    circular.self = circular;
    // The fan-out serializes ONCE now, so the failure surfaces there — before
    // local delivery and before the backplane publish — and is reported, not
    // thrown at the broadcaster.
    assert.strictEqual(rpc.broadcast('ping', circular), 0);
    const failure = entries.find((entry) => entry.event === 'broadcast.serialize');
    assert.ok(failure, 'the non-serializable payload was reported');
    assert.strictEqual(failure.component, 'rooms');
    assert.ok(failure.err instanceof Error);
  });
});

test('logger: false is silent everywhere', async (t) => {
  const written = [];
  const original = globalThis.console;
  globalThis.console = {
    log: (...args) => written.push(args),
    info: (...args) => written.push(args),
    debug: (...args) => written.push(args),
    warn: (...args) => written.push(args),
    error: (...args) => written.push(args),
  };
  try {
    const url = await boot(t, { logger: false });
    const client = await WrpcClient.connect(url, { reconnect: false });
    await client.load('probe');
    await client.api.probe.echo({ ok: 1 });
    await assert.rejects(client.api.probe.boom());
    await client.close();
  } finally {
    globalThis.console = original;
  }
  assert.deepStrictEqual(written, [], 'nothing leaked to the global console');
});

test('a logger that throws never breaks a call', async (t) => {
  const exploding = {
    level: 'trace',
    child() {
      return this;
    },
  };
  for (const level of ['log', 'info', 'debug', 'warn', 'error']) {
    exploding[level] = () => {
      throw new Error(`${level} exploded`);
    };
  }
  const url = await boot(t, { logger: exploding });
  const client = await WrpcClient.connect(url, { reconnect: false });
  await client.load('probe');
  assert.deepStrictEqual(await client.api.probe.echo({ ok: 1 }), { ok: 1 });
  // An uncaught handler error is a 500 whose message stays in the server
  // log: the peer sees the status line, not the exception text.
  await assert.rejects(
    client.api.probe.boom(),
    (error) => error.code === 500 && /Internal Server Error/.test(error.message),
  );
  await client.close();
});

test('the client logger is off by default and complete when on', async (t) => {
  await t.test('no logger means nothing is written', async () => {
    const url = await boot(t, { logger: false });
    const written = [];
    const original = globalThis.console;
    globalThis.console = { log: (...a) => written.push(a), error: (...a) => written.push(a) };
    try {
      const client = await WrpcClient.connect(url, { reconnect: false });
      await client.load('probe');
      await assert.rejects(client.api.probe.boom());
      await client.close();
    } finally {
      globalThis.console = original;
    }
    assert.deepStrictEqual(written, [], 'a client without a logger stays silent');
  });

  await t.test('open and close are logged with the url', async () => {
    const url = await boot(t, { logger: false });
    const { logger, entries } = collector();
    const WsTransport = WrpcClient.transport.ws;
    const transport = new WsTransport(url);
    const client = new WrpcClient(url, transport, { reconnect: false, logger });
    await client.open();
    const open = entries.find((entry) => entry.event === 'open');
    assert.ok(open, 'the open was logged');
    assert.strictEqual(open.url, url);
    await new Promise((resolve) => {
      transport.once('close', resolve);
      client.close();
    });
    assert.ok(
      entries.some((entry) => entry.event === 'close'),
      'the close was logged',
    );
  });

  await t.test('a client error is logged AND emitted, not one or the other', async () => {
    const url = await boot(t, { logger: false });
    const { logger, entries } = collector();
    const WsTransport = WrpcClient.transport.ws;
    const transport = new WsTransport(url);
    const client = new WrpcClient(url, transport, { reconnect: false, logger });
    await client.open();
    const seen = [];
    client.on('error', (error) => seen.push(error));
    const boom = new Error('transport blew up');
    await transport.emit('error', boom);
    client.close();
    assert.deepStrictEqual(seen, [boom], 'the listener still ran');
    const logged = entries.find((entry) => entry.event === 'transport.error');
    assert.ok(logged, 'and the logger observed it too');
    assert.strictEqual(logged.err, boom);
  });
});

test('a subscription that dies server-side is logged', async (t) => {
  const { logger, entries } = collector();
  const server = new Server({
    router: defineRouter({
      feed: {
        broken: procedure({
          access: 'public',
          subscription: true,
          handler: async function* () {
            yield 1;
            throw new Error('feed collapsed');
          },
        }),
      },
    }),
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    timeouts: { bind: 100 },
    logger,
  });
  await server.listen();
  t.after(() => server.close());

  const client = await WrpcClient.connect(`ws://127.0.0.1:${server.address().port}/api`, { reconnect: false });
  await client.load('feed');
  const errors = [];
  await new Promise((resolve) => {
    client.api.feed.broken.subscribe(
      {},
      {
        onError: (error) => {
          errors.push(error);
          resolve();
        },
      },
    );
  });
  await client.close();

  assert.strictEqual(errors.length, 1);
  const ended = entries.find((entry) => entry.event === 'subscribe.end' && entry.code === 500);
  assert.ok(ended, 'the dead subscription was logged with its code');
  assert.strictEqual(ended.method, 'feed/broken');
});

test('a malformed packet is reported through the one funnel it passes', async (t) => {
  const { logger, entries } = collector();
  const url = await boot(t, { logger });
  const client = await WrpcClient.connect(url, { reconnect: false });
  // The server answers the garbage with a 500 the client cannot route; a
  // listener keeps that expected escalation out of the test run's output.
  client.on('error', () => {});
  client.write('this is not json'); // straight onto the wire, bypassing send()
  await new Promise((resolve) => setTimeout(resolve, 50));
  client.close();
  const malformed = entries.find((entry) => entry.event === 'packet.malformed');
  assert.ok(malformed, 'the unparseable frame was reported');
  assert.ok(malformed.bytes > 0);
});
