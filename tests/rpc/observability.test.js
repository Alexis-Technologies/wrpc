'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { RpcServer, WrpcClient, defineRouter, procedure } = require('../../index.js');
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

const boot = async (t, options) => {
  const server = new Server({
    router: createRouter(),
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    timeouts: { bind: 100 },
    ...options,
  });
  await server.listen();
  t.after(() => server.close());
  return `ws://127.0.0.1:${server.address().port}/api`;
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
  await t.test('the rooms child labels a backplane serialization failure', async () => {
    const { logger, entries } = collector();
    const rpc = new RpcServer({ router: createRouter(), logger, backplane: new MemoryBackplane() });
    const circular = {};
    circular.self = circular;
    rpc.broadcast('ping', circular);
    const failure = entries.find((entry) => entry.event === 'backplane.serialize');
    assert.ok(failure, 'the non-serializable envelope was reported');
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
  await assert.rejects(client.api.probe.boom(), /handler exploded/);
  await client.close();
});
