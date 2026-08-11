'use strict';

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { Server, WrpcClient, defineRouter, procedure } = require('../../index.js');
const { handleMessage } = require('../../src/rpc/dispatcher.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

// ---------------------------------------------------------------------------
// Router level: `on` declares handlers, not a method

test('router: the reserved `on` key holds event handlers', async (t) => {
  const router = defineRouter({
    chat: {
      send: async () => ({ ok: true }),
      on: {
        typing: async () => {},
        seen: { access: 'session', handler: async () => {} },
      },
    },
    'chat.1': {
      on: { typing: async () => {} },
    },
  });

  await t.test('handlers are looked up per unit and version', () => {
    assert.ok(router.getEventHandler('chat', '*', 'typing'));
    assert.ok(router.getEventHandler('chat', '1', 'typing'));
    assert.strictEqual(router.getEventHandler('chat', '1', 'seen'), null);
    assert.strictEqual(router.getEventHandler('chat', '*', 'nothing'), null);
    assert.strictEqual(router.getEventHandler('nowhere', '*', 'typing'), null);
  });

  await t.test('handlers are procedures, so access carries over', () => {
    assert.strictEqual(router.getEventHandler('chat', '*', 'typing').access, 'session');
    assert.strictEqual(router.getEventHandler('chat', '*', 'seen').access, 'session');
  });

  await t.test('`on` is not a callable method and not introspected', () => {
    assert.strictEqual(router.getProcedure('chat', '*', 'on'), null);
    const introspection = router.introspect();
    assert.deepStrictEqual(Object.keys(introspection.chat), ['send']);
    assert.deepStrictEqual(Object.keys(introspection['chat.1']), []);
  });

  await t.test('merge carries the event handlers over', () => {
    const merged = router.merge(defineRouter({ other: { on: { hello: async () => {} } } }));
    assert.ok(merged.getEventHandler('chat', '*', 'typing'));
    assert.ok(merged.getEventHandler('chat', '1', 'typing'));
    assert.ok(merged.getEventHandler('other', '*', 'hello'));
    assert.ok(merged.getProcedure('chat', '*', 'send'));
  });

  await t.test('a malformed `on` is rejected', () => {
    assert.throws(() => defineRouter({ chat: { on: 42 } }), TypeError);
    assert.throws(() => defineRouter({ chat: { on: { typing: 42 } } }), TypeError);
  });
});

// ---------------------------------------------------------------------------
// Wire level: client -> server events over a live connection

const createServer = async (router, console) => {
  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    console,
    timeouts: { bind: 100 },
  });
  await server.listen();
  return { server, port: server.address().port };
};

test('events: a client event reaches its router handler', async (t) => {
  const seen = [];
  const warnings = [];
  const router = defineRouter({
    chat: {
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
      on: {
        typing: procedure({
          access: 'public',
          handler: async (context, data) => {
            seen.push({ data, uuid: typeof context.uuid, source: typeof context.client.source });
          },
        }),
        checked: procedure({
          access: 'public',
          input: (value) => {
            if (!value?.ok) throw new Error('not ok');
            return value;
          },
          handler: async (_context, data) => void seen.push({ data }),
        }),
        boom: procedure({
          access: 'public',
          handler: async () => {
            throw new Error('handler blew up');
          },
        }),
        private: procedure({ access: 'session', handler: async () => void seen.push('private') }),
      },
    },
  });
  const console = { ...quiet, warn: (message) => warnings.push(message) };
  const { server, port } = await createServer(router, console);
  t.after(() => server.close());

  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`);
  t.after(() => void client.close());
  await client.load('chat');

  await t.test('the handler receives the data and a context', async () => {
    client.sendEvent('chat/typing', { on: true });
    await waitFor(() => seen.length === 1, 'the event never arrived');
    assert.deepStrictEqual(seen[0], { data: { on: true }, uuid: 'string', source: 'string' });
  });

  await t.test('an event never produces a wire answer', async () => {
    seen.length = 0;
    client.sendEvent('chat/typing', { on: false });
    // The connection is still usable, and nothing came back on it that a
    // call would have to demultiplex around.
    assert.strictEqual(await client.api.chat.ping(), 'pong');
    assert.strictEqual(seen.length, 1);
  });

  await t.test('an unknown event is dropped with a log line', async () => {
    warnings.length = 0;
    client.sendEvent('chat/nothing', {});
    client.sendEvent('nowhere/typing', {});
    await waitFor(() => warnings.length === 2, 'the unknown events were not reported');
    assert.match(warnings[0], /chat\/nothing\tno handler/);
    assert.strictEqual(await client.api.chat.ping(), 'pong', 'the connection survives');
  });

  await t.test('a session-only event without a session is dropped', async () => {
    warnings.length = 0;
    seen.length = 0;
    client.sendEvent('chat/private', {});
    await waitFor(() => warnings.length === 1, 'the denied event was not reported');
    assert.match(warnings[0], /chat\/private\tsession required/);
    assert.deepStrictEqual(seen, []);
  });

  await t.test('a failing handler is reported, not thrown at the peer', async () => {
    warnings.length = 0;
    client.sendEvent('chat/boom', {});
    await waitFor(() => warnings.length === 1, 'the handler failure was not reported');
    assert.match(warnings[0], /chat\/boom\t.*handler blew up/s);
    assert.strictEqual(await client.api.chat.ping(), 'pong');
  });

  await t.test('input validation applies to events too', async () => {
    warnings.length = 0;
    seen.length = 0;
    client.sendEvent('chat/checked', { ok: false });
    await waitFor(() => warnings.length === 1, 'the invalid event was not reported');
    assert.match(warnings[0], /chat\/checked\t.*not ok/s);

    client.sendEvent('chat/checked', { ok: true });
    await waitFor(() => seen.length === 1, 'the valid event never arrived');
  });

  await t.test('a stray pong on a persistent transport is ignored', async () => {
    const failures = [];
    client.on('error', (error) => void failures.push(error));
    client.send({ type: 'pong' });
    await timers.setTimeout(30);
    assert.deepStrictEqual(failures, [], 'a pong nobody asked for is not a protocol error');
    assert.strictEqual(await client.api.chat.ping(), 'pong');
    client.clear('error');
  });

  await t.test('an event packet with no name is a structure error', async () => {
    client.send({ type: 'event', data: 1 });
    // The server answers the malformed packet with an id-less error packet,
    // which the client reports rather than resolving anything.
    const failure = new Promise((resolve) => client.once('error', resolve));
    assert.ok(await failure);
    assert.strictEqual(await client.api.chat.ping(), 'pong');
  });
});

test('events: a request/response transport is answered instead of pinned', async (t) => {
  let invocations = 0;
  const router = defineRouter({
    chat: {
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
      on: { typing: procedure({ access: 'public', handler: async () => void invocations++ }) },
    },
  });
  const { server, port } = await createServer(router, quiet);
  t.after(() => server.close());
  const base = `http://127.0.0.1:${port}/api`;

  const post = async (packet) => {
    const res = await fetch(base, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(packet),
    });
    return { status: res.status, body: await res.json() };
  };

  await t.test('an event over HTTP is refused, not silently dropped', async () => {
    const { status, body } = await post({ type: 'event', name: 'chat/typing', data: {} });
    assert.strictEqual(status, 400);
    assert.strictEqual(body.error.code, 400);
    assert.match(body.error.message, /persistent connection/);
    assert.strictEqual(invocations, 0, 'the handler must not run for a transport that cannot carry events');
  });

  await t.test('a stray pong over HTTP is a structure error', async () => {
    const { status, body } = await post({ type: 'pong' });
    assert.strictEqual(status, 500);
    assert.strictEqual(body.error.code, 500);
  });

  await t.test('a ping over HTTP still answers', async () => {
    const { status, body } = await post({ type: 'ping' });
    assert.strictEqual(status, 200);
    assert.deepStrictEqual(body, { type: 'pong' });
  });

  await t.test('every answered request releases its client', async () => {
    await waitFor(() => server.rpc.clients.size === 0, 'an unanswered request leaked its client');
  });
});

test('events: a malformed target cannot crash the dispatcher', async (t) => {
  const router = defineRouter({
    chat: {
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
      on: { typing: procedure({ access: 'public', handler: async () => {} }) },
    },
  });
  const { server, port } = await createServer(router, quiet);
  t.after(() => server.close());

  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`);
  t.after(() => void client.close());
  await client.load('chat');
  client.on('error', () => {}); // the id-less error packets come back as these

  // A non-string target used to reach String.prototype.indexOf inside an
  // un-awaited async dispatch: the throw became an unhandled rejection, and
  // node's default is to terminate the process on one.
  for (const packet of [
    { type: 'event', name: 12345 },
    { type: 'event', name: { nested: true } },
    { type: 'event', name: ['chat', 'typing'] },
    { type: 'call', id: 'x', method: 12345 },
    { type: 'call', id: 'x', method: { nested: true } },
  ]) {
    client.send(packet);
  }

  await timers.setTimeout(50);
  assert.strictEqual(await client.api.chat.ping(), 'pong', 'the server survived every malformed target');
});

// A rejection out of a fire-and-forget dispatch has nobody to reach, and
// node terminates the process on an unhandled one — so this test failing
// would take the whole run down rather than report.
test('dispatcher: a dispatch that cannot even report its failure is contained', async () => {
  const router = defineRouter({ chat: { on: { typing: async () => {} } } });
  const broken = {
    persistent: false, // sends handleEvent down the error path...
    sessionReady: Promise.resolve(),
    error() {
      throw new Error('transport is gone'); // ...where the answer fails...
    },
    warn() {
      throw new Error('console is gone'); // ...and so does reporting that
    },
    createContext: () => ({}),
  };
  const packet = JSON.stringify({ type: 'event', name: 'chat/typing', data: {} });
  assert.doesNotThrow(() => handleMessage(broken, packet, router));
  await timers.setTimeout(20);
});

test('events: a session event runs once the session exists', async (t) => {
  const seen = [];
  const router = defineRouter({
    auth: {
      login: procedure({
        access: 'public',
        handler: async (context) => {
          context.client.startSession(undefined, { user: 'ada' });
          return { ok: true };
        },
      }),
    },
    chat: {
      on: {
        private: procedure({
          access: 'session',
          handler: async (context) => void seen.push(context.session.state.user),
        }),
      },
    },
  });
  const { server, port } = await createServer(router, quiet);
  t.after(() => server.close());

  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`);
  t.after(() => void client.close());
  await client.load('auth');

  assert.deepStrictEqual(await client.api.auth.login(), { ok: true });
  client.sendEvent('chat/private', {});
  await waitFor(() => seen.length === 1, 'the session event never ran');
  assert.deepStrictEqual(seen, ['ada']);
});
