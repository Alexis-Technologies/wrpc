'use strict';

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { WrpcClient, defineRouter, procedure } = require('../../index.js');
const { bootServer } = require('../helpers/server.js');

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

const codedError = (message, code) => {
  const error = new Error(message);
  error.code = code;
  error.expose = true;
  return error;
};

// Delegates to the shared boot (tests/helpers/server.js) — the local
// signature stays, the copied Server block goes.
const boot = async (t, router, options = {}) => {
  const { server, url } = await bootServer(t, { router, ...options });
  return { server, url };
};

const connect = async (t, url, options = {}) => {
  const client = await WrpcClient.connect(url, { heartbeat: false, reconnect: false, ...options });
  t.after(() => void client.close());
  return client;
};

test('hooks: the three levels flatten in router -> unit -> procedure order', async (t) => {
  const order = [];
  const mark = (label) => () => void order.push(label);
  const router = defineRouter(
    {
      unit: {
        hooks: { preHandler: mark('unit') },
        run: procedure({
          access: 'public',
          preHandler: mark('procedure'),
          handler: async () => 'done',
        }),
      },
    },
    { hooks: { preHandler: [mark('router-a'), mark('router-b')] } },
  );
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');
  // load() itself is a hooked call (system/introspect runs the router-level
  // phases too), so the order is only read from here on.
  order.length = 0;
  assert.strictEqual(await client.api.unit.run(), 'done');
  assert.deepStrictEqual(order, ['router-a', 'router-b', 'unit', 'procedure']);
});

test('hooks: every awaited phase can end the call with its coded error', async (t) => {
  const failAt = { phase: null };
  const hookFor = (phase) => async () => {
    if (failAt.phase === phase) throw codedError(`${phase} said no`, 403);
  };
  const router = defineRouter(
    {
      unit: {
        run: procedure({ access: 'public', handler: async () => 'done' }),
      },
    },
    {
      hooks: {
        onRequest: hookFor('onRequest'),
        preValidation: hookFor('preValidation'),
        preHandler: hookFor('preHandler'),
        preSerialization: hookFor('preSerialization'),
        onSend: hookFor('onSend'),
      },
    },
  );
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');
  for (const phase of ['onRequest', 'preValidation', 'preHandler', 'preSerialization', 'onSend']) {
    failAt.phase = phase;
    await assert.rejects(
      client.api.unit.run(),
      (error) => error.code === 403 && error.message === `${phase} said no`,
      `${phase} must abort with its own code and message`,
    );
  }
  failAt.phase = null;
  assert.strictEqual(await client.api.unit.run(), 'done');
});

test('hooks: onRequest enriches ctx.state and the handler sees it', async (t) => {
  const router = defineRouter(
    {
      unit: {
        who: procedure({ access: 'public', handler: async (context) => context.state.tenant }),
      },
    },
    {
      hooks: {
        onRequest: async (context) => {
          context.state.tenant = 'acme';
        },
      },
    },
  );
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');
  assert.strictEqual(await client.api.unit.who(), 'acme');
});

test('hooks: phase payloads and ordering around validation', async (t) => {
  const seen = [];
  const router = defineRouter({
    unit: {
      hooks: {
        preValidation: async (_context, args) => void seen.push(['preValidation', { ...args }]),
        preHandler: async (_context, args) => void seen.push(['preHandler', { ...args }]),
      },
      double: procedure({
        access: 'public',
        input: (args) => ({ n: Number(args.n) }),
        handler: async (_context, { n }) => n * 2,
      }),
    },
  });
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');
  assert.strictEqual(await client.api.unit.double({ n: '21' }), 42);
  // preValidation sees the raw wire args; preHandler sees the validated ones.
  assert.deepStrictEqual(seen, [
    ['preValidation', { n: '21' }],
    ['preHandler', { n: 21 }],
  ]);
});

test('hooks: preSerialization reshapes, onSend mutates the packet', async (t) => {
  const router = defineRouter(
    {
      unit: {
        secret: procedure({
          access: 'public',
          handler: async () => ({ user: 'ada', password: 'hunter2' }),
        }),
      },
    },
    {
      hooks: {
        preSerialization: async (_context, result) => {
          const { password, ...safe } = result;
          void password;
          return safe;
        },
        onSend: async (_context, packet) => {
          packet.result = { ...packet.result, stamped: true };
        },
      },
    },
  );
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');
  assert.deepStrictEqual(await client.api.unit.secret(), { user: 'ada', stamped: true });
});

test('hooks: the observational phases fire and never break the call', async (t) => {
  const seen = { onResponse: 0, onError: 0, onTimeout: 0 };
  const router = defineRouter(
    {
      unit: {
        ok: procedure({ access: 'public', handler: async () => 'fine' }),
        boom: procedure({
          access: 'public',
          handler: async () => {
            throw codedError('told you', 418);
          },
        }),
        slow: procedure({
          access: 'public',
          timeout: 20,
          handler: async () => timers.setTimeout(500),
        }),
      },
    },
    {
      hooks: {
        onResponse: async () => {
          seen.onResponse++;
          throw new Error('observers must be contained');
        },
        onError: async () => void seen.onError++,
        onTimeout: async () => void seen.onTimeout++,
      },
    },
  );
  const { url } = await boot(t, router);
  const client = await connect(t, url, { callTimeout: 2000 });
  await client.load('unit');
  seen.onResponse = 0; // load() is a hooked call too
  assert.strictEqual(await client.api.unit.ok(), 'fine', 'a throwing onResponse must not fail the call');
  assert.strictEqual(seen.onResponse, 1);
  await assert.rejects(client.api.unit.boom(), (error) => error.code === 418);
  assert.strictEqual(seen.onError, 1);
  await assert.rejects(client.api.unit.slow(), (error) => error.code === 408);
  assert.strictEqual(seen.onTimeout, 1, 'a 408 fires onTimeout');
  assert.strictEqual(seen.onError, 2, 'and onError fires for every failure, the 408 included');
});

test('hooks: subscriptions — onSubscribe refuses, onUnsubscribe fires on every end', async (t) => {
  const seen = { subscribed: 0, unsubscribed: 0 };
  let quotaFull = false;
  const router = defineRouter(
    {
      feed: {
        ticks: procedure.subscription({
          access: 'public',
          handler: async function* () {
            yield { n: 1 };
            yield { n: 2 };
          },
        }),
      },
    },
    {
      hooks: {
        onSubscribe: async () => {
          if (quotaFull) throw codedError('Subscription quota exceeded', 429);
          seen.subscribed++;
        },
        onUnsubscribe: async () => void seen.unsubscribed++,
      },
    },
  );
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('feed');

  const values = [];
  await new Promise((resolve) => {
    client.api.feed.ticks.subscribe({}, { onData: (data) => values.push(data), onEnd: resolve });
  });
  assert.deepStrictEqual(values, [{ n: 1 }, { n: 2 }]);
  assert.strictEqual(seen.subscribed, 1);
  await waitFor(() => seen.unsubscribed === 1, 'onUnsubscribe never fired for a completed feed');

  quotaFull = true;
  const refused = await new Promise((resolve) => {
    client.api.feed.ticks.subscribe({}, { onError: resolve });
  });
  assert.strictEqual(refused.code, 429);
  assert.match(refused.message, /quota/i);
  assert.strictEqual(seen.subscribed, 1, 'a refused subscription never counted');
});

test('hooks: inbound events run the invocation phases', async (t) => {
  const seen = [];
  const router = defineRouter({
    chat: {
      probe: procedure({ access: 'public', handler: async () => seen.splice(0, seen.length) }),
      on: {
        typing: procedure({
          access: 'public',
          preHandler: async (_context, data) => void seen.push(['preHandler', data]),
          handler: async (_context, data) => void seen.push(['handler', data]),
        }),
      },
    },
  });
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('chat');
  client.sendEvent('chat/typing', { user: 'ada' });
  await waitFor(() => seen.length === 2, 'the event handler never ran');
  assert.deepStrictEqual(await client.api.chat.probe(), [
    ['preHandler', { user: 'ada' }],
    ['handler', { user: 'ada' }],
  ]);
});

test('hooks: context carries its call identity across call, subscribe and event', async (t) => {
  const seen = [];
  const identity = (phase) => async (context) => void seen.push([phase, context.method, context.procedure]);
  const router = defineRouter(
    {
      unit: {
        run: procedure({ access: 'public', handler: async (context) => context.method }),
        feed: procedure.subscription({
          access: 'public',
          handler: async function* () {
            yield { ok: true };
          },
        }),
        on: {
          nudge: procedure({ access: 'public', handler: async () => {} }),
        },
      },
    },
    {
      hooks: {
        onRequest: identity('onRequest'),
        preHandler: identity('preHandler'),
        onSubscribe: identity('onSubscribe'),
      },
    },
  );
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');
  seen.length = 0; // load() runs the hooks for system/introspect too

  assert.strictEqual(await client.api.unit.run(), 'unit/run', 'the handler reads its own target');
  const call = seen.filter(([phase]) => phase === 'onRequest' || phase === 'preHandler');
  assert.deepStrictEqual(
    call.map(([phase, method]) => [phase, method]),
    [
      ['onRequest', 'unit/run'],
      ['preHandler', 'unit/run'],
    ],
  );
  const proc = router.getProcedure('unit', undefined, 'run');
  assert.ok(
    call.every((entry) => entry[2] === proc),
    'context.procedure is the resolved Procedure instance',
  );

  seen.length = 0;
  await new Promise((resolve) => {
    client.api.unit.feed.subscribe({}, { onData: () => {}, onEnd: resolve });
  });
  const sub = seen.find(([phase]) => phase === 'onSubscribe');
  assert.strictEqual(sub[1], 'unit/feed');
  assert.strictEqual(sub[2], router.getProcedure('unit', undefined, 'feed'));

  seen.length = 0;
  client.sendEvent('unit/nudge', {});
  await waitFor(() => seen.some(([phase]) => phase === 'preHandler'), 'the event hook never ran');
  const event = seen.find(([phase]) => phase === 'preHandler');
  assert.strictEqual(event[1], 'unit/nudge', 'an inbound event carries its name verbatim');
  assert.strictEqual(event[2], router.getEventHandler('unit', undefined, 'nudge'));
});

test('hooks: onDisconnect receives the rooms the client was in', async (t) => {
  const seen = [];
  const router = defineRouter(
    {
      unit: {
        enter: procedure({
          access: 'public',
          handler: async (context) => {
            context.client.join('lobby');
            context.client.join('games');
            return [...context.client.rooms];
          },
        }),
      },
    },
    {
      hooks: {
        onDisconnect: async (client, payload) => void seen.push([payload, client.rooms]),
      },
    },
  );
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');
  assert.deepStrictEqual(await client.api.unit.enter(), ['lobby', 'games']);
  await client.close();
  await waitFor(() => seen.length > 0, 'onDisconnect never fired');
  const [payload, liveRooms] = seen[0];
  assert.deepStrictEqual(payload.rooms, new Set(['lobby', 'games']), 'the payload snapshots the pre-destroy rooms');
  assert.deepStrictEqual(liveRooms, new Set(), 'the registry itself is already empty by hook time');
});

test('meta: context.callMeta carries the packet meta field to hooks and handlers', async (t) => {
  const seen = [];
  const router = defineRouter(
    {
      unit: {
        run: procedure({ access: 'public', handler: async (context) => context.callMeta }),
        feed: procedure.subscription({
          access: 'public',
          handler: async function* (context) {
            yield context.callMeta;
          },
        }),
        on: {
          poke: procedure({
            access: 'public',
            handler: async (context) => void seen.push(['event', context.callMeta]),
          }),
        },
      },
    },
    { hooks: { onRequest: async (context) => void seen.push(['onRequest', context.callMeta]) } },
  );
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');
  seen.length = 0; // load() is a hooked call too

  // The per-call spelling on the escape hatch...
  assert.deepStrictEqual(await client.call('unit/run', {}, { meta: { idem: 'k1' } }), { idem: 'k1' });
  // ...and the bound-variant spelling on a scaffolded method.
  assert.deepStrictEqual(await client.api.unit.run.withMeta({ idem: 'k2' })(), { idem: 'k2' });
  // A packet with NO meta reads as a frozen empty object, never null.
  const bare = await client.api.unit.run();
  assert.deepStrictEqual(bare, {});
  assert.deepStrictEqual(
    // Spread-normalized: the empty default is null-prototyped by design.
    seen.filter(([phase]) => phase === 'onRequest').map(([, meta]) => ({ ...meta })),
    [{ idem: 'k1' }, { idem: 'k2' }, {}],
  );
  // An inbound EVENT packet carries the field the same way (no client
  // spelling yet — sent raw, as any hand-written peer could).
  client.send({ type: 'event', name: 'unit/poke', data: {}, meta: { trace: 't1' } });
  await waitFor(() => seen.some(([phase]) => phase === 'event'), 'the event never arrived');
  assert.deepStrictEqual({ ...seen.find(([phase]) => phase === 'event')[1] }, { trace: 't1' });
});

test('meta: a malformed or oversize packet meta is refused, the call still runs', async (t) => {
  const router = defineRouter({
    unit: {
      run: procedure({
        access: 'public',
        handler: async (context) => ({
          meta: context.callMeta,
          frozen: Object.isFrozen(context.callMeta),
          protoKey: Object.hasOwn(context.callMeta, '__proto__'),
        }),
      }),
    },
  });
  const { server, url } = await boot(t, router, { metaMaxBytes: 64 });
  const client = await connect(t, url);
  await client.load('unit');
  // An array, an oversize object, a bare string: each is dropped to the
  // frozen empty default — the label is refused, never the call.
  for (const bad of [['a'], { pad: 'x'.repeat(200) }, 'nope', 42]) {
    const out = await client.call('unit/run', {}, { meta: bad });
    assert.deepStrictEqual(out.meta, {}, JSON.stringify(bad));
    assert.strictEqual(out.frozen, true);
  }
  // A declared __proto__ key never reaches the handler's bag.
  const out = await client.call('unit/run', {}, { meta: JSON.parse('{"a":1,"__proto__":{"polluted":1}}') });
  assert.deepStrictEqual(out.meta, { a: 1 });
  assert.strictEqual(out.protoKey, false);
  assert.strictEqual({}.polluted, undefined);
  assert.ok(server); // silence unused
});

test('hooks: an onConnect room join is visible to the very first dispatched call', async (t) => {
  const router = defineRouter({
    unit: {
      rooms: procedure({ access: 'public', handler: async (context) => [...context.client.rooms] }),
    },
  });
  // Slower than a localhost round trip on purpose: without the client.ready
  // gate the introspect and the call would race past the join and this test
  // would flake toward [] — the exact bug the gate closes.
  router.addHook('onConnect', async (client) => {
    await timers.setTimeout(25);
    client.join('lobby');
  });
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');
  assert.deepStrictEqual(await client.api.unit.rooms(), ['lobby']);
});

test('hooks: onConnect and onDisconnect bracket a connection', async (t) => {
  const seen = [];
  const router = defineRouter(
    {
      unit: { ping: procedure({ access: 'public', handler: async () => 'pong' }) },
    },
    {
      hooks: {
        onConnect: async (client) => void seen.push(['connect', client.transportKind]),
        onDisconnect: async (client) => void seen.push(['disconnect', client.transportKind]),
      },
    },
  );
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');
  assert.strictEqual(await client.api.unit.ping(), 'pong');
  await client.close();
  await waitFor(() => seen.some(([kind]) => kind === 'disconnect'), 'onDisconnect never fired');
  assert.ok(seen.some(([kind, transport]) => kind === 'connect' && transport === 'ws'));
});

test('hooks: addHook after construction, and merge carries every level', async (t) => {
  const order = [];
  const base = defineRouter(
    {
      unit: {
        hooks: { preHandler: async () => void order.push('unit') },
        run: procedure({ access: 'public', handler: async () => 'done' }),
      },
    },
    { hooks: { preHandler: async () => void order.push('base-router') } },
  );
  base.addHook('preHandler', async () => void order.push('added'));
  const extra = defineRouter({}, { hooks: { preHandler: async () => void order.push('other-router') } });
  const merged = base.merge(extra);
  const { url } = await boot(t, merged);
  const client = await connect(t, url);
  await client.load('unit');
  order.length = 0; // load() is a hooked call too
  assert.strictEqual(await client.api.unit.run(), 'done');
  assert.deepStrictEqual(order, ['base-router', 'added', 'other-router', 'unit']);
});

test('hooks: configuration mistakes are loud', () => {
  assert.throws(() => defineRouter({}, { hooks: { onRequets: () => {} } }), /unknown hook phase 'onRequets'/);
  assert.throws(() => defineRouter({}, { hooks: { onRequest: 'not-a-fn' } }), /must be a function/);
  assert.throws(
    () => defineRouter({ unit: { hooks: { onConnect: () => {} } } }),
    /unknown hook phase 'onConnect'/,
    'connection phases are router-level only',
  );
  assert.throws(() => defineRouter({}).addHook('nope', () => {}), /unknown hook phase/);
  assert.throws(() => defineRouter({}).addHook('onRequest', 42), /must be a function/);
  assert.throws(() => procedure({ handler: async () => {}, preValidation: 42 }), /must be a function/);
  assert.throws(() => procedure({ handler: async () => {}, access: 'admin' }), /access must be 'public' or 'session'/);
});

// ---------------------------------------------------------------------------
// generateId (pluggable ids) and the wrpc.v1 subprotocol ride in this suite:
// they froze in the same phase and share the boot helpers.

test('generateId: the client uses the injected generator for every packet id', async (t) => {
  const router = defineRouter({
    unit: { echo: procedure({ access: 'public', handler: async (_context, args) => args }) },
  });
  const { url } = await boot(t, router);
  let n = 0;
  const client = await connect(t, url, { generateId: () => `custom-${++n}` });
  await client.load('unit');
  // load() consumed some ids already; the next call takes the next counter
  // value — proving per-call invocation, not a per-client constant.
  const before = n;
  await client.api.unit.echo({ ok: 1 });
  assert.strictEqual(n, before + 1, 'each call draws exactly one id');
  assert.ok(n >= 2, 'load() drew ids from the same generator');
});

test('generateId: the server stamps REST packets and contexts with it', async (t) => {
  const router = defineRouter({
    unit: {
      whoami: procedure({ access: 'public', handler: async (context) => ({ uuid: context.uuid }) }),
    },
  });
  let n = 0;
  const { server } = await boot(t, router, { generateId: () => `srv-${++n}` });
  const port = server.address().port;
  const res = await fetch(`http://127.0.0.1:${port}/api/unit/whoami`);
  const packet = await res.json();
  assert.match(packet.id, /^srv-\d+$/, 'the synthetic REST packet id comes from the injected generator');
  assert.match(packet.result.uuid, /^srv-\d+$/, 'the context uuid does too');
});

test('generateId: a stream id past 255 characters is refused at the source', async (t) => {
  const router = defineRouter({
    unit: { noop: procedure({ access: 'public', handler: async () => null }) },
  });
  const { url } = await boot(t, router);
  const client = await connect(t, url, { generateId: () => 'x'.repeat(256) });
  assert.throws(() => client.createStream('name', 10), /at most 255 characters/);
});

test('subprotocol: the server echoes wrpc.v1 and the client records it', async (t) => {
  const router = defineRouter({
    unit: { ping: procedure({ access: 'public', handler: async () => 'pong' }) },
  });
  const { server, url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');
  assert.strictEqual(await client.api.unit.ping(), 'pong');
  // The engine saw the offer and selected the revision.
  const connections = [...server.wsServer.connections];
  assert.strictEqual(connections.length, 1);
  assert.strictEqual(connections[0].protocol, 'wrpc.v1');
});

test('subprotocol: a peer that offers nothing still connects (1.0 stays valid)', async (t) => {
  const router = defineRouter({
    unit: { ping: procedure({ access: 'public', handler: async () => 'pong' }) },
  });
  const { server, url } = await boot(t, router);
  const client = await connect(t, url, { protocols: [] });
  await client.load('unit');
  assert.strictEqual(await client.api.unit.ping(), 'pong');
  const connections = [...server.wsServer.connections];
  assert.strictEqual(connections[0].protocol, '');
});

test('meta: withMeta normalizes keys, the raw call() escape hatch does not', async (t) => {
  const router = defineRouter({
    unit: { run: procedure({ access: 'public', handler: async (context) => ({ ...context.callMeta }) }) },
  });
  const { url } = await boot(t, router);
  const client = await connect(t, url);
  await client.load('unit');

  // The ergonomic path normalizes, so a camelCase key addresses the same
  // value it would over a header carrier.
  assert.deepStrictEqual(await client.api.unit.run.withMeta({ traceId: 't1', userId: 2 })(), {
    'trace-id': 't1',
    'user-id': 2,
  });

  // The raw seam hands the wire exactly what it was given — this asymmetry
  // is deliberate: it is what the auth hooks write against.
  assert.deepStrictEqual(await client.call('unit/run', {}, { meta: { traceId: 't2' } }), { traceId: 't2' });

  // Values keep their JSON types on the packet path; only a header carrier
  // has to flatten them.
  assert.deepStrictEqual(await client.api.unit.run.withMeta({ retryCount: 3, isRetry: true })(), {
    'retry-count': 3,
    'is-retry': true,
  });
});
