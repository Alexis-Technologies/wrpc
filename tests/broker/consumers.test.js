'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure } = require('../../index.js');
const { MemoryBroker, attachConsumers } = require('../../broker.js');
const { bearerTransport } = require('../../auth.js');
const { bootServer, connectClient } = require('../helpers/server.js');
const { quiet, waitFor } = require('./support.js');

const coded = (message, code) => Object.assign(new Error(message), { code });
const fastRetry = { attempts: 3, backoff: { base: 5, max: 20, jitter: false } };

// A dead-letter queue drained into an array, for assertions.
const drain = async (t, broker, queue) => {
  const dead = [];
  const consumer = await broker.queue.consume(queue, (delivery) => {
    dead.push(delivery);
    return delivery.ack();
  });
  t.after(() => consumer.stop());
  return dead;
};

const boot = (t, units, options = {}) => {
  const broker = new MemoryBroker({ logger: quiet });
  const rpc = new RpcServer({ router: defineRouter(units, options.router), logger: quiet, sse: false, ...options.rpc });
  t.after(async () => {
    await rpc.close();
    broker.close();
  });
  return { broker, rpc };
};

test('attachConsumers: a declared consumer runs the full call pipeline and acks', async (t) => {
  const seen = { phases: [], calls: [] };
  const { broker, rpc } = boot(t, {
    'billing.v1': {
      hooks: {
        onRequest: (_ctx, packet) => void seen.phases.push(`onRequest:${packet.method}`),
        preHandler: () => void seen.phases.push('preHandler'),
      },
      consumes: {
        'orders.created': procedure({
          access: 'public',
          input: (args) => {
            if (typeof args.orderId !== 'string') throw new Error('orderId must be a string');
          },
          consume: { prefetch: 4, meta: ['X-Tenant'], retry: fastRetry },
          handler: async (ctx, args) => {
            seen.calls.push({ args, meta: { ...ctx.callMeta }, kind: ctx.client.transportKind });
            return { charged: true };
          },
        }),
      },
    },
  });
  const consumers = await attachConsumers(rpc, broker);
  assert.deepStrictEqual(consumers.bindings, [
    {
      key: 'billing.v1/orders.created',
      queue: 'orders.created',
      group: 'orders.created',
      method: 'billing.v1/consumes.orders.created',
      healthy: true,
    },
  ]);
  assert.strictEqual(consumers.healthy, true);
  await broker.queue.produce('orders.created', JSON.stringify({ orderId: 'o-1' }), {
    headers: { 'x-tenant': 't1', 'x-ignored': 'nope' },
  });
  await waitFor(() => seen.calls.length === 1);
  const [call] = seen.calls;
  assert.deepStrictEqual(call.args, { orderId: 'o-1' });
  assert.strictEqual(call.kind, 'broker');
  assert.strictEqual(call.meta.queue, 'orders.created');
  assert.strictEqual(call.meta.attempt, 1);
  assert.strictEqual(typeof call.meta.messageId, 'string');
  assert.strictEqual(call.meta['x-tenant'], 't1');
  assert.strictEqual(call.meta['x-ignored'], undefined);
  assert.deepStrictEqual(seen.phases, ['onRequest:billing.v1/consumes.orders.created', 'preHandler']);
  await timers.setTimeout(30);
  assert.strictEqual(seen.calls.length, 1, 'an acked message came back');
  // The consumer client is not a connected peer.
  assert.strictEqual([...rpc.clients].filter((client) => client.persistent).length, 0);
  await consumers.stop();
  await consumers.stop();
  assert.strictEqual(consumers.healthy, false);
});

test('attachConsumers: a consumer procedure is unreachable by a call packet', async (t) => {
  const router = defineRouter({
    billing: {
      charge: procedure({ access: 'public', handler: async () => 'callable' }),
      consumes: { 'orders.created': procedure({ access: 'public', handler: async () => 'internal' }) },
    },
  });
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  assert.strictEqual(await client.call('billing/charge', {}), 'callable');
  await assert.rejects(client.call('billing/consumes.orders.created', {}), (error) => error.code === 404);
  await assert.rejects(client.call('billing/orders.created', {}), (error) => error.code === 404);
  const introspected = await client.call('system/introspect', {});
  assert.deepStrictEqual(Object.keys(introspected.billing), ['charge']);
});

test('attachConsumers: failures retry with backoff, then dead-letter with a reason', async (t) => {
  let calls = 0;
  const deadLetters = [];
  const { broker, rpc } = boot(t, {
    jobs: {
      consumes: {
        flaky: procedure({
          access: 'public',
          consume: { retry: fastRetry },
          handler: async () => {
            calls++;
            if (calls === 1) throw coded('database blip', 503);
            return 'ok';
          },
        }),
        broken: procedure({
          access: 'public',
          consume: { retry: fastRetry },
          handler: async () => {
            throw new Error('always');
          },
        }),
        refused: procedure({
          access: 'public',
          handler: async () => {
            throw coded('bad order', 422);
          },
        }),
      },
    },
  });
  const brokenDead = await drain(t, broker, 'broken.dlq');
  const refusedDead = await drain(t, broker, 'refused.dlq');
  const consumers = await attachConsumers(rpc, broker, {}, { onDeadLetter: (info) => deadLetters.push(info) });
  t.after(() => consumers.stop());
  await broker.queue.produce('flaky', '{}');
  await broker.queue.produce('broken', '{}');
  await broker.queue.produce('refused', '{}');
  await waitFor(() => calls === 2 && brokenDead.length === 1 && refusedDead.length === 1);
  // 500 is retried up to the attempt limit, then dead-lettered.
  assert.strictEqual(brokenDead[0].headers['x-wrpc-attempt'], '3');
  assert.match(brokenDead[0].headers['x-wrpc-dead-reason'], /^500/);
  // 422 is not retryable: dead on the first attempt.
  assert.strictEqual(refusedDead[0].headers['x-wrpc-attempt'], '1');
  assert.strictEqual(refusedDead[0].headers['x-wrpc-dead-reason'], '422 bad order');
  assert.deepStrictEqual(deadLetters.map((info) => [info.queue, info.code, info.method]).sort(), [
    ['broken', 500, 'jobs/consumes.broken'],
    ['refused', 422, 'jobs/consumes.refused'],
  ]);
});

test('attachConsumers: an undecodable body and a failing validator are dead on arrival', async (t) => {
  let ran = 0;
  const { broker, rpc } = boot(t, {
    jobs: {
      consumes: {
        typed: procedure({
          access: 'public',
          input: () => {
            throw new Error('nope');
          },
          consume: { deadLetter: 'typed.dead' },
          handler: async () => void ran++,
        }),
      },
    },
  });
  const dead = await drain(t, broker, 'typed.dead');
  const consumers = await attachConsumers(rpc, broker);
  t.after(() => consumers.stop());
  await broker.queue.produce('typed', '{not json');
  await broker.queue.produce('typed', '{}');
  await waitFor(() => dead.length === 2);
  assert.strictEqual(ran, 0);
  assert.deepStrictEqual(dead.map((delivery) => delivery.headers['x-wrpc-dead-reason'].slice(0, 3)).sort(), [
    '400',
    '400',
  ]);
});

test('attachConsumers: dead-lettering can drop instead', async (t) => {
  const { broker, rpc } = boot(t, {
    jobs: {
      consumes: {
        noisy: procedure({
          access: 'public',
          consume: { deadLetter: false, retry: false },
          handler: async () => {
            throw new Error('x');
          },
        }),
      },
    },
  });
  const seen = [];
  const consumers = await attachConsumers(rpc, broker, {}, { onDeadLetter: (info) => seen.push(info.code) });
  t.after(() => consumers.stop());
  await broker.queue.produce('noisy', '{}');
  await waitFor(() => seen.length === 1);
});

test('attachConsumers: the binding table overrides a declared consumer and binds ordinary procedures', async (t) => {
  const got = [];
  const { broker, rpc } = boot(t, {
    'audit.v1': {
      record: procedure({ access: 'public', handler: async (_ctx, args) => void got.push(['record', args]) }),
      consumes: {
        'orders.created': procedure({
          access: 'public',
          consume: { prefetch: 2 },
          handler: async (_ctx, args) => void got.push(['declared', args]),
        }),
        skipped: procedure({ access: 'public', handler: async () => void got.push(['skipped']) }),
      },
    },
  });
  const consumers = await attachConsumers(
    rpc,
    broker.queue,
    {
      'audit.v1/orders.created': { queue: 'prod.orders.created' },
      'audit.events': { target: 'audit.v1/record', args: (body, headers) => ({ body, from: headers.from }) },
    },
    { auto: false },
  );
  t.after(() => consumers.stop());
  assert.deepStrictEqual(
    consumers.bindings.map((binding) => [binding.queue, binding.method]),
    [
      ['prod.orders.created', 'audit.v1/consumes.orders.created'],
      ['audit.events', 'audit.v1/record'],
    ],
  );
  await broker.queue.produce('prod.orders.created', '{"id":1}');
  await broker.queue.produce('audit.events', 'raw text', { headers: { from: 'svc-a' } });
  await broker.queue.produce('skipped', '{}');
  await waitFor(() => got.length === 2);
  await timers.setTimeout(20);
  assert.deepStrictEqual(got.sort(), [
    ['declared', { id: 1 }],
    ['record', { body: 'raw text', from: 'svc-a' }],
  ]);
});

test('attachConsumers: a malformed table fails the attach before anything runs', async (t) => {
  const { broker, rpc } = boot(t, {
    app: {
      call: procedure({ access: 'public', handler: async () => {} }),
      secret: procedure({ handler: async () => {} }),
      feed: procedure.subscription({ access: 'public', handler: async function* () {} }),
      consumes: { job: procedure({ access: 'public', handler: async () => {} }) },
    },
  });
  const cases = [
    [{ 'app/job': { target: 'app/call' } }, /cannot be retargeted/],
    [{ nowhere: {} }, /neither a declared consumer/],
    [{ q: { target: 'app/missing' } }, /is not a procedure/],
    [{ q: { target: 'app/feed' } }, /is a subscription/],
    [{ q: { target: 'app/secret' } }, /needs identity.trust/],
    [{ q: { target: 'app/call', prefetch: 10_000 } }, /exceeds the server's maxCalls/],
    [{ q: { target: 'app/call', prefetch: 0 } }, /prefetch must be a positive integer/],
    [{ q: { target: 'app/call', queue: '' } }, /queue must be a non-empty string/],
    [{ q: { target: 'app/call', group: '' } }, /group must be a non-empty string/],
    [{ q: { target: 'app/call', deadLetter: 7 } }, /deadLetter must be a queue name or false/],
    [{ q: { target: 'app/call', identity: { trust: 'root' } } }, /identity.trust must be/],
    [{ q: { target: 'app/call', meta: 'x-tenant' } }, /meta must be an array/],
    [{ q: { target: 'app/call', args: 'json' } }, /args must be a function/],
    [{ q: { target: 'app/call', retry: { attempts: -1 } } }, /attempts/],
    [{ q: 'nope' }, /the binding must be an object/],
  ];
  for (const [table, message] of cases) {
    await assert.rejects(attachConsumers(rpc, broker, table), message, JSON.stringify(table));
  }
  await assert.rejects(attachConsumers(rpc, broker, []), /binding table must be an object/);
  await assert.rejects(attachConsumers(rpc, broker, {}, { onDeadLetter: 1 }), /onDeadLetter must be a function/);
  await assert.rejects(attachConsumers({}, broker), /a Server or an RpcServer is required/);
  await assert.rejects(attachConsumers(rpc, { name: 'logs-only', close() {}, log: broker.log }), /no 'queue'/);
});

test('attachConsumers: identities — a service pseudo-session, or the session a bearer token restores', async (t) => {
  const seen = [];
  const rpc = new RpcServer({
    router: defineRouter({
      acct: {
        consumes: {
          'as-service': procedure({
            consume: { identity: { trust: 'service' } },
            handler: async (ctx) => void seen.push(['service', { ...ctx.session.state }]),
          }),
          'as-custom': procedure({
            consume: { identity: { trust: 'service', session: { token: 'svc', state: { role: 'billing' } } } },
            handler: async (ctx) => void seen.push(['custom', { ...ctx.session.state }]),
          }),
          'as-user': procedure({
            consume: { identity: { trust: 'token' }, retry: false, deadLetter: 'as-user.dead' },
            handler: async (ctx) => void seen.push(['user', { ...ctx.session.state }]),
          }),
        },
      },
    }),
    logger: quiet,
    sse: false,
    sessions: { transport: bearerTransport() },
  });
  const broker = new MemoryBroker({ logger: quiet });
  t.after(async () => {
    await rpc.close();
    broker.close();
  });
  const dead = await drain(t, broker, 'as-user.dead');
  const ada = rpc.sessions.create(undefined, { user: 'ada' });
  const bob = rpc.sessions.create(undefined, { user: 'bob' });
  await timers.setTimeout(5);
  const consumers = await attachConsumers(rpc, broker, {}, { tokenClients: 1 });
  await broker.queue.produce('as-service', '{}');
  await broker.queue.produce('as-custom', '{}');
  await broker.queue.produce('as-user', '{}', { headers: { authorization: `Bearer ${ada.token}` } });
  await waitFor(() => seen.length === 3);
  // A second token evicts the first client (tokenClients: 1); the first is re-attached on demand.
  await broker.queue.produce('as-user', '{}', { headers: { authorization: `Bearer ${bob.token}` } });
  await waitFor(() => seen.length === 4);
  await broker.queue.produce('as-user', '{}', { headers: { authorization: `Bearer ${ada.token}` } });
  await broker.queue.produce('as-user', '{}', { headers: { authorization: `Bearer ${ada.token}` } });
  await waitFor(() => seen.length === 6);
  // No token: no session — the session procedure refuses, 403 is not retryable.
  await broker.queue.produce('as-user', '{}');
  await waitFor(() => dead.length === 1);
  assert.match(dead[0].headers['x-wrpc-dead-reason'], /^403/);
  assert.deepStrictEqual(seen.map(([kind, state]) => `${kind} ${JSON.stringify(state)}`).sort(), [
    'custom {"role":"billing"}',
    'service {"consumer":"as-service"}',
    'user {"user":"ada"}',
    'user {"user":"ada"}',
    'user {"user":"ada"}',
    'user {"user":"bob"}',
  ]);
  await consumers.stop();
});

test('attachConsumers: draining pauses intake, finishes what it holds, and close stops the bindings', async (t) => {
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const done = [];
  const { broker, rpc } = boot(t, {
    jobs: {
      consumes: {
        slow: procedure({
          access: 'public',
          consume: { prefetch: 1 },
          handler: async (_ctx, { n }) => {
            if (n === 1) await gate;
            done.push(n);
          },
        }),
      },
    },
  });
  const consumers = await attachConsumers(rpc, broker);
  await broker.queue.produce('slow', '{"n":1}');
  await timers.setTimeout(10);
  const draining = rpc.drain(2000);
  await broker.queue.produce('slow', '{"n":2}');
  await timers.setTimeout(20);
  release();
  await draining;
  await timers.setTimeout(20);
  // The held message finished and was acked; the new one waited.
  assert.deepStrictEqual(done, [1]);
  assert.strictEqual(consumers.healthy, true);
  await rpc.close();
  assert.strictEqual(consumers.healthy, false);
  // The waiting message is still on the queue for another instance.
  const later = [];
  await broker.queue.consume('slow', (delivery) => {
    later.push(delivery.body);
    return delivery.ack();
  });
  await waitFor(() => later.length === 1);
  assert.deepStrictEqual(later, ['{"n":2}']);
});

test('attachConsumers: pause() and resume() by hand; a closed consumer client releases in-flight work', async (t) => {
  const seen = [];
  let hold;
  const { broker, rpc } = boot(t, {
    jobs: {
      consumes: {
        manual: procedure({
          access: 'public',
          handler: async (_ctx, { n }) => {
            seen.push(n);
            if (n === 2) await new Promise((resolve) => (hold = resolve));
          },
        }),
      },
    },
  });
  const consumers = await attachConsumers(rpc, broker);
  await consumers.pause();
  await broker.queue.produce('manual', '{"n":1}');
  await timers.setTimeout(20);
  assert.deepStrictEqual(seen, []);
  await consumers.resume();
  await waitFor(() => seen.length === 1);
  await broker.queue.produce('manual', '{"n":2}');
  await waitFor(() => seen.length === 2);
  // The server closing under an in-flight call: the delivery is released.
  await rpc.close();
  hold();
  const redelivered = [];
  await broker.queue.consume('manual', (delivery) => {
    redelivered.push([delivery.body, delivery.redelivered]);
    return delivery.ack();
  });
  await waitFor(() => redelivered.length === 1);
  assert.deepStrictEqual(redelivered, [['{"n":2}', true]]);
});

test('attachConsumers: a start failure stops whatever already started', async (t) => {
  const { broker, rpc } = boot(t, {
    jobs: { consumes: { a: procedure({ access: 'public', handler: async () => {} }) } },
  });
  let stops = 0;
  const flaky = {
    produce: broker.queue.produce,
    consume: async (queue, handler, options) => {
      if (queue === 'b') throw coded('broker says no', 503);
      const consumer = await broker.queue.consume(queue, handler, options);
      return { ...consumer, stop: () => void stops++ || consumer.stop(), healthy: true };
    },
  };
  await assert.rejects(attachConsumers(rpc, flaky, { b: { target: 'jobs/consumes.a' } }), /is not a procedure/);
  const router = rpc.router;
  assert.ok(router.getConsumer('jobs', undefined, 'a'));
  const rpc2 = new RpcServer({
    router: defineRouter({
      jobs: {
        b: procedure({ access: 'public', handler: async () => {} }),
        consumes: { a: procedure({ access: 'public', handler: async () => {} }) },
      },
    }),
    logger: quiet,
    sse: false,
  });
  t.after(() => rpc2.close());
  await assert.rejects(attachConsumers(rpc2, flaky, { b: { target: 'jobs/b' } }), /broker says no/);
  assert.strictEqual(stops, 1);
});

test('attachConsumers: a settlement the broker refuses is logged, never thrown', async (t) => {
  const errors = [];
  const { broker, rpc } = boot(t, {
    jobs: { consumes: { x: procedure({ access: 'public', handler: async () => {} }) } },
  });
  const refusing = {
    produce: broker.queue.produce,
    consume: (queue, handler, options) =>
      broker.queue.consume(
        queue,
        (delivery) =>
          handler({
            ...delivery,
            ack: async () => {
              throw coded('channel closed', 503);
            },
          }),
        options,
      ),
  };
  const consumers = await attachConsumers(
    rpc,
    refusing,
    {},
    {
      logger: {
        ...quiet,
        error: (entry) => errors.push(entry),
        child() {
          return this;
        },
      },
    },
  );
  t.after(() => consumers.stop());
  await broker.queue.produce('x', '{}');
  await waitFor(() => errors.some((entry) => entry.event === 'broker.settle'));
});
