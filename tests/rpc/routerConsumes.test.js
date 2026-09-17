'use strict';

// The router's reserved `consumes` key and the procedure `consume` option.

const { test } = require('node:test');
const assert = require('node:assert');

const { defineRouter, procedure } = require('../../src/rpc/router.js');

const handler = async () => {};

test('consumes: declared consumers live apart from methods', () => {
  const router = defineRouter({
    'billing.v2': {
      charge: procedure({ handler }),
      consumes: {
        'orders.created': procedure({ handler, consume: { prefetch: 4 } }),
        'orders.refunded': handler,
      },
    },
    audit: { consumes: { events: { handler, access: 'public' } } },
  });
  const consumer = router.getConsumer('billing', 'v2', 'orders.created');
  assert.ok(consumer);
  assert.deepStrictEqual(consumer.consume, { prefetch: 4 });
  assert.ok(Object.isFrozen(consumer.consume));
  assert.strictEqual(router.getConsumer('billing', 'v2', 'orders.refunded').consume, null);
  assert.ok(router.getConsumer('audit', undefined, 'events'));
  assert.strictEqual(router.getConsumer('billing', 'v2', 'charge'), null);
  assert.strictEqual(router.getConsumer('missing', 'v1', 'x'), null);
  assert.strictEqual(router.getProcedure('billing', 'v2', 'orders.created'), null);
  assert.strictEqual(router.getProcedure('billing', 'v2', 'consumes'), null);
  assert.deepStrictEqual(
    router.consumers().map(({ unitKey, unit, version, name }) => [unitKey, unit, version, name]),
    [
      ['billing.v2', 'billing', 'v2', 'orders.created'],
      ['billing.v2', 'billing', 'v2', 'orders.refunded'],
      ['audit', 'audit', '*', 'events'],
    ],
  );
  // Topology, not a client contract: introspection never shows them.
  assert.deepStrictEqual(Object.keys(router.introspect()['billing.v2']), ['charge']);
  assert.deepStrictEqual(router.introspect().audit, {});
});

test('consumes: hooks and compiled schemas reach consumer procedures', () => {
  const seen = [];
  const ajv = { compile: () => Object.assign(() => true, { errors: [] }) };
  const router = defineRouter(
    {
      jobs: {
        hooks: { preHandler: () => seen.push('unit') },
        consumes: { run: procedure({ handler, schema: { body: { type: 'object' } } }) },
      },
    },
    { hooks: { onRequest: () => seen.push('router') }, validation: { ajv } },
  );
  const proc = router.getConsumer('jobs', undefined, 'run');
  const chain = router.hooksFor(proc);
  assert.strictEqual(chain.onRequest.length, 1);
  assert.strictEqual(chain.preHandler.length, 1);
  assert.ok(router.compiledFor(proc).input);
  // merge() carries consumers over.
  const merged = router.merge(defineRouter({ other: { x: handler } }));
  assert.ok(merged.getConsumer('jobs', undefined, 'run'));
  assert.strictEqual(merged.consumers().length, 1);
  // A schema without an injected ajv names the consumer in the error.
  assert.throws(
    () => defineRouter({ jobs: { consumes: { run: procedure({ handler, schema: { body: {} } }) } } }),
    /jobs\/consumes\.run declares schema validation/,
  );
});

test('consumes: misplaced or malformed declarations are refused', () => {
  assert.throws(() => procedure({ handler, consume: 'queue' }), /consume must be an object/);
  assert.throws(() => procedure({ handler, consume: [] }), /consume must be an object/);
  assert.throws(
    () => procedure({ handler, consume: {}, http: { method: 'POST', path: '/x' } }),
    /cannot be combined with a subscription or an http mapping/,
  );
  assert.throws(() => procedure.subscription({ handler: async function* () {}, consume: {} }), /cannot be combined/);
  assert.throws(
    () => defineRouter({ app: { run: procedure({ handler, consume: {} }) } }),
    /app\/run: consume is only valid inside a unit's consumes block/,
  );
  assert.throws(
    () => defineRouter({ app: { on: { ping: procedure({ handler, consume: {} }) } } }),
    /app\/on\.ping: consume is only valid/,
  );
  assert.throws(() => defineRouter({ app: { consumes: 'orders' } }), /app\.consumes must be an object/);
  assert.throws(() => defineRouter({ app: { consumes: [] } }), /app\.consumes must be an object/);
  assert.throws(
    () => defineRouter({ app: { consumes: { feed: procedure.subscription(async function* () {}) } } }),
    /app\.consumes\.feed cannot be a subscription/,
  );
});
