'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { setTimeout: delay } = require('node:timers/promises');

const { Procedure, Router, procedure, defineRouter } = require('../../src/rpc/router.js');

const deferred = () => {
  let release = null;
  const promise = new Promise((resolve) => {
    release = resolve;
  });
  return { promise, resolve: release };
};

test('procedure definitions', async (t) => {
  await t.test('procedure(fn) wraps a bare handler with session access', async () => {
    const handler = async (context, args) => ({ context, args });
    const proc = procedure(handler);
    assert.ok(proc instanceof Procedure);
    assert.strictEqual(proc.access, 'session');
    assert.strictEqual(proc.handler, handler);
    const context = { uuid: 'ctx-1' };
    const result = await proc.invoke(context, { x: 1 });
    assert.strictEqual(result.context, context);
    assert.deepStrictEqual(result.args, { x: 1 });
  });

  await t.test('bare function in a router definition defaults to session access', () => {
    const router = defineRouter({ unit: { m: async () => 1 } });
    const proc = router.getProcedure('unit', '*', 'm');
    assert.ok(proc instanceof Procedure);
    assert.strictEqual(proc.access, 'session');
  });

  await t.test('options-object method values become procedures', async () => {
    const router = defineRouter({
      unit: { sum: { access: 'public', handler: async (_context, args) => args.a + args.b } },
    });
    const proc = router.getProcedure('unit', '*', 'sum');
    assert.ok(proc instanceof Procedure);
    assert.strictEqual(proc.access, 'public');
    assert.strictEqual(await proc.invoke({}, { a: 2, b: 3 }), 5);
  });

  await t.test('Procedure instances are reused as-is', () => {
    const proc = procedure({ access: 'public', handler: async () => 1 });
    const router = defineRouter({ unit: { m: proc } });
    assert.strictEqual(router.getProcedure('unit', '*', 'm'), proc);
  });

  await t.test('definitions without a handler throw TypeError', () => {
    assert.throws(() => procedure({}), TypeError);
    assert.throws(() => procedure({ handler: 42 }), TypeError);
    assert.throws(() => defineRouter({ unit: { m: {} } }), { name: 'TypeError', message: /must be a procedure/ });
    assert.throws(() => defineRouter({ unit: { m: 'nope' } }), TypeError);
  });

  await t.test('bad unit values throw TypeError', () => {
    assert.throws(() => defineRouter({ unit: null }), { name: 'TypeError', message: /Invalid router unit/ });
    assert.throws(() => defineRouter({ unit: 42 }), TypeError);
  });

  await t.test('non-validator input/output options throw TypeError', () => {
    assert.throws(() => procedure({ handler: async () => 1, input: 42 }), TypeError);
    assert.throws(() => procedure({ handler: async () => 1, output: 'bad' }), TypeError);
  });
});

test('versioned units', async (t) => {
  const v1 = procedure({ access: 'public', handler: async () => 'v1' });
  const def = procedure({ access: 'public', handler: async () => 'default' });
  const router = defineRouter({
    'unit.1': { m: v1 },
    unit: { m: def },
  });

  await t.test('unit.1 key registers version 1', () => {
    assert.strictEqual(router.getProcedure('unit', '1', 'm'), v1);
  });

  await t.test('bare unit key registers the default version *', () => {
    assert.strictEqual(router.getProcedure('unit', '*', 'm'), def);
  });

  await t.test('no cross-version fallback', () => {
    assert.strictEqual(router.getProcedure('unit', '2', 'm'), null);
    const only = defineRouter({ 'solo.1': { m: v1 } });
    assert.strictEqual(only.getProcedure('solo', '*', 'm'), null);
    assert.strictEqual(only.getProcedure('solo', '1', 'm'), v1);
  });

  await t.test('getProcedure misses return null', () => {
    assert.strictEqual(router.getProcedure('nope', '*', 'm'), null);
    assert.strictEqual(router.getProcedure('unit', '*', 'nope'), null);
  });
});

test('introspect', async (t) => {
  const noop = async () => {};
  const router = defineRouter({
    unit: {
      open: procedure({
        access: 'public',
        handler: noop,
        meta: { description: 'Open thing' },
        signature: { args: ['id'], result: 'object' },
      }),
      plain: noop,
    },
    'unit.1': { legacy: procedure({ access: 'public', handler: noop }) },
    other: { m: noop },
  });

  await t.test('full introspection lists every unit and version key', () => {
    assert.deepStrictEqual(router.introspect(), {
      unit: {
        open: {
          access: 'public',
          meta: { description: 'Open thing' },
          signature: { args: ['id'], result: 'object' },
        },
        plain: { access: 'session' },
      },
      'unit.1': { legacy: { access: 'public' } },
      other: { m: { access: 'session' } },
    });
  });

  await t.test('empty meta and null signature are omitted', () => {
    const info = router.introspect().other.m;
    assert.ok(!('meta' in info));
    assert.ok(!('signature' in info));
  });

  await t.test('filter by units list', () => {
    assert.deepStrictEqual(Object.keys(router.introspect(['unit'])), ['unit']);
    assert.deepStrictEqual(router.introspect(['unit.1']), { 'unit.1': { legacy: { access: 'public' } } });
    assert.deepStrictEqual(router.introspect(['missing']), {});
  });
});

test('merge', async (t) => {
  const procA = procedure({ access: 'public', handler: async () => 'a' });
  const onlyA = procedure({ access: 'public', handler: async () => 'only-a' });
  const procB = procedure({ access: 'public', handler: async () => 'b' });
  const procX = procedure({ access: 'public', handler: async () => 'x' });
  const versioned = procedure({ access: 'public', handler: async () => 'v2' });
  const a = defineRouter({ unit: { m: procA, only: onlyA }, 'legacy.2': { m: versioned } });
  const b = defineRouter({ unit: { m: procB }, extra: { x: procX } });
  const merged = a.merge(b);

  await t.test('returns a new router with the union of units', () => {
    assert.ok(merged instanceof Router);
    assert.notStrictEqual(merged, a);
    assert.notStrictEqual(merged, b);
    assert.strictEqual(merged.getProcedure('unit', '*', 'only'), onlyA);
    assert.strictEqual(merged.getProcedure('extra', '*', 'x'), procX);
    assert.strictEqual(merged.getProcedure('legacy', '2', 'm'), versioned);
  });

  await t.test('collision: the other router wins', () => {
    assert.strictEqual(merged.getProcedure('unit', '*', 'm'), procB);
  });

  await t.test('originals are untouched', () => {
    assert.strictEqual(a.getProcedure('unit', '*', 'm'), procA);
    assert.strictEqual(b.getProcedure('unit', '*', 'm'), procB);
    assert.strictEqual(a.getProcedure('extra', '*', 'x'), null);
    assert.strictEqual(b.getProcedure('legacy', '2', 'm'), null);
    assert.strictEqual(b.getProcedure('unit', '*', 'only'), null);
  });
});

test('invoke validation', async (t) => {
  await t.test('input validator transforms args before the handler', async () => {
    const proc = procedure({
      handler: async (_context, args) => args,
      input: (args) => ({ ...args, n: Number(args.n) }),
    });
    assert.deepStrictEqual(await proc.invoke({}, { n: '42' }), { n: 42 });
  });

  await t.test('input validator returning undefined keeps args', async () => {
    const original = { n: 1 };
    let seen = null;
    const proc = procedure({
      handler: async (_context, args) => args,
      input: (args) => {
        seen = args;
        return undefined;
      },
    });
    const result = await proc.invoke({}, original);
    assert.strictEqual(result, original);
    assert.strictEqual(seen, original);
  });

  await t.test('input validator rejection maps to code 400', async () => {
    const proc = procedure({
      handler: async () => 'unreachable',
      input: () => {
        throw new Error('n must be a number');
      },
    });
    await assert.rejects(proc.invoke({}, {}), (error) => {
      assert.strictEqual(error.code, 400);
      assert.match(error.message, /Invalid arguments: n must be a number/);
      return true;
    });
  });

  await t.test('output validator rejection maps to code 500', async () => {
    const proc = procedure({
      handler: async () => ({ secret: true }),
      output: () => {
        throw new Error('secret leak');
      },
    });
    await assert.rejects(proc.invoke({}, {}), (error) => {
      assert.strictEqual(error.code, 500);
      assert.match(error.message, /Invalid procedure result: secret leak/);
      return true;
    });
  });

  await t.test('output validator can transform the result', async () => {
    const proc = procedure({
      handler: async () => ({ a: 1, b: 2 }),
      output: (result) => ({ a: result.a }),
    });
    assert.deepStrictEqual(await proc.invoke({}, {}), { a: 1 });
  });

  await t.test('Standard Schema validator success path', async () => {
    const schema = {
      '~standard': { validate: async (value) => ({ value: { ...value, checked: true } }) },
    };
    const proc = procedure({ handler: async (_context, args) => args, input: schema });
    assert.deepStrictEqual(await proc.invoke({}, { n: 1 }), { n: 1, checked: true });
  });

  await t.test('Standard Schema validator issues path maps to code 400', async () => {
    const schema = {
      '~standard': {
        validate: async () => ({ issues: [{ message: 'name required' }, { message: 'age invalid' }] }),
      },
    };
    const proc = procedure({ handler: async () => 'unreachable', input: schema });
    await assert.rejects(proc.invoke({}, {}), (error) => {
      assert.strictEqual(error.code, 400);
      assert.match(error.message, /name required; age invalid/);
      return true;
    });
  });

  await t.test('Standard Schema issues keep their paths as structured details', async () => {
    const schema = {
      '~standard': {
        validate: async () => ({
          issues: [
            { message: 'name required', path: ['name'] },
            { message: 'age invalid', path: ['profile', 'age'] },
          ],
        }),
      },
    };
    const proc = procedure({ handler: async () => 'unreachable', input: schema });
    await assert.rejects(proc.invoke({}, {}), (error) => {
      assert.strictEqual(error.code, 400);
      assert.strictEqual(error.expose, true);
      assert.deepStrictEqual(error.details, {
        issues: [
          { message: 'name required', path: ['name'] },
          { message: 'age invalid', path: ['profile', 'age'] },
        ],
      });
      return true;
    });
  });
});

test('invoke timeout and queue', async (t) => {
  await t.test('timeout rejects with code 408', async () => {
    const proc = procedure({
      handler: () => delay(100).then(() => 'late'),
      timeout: 30,
    });
    await assert.rejects(proc.invoke({}, {}), (error) => {
      assert.strictEqual(error.code, 408);
      assert.match(error.message, /Procedure timeout/);
      return true;
    });
  });

  await t.test('queue overflow rejects with code 503', async () => {
    const gate = deferred();
    const proc = procedure({
      handler: () => gate.promise,
      queue: { concurrency: 1, size: 0 },
    });
    const first = proc.invoke({}, {});
    await assert.rejects(proc.invoke({}, {}), (error) => {
      assert.strictEqual(error.code, 503);
      assert.match(error.message, /queue is full/);
      return true;
    });
    gate.resolve('done');
    assert.strictEqual(await first, 'done');
  });

  await t.test('queued call times out with code 503', async () => {
    const gate = deferred();
    const proc = procedure({
      handler: () => gate.promise,
      queue: { concurrency: 1, size: 1, timeout: 20 },
    });
    const first = proc.invoke({}, {});
    const second = proc.invoke({}, {});
    await assert.rejects(second, (error) => {
      assert.strictEqual(error.code, 503);
      assert.match(error.message, /Semaphore timeout/);
      return true;
    });
    gate.resolve('done');
    assert.strictEqual(await first, 'done');
  });

  await t.test('semaphore is released after a handler throw', async () => {
    let calls = 0;
    const proc = procedure({
      handler: async () => {
        calls++;
        if (calls === 1) throw new Error('boom');
        return 'recovered';
      },
      queue: { concurrency: 1, size: 0 },
    });
    await assert.rejects(proc.invoke({}, {}), /boom/);
    assert.strictEqual(await proc.invoke({}, {}), 'recovered');
    assert.strictEqual(calls, 2);
  });
});

test('review regressions: keys, filters, and queue-timeout interplay', async (t) => {
  await t.test('multi-dot unit keys are rejected instead of silently truncated', () => {
    const method = { m: async () => 1 };
    assert.throws(() => defineRouter({ 'unit.1.2': method }), TypeError);
    assert.throws(() => defineRouter({ 'unit.': method }), TypeError);
    assert.throws(() => defineRouter({ '.1': method }), TypeError);
  });

  await t.test('introspect treats a non-array units argument as no filter', () => {
    const router = defineRouter({ unit: { m: async () => 1 } });
    assert.deepStrictEqual(Object.keys(router.introspect({})), ['unit']);
    assert.deepStrictEqual(Object.keys(router.introspect('unit')), ['unit']);
    assert.deepStrictEqual(Object.keys(router.introspect(42)), ['unit']);
  });

  await t.test('timeout does not release the queue slot while the handler still runs', async () => {
    let active = 0;
    let maxActive = 0;
    const proc = procedure({
      timeout: 30,
      queue: { concurrency: 1, size: 2 },
      handler: async () => {
        active++;
        maxActive = Math.max(maxActive, active);
        await delay(80);
        active--;
        return 'done';
      },
    });
    const first = proc.invoke({}, {});
    const second = proc.invoke({}, {});
    await assert.rejects(first, (error) => error.code === 408);
    // the second call must NOT start before the first handler settles
    await assert.rejects(second, (error) => error.code === 408);
    await delay(200);
    assert.strictEqual(maxActive, 1);
    assert.strictEqual(active, 0);
    // slots released after settlement: a fresh call on the same procedure works
    assert.strictEqual(await procedure({ queue: { concurrency: 1 }, handler: async () => 3 }).invoke({}, {}), 3);
  });

  await t.test('validation failure with a queue releases the slot', async () => {
    const proc = procedure({
      queue: { concurrency: 1, size: 0 },
      input: () => {
        throw new Error('nope');
      },
      handler: async () => 1,
    });
    await assert.rejects(proc.invoke({}, {}), (error) => error.code === 400);
    // slot free again: the next call reaches the validator, not a 503
    await assert.rejects(proc.invoke({}, {}), (error) => error.code === 400);
  });
});

test('addUnit: post-construction units for the mirror feature', async (t) => {
  const { defineRouter, procedure } = require('../../index.js');

  await t.test('adds a unit and rebuilds the hook chains', async () => {
    const trace = [];
    const router = defineRouter({}, { hooks: { preHandler: async () => void trace.push('router') } });
    router.addUnit('late', { hello: procedure({ access: 'public', handler: async () => 'hi' }) });
    const proc = router.getProcedure('late', '*', 'hello');
    assert.notStrictEqual(proc, null);
    assert.strictEqual(router.hooksFor(proc).preHandler.length, 1);
  });

  await t.test('refuses a duplicate unit key', () => {
    const router = defineRouter({ a: { x: procedure({ access: 'public', handler: async () => 1 }) } });
    assert.throws(() => router.addUnit('a', {}), /already registered/);
    // A different VERSION of the same unit is a different key.
    router.addUnit('a.2', { x: procedure({ access: 'public', handler: async () => 2 }) });
    assert.notStrictEqual(router.getProcedure('a', '2', 'x'), null);
  });
});
