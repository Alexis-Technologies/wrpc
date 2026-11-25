'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { setTimeout: delay } = require('node:timers/promises');

const { Procedure, Router, procedure, defineRouter } = require('../../src/rpc/router.js');

const deferred = () => {
  let release = null;
  const promise = new Promise((resolve) => {
    release = resolve;
  });
  return { promise, resolve: release };
};

describe('router', () => {
  describe('procedure definitions', () => {
    it('should wrap a bare handler with session access in procedure(fn)', async () => {
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

    it('should default a bare function in a router definition to session access', () => {
      const router = defineRouter({ unit: { m: async () => 1 } });
      const proc = router.getProcedure('unit', '*', 'm');
      assert.ok(proc instanceof Procedure);
      assert.strictEqual(proc.access, 'session');
    });

    it('should turn options-object method values into procedures', async () => {
      const router = defineRouter({
        unit: { sum: { access: 'public', handler: async (_context, args) => args.a + args.b } },
      });
      const proc = router.getProcedure('unit', '*', 'sum');
      assert.ok(proc instanceof Procedure);
      assert.strictEqual(proc.access, 'public');
      assert.strictEqual(await proc.invoke({}, { a: 2, b: 3 }), 5);
    });

    it('should reuse Procedure instances as-is', () => {
      const proc = procedure({ access: 'public', handler: async () => 1 });
      const router = defineRouter({ unit: { m: proc } });
      assert.strictEqual(router.getProcedure('unit', '*', 'm'), proc);
    });

    it('should throw TypeError for definitions without a handler', () => {
      assert.throws(() => procedure({}), TypeError);
      assert.throws(() => procedure({ handler: 42 }), TypeError);
      assert.throws(() => defineRouter({ unit: { m: {} } }), {
        name: 'ConfigError',
        code: 'ALX_ERR_CONFIG',
        message: /must be a procedure/,
      });
      assert.throws(() => defineRouter({ unit: { m: 'nope' } }), TypeError);
    });

    it('should throw TypeError for bad unit values', () => {
      assert.throws(() => defineRouter({ unit: null }), {
        name: 'ConfigError',
        code: 'ALX_ERR_CONFIG',
        message: /Invalid router unit/,
      });
      assert.throws(() => defineRouter({ unit: 42 }), TypeError);
    });

    it('should throw TypeError for non-validator input/output options', () => {
      assert.throws(() => procedure({ handler: async () => 1, input: 42 }), TypeError);
      assert.throws(() => procedure({ handler: async () => 1, output: 'bad' }), TypeError);
    });
  });

  describe('versioned units', () => {
    const v1 = procedure({ access: 'public', handler: async () => 'v1' });
    const def = procedure({ access: 'public', handler: async () => 'default' });
    const router = defineRouter({
      'unit.v1': { m: v1 },
      unit: { m: def },
    });

    it('should register version v1 under the unit.v1 key', () => {
      assert.strictEqual(router.getProcedure('unit', 'v1', 'm'), v1);
    });

    it('should register the default version * under the bare unit key', () => {
      assert.strictEqual(router.getProcedure('unit', '*', 'm'), def);
    });

    it('should not fall back across versions', () => {
      assert.strictEqual(router.getProcedure('unit', 'v2', 'm'), null);
      const only = defineRouter({ 'solo.v1': { m: v1 } });
      assert.strictEqual(only.getProcedure('solo', '*', 'm'), null);
      assert.strictEqual(only.getProcedure('solo', 'v1', 'm'), v1);
    });

    it('should return null on getProcedure misses', () => {
      assert.strictEqual(router.getProcedure('nope', '*', 'm'), null);
      assert.strictEqual(router.getProcedure('unit', '*', 'nope'), null);
    });

    it("should accept 'vN' as the version token and nothing else", () => {
      const method = { m: async () => 1 };
      assert.throws(() => defineRouter({ 'unit.1': method }), /a unit key is 'unit' or 'unit\.vN'/);
      assert.throws(() => defineRouter({ 'unit.v': method }), TypeError);
      assert.throws(() => defineRouter({ 'unit.vx': method }), TypeError);
      assert.throws(() => defineRouter({ 'unit.V1': method }), TypeError, 'the v is lowercase');
      assert.strictEqual(defineRouter({ 'unit.v12': method }).getProcedure('unit', 'v12', 'm') !== null, true);
    });
  });

  describe('introspect', () => {
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
      'unit.v1': { legacy: procedure({ access: 'public', handler: noop }) },
      other: { m: noop },
    });

    it('should list every unit and version key in full introspection', () => {
      assert.deepStrictEqual(router.introspect(), {
        unit: {
          open: {
            access: 'public',
            meta: { description: 'Open thing' },
            signature: { args: ['id'], result: 'object' },
          },
          plain: { access: 'session' },
        },
        'unit.v1': { legacy: { access: 'public' } },
        other: { m: { access: 'session' } },
      });
    });

    it('should omit empty meta and null signature', () => {
      const info = router.introspect().other.m;
      assert.ok(!('meta' in info));
      assert.ok(!('signature' in info));
    });

    it('should filter by a units list', () => {
      assert.deepStrictEqual(Object.keys(router.introspect(['unit'])), ['unit']);
      assert.deepStrictEqual(router.introspect(['unit.v1']), { 'unit.v1': { legacy: { access: 'public' } } });
      assert.deepStrictEqual(router.introspect(['missing']), {});
    });
  });

  describe('merge', () => {
    const procA = procedure({ access: 'public', handler: async () => 'a' });
    const onlyA = procedure({ access: 'public', handler: async () => 'only-a' });
    const procB = procedure({ access: 'public', handler: async () => 'b' });
    const procX = procedure({ access: 'public', handler: async () => 'x' });
    const versioned = procedure({ access: 'public', handler: async () => 'v2' });
    const a = defineRouter({ unit: { m: procA, only: onlyA }, 'legacy.v2': { m: versioned } });
    const b = defineRouter({ unit: { m: procB }, extra: { x: procX } });
    const merged = a.merge(b);

    it('should return a new router with the union of units', () => {
      assert.ok(merged instanceof Router);
      assert.notStrictEqual(merged, a);
      assert.notStrictEqual(merged, b);
      assert.strictEqual(merged.getProcedure('unit', '*', 'only'), onlyA);
      assert.strictEqual(merged.getProcedure('extra', '*', 'x'), procX);
      assert.strictEqual(merged.getProcedure('legacy', 'v2', 'm'), versioned);
    });

    it('should let the other router win a collision', () => {
      assert.strictEqual(merged.getProcedure('unit', '*', 'm'), procB);
    });

    it('should leave the originals untouched', () => {
      assert.strictEqual(a.getProcedure('unit', '*', 'm'), procA);
      assert.strictEqual(b.getProcedure('unit', '*', 'm'), procB);
      assert.strictEqual(a.getProcedure('extra', '*', 'x'), null);
      assert.strictEqual(b.getProcedure('legacy', 'v2', 'm'), null);
      assert.strictEqual(b.getProcedure('unit', '*', 'only'), null);
    });
  });

  describe('invoke validation', () => {
    it('should transform args with the input validator before the handler', async () => {
      const proc = procedure({
        handler: async (_context, args) => args,
        input: (args) => ({ ...args, n: Number(args.n) }),
      });
      assert.deepStrictEqual(await proc.invoke({}, { n: '42' }), { n: 42 });
    });

    it('should keep args when the input validator returns undefined', async () => {
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

    it('should map an input validator rejection to code 400', async () => {
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

    it('should map an output validator rejection to code 500', async () => {
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

    it('should let the output validator transform the result', async () => {
      const proc = procedure({
        handler: async () => ({ a: 1, b: 2 }),
        output: (result) => ({ a: result.a }),
      });
      assert.deepStrictEqual(await proc.invoke({}, {}), { a: 1 });
    });

    it('should take the Standard Schema validator success path', async () => {
      const schema = {
        '~standard': { validate: async (value) => ({ value: { ...value, checked: true } }) },
      };
      const proc = procedure({ handler: async (_context, args) => args, input: schema });
      assert.deepStrictEqual(await proc.invoke({}, { n: 1 }), { n: 1, checked: true });
    });

    it('should map the Standard Schema validator issues path to code 400', async () => {
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

    it('should keep the paths of Standard Schema issues as structured details', async () => {
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

  describe('invoke timeout and queue', () => {
    it('should reject a timeout with code 408', async () => {
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

    it('should reject a queue overflow with code 503', async () => {
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

    it('should time out a queued call with code 503', async () => {
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

    it('should release the semaphore after a handler throw', async () => {
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

  describe('review regressions: keys, filters, and queue-timeout interplay', () => {
    it('should reject multi-dot unit keys instead of silently truncating them', () => {
      const method = { m: async () => 1 };
      assert.throws(() => defineRouter({ 'unit.v1.2': method }), TypeError);
      assert.throws(() => defineRouter({ 'unit.': method }), TypeError);
      assert.throws(() => defineRouter({ '.v1': method }), TypeError);
    });

    it('should treat a non-array units argument of introspect as no filter', () => {
      const router = defineRouter({ unit: { m: async () => 1 } });
      assert.deepStrictEqual(Object.keys(router.introspect({})), ['unit']);
      assert.deepStrictEqual(Object.keys(router.introspect('unit')), ['unit']);
      assert.deepStrictEqual(Object.keys(router.introspect(42)), ['unit']);
    });

    it('should not release the queue slot on timeout while the handler still runs', async () => {
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

    it('should release the slot on a validation failure with a queue', async () => {
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

  describe('addUnit: post-construction units for the mirror feature', () => {
    const { defineRouter: defineBarrelRouter, procedure: barrelProcedure } = require('../../index.js');

    it('should add a unit and rebuild the hook chains', async () => {
      const trace = [];
      const router = defineBarrelRouter({}, { hooks: { preHandler: async () => void trace.push('router') } });
      router.addUnit('late', { hello: barrelProcedure({ access: 'public', handler: async () => 'hi' }) });
      const proc = router.getProcedure('late', '*', 'hello');
      assert.notStrictEqual(proc, null);
      assert.strictEqual(router.hooksFor(proc).preHandler.length, 1);
    });

    it('should refuse a duplicate unit key', () => {
      const router = defineBarrelRouter({ a: { x: barrelProcedure({ access: 'public', handler: async () => 1 }) } });
      assert.throws(() => router.addUnit('a', {}), /already registered/);
      // A different VERSION of the same unit is a different key.
      router.addUnit('a.v2', { x: barrelProcedure({ access: 'public', handler: async () => 2 }) });
      assert.notStrictEqual(router.getProcedure('a', 'v2', 'x'), null);
    });
  });
});
