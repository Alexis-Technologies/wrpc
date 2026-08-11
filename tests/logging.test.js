'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { Writable } = require('node:stream');

const pino = require('pino');

const { createLoggerWriter, LOG_WRITER } = require('../src/logging.js');

// A real pino writing JSON lines into memory: the only way to be sure the
// (mergingObject, message) convention is honoured is to read what pino wrote.
const createPinoCollector = (level = 'trace') => {
  const chunks = [];
  const destination = new Writable({
    write(chunk, encoding, callback) {
      chunks.push(chunk.toString());
      callback();
    },
  });
  const logger = pino({ level, base: null, timestamp: false }, destination);
  const entries = () =>
    chunks
      .join('')
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  return { logger, entries };
};

const createConsoleCollector = (names = ['log', 'info', 'debug', 'warn', 'error']) => {
  const calls = [];
  const sink = {};
  for (const name of names) sink[name] = (...args) => calls.push([name, ...args]);
  return { sink, calls };
};

test('createLoggerWriter detection ladder', async (t) => {
  await t.test('falsy options disable logging', () => {
    for (const value of [undefined, null, false, 0, '']) {
      const writer = createLoggerWriter(value);
      assert.strictEqual(writer.enabled, false);
    }
  });

  await t.test('non-object truthy options disable logging', () => {
    assert.strictEqual(createLoggerWriter('pino').enabled, false);
    assert.strictEqual(createLoggerWriter(42).enabled, false);
  });

  await t.test('true logs to the global console', () => {
    const original = globalThis.console;
    const { sink, calls } = createConsoleCollector();
    globalThis.console = sink;
    try {
      createLoggerWriter(true).warn({ event: 'x' }, 'careful');
    } finally {
      globalThis.console = original;
    }
    assert.deepStrictEqual(calls, [['warn', 'careful']]);
  });

  await t.test('an object with no known method disables logging', () => {
    assert.strictEqual(createLoggerWriter({ emit() {} }).enabled, false);
  });

  await t.test('a console-shaped object takes the console branch', () => {
    const { sink, calls } = createConsoleCollector();
    createLoggerWriter(sink).info({ event: 'x' }, 'hello');
    assert.deepStrictEqual(calls, [['info', 'hello']]);
  });

  await t.test('a partial console with only table takes the console branch', () => {
    const calls = [];
    const writer = createLoggerWriter({ table() {}, error: (...args) => calls.push(args) });
    writer.error({ event: 'x' }, 'bad');
    assert.deepStrictEqual(calls, [['bad']]);
  });

  await t.test('a bare error-only sink is treated as a console, not disabled', () => {
    const errors = [];
    const writer = createLoggerWriter({ error: (value) => errors.push(value) });
    assert.strictEqual(writer.enabled, true);
    const error = new Error('boom');
    writer.error({ err: error, event: 'x' });
    assert.deepStrictEqual(errors, [error]);
  });

  await t.test('a level property alone marks a sink structured', () => {
    const calls = [];
    const writer = createLoggerWriter({ level: 'info', info: (...args) => calls.push(args) });
    writer.info({ event: 'x' }, 'hello');
    assert.deepStrictEqual(calls, [[{ event: 'x' }, 'hello']]);
  });

  await t.test('a pino takes the structured branch', () => {
    const { logger, entries } = createPinoCollector();
    createLoggerWriter(logger).info({ event: 'call.ok', method: 'chat/send' }, 'done');
    const [entry] = entries();
    assert.strictEqual(entry.msg, 'done');
    assert.strictEqual(entry.event, 'call.ok');
    assert.strictEqual(entry.method, 'chat/send');
    assert.strictEqual(entry.level, 30);
  });

  await t.test('a writer is returned unchanged (idempotent)', () => {
    const writer = createLoggerWriter(pino({ level: 'silent' }));
    assert.strictEqual(createLoggerWriter(writer), writer);
    assert.strictEqual(writer[LOG_WRITER], true);
  });

  await t.test('the disabled writer is a shared frozen singleton', () => {
    const writer = createLoggerWriter(false);
    assert.strictEqual(createLoggerWriter(null), writer);
    assert.strictEqual(createLoggerWriter(writer), writer);
    assert.strictEqual(Object.isFrozen(writer), true);
  });
});

test('writer levels', async (t) => {
  await t.test('every level reaches the matching pino level', () => {
    const { logger, entries } = createPinoCollector();
    const writer = createLoggerWriter(logger);
    writer.log({}, 'a');
    writer.info({}, 'b');
    writer.debug({}, 'c');
    writer.warn({}, 'd');
    writer.error({}, 'e');
    assert.deepStrictEqual(
      entries().map((entry) => [entry.level, entry.msg]),
      [
        [30, 'a'],
        [30, 'b'],
        [20, 'c'],
        [40, 'd'],
        [50, 'e'],
      ],
    );
  });

  await t.test('log falls back to info on a sink without log', () => {
    const calls = [];
    const writer = createLoggerWriter({ level: 'info', info: (...args) => calls.push(['info', ...args]) });
    writer.log({ event: 'x' }, 'hello');
    assert.deepStrictEqual(calls, [['info', { event: 'x' }, 'hello']]);
  });

  await t.test('error falls back to warn, then to log', () => {
    const warned = [];
    const warnOnly = createLoggerWriter({ debug() {}, warn: (...args) => warned.push(args) });
    warnOnly.error({}, 'bad');
    assert.strictEqual(warned.length, 1);

    const logged = [];
    const logOnly = createLoggerWriter({ log: (...args) => logged.push(args) });
    logOnly.error({}, 'bad');
    logOnly.warn({}, 'careful');
    assert.deepStrictEqual(logged, [['bad'], ['careful']]);
  });

  await t.test('a level with no reachable method is a no-op', () => {
    const writer = createLoggerWriter({ debug() {} });
    assert.doesNotThrow(() => writer.warn({}, 'nowhere'));
    assert.doesNotThrow(() => writer.error({}, 'nowhere'));
  });

  await t.test('console writer prints the error itself when there is no message', () => {
    const { sink, calls } = createConsoleCollector();
    const error = new Error('boom');
    createLoggerWriter(sink).error({ err: error, event: 'x' });
    assert.deepStrictEqual(calls, [['error', error]]);
  });

  await t.test('console writer falls back to the entry when there is no message or err', () => {
    const { sink, calls } = createConsoleCollector();
    createLoggerWriter(sink).log({ event: 'x' });
    assert.deepStrictEqual(calls, [['log', { event: 'x' }]]);
  });
});

test('writer children', async (t) => {
  await t.test('a structured child carries its bindings', () => {
    const { logger, entries } = createPinoCollector();
    const writer = createLoggerWriter(logger).child({ component: 'rooms' });
    writer.child({ peer: 'ws://1' }).warn({ event: 'broadcast.send' }, 'dropped');
    const [entry] = entries();
    assert.strictEqual(entry.component, 'rooms');
    assert.strictEqual(entry.peer, 'ws://1');
    assert.strictEqual(entry.event, 'broadcast.send');
  });

  await t.test('a console child is the same writer', () => {
    const { sink } = createConsoleCollector();
    const writer = createLoggerWriter(sink);
    assert.strictEqual(writer.child({ component: 'rooms' }), writer);
  });

  await t.test('a structured sink without child returns the same writer', () => {
    const writer = createLoggerWriter({ level: 'info', info() {}, debug() {} });
    assert.strictEqual(writer.child({ component: 'rooms' }), writer);
  });

  await t.test('a child that throws degrades to the parent', () => {
    const writer = createLoggerWriter({
      info() {},
      child() {
        throw new Error('no children');
      },
    });
    assert.strictEqual(writer.child({ component: 'rooms' }), writer);
  });

  await t.test('the disabled writer child is itself', () => {
    const writer = createLoggerWriter(false);
    assert.strictEqual(writer.child({ component: 'rooms' }), writer);
  });
});

test('a logger that throws never escapes the writer', async (t) => {
  await t.test('every level of a throwing structured sink is contained', () => {
    const throwing = { level: 'trace' };
    for (const level of ['info', 'debug', 'warn', 'error']) {
      throwing[level] = () => {
        throw new Error(level);
      };
    }
    const writer = createLoggerWriter(throwing);
    for (const level of ['log', 'info', 'debug', 'warn', 'error']) {
      assert.doesNotThrow(() => writer[level]({ event: 'x' }, 'message'), level);
    }
  });

  await t.test('every level of a throwing console sink is contained', () => {
    const throwing = {};
    for (const level of ['log', 'info', 'debug', 'warn', 'error']) {
      throwing[level] = () => {
        throw new Error(level);
      };
    }
    const writer = createLoggerWriter(throwing);
    for (const level of ['log', 'info', 'debug', 'warn', 'error']) {
      assert.doesNotThrow(() => writer[level]({ event: 'x' }, 'message'), level);
    }
  });
});
