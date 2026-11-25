'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
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

describe('createLoggerWriter', () => {
  describe('detection ladder', () => {
    it('should disable logging for falsy options', () => {
      for (const value of [undefined, null, false, 0, '']) {
        const writer = createLoggerWriter(value);
        assert.strictEqual(writer.enabled, false);
      }
    });

    it('should disable logging for non-object truthy options', () => {
      assert.strictEqual(createLoggerWriter('pino').enabled, false);
      assert.strictEqual(createLoggerWriter(42).enabled, false);
    });

    it('should log to the global console for true', () => {
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

    it('should disable logging for an object with no known method', () => {
      assert.strictEqual(createLoggerWriter({ emit() {} }).enabled, false);
    });

    it('should take the console branch for a console-shaped object', () => {
      const { sink, calls } = createConsoleCollector();
      createLoggerWriter(sink).info({ event: 'x' }, 'hello');
      assert.deepStrictEqual(calls, [['info', 'hello']]);
    });

    it('should take the console branch for a partial console with only table', () => {
      const calls = [];
      const writer = createLoggerWriter({ table() {}, error: (...args) => calls.push(args) });
      writer.error({ event: 'x' }, 'bad');
      assert.deepStrictEqual(calls, [['bad']]);
    });

    it('should treat a bare error-only sink as a console, not disable it', () => {
      const errors = [];
      const writer = createLoggerWriter({ error: (value) => errors.push(value) });
      assert.strictEqual(writer.enabled, true);
      const error = new Error('boom');
      writer.error({ err: error, event: 'x' });
      assert.deepStrictEqual(errors, [error]);
    });

    it('should mark a sink structured by a level property alone', () => {
      const calls = [];
      const writer = createLoggerWriter({ level: 'info', info: (...args) => calls.push(args) });
      writer.info({ event: 'x' }, 'hello');
      assert.deepStrictEqual(calls, [[{ event: 'x' }, 'hello']]);
    });

    it('should take the structured branch for a pino', () => {
      const { logger, entries } = createPinoCollector();
      createLoggerWriter(logger).info({ event: 'call.ok', method: 'chat/send' }, 'done');
      const [entry] = entries();
      assert.strictEqual(entry.msg, 'done');
      assert.strictEqual(entry.event, 'call.ok');
      assert.strictEqual(entry.method, 'chat/send');
      assert.strictEqual(entry.level, 30);
    });

    it('should return a writer unchanged (idempotent)', () => {
      const writer = createLoggerWriter(pino({ level: 'silent' }));
      assert.strictEqual(createLoggerWriter(writer), writer);
      assert.strictEqual(writer[LOG_WRITER], true);
    });

    it('should make the disabled writer a shared frozen singleton', () => {
      const writer = createLoggerWriter(false);
      assert.strictEqual(createLoggerWriter(null), writer);
      assert.strictEqual(createLoggerWriter(writer), writer);
      assert.strictEqual(Object.isFrozen(writer), true);
    });
  });

  describe('writer levels', () => {
    it('should send every level to the matching pino level', () => {
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

    it('should fall back to info for log on a sink without log', () => {
      const calls = [];
      const writer = createLoggerWriter({ level: 'info', info: (...args) => calls.push(['info', ...args]) });
      writer.log({ event: 'x' }, 'hello');
      assert.deepStrictEqual(calls, [['info', { event: 'x' }, 'hello']]);
    });

    it('should fall back from error to warn, then to log', () => {
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

    it('should do nothing for a level with no reachable method', () => {
      const writer = createLoggerWriter({ debug() {} });
      assert.doesNotThrow(() => writer.warn({}, 'nowhere'));
      assert.doesNotThrow(() => writer.error({}, 'nowhere'));
    });

    it('should print the error itself from a console writer when there is no message', () => {
      const { sink, calls } = createConsoleCollector();
      const error = new Error('boom');
      createLoggerWriter(sink).error({ err: error, event: 'x' });
      assert.deepStrictEqual(calls, [['error', error]]);
    });

    it('should print the entry from a console writer when there is no message or err', () => {
      const { sink, calls } = createConsoleCollector();
      createLoggerWriter(sink).log({ event: 'x' });
      assert.deepStrictEqual(calls, [['log', { event: 'x' }]]);
    });
  });

  describe('writer children', () => {
    it('should carry its bindings in a structured child', () => {
      const { logger, entries } = createPinoCollector();
      const writer = createLoggerWriter(logger).child({ component: 'rooms' });
      writer.child({ peer: 'ws://1' }).warn({ event: 'broadcast.send' }, 'dropped');
      const [entry] = entries();
      assert.strictEqual(entry.component, 'rooms');
      assert.strictEqual(entry.peer, 'ws://1');
      assert.strictEqual(entry.event, 'broadcast.send');
    });

    it('should answer the same writer for a console child', () => {
      const { sink } = createConsoleCollector();
      const writer = createLoggerWriter(sink);
      assert.strictEqual(writer.child({ component: 'rooms' }), writer);
    });

    it('should answer the same writer for a structured sink without child', () => {
      const writer = createLoggerWriter({ level: 'info', info() {}, debug() {} });
      assert.strictEqual(writer.child({ component: 'rooms' }), writer);
    });

    it('should degrade to the parent when child throws', () => {
      const writer = createLoggerWriter({
        info() {},
        child() {
          throw new Error('no children');
        },
      });
      assert.strictEqual(writer.child({ component: 'rooms' }), writer);
    });

    it('should answer the disabled writer itself for its child', () => {
      const writer = createLoggerWriter(false);
      assert.strictEqual(writer.child({ component: 'rooms' }), writer);
    });
  });

  describe('a logger that throws', () => {
    it('should contain every level of a throwing structured sink, never escaping the writer', () => {
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

    it('should contain every level of a throwing console sink, never escaping the writer', () => {
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
});
