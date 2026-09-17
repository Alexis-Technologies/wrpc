'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const {
  isBroker,
  isBrokerLog,
  isBrokerQueue,
  isBrokerDirect,
  capabilityOf,
  brokerName,
} = require('../../src/broker/port.js');
const { codedError, toText, toBytes, toHeaders, encodeToken, signId, openId } = require('../../src/broker/ids.js');
const { DEFAULT_RETRY, normalizeRetry, decide } = require('../../src/broker/retry.js');

const noop = () => {};
const log = { append: noop, read: noop, parseId: noop };
const queue = { produce: noop, consume: noop };
const direct = { inbox: noop, listen: noop, send: noop };
const backplane = { publish: noop, subscribe: noop, close: noop };

test('port: capability checks are structural', () => {
  assert.strictEqual(isBrokerLog(log), true);
  assert.strictEqual(isBrokerLog({ append: noop, read: noop }), false);
  assert.strictEqual(isBrokerQueue(queue), true);
  assert.strictEqual(isBrokerDirect(direct), true);
  assert.strictEqual(isBrokerDirect(null), false);
  assert.strictEqual(isBroker({ close: noop, log }), true);
  assert.strictEqual(isBroker({ close: noop, backplane, queue, direct, log: null }), true);
  assert.strictEqual(isBroker({ close: noop }), false, 'no capability at all');
  assert.strictEqual(isBroker({ log }), false, 'no close');
  assert.strictEqual(isBroker({ close: noop, log: { append: noop } }), false, 'a malformed capability');
});

test('port: capabilityOf accepts a broker or the bare capability', () => {
  assert.strictEqual(capabilityOf(log, 'log', 'x'), log);
  assert.strictEqual(capabilityOf({ name: 'nats', close: noop, log }, 'log', 'x'), log);
  assert.throws(() => capabilityOf({ name: 'kafka', close: noop, log }, 'direct', 'attachBrokerRpc'), {
    message: "attachBrokerRpc: the 'kafka' broker has no 'direct' capability",
  });
  assert.throws(() => capabilityOf(42, 'queue', 'x'), /the given broker has no 'queue'/);
  assert.strictEqual(brokerName({ name: 'redis' }), 'redis');
  assert.strictEqual(brokerName({ name: '' }), 'custom');
  assert.strictEqual(brokerName(null), 'custom');
});

test('ids: coded errors are exposed', () => {
  const error = codedError('nope', 410);
  assert.deepStrictEqual([error.message, error.code, error.expose], ['nope', 410, true]);
});

test('ids: body and header normalization', () => {
  const bytes = new TextEncoder().encode('привіт');
  assert.strictEqual(toText('x'), 'x');
  assert.strictEqual(toText(bytes), 'привіт');
  assert.strictEqual(toText(Buffer.from('buf')), 'buf');
  assert.strictEqual(toText(new DataView(bytes.buffer)), 'привіт');
  assert.strictEqual(toText(bytes.buffer), 'привіт');
  assert.strictEqual(toText(null), '');
  assert.strictEqual(toText(12), '12');
  assert.deepStrictEqual(toBytes('привіт'), bytes);
  assert.strictEqual(toBytes(bytes), bytes);
  assert.deepStrictEqual(toBytes(new DataView(bytes.buffer)), bytes);
  assert.deepStrictEqual(toBytes(bytes.buffer), bytes);
  assert.deepStrictEqual(toBytes(undefined), new Uint8Array(0));
  assert.deepStrictEqual(toBytes(7), new TextEncoder().encode('7'));

  const headers = toHeaders({ a: 'x', b: Buffer.from('y'), c: ['z', 'ignored'], d: null, e: undefined, n: 5 });
  assert.strictEqual(Object.getPrototypeOf(headers), null);
  assert.deepStrictEqual({ ...headers }, { a: 'x', b: 'y', c: 'z', n: '5' });
  const hostile = toHeaders(JSON.parse('{"__proto__":"polluted"}'));
  assert.strictEqual(Object.getOwnPropertyDescriptor(hostile, '__proto__').value, 'polluted');
  assert.strictEqual({}.polluted, undefined);
  assert.deepStrictEqual({ ...toHeaders(null) }, {});
  assert.deepStrictEqual({ ...toHeaders('nope') }, {});
});

test('ids: encodeToken is safe, injective and bounded', () => {
  assert.strictEqual(encodeToken('room-1_ok'), 'room-1_ok');
  assert.strictEqual(encodeToken('room:*'), 'room~3A~2A');
  assert.strictEqual(encodeToken('a.b >c'), 'a~2Eb~20~3Ec');
  // The escape character is itself escaped: two different names never collide.
  assert.notStrictEqual(encodeToken('~3A'), encodeToken(':'));
  assert.strictEqual(encodeToken('~'), '~7E');
  assert.strictEqual(encodeToken('😀'), '~F0~9F~98~80');
  for (const name of ['room:*', 'a.b', 'кімната', 'x'.repeat(1000)]) {
    assert.match(encodeToken(name), /^[A-Za-z0-9_~-]+$/);
  }
  // A custom alphabet (Kafka: dots allowed, `_` as the escape).
  assert.strictEqual(encodeToken('a.b:c_d', { safe: /[A-Za-z0-9.-]/, escape: '_' }), 'a.b_3Ac_5Fd');
  // Past maxLength: shortened, distinct, and within the bound.
  const long1 = encodeToken(`${'r'.repeat(300)}1`, { maxLength: 100 });
  const long2 = encodeToken(`${'r'.repeat(300)}2`, { maxLength: 100 });
  assert.ok(long1.length <= 100);
  assert.notStrictEqual(long1, long2);
  assert.ok(encodeToken('x'.repeat(50), { maxLength: 20 }).length <= 20);
});

test('ids: signed ids verify, and a tampered one does not', () => {
  const signed = signId('secret', '1726000000000-0');
  assert.match(signed, /^1726000000000-0!/);
  assert.strictEqual(openId('secret', signed), '1726000000000-0');
  assert.strictEqual(openId('other', signed), null);
  assert.strictEqual(openId('secret', signed.replace('1726', '0000')), null);
  assert.strictEqual(openId('secret', `${signed}x`), null);
  // Ids that contain the separator still split on the last one.
  const nested = signId('secret', 'k1:0=1!odd');
  assert.strictEqual(openId('secret', nested), 'k1:0=1!odd');
  assert.strictEqual(openId('secret', 'no-signature'), null);
  assert.strictEqual(openId('secret', '!sig'), null);
  assert.strictEqual(openId('secret', 42), null);
});

test('retry: normalization', () => {
  assert.strictEqual(normalizeRetry(undefined, 'x'), DEFAULT_RETRY);
  assert.strictEqual(normalizeRetry(false, 'x').attempts, 1);
  const custom = normalizeRetry({ attempts: 3, backoff: { base: 10 }, retryOn: [503] }, 'x');
  assert.deepStrictEqual(custom.backoff, { base: 10, max: 60_000, factor: 2, jitter: true });
  assert.deepStrictEqual(custom.retryOn, [503]);
  assert.throws(() => normalizeRetry('x', 'bind'), /bind: retry must be an object or false/);
  assert.throws(() => normalizeRetry({ attempts: 0 }, 'x'), /attempts/);
  assert.throws(() => normalizeRetry({ backoff: null }, 'x'), /backoff must be an object/);
  assert.throws(() => normalizeRetry({ backoff: { base: -1 } }, 'x'), /backoff needs/);
  assert.throws(() => normalizeRetry({ backoff: { factor: 0 } }, 'x'), /backoff needs/);
  assert.throws(() => normalizeRetry({ retryOn: ['500'] }, 'x'), /retryOn/);
});

test('retry: the decision table', () => {
  const at = (code, attempt, extra = {}) => decide({ code, attempt, random: () => 1, ...extra });
  assert.deepStrictEqual(at(null, 1), { action: 'ack', delay: 0 });
  assert.deepStrictEqual(at(undefined, 3), { action: 'ack', delay: 0 });
  for (const code of [408, 429, 500, 503]) assert.strictEqual(at(code, 1).action, 'retry', `code ${code}`);
  for (const code of [400, 403, 404, 422, 501]) assert.strictEqual(at(code, 1).action, 'dead', `code ${code}`);
  assert.strictEqual(at(500, 5).action, 'dead', 'attempts exhausted');
  assert.strictEqual(at(500, 4).action, 'retry');
  assert.deepStrictEqual(at(503, 5, { draining: true }), { action: 'release', delay: 0 });
  assert.strictEqual(at(500, 1, { draining: true }).action, 'retry');
  // Backoff grows and caps: base 1000 * 2^(attempt-1), capped at 60 s.
  assert.strictEqual(at(500, 1).delay, 1000);
  assert.strictEqual(at(500, 3).delay, 4000);
  const capped = decide({ code: 500, attempt: 9, random: () => 1, retry: normalizeRetry({ attempts: 20 }, 'x') });
  assert.strictEqual(capped.delay, 60_000);
  const jittered = decide({ code: 500, attempt: 2, random: () => 0.5 });
  assert.strictEqual(jittered.delay, 1000);
});
