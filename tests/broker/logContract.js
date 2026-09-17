'use strict';

// The log contract (src/broker/port.js), executable.
//
// harness: {
//   open(): Promise<{ log, peer?, close(), trim?(topic, keep), foreignId?(), beyondTip?(topic, lastId) }>
//     `peer` is a second log instance on the same broker (another process,
//     in production). `trim` drops all but the newest `keep` entries,
//     `foreignId` answers a well-formed id minted elsewhere, and
//     `beyondTip` an id past the end — each optional, its test skipped
//     without it.
//   prepare?(topic): Promise   create the topic where the broker needs it
//   timeout?: number
// }

const assert = require('node:assert');
const timers = require('node:timers/promises');

const { isBrokerLog } = require('../../src/broker/port.js');
const { unique, collect, firstError, waitFor } = require('./support.js');

const runLogContract = async (t, name, harness) => {
  const timeout = harness.timeout ?? 2000;

  const open = async (sub) => {
    const env = await harness.open();
    sub.after(() => env.close());
    return env;
  };
  const topicFor = async (prefix) => {
    const topic = unique(prefix);
    if (harness.prepare) await harness.prepare(topic);
    return topic;
  };
  const appendAll = async (log, topic, values, options) => {
    const ids = [];
    for (const value of values) ids.push(await log.append(topic, value, options));
    return ids;
  };

  await t.test(`${name}: satisfies the structural contract`, async (sub) => {
    const { log } = await open(sub);
    assert.strictEqual(isBrokerLog(log), true);
  });

  await t.test(`${name}: append answers string ids; earliest reads everything in order`, async (sub) => {
    const { log } = await open(sub);
    const topic = await topicFor('log-order');
    const ids = await appendAll(log, topic, ['a', 'b', 'c']);
    for (const id of ids) assert.strictEqual(typeof id, 'string');
    assert.strictEqual(new Set(ids).size, 3);
    const read = log.read(topic, { from: 'earliest' });
    const entries = await collect(read, 3, { timeout });
    assert.deepStrictEqual(
      entries.map((entry) => entry.value),
      ['a', 'b', 'c'],
    );
  });

  await t.test(`${name}: values and headers survive the round trip`, async (sub) => {
    const { log } = await open(sub);
    const topic = await topicFor('log-headers');
    const value = JSON.stringify({ text: 'привіт 👋', n: 1 });
    await log.append(topic, value, { headers: { tp: '00-abc-def-01', 'x-tenant': 't1' } });
    const [entry] = await collect(log.read(topic, { from: 'earliest' }), 1, { timeout });
    assert.strictEqual(entry.value, value);
    assert.strictEqual(entry.headers.tp, '00-abc-def-01');
    assert.strictEqual(entry.headers['x-tenant'], 't1');
  });

  await t.test(`${name}: 'latest' reads only what is appended once the read is ready`, async (sub) => {
    const { log } = await open(sub);
    const topic = await topicFor('log-latest');
    await appendAll(log, topic, ['old-1', 'old-2']);
    const read = log.read(topic, { from: 'latest' });
    await read.ready;
    const pending = collect(read, 2, { timeout });
    await appendAll(log, topic, ['new-1', 'new-2']);
    const entries = await pending;
    assert.deepStrictEqual(
      entries.map((entry) => entry.value),
      ['new-1', 'new-2'],
    );
  });

  await t.test(`${name}: a yielded id resumes exactly after its entry`, async (sub) => {
    const { log } = await open(sub);
    const topic = await topicFor('log-resume');
    await appendAll(log, topic, ['1', '2', '3', '4', '5']);
    const first = await collect(log.read(topic, { from: 'earliest' }), 2, { timeout });
    const resumed = log.read(topic, { after: first[1].id });
    const rest = await collect(resumed, 3, { timeout });
    assert.deepStrictEqual(
      rest.map((entry) => entry.value),
      ['3', '4', '5'],
    );
    // ...and keeps going live past what existed when it resumed.
    const tail = log.read(topic, { after: rest[2].id });
    const pending = collect(tail, 1, { timeout });
    await tail.ready;
    await log.append(topic, '6');
    assert.deepStrictEqual(
      (await pending).map((entry) => entry.value),
      ['6'],
    );
  });

  await t.test(`${name}: parseId accepts what the log minted and refuses garbage`, async (sub) => {
    const { log } = await open(sub);
    const topic = await topicFor('log-parse');
    await log.append(topic, 'x');
    const [entry] = await collect(log.read(topic, { from: 'earliest' }), 1, { timeout });
    assert.strictEqual(log.parseId(entry.id), entry.id);
    for (const garbage of [42, null, undefined, {}, '', 'x'.repeat(10_000), '../../etc/passwd', '{"$gt":""}']) {
      assert.strictEqual(log.parseId(garbage), null, `parseId(${String(garbage).slice(0, 20)})`);
    }
  });

  await t.test(`${name}: a malformed after-id fails the read with 400`, async (sub) => {
    const { log } = await open(sub);
    const topic = await topicFor('log-malformed');
    await log.append(topic, 'x');
    const error = await firstError(log.read(topic, { after: 'not an id at all' }), { timeout });
    assert.strictEqual(error.code, 400);
  });

  await t.test(`${name}: an id past the tip fails the read with 400`, async (sub) => {
    const env = await open(sub);
    if (!env.beyondTip) return sub.skip('harness has no beyondTip');
    const topic = await topicFor('log-future');
    const [id] = await appendAll(env.log, topic, ['x']);
    const error = await firstError(env.log.read(topic, { after: await env.beyondTip(topic, id) }), { timeout });
    assert.strictEqual(error.code, 400);
  });

  await t.test(`${name}: an id from trimmed history fails the read with 410`, async (sub) => {
    const env = await open(sub);
    if (!env.trim) return sub.skip('harness has no trim');
    const topic = await topicFor('log-trimmed');
    const ids = await appendAll(env.log, topic, ['1', '2', '3', '4', '5', '6']);
    await env.trim(topic, 2);
    const error = await firstError(env.log.read(topic, { after: ids[0] }), { timeout });
    assert.strictEqual(error.code, 410);
  });

  await t.test(`${name}: an id minted by another log fails the read with 410`, async (sub) => {
    const env = await open(sub);
    if (!env.foreignId) return sub.skip('harness has no foreignId');
    const topic = await topicFor('log-foreign');
    await env.log.append(topic, 'x');
    const error = await firstError(env.log.read(topic, { after: await env.foreignId(topic) }), { timeout });
    assert.strictEqual(error.code, 410);
  });

  await t.test(`${name}: aborting the signal ends a waiting read cleanly`, async (sub) => {
    const { log } = await open(sub);
    const topic = await topicFor('log-abort');
    const controller = new AbortController();
    const read = log.read(topic, { from: 'latest', signal: controller.signal });
    await read.ready;
    const iterator = read[Symbol.asyncIterator]();
    const next = iterator.next();
    await timers.setTimeout(20);
    controller.abort();
    const result = await Promise.race([next, timers.setTimeout(timeout, 'timeout')]);
    assert.notStrictEqual(result, 'timeout', 'the read did not end on abort');
    assert.strictEqual(result.done, true);
  });

  await t.test(`${name}: concurrent readers of one topic each see every entry`, async (sub) => {
    const { log } = await open(sub);
    const topic = await topicFor('log-fanout');
    const readers = [log.read(topic, { from: 'latest' }), log.read(topic, { from: 'latest' })];
    await Promise.all(readers.map((read) => read.ready));
    const pending = readers.map((read) => collect(read, 3, { timeout }));
    await appendAll(log, topic, ['x', 'y', 'z']);
    for (const entries of await Promise.all(pending)) {
      assert.deepStrictEqual(
        entries.map((entry) => entry.value),
        ['x', 'y', 'z'],
      );
    }
  });

  await t.test(`${name}: breaking out of one read releases it; the topic stays readable`, async (sub) => {
    const { log } = await open(sub);
    const topic = await topicFor('log-release');
    await appendAll(log, topic, ['1', '2']);
    for await (const entry of log.read(topic, { from: 'earliest' })) {
      assert.strictEqual(entry.value, '1');
      break;
    }
    const again = await collect(log.read(topic, { from: 'earliest' }), 2, { timeout });
    assert.strictEqual(again.length, 2);
  });

  await t.test(`${name}: topics are isolated`, async (sub) => {
    const { log } = await open(sub);
    const left = await topicFor('log-left');
    const right = await topicFor('log-right');
    const read = log.read(left, { from: 'latest' });
    await read.ready;
    const pending = collect(read, 1, { timeout });
    await log.append(right, 'wrong');
    await log.append(left, 'right');
    assert.deepStrictEqual(
      (await pending).map((entry) => entry.value),
      ['right'],
    );
  });

  await t.test(`${name}: an entry appended through a peer instance is read here`, async (sub) => {
    const env = await open(sub);
    if (!env.peer) return sub.skip('harness has no peer');
    const topic = await topicFor('log-peer');
    const read = env.log.read(topic, { from: 'latest' });
    await read.ready;
    const pending = collect(read, 1, { timeout });
    await env.peer.append(topic, 'from the peer');
    const [entry] = await pending;
    assert.strictEqual(entry.value, 'from the peer');
    await waitFor(() => env.log.parseId(entry.id) === entry.id, { timeout });
  });
};

module.exports = { runLogContract };
