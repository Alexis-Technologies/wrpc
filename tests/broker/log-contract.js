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

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const timers = require('node:timers/promises');

const { isBrokerLog } = require('../../src/broker/port.js');
const { unique, collect, firstError, waitFor } = require('./support.js');

// Registers one `describe(title)` of `it` cases. `options` goes to every case:
// an integration suite's `{ skip }` lists each of them as skipped (a skipped
// describe never runs its body, so its cases would not show at all).
const runLogContract = (title, harness, options = {}) =>
  describe(title, () => {
    const timeout = harness.timeout ?? 2000;

    const open = async (t) => {
      const env = await harness.open();
      t.after(() => env.close());
      return env;
    };
    const topicFor = async (prefix) => {
      const topic = unique(prefix);
      if (harness.prepare) await harness.prepare(topic);
      return topic;
    };
    const appendAll = async (log, topic, values, appendOptions) => {
      const ids = [];
      for (const value of values) ids.push(await log.append(topic, value, appendOptions));
      return ids;
    };

    it('should satisfy the structural contract', options, async (t) => {
      const { log } = await open(t);
      assert.strictEqual(isBrokerLog(log), true);
    });

    it('should answer string ids from append, and read everything in order from earliest', options, async (t) => {
      const { log } = await open(t);
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

    it('should carry values and headers through the round trip', options, async (t) => {
      const { log } = await open(t);
      const topic = await topicFor('log-headers');
      const value = JSON.stringify({ text: 'привіт 👋', n: 1 });
      await log.append(topic, value, { headers: { tp: '00-abc-def-01', 'x-tenant': 't1' } });
      const [entry] = await collect(log.read(topic, { from: 'earliest' }), 1, { timeout });
      assert.strictEqual(entry.value, value);
      assert.strictEqual(entry.headers.tp, '00-abc-def-01');
      assert.strictEqual(entry.headers['x-tenant'], 't1');
    });

    it("should read from 'latest' only what is appended once the read is ready", options, async (t) => {
      const { log } = await open(t);
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

    it("should keep, from 'latest', what is appended between ready and the first next()", options, async (t) => {
      const { log } = await open(t);
      const topic = await topicFor('log-ready');
      await appendAll(log, topic, ['old']);
      const read = log.read(topic, { from: 'latest' });
      await read.ready;
      await appendAll(log, topic, ['during']);
      // A window, not a wait: by now the entry sits in the shared tail's queue
      // on every adapter, and the read has not been iterated yet. A position
      // taken at the first next() rather than at ready would call it covered.
      await timers.setTimeout(50);
      const pending = collect(read, 2, { timeout });
      await appendAll(log, topic, ['after']);
      assert.deepStrictEqual(
        (await pending).map((entry) => entry.value),
        ['during', 'after'],
      );
    });

    it("should release the broker's live read when a read is abandoned before its first next()", options, async (t) => {
      const env = await open(t);
      // Only where the harness can count: an in-process broker has nothing to
      // release, and a fake can say how many readers are joined.
      if (typeof env.liveReads !== 'function') return;
      const topic = await topicFor('log-abandon');
      const read = env.log.read(topic, { from: 'latest' });
      await read.ready;
      await read[Symbol.asyncIterator]().return();
      await waitFor(() => env.liveReads() === 0, { timeout, message: `live reads held: ${env.liveReads()}` });
      const controller = new AbortController();
      const other = env.log.read(topic, { from: 'latest', signal: controller.signal });
      await other.ready;
      controller.abort();
      await waitFor(() => env.liveReads() === 0, { timeout, message: `live reads held: ${env.liveReads()}` });
    });

    it('should read every entry to the tip in a catch-up, however the broker pages', options, async (t) => {
      const { log } = await open(t);
      const topic = await topicFor('catchup');
      // More than one page (256), with whatever pauses the broker puts in the
      // deliveries: a page cut short by a pause used to pass for the tip, and
      // the reader joined the live tail past what it had not read.
      const values = Array.from({ length: 300 }, (_, i) => String(i));
      const ids = await appendAll(log, topic, values);
      const everything = await collect(log.read(topic, { from: 'earliest' }), 300, { timeout: timeout * 4 });
      assert.deepStrictEqual(
        everything.map((entry) => entry.value),
        values,
      );
      const rest = await collect(log.read(topic, { after: ids[100] }), 199, { timeout: timeout * 4 });
      assert.deepStrictEqual(
        rest.map((entry) => entry.value),
        values.slice(101),
      );
    });

    it('should replace a live tail the broker ended, and the reader misses nothing', options, async (t) => {
      const env = await open(t);
      if (typeof env.endLiveRead !== 'function') return t.skip('harness has no endLiveRead');
      const topic = await topicFor('ended');
      const read = env.log.read(topic, { from: 'latest' });
      await read.ready;
      const iterator = read[Symbol.asyncIterator]();
      await env.log.append(topic, 'before');
      assert.strictEqual((await iterator.next()).value.value, 'before');
      // The broker ends the tail on its own — a node gone, a consumer
      // cancelled: every subscription of the topic used to freeze for good.
      await env.endLiveRead(topic);
      await env.log.append(topic, 'during');
      await timers.setTimeout(50);
      await env.log.append(topic, 'after');
      assert.strictEqual((await iterator.next()).value.value, 'during');
      assert.strictEqual((await iterator.next()).value.value, 'after');
      await iterator.return();
    });

    it('should resume a yielded id exactly after its entry', options, async (t) => {
      const { log } = await open(t);
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

    it('should accept in parseId what the log minted and refuse garbage', options, async (t) => {
      const { log } = await open(t);
      const topic = await topicFor('log-parse');
      await log.append(topic, 'x');
      const [entry] = await collect(log.read(topic, { from: 'earliest' }), 1, { timeout });
      assert.strictEqual(log.parseId(entry.id), entry.id);
      for (const garbage of [42, null, undefined, {}, '', 'x'.repeat(10_000), '../../etc/passwd', '{"$gt":""}']) {
        assert.strictEqual(log.parseId(garbage), null, `parseId(${String(garbage).slice(0, 20)})`);
      }
    });

    it('should fail the read of a malformed after-id with 400', options, async (t) => {
      const { log } = await open(t);
      const topic = await topicFor('log-malformed');
      await log.append(topic, 'x');
      const error = await firstError(log.read(topic, { after: 'not an id at all' }), { timeout });
      assert.strictEqual(error.code, 400);
    });

    it('should fail the read of an id past the tip with 400', options, async (t) => {
      const env = await open(t);
      if (!env.beyondTip) return t.skip('harness has no beyondTip');
      const topic = await topicFor('log-future');
      const [id] = await appendAll(env.log, topic, ['x']);
      const error = await firstError(env.log.read(topic, { after: await env.beyondTip(topic, id) }), { timeout });
      assert.strictEqual(error.code, 400);
    });

    it('should fail the read of an id from trimmed history with 410', options, async (t) => {
      const env = await open(t);
      if (!env.trim) return t.skip('harness has no trim');
      const topic = await topicFor('log-trimmed');
      const ids = await appendAll(env.log, topic, ['1', '2', '3', '4', '5', '6']);
      await env.trim(topic, 2);
      const error = await firstError(env.log.read(topic, { after: ids[0] }), { timeout });
      assert.strictEqual(error.code, 410);
    });

    it('should fail the read of an id minted by another log with 410', options, async (t) => {
      const env = await open(t);
      if (!env.foreignId) return t.skip('harness has no foreignId');
      const topic = await topicFor('log-foreign');
      await env.log.append(topic, 'x');
      const error = await firstError(env.log.read(topic, { after: await env.foreignId(topic) }), { timeout });
      assert.strictEqual(error.code, 410);
    });

    it('should end a waiting read cleanly when its signal aborts', options, async (t) => {
      const { log } = await open(t);
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

    it('should show every entry to each concurrent reader of one topic', options, async (t) => {
      const { log } = await open(t);
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

    it('should release a read broken out of; the topic stays readable', options, async (t) => {
      const { log } = await open(t);
      const topic = await topicFor('log-release');
      await appendAll(log, topic, ['1', '2']);
      for await (const entry of log.read(topic, { from: 'earliest' })) {
        assert.strictEqual(entry.value, '1');
        break;
      }
      const again = await collect(log.read(topic, { from: 'earliest' }), 2, { timeout });
      assert.strictEqual(again.length, 2);
    });

    it('should isolate topics', options, async (t) => {
      const { log } = await open(t);
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

    it('should read here an entry appended through a peer instance', options, async (t) => {
      const env = await open(t);
      if (!env.peer) return t.skip('harness has no peer');
      const topic = await topicFor('log-peer');
      const read = env.log.read(topic, { from: 'latest' });
      await read.ready;
      const pending = collect(read, 1, { timeout });
      await env.peer.append(topic, 'from the peer');
      const [entry] = await pending;
      assert.strictEqual(entry.value, 'from the peer');
      await waitFor(() => env.log.parseId(entry.id) === entry.id, { timeout });
    });
  });

module.exports = { runLogContract };
