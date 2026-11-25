'use strict';

// The direct contract (src/broker/port.js), executable.
//
// harness: {
//   open(): Promise<{ direct, peer?, close() }>
//   timeout?: number
//   settle?: number     ms after listen() before a real broker routes to it
// }

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const timers = require('node:timers/promises');

const { isBrokerDirect } = require('../../src/broker/port.js');
const { toBytes, toText } = require('../../src/broker/ids.js');
const { unique, waitFor } = require('./support.js');

// Registers one `describe(title)` of `it` cases. `options` goes to every case:
// an integration suite's `{ skip }` lists each of them as skipped (a skipped
// describe never runs its body, so its cases would not show at all).
const runDirectContract = (title, harness, options = {}) =>
  describe(title, () => {
    const timeout = harness.timeout ?? 2000;
    const settle = harness.settle ?? 0;

    const open = async (t) => {
      const env = await harness.open();
      t.after(() => env.close());
      return env;
    };
    const listen = async (t, direct, address, handler, listenOptions) => {
      const stop = await direct.listen(address, handler, listenOptions);
      t.after(() => stop());
      if (settle > 0) await timers.setTimeout(settle);
      return stop;
    };
    const sender = (env) => env.peer ?? env.direct;

    it('should satisfy the structural contract, with unique inboxes', options, async (t) => {
      const { direct } = await open(t);
      assert.strictEqual(isBrokerDirect(direct), true);
      const a = direct.inbox();
      const b = direct.inbox();
      assert.strictEqual(typeof a, 'string');
      assert.notStrictEqual(a, b);
    });

    it('should deliver a message to the listener with its envelope', options, async (t) => {
      const env = await open(t);
      const address = env.direct.inbox();
      const seen = [];
      await listen(t, env.direct, address, (message) => seen.push(message));
      const replyTo = env.direct.inbox();
      await sender(env).send(address, 'ping', { headers: { tp: '00-abc' }, correlationId: 'c-1', replyTo });
      await waitFor(() => seen.length === 1, { timeout });
      assert.strictEqual(toText(seen[0].body), 'ping');
      assert.strictEqual(seen[0].headers.tp, '00-abc');
      assert.strictEqual(seen[0].correlationId, 'c-1');
      assert.strictEqual(seen[0].replyTo, replyTo);
    });

    it('should carry binary bodies byte for byte', options, async (t) => {
      const env = await open(t);
      const address = env.direct.inbox();
      const seen = [];
      await listen(t, env.direct, address, (message) => seen.push(message));
      const bytes = new Uint8Array(4096).map((_, i) => (i * 31) % 256);
      await sender(env).send(address, bytes);
      await waitFor(() => seen.length === 1, { timeout });
      assert.deepStrictEqual(toBytes(seen[0].body), bytes);
    });

    it('should deliver the messages of one sender in send order', options, async (t) => {
      const env = await open(t);
      const address = env.direct.inbox();
      const seen = [];
      await listen(t, env.direct, address, (message) => seen.push(Number(toText(message.body))));
      const from = sender(env);
      for (let i = 0; i < 500; i++) void from.send(address, String(i));
      await waitFor(() => seen.length === 500, { timeout: timeout + 2000, message: `received ${seen.length} of 500` });
      assert.ok(
        seen.every((value, i) => value === i),
        'out of order',
      );
    });

    it('should make listeners in one group compete, while ungrouped listeners each receive', options, async (t) => {
      const env = await open(t);
      const address = unique('svc');
      const grouped = [[], []];
      const plain = [];
      await listen(t, env.direct, address, (m) => grouped[0].push(toText(m.body)), { group: address });
      await listen(t, env.peer ?? env.direct, address, (m) => grouped[1].push(toText(m.body)), { group: address });
      await listen(t, env.direct, address, (m) => plain.push(toText(m.body)));
      for (let i = 0; i < 20; i++) await env.direct.send(address, `r${i}`);
      await waitFor(() => grouped[0].length + grouped[1].length === 20 && plain.length === 20, { timeout });
      await timers.setTimeout(50);
      const all = [...grouped[0], ...grouped[1]].sort();
      assert.strictEqual(new Set(all).size, 20, 'a grouped message was delivered twice');
      assert.strictEqual(all.length, 20);
    });

    it('should carry request/reply over replyTo and correlationId', options, async (t) => {
      const env = await open(t);
      const service = unique('svc-echo');
      await listen(
        t,
        env.peer ?? env.direct,
        service,
        (message) =>
          (env.peer ?? env.direct).send(message.replyTo, `echo:${toText(message.body)}`, {
            correlationId: message.correlationId,
          }),
        { group: service },
      );
      const inbox = env.direct.inbox();
      const replies = [];
      await listen(t, env.direct, inbox, (message) => replies.push(message));
      await env.direct.send(service, 'hi', { correlationId: 'req-7', replyTo: inbox });
      await waitFor(() => replies.length === 1, { timeout });
      assert.strictEqual(toText(replies[0].body), 'echo:hi');
      assert.strictEqual(replies[0].correlationId, 'req-7');
    });

    it('should deliver nothing more to a stopped listener', options, async (t) => {
      const env = await open(t);
      const address = env.direct.inbox();
      const seen = [];
      const stop = await env.direct.listen(address, (message) => seen.push(toText(message.body)));
      if (settle > 0) await timers.setTimeout(settle);
      await sender(env).send(address, 'before');
      await waitFor(() => seen.length === 1, { timeout });
      await stop();
      await stop();
      await sender(env)
        .send(address, 'after')
        .catch((error) => assert.strictEqual(error.code, 503));
      await timers.setTimeout(Math.max(50, settle));
      assert.deepStrictEqual(seen, ['before']);
    });

    it('should resolve a send to nobody, or reject it with 503', options, async (t) => {
      const env = await open(t);
      const result = await env.direct.send(unique('nobody'), 'x').then(
        () => 'sent',
        (error) => error,
      );
      if (result !== 'sent') assert.strictEqual(result.code, 503);
    });
  });

module.exports = { runDirectContract };
