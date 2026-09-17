'use strict';

// The direct contract (src/broker/port.js), executable.
//
// harness: {
//   open(): Promise<{ direct, peer?, close() }>
//   timeout?: number
//   settle?: number     ms after listen() before a real broker routes to it
// }

const assert = require('node:assert');
const timers = require('node:timers/promises');

const { isBrokerDirect } = require('../../src/broker/port.js');
const { toBytes, toText } = require('../../src/broker/ids.js');
const { unique, waitFor } = require('./support.js');

const runDirectContract = async (t, name, harness) => {
  const timeout = harness.timeout ?? 2000;
  const settle = harness.settle ?? 0;

  const open = async (sub) => {
    const env = await harness.open();
    sub.after(() => env.close());
    return env;
  };
  const listen = async (sub, direct, address, handler, options) => {
    const stop = await direct.listen(address, handler, options);
    sub.after(() => stop());
    if (settle > 0) await timers.setTimeout(settle);
    return stop;
  };
  const sender = (env) => env.peer ?? env.direct;

  await t.test(`${name}: satisfies the structural contract; inboxes are unique`, async (sub) => {
    const { direct } = await open(sub);
    assert.strictEqual(isBrokerDirect(direct), true);
    const a = direct.inbox();
    const b = direct.inbox();
    assert.strictEqual(typeof a, 'string');
    assert.notStrictEqual(a, b);
  });

  await t.test(`${name}: a message reaches the listener with its envelope`, async (sub) => {
    const env = await open(sub);
    const address = env.direct.inbox();
    const seen = [];
    await listen(sub, env.direct, address, (message) => seen.push(message));
    const replyTo = env.direct.inbox();
    await sender(env).send(address, 'ping', { headers: { tp: '00-abc' }, correlationId: 'c-1', replyTo });
    await waitFor(() => seen.length === 1, { timeout });
    assert.strictEqual(toText(seen[0].body), 'ping');
    assert.strictEqual(seen[0].headers.tp, '00-abc');
    assert.strictEqual(seen[0].correlationId, 'c-1');
    assert.strictEqual(seen[0].replyTo, replyTo);
  });

  await t.test(`${name}: binary bodies survive byte for byte`, async (sub) => {
    const env = await open(sub);
    const address = env.direct.inbox();
    const seen = [];
    await listen(sub, env.direct, address, (message) => seen.push(message));
    const bytes = new Uint8Array(4096).map((_, i) => (i * 31) % 256);
    await sender(env).send(address, bytes);
    await waitFor(() => seen.length === 1, { timeout });
    assert.deepStrictEqual(toBytes(seen[0].body), bytes);
  });

  await t.test(`${name}: messages from one sender arrive in send order`, async (sub) => {
    const env = await open(sub);
    const address = env.direct.inbox();
    const seen = [];
    await listen(sub, env.direct, address, (message) => seen.push(Number(toText(message.body))));
    const from = sender(env);
    for (let i = 0; i < 500; i++) void from.send(address, String(i));
    await waitFor(() => seen.length === 500, { timeout: timeout + 2000, message: `received ${seen.length} of 500` });
    assert.ok(
      seen.every((value, i) => value === i),
      'out of order',
    );
  });

  await t.test(`${name}: listeners in one group compete; ungrouped listeners each receive`, async (sub) => {
    const env = await open(sub);
    const address = unique('svc');
    const grouped = [[], []];
    const plain = [];
    await listen(sub, env.direct, address, (m) => grouped[0].push(toText(m.body)), { group: address });
    await listen(sub, env.peer ?? env.direct, address, (m) => grouped[1].push(toText(m.body)), { group: address });
    await listen(sub, env.direct, address, (m) => plain.push(toText(m.body)));
    for (let i = 0; i < 20; i++) await env.direct.send(address, `r${i}`);
    await waitFor(() => grouped[0].length + grouped[1].length === 20 && plain.length === 20, { timeout });
    await timers.setTimeout(50);
    const all = [...grouped[0], ...grouped[1]].sort();
    assert.strictEqual(new Set(all).size, 20, 'a grouped message was delivered twice');
    assert.strictEqual(all.length, 20);
  });

  await t.test(`${name}: request/reply over replyTo and correlationId`, async (sub) => {
    const env = await open(sub);
    const service = unique('svc-echo');
    await listen(
      sub,
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
    await listen(sub, env.direct, inbox, (message) => replies.push(message));
    await env.direct.send(service, 'hi', { correlationId: 'req-7', replyTo: inbox });
    await waitFor(() => replies.length === 1, { timeout });
    assert.strictEqual(toText(replies[0].body), 'echo:hi');
    assert.strictEqual(replies[0].correlationId, 'req-7');
  });

  await t.test(`${name}: a stopped listener hears nothing more`, async (sub) => {
    const env = await open(sub);
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

  await t.test(`${name}: sending to nobody resolves, or rejects with 503`, async (sub) => {
    const env = await open(sub);
    const result = await env.direct.send(unique('nobody'), 'x').then(
      () => 'sent',
      (error) => error,
    );
    if (result !== 'sent') assert.strictEqual(result.code, 503);
  });
};

module.exports = { runDirectContract };
