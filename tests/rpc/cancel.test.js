'use strict';

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { Server, WrpcClient, defineRouter, procedure, createEventStream } = require('../../index.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

const state = { started: 0, aborted: 0, finished: 0, lastSignal: null };

const router = defineRouter({
  slow: {
    wait: procedure({
      access: 'public',
      handler: async (context, { ms = 500 } = {}) => {
        state.started++;
        state.lastSignal = context.signal;
        try {
          await timers.setTimeout(ms, undefined, { signal: context.signal });
        } catch {
          state.aborted++;
          throw new Error('aborted');
        }
        state.finished++;
        return { done: true };
      },
    }),
    // A handler that ignores its signal still has to have its late result
    // dropped rather than delivered to a caller that already gave up.
    stubborn: procedure({
      access: 'public',
      handler: async () => {
        await timers.setTimeout(60);
        state.finished++;
        return { late: true };
      },
    }),
    quick: procedure({ access: 'public', handler: async () => 'fast' }),
  },
});

const createServer = async (options = {}) => {
  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    console: quiet,
    timeouts: { bind: 50 },
    ...options,
  });
  await server.listen();
  return { server, port: server.address().port };
};

test('cancellation: an AbortSignal takes a call back', async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
  t.after(() => void client.close());
  await client.load('slow');

  await t.test('the caller is rejected with 499', async () => {
    const controller = new AbortController();
    const pending = client.api.slow.wait({ ms: 5000 }, { signal: controller.signal });
    await waitFor(() => state.started === 1, 'the handler never started');
    controller.abort();
    const error = await pending.then(
      () => null,
      (failure) => failure,
    );
    assert.strictEqual(error.code, 499);
    assert.match(error.message, /Cancelled/);
  });

  await t.test("the handler's ctx.signal is aborted too", async () => {
    await waitFor(() => state.aborted === 1, 'ctx.signal never reached the handler');
    assert.strictEqual(state.lastSignal.aborted, true);
    assert.strictEqual(state.finished, 0, 'the handler stopped instead of running to completion');
  });

  await t.test('the server forgets the call', async () => {
    const [serverClient] = server.rpc.clients;
    await waitFor(() => serverClient.calls.size === 0, 'the cancelled call was never released');
  });

  await t.test('an already-aborted signal never reaches the wire', async () => {
    const before = state.started;
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(client.api.slow.quick({}, { signal: controller.signal }), (error) => error.code === 499);
    await timers.setTimeout(30);
    assert.strictEqual(state.started, before);
  });

  await t.test('a handler that ignores the signal has its late result dropped', async () => {
    const controller = new AbortController();
    const before = state.finished;
    const pending = client.api.slow.stubborn({}, { signal: controller.signal });
    await timers.setTimeout(10);
    controller.abort();
    await assert.rejects(pending, (error) => error.code === 499);
    await waitFor(() => state.finished === before + 1, 'the handler never finished');
    // It finished, and nothing was delivered: the client would have thrown
    // "Callback not found" on a late answer, which is what this rules out.
    const failure = new Promise((resolve) => client.once('error', resolve));
    const raced = await Promise.race([failure, timers.setTimeout(60).then(() => 'quiet')]);
    assert.strictEqual(raced, 'quiet');
  });

  await t.test('cancelling twice, or an unknown id, is a no-op', async () => {
    client.send({ type: 'cancel', id: 'never-existed' });
    await timers.setTimeout(20);
    assert.strictEqual(await client.api.slow.quick(), 'fast', 'the connection is unharmed');
  });
});

test('cancellation: a batched call cancelled before it ships never ships', async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
    heartbeat: false,
    batch: { flush: 50 },
  });
  t.after(() => void client.close());
  await client.load('slow');

  const before = state.started;
  const controller = new AbortController();
  const pending = client.api.slow.wait({ ms: 5000 }, { signal: controller.signal });
  controller.abort(); // still sitting in the batch queue
  await assert.rejects(pending, (error) => error.code === 499);
  await timers.setTimeout(80);
  assert.strictEqual(state.started, before, 'the call was dropped instead of racing a cancel ahead of it');
});

test('cancellation: a disconnect aborts what is still running', async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
  await client.load('slow');

  const before = state.aborted;
  client.api.slow.wait({ ms: 5000 }).catch(noop);
  await waitFor(() => state.lastSignal && !state.lastSignal.aborted, 'the handler never started');
  client.close();
  await waitFor(() => state.aborted === before + 1, 'a dropped connection left the handler running');
});

test('cancellation requires a persistent transport', async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());
  const res = await fetch(`http://127.0.0.1:${port}/api`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'cancel', id: 'x' }),
  });
  const body = await res.json();
  assert.strictEqual(res.status, 400);
  assert.match(body.error.message, /persistent connection/);
});

test('subscriptions: ctx.signal reaches a subscription handler too', async (t) => {
  let live = 0;
  const subs = defineRouter({
    feed: {
      forever: procedure.subscription({
        access: 'public',
        handler: async function* (context, _args, options) {
          assert.strictEqual(options.signal, context.signal, 'both spellings are the same signal');
          live++;
          try {
            yield* createEventStream({ signal: options.signal });
          } finally {
            live--;
          }
        },
      }),
    },
  });
  const server = new Server({
    router: subs,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    console: quiet,
    timeouts: { bind: 50 },
  });
  await server.listen();
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${server.address().port}/api`, { heartbeat: false });
  t.after(() => void client.close());
  await client.load('feed');

  const handle = client.api.feed.forever.subscribe();
  await waitFor(() => live === 1, 'the subscription never started');
  handle.unsubscribe();
  await waitFor(() => live === 0, 'the signal never reached the generator');
});
