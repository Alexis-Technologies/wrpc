'use strict';

const timers = require('node:timers/promises');
const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

const { Server, WrpcClient, defineRouter, procedure, createEventStream } = require('../../index.js');
const { waitFor } = require('../helpers/wait.js');

const noop = () => {};

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
    logger: false,
    timeouts: { bind: 50 },
    ...options,
  });
  await server.listen();
  return { server, port: server.address().port };
};

describe('cancellation', () => {
  describe('an AbortSignal takes a call back', () => {
    let server = null;
    let client = null;
    before(async () => {
      const booted = await createServer();
      server = booted.server;
      client = await WrpcClient.connect(`ws://127.0.0.1:${booted.port}/api`, { heartbeat: false });
      await client.load('slow');
    });
    after(async () => {
      await server?.close();
      void client?.close();
    });

    it('should reject the caller with 499', async () => {
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

    it("should abort the handler's ctx.signal too", async () => {
      await waitFor(() => state.aborted === 1, 'ctx.signal never reached the handler');
      assert.strictEqual(state.lastSignal.aborted, true);
      // Ended on purpose by the caller: a reason a handler can tell from a
      // connection that went away.
      assert.strictEqual(state.lastSignal.reason.code, 'ALX_ERR_ABORTED');
      assert.strictEqual(state.finished, 0, 'the handler stopped instead of running to completion');
    });

    it('should make the server forget the call', async () => {
      const [serverClient] = server.rpc.clients;
      await waitFor(() => serverClient.calls.size === 0, 'the cancelled call was never released');
    });

    it('should never put an already-aborted signal on the wire', async () => {
      const startedBefore = state.started;
      const controller = new AbortController();
      controller.abort();
      await assert.rejects(client.api.slow.quick({}, { signal: controller.signal }), (error) => error.code === 499);
      await timers.setTimeout(30);
      assert.strictEqual(state.started, startedBefore);
    });

    it('should drop the late result of a handler that ignores the signal', async () => {
      const controller = new AbortController();
      const finishedBefore = state.finished;
      const pending = client.api.slow.stubborn({}, { signal: controller.signal });
      await timers.setTimeout(10);
      controller.abort();
      await assert.rejects(pending, (error) => error.code === 499);
      await waitFor(() => state.finished === finishedBefore + 1, 'the handler never finished');
      // It finished, and nothing was delivered: the client would have thrown
      // "Callback not found" on a late answer, which is what this rules out.
      const failure = new Promise((resolve) => client.once('error', resolve));
      const raced = await Promise.race([failure, timers.setTimeout(60).then(() => 'quiet')]);
      assert.strictEqual(raced, 'quiet');
    });

    it('should treat cancelling twice, or an unknown id, as a no-op', async () => {
      client.send({ type: 'cancel', id: 'never-existed' });
      await timers.setTimeout(20);
      assert.strictEqual(await client.api.slow.quick(), 'fast', 'the connection is unharmed');
    });
  });

  it('should take a call back with an AbortSignal over the event transport, through attachPort', async (t) => {
    const { server } = await createServer();
    t.after(() => server.close());
    // The same bridge a page hands its worker: 'wrpc:connect' carries the
    // port this server attaches directly with attachPort — no socket at all.
    const worker = {
      postMessage(message, transfer) {
        if (message?.type === 'wrpc:connect') server.rpc.attachPort(transfer[0]);
      },
    };
    const client = await WrpcClient.connect('local:bridge', { transport: 'event', worker, heartbeat: false });
    t.after(() => void client.close());
    await client.load('slow');

    const beforeStarted = state.started;
    const beforeAborted = state.aborted;
    const beforeFinished = state.finished;
    const controller = new AbortController();
    const pending = client.api.slow.wait({ ms: 5000 }, { signal: controller.signal });
    await waitFor(() => state.started === beforeStarted + 1, 'the handler never started');
    controller.abort();
    const error = await pending.then(
      () => null,
      (failure) => failure,
    );
    assert.strictEqual(error.code, 499);
    assert.match(error.message, /Cancelled/);
    await waitFor(() => state.aborted === beforeAborted + 1, 'ctx.signal never reached the handler');
    assert.strictEqual(state.lastSignal.aborted, true);
    assert.strictEqual(state.finished, beforeFinished, 'the handler stopped instead of running to completion');
    const [serverClient] = server.rpc.clients;
    await waitFor(() => serverClient.calls.size === 0, 'the cancelled call was never released');
  });

  it('should never ship a batched call cancelled before it ships', async (t) => {
    const { server, port } = await createServer();
    t.after(() => server.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, {
      heartbeat: false,
      batch: { flush: 50 },
    });
    t.after(() => void client.close());
    await client.load('slow');

    const startedBefore = state.started;
    const controller = new AbortController();
    const pending = client.api.slow.wait({ ms: 5000 }, { signal: controller.signal });
    controller.abort(); // still sitting in the batch queue
    await assert.rejects(pending, (error) => error.code === 499);
    await timers.setTimeout(80);
    assert.strictEqual(state.started, startedBefore, 'the call was dropped instead of racing a cancel ahead of it');
  });

  it('should abort what is still running on a disconnect', async (t) => {
    const { server, port } = await createServer();
    t.after(() => server.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
    await client.load('slow');

    const abortedBefore = state.aborted;
    client.api.slow.wait({ ms: 5000 }).catch(noop);
    await waitFor(() => state.lastSignal && !state.lastSignal.aborted, 'the handler never started');
    client.close();
    await waitFor(() => state.aborted === abortedBefore + 1, 'a dropped connection left the handler running');
  });

  it('should require a persistent transport', async (t) => {
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
});

describe('subscriptions', () => {
  it('should let ctx.signal reach a subscription handler too', async (t) => {
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
      logger: false,
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
});

describe('maxCalls', () => {
  it('should cap the in-flight calls from one connection with 429', async (t) => {
    const { server, port } = await createServer({ maxCalls: 2 });
    t.after(() => server.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
    t.after(() => void client.close());
    await client.load('slow');

    // One connection processes its packets in order, so by the time `quick`
    // is dispatched both waits are registered in-flight — no waiting needed.
    const first = client.api.slow.wait({ ms: 300 });
    const second = client.api.slow.wait({ ms: 300 });
    await assert.rejects(client.api.slow.quick(), (error) => error.code === 429);
    // The cap is per in-flight set, not a ban: once a slot frees, calls run.
    assert.deepStrictEqual(await first, { done: true });
    assert.deepStrictEqual(await second, { done: true });
    assert.strictEqual(await client.api.slow.quick(), 'fast');
  });
});

describe('a deadline that passes', () => {
  // The 408 answered the caller while the handler ran on: its ctx.signal never
  // fired, and once the call left client.calls no disconnect could fire it.
  it('should abort the handler signal when the procedure timeout answers 408', async (t) => {
    let signal = null;
    const deadlined = defineRouter({
      slow: {
        hang: procedure({
          access: 'public',
          timeout: 30,
          handler: (context) => {
            signal = context.signal;
            return new Promise(noop);
          },
        }),
      },
    });
    const { server, port } = await createServer({ router: deadlined });
    t.after(() => server.close());
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
    t.after(() => void client.close());
    await client.load('slow');

    await assert.rejects(client.api.slow.hang(), (error) => error.code === 408);
    assert.strictEqual(signal.aborted, true);
    assert.strictEqual(signal.reason.code, 408);
  });
});
