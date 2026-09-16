'use strict';

// Cancellation over the sse client transport: ClientSseTransport does not
// override `persistent` (src/sse/client.js), so it keeps the base class's
// persistent = true — a {type:'cancel'} packet rides a POST like any other
// call, and its answer comes back down the event stream. This pins that
// the round trip tests/rpc/cancel.test.js checks for ws holds here too.

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { Server, WrpcClient, defineRouter, procedure } = require('../../index.js');
require('../../sse.js'); // registers the sse transport

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 300; i++) {
    if (predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

test('cancellation over SSE: an AbortSignal takes a call back through the POST leg', async (t) => {
  const state = { started: 0, aborted: 0, finished: 0, lastSignal: null };
  const router = defineRouter({
    slow: {
      wait: procedure({
        access: 'public',
        handler: async (context, { ms = 5000 } = {}) => {
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
    },
  });
  const server = new Server({ router, host: '127.0.0.1', port: 0, protocol: 'http', logger: false });
  await server.listen();
  t.after(() => server.close());
  const client = await WrpcClient.connect(`http://127.0.0.1:${server.address().port}/api`, {
    transport: 'sse',
    heartbeat: false,
  });
  t.after(() => void client.close());
  await client.load('slow');

  const controller = new AbortController();
  const pending = client.api.slow.wait({}, { signal: controller.signal });
  await waitFor(() => state.started === 1, 'the handler never started');
  controller.abort();
  const error = await pending.then(
    () => null,
    (failure) => failure,
  );
  assert.strictEqual(error.code, 499);
  assert.match(error.message, /Cancelled/);
  await waitFor(() => state.aborted === 1, 'ctx.signal never reached the handler');
  assert.strictEqual(state.lastSignal.aborted, true);
  assert.strictEqual(state.finished, 0, 'the handler stopped instead of running to completion');
  const [serverClient] = server.rpc.clients;
  await waitFor(() => serverClient.calls.size === 0, 'the cancelled call was never released');
});
