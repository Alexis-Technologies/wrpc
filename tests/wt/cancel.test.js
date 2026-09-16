'use strict';

// Cancellation over the wt (WebTransport) client transport, end to end,
// over the in-memory fake session pair: ClientWtTransport declares
// persistent = true (src/client/webtransport.js:69), so core.js's cancel
// gate sends {type:'cancel'} on it exactly like it does on ws — this pins
// that the whole round trip (dispatcher's handleCancel aborting ctx.signal,
// the 499 the caller sees) survives the wt framing too.

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { defineRouter, procedure } = require('../../index.js');
const { acceptSessions } = require('../../wt.js');
const { createFakeWt } = require('./fakeWebTransport.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const routerFor = (state) =>
  defineRouter({
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

test('cancellation over WebTransport: an AbortSignal takes a call back', async (t) => {
  const state = { started: 0, aborted: 0, finished: 0, lastSignal: null };
  const { server, url } = await bootServer(t, { router: routerFor(state) });
  const world = createFakeWt();
  const acceptor = acceptSessions(server, world.sessions);
  t.after(() => acceptor.stop());
  const client = await connectClient(t, url, { transport: 'wt', wt: { WebTransport: world.WebTransport } });
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
