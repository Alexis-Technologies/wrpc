'use strict';

// Cancellation over a webrtc client transport, end to end: attachChannel
// (the attachPort of WebRTC) plugs a raw data channel straight into an
// RpcServer, so the same {type:'cancel'} contract tests/rpc/cancel.test.js
// pins for ws must hold here too — persistent = true on ClientRtcTransport
// (src/webrtc/transport.js:104) means core.js's cancel gate admits it.

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure } = require('../../src/rpc/router.js');
const { WrpcClient } = require('../../src/client/core.js');
const { attachChannel } = require('../../src/webrtc/index.js');
const { rawChannelPair } = require('./rawChannel.js');

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 300; i++) {
    if (predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

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

test('cancellation over webrtc: an AbortSignal takes a call back through a raw data channel', async (t) => {
  const state = { started: 0, aborted: 0, finished: 0, lastSignal: null };
  const rpc = new RpcServer({ router: routerFor(state), logger: false });
  t.after(() => rpc.close());
  const { a, b } = await rawChannelPair(t);
  const attached = attachChannel(rpc, b, { peer: 'client' });

  const client = await WrpcClient.connect('webrtc:host', {
    transport: 'webrtc',
    channel: a,
    heartbeat: false,
    reconnect: false,
  });
  t.after(() => client.close());
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
  await waitFor(() => attached.calls.size === 0, 'the cancelled call was never released');
});
