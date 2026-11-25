'use strict';

const timers = require('node:timers/promises');
const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

const {
  Server,
  WrpcClient,
  defineRouter,
  procedure,
  createEventStream,
  createEventLog,
  tracked,
} = require('../../index.js');
const { MemoryBackplane } = require('../../src/scaling/index.js');
const { waitFor } = require('../helpers/wait.js');

// Two instances, one backplane, one logical feed. wrpc supplies the pieces —
// the log for resume, the stream for push->pull — and the application wires
// them to whatever carries messages between processes, exactly as it does
// with rooms.
const createInstance = async (backplane, instanceId) => {
  const log = createEventLog({ size: 50, epoch: 'e' });
  const live = new Set();

  const publish = (text) => {
    const message = JSON.stringify({ from: instanceId, text });
    backplane.publish('feed', message);
    deliver(message);
  };

  const deliver = (message) => {
    const { text } = JSON.parse(message);
    const id = log.push(text);
    for (const stream of live) stream.push(tracked(id, text));
  };

  // Everything published by ANOTHER instance arrives here.
  backplane.subscribe('feed', (message) => {
    if (JSON.parse(message).from === instanceId) return; // echo suppression
    deliver(message);
  });

  const router = defineRouter({
    feed: {
      messages: procedure.subscription({
        access: 'public',
        handler: async function* (_context, _args, { lastEventId, signal }) {
          for (const missed of log.since(lastEventId) ?? []) yield missed;
          const stream = createEventStream({ signal });
          live.add(stream);
          try {
            yield* stream;
          } finally {
            live.delete(stream);
          }
        },
      }),
      say: procedure({
        access: 'public',
        handler: async (_context, { text }) => {
          publish(text);
          return { ok: true };
        },
      }),
    },
  });

  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 50 },
    backplane,
    instanceId,
  });
  await server.listen();
  return { server, port: server.address().port, live, log };
};

describe('subscriptions', () => {
  describe('a feed that spans instances through the backplane', () => {
    const backplane = new MemoryBackplane();
    const clients = [];
    const seenHere = [];
    const seenThere = [];
    let first = null;
    let second = null;
    let here = null;
    let there = null;
    let remote = null;

    const connect = async (port) => {
      const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false });
      clients.push(client);
      await client.load('feed');
      return client;
    };

    before(async () => {
      first = await createInstance(backplane, 'node-1');
      second = await createInstance(backplane, 'node-2');
      here = await connect(first.port);
      there = await connect(second.port);
      here.api.feed.messages.subscribe({}, { onData: (data) => seenHere.push(data) });
      remote = there.api.feed.messages.subscribe({}, { onData: (data) => seenThere.push(data) });
      await waitFor(() => first.live.size === 1 && second.live.size === 1, 'the subscriptions never opened');
    });

    after(async () => {
      for (const client of clients) void client.close();
      await first?.server.close();
      await second?.server.close();
      backplane.close();
    });

    it('should deliver a value published on one instance to the other', async () => {
      await here.api.feed.say({ text: 'hello' });
      await waitFor(() => seenThere.length === 1, 'the value never crossed instances');
      assert.deepStrictEqual(seenThere, ['hello']);
    });

    it('should deliver it to the publishing instance exactly once', async () => {
      await waitFor(() => seenHere.length === 1, 'the local subscriber missed it');
      await timers.setTimeout(40);
      assert.deepStrictEqual(seenHere, ['hello'], 'echo suppression kept it from arriving twice');
    });

    it('should let the remote subscriber resume from its own eventId', async () => {
      assert.strictEqual(remote.lastEventId, 'e.0');
      remote.unsubscribe();
      await waitFor(() => second.live.size === 0, 'the unsubscribe never landed');

      // Two more while that subscriber is away.
      await here.api.feed.say({ text: 'second' });
      await here.api.feed.say({ text: 'third' });
      await waitFor(() => second.log.length === 3, 'the instance did not keep receiving');

      const resumed = [];
      there.api.feed.messages.subscribe({}, { lastEventId: 'e.0', onData: (data) => resumed.push(data) });
      await waitFor(() => resumed.length === 2, `only ${resumed.length} replayed`);
      assert.deepStrictEqual(resumed, ['second', 'third'], 'exactly what it missed');
    });
  });
});
