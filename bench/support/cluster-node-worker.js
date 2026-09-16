'use strict';

// One wrpc instance of the multi-node bench (bench/cluster-nodes.js): a
// Server over a Redis backplane and a Redis session store, told its
// instanceId and the Redis URL by the orchestrator, reporting its port and
// its gap count back over IPC. Not discovered by bench/run-all.js (it lives
// under support/).

const { Server } = require('../../src/server.js');
const { defineRouter, procedure } = require('../../src/rpc/router.js');
const { createRedisAdapter, createRedisSessionStore } = require('../../src/scaling/index.js');
const { bearerTransport } = require('../../src/auth/index.js');

const { REDIS_URL, WRPC_INSTANCE = 'node' } = process.env;
const Redis = require('ioredis');

let gaps = 0;
// A logger that counts the loss-detection events and stays quiet otherwise.
const logger = {
  log() {},
  info() {},
  debug() {},
  error(entry, message) {
    console.error(`[${WRPC_INSTANCE}]`, entry?.err?.stack ?? entry, message ?? '');
  },
  warn(entry) {
    if (entry && entry.event === 'backplane.gap') gaps += entry.missed ?? 1;
  },
  child() {
    return this;
  },
};

const main = async () => {
  const pub = new Redis(REDIS_URL, { lazyConnect: false, maxRetriesPerRequest: null });
  const store = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
  const router = defineRouter({
    bench: {
      echo: procedure({ access: 'public', handler: async (_ctx, args) => args }),
      join: procedure({
        access: 'public',
        handler: async (ctx, { room }) => {
          ctx.client.join(room);
          return ctx.server.instanceId;
        },
      }),
      leave: procedure({
        access: 'public',
        handler: async (ctx, { room }) => {
          ctx.client.leave(room);
          return true;
        },
      }),
      // A cross-instance emit from THIS instance: every member everywhere.
      // (`emit`/`ask` would collide with the client-side unit Emitter's own methods.)
      fanout: procedure({
        access: 'public',
        handler: async (ctx, { room, data }) => ctx.server.to(room).emit('bench/tick', data),
      }),
      inquire: procedure({
        access: 'public',
        handler: async (ctx, { room }) => {
          const { answers, expected } = await ctx.server.to(room).ask('bench/q', null, { timeout: 5000 });
          return { answers: answers.length, expected };
        },
      }),
      count: procedure({ access: 'public', handler: async (ctx, { room }) => ctx.server.cluster.count(room) }),
      login: procedure({
        access: 'public',
        handler: async (ctx, { user }) => {
          ctx.client.startSession(undefined, { user });
          return { access: ctx.client.session.token };
        },
      }),
      whoami: procedure({ access: 'session', handler: async (ctx) => ctx.session.state.user }),
      gaps: procedure({ access: 'public', handler: async () => gaps }),
    },
  });
  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger,
    instanceId: WRPC_INSTANCE,
    backplane: createRedisAdapter({ pub, logger: false }),
    sessions: {
      store: createRedisSessionStore({ client: store, prefix: 'wrpc:bench:session:' }),
      transport: bearerTransport(),
    },
    cluster: { presenceInterval: 1000 },
  });
  await server.listen();
  // The backplane and cluster channels subscribe asynchronously after
  // listen(); a client that joins before they land publishes deltas nobody
  // hears (at-most-once), and the digest corrects that a presence interval
  // later. A short pause keeps the first scenario from measuring that.
  await new Promise((resolve) => setTimeout(resolve, 250));
  process.send({ type: 'ready', port: server.address().port, instance: WRPC_INSTANCE });
  process.on('message', async (message) => {
    if (message?.type === 'stop') {
      await server.close();
      pub.disconnect();
      store.disconnect();
      process.exit(0);
    }
  });
};

main().catch((error) => {
  process.send?.({ type: 'error', message: error.message });
  process.exit(1);
});
