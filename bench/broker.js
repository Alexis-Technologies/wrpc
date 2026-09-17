'use strict';

// What the broker bindings cost on top of a broker, measured against the
// in-process MemoryBroker so the numbers are wrpc's own overhead — the
// dispatch, the settlement, the feed pump — and not a network's.
//
//   - a queue delivery into a procedure, end to end: decode, the call
//     pipeline (hooks, validators, access), the settlement;
//   - the same with a validator and a hook, so the pipeline's share shows;
//   - a publish through createPublisher (validate, encode, append);
//   - a durable feed's pump: append -> tail -> tracked value on the wire.
//
// Run with `pnpm bench` (bench/run-all.js) or `node bench/broker.js`.

const { RpcServer } = require('../src/rpc/core.js');
const { defineRouter, procedure } = require('../src/rpc/router.js');
const { MemoryBroker, attachConsumers, createPublisher, brokerFeed } = require('../src/broker/index.js');
const { bench } = require('./support/harness.js');

const quiet = { log() {}, info() {}, warn() {}, error() {}, debug() {} };

const settled = () => {
  let resolve;
  const promise = new Promise((_resolve) => {
    resolve = _resolve;
  });
  return { promise, resolve };
};

// One delivery at a time: the number that matters is per-message latency
// through the whole binding, not how deep a queue can be filled.
const consumerBench = async (label, build, options = {}) => {
  const broker = new MemoryBroker({ logger: quiet });
  let pending = null;
  const rpc = new RpcServer({ router: build(() => pending?.resolve()), logger: quiet, sse: false });
  const consumers = await attachConsumers(rpc, broker, {}, { logger: quiet });
  const result = await bench(label, async () => {
    pending = settled();
    await broker.queue.produce('work', '{"n":1}', options.headers ? { headers: options.headers } : undefined);
    await pending.promise;
  });
  await consumers.stop();
  await rpc.close();
  broker.close();
  return result;
};

const run = async () => {
  console.log('\nbroker bindings (MemoryBroker — wrpc overhead only)\n');

  await consumerBench('queue delivery -> procedure (public, no validators)', (done) =>
    defineRouter({
      bench: {
        consumes: {
          work: procedure({ access: 'public', handler: async () => void done() }),
        },
      },
    }),
  );

  await consumerBench(
    'queue delivery -> procedure (validator, hook, meta header)',
    (done) =>
      defineRouter({
        bench: {
          hooks: { preHandler: () => {} },
          consumes: {
            work: procedure({
              access: 'public',
              input: (args) => {
                if (typeof args.n !== 'number') throw new Error('n must be a number');
              },
              consume: { meta: ['x-tenant'] },
              handler: async () => void done(),
            }),
          },
        },
      }),
    { headers: { 'x-tenant': 't1' } },
  );

  // Publishing: validate, encode, append.
  const broker = new MemoryBroker({ logger: quiet });
  const router = defineRouter({
    'orders.v1': {
      emits: { created: { data: { id: 'string' } } },
      place: procedure({ access: 'public', handler: async () => ({}) }),
    },
  });
  const rpc = new RpcServer({ router, logger: quiet, sse: false });
  const publisher = createPublisher(rpc, broker, {
    'orders.v1/created': { validate: (order) => order },
  });
  await bench('createPublisher.publish -> log append', () => publisher.publish('orders.v1/created', { id: 'o-1' }));

  // A live feed: one append, one tracked value out of the pump.
  const feed = brokerFeed(broker, 'orders.v1.created');
  const controller = new AbortController();
  const iterator = feed({}, {}, { signal: controller.signal });
  let next = iterator.next();
  await bench('brokerFeed: append -> tracked value', async () => {
    await broker.log.append('orders.v1.created', '{"id":"o"}');
    await next;
    next = iterator.next();
  });
  controller.abort();
  await iterator.return?.();
  await rpc.close();
  broker.close();
};

if (require.main === module) {
  run().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

module.exports = { run };
