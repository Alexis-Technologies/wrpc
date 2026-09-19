import { expectAssignable, expectError, expectType } from 'tsd';
import * as broker from '../broker.js';
import type {
  Broker,
  BrokerDirect,
  BrokerRpcOptions,
  ClientBrokerTransport,
  BrokerLog,
  BrokerQueue,
  Delivery,
  DirectMessage,
  LogEntry,
  QueueConsumer,
} from '../broker.js';
import type { Backplane } from '../scaling.js';
import type { AttachOptions, RpcServer, Server } from '../index.js';
import { procedure, defineRouter, connect } from '../index.js';

const memory = new broker.MemoryBroker({ logger: false, retention: { maxEntries: 100 } });
expectAssignable<Broker>(memory);
expectType<'memory'>(memory.name);
expectType<Backplane>(memory.backplane);
expectAssignable<Broker>(broker.createMemoryBroker());

// log
declare const log: BrokerLog;
expectType<Promise<string>>(log.append('orders', '{}', { headers: { tp: 'x' } }));
const read = log.read('orders', { after: '1-0', signal: new AbortController().signal });
expectType<Promise<void>>(read.ready);
(async () => {
  for await (const entry of read) expectType<LogEntry>(entry);
})();
expectType<string | null>(log.parseId('1-0'));
expectError(log.read('orders', { from: 'middle' }));

// queue
declare const queue: BrokerQueue;
expectType<Promise<void>>(queue.produce('jobs', 'body'));
const consumer = queue.consume(
  'jobs',
  async (delivery) => {
    expectType<Delivery>(delivery);
    expectType<number>(delivery.attempt);
    await delivery.retry({ delay: 100 });
  },
  { prefetch: 8, deadLetter: 'jobs.dlq' },
);
expectType<Promise<QueueConsumer>>(consumer);

// direct
declare const direct: BrokerDirect;
expectType<string>(direct.inbox());
direct.listen('svc', (message) => expectType<DirectMessage>(message), { group: 'svc' });
expectType<Promise<void>>(direct.send('svc', new Uint8Array(1), { correlationId: 'c', replyTo: direct.inbox() }));

// Structural: anything with the shape is a broker.
expectAssignable<Broker>({ name: 'custom', queue, close: () => {} });
expectType<boolean>(broker.isBroker(memory));
if (broker.isBrokerLog(memory.log)) expectType<BrokerLog>(memory.log);

// Building blocks
const tails = new broker.TopicTails<number, { seq: number; value: string; headers: Record<string, string> }>({
  live: async () => 0,
  range: async () => [],
  covered: (cursor, entry) => entry.seq <= cursor,
  advance: (_cursor, entry) => entry.seq,
});
expectType<number>(tails.size);
expectType<string>(broker.encodeToken('room:*', { maxLength: 100 }));
expectType<number>(broker.DEFAULT_RETRY.attempts);

// The core seam: attach with an identity.
declare const rpc: RpcServer;
declare const transport: Parameters<RpcServer['attach']>[0];
rpc.attach(transport, { session: { token: 'svc', state: { service: 'billing' } } });
rpc.attach(transport, { request: { headers: { authorization: 'Bearer t' } } });
expectAssignable<AttachOptions>({ meta: null });
expectError(rpc.attach(transport, { session: { token: 'x' }, request: { headers: {} } }));

// Durable feeds plug straight into a subscription procedure.
procedure.subscription({
  access: 'session',
  handler: broker.brokerFeed(memory, (_ctx, args: { tenant: string }) => `orders.${args.tenant}`, {
    map: (order: { id: string }, entry) => ({ ...order, tp: entry.headers.tp }),
    onGap: async (_ctx, _args, { code }) => [{ snapshot: code }],
    secret: 'feed-secret',
  }),
});
broker.brokerFeed(memory.log, 'orders', { from: 'earliest', decode: 'text' });
expectError(broker.brokerFeed(memory, 'orders', { from: 'middle' }));

// Consumers: declared in the router, bound (and overridden) at attach time.
defineRouter({
  'billing.v1': {
    consumes: {
      'orders.created': procedure({
        consume: { prefetch: 8, retry: { attempts: 3 }, deadLetter: false, identity: { trust: 'service' } },
        handler: async (ctx, args: { orderId: string }) => ({ charged: args.orderId, by: ctx.callMeta }),
      }),
    },
  },
});
declare const server: Server;
(async () => {
  const consumers = await broker.attachConsumers(
    server,
    memory,
    {
      'billing.v1/orders.created': { queue: 'prod.orders', prefetch: 32 },
      'audit.events': { target: 'audit.v1/record', args: (body) => ({ body }) },
    },
    { onDeadLetter: ({ code, delivery }) => void [code, delivery.attempt] },
  );
  expectType<boolean>(consumers.healthy);
  expectType<string>(consumers.bindings[0].queue);
  await consumers.stop();

  const publisher = broker.createPublisher(rpc, memory, {
    'orders.v1/created': { to: 'queue', key: (order: { id: string }) => order.id },
  });
  expectType<Promise<string | undefined>>(publisher.publish('orders.v1/created', { id: 'o-1' }));
})();
expectError(broker.attachConsumers(server, memory, { q: { prefetch: 'many' } }));

// RPC over a broker.
(async () => {
  const handle = await broker.attachBrokerRpc(server, memory, { service: 'billing', idleTimeout: 60_000 });
  expectType<string>(handle.address);
  expectType<number>(handle.sessions);
  const client = await connect('broker://billing', { transport: 'broker', broker: memory, mode: 'session' });
  void client;
  const transport = new broker.ClientBrokerTransport('broker://billing');
  expectType<'stateless' | 'session'>(transport.mode);
})();
expectError(connect('broker://billing', { transport: 'broker', broker: memory, mode: 'duplex' }));

// Per-message compression on the binding: the server option, the client's, and the getter
expectAssignable<BrokerRpcOptions>({ service: 'x', compression: true, maxMessage: 1 << 20 });
expectAssignable<BrokerRpcOptions>({ service: 'x', compression: { threshold: 512 } });
expectError<BrokerRpcOptions>({ service: 'x', compression: 'lz4' });
declare const brokerTransport: ClientBrokerTransport;
expectType<string | null>(brokerTransport.compression);
