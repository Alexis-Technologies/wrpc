import { expectAssignable, expectError, expectType } from 'tsd';
import * as broker from '../broker.js';
import type {
  Broker,
  BrokerDirect,
  BrokerLog,
  BrokerQueue,
  Delivery,
  DirectMessage,
  LogEntry,
  QueueConsumer,
} from '../broker.js';
import type { Backplane } from '../scaling.js';
import type { AttachOptions, RpcServer } from '../index.js';
import { procedure } from '../index.js';

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
