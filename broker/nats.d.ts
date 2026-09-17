/**
 * `@alexify/wrpc/broker/nats` — the NATS broker: core subjects for the
 * backplane and RPC, JetStream for durable feeds and queues. The connection
 * and the JetStream factories are INJECTED (`@nats-io/transport-node` and
 * `@nats-io/jetstream` are devDependencies here, never runtime ones).
 *
 * @experimental Part of the `@alexify/wrpc/broker*` family.
 */

import type { Broker } from '../broker.js';
import type { WrpcLogger } from '../index.js';

/** The connection surface the adapter uses; a nats.js connection satisfies it. */
export interface NatsConnection {
  publish(subject: string, data: string | Uint8Array, options?: { headers?: unknown; reply?: string }): void;
  subscribe(
    subject: string,
    options?: { queue?: string; callback?: (error: Error | null, message: any) => void },
  ): { unsubscribe(): void };
  flush(): Promise<void>;
  isClosed?(): boolean;
}

export interface NatsBrokerOptions {
  nc: NatsConnection;
  /** The `headers()` factory — a package export, not a method on the connection. */
  headers: () => any;
  /** `jetstream(nc)`; omit it (and the manager) for a backplane + direct broker. */
  jetstream?: (nc: NatsConnection) => any;
  /** `jetstreamManager(nc)`; comes with `jetstream`. */
  jetstreamManager?: (nc: NatsConnection) => Promise<any>;
  /** `createInbox()`; a uuid-based `_INBOX.` subject otherwise. */
  createInbox?: () => string;
  /** Subject and stream-name namespace; default 'wrpc'. */
  prefix?: string;
  logger?: WrpcLogger | boolean;
  /** JetStream ack_wait for queue consumers, in ms. Default 30 000. */
  ackWait?: number;
  /** Extra stream configuration, merged into what the adapter creates. */
  stream?: { log?: Record<string, unknown>; queue?: Record<string, unknown> };
}

/** Without `jetstream`/`jetstreamManager` the broker has no `log` and no `queue`. */
export declare function createNatsBroker(options: NatsBrokerOptions): Broker;
