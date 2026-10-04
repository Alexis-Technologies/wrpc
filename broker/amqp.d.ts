/**
 * `@alexify/wrpc/broker/amqp` — the RabbitMQ broker (AMQP 0-9-1): all four
 * capabilities over an INJECTED amqplib connection (`amqplib` is a
 * devDependency here, never a runtime one).
 *
 * @experimental Part of the `@alexify/wrpc/broker*` family.
 */

import type { Broker } from '../broker.js';
import type { WrpcLogger } from '../index.js';

/** The connection surface the adapter uses; an amqplib connection satisfies it. */
export interface AmqpConnection {
  createChannel(): Promise<any>;
  createConfirmChannel(): Promise<any>;
}

export interface AmqpBrokerOptions {
  /**
   * Mints every id this adapter puts on the wire — message ids and direct inbox queue names. Used
   * VERBATIM — wrpc never truncates it, so a generator answering characters
   * the broker refuses in a queue name fails at the driver, not here.
   * Strict: a non-function, or a function that does not answer a non-empty
   * string of at most 255 characters, is a TypeError at construction.
   */
  generateId?: () => string;
  connection: AmqpConnection;
  /** Exchange, queue and routing-key namespace; default 'wrpc'. */
  prefix?: string;
  logger?: WrpcLogger | boolean;
  /** Work queues' `x-queue-type`; default 'quorum'. */
  queueType?: 'quorum' | 'classic';
  /** `x-expires` of a service group's shared queue, in ms. Default 60 000. */
  inboxTtl?: number;
  /** `x-max-length-bytes` on a log's stream queue; 0 (default) never trims. */
  streamMaxBytes?: number;
}

export declare function createAmqpBroker(options: AmqpBrokerOptions): Broker;
