/**
 * `@alexify/wrpc/broker/kafka` — the Kafka broker: durable feeds and work
 * queues over an INJECTED KafkaJS-shaped client (`kafkajs` or
 * `@confluentinc/kafka-javascript`'s `.KafkaJS`), plus a caveated backplane.
 * There is deliberately no `direct` capability — see the guide.
 *
 * @experimental Part of the `@alexify/wrpc/broker*` family.
 */

import type { Broker } from '../broker.js';
import type { WrpcLogger } from '../index.js';

/** The client surface the adapter uses; both KafkaJS-shaped clients satisfy it. */
export interface KafkaClient {
  producer(config?: unknown): any;
  consumer(config?: unknown): any;
  admin(config?: unknown): any;
  /** Present on kafkajs' client only — how the flavor is detected. */
  logger?: () => unknown;
}

export interface KafkaBrokerOptions {
  /**
   * Mints every id this adapter puts on the wire — the backplane and reader consumer group ids. Used
   * VERBATIM — wrpc never truncates it, so a generator answering characters
   * the broker refuses in a group id fails at the driver, not here.
   * Strict: a non-function, or a function that does not answer a non-empty
   * string of at most 255 characters, is a TypeError at construction.
   */
  generateId?: () => string;
  kafka: KafkaClient;
  /** Detected from the client; force it when the detection is wrong. */
  flavor?: 'kafkajs' | 'confluent';
  /** Topic namespace; default 'wrpc'. */
  prefix?: string;
  logger?: WrpcLogger | boolean;
  /** Partitions for queue topics (the concurrency ceiling). Default 3. */
  partitions?: number;
  /** Partitions for log topics; 1 (the default) keeps a feed globally ordered. */
  logPartitions?: number;
  /**
   * Replication factor of the topics the adapter creates. Default `-1`: the
   * broker's own `default.replication.factor` (KIP-464, Kafka 2.4+). An
   * existing topic keeps the factor it was created with.
   */
  replicationFactor?: number;
  backplane?: { topic?: string; partitions?: number };
  /** Cap on how long a retry's delay may wait in-process. Default 60 000. */
  maxRetryDelay?: number;
  /**
   * Catch-up pages of the log read at once (default 4). Each page is a
   * consumer group of its own — a connection and a JoinGroup — so a resume
   * storm queues here instead of opening hundreds together; identical
   * pages (same topic, cursor and limit) are one read, shared. A positive
   * integer.
   */
  maxCatchUp?: number;
}

/** The returned broker has `backplane`, `log` and `queue` — never `direct`. */
export declare function createKafkaBroker(options: KafkaBrokerOptions): Broker;

/** The resume token: `k1:<partition>=<offset>,…`, partitions sorted. */
export declare function encodeVector(cursor: Record<number | string, number>): string;
export declare function decodeVector(text: unknown): Record<number, number> | null;
