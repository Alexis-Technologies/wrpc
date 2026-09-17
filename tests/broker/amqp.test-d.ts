import { expectAssignable, expectError, expectType } from 'tsd';
import * as amqp from '../../broker/amqp.js';
import type { Broker } from '../../broker.js';
import type { AmqpBrokerOptions, AmqpConnection } from '../../broker/amqp.js';

declare const connection: AmqpConnection;
const broker = amqp.createAmqpBroker({ connection, prefix: 'app', queueType: 'classic', streamMaxBytes: 1_000_000 });
expectAssignable<Broker>(broker);
expectType<string>(broker.name);
expectAssignable<AmqpBrokerOptions>({ connection, inboxTtl: 30_000, logger: false });
expectError(amqp.createAmqpBroker({}));
expectError(amqp.createAmqpBroker({ connection, queueType: 'stream' }));
