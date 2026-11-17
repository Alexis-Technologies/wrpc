import { expectAssignable, expectError, expectType } from 'tsd';
import * as amqp from '../../src/broker/amqp/index.js';
import type { Broker } from '../../src/broker/index.js';
import type { AmqpBrokerOptions, AmqpConnection } from '../../src/broker/amqp/index.js';

// The real connection type (amqplib ships its own), a devDependency used only here.
import type { ChannelModel } from 'amqplib';
declare const realConnection: ChannelModel;
expectAssignable<AmqpConnection>(realConnection);

declare const connection: AmqpConnection;
const broker = amqp.createAmqpBroker({ connection, prefix: 'app', queueType: 'classic', streamMaxBytes: 1_000_000 });
expectAssignable<Broker>(broker);
expectType<string>(broker.name);
expectAssignable<AmqpBrokerOptions>({ connection, inboxTtl: 30_000, logger: false });
expectError(amqp.createAmqpBroker({}));
expectError(amqp.createAmqpBroker({ connection, queueType: 'stream' }));
