import { expectAssignable, expectError, expectType } from 'tsd';
import * as kafka from '../../broker/kafka.js';
import type { Broker } from '../../broker.js';
import type { KafkaBrokerOptions, KafkaClient } from '../../broker/kafka.js';

// Both real clients, devDependencies used only here.
import type { Kafka } from 'kafkajs';
import type { KafkaJS } from '@confluentinc/kafka-javascript';
declare const realKafkajs: Kafka;
declare const realConfluent: KafkaJS.Kafka;
expectAssignable<KafkaClient>(realKafkajs);
expectAssignable<KafkaClient>(realConfluent);
kafka.createKafkaBroker({ kafka: realKafkajs });
kafka.createKafkaBroker({ kafka: realConfluent, flavor: 'confluent' });

declare const client: KafkaClient;
const broker = kafka.createKafkaBroker({ kafka: client, prefix: 'app', partitions: 6, logPartitions: 1 });
expectAssignable<Broker>(broker);
expectType<string>(kafka.encodeVector({ 0: 12, 1: 3 }));
expectType<Record<number, number> | null>(kafka.decodeVector('k1:0=12'));
expectAssignable<KafkaBrokerOptions>({ kafka: client, flavor: 'confluent', backplane: { partitions: 1 } });
expectError(kafka.createKafkaBroker({}));
expectError(kafka.createKafkaBroker({ kafka: client, flavor: 'librdkafka' }));
