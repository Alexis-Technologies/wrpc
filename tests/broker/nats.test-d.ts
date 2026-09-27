import { expectAssignable, expectError, expectType } from 'tsd';
import * as nats from '../../broker/nats.js';
import type { Broker } from '../../broker.js';
import type { NatsBrokerOptions, NatsConnection } from '../../broker/nats.js';

// The real connection type — what the transport's connect() resolves to —
// a devDependency used only here.
import type { connect as natsConnect } from '@nats-io/transport-node';
declare const realNc: Awaited<ReturnType<typeof natsConnect>>;
expectAssignable<NatsConnection>(realNc);

declare const nc: NatsConnection;
declare const headers: () => any;
declare const jetstream: (connection: NatsConnection) => any;
declare const jetstreamManager: (connection: NatsConnection) => Promise<any>;

const full = nats.createNatsBroker({ nc, headers, jetstream, jetstreamManager, prefix: 'app', ackWait: 10_000 });
expectAssignable<Broker>(full);
expectType<string>(full.name);
// A backplane + direct broker: JetStream is optional.
expectAssignable<Broker>(nats.createNatsBroker({ nc, headers, logger: false }));
expectAssignable<NatsBrokerOptions>({ nc, headers, stream: { queue: { storage: 'memory' } } });
expectError(nats.createNatsBroker({ headers }));
expectError(nats.createNatsBroker({ nc, headers, ackWait: 'soon' }));
// The group's cap is a broker option; null leaves JetStream's default.
expectAssignable<NatsBrokerOptions>({ nc, headers, maxAckPending: 64 });
expectAssignable<NatsBrokerOptions>({ nc, headers, maxAckPending: null });
expectError(nats.createNatsBroker({ nc, headers, maxAckPending: '64' }));
