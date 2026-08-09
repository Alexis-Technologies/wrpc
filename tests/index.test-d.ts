import { expectAssignable, expectError, expectType } from 'tsd';
import * as wrpc from '../index.js';
import type {
  Emitter,
  WrpcClient,
  WrpcClientProxy,
  WrpcError,
  Server,
  Client,
  Context,
  Session,
  ServerTransport,
  WrpcReadable,
  WrpcWritable,
  WebsocketServer,
  Connection,
  Frame,
  FrameParser,
  ClientTransport,
  ServerHttpTransport,
} from '../index.js';

expectType<typeof Emitter>(wrpc.Emitter);
expectType<typeof WrpcClient>(wrpc.WrpcClient);
expectType<typeof WrpcClientProxy>(wrpc.WrpcClientProxy);
expectType<typeof WrpcError>(wrpc.WrpcError);
expectType<typeof Server>(wrpc.Server);
expectType<typeof Client>(wrpc.Client);
expectType<typeof Context>(wrpc.Context);
expectType<typeof Session>(wrpc.Session);
expectType<typeof ServerTransport>(wrpc.ServerTransport);
expectType<typeof WrpcReadable>(wrpc.WrpcReadable);
expectType<typeof WrpcWritable>(wrpc.WrpcWritable);
expectType<typeof WebsocketServer>(wrpc.WebsocketServer);
expectType<typeof Connection>(wrpc.Connection);
expectType<typeof Frame>(wrpc.Frame);
expectType<typeof FrameParser>(wrpc.FrameParser);

// Connection send methods: sendClose is void, the rest report acceptance
declare const connection: Connection;
expectType<void>(connection.sendClose());
expectType<void>(connection.sendClose(1000, 'bye'));
expectType<boolean>(connection.sendText('x'));
expectType<boolean>(connection.sendBinary(Buffer.alloc(0)));
expectType<boolean>(connection.sendPing());
expectType<boolean>(connection.sendPong());

// Server is an Emitter
declare const server: Server;
expectAssignable<Emitter>(server);

// Client.emit keeps the base Emitter Promise contract
declare const client: Client;
expectType<Promise<void>>(client.emit('room/event', { x: 1 }));
expectType<Promise<void>>(client.emit('close'));

// Transport subclasses are type-only: not reachable as runtime values...
expectError(wrpc.ClientTransport);
expectError(wrpc.ServerHttpTransport);
expectError(wrpc.ServerWsTransport);
expectError(wrpc.ServerEventTransport);
// ...but their types stay usable through the static registries
declare const clientWs: ClientTransport;
expectType<boolean>(clientWs.active);
expectType<ClientTransport>(new wrpc.WrpcClient.transport.ws('ws://localhost'));
declare const httpTransport: ServerHttpTransport;
expectType<Record<string, string>>(httpTransport.getCookies());
declare const req: import('node:http').IncomingMessage;
declare const res: import('node:http').ServerResponse;
expectType<ServerHttpTransport>(new wrpc.ServerTransport.transport.http(req, res));

// WrpcClientOptions: real proxy option is typed, phantom handlers are gone
expectAssignable<wrpc.WrpcClientOptions>({ proxy: (data: string) => void data });
expectError<wrpc.WrpcClientOptions>({ packetHandler: (data: string) => void data });
expectError<wrpc.WrpcClientOptions>({ binaryHandler: (input: ArrayBuffer) => void input });

// Server Options: phantom kind/ports removed, timeouts fully optional
expectAssignable<wrpc.Options>({ host: 'localhost', port: 8000 });
expectAssignable<wrpc.Options>({ host: 'localhost', port: 8000, timeouts: {} });
expectError<wrpc.Options>({ host: 'localhost', port: 8000, kind: 'server' });
expectError<wrpc.Options>({ host: 'localhost', port: 8000, ports: [8001] });

// Stream surface: previously undeclared public methods
declare const readable: WrpcReadable;
expectType<Promise<void>>(readable.stop());
expectType<Promise<void>>(readable.stop(true));
expectType<ArrayBufferView | undefined>(readable.pull());
expectType<void>(readable.checkStreamLimits());
expectType<Promise<unknown>>(readable.waitEvent('pull'));
declare const writable: WrpcWritable;
expectType<void>(writable.init());
