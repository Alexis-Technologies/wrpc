import { expectAssignable, expectType } from 'tsd';
import * as engine from '../engine.js';
import type { Engine, WrpcSocket, EngineConnectionSource } from '../engine.js';
import type { Connection } from '../ws.js';

const nodeEngine = engine.createNodeEngine();
expectType<Engine>(nodeEngine);
expectType<string>(nodeEngine.name);
expectType<boolean>(nodeEngine.capabilities.backpressure);
expectType<boolean>(nodeEngine.capabilities.pause);
expectType<void>(nodeEngine.close());
expectType<void>(nodeEngine.close({ code: 1001, reason: 'bye' }));

declare const httpServer: import('node:http').Server;
const source = nodeEngine.attach({ server: httpServer, pingInterval: 5000 });
expectType<EngineConnectionSource>(source);
source.on('connection', (socket, req) => {
  expectType<WrpcSocket>(socket);
  expectType<import('node:http').IncomingMessage>(req);
  expectType<boolean>(socket.send('x'));
  expectType<number>(socket.bufferedAmount);
  socket.close(1000, 'done');
  socket.terminate();
  socket.pause?.();
  socket.resume?.();
});

// The built-in Connection satisfies the WrpcSocket contract
declare const connection: Connection;
expectAssignable<WrpcSocket>(connection);

// isEngine narrows unknown values
declare const candidate: unknown;
if (engine.isEngine(candidate)) {
  expectType<Engine>(candidate);
}
