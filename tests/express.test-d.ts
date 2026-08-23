import { expectAssignable, expectError, expectType } from 'tsd';
import * as adapter from '../express.js';
import type { CreateWrpcOptions, WrpcMiddleware } from '../express.js';
import type { Engine, EngineConnectionSource } from '../engine.js';
import type { RpcServer, Router } from '../index.js';

type IncomingMessage = import('node:http').IncomingMessage;
type ServerResponse = import('node:http').ServerResponse;
type Duplex = import('node:stream').Duplex;

declare const router: Router;
declare const rpc: RpcServer;
declare const engine: Engine;
declare const req: IncomingMessage;
declare const res: ServerResponse;
declare const socket: Duplex;
declare const head: Buffer;

const wrpc = adapter.createWrpc({ router });
expectType<WrpcMiddleware>(wrpc);
expectType<WrpcMiddleware>(adapter.createWrpc());

// The pieces the app wires up itself: nothing here owns the listener
expectType<RpcServer>(wrpc.rpc);
expectType<Engine>(wrpc.engine);
expectType<EngineConnectionSource>(wrpc.wsServer);
expectType<Promise<void>>(wrpc.close());

// handler: express/connect middleware, (req, res, next) => void
expectType<void>(wrpc.handler(req, res, () => {}));
expectAssignable<(req: IncomingMessage, res: ServerResponse, next: () => void) => void>(wrpc.handler);
expectError(wrpc.handler(req, res));
expectError(wrpc.handler(req));

// upgrade: wire to httpServer.on('upgrade', wrpc.upgrade)
expectType<void>(wrpc.upgrade(req, socket, head));
expectAssignable<(req: IncomingMessage, socket: Duplex, head: Buffer) => void>(wrpc.upgrade);
declare const httpServer: import('node:http').Server;
httpServer.on('upgrade', wrpc.upgrade);

// Options: the RpcServer surface plus the adapter-specific knobs
expectAssignable<CreateWrpcOptions>({});
expectAssignable<CreateWrpcOptions>({
  router,
  sessions: { cookie: { name: 'sid', maxAge: 3600 } },
  cors: { origins: ['https://app.example'], credentials: true },
  basePath: '/rpc',
  logger: globalThis.console,
});
expectAssignable<CreateWrpcOptions>({ rpc });
expectAssignable<CreateWrpcOptions>({ router, engine, maxBodySize: 1024 });
expectAssignable<CreateWrpcOptions>({
  router,
  ws: { path: '/ws', protocols: ['wrpc'], maxBackpressure: 4096, verifyClient: () => true },
});
// `ws` is forwarded to attach() minus the server the adapter never binds
expectError<CreateWrpcOptions>({ router, ws: { server: httpServer } });
expectError<CreateWrpcOptions>({ router, maxBodySize: '1024' });
expectError<CreateWrpcOptions>({ router, handler: () => {} });
