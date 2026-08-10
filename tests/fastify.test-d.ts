import { expectAssignable, expectError, expectType } from 'tsd';
import * as adapter from '../fastify.js';
import type { FastifyLike, WrpcFastifyOptions } from '../fastify.js';
import type { Engine } from '../engine.js';
import type { RpcServer, Router } from '../index.js';
import type { UwsApp } from '../uws.js';

declare const fastify: FastifyLike;
declare const router: Router;
declare const rpc: RpcServer;
declare const engine: Engine;

// The plugin is an async fastify plugin: (instance, options) => Promise<void>
expectType<Promise<void>>(adapter.wrpcFastify(fastify));
expectType<Promise<void>>(adapter.wrpcFastify(fastify, { router }));
expectAssignable<(instance: FastifyLike, options?: WrpcFastifyOptions) => Promise<void>>(adapter.wrpcFastify);
expectError(adapter.wrpcFastify());
expectError(adapter.wrpcFastify({}));

// A real fastify instance is what the plugin actually receives
declare const realFastify: import('fastify').FastifyInstance;
expectAssignable<FastifyLike>(realFastify);

// Options: the RpcServer surface plus the three adapter-specific knobs
expectAssignable<WrpcFastifyOptions>({});
expectAssignable<WrpcFastifyOptions>({ router });
expectAssignable<WrpcFastifyOptions>({
  router,
  sessions: { cookie: { name: 'sid', sameSite: 'Strict' }, generateToken: () => 'token' },
  cors: { origins: ['https://app.example'], credentials: true },
  basePath: '/rpc',
  console: globalThis.console,
});
// `rpc` reuses an existing core, `engine` skips backend detection
expectAssignable<WrpcFastifyOptions>({ rpc });
expectAssignable<WrpcFastifyOptions>({ router, engine });
// `ws` is forwarded to the engine's attach() minus the server it never owns
expectAssignable<WrpcFastifyOptions>({
  router,
  ws: { path: '/ws', protocols: ['wrpc'], pingInterval: 5000, verifyClient: () => true },
});
declare const httpServer: import('node:http').Server;
expectError<WrpcFastifyOptions>({ router, ws: { server: httpServer } });
expectError<WrpcFastifyOptions>({ router: 'chat' });
expectError<WrpcFastifyOptions>({ router, basePath: 8000 });

// findUwsApp digs the uWebSockets.js app out of a fastify-uws server
expectType<UwsApp | null>(adapter.findUwsApp(realFastify.server));
expectType<UwsApp | null>(adapter.findUwsApp(null));
expectType<UwsApp | null>(adapter.findUwsApp({}));
expectError(adapter.findUwsApp());
