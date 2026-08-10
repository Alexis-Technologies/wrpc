import { expectAssignable, expectError, expectType } from 'tsd';
import * as wrpc from '../index.js';
import type {
  Emitter,
  WrpcClient,
  WrpcClientProxy,
  WrpcError,
  Server,
  RpcServer,
  Client,
  Context,
  Session,
  ServerTransport,
  WrpcReadable,
  WrpcWritable,
  ClientTransport,
  ServerHttpTransport,
  Router,
  Procedure,
  SessionStore,
  HttpCall,
} from '../index.js';
import type { Engine, EngineConnectionSource } from '../engine.js';

expectType<typeof Emitter>(wrpc.Emitter);
expectType<typeof WrpcClient>(wrpc.WrpcClient);
expectType<typeof WrpcClientProxy>(wrpc.WrpcClientProxy);
expectType<typeof WrpcError>(wrpc.WrpcError);
expectType<typeof Server>(wrpc.Server);
expectType<typeof RpcServer>(wrpc.RpcServer);
expectType<typeof Client>(wrpc.Client);
expectType<typeof Context>(wrpc.Context);
expectType<typeof Session>(wrpc.Session);
expectType<typeof ServerTransport>(wrpc.ServerTransport);
expectType<typeof WrpcReadable>(wrpc.WrpcReadable);
expectType<typeof WrpcWritable>(wrpc.WrpcWritable);
expectType<typeof Router>(wrpc.Router);
expectType<typeof Procedure>(wrpc.Procedure);

// Engine internals moved to the '@alexify/wrpc/ws' subpath (see ws.test-d.ts)
expectError(wrpc.WebsocketServer);
expectError(wrpc.Connection);
expectError(wrpc.Frame);
expectError(wrpc.FrameParser);

// Router / procedures
const router = wrpc.defineRouter({
  chat: {
    send: wrpc.procedure({
      access: 'public',
      handler: async (context, args) => {
        expectType<Context>(context);
        return { ok: true, args };
      },
    }),
    shorthand: async () => 42,
  },
  'auth.1': {
    signIn: { handler: async () => true },
  },
});
expectType<Router>(router);
expectType<Procedure | null>(router.getProcedure('chat', '*', 'send'));
expectType<Router>(router.merge(router));
expectType<Procedure>(wrpc.procedure(async () => 1));
expectError(wrpc.procedure({ access: 'public' })); // handler is required
expectAssignable<wrpc.ProcedureOptions>({
  handler: async () => 1,
  input: (value: unknown) => value,
  timeout: 5000,
  queue: { concurrency: 8, size: 16, timeout: 1000 },
  meta: { description: 'x' },
});

// Sessions
expectAssignable<SessionStore>(new wrpc.MemorySessionStore());
expectAssignable<wrpc.SessionsOptions>({
  store: new wrpc.MemorySessionStore(),
  cookie: { name: 'sid', secure: false, sameSite: 'Strict', maxAge: 3600 },
  generateToken: () => 'token',
});

// Server options: router-based signature; the old (application, options) is gone
expectAssignable<wrpc.ServerOptions>({ router });
expectAssignable<wrpc.ServerOptions>({
  router,
  host: 'localhost',
  port: 8000,
  protocol: 'http',
  basePath: '/rpc',
  cors: { origins: ['https://app.example'], credentials: true },
  ws: { pingInterval: 5000, perMessageDeflate: true },
});
expectAssignable<wrpc.CorsOptions>({ origins: (origin: string) => origin.endsWith('.example') });
expectError<wrpc.ServerOptions>({ router, kind: 'server' });

// RpcServer core surface
declare const rpc: RpcServer;
expectType<Router>(rpc.router);
expectType<string>(rpc.basePath);
expectType<Set<Client>>(rpc.clients);
expectType<Promise<void>>(rpc.close());
expectAssignable<HttpCall>({
  method: 'POST',
  url: '/api',
  headers: {},
  body: '{}',
  respond: (response) => {
    expectType<number>(response.status);
  },
});

// Server is an Emitter and exposes the core
declare const server: Server;
expectAssignable<Emitter>(server);
expectType<RpcServer>(server.rpc);
expectType<Promise<Server>>(server.listen());
expectType<Promise<void>>(server.close());

// A standalone engine (uWebSockets.js) owns the network stack, so there is
// no node http server: read the bound address through server.address().
expectType<import('node:http').Server | null>(server.httpServer);
expectType<EngineConnectionSource | null>(server.wsServer);
expectType<{ address: string; family: string; port: number } | string | null>(server.address());
expectError(server.address('http'));

// Engines are injected through ServerOptions.engine
declare const engine: Engine;
expectAssignable<wrpc.ServerOptions>({ router, engine });
expectAssignable<wrpc.ServerOptions>({ router, engine, ws: { path: '/ws' } });
expectError<wrpc.ServerOptions>({ router, engine: { name: 'broken' } });

// Client sessions are store-backed and async where they touch the store
declare const client: Client;
expectType<boolean>(client.initializeSession());
expectType<boolean>(client.startSession('token', {}));
expectType<Promise<boolean>>(client.restoreSession('token'));
expectType<Promise<boolean>>(client.finalizeSession());
expectType<Promise<unknown>>(client.sessionReady);
expectType<Promise<void>>(client.emit('room/event', { x: 1 }));

// Context.session mirrors the live client session
declare const context: Context;
expectType<Session | null>(context.session);

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
expectType<boolean>(httpTransport.responded);

// buildHeaders computes per-request CORS headers
expectType<Record<string, string>>(wrpc.buildHeaders());
expectType<Record<string, string>>(wrpc.buildHeaders({ origins: ['https://a'] }, 'https://a'));

// WrpcClientOptions: real proxy option is typed, phantom handlers are gone
expectAssignable<wrpc.WrpcClientOptions>({ proxy: (data: string) => void data });
expectError<wrpc.WrpcClientOptions>({ packetHandler: (data: string) => void data });

// Stream surface
declare const readable: WrpcReadable;
expectType<Promise<void>>(readable.stop());
expectType<ArrayBufferView | undefined>(readable.pull());
declare const writable: WrpcWritable;
expectType<boolean>(writable.write(new Uint8Array(1)));
expectType<boolean>(writable.closed);
