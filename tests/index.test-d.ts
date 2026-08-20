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
  RoomRegistry,
  Broadcast,
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
  'auth.v1': {
    signIn: { handler: async () => true },
  },
});
expectType<Router>(router);
expectType<Procedure | null>(router.getProcedure('chat', '*', 'send'));

// The REST version strategy: 'path' or a function taking the vN token
expectAssignable<Parameters<typeof wrpc.defineRouter>[1]>({ rest: { version: 'path' } });
expectAssignable<Parameters<typeof wrpc.defineRouter>[1]>({
  rest: { version: (version: string, path: string) => `/${version}${path}` },
});
expectError(wrpc.defineRouter({}, { rest: { version: 'header' } }));

// Inbound events: the reserved `on` key, handlers shaped like procedures
const eventful = wrpc.defineRouter({
  chat: {
    send: async () => ({ ok: true }),
    on: {
      typing: async (context, data) => {
        expectType<Context>(context);
        void data;
      },
      seen: { access: 'session', handler: async () => {} },
    },
  },
});
expectType<Procedure | null>(eventful.getEventHandler('chat', '*', 'typing'));
expectType<Procedure | null>(eventful.getEventHandler('chat', undefined, 'typing'));
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

// The codegen descriptor: a closed shape language, so a typo is a type error
// rather than a silent `unknown` in whatever the CLI generates.
expectAssignable<wrpc.Signature>({ args: { room: 'string', 'limit?': 'number' }, returns: 'boolean' });
expectAssignable<wrpc.Signature>({ args: 'object', data: [{ id: 'string', tags: 'string[]' }] });
expectAssignable<wrpc.Signature>({});
expectAssignable<wrpc.SignatureShape>({ nested: { deeper: ['string'] } });
// An array describes an array OF one shape, so a tuple is not a descriptor
expectError<wrpc.Signature>({ args: ['string', 'number'] });
expectError<wrpc.Signature>({ args: 42 });
expectError<wrpc.Signature>({ nonsense: 'string' });
expectAssignable<wrpc.ProcedureOptions>({
  handler: async () => 1,
  signature: { args: { text: 'string' }, returns: { id: 'string' } },
});

// Subscriptions: an async generator handler is one, and so is the explicit
// spelling; both scaffold subscribe/iterate on the client.
const feed = wrpc.defineRouter({
  chat: {
    onMessage: {
      access: 'public',
      handler: async function* (context, args, options) {
        expectType<Context>(context);
        expectType<AbortSignal>(options.signal);
        expectType<string | undefined>(options.lastEventId);
        void args;
        yield wrpc.tracked('1', { text: 'hi' });
      },
    },
    onTyping: wrpc.procedure.subscription({
      access: 'public',
      handler: async function* () {
        yield { typing: true };
      },
    }),
  },
});
expectType<Procedure | null>(feed.getProcedure('chat', '*', 'onMessage'));
// queue and timeout mean something else for a stream, so they are refused
expectError(wrpc.procedure.subscription({ handler: async function* () {}, timeout: 100 }));

declare const subscriptionMethod: wrpc.SubscriptionMethod;
expectType<'subscription'>(subscriptionMethod.kind);
const subscription = subscriptionMethod.subscribe({ room: 'a' }, { lastEventId: '3', onData: () => {} });
expectType<string>(subscription.id);
expectType<string | undefined>(subscription.lastEventId);
expectType<boolean>(subscription.closed);
expectType<boolean>(subscription.unsubscribe());

// The resume primitives
const log = wrpc.createEventLog<{ text: string }>({ size: 10 });
expectType<string>(log.push({ text: 'hi' }));
expectType<Array<wrpc.Tracked<{ text: string }>> | null>(log.since('2'));
expectType<string | null>(log.lastEventId);
const stream = wrpc.createEventStream<number>({ highWaterMark: 4 });
expectType<boolean>(stream.push(1));
expectType<void>(stream.end());
expectType<number>(stream.dropped);

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

// The logger option: a Console and a structured logger both fit the same
// all-optional shape, and `false` is the way to silence a server outright.
expectAssignable<wrpc.WrpcLogger>(globalThis.console);
declare const pinoLike: {
  level: string;
  child(bindings: Record<string, unknown>): typeof pinoLike;
  info(entry: object, message?: string): void;
  debug(entry: object, message?: string): void;
  warn(entry: object, message?: string): void;
  error(entry: object, message?: string): void;
};
expectAssignable<wrpc.WrpcLogger>(pinoLike);
expectAssignable<wrpc.ServerOptions>({ router, logger: false });
expectAssignable<wrpc.ServerOptions>({ router, logger: pinoLike });
expectAssignable<wrpc.RpcServerOptions>({ router, logger: globalThis.console });
expectError<wrpc.ServerOptions>({ router, logger: 'verbose' });
expectAssignable<wrpc.WrpcClientOptions>({ logger: pinoLike });
expectAssignable<wrpc.WrpcClientOptions>({ logger: false });

// Telemetry: both injection modes, and a real OTel span shape assignable to
// the structural view without importing @opentelemetry/api.
declare const tracer: wrpc.WrpcTracer;
declare const meter: wrpc.WrpcMeter;
declare const otelApi: wrpc.WrpcTelemetryApi;
expectAssignable<wrpc.ServerOptions>({ router, telemetry: { api: otelApi } });
expectAssignable<wrpc.ServerOptions>({ router, telemetry: { tracer, meter, includeIdentity: false } });
expectAssignable<wrpc.RpcServerOptions>({ router, telemetry: { tracer } });
expectAssignable<wrpc.RpcServerOptions>({ router, telemetry: null });
expectAssignable<wrpc.WrpcSpan>({ end: () => {} });
expectError<wrpc.ServerOptions>({ router, telemetry: { tracer: 'a tracer' } });

// RpcServer core surface
declare const rpc: RpcServer;
expectType<Router>(rpc.router);
expectType<string>(rpc.basePath);
expectType<Set<Client>>(rpc.clients);
expectType<Promise<void>>(rpc.close());

// Rooms: chainable targets, a local recipient count
declare const someClient: Client;
expectType<Broadcast>(rpc.to('chat'));
expectType<Broadcast>(rpc.to('chat', 'lobby').except(someClient).local());
expectType<number>(rpc.to('chat').emit('message', { text: 'hi' }));
expectType<number>(rpc.broadcast('announce'));
expectType<Array<string> | null>(rpc.to('chat').rooms);
expectType<RoomRegistry>(rpc.rooms);
expectType<Set<Client>>(rpc.rooms.members('chat'));
expectType<string>(rpc.instanceId);
expectType<boolean>(someClient.join('chat'));
expectType<boolean>(someClient.leave('chat'));
expectType<Set<string>>(someClient.rooms);
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
// ...and Context.server is how a handler reaches rooms
expectType<RpcServer | null>(context.server);
expectType<RpcServer | null>(client.server);

// Every public getter of Context and Client is typed: the documented
// `context.log.info(...)` pattern must compile, and so must the rest —
// this block is what makes .d.ts drift a CI failure instead of a user bug.
expectType<Context['log']>(context.log);
context.log.info({ event: 'probe' }, 'message');
expectType<string>(context.uuid);
expectType<Record<string, unknown>>(context.state);
expectType<AbortSignal | null>(context.signal);
expectType<Client>(context.client);
// The call identity: the wire target and the resolved procedure
expectType<string | null>(context.method);
expectType<wrpc.Procedure | null>(context.procedure);
// onDisconnect's payload carries the pre-destroy room snapshot
expectAssignable<wrpc.ConnectionHook>((_client: Client, payload: { rooms: Set<string> } | null) => void payload);
expectType<Client['log']>(client.log);
expectType<string>(client.transportKind);
expectType<boolean>(client.persistent);
expectType<boolean>(client.binary);
expectType<string>(client.generateId());
expectType<number>(client.maxCalls);
expectType<number>(client.maxSubscriptions);
expectType<Set<string>>(client.rooms);

// The Phase-2 options compile everywhere they are accepted
expectAssignable<wrpc.WrpcClientOptions>({ generateId: () => 'id-1', protocols: ['wrpc.v1'] });
expectAssignable<wrpc.WrpcClientOptions>({ protocols: [] });
expectAssignable<wrpc.RpcServerOptions>({ router, generateId: () => 'id-1' });
expectAssignable<wrpc.RpcServerOptions>({ router, introspection: 'session', maxCalls: 64 });
expectAssignable<wrpc.ServerOptions>({ router, maxBodySize: 1024 });

// Cluster: identity, replicated presence, introspection, commands, messaging
expectType<string>(client.id);
expectType<Record<string, unknown>>(client.data);
expectType<Promise<unknown>>(client.ask('chat/confirm', { q: 1 }, { timeout: 1000 }));
expectType<Promise<unknown>>(client.expectAnswer('id-1', 1000));
expectType<boolean>(client.settleAnswer({ id: 'id-1', result: 1 }));
expectType<wrpc.Cluster>(rpc.cluster);
expectType<Client | undefined>(rpc.getClient('node-1.abc'));
// The shell delegates, and Cluster is a VALUE export (instanceof works).
expectType<wrpc.Cluster>(server.cluster);
expectType<Client | undefined>(server.getClient('node-1.abc'));
expectType<typeof wrpc.Cluster>(wrpc.Cluster);
expectType<string>(rpc.cluster.instanceId);
expectType<string>(rpc.cluster.epoch);
expectType<boolean>(rpc.cluster.connected);
expectType<number>(rpc.cluster.count('chat'));
expectType<{ total: number; instances: Record<string, number> }>(rpc.cluster.presence('chat'));
expectType<Array<string>>(rpc.cluster.instances());
expectType<Promise<Array<wrpc.ClientDescriptor> & { incomplete?: boolean }>>(rpc.cluster.fetchClients({ room: 'x' }));
expectType<void>(rpc.cluster.join('node-1.abc', 'ops'));
expectType<void>(rpc.cluster.leave({ room: 'chat' }, 'archive'));
expectType<void>(rpc.cluster.disconnect({}));
expectType<void>(rpc.cluster.sendEvent('cache/invalidate', { key: 'users' }));
expectType<Promise<wrpc.ClusterAskResult>>(rpc.cluster.ask('stats', {}, { timeout: 500 }));
expectType<void>(rpc.cluster.respond('stats', (data, from) => ({ data, from })));
expectType<boolean>(rpc.cluster.unrespond('stats'));
expectAssignable<wrpc.RpcServerOptions>({ router, cluster: { presenceInterval: 1000, requestTimeout: 500 } });

// Acks: the broadcast question aggregates, never rejects
expectType<Promise<wrpc.AskResult>>(rpc.to('chat').ask('chat/poll', { q: 1 }, { timeout: 1000 }));
declare const askResult: wrpc.AskResult;
expectType<Array<unknown>>(askResult.answers);
expectType<Array<{ message: string; code: number; details?: unknown }>>(askResult.errors);
expectType<number>(askResult.expected);
expectType<boolean>(askResult.incomplete);

// Hooks: three registration levels type-check, unknown phases do not
const hook: wrpc.Hook = async (ctx, payload) => {
  void ctx.state;
  void payload;
};
expectAssignable<wrpc.RouterHooks>({ onRequest: hook, preHandler: [hook, hook] });
expectAssignable<wrpc.RouterHooks>({ onConnect: async (c) => void c.transportKind });
expectError<wrpc.RouterHooks>({ onRequets: hook });
expectError<wrpc.UnitHooks>({ onConnect: hook });
wrpc.defineRouter({}, { hooks: { onRequest: hook } }).addHook('onError', hook);
expectAssignable<wrpc.ProcedureOptions>({
  handler: async () => null,
  preHandler: hook,
  preSerialization: [hook],
});
expectError<wrpc.ProcedureOptions>({ handler: async () => null, access: 'admin' });

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

// Resilience options: backoff, heartbeat, and their off switches
expectAssignable<wrpc.WrpcClientOptions>({
  reconnect: { minDelay: 100, maxDelay: 5000, factor: 1.5, jitter: false, retries: 10 },
  heartbeat: { interval: 1000, timeout: 200 },
  random: () => 0.5,
});
expectAssignable<wrpc.WrpcClientOptions>({ reconnect: false, heartbeat: false });
expectError<wrpc.WrpcClientOptions>({ reconnect: { minDelay: '100' } });
declare const wsClient: WrpcClient;
expectType<number>(wsClient.attempt);
expectType<void>(wsClient.sendEvent('chat/typing', { on: true }));
// use(): the static introspection artifact — raw system/introspect shape
expectType<WrpcClient>(wsClient.use({ chat: { ping: { access: 'public' } } }));
expectAssignable<Parameters<WrpcClient['use']>[0]>({
  chat: {
    ping: { access: 'public' },
    onMessage: { access: 'public', kind: 'subscription' },
    create: {
      access: 'public',
      http: { method: 'POST', path: '/chats', status: 201 },
      schema: { body: { type: 'object' } },
    },
  },
});
expectError(wsClient.use('nope'));
expectError(wsClient.use({ chat: { ping: { kind: 'subscription' } } })); // access is required
// The receiving half of a server ask: one responder per name.
expectType<void>(wsClient.respond('chat/confirm', (data) => ({ ok: true, data })));
expectType<void>(wsClient.respond('chat/confirm', async () => 'answer'));
expectType<boolean>(wsClient.unrespond('chat/confirm'));

// ---------------------------------------------------------------------------
// Contract-first typed client. The contract is a plain interface; nothing is
// generated, and an UNtyped client must keep behaving exactly as it did.

interface Contract {
  chat: {
    send(args: { text: string }): Promise<{ id: string }>;
    ping(): Promise<void>;
    find(args?: { q: string }): Promise<Array<string>>;
    /** A handler declared without a promise is still awaited on the wire. */
    sync(args: { a: 1 }): { done: true };
    onMessage: wrpc.SubscriptionContract<{ room: string }, { text: string }>;
    onTick: wrpc.SubscriptionContract<void, number>;
  };
  'auth.v1': {
    signIn(args: { login: string }): Promise<{ token: string }>;
  };
}

declare const typed: WrpcClient<Contract>;

// A call keeps its declared argument and gains the trailing CallOptions
expectType<Promise<{ id: string }>>(typed.api.chat.send({ text: 'hi' }));
expectType<Promise<{ id: string }>>(typed.api.chat.send({ text: 'hi' }, { signal: AbortSignal.timeout(1) }));
expectType<Promise<Array<string>>>(typed.api.chat.find());
expectType<Promise<Array<string>>>(typed.api.chat.find({ q: 'x' }, {}));
expectType<Promise<{ done: true }>>(typed.api.chat.sync({ a: 1 }));
expectType<Promise<{ token: string }>>(typed.api['auth.v1'].signIn({ login: 'a' }));
expectError(typed.api.chat.send({ text: 42 }));
expectError(typed.api.chat.send());
expectError(typed.api.chat.missing({}));
expectError(typed.api.missing);
expectError(typed.api.chat.send({ text: 'x' }, { signal: 'now' }));

// A zero-argument member KEEPS its args slot, because slot 0 on the wire is
// always the procedure's arguments. Typing it away would make `ping({signal})`
// compile and then send `{"signal":{}}` as the args, dropping the cancellation.
expectType<Promise<void>>(typed.api.chat.ping());
expectType<Promise<void>>(typed.api.chat.ping(undefined));
expectType<Promise<void>>(typed.api.chat.ping(undefined, { signal: AbortSignal.timeout(1) }));
expectError(typed.api.chat.ping({ signal: AbortSignal.timeout(1) }));

// A procedure takes ONE args object, so these cannot be called as declared
interface Malformed {
  unit: {
    two(a: number, b: string): Promise<void>;
    variadic(...args: Array<number>): Promise<void>;
    notAMethod: string;
  };
}
declare const malformed: WrpcClient<Malformed>;
expectError(malformed.api.unit.two(1, 'a'));
expectError(malformed.api.unit.variadic(1, 2, 3));
expectError(malformed.api.unit.notAMethod());
expectType<wrpc.InvalidContractMember>(malformed.api.unit.notAMethod);

// `load` only accepts unit keys the contract declares
expectType<Promise<void>>(typed.load('chat', 'auth.v1'));
expectError(typed.load('missing'));

// A subscription is NOT callable: it answers with a stream
expectType<'subscription'>(typed.api.chat.onMessage.kind);
expectError(typed.api.chat.onMessage({ room: 'a' }));
expectType<wrpc.Subscription>(typed.api.chat.onMessage.subscribe({ room: 'a' }));
expectType<wrpc.Subscription>(
  typed.api.chat.onMessage.subscribe({ room: 'a' }, { onData: (data) => expectType<{ text: string }>(data) }),
);
// `void` args mean the argument may be left out entirely
expectType<wrpc.Subscription>(typed.api.chat.onTick.subscribe());
expectType<wrpc.Subscription>(
  typed.api.chat.onTick.subscribe(undefined, { onData: (data) => expectType<number>(data) }),
);
expectError(typed.api.chat.onMessage.subscribe({ room: 42 }));
expectType<AsyncIterableIterator<{ text: string }> & { subscription: wrpc.Subscription }>(
  typed.api.chat.onMessage.iterate({ room: 'a' }),
);
// A unit is still an Emitter — that is where server -> client events arrive
expectType<void>(typed.api.chat.on('message', () => {}));

// connect() is WrpcClient.connect under a name that carries the type argument
expectType<Promise<WrpcClient<Contract>>>(wrpc.connect<Contract>('wss://host'));
expectType<Promise<WrpcClient<Contract>>>(wrpc.WrpcClient.connect<Contract>('wss://host'));
// ...and with no contract, nothing changes: `api` stays the loose record
expectType<Promise<WrpcClient>>(wrpc.connect('ws://host'));
declare const untyped: WrpcClient;
expectType<Emitter & Record<string, any>>(untyped.api.anything);
// Deliberately `any`, not `Promise<unknown>`: an untyped client cannot know
// what a method takes or answers, and a cast at every call site is worse.
expectType<any>(untyped.api.anything.whatever({ x: 1 }));
expectType<Promise<void>>(untyped.load('anything'));

// Inference utilities, on the declared side...
expectType<{ text: string }>(null as unknown as wrpc.InferArgs<Contract['chat']['send']>);
expectType<{ id: string }>(null as unknown as wrpc.InferResult<Contract['chat']['send']>);
expectType<{ room: string }>(null as unknown as wrpc.InferArgs<Contract['chat']['onMessage']>);
expectType<{ text: string }>(null as unknown as wrpc.InferResult<Contract['chat']['onMessage']>);
// A zero-arg member takes `void`, NOT `unknown` — a zero-parameter function is
// assignable to a one-parameter target, so naive inference lands on `unknown`.
expectType<void>(null as unknown as wrpc.InferArgs<Contract['chat']['ping']>);
expectType<{ q: string } | undefined>(null as unknown as wrpc.InferArgs<Contract['chat']['find']>);
// ...and on the mapped side
expectType<{ room: string }>(null as unknown as wrpc.InferArgs<typeof typed.api.chat.onMessage>);
expectType<{ text: string }>(null as unknown as wrpc.InferResult<typeof typed.api.chat.onMessage>);

// A contract key named `on` is NOT mapped: a unit is an Emitter at runtime, so
// `api.chat.on` has to stay the listener registration. An optional one used to
// reduce the whole unit to `never` and break every member access on it.
interface WithOn {
  chat: {
    send(args: { text: string }): Promise<void>;
    on?: { typing: (data: unknown) => void };
  };
}
declare const withOn: WrpcClient<WithOn>;
expectType<Promise<void>>(withOn.api.chat.send({ text: 'x' }));
expectType<void>(withOn.api.chat.on('typing', () => {}));

// Stream surface
declare const readable: WrpcReadable;
expectType<Promise<void>>(readable.stop());
expectType<ArrayBufferView | undefined>(readable.pull());
declare const writable: WrpcWritable;
expectType<boolean>(writable.write(new Uint8Array(1)));
expectType<boolean>(writable.closed);

// error.details: optional wire field on WrpcError and ask errors
declare const wrpcError: wrpc.WrpcError;
expectAssignable<unknown>(wrpcError.details);
expectAssignable<wrpc.WrpcError>(new wrpc.WrpcError({ message: 'x', code: 400, details: { issues: [] } }));

// Declarative REST: http + fastify-shaped schema on a procedure
const mapped = wrpc.procedure({
  access: 'public',
  http: { method: 'POST', path: '/projects/:orgId', status: 201 },
  schema: {
    params: { type: 'object' },
    query: { type: 'object' },
    response: { 201: { type: 'object' }, 500: false },
    tags: ['Projects'],
  },
  handler: async (_context, args) => args,
});
expectType<wrpc.HttpRoute | null>(mapped.http);
expectType<wrpc.ProcedureSchema | null>(mapped.schema);
expectType<wrpc.ProcedureSchema>(wrpc.effectiveSchema(mapped));
expectType<boolean>(router.hasRestRoutes);
const matched = router.matchRest('POST', ['projects', '42']);
if (matched && 'proc' in matched) {
  expectType<Record<string, string>>(matched.params);
  expectType<wrpc.HttpRoute>(matched.http);
}
expectType<Array<{ unitKey: string; methodName: string; proc: wrpc.Procedure; http: wrpc.HttpRoute }>>(
  router.restRoutes(),
);
expectAssignable<wrpc.RpcServerOptions>({ router, querystring: { parse: (text) => ({ text }) } });

// Injected validation compilers
expectAssignable<wrpc.ValidationOptions>({
  ajv: { compile: (schema) => (value) => Boolean(schema && value) },
  serializer: { compile: () => (value) => JSON.stringify(value) },
});
const compiledRouter = wrpc.defineRouter({}, { validation: { ajv: { compile: () => () => true } } });
expectType<wrpc.CompiledArtifacts | null>(compiledRouter.compiledFor(mapped));
expectType<boolean>(compiledRouter.hasSerializers);
expectAssignable<wrpc.RpcServerOptions>({ router, introspection: { access: 'session', schemas: false } });

// Client options: pluggable querystring + injected pre-validation
expectAssignable<Parameters<typeof wrpc.WrpcClient.connect>[1]>({
  querystring: { stringify: (query: object) => String(query) },
  validation: { ajv: { compile: () => () => true } },
});

// Transport fallback list
expectAssignable<Parameters<typeof wrpc.WrpcClient.connect>[1]>({ transport: ['ws', 'sse', 'http'] });

// Wire codec
expectAssignable<wrpc.WrpcCodec>({ encode: (packet) => JSON.stringify(packet), decode: (text) => JSON.parse(text) });
expectAssignable<wrpc.RpcServerOptions>({
  router,
  codec: { encode: () => '', decode: () => ({}), contentType: 'application/x-toy' },
});
expectType<boolean>(wrpc.isCodec({}));
// codec.rest: the REST body codec — binary allowed, rest-only codecs valid
expectAssignable<wrpc.WrpcRestCodec>({
  encode: (value) => new Uint8Array([1]),
  decode: (body) => body,
  contentType: 'application/msgpack',
});
expectAssignable<wrpc.WrpcCodec>({ rest: { encode: () => new Uint8Array(), decode: () => null } });
expectAssignable<wrpc.WrpcCodec>({
  encode: () => '',
  decode: () => ({}),
  rest: { encode: () => '', decode: () => null },
});
expectError<wrpc.WrpcCodec>({ rest: { encode: () => new Uint8Array() } }); // decode required
expectAssignable<wrpc.RpcServerOptions>({
  router,
  codec: { rest: { encode: () => new Uint8Array(), decode: () => null, contentType: 'application/msgpack' } },
});
expectAssignable<Parameters<typeof wrpc.WrpcClient.connect>[1]>({
  codec: { rest: { encode: () => new Uint8Array(), decode: () => null } },
});
declare const rpcForCodec: RpcServer;
expectType<wrpc.WrpcCodec | null>(rpcForCodec.codec);
