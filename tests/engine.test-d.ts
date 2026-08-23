import { expectAssignable, expectError, expectType } from 'tsd';
import * as engine from '../engine.js';
import type {
  Engine,
  EngineAttachOptions,
  EngineConnectionSource,
  EngineRequest,
  EngineVerifyClientInfo,
  WrpcSocket,
} from '../engine.js';
import type { Connection } from '../ws.js';

const nodeEngine = engine.createNodeEngine();
expectType<Engine>(nodeEngine);
expectType<string>(nodeEngine.name);
expectType<boolean>(nodeEngine.capabilities.backpressure);
expectType<boolean>(nodeEngine.capabilities.pause);
expectType<void>(nodeEngine.close());
expectType<void>(nodeEngine.close({ code: 1001, reason: 'bye' }));

// The port has two kinds; both markers are optional on the base contract
expectType<boolean | undefined>(nodeEngine.standalone);
expectType<((options: { host?: string; port?: number }) => Promise<unknown>) | undefined>(nodeEngine.listen);
if (nodeEngine.listen) {
  expectType<Promise<unknown>>(nodeEngine.listen({ host: '127.0.0.1', port: 0 }));
}

declare const httpServer: import('node:http').Server;
const source = nodeEngine.attach({ server: httpServer, pingInterval: 5000 });
expectType<EngineConnectionSource>(source);
source.on('connection', (socket, req) => {
  expectType<WrpcSocket>(socket);
  // The port promises an IncomingMessage-shaped request, not a node one:
  // a standalone engine synthesizes its own (see uws.test-d.ts).
  expectType<EngineRequest>(req);
  expectType<string | undefined>(req.url);
  expectType<string | undefined>(req.socket.remoteAddress);
  expectType<boolean>(socket.send('x'));
  expectType<number>(socket.bufferedAmount);
  socket.close(1000, 'done');
  socket.terminate();
  socket.pause?.();
  socket.resume?.();
});

// A real IncomingMessage satisfies the port's request shape
declare const incoming: import('node:http').IncomingMessage;
expectAssignable<EngineRequest>(incoming);

// Manual-upgrade mode: hosted engines MAY be driven from the app's own
// 'upgrade' listener instead of binding to a server (the express adapter).
declare const duplex: import('node:stream').Duplex;
declare const head: Buffer;
expectType<
  ((req: import('node:http').IncomingMessage, socket: import('node:stream').Duplex, head: Buffer) => void) | undefined
>(source.handleUpgrade);
if (source.handleUpgrade) {
  expectType<void>(source.handleUpgrade(incoming, duplex, head));
}

// attach(): `server` is optional — absent for standalone engines and for
// the manual-upgrade mode; `onHttpCall` is the standalone HTTP entry point.
expectAssignable<EngineAttachOptions>({});
expectAssignable<EngineAttachOptions>({ server: httpServer });
expectAssignable<EngineAttachOptions>({ path: '/ws', protocols: ['wrpc'], closeTimeout: 1000 });
expectAssignable<EngineAttachOptions>({
  onHttpCall: (call) => {
    expectType<string>(call.method);
    expectType<string>(call.url);
    return undefined;
  },
});
expectAssignable<EngineAttachOptions>({
  verifyClient: (info) => {
    expectType<EngineVerifyClientInfo>(info);
    expectType<EngineRequest>(info.req);
    expectType<import('node:stream').Duplex | null>(info.socket);
    expectType<Buffer | null>(info.head);
    return true;
  },
  handleProtocols: (offered, req) => {
    expectType<Array<string>>(offered);
    expectType<EngineRequest>(req);
    return offered[0] ?? false;
  },
});
expectError<EngineAttachOptions>({ standalone: true });
// createNodeEngine is pre-attach configuration: it never takes a server
expectError(engine.createNodeEngine({ server: httpServer }));

// The built-in Connection satisfies the WrpcSocket contract
declare const connection: Connection;
expectAssignable<WrpcSocket>(connection);

// A hand-rolled standalone engine satisfies the port
expectAssignable<Engine>({
  name: 'custom',
  standalone: true,
  capabilities: { backpressure: true, ping: false, deflate: false, cork: false, pause: false },
  attach: () => source,
  listen: async () => ({ address: '127.0.0.1', family: 'IPv4', port: 8000 }),
  close: () => {},
});

// isEngine narrows unknown values
declare const candidate: unknown;
if (engine.isEngine(candidate)) {
  expectType<Engine>(candidate);
}
