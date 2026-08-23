import { expectAssignable, expectError, expectType } from 'tsd';
import * as uws from '../uws.js';
import type {
  UwsApp,
  UwsAttachOptions,
  UwsEngine,
  UwsEngineConnectionSource,
  UwsEngineOptions,
  UwsModule,
  UwsSocket,
  UpgradeRequest,
} from '../uws.js';
import type { Engine, WrpcSocket } from '../engine.js';

declare const uwsModule: UwsModule;
declare const uwsApp: UwsApp;

const engine = uws.createUwsEngine({ uws: uwsModule });
expectType<UwsEngine>(engine);
// A uws engine is a plain Engine as far as the Server shell is concerned
expectAssignable<Engine>(engine);

// Identity: the standalone kind of the engine port
expectType<'uws'>(engine.name);
expectType<true>(engine.standalone);
expectType<UwsApp>(engine.app);

// Capabilities are plain booleans (values are decided at runtime: uws has no
// ping/pause, and deflate only when `compression` is passed)
expectType<boolean>(engine.capabilities.backpressure);
expectType<boolean>(engine.capabilities.ping);
expectType<boolean>(engine.capabilities.deflate);
expectType<boolean>(engine.capabilities.cork);
expectType<boolean>(engine.capabilities.pause);

// Engine options: module injection, an existing app, uws tuning knobs
expectAssignable<UwsEngineOptions>({ uws: uwsModule });
expectAssignable<UwsEngineOptions>({ app: uwsApp });
expectAssignable<UwsEngineOptions>({
  uws: uwsModule,
  ssl: { key_file_name: 'key.pem', cert_file_name: 'cert.pem' },
  idleTimeout: 60,
  maxPayloadLength: 1024,
  maxBackpressure: 2048,
  closeOnBackpressureLimit: true,
  maxLifetime: 0,
  compression: 1,
  sendPingsAutomatically: false,
  maxBodySize: 4096,
});
expectAssignable<UwsEngineOptions>({ uws: uwsModule, ssl: null, compression: null });
expectError(uws.createUwsEngine({ uws: 5 }));
expectError(uws.createUwsEngine({ app: 'app' }));
expectError<UwsEngineOptions>({ uws: uwsModule, idleTimeout: '60' });

// attach(): no `server` — this engine owns the network stack
const source = engine.attach();
expectType<UwsEngineConnectionSource>(source);
expectType<UwsEngineConnectionSource>(engine.attach({ path: '/ws' }));
expectAssignable<UwsAttachOptions>({
  path: '/*',
  protocols: ['wrpc'],
  verifyClient: (info) => {
    expectType<UpgradeRequest>(info.req);
    expectType<null>(info.socket);
    expectType<null>(info.head);
    return info.req.headers.origin === 'https://app.example';
  },
  handleProtocols: (offered, req) => {
    expectType<Array<string>>(offered);
    expectType<UpgradeRequest>(req);
    return offered[0] ?? false;
  },
  onHttpCall: (call) => {
    expectType<string>(call.method);
    expectType<string>(call.url);
    expectType<Record<string, string | undefined>>(call.headers);
  },
});
expectError<UwsAttachOptions>({ server: uwsApp });

// The 'connection' listener gets a UwsSocket plus the synthesized request
source.on('connection', (socket, req) => {
  expectType<UwsSocket>(socket);
  expectType<UpgradeRequest>(req);
  expectType<string>(req.method);
  expectType<string>(req.url);
  expectType<Record<string, string>>(req.headers);
  expectType<string>(req.socket.remoteAddress);
  expectType<boolean>(socket.send('x'));
  expectType<boolean>(socket.send(Buffer.alloc(1)));
  expectType<number>(socket.bufferedAmount);
  expectType<boolean>(socket.closed);
  socket.close(1000, 'done');
  socket.terminate();
});
source.on('close', () => {});

// A UwsSocket satisfies the engine port's socket contract
declare const socket: UwsSocket;
expectAssignable<WrpcSocket>(socket);
expectType<void>(socket.markClosed(1006, 'gone'));
expectType<typeof UwsSocket>(uws.UwsSocket);
expectAssignable<UwsSocket>(new uws.UwsSocket({}, { remoteAddress: '127.0.0.1', protocol: 'wrpc' }));

// listen(): standalone engines own the listener and report the bound address
expectType<Promise<{ address: string; family: string; port: number }>>(engine.listen());
expectType<Promise<{ address: string; family: string; port: number }>>(engine.listen({ host: '127.0.0.1', port: 0 }));
expectType<void>(engine.close());
expectType<void>(engine.close({ code: 1001, reason: 'bye' }));

// uws send() status codes
expectType<0>(uws.BACKPRESSURE);
expectType<1>(uws.SUCCESS);
expectType<2>(uws.DROPPED);
