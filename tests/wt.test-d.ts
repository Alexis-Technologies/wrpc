import { expectAssignable, expectError, expectType } from 'tsd';
import * as wt from '../wt.js';
import type { AttachSessionOptions, SessionAcceptor, WtSession, WtSocket, WtStream } from '../wt.js';
import type { Client, Server, WrpcClientOptions } from '../index.js';

expectType<typeof WtSocket>(wt.WtSocket);
expectType<typeof wt.StreamParser>(wt.StreamParser);
expectType<0>(wt.KIND_TEXT);
expectType<1>(wt.KIND_BINARY);
expectType<5>(wt.HEADER_BYTES);

// The session contract is structural: anything W3C-shaped qualifies
declare const stream: WtStream;
declare const session: {
  closed: Promise<{ closeCode: number; reason: string }>;
  incomingBidirectionalStreams: ReadableStream<WtStream>;
  createBidirectionalStream(): Promise<WtStream>;
  close(info?: { closeCode?: number; reason?: string }): void;
};
expectAssignable<WtSession>(session);
expectError<WtSession>({ close() {} }); // no streams
expectType<boolean>(wt.isWtSession(session));

// Attaching: a Server or an RpcServer, the CONNECT request alongside
declare const server: Server;
expectType<Promise<Client | null>>(wt.attachSession(server, session));
expectType<Promise<Client | null>>(
  wt.attachSession(server.rpc, session, { headers: { origin: 'https://a' }, url: '/api?wrpc_h=%7B%7D' }),
);
expectAssignable<AttachSessionOptions>({ verify: () => false, acceptTimeout: 1000, kind: 'wt', highWaterMark: 1 });
expectError(wt.attachSession(server, session, { acceptTimeout: 'soon' }));

// Accepting a host's stream of sessions
declare const sessions: ReadableStream<WtSession>;
const acceptor = wt.acceptSessions(server, sessions, { onClient: (client) => void expectType<Client>(client) });
expectType<SessionAcceptor>(acceptor);
expectType<Promise<void>>(acceptor.stop());
expectType<Promise<void>>(acceptor.done);
wt.acceptSessions(server, [session], { meta: () => ({ url: '/' }) });

// The adapters read the request off the host's objects
expectType<string>(wt.fromFails({}).url);
expectType<Promise<{ path: string } & Record<string, unknown>>>(
  wt.failsRequestCallback({ header: { ':path': '/api?x' } }),
);
expectAssignable<AttachSessionOptions>({ idleTimeout: 60_000 });
declare const req: { headers: Record<string, string>; on(): void };
declare const res: {
  writeHead(): void;
  end(): void;
  sendDatagram(): void;
  createBidirectionalStream(): unknown;
};
expectType<WtSession>(wt.fromQuico(req, res).session);

// The socket is the engine-port shape
const socket = new wt.WtSocket(session, stream, { remoteAddress: '::1' });
expectType<boolean>(socket.send('{"type":"ping"}'));
expectType<number>(socket.bufferedAmount);
socket.on('message', (data, isBinary) => {
  expectType<string | Uint8Array>(data);
  expectType<boolean>(isBinary);
});

// Framing
const parser = new wt.StreamParser({ onMessage: (kind, data) => void [kind, data] });
expectType<void>(parser.push(wt.frameText('{}')));
expectType<Uint8Array>(wt.frame(wt.KIND_BINARY, new Uint8Array(2)));
expectError(wt.frame(3, new Uint8Array(2)));
expectType<Uint8Array>(wt.frameCaps('{"streams":true}'));
expectType<2>(wt.KIND_CAPS);

// The client side lives in the base entry and is named at connect time
expectAssignable<WrpcClientOptions>({
  transport: ['wt', 'ws'],
  wt: { serverCertificateHashes: [{ algorithm: 'sha-256', value: 'x' }] },
});
expectAssignable<WrpcClientOptions>({ transport: 'wt', wt: { WebTransport: class {} } });
expectError<WrpcClientOptions>({ transport: 'wt', wt: { congestionControl: 'fast' } });

// Unreliable delivery is an option on every event send, never a new API
declare const wsClient: import('../index.js').WrpcClient;
expectType<void>(wsClient.sendEvent('game/position', { x: 1 }, { unreliable: true }));
declare const attached: Client;
expectType<void>(attached.sendEvent('game/state', {}, { unreliable: true }));
expectType<boolean>(attached.sendRaw('{}', { unreliable: true }));
expectType<number>(server.to('r').emit('game/state', {}, { unreliable: true }));
expectType<boolean>(socket.sendUnreliable('{}'));
expectType<number>(socket.maxDatagramSize);
expectType<string | null>(wt.parseDatagram(wt.datagramText('{}')));
