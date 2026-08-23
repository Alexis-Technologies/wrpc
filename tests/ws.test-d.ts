import { expectAssignable, expectError, expectType } from 'tsd';
import * as ws from '../ws.js';
import type {
  WebsocketServer,
  Connection,
  Frame,
  FrameParser,
  FrameHeader,
  ParseError,
  WebsocketServerOptions,
} from '../ws.js';

expectType<typeof WebsocketServer>(ws.WebsocketServer);
expectType<typeof Connection>(ws.Connection);
expectType<typeof Frame>(ws.Frame);
expectType<typeof FrameParser>(ws.FrameParser);
expectType<typeof ParseError>(ws.ParseError);
expectType<string>(ws.MAGIC);
expectType<number>(ws.CLOSE_TIMEOUT);
expectType<number>(ws.RSV1);
expectType<1009>(ws.CLOSE_CODES.MESSAGE_TOO_BIG);
expectType<0x01>(ws.OPCODES.TEXT);

// Connection send methods: sendClose is void, data sends report acceptance
declare const connection: Connection;
expectType<void>(connection.sendClose());
expectType<void>(connection.sendClose(1000, 'bye'));
expectType<boolean>(connection.sendText('x'));
expectType<boolean>(connection.sendBinary(Buffer.alloc(0)));
expectType<boolean>(connection.sendPing());
expectType<boolean>(connection.sendPong());
expectType<boolean>(connection.send('x'));

// Backpressure / flow-control surface
expectType<number>(connection.bufferedAmount);
expectType<boolean>(connection.isPaused);
expectType<void>(connection.pause());
expectType<void>(connection.resume());
expectType<string>(connection.protocol);
connection.on('drain', () => {});
connection.on('ping', (payload) => {
  expectType<Buffer>(payload);
});

// Server options: subprotocols, deflate, backpressure caps
declare const httpServer: import('node:http').Server;
expectAssignable<WebsocketServerOptions>({ server: httpServer });
expectAssignable<WebsocketServerOptions>({
  server: httpServer,
  protocols: ['wrpc'],
  maxBackpressure: 1024,
  fragmentThreshold: 65536,
  perMessageDeflate: { threshold: 512 },
});
expectAssignable<WebsocketServerOptions>({
  server: httpServer,
  handleProtocols: (offered, req) => {
    expectType<Array<string>>(offered);
    expectType<import('node:http').IncomingMessage>(req);
    return offered[0] ?? false;
  },
});
expectError<WebsocketServerOptions>({ server: httpServer, perMessageDeflate: { level: 9 } });

declare const wss: WebsocketServer;
expectType<Set<Connection>>(wss.connections);
expectType<void>(wss.close());
expectType<void>(wss.close({ code: 1001, reason: 'bye' }));

// FrameParser: header parsing and extension-aware RSV handling
declare const buffer: Buffer;
const headerResult = ws.FrameParser.parseHeader(buffer, { allowedRsv: ws.RSV1 });
expectType<FrameHeader | null>(headerResult.value);
expectType<ParseError | null>(headerResult.error);
const parseResult = ws.FrameParser.parse(buffer, { allowedRsv: 0 });
expectType<{ frame: Frame; bytesUsed: number } | null>(parseResult.value);
