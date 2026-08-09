import { expectType } from 'tsd';
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
