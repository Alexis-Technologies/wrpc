'use strict';

const { Emitter } = require('./utils.js');
const { WrpcClient, WrpcClientProxy, WrpcError } = require('./client.js');
const { Server, Client, Context, Session, createProxy } = require('./server.js');
const { ServerTransport, buildHeaders } = require('./transport.js');
const { WrpcReadable, WrpcWritable } = require('./streams.js');
const { chunkEncode, chunkDecode } = require('./chunks.js');

// The WebSocket engine internals (WebsocketServer, Connection, Frame,
// FrameParser, ...) are published via the '@alexify/wrpc/ws' subpath.
module.exports = {
  Emitter,
  WrpcClient,
  WrpcClientProxy,
  WrpcError,
  Server,
  Client,
  Context,
  Session,
  createProxy,
  ServerTransport,
  buildHeaders,
  WrpcReadable,
  WrpcWritable,
  chunkEncode,
  chunkDecode,
};
