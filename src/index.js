'use strict';

const { Emitter, createEventStream, EventStream, isCodec } = require('./utils.js');
const { isCompressor } = require('./compression/index.js');
const { dictionaryCompressor } = require('./compression/dictionary.js');
const { deflateCompressor, brotliCompressor, zstdCompressor } = require('./compression/codecs.js');
const { buildDictionary } = require('./rpc/dictionary.js');
const { WrpcClient, WrpcClientProxy, WrpcError, connect } = require('./client.js');
const { ConfigError, ArgumentError, ClosedError, ProtocolError, TransportError } = require('./errors.js');
const { Server } = require('./server.js');
const { RpcServer, Client, Context } = require('./rpc/core.js');
const { readHandshake } = require('./rpc/handshake.js');
const { defineRouter, procedure, Router, Procedure, effectiveSchema } = require('./rpc/router.js');
const { RoomRegistry, Broadcast } = require('./rpc/rooms.js');
const { Cluster } = require('./rpc/cluster.js');
const { tracked, isTracked, createEventLog, EventLog } = require('./rpc/subscriptions.js');
const { Session, MemorySessionStore, createProxy, isTokenTransport } = require('./rpc/sessions.js');
const { ServerTransport, buildHeaders } = require('./transport.js');
const { WrpcReadable, WrpcWritable } = require('./streams.js');
const { chunkEncode, chunkDecode } = require('./chunks.js');

// The WebSocket engine internals live in the '@alexify/wrpc/ws' subpath;
// the engine port contract in '@alexify/wrpc/engine'.
module.exports = {
  Emitter,
  createEventStream,
  EventStream,
  isCodec,
  isCompressor,
  dictionaryCompressor,
  deflateCompressor,
  brotliCompressor,
  zstdCompressor,
  buildDictionary,
  WrpcClient,
  WrpcClientProxy,
  WrpcError,
  connect,
  ConfigError,
  ArgumentError,
  ClosedError,
  ProtocolError,
  TransportError,
  Server,
  RpcServer,
  Client,
  Context,
  readHandshake,
  defineRouter,
  procedure,
  Router,
  Procedure,
  effectiveSchema,
  RoomRegistry,
  Broadcast,
  Cluster,
  tracked,
  isTracked,
  createEventLog,
  EventLog,
  Session,
  createProxy,
  isTokenTransport,
  MemorySessionStore,
  ServerTransport,
  buildHeaders,
  WrpcReadable,
  WrpcWritable,
  chunkEncode,
  chunkDecode,
};
