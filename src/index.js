'use strict';

const { Emitter, createEventStream, EventStream } = require('./utils.js');
const { WrpcClient, WrpcClientProxy, WrpcError, connect } = require('./client.js');
const { Server } = require('./server.js');
const { RpcServer, Client, Context } = require('./rpc/core.js');
const { defineRouter, procedure, Router, Procedure } = require('./rpc/router.js');
const { RoomRegistry, Broadcast } = require('./rpc/rooms.js');
const { tracked, isTracked, createEventLog, EventLog } = require('./rpc/subscriptions.js');
const { Session, MemorySessionStore, createProxy } = require('./rpc/sessions.js');
const { ServerTransport, buildHeaders } = require('./transport.js');
const { WrpcReadable, WrpcWritable } = require('./streams.js');
const { chunkEncode, chunkDecode } = require('./chunks.js');

// The WebSocket engine internals live in the '@alexify/wrpc/ws' subpath;
// the engine port contract in '@alexify/wrpc/engine'.
module.exports = {
  Emitter,
  WrpcClient,
  WrpcClientProxy,
  WrpcError,
  connect,
  Server,
  RpcServer,
  Client,
  Context,
  Session,
  createProxy,
  defineRouter,
  procedure,
  Router,
  Procedure,
  RoomRegistry,
  Broadcast,
  tracked,
  isTracked,
  createEventLog,
  EventLog,
  createEventStream,
  EventStream,
  MemorySessionStore,
  ServerTransport,
  buildHeaders,
  WrpcReadable,
  WrpcWritable,
  chunkEncode,
  chunkDecode,
};
