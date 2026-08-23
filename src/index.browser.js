'use strict';

const { Emitter, createEventStream, EventStream, isCodec } = require('./utils.js');
const { WrpcClient, WrpcClientProxy, WrpcError, connect } = require('./client.js');
const { WrpcReadable, WrpcWritable } = require('./streams.js');
const { chunkEncode, chunkDecode } = require('./chunks.browser.js');

module.exports = {
  isCodec,
  Emitter,
  createEventStream,
  EventStream,
  WrpcClient,
  WrpcClientProxy,
  WrpcError,
  connect,
  WrpcReadable,
  WrpcWritable,
  chunkEncode,
  chunkDecode,
};
