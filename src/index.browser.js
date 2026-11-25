'use strict';

const { Emitter, createEventStream, EventStream, isCodec } = require('./utils.js');
const { isCompressor } = require('./compression/index.js');
const { WrpcClient, WrpcClientProxy, WrpcError, connect } = require('./client.js');
const { ConfigError, ArgumentError } = require('./errors.js');
const { WrpcReadable, WrpcWritable } = require('./streams.js');
const { chunkEncode, chunkDecode } = require('./chunks.browser.js');

module.exports = {
  isCodec,
  isCompressor,
  Emitter,
  createEventStream,
  EventStream,
  WrpcClient,
  WrpcClientProxy,
  WrpcError,
  ConfigError,
  ArgumentError,
  connect,
  WrpcReadable,
  WrpcWritable,
  chunkEncode,
  chunkDecode,
};
