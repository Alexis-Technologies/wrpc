'use strict';

const { OPCODES, CLOSE_CODES, RSV1 } = require('./constants.js');
const { WebsocketServer, MAGIC } = require('./server.js');
const { Connection, CLOSE_TIMEOUT } = require('./connection.js');
const { Frame } = require('./frame.js');
const { FrameParser, ParseError, PARSE_ERR_CODES } = require('./frameParser.js');

module.exports = {
  OPCODES,
  CLOSE_CODES,
  RSV1,
  MAGIC,
  WebsocketServer,
  CLOSE_TIMEOUT,
  Connection,
  Frame,
  FrameParser,
  ParseError,
  PARSE_ERR_CODES,
};
