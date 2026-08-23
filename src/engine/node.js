'use strict';

const { WebsocketServer } = require('../websocket/ws.js');

// Default engine: wraps the built-in RFC 6455 implementation. Its
// Connection objects satisfy the WrpcSocket contract natively (send ->
// boolean, bufferedAmount, close/terminate, pause/resume, 'message' /
// 'drain' / 'ping' / 'pong' / 'close'(code, reason) / 'error' events).
const createNodeEngine = (engineOptions = {}) => {
  let wss = null;
  return {
    name: 'node',
    // Hosted engine: the Server shell owns node:http(s) and the listener.
    standalone: false,
    capabilities: {
      backpressure: true,
      ping: true,
      deflate: true,
      cork: true,
      pause: true,
    },
    // attachOptions: { server, path, verifyClient, protocols,
    //   handleProtocols, perMessageDeflate, pingInterval, maxBuffer,
    //   maxPayload, maxBackpressure, fragmentThreshold, closeTimeout }
    // Returns an EventEmitter with 'connection'(socket, req) events.
    attach(attachOptions) {
      wss = new WebsocketServer({ ...engineOptions, ...attachOptions });
      return wss;
    },
    close(options) {
      if (wss) wss.close(options);
    },
  };
};

module.exports = { createNodeEngine };
