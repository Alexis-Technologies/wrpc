'use strict';

// `@alexify/wrpc/sse` — Server-Sent Events as a wrpc transport.
//
// Requiring this registers the client-side transport, after which
// `WrpcClient.connect(url, { transport: 'sse' })` works. The server side is
// built into RpcServer (the `{basePath}/events` route); what is exported
// here is the machinery behind it, for hosts that drive it themselves.
//
// In a browser bundle this resolves to src/sse/client.js alone — the server
// half never ships to a browser.

const { ServerSseTransport, SseChannel, SseChannels, SSE_HEADERS, CHANNEL_HEADER } = require('./server.js');
const { ClientSseTransport, SseParser } = require('./client.js');

module.exports = {
  ClientSseTransport,
  SseParser,
  ServerSseTransport,
  SseChannel,
  SseChannels,
  SSE_HEADERS,
  CHANNEL_HEADER,
};
