'use strict';

const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');

const { WebsocketServer } = require('#ws');
const { ProtocolClient } = require('./protocolClient.js');

test('WebsocketServer: accepts new connection after socket error', async () => {
  const httpServer = http.createServer();
  void new WebsocketServer({
    server: httpServer,
    pingInterval: 50,
  });

  await new Promise((resolve) => httpServer.listen(0, resolve));
  const port = httpServer.address().port;

  const first = new ProtocolClient(`ws://localhost:${port}`);
  await new Promise((resolve) => first.on('open', resolve));
  first.socket.destroy();

  await new Promise((resolve) => setTimeout(resolve, 150));

  const second = new ProtocolClient(`ws://localhost:${port}`);
  const opened = await new Promise((resolve) => {
    second.on('open', () => resolve(true));
    second.on('close', () => resolve(false));
  });

  assert.strictEqual(opened, true);
  second.close();
  await new Promise((resolve) => httpServer.close(resolve));
});

test('WebsocketServer: constructor requires an http(s) server instance', () => {
  assert.throws(() => new WebsocketServer({}), /options\.server .* is required/);
  assert.throws(() => new WebsocketServer({ server: {} }), /options\.server .* is required/);
});

test('WebsocketServer: forwards http server errors when it has its own listeners', async () => {
  const httpServer = http.createServer();
  const wsServer = new WebsocketServer({ server: httpServer, pingInterval: 50 });
  await new Promise((resolve) => httpServer.listen(0, resolve));

  const forwarded = new Promise((resolve) => wsServer.once('error', resolve));
  httpServer.emit('error', new Error('boom'));
  const error = await forwarded;
  assert.strictEqual(error.message, 'boom');

  await new Promise((resolve) => httpServer.close(resolve));
});

test('WebsocketServer: does not forward http server errors already handled elsewhere', async () => {
  const httpServer = http.createServer();
  httpServer.on('error', () => {}); // an external listener: httpHasOtherListeners = true
  void new WebsocketServer({ server: httpServer, pingInterval: 50 });
  await new Promise((resolve) => httpServer.listen(0, resolve));

  // wsServer has no 'error' listeners of its own. Node's EventEmitter throws
  // synchronously on an 'error' emit with zero listeners, so if the guard in
  // #init forwarded here anyway, this call would throw.
  assert.doesNotThrow(() => httpServer.emit('error', new Error('already handled')));

  await new Promise((resolve) => httpServer.close(resolve));
});
