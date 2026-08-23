'use strict';

// The boot lines every server-driving suite used to copy: an http Server on
// a free port with quiet logs, torn down through t.after the moment it
// resolves (never as a trailing await — a failing assertion before an
// un-hooked close wedges the run, and with a uws engine it wedges it with a
// native listen socket). Not a *.test.js: node --test must not run helpers.

const timers = require('node:timers/promises');
const assert = require('node:assert');

const { Server, WrpcClient } = require('../../index.js');

/** Boots a Server on 127.0.0.1:0 and registers its teardown. */
const bootServer = async (t, options = {}) => {
  const server = new Server({
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 50 },
    ...options,
  });
  await server.listen();
  t.after(() => server.close());
  const { port } = server.address();
  return { server, port, url: `ws://127.0.0.1:${port}${server.rpc.basePath}`, origin: `http://127.0.0.1:${port}` };
};

/** Connects a WrpcClient (no heartbeat, no reconnect) and registers its close. */
const connectClient = async (t, url, options = {}) => {
  const client = await WrpcClient.connect(url, { heartbeat: false, reconnect: false, ...options });
  t.after(() => void client.close());
  return client;
};

/** Polls `predicate` every 5ms; fails the test after ~1.5s. */
const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 300; i++) {
    if (await predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

module.exports = { bootServer, connectClient, waitFor };
