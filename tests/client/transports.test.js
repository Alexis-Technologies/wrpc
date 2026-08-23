'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { WrpcClient, ClientTransport, isClientTransport } = require('../../src/client.js');
require('../../sse.js'); // registers the sse transport
const { runTransportContract } = require('./transportContract.js');

test('client transports: every registered transport passes the shared contract', (t) => {
  const names = Object.keys(WrpcClient.transport);
  assert.deepStrictEqual([...names].sort(), ['event', 'http', 'sse', 'ws']);
  for (const name of names) runTransportContract(t, name, WrpcClient.transport[name]);
});

test('client transports: the registry is null-prototyped and assigned into', () => {
  // A peer-influenced name must not answer with Object.prototype's function.
  assert.strictEqual(WrpcClient.transport.toString, undefined);
  assert.strictEqual(Object.getPrototypeOf(WrpcClient.transport), null);
});

test('client transports: a malformed registrant is refused with the port-shaped TypeError', async (t) => {
  class Broken {} // no open/close/write
  WrpcClient.transport.broken = Broken;
  t.after(() => delete WrpcClient.transport.broken);
  assert.strictEqual(isClientTransport(Broken), false);
  await assert.rejects(
    WrpcClient.connect('x://host', { transport: 'broken' }),
    (error) => error instanceof TypeError && /ClientTransport contract/.test(error.message),
  );
  await assert.rejects(
    WrpcClient.connect('x://host', { transport: ['broken', 'ws'] }),
    (error) => error instanceof TypeError && /ClientTransport contract/.test(error.message),
  );
  // The bare base class does NOT qualify: open/write are exactly what a
  // subclass exists to provide, and a registrant that forgot them must fail
  // here, not on the first connect.
  assert.strictEqual(isClientTransport(ClientTransport), false);
});
