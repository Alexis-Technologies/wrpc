'use strict';

// A limit is one comparison, and a comparison with a string or with NaN is
// always false: `maxStreamBuffer: '64mb'` was accepted and lifted the cap in
// silence, while `null` and -1 refused everything. A limit that is none is a
// TypeError where the option is taken.

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { checkLimit } = require('../src/utils.js');
const { RpcServer, WrpcClient, defineRouter, procedure } = require('../index.js');
const { PeerHost } = require('../src/webrtc/index.js');
const { bootServer, connectClient, waitFor } = require('./helpers/server.js');
const { MIB, bytes } = require('./helpers/streams.js');

const router = () => defineRouter({ files: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } });
const WRONG = ['64mb', NaN, -1, null, 1.5, {}, true];
const SERVER_LIMITS = [
  'maxBatch',
  'maxSubscriptions',
  'maxCalls',
  'maxStreams',
  'maxStreamBuffer',
  'streamIdleTimeout',
];

describe('checkLimit', () => {
  it('should accept a non-negative integer or Infinity, and refuse anything else', () => {
    for (const value of [0, 1, 2 ** 40, Infinity]) assert.strictEqual(checkLimit('X', 'limit', value), value);
    for (const value of [...WRONG, undefined, -Infinity]) {
      assert.throws(() => checkLimit('X', 'limit', value), {
        name: 'ConfigError',
        code: 'ALX_ERR_CONFIG',
        message: 'X: limit must be a non-negative integer or Infinity',
      });
    }
  });
});

describe('a limit that is none', () => {
  it('should be refused by RpcServer and PeerHost', () => {
    for (const name of SERVER_LIMITS) {
      for (const value of WRONG) {
        const message = new RegExp(`: ${name} must be a non-negative integer or Infinity$`);
        assert.throws(() => new RpcServer({ router: router(), logger: false, [name]: value }), {
          name: 'ConfigError',
          code: 'ALX_ERR_CONFIG',
          message,
        });
        assert.throws(() => new PeerHost({ router: router(), logger: false, [name]: value }), {
          name: 'ConfigError',
          code: 'ALX_ERR_CONFIG',
          message,
        });
      }
    }
    const rpc = new RpcServer({ router: router(), logger: false, maxStreamBuffer: Infinity, maxStreams: 0 });
    assert.deepStrictEqual([rpc.limits.maxStreamBuffer, rpc.limits.maxStreams], [Infinity, 0]);
  });

  it('should be refused by WrpcClient for a stream limit', async (t) => {
    const { url } = await bootServer(t, { router: router() });
    for (const name of ['maxStreams', 'maxStreamBuffer', 'streamIdleTimeout']) {
      for (const value of WRONG) {
        await assert.rejects(WrpcClient.connect(url, { reconnect: false, heartbeat: false, [name]: value }), {
          name: 'ConfigError',
          code: 'ALX_ERR_CONFIG',
          message: `WrpcClient: ${name} must be a non-negative integer or Infinity`,
        });
      }
    }
    const client = await connectClient(t, url, { maxStreams: 8, maxStreamBuffer: Infinity, streamIdleTimeout: 1000 });
    assert.strictEqual(client.active, true);
  });

  // What the silence cost: a megabyte nobody read, under a "limit" of a megabyte.
  it('should no longer lift the limit when written as a string — it is refused before it can', async (t) => {
    assert.throws(() => new RpcServer({ router: router(), logger: false, maxStreamBuffer: '1mb' }), TypeError);
    const { url } = await bootServer(t, { router: router(), maxStreamBuffer: 1 * MIB });
    const client = await connectClient(t, url);
    client.on('error', () => {});
    const upload = client.createStream('big.bin', 4 * MIB);
    await upload.writeAll(bytes(4 * MIB));
    await waitFor(() => upload.closed, { message: 'the refused upload never closed', timeout: 3000 });
    assert.strictEqual(upload.error.code, 413);
  });
});
