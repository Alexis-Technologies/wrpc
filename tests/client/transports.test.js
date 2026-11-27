'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { WrpcClient, ClientTransport, isClientTransport } = require('../../src/client.js');
const { defineRouter, procedure } = require('../../index.js');
require('../../src/sse/index.js'); // registers the sse transport
require('../../src/webrtc/index.js'); // registers the webrtc transport
require('../../src/broker/index.js'); // registers the broker transport
const { runTransportContract } = require('./transport-contract.js');
const { bootServer, connectClient } = require('../helpers/server.js');

const echoRouter = () =>
  defineRouter({
    echo: { ping: procedure({ access: 'public', handler: async () => 'pong' }) },
  });

describe('client transports', () => {
  it('should register every transport the entries bring', () => {
    const names = Object.keys(WrpcClient.transport);
    assert.deepStrictEqual([...names].sort(), ['broker', 'event', 'http', 'sse', 'webrtc', 'ws', 'wt']);
  });

  for (const name of Object.keys(WrpcClient.transport)) runTransportContract(name, WrpcClient.transport[name]);

  it('should keep the registry null-prototyped and assigned into', () => {
    // A peer-influenced name must not answer with Object.prototype's function.
    assert.strictEqual(WrpcClient.transport.toString, undefined);
    assert.strictEqual(Object.getPrototypeOf(WrpcClient.transport), null);
  });

  it('should refuse a malformed registrant with the port-shaped TypeError', async (t) => {
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

  // A transport named explicitly gets the url as written and the whole
  // connect() options bag in open(): that is how a peer-to-peer transport
  // receives its link (the same way the event transport receives `worker`),
  // and 'webrtc:<peer>' is a scheme mapScheme leaves alone.
  it('should hand a named transport the raw url and the options bag on open()', async (t) => {
    const seen = { urls: [], options: [] };
    class Recording extends ClientTransport {
      constructor(url) {
        super(url);
        seen.urls.push(url);
      }
      async open(options) {
        seen.options.push(options);
        this.active = true;
        this.emit('open');
      }
      close() {
        this.active = false;
        this.emit('close');
      }
      write() {}
    }
    WrpcClient.transport.webrtc = Recording;
    t.after(() => delete WrpcClient.transport.webrtc);
    const link = { token: 'link' };
    const client = await WrpcClient.connect('webrtc:peer-42', { transport: 'webrtc', link, heartbeat: false });
    t.after(() => client.close());
    assert.deepStrictEqual(seen.urls, ['webrtc:peer-42']);
    assert.strictEqual(seen.options[0].link, link);
    assert.strictEqual(seen.options[0].transport, 'webrtc');
  });

  // The http/sse transports must call an injected `fetch`, never the global
  // one — the seam that lets a Node app hand in undici's fetch bound to a
  // tuned Agent/Pool (keep-alive, proxying, a caching interceptor) for
  // server-to-server wrpc traffic without wrpc depending on undici itself.
  for (const transportName of ['http', 'sse']) {
    it(`should have the ${transportName} transport call an injected fetch, not the global one`, async (t) => {
      const { origin } = await bootServer(t, { router: echoRouter() });
      const globalFetch = globalThis.fetch;
      globalThis.fetch = async () => {
        throw new Error('must not reach the global fetch');
      };
      t.after(() => void (globalThis.fetch = globalFetch));
      let calls = 0;
      const injected = (...args) => {
        calls++;
        return globalFetch(...args);
      };
      const client = await connectClient(t, `${origin}/api`, { transport: transportName, fetch: injected });
      await client.load('echo');
      assert.strictEqual(await client.api.echo.ping(), 'pong');
      assert.ok(calls > 0, 'the injected fetch was never called');
    });
  }
  // Both the socket's 'error' and 'close' route to one handler, which emitted
  // what the socket gave it: an Event, never an Error, so a listener read no
  // message and a log line no stack.
  it('should emit an Error that carries the socket event when a ws open fails', async () => {
    const net = require('node:net');
    const port = await new Promise((resolve) => {
      const probe = net.createServer().listen(0, '127.0.0.1', () => {
        const { port: free } = probe.address();
        probe.close(() => resolve(free));
      });
    });
    const { ws: ClientWsTransport } = WrpcClient.transport;
    const transport = new ClientWsTransport(`ws://127.0.0.1:${port}/api`);
    const emitted = [];
    transport.on('error', (error) => void emitted.push(error));
    await assert.rejects(transport.open(), (error) => error instanceof Error && error.cause !== undefined);
    assert.strictEqual(emitted.length, 1);
    assert.ok(emitted[0] instanceof Error, 'the error listener got an Error');
    assert.strictEqual(typeof emitted[0].cause?.type, 'string', 'with the socket event as its cause');
  });
});
