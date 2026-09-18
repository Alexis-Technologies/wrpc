'use strict';

// The `generateId` contract, in one table.
//
// Three call-site spellings had drifted apart before this suite existed —
// one threw, one fell back silently, one assigned only when the value
// happened to be a function — so a mistyped option behaved differently
// depending on which constructor received it. The table below is what makes
// "one canonical resolver" a fact rather than an intention: every
// constructor that takes the option answers the same five cases the same
// way, modulo the one deliberate difference (strict vs. reported).

const test = require('node:test');
const assert = require('node:assert');

const { RpcServer, defineRouter, procedure, WrpcClient, Emitter } = require('../../index.js');
const { ClientTransport } = require('../../src/client/core.js');
const { SseChannels } = require('../../src/sse/server.js');
const { MemoryBroker } = require('../../broker.js');
const { PeerHost, wrpcSignaler } = require('../../webrtc.js');
const { bootServer, connectClient } = require('../helpers/server.js');

const router = defineRouter({
  unit: { ping: procedure({ access: 'public', handler: async () => 'pong' }) },
});

/** Collects error entries so the reported mode can be asserted, not merely survived. */
const recorder = () => {
  const entries = [];
  const writer = {
    level: 'debug',
    child() {
      return this;
    },
    log() {},
    info() {},
    debug() {},
    warn() {},
    error(entry) {
      entries.push(entry);
    },
  };
  return { entries, writer };
};

// A client the signaler will accept without a server behind it — the same
// shape tests/webrtc/signaler.test.js drives it with.
class FakeClient extends Emitter {
  api = {};
  use(introspection) {
    for (const unit of Object.keys(introspection)) this.api[unit] ??= new Emitter();
    return this;
  }
  async call() {
    return {};
  }
  sendEvent() {}
}

// `mode` is the only axis the constructors differ on: 'strict' throws on a
// bad generator, 'reported' logs it and falls back. Options added since 1.0
// are strict; the three that shipped in it cannot become strict before 2.0
// without breaking the stability policy.
const CONSTRUCTORS = [
  {
    label: 'RpcServer',
    mode: 'reported',
    build: (generateId, logger) => new RpcServer({ router, generateId, logger }),
  },
  {
    label: 'WrpcClient',
    mode: 'reported',
    // The transport is the second POSITIONAL argument; an unconnected one
    // is enough, because the option is resolved in the constructor.
    build: (generateId, logger) =>
      new WrpcClient('http://127.0.0.1:1/', new ClientTransport('http://127.0.0.1:1/'), { generateId, logger }),
  },
  {
    label: 'PeerHost',
    mode: 'reported',
    build: (generateId, logger) => new PeerHost({ router, generateId, logger }),
  },
  {
    label: 'SseChannels',
    mode: 'strict',
    build: (generateId) => new SseChannels({ addClient: () => {}, generateId }),
  },
  {
    label: 'MemoryBroker',
    mode: 'strict',
    build: (generateId) => new MemoryBroker({ generateId, logger: false }),
  },
  {
    label: 'wrpcSignaler',
    mode: 'strict',
    build: (generateId) => wrpcSignaler(new FakeClient(), { generateId }),
  },
];

// Every rejected shape, and why it must not reach the wire: a number keys a
// Map that a string id looks up, an empty id makes two packets collide, and
// anything past 255 overflows the binary chunk header's one-byte length.
const REJECTED = [
  ['not a function', 'nope'],
  ['a function answering a number', () => 42],
  ['a function answering an empty string', () => ''],
  ['a function answering 256 characters', () => 'x'.repeat(256)],
  [
    'a function that throws',
    () => {
      throw new Error('boom');
    },
  ],
];

for (const { label, mode, build } of CONSTRUCTORS) {
  test(`generateId: ${label} accepts a valid generator and defaults without one`, () => {
    const { writer } = recorder();
    assert.doesNotThrow(() => build(undefined, writer), 'omitted means uuid v4');
    assert.doesNotThrow(() => build(() => `id-${Math.random().toString(36).slice(2)}`, writer));
  });

  for (const [what, value] of REJECTED) {
    test(`generateId: ${label} refuses ${what}`, () => {
      const { entries, writer } = recorder();
      if (mode === 'strict') {
        assert.throws(() => build(value, writer), new RegExp(`^TypeError: ${label}: generateId `));
        return;
      }
      // Reported: an observability-shaped mistake must not stop a server
      // booting or a client connecting, so it lands in the log instead.
      assert.doesNotThrow(() => build(value, writer));
      assert.strictEqual(entries.length, 1, 'exactly one entry, not one per id');
      assert.strictEqual(entries[0].event, 'options.generateId');
      assert.ok(entries[0].err instanceof TypeError);
      assert.match(entries[0].err.message, new RegExp(`^${label}: generateId `));
    });
  }
}

test('generateId: one generator feeds every id an RpcServer mints', async (t) => {
  let n = 0;
  const { server, url, origin } = await bootServer(t, {
    router,
    generateId: () => `x${++n}`,
    sse: {},
  });
  const rpc = server.rpc;
  // The probe's id IS the instanceId, so nothing is minted and discarded.
  assert.strictEqual(rpc.instanceId, 'x1');
  const client = await connectClient(t, url);
  await client.load('unit');
  assert.strictEqual(await client.api.unit.ping(), 'pong');
  const [connected] = [...rpc.clients];
  assert.match(connected.id, /^x1\.x\d+$/, 'client ids carry the instance prefix and the generator');
  // The SSE channel id is minted by the same generator — it used to be the
  // one core id an application could not control. It is announced in the
  // stream's first event, never in a header.
  const stream = await fetch(`${origin}${rpc.basePath}/events`, { headers: { accept: 'text/event-stream' } });
  const reader = stream.body.getReader();
  const { value } = await reader.read();
  const ready = new TextDecoder().decode(value);
  await reader.cancel();
  const channel = JSON.parse(/^data: (.*)$/m.exec(ready)[1]).channel;
  assert.match(channel, /^x\d+$/, 'the SSE channel id comes from the injected generator');
});

test('generateId: an instance id may not contain a dot, whoever produced it', () => {
  assert.throws(
    () => new RpcServer({ router, generateId: () => 'a.b', logger: false }),
    /RpcServer: generateId must not return "\."/,
    'the message names the option that actually produced the id',
  );
  assert.throws(
    () => new RpcServer({ router, instanceId: 'a.b', logger: false }),
    /RpcServer: options\.instanceId must not contain "\."/,
  );
});

test('generateId: a broker transport mints its session id with the client generator', () => {
  const { ClientBrokerTransport } = require('../../broker.js');
  const transport = new ClientBrokerTransport('broker://billing');
  // Assigned by the owning client the way `codec` and `log` are, so one
  // option covers packet ids and the broker session id alike.
  assert.strictEqual(typeof transport.generateId, 'function');
  transport.generateId = () => 'session-1';
  assert.strictEqual(transport.generateId(), 'session-1');
});
