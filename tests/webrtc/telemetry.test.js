'use strict';

// Telemetry on a peer, against the real OpenTelemetry SDK exporting into
// memory (the same rig as tests/telemetry.test.js): server spans for the
// calls a peer answers, linked to the calling peer's client spans through
// the packet's traceparent; the connection gauge under the 'webrtc'
// transport; and the three rtc instruments — open links, redials, ICE
// restarts — over a failure that the redial cycle recovers from.

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

const otelApi = require('@opentelemetry/api');
const { W3CTraceContextPropagator } = require('@opentelemetry/core');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace-base');

const { defineRouter, procedure } = require('../../index.js');
const { SCOPE_NAME } = require('../../src/telemetry/index.js');
const { WrpcPeer, PeerHost } = require('../../src/webrtc/index.js');
const { createFakeRtc } = require('./fake-rtc.js');
const { FakeSignalHub } = require('./fake-signal-hub.js');
const { within, waitFor } = require('./port-contract.js');
const { createMetrics, point } = require('../helpers/metrics.js');

const quiet = {
  log() {},
  info() {},
  warn() {},
  error() {},
  debug() {},
  child() {
    return this;
  },
};

const createTracing = () => {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  return { provider, tracer: provider.getTracer(SCOPE_NAME), spans: () => exporter.getFinishedSpans() };
};

const onceEvent = (emitter, name) => new Promise((resolve) => emitter.once(name, resolve));

describe('telemetry', () => {
  it('should count the verifier\'s outcome in wrpc.rtc.assertions — each refusal by its reason, never all as "invalid"', async (t) => {
    const { createAssertionIssuer, generateAssertionKeys } = require('../../src/webrtc/assertion-issuer.js');
    const metrics = createMetrics();
    t.after(() => metrics.provider.shutdown());
    const telemetry = { meter: metrics.meter };
    const keys = await generateAssertionKeys({ kid: 'hub' });
    const issuer = createAssertionIssuer({ key: keys.privateKey, ttl: 60, issuer: 'hub.test' });
    const fake = createFakeRtc();
    const hub = new FakeSignalHub({ issuer });
    const peers = [];
    t.after(() => {
      for (const peer of peers) peer.close();
      fake.world.close();
    });
    const peer = (id, { signaler = hub.signaler(id), trusted = true } = {}) => {
      const instance = new WrpcPeer({
        router: routerOf(),
        signaler,
        rtc: fake.adapter,
        logger: quiet,
        telemetry,
        ...(trusted ? { assertions: { issuer: 'hub.test' }, host: { trust: 'assertion' } } : {}),
        client: { heartbeat: false, telemetry, reconnect: { minDelay: 5, maxDelay: 20, jitter: false } },
        connectTimeout: 500,
        restartTimeout: 30,
        redial: { minDelay: 5, maxDelay: 20, jitter: false, retries: 1 },
      });
      instance.on('error', () => {});
      peers.push(instance);
      return instance;
    };
    const a = peer('a');
    const refused = async (dialler, label) => {
      await assert.rejects(within(dialler.connect('a'), label), /closed/);
      assert.strictEqual(a.link(dialler.id ?? label), undefined);
    };
    // No token at all.
    await refused(peer('plain', { trusted: false }), 'plain');
    // A key of its own: the signature does not verify.
    const rogue = await generateAssertionKeys({ kid: 'hub' });
    await refused(
      peer('rogue', { signaler: hub.signaler('rogue', { issuer: createAssertionIssuer({ key: rogue.privateKey }) }) }),
      'rogue',
    );
    // A replayed token names another certificate.
    const replay = {
      sign: (claims) => issuer.sign({ ...claims, fp: 'sha-256 ' + 'AA:'.repeat(31) + 'AA' }),
      publicKeys: () => issuer.publicKeys(),
    };
    await refused(peer('replay', { signaler: hub.signaler('replay', { issuer: replay }) }), 'replay');
    // A good one.
    const b = peer('b');
    await within(a.connect('b'), 'b accepted');
    // A token that was valid once. A peer measures the hub's clock from its
    // OWN assertions' iat, set when it first signs — so it has to be the
    // dialler (the lower id), stamping its offer before it verifies the
    // answer: a peer whose issuer reports an iat ten minutes ahead then reads
    // b's fresh answer (ttl 60 s) as expired.
    const ahead = {
      sign: async (claims) => {
        const signed = await issuer.sign(claims);
        return { ...signed, iat: signed.iat + 600 };
      },
      publicKeys: () => issuer.publicKeys(),
    };
    const aa = peer('aa', { signaler: hub.signaler('aa', { issuer: ahead }) });
    await assert.rejects(within(aa.connect('b'), 'aa refuses the answer'), /closed/);
    assert.strictEqual(aa.link('b'), undefined);
    assert.ok(b.link('a'), 'b still has a');
    const exported = await metrics.collect();
    const outcome = (name) =>
      point(exported, 'wrpc.rtc.assertions', (attrs) => attrs['wrpc.rtc.outcome'] === name)?.value ?? 0;
    assert.ok(outcome('missing') >= 1, 'missing');
    assert.ok(outcome('signature') >= 1, 'signature');
    assert.ok(outcome('fingerprint') >= 1, 'fingerprint');
    assert.ok(outcome('expired') >= 1, 'expired');
    assert.ok(outcome('ok') >= 2, 'ok, both ways');
    assert.strictEqual(outcome('invalid'), 0, 'nothing hides behind "invalid"');
  });
});

// What real propagation needs and wrpc deliberately does not supply: an
// active context manager and a W3C propagator, both from the injected api.
const withOtelGlobals = (t) => {
  const manager = new AsyncLocalStorageContextManager().enable();
  otelApi.context.setGlobalContextManager(manager);
  otelApi.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  t.after(() => {
    otelApi.context.disable();
    otelApi.propagation.disable();
  });
};

const routerOf = () =>
  defineRouter({
    calc: {
      add: procedure({ handler: async (_ctx, { a, b }) => a + b }),
      boom: procedure({
        handler: async () => {
          throw Object.assign(new Error('nope'), { code: 418 });
        },
      }),
    },
  });

describe('peer telemetry', () => {
  // One link for the five cases, which run in order: each reads what the ones
  // before it left (the spans of the setup's calls, the failure, the goodbye).
  // The setup and its teardown are the suite's; withOtelGlobals takes anything
  // with an after(), and gets the suite's list.
  describe('spans on both ends of a link, joined by the traceparent', () => {
    const teardown = [];
    let tracing = null;
    let metrics = null;
    let fake = null;
    let hub = null;
    let telemetry = null;
    let ab = null;
    let b = null;

    const peer = (id) =>
      new WrpcPeer({
        router: routerOf(),
        signaler: hub.signaler(id),
        rtc: fake.adapter,
        logger: quiet,
        telemetry,
        client: { heartbeat: false, telemetry, reconnect: { minDelay: 5, maxDelay: 20, jitter: false } },
        connectTimeout: 500,
        restartTimeout: 30,
        // A redial window wide enough for the metric collects between the
        // failure and the unmute to finish first: the recovery below must be
        // the first redial, not a later one racing the assertions.
        redial: { minDelay: 150, maxDelay: 150, jitter: false, retries: 5 },
      });

    before(async () => {
      withOtelGlobals({ after: (fn) => teardown.push(fn) });
      tracing = createTracing();
      metrics = createMetrics();
      teardown.push(() => metrics.provider.shutdown());
      telemetry = {
        tracer: tracing.tracer,
        meter: metrics.meter,
        propagation: otelApi.propagation,
        context: otelApi.context,
      };
      fake = createFakeRtc();
      hub = new FakeSignalHub();
      const a = peer('a');
      b = peer('b');
      teardown.push(() => {
        a.close();
        b.close();
        fake.world.close();
      });
      ab = await within(a.connect('b'), 'open');
      await within(b.link('a').ready(), 'open');
      await ab.load('calc');
      assert.strictEqual(await ab.api.calc.add({ a: 2, b: 3 }), 5);
      await assert.rejects(ab.api.calc.boom(), (error) => error.code === 418);
    });

    after(async () => {
      for (const fn of teardown) await fn();
    });

    it('should give the call a peer answered a SERVER span under the webrtc transport', () => {
      const server = tracing.spans().find((span) => span.name === 'calc/add' && span.kind === 1);
      assert.ok(server, 'b traced the call it answered');
      assert.strictEqual(server.attributes['wrpc.status'], 'ok');
      assert.strictEqual(server.attributes['wrpc.transport'], 'webrtc');
      assert.strictEqual(server.attributes['rpc.service'], 'calc');
      const failed = tracing.spans().find((span) => span.name === 'calc/boom' && span.kind === 1);
      assert.strictEqual(failed.attributes['wrpc.status'], 'error');
      assert.strictEqual(failed.attributes['rpc.wrpc.status_code'], 418);
    });

    it("should make the calling peer's CLIENT span the server span's parent", () => {
      const client = tracing.spans().find((span) => span.name === 'calc/add' && span.kind === 2);
      const server = tracing.spans().find((span) => span.name === 'calc/add' && span.kind === 1);
      assert.ok(client, 'a traced the call it made');
      assert.strictEqual(server.spanContext().traceId, client.spanContext().traceId, 'one trace across the link');
      assert.strictEqual(server.parentSpanContext?.spanId ?? server.parentSpanId, client.spanContext().spanId);
    });

    it('should count each direction of the link as a webrtc connection in the connection gauge', async () => {
      const exported = await metrics.collect();
      const connections = point(exported, 'wrpc.server.connections', (attrs) => attrs['wrpc.transport'] === 'webrtc');
      assert.ok(connections, 'the gauge has a webrtc series');
      // One writer per peer, two peers sharing one meter: a's host holds b's
      // client and b's host holds a's — two live connections.
      assert.strictEqual(connections.value, 2);
      const links = point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'initiator');
      const responder = point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'responder');
      assert.strictEqual(links.value, 1);
      assert.strictEqual(responder.value, 1);
    });

    it('should count an ICE restart and a redial on a failure, and drop the gauge until the link is back', async () => {
      hub.mute('a');
      hub.mute('b');
      ab.link.pc.failIce();
      await waitFor(() => ab.link.state === 'failed', 'the restart failed');
      // Each end gives up on its own restart timer, and the responder's can
      // land after the initiator's link reads `failed` — on a slow runner it
      // did, and the count read 1. The count is waited for, not read once.
      let exported = null;
      const failedRestarts = () =>
        point(exported, 'wrpc.rtc.ice_restarts', (attrs) => attrs['wrpc.rtc.outcome'] === 'failed')?.value;
      await waitFor(async () => {
        exported = await metrics.collect();
        return failedRestarts() === 2;
      }, 'both ends counted their failed restart');
      assert.strictEqual(point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'initiator').value, 0);
      assert.strictEqual(
        point(exported, 'wrpc.rtc.ice_restarts', (attrs) => attrs['wrpc.rtc.outcome'] === 'requested').value,
        2,
      );
      assert.strictEqual(
        point(exported, 'wrpc.rtc.ice_restarts', (attrs) => attrs['wrpc.rtc.outcome'] === 'failed').value,
        2,
      );
      assert.strictEqual(
        point(exported, 'wrpc.server.connections', (attrs) => attrs['wrpc.transport'] === 'webrtc').value,
        0,
      );
      const reconnected = onceEvent(ab, 'reconnect');
      hub.unmute('a');
      hub.unmute('b');
      await within(reconnected, 'redialled');
      await waitFor(() => b.link('a').state === 'open', 'the responder followed');
      exported = await metrics.collect();
      assert.strictEqual(point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'initiator').value, 1);
      assert.ok(point(exported, 'wrpc.rtc.redials', (attrs) => attrs['wrpc.rtc.role'] === 'initiator').value >= 1);
      // Counted where a knock actually went out — a responder rebuilt by the
      // initiator's offer before its own timer fired knocked never, and used
      // to be counted anyway.
      const knocks = hub.sent.filter((entry) => entry.from === 'b' && entry.type === 'connect').length;
      const responder = point(exported, 'wrpc.rtc.redials', (attrs) => attrs['wrpc.rtc.role'] === 'responder');
      assert.strictEqual(responder?.value ?? 0, knocks);
      assert.strictEqual(
        point(exported, 'wrpc.server.connections', (attrs) => attrs['wrpc.transport'] === 'webrtc').value,
        2,
      );
    });

    it('should take the link out of the gauge exactly once on a goodbye, and count it by reason on both ends', async () => {
      ab.close();
      await waitFor(() => b.links.size === 0, 'b saw the goodbye');
      const exported = await metrics.collect();
      assert.strictEqual(point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'initiator').value, 0);
      assert.strictEqual(point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'responder').value, 0);
      const closes = (reason, side) =>
        point(
          exported,
          'wrpc.rtc.closes',
          (attrs) => attrs['wrpc.rtc.reason'] === reason && attrs['wrpc.rtc.side'] === side,
        );
      assert.strictEqual(closes('goodbye', 'local').value, 1, "a's own goodbye");
      assert.strictEqual(closes('goodbye', 'remote').value, 1, 'b heard it');
      assert.strictEqual(closes('gave-up', 'local'), undefined, 'the redial above recovered: nothing gave up');
      assert.strictEqual(
        point(exported, 'wrpc.server.connections', (attrs) => attrs['wrpc.transport'] === 'webrtc').value,
        0,
      );
    });
  });

  it('should still count the links of a client-only peer, and expose the writer of a bare PeerHost', async (t) => {
    const metrics = createMetrics();
    t.after(() => metrics.provider.shutdown());
    const fake = createFakeRtc();
    const hub = new FakeSignalHub();
    const lone = new WrpcPeer({
      signaler: hub.signaler('lone'),
      rtc: fake.adapter,
      logger: quiet,
      telemetry: { meter: metrics.meter },
      client: { heartbeat: false },
    });
    const served = new WrpcPeer({
      router: routerOf(),
      signaler: hub.signaler('served'),
      rtc: fake.adapter,
      logger: quiet,
      client: { heartbeat: false },
    });
    t.after(() => {
      lone.close();
      served.close();
      fake.world.close();
    });
    await within(lone.connect('served'), 'open');
    const exported = await metrics.collect();
    assert.strictEqual(point(exported, 'wrpc.rtc.links', (attrs) => attrs['wrpc.rtc.role'] === 'initiator').value, 1);
    assert.strictEqual(point(exported, 'wrpc.server.connections'), undefined, 'no host, no connections');
    assert.strictEqual(served.host.otel.enabled, false, 'nothing injected: the disabled writer');
    const host = new PeerHost({ router: routerOf(), logger: quiet, telemetry: { meter: metrics.meter } });
    assert.strictEqual(host.otel.enabled, true);
  });
});
