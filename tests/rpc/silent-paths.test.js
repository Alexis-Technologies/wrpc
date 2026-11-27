'use strict';

// The paths that used to fail without saying so.
//
// Each of these was a real report waiting to happen: a call refused with no
// line, a connection that "just disconnects", a user silently signed out, a
// resume token quietly rejected. The assertions are on the `event` name,
// because that is the field docs/guide/logging.md tells operators to build
// alerts on — a line with the wrong event is as good as no line.

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { defineRouter, procedure } = require('../../index.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');
const { recorder } = require('../helpers/recorder.js');

const router = defineRouter({
  unit: {
    ping: procedure({ access: 'public', handler: async () => 'pong' }),
  },
});

describe('silent paths', () => {
  describe('dispatcher', () => {
    it('should say so when a call is for a method that does not exist', async (t) => {
      const { writer, find } = recorder();
      const { url } = await bootServer(t, { router, logger: writer });
      const client = await connectClient(t, url);
      await assert.rejects(() => client.call('unit/nope', {}));
      await waitFor(() => find('call.unknown'), 'the 404 never reached the log');
      const entry = find('call.unknown');
      assert.strictEqual(entry.level, 'warn');
      assert.strictEqual(entry.code, 404);
      assert.strictEqual(entry.method, 'unit/nope', 'the method is in the log even though the metric says <unknown>');
    });

    it('should log the capacity refusal at debug, not warn', async (t) => {
      const slow = defineRouter({
        unit: {
          wait: procedure({
            access: 'public',
            handler: () => new Promise((resolve) => setTimeout(() => resolve(1), 200)),
          }),
        },
      });
      const { writer, find, loud } = recorder();
      const { url } = await bootServer(t, { router: slow, logger: writer, maxCalls: 2 });
      const client = await connectClient(t, url);
      const calls = [client.call('unit/wait', {}), client.call('unit/wait', {}), client.call('unit/wait', {})];
      await Promise.allSettled(calls);
      const entry = find('call.capacity');
      assert.ok(entry, 'the 429 never reached the log');
      // Any peer can drive this in a loop without authenticating, so warning on
      // it would turn a refused flood into a log flood.
      assert.strictEqual(entry.level, 'debug');
      assert.strictEqual(entry.code, 429);
      // The answer going out is a debug trace too — not an error-level twin
      // that would have made the choice above moot.
      assert.strictEqual(find('rpc.error').level, 'debug');
      assert.deepStrictEqual(loud(), []);
    });

    it('should report a packet of an unknown type, like a malformed one', async (t) => {
      const { writer, find } = recorder();
      const { server, origin } = await bootServer(t, { router, logger: writer });
      // Structurally valid JSON that is not a packet this protocol has — a
      // version skew, or somebody else's client pointed at this port. Malformed
      // FRAMES already logged; this half of the same funnel did not.
      const res = await fetch(`${origin}${server.rpc.basePath}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ type: 'nonsense', id: 'a' }),
      });
      await res.json();
      const entry = find('packet.unknown');
      assert.ok(entry, 'an unknown packet type stayed silent');
      assert.strictEqual(entry.level, 'warn');
      assert.strictEqual(entry.type, 'nonsense');
    });

    it('should report an oversize batch at debug', async (t) => {
      const { writer, find, loud } = recorder();
      const { server, origin } = await bootServer(t, { router, logger: writer, maxBatch: 2 });
      const batch = [1, 2, 3].map((n) => ({ type: 'call', id: `c${n}`, method: 'unit/ping', args: {} }));
      await fetch(`${origin}${server.rpc.basePath}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(batch),
      });
      const entry = find('batch.refused');
      assert.ok(entry, 'the oversize batch stayed silent');
      assert.strictEqual(entry.level, 'debug');
      assert.strictEqual(entry.size, 3);
      assert.strictEqual(entry.max, 2);
      assert.strictEqual(find('rpc.error').level, 'debug');
      assert.deepStrictEqual(loud(), []);
    });

    it('should log a malformed frame as one warn, not three lines', async (t) => {
      const { writer, find, loud, entries } = recorder();
      const { server, origin } = await bootServer(t, { router, logger: writer });
      const res = await fetch(`${origin}${server.rpc.basePath}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{"type":"call",',
      });
      const answer = await res.json();
      assert.strictEqual(answer.type, 'callback');
      assert.strictEqual(answer.error.code, 500);
      // The funnel line, and only it: the empty packet it fell through as used
      // to be reported again as `packet.unknown`, and the answer at error.
      assert.strictEqual(find('packet.malformed').level, 'warn');
      assert.strictEqual(find('packet.unknown'), undefined);
      assert.strictEqual(find('rpc.error').level, 'debug');
      assert.strictEqual(loud().length, 1, JSON.stringify(entries));
    });

    it('should clip the peer strings in a refusal before they become a line', async (t) => {
      const { writer, find } = recorder();
      const { url } = await bootServer(t, { router, logger: writer });
      const client = await connectClient(t, url);
      const method = `unit/${'m'.repeat(4000)}`;
      await assert.rejects(() => client.call(method, {}));
      await waitFor(() => find('call.unknown'), 'the 404 never reached the log');
      const entry = find('call.unknown');
      assert.strictEqual(entry.method.length, 129);
      assert.ok(entry.method.endsWith('…'));
      assert.strictEqual(entry.method.slice(0, 128), method.slice(0, 128));
      // The answer's line is a debug trace of the same refusal, id intact.
      const trace = find('rpc.error');
      assert.strictEqual(trace.level, 'debug');
      assert.strictEqual(trace.id, entry.id);
    });
  });

  describe('sessions', () => {
    it('should not take the process down when a store cannot delete', async () => {
      const { SessionManager } = require('../../src/rpc/sessions.js');
      const { writer, find } = recorder();
      const store = {
        async get() {
          return null;
        },
        async set() {},
        async delete() {
          throw new Error('redis is down');
        },
      };
      const sessions = new SessionManager({ store }, writer);
      // initializeSession() calls finalizeSession() through `void`, so a
      // rejection here used to become an unhandled rejection — a store blip
      // ending the server rather than one session.
      await assert.doesNotReject(() => sessions.destroy('token'));
      const entry = find('session.destroy');
      assert.ok(entry, 'the failed delete stayed silent');
      assert.strictEqual(entry.level, 'error');
      assert.strictEqual(entry.err.message, 'redis is down');
    });

    it('should log once, with a count, when evicting live sessions for capacity', () => {
      const { MemorySessionStore } = require('../../src/rpc/sessions.js');
      const { entries, writer, find } = recorder();
      const store = new MemorySessionStore({ maxSessions: 2, logger: writer });
      for (const token of ['a', 'b', 'c', 'd']) store.set(token, { user: token });
      const evictions = entries.filter((entry) => entry.event === 'session.evict');
      assert.ok(find('session.evict'), 'a live session was dropped silently');
      assert.strictEqual(find('session.evict').level, 'warn');
      // One line per sweep carrying a count — a store pinned at its ceiling
      // evicts on every create, and a line each would be the flood.
      assert.ok(
        evictions.every((entry) => typeof entry.evicted === 'number' && entry.evicted > 0),
        'every eviction line carries its count',
      );
    });
  });

  describe('logging', () => {
    it('should carry an event on every entry the server writes', async (t) => {
      // The rule that keeps the catalogue in docs/guide/logging.md true. It was
      // broken exactly once, in the redis backplane, where entries went out as
      // `{ err, component }` and so could not be alerted on alongside the rest
      // — invisible until somebody went looking. An assertion is cheaper than
      // looking.
      const offenders = [];
      const assertEvent = (level) => (entry) => {
        if (typeof entry?.event !== 'string' || entry.event.length === 0) {
          offenders.push({ level, entry });
        }
      };
      const strict = {
        level: 'debug',
        child() {
          return this;
        },
        log: assertEvent('log'),
        info: assertEvent('info'),
        debug: assertEvent('debug'),
        warn: assertEvent('warn'),
        error: assertEvent('error'),
      };

      const wide = defineRouter({
        unit: {
          ping: procedure({ access: 'public', handler: async () => 'pong' }),
          guarded: procedure({ access: 'session', handler: async () => 'secret' }),
        },
      });
      const { server, url, origin } = await bootServer(t, { router: wide, logger: strict, sse: {}, maxBatch: 2 });
      const client = await connectClient(t, url);

      // Drive the paths that log: a good call, a 404, a 403, an oversize batch,
      // an unknown packet and a malformed frame.
      await client.load('unit');
      assert.strictEqual(await client.api.unit.ping(), 'pong');
      await assert.rejects(() => client.call('unit/nope', {}));
      await assert.rejects(() => client.call('unit/guarded', {}));
      const post = (body) =>
        fetch(`${origin}${server.rpc.basePath}`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body,
        }).then((res) => res.text());
      await post(JSON.stringify([1, 2, 3].map((n) => ({ type: 'call', id: `c${n}`, method: 'unit/ping', args: {} }))));
      await post(JSON.stringify({ type: 'nonsense' }));
      await post('{not json');
      await fetch(`${origin}${server.rpc.basePath}/does-not-exist`);

      assert.deepStrictEqual(offenders, [], 'every log entry must carry a non-empty `event`');
    });
  });

  describe('scaling', () => {
    it('should report a corrupt stored session, and never log the token', async () => {
      const { createRedisSessionStore } = require('../../src/scaling/index.js');
      const { writer, find } = recorder();
      const client = {
        async get() {
          return 'not json{{';
        },
        async set() {},
        async del() {},
      };
      const store = createRedisSessionStore({ client, logger: writer });
      assert.strictEqual(await store.get('a-real-session-token'), null);
      const entry = find('session.corrupt');
      assert.ok(entry, 'a silently signed-out user left no trace');
      assert.strictEqual(entry.level, 'warn');
      // The token is the credential. A log pipeline is not where it goes.
      assert.ok(!JSON.stringify(entry).includes('a-real-session-token'), 'the token must not reach the log');
    });
  });

  describe('broker', () => {
    it('should warn about a resume token that fails its signature, by reason', async () => {
      const { MemoryBroker, brokerFeed } = require('../../src/broker/index.js');
      const broker = new MemoryBroker({ logger: false });
      const { writer, find } = recorder();
      const feed = brokerFeed(broker, 'topic', { secret: 'shhh' });
      const context = { log: writer };
      // A tampered token: not merely stale, but one whose HMAC does not check
      // out — someone reading a topic from an offset they invented. It used to
      // be indistinguishable from a garbled one, and silent.
      const iterator = feed(context, {}, { lastEventId: 'forged!deadbeef' });
      await assert.rejects(() => iterator.next(), { name: 'WrpcError', code: 400, message: 'Invalid event id' });
      const entry = find('broker.feed.resume');
      assert.ok(entry, 'a forged resume token was refused silently');
      assert.strictEqual(entry.reason, 'signature');
      assert.strictEqual(entry.level, 'warn');
      // The id itself is peer-controlled text and stays out of the entry.
      assert.ok(!JSON.stringify(entry).includes('forged!deadbeef'));
      await broker.close();
    });
  });

  describe('http', () => {
    it('should make what throws while a request is routed one error line and one 500', async () => {
      const { RpcServer } = require('../../index.js');
      const { writer, all } = recorder();
      const rpc = new RpcServer({
        router,
        logger: writer,
        querystring: {
          parse() {
            throw new Error('parser bug');
          },
        },
      });
      const answers = [];
      const call = (extra = {}) => ({
        method: 'GET',
        url: '/api/unit/ping?a=1',
        headers: {},
        respond: (response) => answers.push(response.status),
        ...extra,
      });
      await rpc.handleHttpCall(call());
      assert.deepStrictEqual(answers, [500], 'answered once');
      const [entry] = all('http.failed');
      assert.strictEqual(entry.level, 'error');
      assert.strictEqual(entry.err.message, 'parser bug');

      await rpc.close();

      // A response the host already started streaming is not answered again:
      // the throw comes from the host's own stream(), after the core began.
      const plain = new RpcServer({ router, logger: writer });
      answers.length = 0;
      await plain.handleHttpCall(
        call({
          url: '/api/events',
          headers: { accept: 'text/event-stream' },
          stream() {
            throw new Error('host refused the stream');
          },
        }),
      );
      assert.deepStrictEqual(answers, [], 'nothing written over a started stream');
      assert.strictEqual(all('http.failed').length, 2);
      await plain.close();
    });
  });

  describe('dispatcher: refused subscriptions and streams', () => {
    it('should name a refused subscription by its id clipped, never the peer text whole', async (t) => {
      const { writer, find } = recorder();
      const feeds = defineRouter({
        u: { feed: procedure.subscription({ access: 'public', handler: async function* () {} }) },
      });
      const { url } = await bootServer(t, { router: feeds, logger: writer });
      const http = url.replace(/^ws/, 'http');
      // Over HTTP a subscription is refused (400): its line carries the id.
      const res = await fetch(http, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'subscribe', id: 'x'.repeat(255), method: 'u/feed' }),
      });
      await res.text();
      await waitFor(() => find('subscribe.refused'), 'the refusal reached the log');
      const entry = find('subscribe.refused');
      assert.ok(entry.id.length <= 129, `clipped (${entry.id.length})`);
      assert.ok(entry.message === undefined || entry.message.length < 200);
    });

    it('should answer a stream packet past maxStreams with 429, and say so at debug', async (t) => {
      const { writer, find } = recorder();
      const { url } = await bootServer(t, { router, logger: writer, maxStreams: 2 });
      const socket = new WebSocket(url, ['wrpc.v2']);
      t.after(() => socket.close());
      const answers = [];
      socket.addEventListener('message', ({ data }) => answers.push(JSON.parse(data)));
      await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));
      // An unauthenticated peer announced 20 000 streams and the server held them all.
      for (let i = 1; i <= 3; i++) socket.send(JSON.stringify({ type: 'stream', id: `s${i}`, name: 'blob', size: 10 }));
      await waitFor(() => answers.length === 1, 'the third is refused');
      assert.strictEqual(answers[0].id, 's3');
      assert.strictEqual(answers[0].error.code, 429);
      assert.strictEqual(find('stream.capacity').level, 'debug');
      // Ending one frees a slot (a round trip later: the end is asynchronous).
      socket.send(JSON.stringify({ type: 'stream', id: 's1', status: 'terminate' }));
      socket.send(JSON.stringify({ type: 'call', id: 'c0', method: 'unit/ping', args: {} }));
      await waitFor(() => answers.some((answer) => answer.id === 'c0'), 'the call after the end');
      socket.send(JSON.stringify({ type: 'stream', id: 's4', name: 'blob', size: 10 }));
      socket.send(JSON.stringify({ type: 'call', id: 'c1', method: 'unit/ping', args: {} }));
      await waitFor(() => answers.some((answer) => answer.id === 'c1'), 'the call after the fourth');
      assert.ok(!answers.some((answer) => answer.id === 's4'), 'the fourth fits once one ended');
    });
  });

  describe("a peer's text in a log line", () => {
    // --- A peer's text never ends or splits a log line ----------------------------

    it('should keep a line break or a tab in a peer string out of a dispatcher log line, and bound a long one', async (t) => {
      // A Console-shaped logger: what prints the tab-separated message as it is.
      const printed = [];
      const print = (...parts) => void printed.push(parts.map(String).join(' '));
      const logger = { debug: print, info: print, log: print, warn: print, error: print };
      const { url } = await bootServer(t, { router, logger });
      const socket = new WebSocket(url);
      t.after(() => socket.close());
      await new Promise((resolve) => socket.addEventListener('open', resolve));
      const forged = 'unit/nope\n2026-11-16 ERROR admin logged in\tfrom 10.0.0.1';
      socket.send(JSON.stringify({ type: 'call', id: 'c\n1', method: forged, args: {} }));
      socket.send(JSON.stringify({ type: 'subscribe', id: 's\n1', method: forged, args: {} }));
      socket.send(JSON.stringify({ type: 'event', name: `unit/${'e'.repeat(5000)}\nforged`, data: {} }));
      const fromPeer = () => printed.filter((text) => text.includes('admin logged in') || text.includes('eeee'));
      await waitFor(() => fromPeer().length >= 3, 'the three refusals were written');
      for (const text of fromPeer()) {
        // The line's own tabs are its columns; none of them may come from the
        // peer, so the forged text sits in one column, on one line.
        assert.ok(!text.includes('\n'), `a line break reached the log: ${JSON.stringify(text.slice(0, 80))}`);
        assert.ok(!text.includes('ERROR admin logged in\tfrom'), 'a tab of the peer moved a column');
        assert.ok(text.length < 400, `${text.length} characters of a peer's text in one line`);
      }
    });

    it('should bound a string with clip, turn every control character into a space, and pass anything else through', () => {
      const { clip } = require('../../src/rpc/errors.js');
      assert.strictEqual(clip('a\nb\tc\r\u0000d\u2028e\u2029f'), 'a b c  d e f');
      assert.strictEqual(clip('x'.repeat(200)).length, 129);
      assert.strictEqual(clip('x\ny', 2), 'x …');
      assert.strictEqual(clip(42), 42);
      assert.strictEqual(clip(undefined), undefined);
    });
  });

  describe('dispatcher: a refusal repeated in a loop', () => {
    // --- What a peer can do in a loop is one warning, then debug --------------------

    it('should make the first refusal of a kind a warning, and the ones behind it debug', async (t) => {
      const { writer, all } = recorder();
      const slow = defineRouter({
        unit: {
          ping: procedure({ access: 'public', handler: async () => 'pong' }),
          slow: procedure({ access: 'public', handler: () => new Promise((resolve) => setTimeout(resolve, 200)) }),
          locked: procedure({ handler: async () => 'never' }),
          capped: procedure({ access: 'public', timeout: 20, handler: () => new Promise(() => {}) }),
        },
      });
      const { url } = await bootServer(t, { router: slow, logger: writer });
      const socket = new WebSocket(url);
      t.after(() => socket.close());
      const answers = [];
      socket.addEventListener('message', (event) => answers.push(JSON.parse(String(event.data))));
      await new Promise((resolve) => socket.addEventListener('open', resolve));
      const call = (id, method, extra = {}) =>
        socket.send(JSON.stringify({ type: 'call', id, method, args: {}, ...extra }));
      for (let i = 0; i < 3; i++) call(`u${i}`, 'unit/nope');
      for (let i = 0; i < 3; i++) call(`f${i}`, 'unit/locked');
      // A deadline of the caller's own, shorter than anything the procedure set.
      for (let i = 0; i < 3; i++) call(`d${i}`, 'unit/slow', { timeout: 5 });
      for (let i = 0; i < 3; i++) socket.send('{not json');
      call('own', 'unit/capped');
      await waitFor(() => answers.length === 13, 'every packet is answered');
      assert.deepStrictEqual(
        all('call.unknown').map((entry) => entry.level),
        ['warn', 'debug', 'debug'],
      );
      assert.deepStrictEqual(
        all('packet.malformed').map((entry) => entry.level),
        ['warn', 'debug', 'debug'],
      );
      const answersOf = (code) => all('rpc.error').filter((entry) => entry.code === code);
      // 403: the calls that needed a session. 408: the caller's deadlines — and
      // the one the PROCEDURE's own timeout cut, which stays an error.
      assert.deepStrictEqual(
        answersOf(403).map((entry) => entry.level),
        ['warn', 'debug', 'debug'],
      );
      const timeouts = answersOf(408);
      assert.deepStrictEqual(
        timeouts.filter((entry) => entry.id !== 'own').map((entry) => entry.level),
        ['warn', 'debug', 'debug'],
      );
      assert.strictEqual(timeouts.find((entry) => entry.id === 'own').level, 'error');
    });
  });

  describe('RpcServer cors option', () => {
    // --- A mistyped cors option is said, not ignored ------------------------------

    it('should refuse cors.origin, warn of an unknown cors key, and throw a TypeError on a bad origins', () => {
      const { RpcServer } = require('../../src/rpc/core.js');
      const build = (cors, logger = false) => new RpcServer({ router, logger, cors });
      // The spelling of the `cors` package: it left the allowlist empty, and an
      // upgrade from any origin was accepted.
      assert.throws(() => build({ origin: 'https://app.example' }), /cors\.origin is not an option.*origins/);
      assert.throws(() => build({ origins: 42 }), /cors\.origins must be/);
      assert.throws(() => build({ origins: ['https://a.example', 7] }), /cors\.origins must be/);
      assert.throws(() => build('https://app.example'), /cors must be an object/);
      const { writer, all } = recorder();
      build({ origins: ['https://app.example'], credential: true, methods: 'POST' }, writer);
      assert.deepStrictEqual(
        all('cors.option').map((entry) => [entry.level, entry.key]),
        [['warn', 'credential']],
      );
      assert.doesNotThrow(() =>
        build({ origins: () => true, credentials: true, headers: 'Content-Type', metaHeaders: [] }),
      );
      assert.doesNotThrow(() => build(null));
    });
  });
});
