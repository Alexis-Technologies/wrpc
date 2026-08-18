'use strict';

// Acks: the server asks, the client answers. On the wire it is the existing
// event packet with an optional `id`, answered by the existing callback
// packet — no new type. End-to-end over a real Server and WrpcClient.

const test = require('node:test');
const assert = require('node:assert');

const { defineRouter, procedure } = require('../../index.js');
const { MemoryBackplane } = require('../../scaling.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const api = () =>
  defineRouter({
    chat: {
      hello: procedure({ access: 'public', handler: async (_ctx, args) => args }),
      whoami: procedure({
        access: 'public',
        handler: async (ctx) => ({ id: ctx.client.id }),
      }),
    },
  });

const grabClient = (server) => {
  // The one persistent client this test connected.
  for (const client of server.rpc.clients) {
    if (client.persistent) return client;
  }
  return null;
};

test('acks: client.ask resolves with the responder answer', async (t) => {
  const { server, url } = await bootServer(t, { router: api() });
  const client = await connectClient(t, url);
  await client.load('chat');

  client.respond('chat/confirm', async (data) => ({ ok: true, got: data.q }));
  const peer = grabClient(server);
  const answer = await peer.ask('chat/confirm', { q: 42 });
  assert.deepStrictEqual(answer, { ok: true, got: 42 });
});

test('acks: a throwing responder answers a coded error', async (t) => {
  const { server, url } = await bootServer(t, { router: api() });
  const client = await connectClient(t, url);
  await client.load('chat');

  client.respond('chat/confirm', async () => {
    const error = new Error('not now');
    error.code = 409;
    throw error;
  });
  const peer = grabClient(server);
  await assert.rejects(peer.ask('chat/confirm', {}), (error) => error.code === 409 && /not now/.test(error.message));
});

test('acks: no responder answers 501 immediately, not at the timeout', async (t) => {
  const { server, url } = await bootServer(t, { router: api() });
  const client = await connectClient(t, url);
  await client.load('chat');
  const unhandled = [];
  client.on('unhandled-event', (event) => unhandled.push(event));

  const peer = grabClient(server);
  const started = Date.now();
  await assert.rejects(
    peer.ask('chat/confirm', {}, { timeout: 5_000 }),
    (error) => error.code === 501 && /No responder/.test(error.message),
  );
  assert.ok(Date.now() - started < 1_000, 'the 501 arrived, the timeout never had to');
  await waitFor(() => unhandled.length === 1);
  assert.strictEqual(unhandled[0].name, 'chat/confirm');
});

test('acks: the ask timeout rejects with 408', async (t) => {
  const { server, url } = await bootServer(t, { router: api() });
  const client = await connectClient(t, url);
  await client.load('chat');

  client.respond('chat/confirm', () => new Promise(() => {})); // never answers
  const peer = grabClient(server);
  await assert.rejects(peer.ask('chat/confirm', {}, { timeout: 50 }), (error) => error.code === 408);
});

test('acks: a disconnect settles pending asks with 503', async (t) => {
  const { server, url } = await bootServer(t, { router: api() });
  const client = await connectClient(t, url);
  await client.load('chat');

  client.respond('chat/confirm', () => new Promise(() => {}));
  const peer = grabClient(server);
  const pending = peer.ask('chat/confirm', {}, { timeout: 5_000 });
  client.close();
  await assert.rejects(pending, (error) => error.code === 503);
});

test('acks: duplicate responder throws; unrespond frees the name', async (t) => {
  const { url } = await bootServer(t, { router: api() });
  const client = await connectClient(t, url);

  client.respond('chat/confirm', async () => 1);
  assert.throws(() => client.respond('chat/confirm', async () => 2), /Duplicate responder/);
  assert.strictEqual(client.unrespond('chat/confirm'), true);
  client.respond('chat/confirm', async () => 2);
  assert.throws(() => client.respond('chat/confirm'), /handler must be a function/);
});

test('acks: broadcast ask aggregates answers and survives a missing responder', async (t) => {
  const { server, url } = await bootServer(t, { router: api() });
  const first = await connectClient(t, url);
  const second = await connectClient(t, url);
  const third = await connectClient(t, url);
  await first.load('chat');
  await second.load('chat');
  await third.load('chat');

  first.respond('chat/poll', async (q) => ({ vote: 'yes', q }));
  second.respond('chat/poll', async () => ({ vote: 'no' }));
  // third has no responder: its 501 lands in errors, not in silence.

  for (const client of server.rpc.clients) client.join('room');
  const result = await server.rpc.to('room').ask('chat/poll', { q: 1 }, { timeout: 2_000 });
  assert.strictEqual(result.expected, 3);
  assert.strictEqual(result.incomplete, false);
  assert.strictEqual(result.answers.length, 2);
  assert.deepStrictEqual(new Set(result.answers.map((a) => a.vote)), new Set(['yes', 'no']));
  assert.strictEqual(result.errors.length, 1);
  assert.strictEqual(result.errors[0].code, 501);
});

test('acks: broadcast ask narrowed to no rooms asks nobody', async (t) => {
  const { server, url } = await bootServer(t, { router: api() });
  await connectClient(t, url);
  const result = await server.rpc.to().ask('chat/poll', {});
  assert.deepStrictEqual(result, { answers: [], errors: [], expected: 0, incomplete: false });
});

test('acks: empty-target ask reaches nobody on OTHER instances either', async (t) => {
  // The clustered variant of the guard above — without it, an empty rooms
  // array asked nobody locally but every client of every other instance.
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = await bootServer(t, { router: api(), backplane, instanceId: 'a' });
  const b = await bootServer(t, { router: api(), backplane, instanceId: 'b' });
  const remote = await connectClient(t, b.url);
  let askedRemote = false;
  remote.respond('chat/poll', async () => {
    askedRemote = true;
    return 'leaked';
  });
  await waitFor(() => a.server.rpc.cluster.instances().length === 2);

  const result = await a.server.rpc.to().ask('chat/poll', { secret: 'x' }, { timeout: 300 });
  assert.deepStrictEqual(result, { answers: [], errors: [], expected: 0, incomplete: false });
  // Give any stray wire delivery a moment to prove itself absent.
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.strictEqual(askedRemote, false, 'the remote responder was never invoked');
});

test('acks: an HTTP request carrying a callback packet is answered, not hung', async (t) => {
  const { port, origin } = await bootServer(t, { router: api() });
  const endpoint = `${origin.replace('http', 'http')}/api`;
  assert.ok(port);

  // A single callback packet: must answer 400, not park the request.
  const single = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'callback', id: 'x', result: 1 }),
    signal: AbortSignal.timeout(2_000),
  });
  assert.strictEqual(single.status, 400);
  const answer = await single.json();
  assert.strictEqual(answer.error.code, 400);

  // Inside a batch: the callback slot answers 400 and the real calls still
  // answer — one malformed item must not withhold the whole batch.
  const batch = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify([
      { type: 'call', id: 'c1', method: 'chat/hello', args: { n: 1 } },
      { type: 'callback', id: 'cb', result: 1 },
      { type: 'call', id: 'c2', method: 'chat/hello', args: { n: 2 } },
    ]),
    signal: AbortSignal.timeout(2_000),
  });
  const answers = await batch.json();
  assert.strictEqual(answers.length, 3);
  assert.deepStrictEqual(answers[0].result, { n: 1 });
  assert.strictEqual(answers[1].error.code, 400);
  assert.deepStrictEqual(answers[2].result, { n: 2 });
});

test('acks: broadcast ask serializes the payload exactly once', async (t) => {
  const { server, url } = await bootServer(t, { router: api() });
  const clients = [await connectClient(t, url), await connectClient(t, url), await connectClient(t, url)];
  for (const client of clients) client.respond('chat/poll', async () => 'ok');
  for (const client of server.rpc.clients) client.join('room');

  // The spy: a payload whose toJSON counts how many times JSON.stringify
  // visited it. One broadcast to three clients must serialize once — the
  // per-recipient id is a suffix concatenation, not a re-stringify.
  let serialized = 0;
  const payload = {
    toJSON() {
      serialized++;
      return { question: 'q' };
    },
  };
  const result = await server.rpc.to('room').ask('chat/poll', payload, { timeout: 2_000 });
  assert.strictEqual(result.answers.length, 3);
  assert.strictEqual(serialized, 1, 'one JSON.stringify for the whole fan-out');

  // The regression guard for plain emit(): still exactly one.
  serialized = 0;
  server.rpc.to('room').emit('chat/note', payload);
  assert.strictEqual(serialized, 1);
});

test('acks: cluster broadcast ask reaches members of every instance', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = await bootServer(t, { router: api(), backplane, instanceId: 'a' });
  const b = await bootServer(t, { router: api(), backplane, instanceId: 'b' });

  const mine = await connectClient(t, a.url);
  const theirs = await connectClient(t, b.url);
  mine.respond('chat/poll', async () => 'from-a');
  theirs.respond('chat/poll', async () => 'from-b');
  for (const client of a.server.rpc.clients) client.join('room');
  for (const client of b.server.rpc.clients) client.join('room');
  // Wait for the presence deltas so a knows b holds a member of 'room'.
  await waitFor(() => a.server.rpc.cluster.count('room') === 2);

  const result = await a.server.rpc.to('room').ask('chat/poll', {}, { timeout: 2_000 });
  assert.strictEqual(result.expected, 2, 'both instances counted their members');
  assert.deepStrictEqual(new Set(result.answers), new Set(['from-a', 'from-b']));
  assert.strictEqual(result.errors.length, 0);
  assert.strictEqual(result.incomplete, false);

  // local() keeps the question on this instance.
  const local = await a.server.rpc.to('room').local().ask('chat/poll', {}, { timeout: 2_000 });
  assert.deepStrictEqual(local.answers, ['from-a']);
  assert.strictEqual(local.expected, 1);
});

test('acks: an unmatched callback is logged, never answered', async (t) => {
  const warned = [];
  const { server, url } = await bootServer(t, {
    router: api(),
    logger: { log() {}, info() {}, error() {}, debug() {}, warn: (entry) => warned.push(entry) },
  });
  const client = await connectClient(t, url);
  await client.load('chat');

  // A callback for an ask nobody sent: fabricated straight onto the wire.
  client.write(JSON.stringify({ type: 'callback', id: 'never-asked', result: 1 }));
  await waitFor(() => warned.some((entry) => /no pending ask/.test(String(entry))));
  // And the connection is still perfectly usable — no error ping-pong.
  const echo = await client.api.chat.hello({ x: 1 });
  assert.deepStrictEqual(echo, { x: 1 });
  assert.ok(server.rpc.clients.size >= 1);
});
