'use strict';

// The client half of the SSE transport against a fetch that is a script:
// the answers a real server gives only when something went wrong on its
// side — a channel it no longer holds, a replay it cannot honour, a POST it
// refuses — which the end-to-end suite cannot make it give on demand.

const { test } = require('node:test');
const assert = require('node:assert');

const { ClientSseTransport, CHANNEL_HEADER } = require('../../src/sse/index.js');
const { waitFor } = require('../helpers/wait.js');

const encoder = new TextEncoder();

// One event stream the test writes into.
const eventStream = () => {
  let controller = null;
  const body = new ReadableStream({
    start(c) {
      controller = c;
    },
  });
  return {
    response: new Response(body, { status: 200, headers: { 'content-type': 'text/event-stream' } }),
    push: (text) => controller.enqueue(encoder.encode(text)),
    ready: (channel) => controller.enqueue(encoder.encode(`event: ready\ndata: ${JSON.stringify({ channel })}\n\n`)),
    end: () => controller.close(),
  };
};

// A fetch that answers each request with the next entry of `script` — a
// Response, or a function of the request — and remembers what it was asked.
const scripted = (script) => {
  const requests = [];
  const fetch = async (url, init = {}) => {
    requests.push({ url, method: init.method ?? 'GET', headers: init.headers ?? {}, body: init.body });
    const next = script.shift();
    if (next === undefined) throw new Error(`an unexpected request to ${url}`);
    return typeof next === 'function' ? next(init) : next;
  };
  return { fetch, requests };
};

const recorder = () => {
  const lines = [];
  return { lines, warn: (entry) => lines.push(entry), debug() {}, info() {}, error() {} };
};

const listen = (transport) => {
  const seen = { messages: [], errors: [], closes: 0 };
  transport.on('message', (data) => seen.messages.push(data));
  transport.on('error', (error) => seen.errors.push(error.message));
  transport.on('close', () => seen.closes++);
  return seen;
};

const CALL = JSON.stringify({ type: 'call', id: 'c1', method: 'data/echo', args: {} });

test('sse client: a channel the server no longer holds is forgotten, and a fresh one opened', async () => {
  const first = eventStream();
  const second = eventStream();
  const { fetch, requests } = scripted([first.response, new Response('gone', { status: 409 }), second.response]);
  const transport = new ClientSseTransport('http://h/api');
  const seen = listen(transport);
  const opening = transport.open({ fetch });
  first.ready('ch-1.secret');
  await opening;
  first.push('id: 7\ndata: {"type":"event","name":"data/tick","data":1}\n\n');
  await waitFor(() => seen.messages.length === 1, 'the event arrived');
  // The stream dies; the transport remembers its channel and where it stopped.
  first.end();
  await waitFor(() => seen.closes === 1, 'the stream closed');

  const reopening = transport.open({ fetch });
  await waitFor(() => requests.length === 3, 'a 409 was answered with a second attempt');
  second.ready('ch-2.secret');
  await reopening;
  assert.strictEqual(requests[1].headers[CHANNEL_HEADER], 'ch-1.secret', 'the re-attach presents the channel');
  assert.strictEqual(requests[1].headers['last-event-id'], '7', '…and where it stopped');
  assert.strictEqual(requests[2].headers[CHANNEL_HEADER], undefined, 'the fresh open presents nothing');
  assert.strictEqual(requests[2].headers['last-event-id'], undefined);
  assert.ok(transport.active);
  transport.close();
});

test('sse client: a 409 with no channel to forget is a refused stream, not a loop', async () => {
  const { fetch, requests } = scripted([new Response('no', { status: 409 })]);
  const transport = new ClientSseTransport('http://h/api');
  await assert.rejects(transport.open({ fetch }), /SSE stream refused with 409/);
  assert.strictEqual(requests.length, 1);
  assert.strictEqual(transport.active, false);
});

test('sse client: a second 409 in a row is the refusal — one retry, never two', async () => {
  const first = eventStream();
  const gone = () => new Response('gone', { status: 409 });
  const { fetch, requests } = scripted([first.response, gone, gone]);
  const transport = new ClientSseTransport('http://h/api');
  const seen = listen(transport);
  const opening = transport.open({ fetch });
  first.ready('ch-1.secret');
  await opening;
  first.end();
  await waitFor(() => seen.closes === 1);
  await assert.rejects(transport.open({ fetch }), /SSE stream refused with 409/);
  assert.strictEqual(requests.length, 3);
});

test('sse client: a gap frame is event loss — said, and the channel is dropped with its position', async () => {
  const first = eventStream();
  const second = eventStream();
  const { fetch, requests } = scripted([first.response, second.response]);
  const transport = new ClientSseTransport('http://h/api');
  const log = recorder();
  transport.log = log;
  const seen = listen(transport);
  const opening = transport.open({ fetch });
  first.ready('ch-1.secret');
  await opening;
  first.push('id: 3\ndata: {"type":"event","name":"data/tick","data":1}\n\n');
  first.push('event: gap\ndata: {}\n\n');
  await waitFor(() => seen.closes === 1, 'the transport closed on the gap');
  assert.deepStrictEqual(log.lines, [{ event: 'sse.gap' }]);
  assert.strictEqual(transport.active, false);

  const reopening = transport.open({ fetch });
  second.ready('ch-2.secret');
  await reopening;
  assert.strictEqual(requests[1].headers[CHANNEL_HEADER], undefined, 'nothing of the lost channel is presented');
  assert.strictEqual(requests[1].headers['last-event-id'], undefined);
  transport.close();
});

const opened = async (script) => {
  const stream = eventStream();
  const { fetch, requests } = scripted([stream.response, ...script]);
  const transport = new ClientSseTransport('http://h/api');
  const seen = listen(transport);
  const opening = transport.open({ fetch });
  stream.ready('ch-1.secret');
  await opening;
  return { transport, seen, requests, stream };
};

test('sse client: a POST answered 202 says nothing; one answered 200 with a body is a message', async () => {
  const answer = JSON.stringify({ type: 'callback', id: 'c1', result: 1 });
  const { transport, seen, requests } = await opened([
    new Response(null, { status: 202 }),
    new Response(answer, { status: 200 }),
    new Response('', { status: 200 }),
  ]);
  transport.write(CALL);
  await waitFor(() => requests.length === 2);
  assert.strictEqual(requests[1].method, 'POST');
  assert.strictEqual(requests[1].headers[CHANNEL_HEADER], 'ch-1.secret');
  transport.write(CALL);
  await waitFor(() => seen.messages.length === 1, 'the body came through');
  assert.deepStrictEqual(seen.messages, [answer]);
  transport.write(CALL);
  await waitFor(() => requests.length === 4);
  assert.deepStrictEqual(seen.messages, [answer], 'an empty 200 is nothing');
  assert.deepStrictEqual(seen.errors, []);
  transport.close();
});

test('sse client: a POST answered 409 is a channel that died — the transport closes', async () => {
  const { transport, seen } = await opened([new Response('gone', { status: 409 })]);
  transport.write(CALL);
  await waitFor(() => seen.closes === 1, 'the orphaned stream was closed');
  assert.strictEqual(transport.active, false);
  assert.deepStrictEqual(seen.errors, [], 'a dead channel is a reconnect, not an error');
  assert.deepStrictEqual(seen.messages, [], 'and the close settles what was in flight, not a made-up answer');
});

test('sse client: a refused POST fails the calls it carried at once, with its status', async () => {
  const { transport, seen } = await opened([new Response('too many', { status: 429 })]);
  const batch = JSON.stringify([
    { type: 'call', id: 'a', method: 'data/echo', args: {} },
    { type: 'event', name: 'data/note', data: 1 },
    { type: 'call', id: 'b', method: 'data/echo', args: {} },
  ]);
  transport.write(batch);
  await waitFor(() => seen.messages.length === 1, 'the calls were answered');
  assert.deepStrictEqual(seen.errors, ['SSE post failed with 429: too many']);
  assert.deepStrictEqual(JSON.parse(seen.messages[0]), [
    { type: 'callback', id: 'a', error: { message: 'HTTP request failed (429)', code: 429 } },
    { type: 'callback', id: 'b', error: { message: 'HTTP request failed (429)', code: 429 } },
  ]);
  assert.ok(transport.active, 'the channel itself is still there');
  transport.close();
});

test('sse client: a POST that never reached the server fails its calls as unavailable', async () => {
  const { transport, seen } = await opened([
    () => {
      throw new TypeError('fetch failed');
    },
  ]);
  transport.write(CALL);
  await waitFor(() => seen.messages.length === 1);
  assert.deepStrictEqual(seen.errors, ['fetch failed']);
  assert.deepStrictEqual(JSON.parse(seen.messages[0]), {
    type: 'callback',
    id: 'c1',
    error: { message: 'HTTP request failed (503)', code: 503 },
  });
  transport.close();
});

test('sse client: a stream that ends, or answers without a body, before ready never opened', async () => {
  const ended = eventStream();
  const first = scripted([ended.response]);
  const transport = new ClientSseTransport('http://h/api');
  const opening = transport.open({ fetch: first.fetch });
  ended.end();
  await assert.rejects(opening, /SSE stream ended before ready/);
  assert.strictEqual(transport.active, false);

  const second = scripted([new Response(null, { status: 200 })]);
  await assert.rejects(
    new ClientSseTransport('http://h/api').open({ fetch: second.fetch }),
    /SSE stream refused with 200/,
  );
  const third = scripted([new Response('down', { status: 503 })]);
  await assert.rejects(
    new ClientSseTransport('http://h/api').open({ fetch: third.fetch }),
    /SSE stream refused with 503/,
  );
});

test('sse client: write() before a channel, and bytes on a text carrier, are refused where they are made', async () => {
  const transport = new ClientSseTransport('http://h/api');
  assert.throws(() => transport.write(CALL), /Not connected/);
  const { transport: live } = await opened([]);
  assert.throws(() => live.write(new Uint8Array(4)), /SSE carries text only/);
  live.close();
});
