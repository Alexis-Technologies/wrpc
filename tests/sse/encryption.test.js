'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { defineRouter, procedure, WrpcClient } = require('../../index.js');
require('../../sse.js');
const { createEncryption, generateKey } = require('../../encryption.js');
const { openSealedStream, isSealedStream, SEALED_STREAM_TYPE } = require('../../src/encryption/http.js');
const { aead } = require('../../src/encryption/aead.js');
const browser = require('../../src/encryption/aead.browser.js');
const { OpenError } = require('../../src/encryption/contracts.js');
const { counterNonce } = require('../../src/encryption/bytes.js');
const { bootServer, waitFor } = require('../helpers/server.js');

const SECRET = 'not for the proxy that holds this stream open: 4111 1111 1111 1111';

const router = defineRouter({
  data: {
    echo: procedure({ access: 'public', handler: async (ctx, args) => ({ args, kind: ctx.client.transportKind }) }),
    shout: procedure({
      access: 'public',
      handler: async (ctx, { text }) => {
        ctx.server.broadcast('data/shouted', { text });
        return true;
      },
    }),
  },
});

// Every outer exchange as a proxy in the middle would see it — a stream's
// body is tee'd, so what is recorded is what actually crossed the wire.
const spyingFetch = () => {
  const seen = [];
  const fetchSpy = async (url, init) => {
    const response = await fetch(url, init);
    const entry = {
      url: String(url),
      init,
      status: response.status,
      type: response.headers.get('content-type'),
      text: '',
    };
    seen.push(entry);
    if (!isSealedStream(entry.type)) {
      entry.text = Buffer.from(await response.clone().arrayBuffer()).toString('latin1');
      return response;
    }
    // Recorded as it passes, and killable: `entry.drop()` is the network
    // taking the stream away while the channel lives on.
    const reader = response.body.getReader();
    const body = new ReadableStream({
      start(controller) {
        entry.drop = () => {
          reader.cancel().catch(() => {});
          controller.error(new Error('stream dropped'));
        };
      },
      async pull(controller) {
        const { value, done } = await reader.read().catch(() => ({ done: true }));
        if (done) return void controller.close();
        entry.text += Buffer.from(value).toString('latin1');
        controller.enqueue(value);
      },
    });
    return new Response(body, { status: response.status, headers: response.headers });
  };
  return { seen, fetch: fetchSpy };
};

const secure = async (t, options = {}) => {
  const booted = await bootServer(t, { router, encryption: { keys: generateKey(), required: true }, ...options });
  const endpoint = `${booted.origin}${booted.server.rpc.basePath}`;
  const serverKey = await booted.server.rpc.encryptionKey();
  const connect = async (extra = {}) => {
    const client = await WrpcClient.connect(endpoint, {
      transport: 'sse',
      encryption: createEncryption({ serverKey }),
      logger: false,
      reconnect: false,
      heartbeat: false,
      ...extra,
    });
    t.after(() => void client.close());
    return client;
  };
  return { ...booted, endpoint, serverKey, connect };
};

test('sse encryption: a whole channel — the stream, the calls, an event — and an observer sees opaque POSTs', async (t) => {
  const { endpoint, connect } = await secure(t);
  const spy = spyingFetch();
  const client = await connect({ fetch: spy.fetch });
  await client.load('data');
  assert.deepStrictEqual(await client.api.data.echo({ note: SECRET }), { args: { note: SECRET }, kind: 'sse' });
  const heard = new Promise((resolve) => client.api.data.on('shouted', resolve));
  await client.api.data.shout({ text: SECRET });
  assert.deepStrictEqual(await heard, { text: SECRET });

  const stream = spy.seen.find((entry) => isSealedStream(entry.type));
  assert.ok(stream, 'the events request was answered with a sealed stream');
  assert.strictEqual(stream.type, SEALED_STREAM_TYPE);
  await waitFor(() => stream.text.split('\n\n').length > 3);
  for (const event of stream.text.split('\n\n').filter(Boolean)) {
    assert.match(event, /^data: [A-Za-z0-9+/]+=*$/, 'every event is one opaque data line — no id, no event name');
  }
  for (const { url, init, text } of spy.seen) {
    assert.strictEqual(url, endpoint, 'the stream GET included: one endpoint');
    assert.strictEqual(init.method, 'POST');
    assert.deepStrictEqual(Object.keys(init.headers), ['Content-Type'], 'no x-wrpc-channel, no last-event-id outside');
    for (const needle of ['4111', 'ready', 'channel', 'shouted', 'callback', 'events']) {
      assert.ok(!Buffer.from(init.body).toString('latin1').includes(needle), `request: ${needle}`);
      assert.ok(!text.includes(needle), `response: ${needle}`);
    }
  }
});

test('sse encryption: a dropped stream re-attaches and replays — under a new key, from a counter of zero', async (t) => {
  const { connect, server } = await secure(t);
  const spy = spyingFetch();
  const client = await connect({ fetch: spy.fetch, reconnect: { delay: 10, maxDelay: 20 } });
  await client.load('data');
  const heard = [];
  client.api.data.on('shouted', (data) => heard.push(data.text));
  await client.api.data.shout({ text: 'before' });
  await waitFor(() => heard.length === 1);
  // The stream dies under the client; the channel outlives it
  const reconnected = new Promise((resolve) => client.once('reconnect', resolve));
  spy.seen.find((entry) => isSealedStream(entry.type)).drop();
  server.rpc.broadcast('data/shouted', { text: 'while away' });
  await reconnected;
  await waitFor(() => heard.includes('while away'), 'the missed event was not replayed');
  await client.api.data.shout({ text: 'after' });
  await waitFor(() => heard.includes('after'));
  const streams = spy.seen.filter((entry) => isSealedStream(entry.type));
  assert.strictEqual(streams.length, 2);
  assert.notStrictEqual(streams[0].text.slice(0, 60), streams[1].text.slice(0, 60), 'another key: nothing repeats');
});

test('sse encryption: the http content coding is not applied to a sealed stream or a sealed answer', async (t) => {
  const { connect } = await secure(t, { http: { compression: true }, sse: { compression: true } });
  const spy = spyingFetch();
  const client = await connect({ fetch: spy.fetch });
  await client.load('data');
  const text = 'q'.repeat(20_000);
  assert.strictEqual((await client.api.data.echo({ text })).args.text.length, 20_000);
  assert.ok(spy.seen.length > 2);
});

test('sse encryption: no serverKey, nothing to seal a request to', async (t) => {
  const { endpoint } = await secure(t);
  await assert.rejects(
    WrpcClient.connect(endpoint, { transport: 'sse', encryption: createEncryption({ pattern: 'NN' }), logger: false }),
    /has no serverKey to seal a request to — the sse transport needs one/,
  );
});

const sealedBody = (key, chunks, mangle = (lines) => lines) => {
  const lines = chunks.map(
    (chunk, i) => `data: ${Buffer.from(key.seal(counterNonce(i), Buffer.from(chunk), null)).toString('base64')}\n\n`,
  );
  const text = mangle(lines).join('');
  // Split at awkward places: a frame boundary is not a read boundary
  const pieces = [text.slice(0, 7), text.slice(7, 61), text.slice(61)];
  return new ReadableStream({
    start(controller) {
      for (const piece of pieces) controller.enqueue(Buffer.from(piece));
      controller.close();
    },
  });
};

const drain = async (stream) => {
  const reader = stream.getReader();
  let text = '';
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return text;
    text += Buffer.from(value).toString();
  }
};

test('sealed stream: what comes out is the real stream, whatever the read boundaries — on both cipher halves', async () => {
  const raw = new Uint8Array(32).fill(3);
  const chunks = ['event: ready\ndata: {"channel":"c1"}\n\n', 'id: 0\ndata: {"привіт":1}\n\n', ': ping\n\n'];
  for (const key of [aead().key(raw), await browser.aead().key(raw)]) {
    assert.strictEqual(await drain(openSealedStream(sealedBody(aead().key(raw), chunks), key)), chunks.join(''));
  }
  // A comment an intermediary injected is skipped, not opened
  const noisy = sealedBody(aead().key(raw), chunks, (lines) => [': keep-alive\n\n', ...lines]);
  assert.strictEqual(await drain(openSealedStream(noisy, aead().key(raw))), chunks.join(''));
});

test('sealed stream: a frame dropped, reordered, altered or not base64 errors the stream', async () => {
  const raw = new Uint8Array(32).fill(3);
  const chunks = ['one\n\n', 'two\n\n', 'three\n\n'];
  const key = () => aead().key(raw);
  const broken = [
    (lines) => [lines[0], lines[2]],
    (lines) => [lines[1], lines[0], lines[2]],
    (lines) => [lines[0].replace(/data: ../, 'data: AA'), lines[1], lines[2]],
    () => ['data: !!!not base64!!!\n\n'],
  ];
  for (const mangle of broken) {
    await assert.rejects(drain(openSealedStream(sealedBody(key(), chunks, mangle), key())), OpenError);
  }
  // Cancelling the opened stream cancels the one underneath
  let cancelled = false;
  const body = new ReadableStream({ cancel: () => void (cancelled = true) });
  await openSealedStream(body, key()).cancel('done');
  assert.strictEqual(cancelled, true);
});
