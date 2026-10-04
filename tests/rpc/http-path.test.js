'use strict';

// The HTTP request path as bench/http-call.js measures it, held to what it
// promised before it was made cheaper: an answered call is no longer in
// flight (its signal is not aborted by the per-request client's own close,
// and a cancel arriving after the answer is not answered again), a request
// that goes away before its answer still aborts the handler, and a packet
// body is parsed once — an empty one included, which is a malformed packet
// rather than a routing failure.

const { test } = require('node:test');
const assert = require('node:assert');

const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure } = require('../../src/rpc/router.js');
const { handleRpc, handleCancel } = require('../../src/rpc/dispatcher.js');
const { Emitter } = require('../../src/utils.js');
const { recorder } = require('../helpers/recorder.js');
const { waitFor } = require('../helpers/wait.js');

const deferred = () => {
  let settle;
  const promise = new Promise((resolve) => {
    settle = resolve;
  });
  return { promise, resolve: settle };
};

// One request through handleHttpCall, answered into a stub: resolves with
// the status and the parsed body once wrpc has written them.
const request = (rpc, { method = 'POST', url, body, headers = {}, onAbort } = {}) =>
  new Promise((resolve) => {
    void rpc.handleHttpCall({
      method,
      url,
      headers,
      body,
      remoteAddress: '127.0.0.1',
      respond: ({ status, body: answer }) =>
        resolve({ status, body: answer && answer.length > 0 ? JSON.parse(String(answer)) : null }),
      onAbort,
    });
  });

test('an answered HTTP call is not aborted by the close its answer causes', async (t) => {
  const signals = [];
  const atResponse = [];
  const router = defineRouter(
    {
      unit: {
        echo: procedure({
          access: 'public',
          http: { method: 'POST', path: '/echo' },
          handler: async (context, { body }) => {
            signals.push(context.signal);
            return body;
          },
        }),
        args: procedure({
          access: 'public',
          handler: async (context, args) => {
            signals.push(context.signal);
            return args;
          },
        }),
        fails: procedure({
          access: 'public',
          handler: async (context) => {
            signals.push(context.signal);
            throw Object.assign(new Error('nope'), { code: 409, expose: true });
          },
        }),
      },
    },
    { hooks: { onResponse: [async (context) => void atResponse.push(context.signal.aborted)] } },
  );
  const rpc = new RpcServer({ router, logger: false });
  t.after(() => rpc.close());

  const rest = await request(rpc, { url: '/api/echo', body: Buffer.from('{"name":"Ada"}') });
  assert.deepStrictEqual(rest, { status: 200, body: { name: 'Ada' } });
  const packet = await request(rpc, {
    url: '/api',
    body: Buffer.from(JSON.stringify({ type: 'call', id: '1', method: 'unit/args', args: { n: 1 } })),
  });
  assert.deepStrictEqual(packet, { status: 200, body: { type: 'callback', id: '1', result: { n: 1 } } });
  const failed = await request(rpc, {
    url: '/api',
    body: Buffer.from(JSON.stringify({ type: 'call', id: '2', method: 'unit/fails' })),
  });
  assert.strictEqual(failed.body.error.code, 409);

  assert.strictEqual(signals.length, 3);
  for (const signal of signals) assert.strictEqual(signal.aborted, false, 'an answered call keeps its signal');
  assert.deepStrictEqual(atResponse, [false, false], 'onResponse runs on a call that is still not aborted');
  // The per-request clients are gone all the same.
  assert.strictEqual(rpc.clients.size, 0);
});

test('a request that goes away before its answer still aborts its handler', async (t) => {
  const started = deferred();
  let reason = null;
  const router = defineRouter({
    unit: {
      wait: procedure({
        access: 'public',
        handler: (context) =>
          new Promise((_resolve, reject) => {
            context.signal.addEventListener('abort', () => {
              reason = context.signal.reason;
              reject(context.signal.reason);
            });
            started.resolve();
          }),
      }),
    },
  });
  const rpc = new RpcServer({ router, logger: false });
  t.after(() => rpc.close());
  let abort = null;
  const answered = [];
  void rpc.handleHttpCall({
    method: 'POST',
    url: '/api',
    headers: {},
    body: Buffer.from(JSON.stringify({ type: 'call', id: '1', method: 'unit/wait' })),
    respond: (response) => answered.push(response),
    onAbort: (listener) => {
      abort = listener;
    },
  });
  await started.promise;
  abort();
  await waitFor(() => reason !== null, 'the handler was never aborted');
  assert.match(reason.message, /Client disconnected/);
  assert.strictEqual(rpc.clients.size, 0);
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepStrictEqual(answered, [], 'nothing is written for a request that is gone');
});

test('an empty packet POST is a malformed packet, answered once', async (t) => {
  const router = defineRouter({ unit: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } });
  const { writer, all } = recorder();
  const rpc = new RpcServer({ router, logger: writer });
  t.after(() => rpc.close());
  for (const body of [null, undefined, Buffer.alloc(0)]) {
    const answer = await request(rpc, { url: '/api', body });
    assert.strictEqual(answer.status, 500);
    assert.strictEqual(answer.body.type, 'callback');
  }
  assert.strictEqual(all('packet.malformed').length, 3, 'the funnel every unparseable packet goes through');
  assert.deepStrictEqual(all('http.failed'), [], 'not a routing failure');
});

test('a cancel that arrives after the answer is not answered again', async (t) => {
  const hold = deferred();
  const entered = deferred();
  const router = defineRouter(
    { unit: { ok: procedure({ access: 'public', handler: async () => 'fine' }) } },
    {
      hooks: {
        onResponse: [
          async () => {
            entered.resolve();
            await hold.promise;
          },
        ],
      },
    },
  );
  const rpc = new RpcServer({ router, logger: false, sse: false });
  t.after(() => rpc.close());
  // A persistent transport: the answer does not close it.
  class CaptureTransport extends Emitter {
    kind = 'capture';
    source = 'capture';
    connection = true;
    sent = [];
    write() {
      return true;
    }
    send(packet) {
      this.sent.push(packet);
      return true;
    }
    error(code, { id }) {
      this.sent.push({ type: 'callback', id, error: { code } });
      return true;
    }
    close() {
      this.emit('close');
    }
  }
  const transport = new CaptureTransport();
  const client = rpc.attach(transport);
  await client.ready;
  const done = handleRpc(client, { type: 'call', id: 'c1', method: 'unit/ok', args: {} }, rpc.router);
  await entered.promise;
  assert.deepStrictEqual(transport.sent, [{ type: 'callback', id: 'c1', result: 'fine' }]);
  assert.strictEqual(client.calls.size, 0, 'released when its answer was written');
  // While the onResponse hook still runs, the caller gives up on the call.
  handleCancel(client, { type: 'cancel', id: 'c1' });
  hold.resolve();
  await done;
  assert.deepStrictEqual(transport.sent, [{ type: 'callback', id: 'c1', result: 'fine' }], 'no 499 after a result');
});
