'use strict';

// The published 2.0.1 and this tree, talking to each other for real. The
// protocol revision cannot tell the two apart — both speak `wrpc.v2` — so
// what binary streams gained after 2.0.1 is asked for by a field 2.0.1 does
// not know, and everything this file checks is a sentence of
// protocol.md#stream-both-directions about "an older peer": written about a
// package that shipped, and run against it.

const { test } = require('node:test');
const assert = require('node:assert');

const { next, previous, boot, connect } = require('./peers.js');
const { waitFor } = require('../helpers/wait.js');

const skip = previous ? false : 'wrpc-v2 (the published 2.0.1) is not installed';

const PAIRS = previous
  ? [
      { name: '2.0.1 client → this server', server: next, client: previous },
      { name: 'this client → 2.0.1 server', server: previous, client: next },
    ]
  : [];

const errorsOf = (client) => {
  const escalated = [];
  client.on('error', (error) => escalated.push(error));
  return escalated;
};

test('interop: the published 2.0.1 is what the alias resolves to', { skip }, () => {
  assert.strictEqual(require('wrpc-v2/package.json').version, '2.0.1');
  assert.notStrictEqual(previous.WrpcClient, next.WrpcClient, 'two packages, not one module twice');
  // What this tree added to the sending half: 2.0.1 has none of it.
  assert.strictEqual(previous.WrpcWritable.prototype.cancel, undefined);
  assert.strictEqual(typeof next.WrpcWritable.prototype.cancel, 'function');
});

for (const pair of PAIRS) {
  test(`interop 2.0.1: a call, and a binary stream up and down — ${pair.name}`, { skip }, async (t) => {
    const { ws } = await boot(t, pair.server);
    const client = await connect(t, pair.client, ws);
    const escalated = errorsOf(client);
    assert.deepStrictEqual(await client.api.echo.say({ at: 1 }), { at: 1 });
    const pieces = 8;
    const size = pieces * 64 * 1024;
    const upload = client.createStream('up.bin', size);
    const received = client.api.echo.upload({ id: upload.id });
    for (let i = 0; i < pieces; i++) upload.write(new Uint8Array(size / pieces).fill(i + 1));
    upload.end();
    assert.deepStrictEqual(await received, { name: 'up.bin', size });
    const readable = client.getStream(await client.api.echo.download({ size, pieces }));
    let downloaded = 0;
    for await (const chunk of readable) downloaded += chunk.length;
    assert.strictEqual(downloaded, size);
    assert.deepStrictEqual(escalated, []);
  });

  // The whole upload on the wire before the call that reads it.
  test(`interop 2.0.1: an upload sent ahead of the call that reads it — ${pair.name}`, { skip }, async (t) => {
    const { ws } = await boot(t, pair.server);
    const client = await connect(t, pair.client, ws);
    const size = 256 * 1024;
    const upload = client.createStream('ahead.bin', size);
    upload.write(new Uint8Array(size).fill(9));
    upload.end();
    assert.deepStrictEqual(await client.api.echo.upload({ id: upload.id }), { name: 'ahead.bin', size });
  });

  // `cancel` is per stream: 2.0.1 never announces it, so it is never told
  // that a reader let go — it would report a callback nobody waited for —
  // and it never tells: the stream is sent whole and dropped on arrival.
  test(`interop 2.0.1: an upload its handler lets go of is sent whole — ${pair.name}`, { skip }, async (t) => {
    const { ws } = await boot(t, pair.server);
    const client = await connect(t, pair.client, ws);
    const escalated = errorsOf(client);
    const pieces = 8;
    const size = pieces * 16 * 1024;
    const upload = client.createStream('refused.bin', size);
    upload.write(new Uint8Array(size / pieces));
    assert.strictEqual(await client.api.echo.refuse({ id: upload.id }), true);
    assert.deepStrictEqual(await client.api.echo.say({ at: 2 }), { at: 2 });
    assert.strictEqual(upload.closed, false, 'a sender facing 2.0.1 was told to stop');
    for (let i = 1; i < pieces; i++) assert.notStrictEqual(upload.write(new Uint8Array(size / pieces)), false);
    upload.end();
    assert.deepStrictEqual(await client.api.echo.say({ at: 3 }), { at: 3 });
    assert.deepStrictEqual(escalated, [], 'a reader letting go surfaced as an error on a 2.0.1 pairing');
  });

  test(`interop 2.0.1: a download its reader lets go of is sent whole — ${pair.name}`, { skip }, async (t) => {
    const warned = [];
    const quiet = () => {};
    const logger = { log: quiet, debug: quiet, info: quiet, warn: (...args) => warned.push(args), error: quiet };
    const { ws } = await boot(t, pair.server, { logger });
    const client = await connect(t, pair.client, ws);
    const escalated = errorsOf(client);
    const size = 8 * 16 * 1024;
    const id = await client.api.echo.pour({ size });
    await client.getStream(id).terminate();
    const forgotten = () => {
      try {
        client.getStream(id);
        return false;
      } catch {
        return true;
      }
    };
    if (pair.client === next) {
      // This tree's server asked to hear and stops; a 2.0.1 server did not
      // ask, is told nothing, and sends to its end — which is what makes
      // the client forget the stream.
      await waitFor(forgotten, 'the whole download, to its end');
    }
    assert.deepStrictEqual(await client.api.echo.say({ at: 4 }), { at: 4 });
    assert.deepStrictEqual(escalated, []);
    assert.deepStrictEqual(warned, [], 'the server logged an answer nobody waited for');
  });
}

// A stream the server refuses. 2.0.1 looked the answer up among its calls:
// "Callback <id> not found", and the writer went on. This tree's client
// stops its writable — and the `terminate` it then sends is, to a 2.0.1
// server, the end of a stream it does not hold: answered 400, where this
// tree's server says nothing.
test('interop 2.0.1: a refused stream — this client → 2.0.1 server', { skip }, async (t) => {
  const { ws } = await boot(t, previous, { maxStreams: 1 });
  const client = await connect(t, next, ws);
  const escalated = errorsOf(client);
  client.createStream('first', 64 * 1024);
  const second = client.createStream('second', 64 * 1024);
  await waitFor(() => second.closed, 'the refusal never reached the writable');
  assert.deepStrictEqual([second.error.code, second.error.message], [429, 'Too many open streams']);
  assert.deepStrictEqual(await client.api.echo.say({ at: 5 }), { at: 5 });
  // One more error than against this tree's server: 2.0.1 answers the terminate.
  assert.strictEqual(escalated.length, 1);
  assert.strictEqual(escalated[0].code, 400);
  assert.match(escalated[0].message, /is not initialized/);
});

test('interop 2.0.1: a refused stream — 2.0.1 client → this server', { skip }, async (t) => {
  const { ws } = await boot(t, next, { maxStreams: 1 });
  const client = await connect(t, previous, ws);
  const escalated = errorsOf(client);
  client.createStream('first', 64 * 1024);
  const second = client.createStream('second', 64 * 1024);
  // 2.0.1 does not route the answer to its writable: it goes on, and says so.
  await waitFor(() => escalated.length === 1, 'the refusal, as 2.0.1 reports it');
  assert.deepStrictEqual(await client.api.echo.say({ at: 6 }), { at: 6 });
  assert.strictEqual(second.closed, false);
  assert.strictEqual(escalated.length, 1);
  assert.match(escalated[0].message, /Callback .* not found/);
});

// A stream that is cut short is thrown to its reader on this tree, whoever
// sent it: 2.0.1's terminate is the same packet.
test("interop 2.0.1: an upload a 2.0.1 sender terminates is thrown to this server's handler", { skip }, async (t) => {
  const { ws } = await boot(t, next);
  const client = await connect(t, previous, ws);
  const upload = client.createStream('cut.bin', 1024 * 1024);
  const answer = client.api.echo.upload({ id: upload.id }).then(
    () => null,
    (error) => error,
  );
  upload.write(new Uint8Array(64 * 1024));
  assert.deepStrictEqual(await client.api.echo.say({ at: 7 }), { at: 7 });
  upload.terminate();
  const error = await answer;
  assert.ok(error, 'a tenth of an upload was answered as a file');
  assert.strictEqual(error.code, 499);
});

// A stream of no bytes is one on this tree, in both directions. 2.0.1 took
// it for ended the moment it was announced: its receiver forgets it at the
// `end`, before a reader can ask for it.
test(
  'interop 2.0.1: a stream of no bytes — to this server it arrives, to a 2.0.1 server it is gone',
  { skip },
  async (t) => {
    const here = await boot(t, next);
    const old = await connect(t, previous, here.ws);
    const empty = old.createStream('empty.bin', 0);
    empty.end();
    assert.deepStrictEqual(await old.api.echo.upload({ id: empty.id }), { name: 'empty.bin', size: 0 });

    const there = await boot(t, previous);
    const client = await connect(t, next, there.ws);
    const sent = client.createStream('empty.bin', 0);
    sent.end();
    // A round trip later — a reader that asks for it afterwards finds no
    // such stream (a handler's uncoded throw: 2.0.1 answers it 500).
    assert.deepStrictEqual(await client.api.echo.say({ at: 8 }), { at: 8 });
    await assert.rejects(client.api.echo.upload({ id: sent.id }), (error) => error.code === 500);
  },
);
