'use strict';

// A stream that is cut short — its sender terminated it, its connection
// closed — used to be the quiet end: the reader's loop stopped, nothing was
// thrown, and whoever read it had to compare counts to know that what it
// had was a part. It is a failure for its reader now, on both ends; only the
// reader's own terminate() is quiet. Each test below failed before that.

const { test } = require('node:test');
const assert = require('node:assert');
const { Writable } = require('node:stream');

const { WrpcReadable } = require('../src/streams.js');
const { defineRouter, procedure } = require('../index.js');
const { bootServer, connectClient, waitFor } = require('./helpers/server.js');
const { recorder } = require('./helpers/recorder.js');
const { KIB, bytes, outcome } = require('./helpers/streams.js');

// A handler the way the guide shows one: loop, keep each chunk, answer.
const fixture = () => {
  const stored = [];
  const downloads = new Map();
  const router = defineRouter({
    files: {
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
      store: procedure({
        access: 'public',
        handler: async (context, { id }) => {
          const stream = context.client.getStream(id);
          const record = { bytes: 0, threw: null, finished: false };
          stored.push(record);
          try {
            for await (const chunk of stream) record.bytes += chunk.length;
          } catch (error) {
            record.threw = error.code;
            throw error;
          }
          record.finished = true;
          return { name: stream.name, size: stream.bytesRead };
        },
      }),
      // A download of `size` bytes of which only `sent` are written; what
      // ends it is the test's to choose.
      part: procedure({
        access: 'public',
        handler: async (context, { size, sent }) => {
          const stream = context.client.createStream('part.bin', size);
          downloads.set(stream.id, stream);
          await stream.writeAll(bytes(sent));
          return stream.id;
        },
      }),
      cut: procedure({ access: 'public', handler: async (context, { id }) => void downloads.get(id).terminate() }),
      hangUp: procedure({ access: 'public', handler: async (context) => void context.client.close() }),
    },
  });
  return { router, stored };
};

test('server: an upload its sender terminated is thrown to the handler, not stored as a file', async (t) => {
  const { router, stored } = fixture();
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('files');
  const upload = client.createStream('backup.tar', 1024 * KIB);
  const answer = outcome(client.api.files.store({ id: upload.id }), 2000);
  await upload.writeAll(bytes(300 * KIB));
  await client.api.files.ping({});
  // What createBlobUploader does when its source fails.
  upload.terminate();
  const { status, error } = await answer;
  assert.strictEqual(status, 'rejected', 'a handler answered for 300 KiB of a megabyte as if it had the file');
  assert.strictEqual(error.code, 499);
  assert.match(error.message, /backup\.tar was terminated by its sender/);
  assert.deepStrictEqual(stored, [{ bytes: 300 * KIB, threw: 499, finished: false }]);
});

test("server: a connection lost mid-upload is thrown to the handler, and is nobody's log line", async (t) => {
  const { router, stored } = fixture();
  const { writer, entries, loud } = recorder();
  const { url } = await bootServer(t, { router, logger: writer });
  const client = await connectClient(t, url);
  client.on('error', () => {});
  await client.load('files');
  const upload = client.createStream('backup.tar', 1024 * KIB);
  client.api.files.store({ id: upload.id }).catch(() => {});
  await upload.writeAll(bytes(300 * KIB));
  await client.api.files.ping({});
  entries.length = 0;
  client.close();
  await waitFor(() => stored.length === 1 && stored[0].threw !== null, 'the handler of a lost upload was never told');
  assert.deepStrictEqual(stored, [{ bytes: 300 * KIB, threw: 503, finished: false }]);
  // A client that went away is not an incident: one line per upload it had
  // in flight would be a peer's to print.
  assert.deepStrictEqual(loud(), []);
});

test('pipe(): a stream that is cut short destroys its sink instead of ending it', async () => {
  const readable = new WrpcReadable('s', 'piped', 8);
  const written = [];
  let finished = false;
  const sink = new Writable({
    write(chunk, encoding, callback) {
      written.push(chunk.length);
      callback();
    },
  });
  sink.on('finish', () => (finished = true));
  const destroyed = new Promise((resolve) => sink.on('close', resolve));
  sink.on('error', () => {});
  readable.pipe(sink);
  await readable.push(bytes(4));
  await waitFor(() => written.length === 1, 'the sink took the first chunk');
  // What the feeder of the readable does for its sender's terminate.
  readable.fail('Stream piped was terminated by its sender', 499);
  await destroyed;
  assert.strictEqual(finished, false, 'half a stream was finished into the sink as a whole one');
  assert.strictEqual(sink.destroyed, true);
  assert.strictEqual(sink.errored, readable.error);
});

test('client: a download its server terminated, and one cut by the connection, are thrown to the reader', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  const escalated = [];
  client.on('error', (error) => escalated.push(error));
  await client.load('files');

  const first = client.getStream(await client.api.files.part({ size: 1024 * KIB, sent: 256 * KIB }));
  let read = 0;
  const reading = outcome(
    (async () => {
      for await (const chunk of first) read += chunk.length;
    })(),
    2000,
  );
  await waitFor(() => read === 256 * KIB, 'the part that was sent');
  await client.api.files.cut({ id: first.id });
  const cut = await reading;
  assert.strictEqual(cut.status, 'rejected', 'a quarter of a download read as a whole one');
  assert.deepStrictEqual([cut.error.code, first.status, first.bytesRead], [499, 'terminated', 256 * KIB]);

  const second = client.getStream(await client.api.files.part({ size: 1024 * KIB, sent: 256 * KIB }));
  const lost = outcome(second.toBlob(), 2000);
  client.api.files.hangUp({}).catch(() => {});
  const { status, error } = await lost;
  assert.strictEqual(status, 'rejected');
  assert.strictEqual(error.code, 503);
  // The reader's to handle, like a call that failed.
  assert.deepStrictEqual(escalated, []);
});

test("the reader's own terminate() stays the quiet end", async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('files');
  const readable = client.getStream(await client.api.files.part({ size: 1024 * KIB, sent: 256 * KIB }));
  let read = 0;
  for await (const chunk of readable) {
    read += chunk.length;
    await readable.terminate();
  }
  assert.ok(read > 0);
  assert.deepStrictEqual([readable.status, readable.error], ['terminated', null]);
});
