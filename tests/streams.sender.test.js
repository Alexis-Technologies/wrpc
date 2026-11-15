'use strict';

// The sending half of a binary stream: what a writer is told, and what it
// is not allowed to say. Each test below failed before the fix it names.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure } = require('../index.js');
const { bootServer, connectClient, waitFor } = require('./helpers/server.js');
const { KIB, bytes, outcome } = require('./helpers/streams.js');

const fixture = () => {
  const router = defineRouter({
    files: {
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
      refuse: procedure({
        access: 'public',
        handler: async (context, { id }) => {
          await context.client.getStream(id).terminate('Quota exceeded', 413);
          return 'refused';
        },
      }),
      hangUp: procedure({ access: 'public', handler: async (context) => void context.client.close() }),
    },
  });
  return { router };
};

// A Blob-shaped source whose last read comes a while after its last chunk:
// a file read from disk, a body from the network.
const lagging = (size, gap, name = 'report.pdf') => ({
  name,
  size,
  stream: () =>
    (async function* source() {
      yield bytes(size / 2);
      await timers.setTimeout(gap);
    })(),
});

// upload() looked at `closed` after each write — and not after the wait for
// the source's end, in which the server's refusal had arrived. `end()` on a
// closed stream is a no-op: the upload RESOLVED, no 'error' was raised, and
// the reason the server gave was on a writable nobody looked at again.
test('upload() rejects with a refusal that arrived while its source was ending', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  const escalated = [];
  client.on('error', (error) => escalated.push(error));
  await client.load('files');
  const { id, upload } = client.createBlobUploader(lagging(128 * KIB, 150));
  const uploading = outcome(upload(), 2000);
  assert.strictEqual(await client.api.files.refuse({ id }), 'refused');
  const { status, error } = await uploading;
  assert.strictEqual(status, 'rejected', 'an upload its server refused resolved');
  assert.deepStrictEqual([error.code, error.message], [413, 'Quota exceeded']);
  assert.deepStrictEqual(escalated, []);
});

test('upload() rejects when the connection closes while its source is ending', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  client.on('error', () => {});
  await client.load('files');
  const { upload } = client.createBlobUploader(lagging(128 * KIB, 150));
  const uploading = outcome(upload(), 2000);
  await client.api.files.ping({});
  client.api.files.hangUp({}).catch(() => {});
  await waitFor(() => !client.active, 'the connection closed');
  const { status, error } = await uploading;
  assert.strictEqual(status, 'rejected', 'an upload cut by its connection resolved');
  assert.strictEqual(error.code, 503);
});
