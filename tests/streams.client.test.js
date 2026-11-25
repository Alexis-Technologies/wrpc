'use strict';

// The receiving half of a stream on the CLIENT. A server caps what a client
// may make it hold; a client held whatever its server sent — and what sends
// the streams may be a peer nobody vouches for. Each test below failed
// before the client had limits of its own.

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { defineRouter, procedure } = require('../index.js');
const { bootServer, connectClient, waitFor } = require('./helpers/server.js');
const { KIB, MIB, bytes, outcome, drain } = require('./helpers/streams.js');

const fixture = () => {
  const downloads = new Map();
  const router = defineRouter({
    files: {
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
      // `count` downloads of `size` bytes each, written whole and ended.
      flood: procedure({
        access: 'public',
        handler: async (context, { count, size, end = true }) => {
          const ids = [];
          for (let i = 0; i < count; i++) {
            const stream = context.client.createStream(`flood-${i}`, size);
            downloads.set(stream.id, stream);
            ids.push(stream.id);
            await stream.writeAll(bytes(size));
            if (end) stream.end();
          }
          return ids;
        },
      }),
      // One download, written a piece at a time for as long as it is taken.
      pour: procedure({
        access: 'public',
        handler: async (context, { size, piece }) => {
          const stream = context.client.createStream('pour.bin', size);
          downloads.set(stream.id, stream);
          queueMicrotask(async () => {
            for (let sent = 0; sent < size && !stream.closed; sent += piece) {
              await stream.writeAll(bytes(piece));
              await new Promise((resolve) => setImmediate(resolve));
            }
            stream.end();
          });
          return stream.id;
        },
      }),
    },
  });
  return { router, downloads };
};

const holds = (client, ids) =>
  ids.filter((id) => {
    try {
      client.getStream(id);
      return true;
    } catch {
      return false;
    }
  });

describe('client streams', () => {
  it('should cap the streams a server may have open here (maxStreams)', async (t) => {
    const { router, downloads } = fixture();
    const { url } = await bootServer(t, { router });
    const client = await connectClient(t, url, { maxStreams: 4, streamIdleTimeout: Infinity });
    const escalated = [];
    client.on('error', (error) => escalated.push(error));
    await client.load('files');
    const ids = await client.api.files.flood({ count: 10, size: 1 * KIB, end: false });
    await client.api.files.ping({});
    assert.strictEqual(holds(client, ids).length, 4, 'a server opened as many streams here as it liked');
    // The six that were refused: their sender asked to hear, and was told.
    const refused = ids.filter((id) => !holds(client, ids).includes(id));
    await waitFor(() => refused.every((id) => downloads.get(id).closed), 'a refused download was never stopped');
    for (const id of refused) assert.strictEqual(downloads.get(id).error.code, 429);
    // What it sent into a stream it was refused, and its terminate, are nobody's error here.
    assert.deepStrictEqual(
      escalated.filter((error) => !/is not initialized/.test(error.message)),
      [],
    );
  });

  it('should cap what unread downloads hold (maxStreamBuffer)', async (t) => {
    const { router, downloads } = fixture();
    const { url } = await bootServer(t, { router });
    const client = await connectClient(t, url, { maxStreamBuffer: 256 * KIB, streamIdleTimeout: Infinity });
    client.on('error', () => {});
    await client.load('files');
    const id = await client.api.files.pour({ size: 4 * MIB, piece: 64 * KIB });
    // Held, and not read.
    const readable = client.getStream(id);
    const writable = downloads.get(id);
    await waitFor(() => writable.closed, 'megabytes nobody reads were taken under a 256 KiB cap');
    assert.strictEqual(writable.error.code, 413);
    assert.match(writable.error.message, /nobody reads yet/);
    assert.ok(readable.bytesReceived <= 256 * KIB, `${readable.bytesReceived} bytes were held`);
    // Its reader, when one comes, is told why.
    const { status, error } = await outcome(drain(readable));
    assert.strictEqual(status, 'rejected');
    assert.strictEqual(error.code, 413);
  });

  it('should hold a reader that falls behind to the same cap — nothing pauses a server here', async (t) => {
    const { router, downloads } = fixture();
    const { url } = await bootServer(t, { router });
    const client = await connectClient(t, url, { maxStreamBuffer: 512 * KIB });
    client.on('error', () => {});
    await client.load('files');
    const id = await client.api.files.pour({ size: 16 * MIB, piece: 16 * KIB });
    const readable = client.getStream(id);
    assert.ok((await readable.read()).length > 0); // a reader, which then stalls
    const writable = downloads.get(id);
    await waitFor(() => writable.closed, 'a download ran sixteen megabytes ahead of a reader that had stopped');
    assert.strictEqual(writable.error.code, 413);
    assert.match(writable.error.message, /too far ahead of its reader/);
    assert.strictEqual(readable.error.code, 413);
  });

  it('should not count a download that is read as it comes', async (t) => {
    const { router } = fixture();
    const { url } = await bootServer(t, { router });
    const client = await connectClient(t, url, { maxStreamBuffer: 256 * KIB });
    const escalated = [];
    client.on('error', (error) => escalated.push(error));
    await client.load('files');
    const size = 4 * MIB;
    const id = await client.api.files.pour({ size, piece: 64 * KIB });
    assert.strictEqual(await drain(client.getStream(id)), size);
    assert.deepStrictEqual(escalated, []);
  });
});
