'use strict';

// What an operator can see of a stream. A 413, a 429 and a 499 are said at
// debug in the log, because any peer can drive them — and were in no metric
// at all: `wrpc.server.stream.bytes` was the one instrument, and it missed
// the chunk a cap refused. `wrpc.server.streams` counts how each stream ended.

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { defineRouter, procedure } = require('../index.js');
const { bootServer, connectClient, waitFor } = require('./helpers/server.js');
const { createMetrics } = require('./helpers/metrics.js');
const { KIB, bytes, drain } = require('./helpers/streams.js');

const fixture = () => {
  const downloads = new Map();
  const router = defineRouter({
    files: {
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
      read: procedure({ access: 'public', handler: async (context, { id }) => drain(context.client.getStream(id)) }),
      letGo: procedure({
        access: 'public',
        handler: async (context, { id }) => void context.client.getStream(id).terminate(),
      }),
      download: procedure({
        access: 'public',
        handler: async (context, { size, sent, end }) => {
          const stream = context.client.createStream('down.bin', size);
          downloads.set(stream.id, stream);
          await stream.writeAll(bytes(sent));
          if (end) stream.end();
          return stream.id;
        },
      }),
    },
  });
  return { router, downloads };
};

// The data points of an instrument as `direction outcome code -> count`.
const ended = (metrics) => {
  const out = {};
  const metric = metrics.find((entry) => entry.descriptor.name === 'wrpc.server.streams');
  for (const point of metric?.dataPoints ?? []) {
    const {
      'wrpc.stream.direction': direction,
      'wrpc.stream.outcome': outcome,
      'rpc.wrpc.status_code': code,
    } = point.attributes;
    out[`${direction} ${outcome}${code === undefined ? '' : ` ${code}`}`] = point.value;
  }
  return out;
};

const received = (metrics) =>
  metrics
    .find((entry) => entry.descriptor.name === 'wrpc.server.stream.bytes')
    ?.dataPoints.find((point) => point.attributes['wrpc.stream.direction'] === 'receive')?.value ?? 0;

describe('stream telemetry', () => {
  it('should count in wrpc.server.streams how every stream ended, by direction, outcome and code', async (t) => {
    const { router, downloads } = fixture();
    const { meter, collect } = createMetrics();
    const { url } = await bootServer(t, { router, telemetry: { meter }, maxStreams: 2, maxStreamBuffer: 256 * KIB });
    const client = await connectClient(t, url);
    client.on('error', () => {});
    await client.load('files');

    // An upload read to its end.
    const whole = client.createStream('whole.bin', 64 * KIB);
    const reading = client.api.files.read({ id: whole.id });
    await whole.writeAll(bytes(64 * KIB));
    whole.end();
    assert.strictEqual(await reading, 64 * KIB);

    // One past `maxStreamBuffer`: 512 KiB nobody reads.
    const capped = client.createStream('capped.bin', 512 * KIB);
    await capped.writeAll(bytes(512 * KIB));
    await waitFor(() => capped.closed, 'the cap');

    // One its handler lets go of, and one its sender gives up.
    const refused = client.createStream('refused.bin', 512 * KIB);
    refused.write(bytes(16 * KIB));
    await client.api.files.letGo({ id: refused.id });
    await waitFor(() => refused.closed, 'the handler let go');
    const given = client.createStream('given.bin', 512 * KIB);
    given.write(bytes(16 * KIB));
    await client.api.files.ping({});
    given.terminate();

    // One announcement too many.
    const held = [client.createStream('a', 8), client.createStream('b', 8)];
    const over = client.createStream('c', 8);
    await waitFor(() => over.closed, 'the announcement past maxStreams');
    for (const stream of held) stream.terminate();

    // Downloads: one read to its end, one its reader lets go of.
    assert.strictEqual(
      await drain(client.getStream(await client.api.files.download({ size: 8, sent: 8, end: true }))),
      8,
    );
    const open = await client.api.files.download({ size: 512 * KIB, sent: 16 * KIB, end: false });
    await client.getStream(open).terminate();
    await waitFor(() => downloads.get(open).closed, 'the reader let go of the download');
    await client.api.files.ping({});

    assert.deepStrictEqual(ended(await collect()), {
      'receive closed': 1,
      'receive failed 413': 1,
      'receive terminated': 1,
      // The sender's own terminate: cut short, for its reader.
      'receive failed 499': 3,
      'receive refused 429': 1,
      'send closed': 1,
      'send cancelled 499': 1,
    });
  });

  it('should count in wrpc.server.stream.bytes what arrived, the chunk a cap refused included', async (t) => {
    const { router } = fixture();
    const { meter, collect } = createMetrics();
    const { url } = await bootServer(t, { router, telemetry: { meter }, maxStreamBuffer: 256 * KIB });
    const client = await connectClient(t, url);
    client.on('error', () => {});
    await client.load('files');
    const upload = client.createStream('capped.bin', 512 * KIB);
    // Eight pieces written in one turn: all of them leave before the server answers.
    await upload.writeAll(bytes(512 * KIB));
    await waitFor(() => upload.closed, 'the cap');
    await client.api.files.ping({});
    assert.strictEqual(received(await collect()), 512 * KIB, 'what the peer sent is not what the counter saw');
  });

  it('should count a connection that closes under a download as lost', async (t) => {
    const { router, downloads } = fixture();
    const { meter, collect } = createMetrics();
    const { url } = await bootServer(t, { router, telemetry: { meter } });
    const client = await connectClient(t, url);
    client.on('error', () => {});
    await client.load('files');
    const id = await client.api.files.download({ size: 512 * KIB, sent: 16 * KIB, end: false });
    client.close();
    await waitFor(() => downloads.get(id).closed, 'the transport closed under the writable');
    assert.deepStrictEqual(ended(await collect()), { 'send lost': 1 });
  });
});
