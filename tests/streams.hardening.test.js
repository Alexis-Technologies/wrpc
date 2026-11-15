'use strict';

// The receiving half of a binary stream believed whatever its sender did.
// A readable its handler had let go of kept every chunk still sent into it;
// one nobody read yet was held whole, whatever size it had announced; an
// `end` short or long of that size left `close()` waiting for a count that
// never comes — and the handler's `for await` with it, until the connection
// closed; and a stream of no bytes at all could be announced by one end
// only. Each test below failed (or hung) before the fix it names.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { Emitter } = require('../src/utils.js');
const { WrpcReadable, WrpcWritable, CHUNK_COST } = require('../src/streams.js');
const { chunkEncode, chunkDecode } = require('../src/chunks.js');
const { Client } = require('../src/rpc/client.js');
const { WrpcClient } = require('../src/client.js');
const { defineRouter, procedure, WrpcError } = require('../index.js');
const { bootServer, connectClient, waitFor } = require('./helpers/server.js');
const { recorder } = require('./helpers/recorder.js');
const { KIB, bytes, tick, outcome, drain, rawPeer } = require('./helpers/streams.js');

// The streams a handler was handed, kept by the test: a stream is forgotten
// by its connection at its end, and what it still holds then is the point.
const fixture = () => {
  const seen = new Map();
  const router = defineRouter({
    files: {
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
      read: procedure({
        access: 'public',
        handler: async (context, { id }) => {
          const stream = context.client.getStream(id);
          seen.set(id, stream);
          return drain(stream);
        },
      }),
      refuse: procedure({
        access: 'public',
        handler: async (context, { id }) => {
          const stream = context.client.getStream(id);
          seen.set(id, stream);
          await stream.terminate();
          return 'refused';
        },
      }),
      held: procedure({
        access: 'public',
        handler: async (context) => {
          let held = 0;
          for (const stream of context.client.streams.values()) {
            for (const chunk of stream.queue) held += chunk.length;
          }
          return { streams: context.client.streams.size, bytes: held };
        },
      }),
      download: procedure({
        access: 'public',
        handler: async (context, { size, send = size }) => {
          const stream = context.client.createStream('down', size);
          queueMicrotask(() => {
            if (send > 0) stream.write(bytes(send, 7));
            stream.end();
          });
          return stream.id;
        },
      }),
      // Refuses an upload and says why: the reason its sender is to hear.
      reject: procedure({
        access: 'public',
        handler: async (context, { id, message, code }) => {
          const stream = context.client.getStream(id);
          seen.set(id, stream);
          await stream.terminate(message, code);
          return 'rejected';
        },
      }),
      // A download written a piece a turn for as long as its writable takes
      // one: what a server sending a large file does.
      pour: procedure({
        access: 'public',
        handler: async (context, { size, piece = 16 * KIB }) => {
          const stream = context.client.createStream('pour', size);
          const record = { stream, written: 0, done: false };
          seen.set(stream.id, record);
          const run = async () => {
            while (record.written < size && (await stream.writeAll(bytes(piece, 3)))) {
              record.written += piece;
              await tick();
            }
            stream.end();
            record.done = true;
          };
          queueMicrotask(run);
          return stream.id;
        },
      }),
    },
  });
  return { router, seen };
};

// 1 ─ a stream that is no longer streaming keeps nothing ────────────────

test('WrpcReadable: a stream that is no longer streaming drops what is pushed into it', async (t) => {
  await t.test('after terminate()', async () => {
    const readable = new WrpcReadable('id', 'blob', 1000 * 64 * KIB);
    await readable.terminate();
    for (let i = 0; i < 1000; i++) await readable.push(bytes(64 * KIB));
    assert.strictEqual(readable.queue.length, 0, 'a terminated stream held every chunk pushed after it');
    assert.strictEqual(readable.status, 'terminated');
  });

  await t.test('after close()', async () => {
    const readable = new WrpcReadable('id', 'blob', 3);
    const read = drain(readable);
    await readable.push(bytes(3));
    await readable.close();
    assert.strictEqual(await read, 3);
    await readable.push(bytes(3));
    assert.strictEqual(readable.queue.length, 0);
    assert.strictEqual(readable.status, 'closed');
  });

  await t.test('a chunk that waited for room when the stream ended', async () => {
    const readable = new WrpcReadable('id', 'blob', 100, { highWaterMark: 1 });
    await readable.push(bytes(1));
    await readable.read(); // a consumer attached: the high-water mark applies
    await readable.push(bytes(1));
    await readable.push(bytes(1));
    const parked = readable.push(bytes(1));
    await tick();
    await readable.terminate();
    await parked;
    assert.strictEqual(readable.queue.length, 0, 'the parked chunk was queued into a terminated stream');
  });

  await t.test('the first end decides the status', async () => {
    const readable = new WrpcReadable('id', 'blob', 10);
    await readable.terminate();
    await readable.close();
    assert.strictEqual(readable.status, 'terminated', 'a close() after the fact renamed a terminated stream closed');
  });
});

test('server: a handler that let go of an upload does not keep the rest of it', async (t) => {
  const { router, seen } = fixture();
  const { url } = await bootServer(t, { router });
  // A sender that does not ask to hear of it (a 1.0 client, 2.0.1): it is
  // told nothing, and goes on with the body.
  const peer = await rawPeer(t, url);
  peer.announce('big', 200 * 64 * KIB);
  peer.call('c', 'files/refuse', { id: 'big' });
  await waitFor(() => peer.answer('c'), 'the handler refused');
  for (let i = 0; i < 200; i++) peer.chunk('big', bytes(64 * KIB));
  await peer.settle();
  assert.strictEqual(seen.get('big').queue.length, 0, 'the refused upload was still held');
  peer.call('h', 'files/held');
  await waitFor(() => peer.answer('h'), 'held');
  assert.deepStrictEqual(peer.answer('h').result, { streams: 1, bytes: 0 });
  assert.strictEqual(peer.answer('big'), undefined, 'a sender that did not ask was answered');
  peer.end('big');
  await peer.settle();
  peer.call('h2', 'files/held');
  await waitFor(() => peer.answer('h2'), 'held after the end');
  assert.deepStrictEqual(peer.answer('h2').result, { streams: 0, bytes: 0 }, 'the end forgets the stream');
});

// 2 ─ the announced size is a promise, and what nobody reads is bounded ──

test('WrpcReadable: a chunk past the announced size fails the stream', async () => {
  const readable = new WrpcReadable('id', 'blob', 10);
  const reading = outcome(drain(readable));
  await readable.push(bytes(8));
  await assert.rejects(() => readable.push(bytes(8)), /announced size/);
  assert.strictEqual(readable.streaming, false);
  assert.strictEqual(readable.status, 'terminated');
  assert.match(readable.error.message, /announced size/);
  assert.strictEqual(readable.error.code, 400);
  // Said once: what follows is dropped, not answered chunk by chunk.
  assert.strictEqual(await readable.push(bytes(8)), undefined);
  assert.strictEqual(readable.queue.length, 0);
  const read = await reading;
  assert.strictEqual(read.status, 'rejected', 'the reader of an overshot stream saw nothing wrong');
  assert.strictEqual(read.error, readable.error);
});

test('server: a chunk past the announced size answers 400 once, and the reader sees why', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const peer = await rawPeer(t, url);
  peer.announce('s', 10);
  peer.call('c', 'files/read', { id: 's' });
  for (let i = 0; i < 5; i++) peer.chunk('s', bytes(8));
  await waitFor(() => peer.answer('c'), 'the reader hung on a stream that overshot its size');
  await peer.settle();
  const refusals = peer.answers.filter((packet) => packet.id === 's');
  assert.strictEqual(refusals.length, 1, 'one answer for the stream, not one per chunk');
  assert.strictEqual(refusals[0].error.code, 400);
  assert.match(refusals[0].error.message, /announced size/);
  assert.strictEqual(peer.answer('c').error.code, 400);
  assert.match(peer.answer('c').error.message, /announced size/);
  // Kept — empty — until its sender ends it, so the chunks still on their
  // way are dropped instead of answered "not initialized" one by one.
  peer.call('h', 'files/held');
  await waitFor(() => peer.answer('h'), 'held');
  assert.deepStrictEqual(peer.answer('h').result, { streams: 1, bytes: 0 });
  peer.end('s');
  await peer.settle();
  peer.call('h2', 'files/held');
  await waitFor(() => peer.answer('h2'), 'held after the end');
  assert.deepStrictEqual(peer.answer('h2').result, { streams: 0, bytes: 0 });
  assert.strictEqual(
    peer.answers.filter((packet) => packet.id === 's').length,
    1,
    'the end of a failed stream is quiet',
  );
});

test('server: streams nobody reads yet are capped in bytes per connection (maxStreamBuffer)', async (t) => {
  const { router } = fixture();
  const { writer, find, loud } = recorder();
  // Room for 64 KiB in four chunks: a chunk counts as its bytes and what it
  // costs to hold.
  const cap = 64 * KIB + 4 * CHUNK_COST;
  const { url } = await bootServer(t, { router, logger: writer, maxStreamBuffer: cap });
  const peer = await rawPeer(t, url);
  // Ten bytes announced, a megabyte sent would be caught by the size; a
  // terabyte announced is caught by nothing but the cap.
  peer.announce('a', 2 ** 40);
  peer.announce('b', 2 ** 40);
  for (let i = 0; i < 3; i++) peer.chunk('a', bytes(16 * KIB));
  peer.chunk('b', bytes(16 * KIB));
  await peer.settle();
  assert.strictEqual(peer.answer('a'), undefined, 'under the cap nothing is refused');
  assert.strictEqual(peer.answer('b'), undefined);
  // 64 KiB held across the two; the next byte into either is one too many.
  peer.chunk('b', bytes(1));
  await waitFor(() => peer.answer('b'), 'the stream past the cap was held');
  assert.strictEqual(peer.answer('b').error.code, 413);
  assert.strictEqual(find('stream.buffer').level, 'debug', 'any peer can drive it: not a warning');
  assert.strictEqual(find('stream.buffer').max, cap);
  assert.deepStrictEqual(loud(), []);
  // The one that tipped it is let go of; the other keeps what it had.
  peer.call('h', 'files/held');
  await waitFor(() => peer.answer('h'), 'held');
  assert.deepStrictEqual(peer.answer('h').result, { streams: 2, bytes: 48 * KIB });
  // Its reader is told, as for any failed stream.
  peer.call('rb', 'files/read', { id: 'b' });
  await waitFor(() => peer.answer('rb'), 'the reader of the refused stream');
  assert.strictEqual(peer.answer('rb').error.code, 413);
});

test('server: a stream that failed is answered even when every announced byte had arrived', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router, maxStreamBuffer: 4 + CHUNK_COST });
  const peer = await rawPeer(t, url);
  // Whole, unread, exactly at the cap — and then one byte more: past the
  // cap before it is past the size. Nothing is "left to stop" by the count,
  // but the sender broke its word and is told.
  peer.announce('s', 4);
  peer.chunk('s', bytes(4));
  await peer.settle();
  assert.strictEqual(peer.answer('s'), undefined);
  peer.chunk('s', bytes(1));
  await waitFor(() => peer.answer('s'), 'the sender of a failed stream was told nothing');
  assert.strictEqual(peer.answer('s').error.code, 413);
});

test('server: a stream being read is not counted against maxStreamBuffer', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router, maxStreamBuffer: 64 * KIB });
  const client = await connectClient(t, url);
  await client.load('files');
  const size = 32 * 64 * KIB;
  const upload = client.createStream('big', size);
  const received = client.api.files.read({ id: upload.id });
  await client.api.files.ping({}); // the handler is reading by now
  for (let i = 0; i < 32; i++) upload.write(bytes(64 * KIB));
  upload.end();
  assert.strictEqual(await received, size, 'two megabytes through a 64 KiB cap, read as they came');
});

// 3 ─ an end that does not match the size ends the reader, with a reason ─

test('WrpcReadable: an end short of the announced size fails the reader instead of hanging it', async (t) => {
  await t.test('with a reader waiting for the rest', async () => {
    const readable = new WrpcReadable('id', 'blob', 10);
    const reading = outcome(drain(readable));
    await readable.push(bytes(4));
    const ended = await outcome(readable.end());
    assert.strictEqual(ended.status, 'rejected', `end() of a short stream: ${ended.status}`);
    assert.match(ended.error.message, /4 of 10/);
    assert.strictEqual(ended.error.code, 400);
    const read = await reading;
    assert.strictEqual(read.status, 'rejected', `the reader of a short stream: ${read.status}`);
    assert.strictEqual(read.error, ended.error);
    assert.strictEqual(readable.error, ended.error);
    assert.strictEqual(readable.status, 'terminated');
    assert.strictEqual(readable.bytesRead, 4);
  });

  await t.test('with nobody reading yet: the reader that comes later is told', async () => {
    const readable = new WrpcReadable('id', 'blob', 10);
    await readable.push(bytes(4));
    await assert.rejects(() => readable.end(), /4 of 10/);
    assert.strictEqual(readable.queue.length, 0);
    // close() is what a host waits on before it forgets the stream: not
    // before somebody came to be told.
    const closing = readable.close();
    assert.strictEqual((await outcome(closing, 30)).status, 'pending', 'a failed stream nobody read was let go of');
    await assert.rejects(() => drain(readable), /4 of 10/);
    await closing;
    await assert.rejects(() => readable.toBlob(), /4 of 10/);
    assert.strictEqual(readable.status, 'terminated');
  });

  await t.test('an end that matches still closes once everything was read', async () => {
    const readable = new WrpcReadable('id', 'blob', 6);
    await readable.push(bytes(3));
    await readable.push(bytes(3));
    const ending = readable.end();
    assert.strictEqual(await drain(readable), 6);
    await ending;
    assert.strictEqual(readable.status, 'closed');
    assert.strictEqual(readable.error, null);
  });

  await t.test('terminate() stays the quiet end: the reader stops where it is', async () => {
    const readable = new WrpcReadable('id', 'blob', 10);
    const reading = drain(readable);
    await readable.push(bytes(4));
    await tick();
    await readable.terminate();
    assert.strictEqual(await reading, 4);
    assert.strictEqual(readable.error, null);
  });
});

class Sink extends Emitter {
  chunks = [];
  destroyed = null;

  write(chunk) {
    this.chunks.push(chunk);
    return true;
  }

  end() {
    queueMicrotask(() => this.emit('close'));
  }

  destroy(error) {
    this.destroyed = error ?? true;
  }

  waitEvent(event) {
    return new Promise((resolve) => this.once(event, resolve));
  }

  removeListener(event, listener) {
    this.off(event, listener);
  }
}

test('WrpcReadable.pipe: a failed stream takes its sink with it, and is nobody’s unhandled error', async (t) => {
  await t.test('with nobody listening for it', async (sub) => {
    const unhandled = [];
    const onUnhandled = (reason) => unhandled.push(reason);
    process.on('unhandledRejection', onUnhandled);
    sub.after(() => process.off('unhandledRejection', onUnhandled));
    const readable = new WrpcReadable('id', 'blob', 10);
    const sink = new Sink();
    readable.pipe(sink);
    await readable.push(bytes(4));
    await assert.rejects(() => readable.end(), /4 of 10/);
    await waitFor(() => sink.destroyed !== null, 'the sink of a failed stream was left open');
    assert.strictEqual(sink.destroyed, readable.error);
    await tick();
    assert.deepStrictEqual(unhandled, []);
  });

  await t.test("with an 'error' listener: it hears the reason", async () => {
    const readable = new WrpcReadable('id', 'blob', 10);
    const heard = new Promise((resolve) => readable.once('error', resolve));
    readable.pipe(new Sink());
    await readable.push(bytes(4));
    await assert.rejects(() => readable.end(), /4 of 10/);
    assert.strictEqual(await heard, readable.error);
  });
});

test('server: an end short of the announced size answers the reader instead of hanging it', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const peer = await rawPeer(t, url);
  peer.announce('s', 10);
  peer.call('c', 'files/read', { id: 's' });
  peer.chunk('s', bytes(4));
  peer.end('s');
  await waitFor(() => peer.answer('c'), 'the handler hung on a stream that ended short of its size');
  assert.strictEqual(peer.answer('c').error.code, 400);
  assert.match(peer.answer('c').error.message, /4 of 10/);
  // The sender is told too, on the stream's id — and the stream forgotten.
  await waitFor(() => peer.answer('s'), 'the sender of a short stream was told nothing');
  assert.strictEqual(peer.answer('s').error.code, 400);
  peer.call('h', 'files/held');
  await waitFor(() => peer.answer('h'), 'held');
  assert.deepStrictEqual(peer.answer('h').result, { streams: 0, bytes: 0 });
});

test('server: a stream that ended short before its handler asked for it is still there to say why', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const peer = await rawPeer(t, url);
  peer.announce('s', 10);
  peer.chunk('s', bytes(4));
  peer.end('s');
  await waitFor(() => peer.answer('s'), 'the sender of a short stream was told nothing');
  peer.call('c', 'files/read', { id: 's' });
  await waitFor(() => peer.answer('c'), 'the late reader of a short stream');
  assert.strictEqual(peer.answer('c').error.code, 400);
  assert.match(peer.answer('c').error.message, /4 of 10/, 'the reader was told the stream never existed');
  // Handed to its reader, it is forgotten.
  await waitFor(async () => {
    const id = `h${peer.answers.length}`;
    peer.call(id, 'files/held');
    await waitFor(() => peer.answer(id), 'held');
    return peer.answer(id).result.streams === 0;
  }, 'a failed stream stayed after its reader came');
});

test('server: a status the protocol does not have is refused, and the stream left to its reader', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const peer = await rawPeer(t, url);
  peer.announce('s', 4);
  peer.call('c', 'files/read', { id: 's' });
  await peer.settle();
  peer.end('s', 'finished'); // forgot the stream, and its reader with it
  await waitFor(() => peer.answer('s'), 'an unknown status');
  assert.strictEqual(peer.answer('s').error.code, 400);
  peer.chunk('s', bytes(4));
  peer.end('s');
  await waitFor(() => peer.answer('c'), 'the reader was orphaned by a status nobody knows');
  assert.strictEqual(peer.answer('c').result, 4);
});

test('server: a negative size is not an announcement', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const peer = await rawPeer(t, url);
  peer.announce('s', -1);
  await waitFor(() => peer.answer('s'), 'a stream of minus one byte was accepted');
  assert.strictEqual(peer.answer('s').error.code, 400);
});

test('client: a download that ends short of its size fails the reader instead of hanging it', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('files');
  const escalated = [];
  client.on('error', (error) => escalated.push(error));
  const id = await client.api.files.download({ size: 10, send: 4 });
  await client.api.files.ping({}); // the short end has arrived by now
  const read = await outcome(drain(client.getStream(id)));
  assert.strictEqual(read.status, 'rejected', `the reader of a short download: ${read.status}`);
  assert.match(read.error.message, /4 of 10/);
  // Said on the client's own error channel too, once.
  assert.strictEqual(escalated.length, 1);
  assert.strictEqual(escalated[0], read.error);
  await tick();
  assert.throws(() => client.getStream(id), /not initialized/, 'forgotten once its reader was told');
});

test('client: a download past its size fails the reader', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('files');
  client.on('error', () => {});
  const id = await client.api.files.download({ size: 4, send: 10 });
  const read = await outcome(drain(client.getStream(id)));
  assert.strictEqual(read.status, 'rejected', `the reader of an overshot download: ${read.status}`);
  assert.match(read.error.message, /announced size/);
});

// The client held every chunk back a turn (an awaited conversion) and so
// handled the `end` behind it first — which a readable that counts what it
// was sent would take for a stream that ended empty.
class BareTransport extends Emitter {
  active = true;
  write() {}
  send() {}
  online() {}
  offline() {}
}

test('client: a chunk and the end behind it are counted in the order they arrived', async (t) => {
  const bare = () => {
    const transport = new BareTransport();
    const client = new WrpcClient('fake://x', transport);
    const escalated = [];
    client.on('error', (error) => escalated.push(error));
    const announce = (id, size) =>
      transport.emit('message', JSON.stringify({ type: 'stream', id, name: 'down', size }));
    const end = (id, status = 'end') => transport.emit('message', JSON.stringify({ type: 'stream', id, status }));
    return { transport, client, escalated, announce, end };
  };

  await t.test('bytes: three messages of one read, no turn between them', async () => {
    const { transport, client, escalated, announce, end } = bare();
    announce('s', 4);
    transport.emit('message', chunkEncode('s', bytes(4)).buffer);
    end('s');
    assert.strictEqual(await drain(client.getStream('s')), 4);
    assert.deepStrictEqual(escalated, []);
  });

  await t.test('a Blob (a transport that did not ask for ArrayBuffers): its end waits for the conversion', async () => {
    const { transport, client, escalated, announce, end } = bare();
    announce('s', 8);
    transport.emit('message', new Blob([chunkEncode('s', bytes(4, 1))]));
    transport.emit('message', new Blob([chunkEncode('s', bytes(4, 2))]));
    end('s');
    const parts = [];
    for await (const chunk of client.getStream('s')) parts.push(...chunk);
    assert.deepStrictEqual(parts, [1, 1, 1, 1, 2, 2, 2, 2], 'converted out of order, or after the end');
    assert.deepStrictEqual(escalated, []);
  });

  await t.test('a Blob that cannot be read fails its own message, not the ones behind it', async () => {
    const { transport, client, escalated, announce, end } = bare();
    announce('s', 4);
    transport.emit('message', { arrayBuffer: () => Promise.reject(new Error('unreadable')) });
    transport.emit('message', new Blob([chunkEncode('s', bytes(4))]));
    end('s');
    assert.strictEqual(await drain(client.getStream('s')), 4);
    assert.deepStrictEqual(
      escalated.map((error) => error.message),
      ['unreadable'],
    );
  });

  await t.test('a status the protocol does not have ends nothing', async () => {
    const { transport, client, announce, end } = bare();
    announce('s', 4);
    end('s', 'finished');
    await tick();
    transport.emit('message', chunkEncode('s', bytes(4)).buffer);
    end('s');
    assert.strictEqual(await drain(client.getStream('s')), 4);
  });

  // It was the quiet end: the reader's loop stopped at four of eight bytes
  // as if the stream were over, and whoever read it had to compare counts
  // to know. The sender took its promise back — the reader is told.
  await t.test("the server's terminate fails the stream for its reader", async () => {
    const { transport, client, escalated, announce, end } = bare();
    announce('s', 8);
    const readable = client.getStream('s');
    const reading = outcome(drain(readable));
    transport.emit('message', chunkEncode('s', bytes(4)).buffer);
    await tick();
    end('s', 'terminate');
    const read = await reading;
    assert.strictEqual(read.status, 'rejected', 'half a stream read as a whole one');
    assert.strictEqual(read.error, readable.error);
    assert.deepStrictEqual([readable.error.code, readable.status], [499, 'terminated']);
    assert.match(readable.error.message, /terminated by its sender/);
    // The reader's to handle, like a call that failed: not the client's 'error'.
    assert.deepStrictEqual(escalated, []);
    assert.throws(() => client.getStream('s'), /not initialized/);
  });
});

// 4 ─ a stream of no bytes, on both ends ─────────────────────────────────

test('WrpcReadable: an empty stream waits for its reader, then closes', async (t) => {
  await t.test('ended before anybody read it', async () => {
    const readable = new WrpcReadable('id', 'empty', 0);
    const ending = readable.end();
    await tick();
    assert.strictEqual(readable.streaming, true, 'an empty stream was gone before its reader could ask for it');
    assert.strictEqual(await drain(readable), 0);
    await ending;
    assert.strictEqual(readable.status, 'closed');
  });

  await t.test('read before it ended', async () => {
    const readable = new WrpcReadable('id', 'empty', 0);
    const reading = drain(readable);
    await tick();
    await readable.end();
    assert.strictEqual(await reading, 0);
    assert.strictEqual(readable.status, 'closed');
  });

  await t.test('read() on a stream that already ended answers null, not a wait', async () => {
    const readable = new WrpcReadable('id', 'blob', 10);
    await readable.terminate();
    const read = await outcome(readable.read());
    assert.deepStrictEqual(read, { status: 'fulfilled', value: null });
  });
});

test('server Client.createStream: a stream of no bytes is a stream', () => {
  const sent = [];
  const transport = new (class extends Emitter {
    connection = {};
    send(packet) {
      sent.push(packet);
    }
    write() {
      return true;
    }
  })();
  const client = new Client(transport, { log: null });
  const stream = client.createStream('empty', 0);
  stream.end();
  assert.deepStrictEqual(sent, [
    // `cancel`: this end hears a receiver that no longer wants the stream.
    { type: 'stream', id: stream.id, name: 'empty', size: 0, cancel: true },
    { type: 'stream', id: stream.id, status: 'end' },
  ]);
  assert.throws(() => client.createStream('name'), /Stream size/);
  assert.throws(() => client.createStream('name', -1), /Stream size/);
  assert.throws(() => client.createStream('name', 1.5), /Stream size/);
  assert.throws(() => client.createStream('name', '10'), /Stream size/);
});

test('a stream of no bytes travels both ways', async (t) => {
  const { router } = fixture();
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('files');

  await t.test('up: announced, ended, then read by the handler', async () => {
    const upload = client.createStream('empty', 0);
    const received = client.api.files.read({ id: upload.id });
    upload.end();
    assert.deepStrictEqual(await outcome(received), { status: 'fulfilled', value: 0 });
  });

  await t.test('up: an empty Blob through the uploader', async () => {
    const uploader = client.createBlobUploader(new Blob([]));
    const received = client.api.files.read({ id: uploader.id });
    await uploader.upload();
    assert.deepStrictEqual(await outcome(received), { status: 'fulfilled', value: 0 });
  });

  await t.test('down: ended by the server before the caller asked for it', async () => {
    const id = await client.api.files.download({ size: 0 });
    await client.api.files.ping({}); // the end has arrived by now
    const readable = client.getStream(id);
    assert.deepStrictEqual(await outcome(drain(readable)), { status: 'fulfilled', value: 0 });
    assert.strictEqual(readable.status, 'closed');
  });

  await t.test('nothing is left behind on either end', async () => {
    await waitFor(async () => (await client.api.files.held({})).streams === 0, 'the server forgot them');
  });
});

// 6 ─ every byte of a buffer, written as the transport takes it ──────────

class PacedTransport extends Emitter {
  connection = {};
  accept = true;
  sent = [];
  chunks = [];

  send(packet) {
    this.sent.push(packet);
  }

  write(chunk) {
    this.chunks.push(chunkDecode(chunk).payload);
    return this.accept;
  }
}

test('WrpcWritable.writeAll: a buffer in pieces, each when the transport takes it', async (t) => {
  await t.test('64 KiB at a time, in order, and the end left to the caller', async () => {
    const transport = new PacedTransport();
    const stream = new WrpcWritable('s', 'blob', 150 * KIB, transport);
    const data = new Uint8Array(150 * KIB);
    for (let i = 0; i < data.length; i++) data[i] = i % 251;
    assert.strictEqual(await stream.writeAll(data), true);
    assert.deepStrictEqual(
      transport.chunks.map((chunk) => chunk.length),
      [64 * KIB, 64 * KIB, 22 * KIB],
    );
    assert.deepStrictEqual(Buffer.concat(transport.chunks), Buffer.from(data));
    assert.deepStrictEqual(transport.sent, [{ type: 'stream', id: 's', name: 'blob', size: 150 * KIB }]);
    // Several buffers make one stream.
    assert.strictEqual(await stream.writeAll(Buffer.from('tail'), 2), true);
    assert.deepStrictEqual(
      transport.chunks.slice(3).map((chunk) => Buffer.from(chunk).toString()),
      ['ta', 'il'],
    );
    stream.end();
    assert.strictEqual(transport.sent.at(-1).status, 'end');
  });

  await t.test("a piece the transport did not accept is waited out on 'drain'", async () => {
    const transport = new PacedTransport();
    const stream = new WrpcWritable('s', 'blob', 3, transport);
    transport.accept = false;
    const writing = stream.writeAll(Uint8Array.of(1, 2, 3), 1);
    await tick();
    assert.strictEqual(transport.chunks.length, 1, 'wrote past a transport that said to wait');
    void transport.emit('drain');
    await tick();
    assert.strictEqual(transport.chunks.length, 2);
    transport.accept = true;
    void transport.emit('drain');
    assert.strictEqual(await writing, true);
    assert.strictEqual(transport.chunks.length, 3);
  });

  await t.test('a transport that closes mid-way answers false, and nothing more is written', async () => {
    const transport = new PacedTransport();
    const stream = new WrpcWritable('s', 'blob', 3, transport);
    transport.accept = false;
    const writing = stream.writeAll(Uint8Array.of(1, 2, 3), 1);
    await tick();
    void transport.emit('close');
    assert.deepStrictEqual(await outcome(writing), { status: 'fulfilled', value: false });
    assert.strictEqual(transport.chunks.length, 1);
    assert.strictEqual(await stream.writeAll(Uint8Array.of(4)), false, 'a closed stream takes nothing');
  });

  await t.test("a stream ended under it answers false instead of waiting for a 'drain' that never comes", async () => {
    const transport = new PacedTransport();
    const stream = new WrpcWritable('s', 'blob', 3, transport);
    stream.end();
    assert.deepStrictEqual(await outcome(stream.writeAll(Uint8Array.of(1, 2, 3))), {
      status: 'fulfilled',
      value: false,
    });
    assert.strictEqual(transport.chunks.length, 0);
  });

  await t.test('nothing to write is written', async () => {
    const transport = new PacedTransport();
    const stream = new WrpcWritable('s', 'empty', 0, transport);
    assert.strictEqual(await stream.writeAll(new Uint8Array(0)), true);
    assert.strictEqual(transport.chunks.length, 0);
  });
});

test('writeAll carries a megabyte each way over one connection', async (t) => {
  const size = 1024 * KIB + 123;
  const payload = new Uint8Array(size);
  for (let i = 0; i < size; i++) payload[i] = (i * 31) % 256;
  const { router: base } = fixture();
  const router = base.merge(
    defineRouter({
      blobs: {
        echo: procedure({
          access: 'public',
          handler: async (context, { id }) => {
            const parts = [];
            for await (const chunk of context.client.getStream(id)) parts.push(chunk);
            const body = Buffer.concat(parts);
            const down = context.client.createStream('echo', body.length);
            queueMicrotask(async () => {
              await down.writeAll(body);
              down.end();
            });
            return down.id;
          },
        }),
      },
    }),
  );
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('blobs');
  const up = client.createStream('up', size);
  const answered = client.api.blobs.echo({ id: up.id });
  assert.strictEqual(await up.writeAll(payload), true);
  up.end();
  const parts = [];
  for await (const chunk of client.getStream(await answered)) parts.push(chunk);
  assert.ok(parts.length >= 16, `${parts.length} chunks: the megabyte travelled as one message`);
  assert.deepStrictEqual(Buffer.concat(parts), Buffer.from(payload));
});

// 7 ─ a sender hears what became of its stream ───────────────────────────
//
// A server that refused or failed a stream said so on the stream's id, and
// the client looked the id up among its CALLS: `Callback <id> not found`
// on the error channel, the reason gone, the writer still writing.

const CANCELLED = 499;
const refusal = (message, code) => Object.assign(new Error(message), { code });

test("WrpcWritable.cancel: the receiver's word ends the stream for its writer", async (t) => {
  await t.test('nothing more is written, and the receiver is let go of with a terminate', async () => {
    const transport = new PacedTransport();
    const registry = new Map();
    const stream = new WrpcWritable('s', 'blob', 10, transport, null, registry);
    // Registered, so the answer finds it — and announced as listening.
    assert.strictEqual(registry.get('s'), stream);
    assert.deepStrictEqual(transport.sent, [{ type: 'stream', id: 's', name: 'blob', size: 10, cancel: true }]);
    let closes = 0;
    stream.on('close', () => closes++);
    const error = refusal('Too many bytes in streams nobody reads yet', 413);
    stream.cancel(error);
    assert.strictEqual(stream.closed, true);
    assert.strictEqual(stream.error, error);
    assert.strictEqual(stream.write(bytes(1)), false);
    assert.strictEqual(await stream.writeAll(bytes(4)), false);
    assert.strictEqual(transport.chunks.length, 0, 'a cancelled stream went on writing');
    assert.deepStrictEqual(transport.sent.at(-1), { type: 'stream', id: 's', status: 'terminate' });
    assert.strictEqual(registry.size, 0, 'a cancelled stream stayed registered');
    // Once: a second word, and the writer's own end, change nothing.
    stream.cancel(refusal('again', 400));
    stream.end();
    stream.terminate();
    await tick();
    assert.strictEqual(stream.error, error);
    assert.strictEqual(transport.sent.length, 2);
    assert.strictEqual(closes, 1);
  });

  await t.test("a writer parked on 'drain' is released", async () => {
    const transport = new PacedTransport();
    const stream = new WrpcWritable('s', 'blob', 3, transport, null, new Map());
    transport.accept = false;
    const writing = stream.writeAll(Uint8Array.of(1, 2, 3), 1);
    await tick();
    stream.cancel(refusal('Stream blob is no longer read', CANCELLED));
    assert.deepStrictEqual(await outcome(writing), { status: 'fulfilled', value: false });
    assert.strictEqual(transport.chunks.length, 1);
  });

  await t.test('a stream its writer already ended has nothing left to stop', async () => {
    const transport = new PacedTransport();
    const stream = new WrpcWritable('s', 'blob', 0, transport, null, new Map());
    stream.end();
    stream.cancel(refusal('late', 400));
    assert.strictEqual(stream.closed, false);
    assert.strictEqual(stream.error, null);
    assert.strictEqual(transport.sent.length, 2, 'a terminate behind its own end');
  });

  await t.test('the registry lets go of it at end(), terminate() and the close of the transport', async () => {
    const transport = new PacedTransport();
    const registry = new Map();
    const make = (id) => new WrpcWritable(id, 'blob', 1, transport, null, registry);
    make('a').end();
    make('b').terminate();
    const open = make('c');
    assert.deepStrictEqual([...registry.keys()], ['c']);
    void transport.emit('close');
    assert.strictEqual(registry.size, 0, 'a writable outlived its transport in the registry');
    assert.strictEqual(open.closed, true);
    assert.strictEqual(open.error, null, 'a lost connection is not a reason the receiver gave');
  });

  await t.test('a writable nobody routes answers to does not ask for one', async () => {
    const transport = new PacedTransport();
    const stream = new WrpcWritable('s', 'blob', 1, transport);
    assert.deepStrictEqual(transport.sent, [{ type: 'stream', id: 's', name: 'blob', size: 1 }]);
    assert.strictEqual(stream.error, null);
  });
});

// A client against a real server, its error channel recorded: what used to
// arrive there as a missing callback.
const uploader = async (t, options = {}) => {
  const { router, seen } = fixture();
  const { writer, loud } = recorder();
  const { url } = await bootServer(t, { router, logger: writer, ...options });
  const client = await connectClient(t, url);
  await client.load('files');
  const escalated = [];
  client.on('error', (error) => escalated.push(error));
  const held = async () => (await client.api.files.held({})).streams;
  return { client, escalated, seen, held, loud };
};

const closedSoon = (stream, what) => waitFor(() => stream.closed, { message: what, timeout: 1500 });

test("client: a chunk past the announced size closes the writable with the server's reason", async (t) => {
  const { client, escalated, held } = await uploader(t);
  const upload = client.createStream('big', 10);
  upload.write(bytes(8));
  upload.write(bytes(8));
  await closedSoon(upload, 'the sender of an overshot stream was never told');
  assert.ok(upload.error instanceof WrpcError, `the reason: ${upload.error}`);
  assert.strictEqual(upload.error.code, 400);
  assert.match(upload.error.message, /announced size/);
  assert.strictEqual(upload.write(bytes(1)), false, 'the writer went on after the refusal');
  // Its terminate went out with the refusal: the server forgot the stream
  // without waiting for an end.
  await waitFor(async () => (await held()) === 0, 'the server kept a stream its sender had given up');
  assert.deepStrictEqual(escalated, [], 'the refusal of a stream was reported as a missing callback');
});

test('client: an upload past maxStreamBuffer stops at the 413, and writeAll says it did not all go', async (t) => {
  const { client, escalated } = await uploader(t, { maxStreamBuffer: 64 * KIB });
  const upload = client.createStream('ahead', 2 ** 30);
  assert.strictEqual(await upload.writeAll(bytes(128 * KIB)), true);
  await closedSoon(upload, 'the sender of an upload past the cap was never told');
  assert.strictEqual(upload.error.code, 413);
  assert.match(upload.error.message, /nobody reads yet/);
  assert.strictEqual(await upload.writeAll(bytes(128 * KIB)), false);
  await client.api.files.ping({});
  assert.deepStrictEqual(escalated, []);
});

test('client: an announcement past maxStreams closes that writable with the 429', async (t) => {
  const { client, escalated } = await uploader(t, { maxStreams: 1 });
  const first = client.createStream('a', 10);
  const second = client.createStream('b', 10);
  await closedSoon(second, 'the sender of a stream that was never opened was never told');
  assert.strictEqual(second.error.code, 429);
  assert.match(second.error.message, /Too many open streams/);
  assert.strictEqual(first.closed, false, 'the refusal of one stream closed another');
  // The terminate of a stream the server never held is not an error of its
  // own: nothing comes back for it.
  await client.api.files.ping({});
  assert.deepStrictEqual(escalated, []);
});

test("client: a refusal that arrives after end() is the server's own error, not a missing callback", async (t) => {
  const { client, escalated } = await uploader(t);
  const upload = client.createStream('short', 10);
  upload.write(bytes(4));
  upload.end();
  await waitFor(() => escalated.length > 0, { message: 'a short upload went unreported', timeout: 1500 });
  await client.api.files.ping({});
  assert.strictEqual(escalated.length, 1);
  assert.ok(escalated[0] instanceof WrpcError, `escalated: ${escalated[0]}`);
  assert.strictEqual(escalated[0].code, 400);
  assert.match(escalated[0].message, /4 of 10/);
});

test('client: a callback nobody waits for, with no error in it, is still a missing callback', async () => {
  const transport = new BareTransport();
  const client = new WrpcClient('fake://x', transport);
  const escalated = [];
  client.on('error', (error) => escalated.push(error));
  void transport.emit('message', JSON.stringify({ type: 'callback', id: 'nobody', result: 1 }));
  await tick();
  assert.match(escalated[0].message, /Callback nobody not found/);
});

test('client: a Blob upload the server refuses rejects with its reason', async (t) => {
  const { client, escalated } = await uploader(t, { maxStreamBuffer: 64 * KIB });
  // What createBlobUploader reads of a Blob, a piece a turn — a file read
  // from disk does not arrive in one tick.
  const blob = {
    name: 'big.bin',
    size: 64 * 64 * KIB,
    async *stream() {
      for (let i = 0; i < 64; i++) {
        yield bytes(64 * KIB);
        await timers.setTimeout(5);
      }
    },
  };
  const { upload } = client.createBlobUploader(blob);
  const result = await outcome(upload(), 3000);
  assert.strictEqual(result.status, 'rejected', `the upload of a refused stream: ${result.status}`);
  assert.ok(result.error instanceof WrpcError, `rejected with: ${result.error}`);
  assert.strictEqual(result.error.code, 413);
  await client.api.files.ping({});
  assert.deepStrictEqual(escalated, []);
});

// 8 ─ a receiver that lets go says so to a sender that asked ─────────────
//
// After readable.terminate() — a handler refusing an upload, a page
// dropping a download — the sender sent the whole body all the same, to be
// dropped on arrival.

test('WrpcReadable.onCancel: whoever feeds a stream hears, once, that it is over before its sender ended it', async (t) => {
  const watched = (size = 10) => {
    const readable = new WrpcReadable('id', 'blob', size);
    const heard = [];
    readable.onCancel = (error) => heard.push(error);
    return { readable, heard };
  };

  await t.test('terminate(): the quiet end, no reason', async () => {
    const { readable, heard } = watched();
    await readable.push(bytes(4));
    await readable.terminate();
    await readable.terminate();
    assert.deepStrictEqual(heard, [null]);
    assert.strictEqual(readable.onCancel, null, 'the hook outlived the stream');
  });

  await t.test("terminate(message, code): the reader's reason, and still the quiet end for the reader", async () => {
    const { readable, heard } = watched();
    const reading = drain(readable);
    await readable.push(bytes(4));
    await tick();
    await readable.terminate('Not this one', 422);
    assert.strictEqual(heard.length, 1);
    assert.strictEqual(heard[0].message, 'Not this one');
    assert.strictEqual(heard[0].code, 422);
    assert.strictEqual(await reading, 4, 'the loop of a reader that let go threw');
    assert.strictEqual(readable.error, null);
    // The code is the plain one unless given.
    const other = watched();
    await other.readable.terminate('Changed my mind');
    assert.strictEqual(other.heard[0].code, CANCELLED);
    // terminate handed over as a listener is called with what that emits:
    // not a reason.
    const listened = watched();
    await listened.readable.terminate({ type: 'abort' });
    assert.deepStrictEqual(listened.heard, [null]);
  });

  await t.test('fail(): the error of the stream', async () => {
    const { readable, heard } = watched();
    const error = readable.fail('Too many bytes in streams nobody reads yet', 413);
    await readable.terminate('ignored: the stream was over');
    assert.deepStrictEqual(heard, [error]);
    assert.strictEqual(error.code, 413);
  });

  await t.test('a chunk past the announced size: with the reason', async () => {
    const { readable, heard } = watched();
    await assert.rejects(() => readable.push(bytes(11)), /announced size/);
    assert.deepStrictEqual(heard, [readable.error]);
  });

  await t.test('a stream read to its end: never', async () => {
    const { readable, heard } = watched(4);
    await readable.push(bytes(4));
    const ending = readable.end();
    assert.strictEqual(await drain(readable), 4);
    await ending;
    await readable.terminate();
    assert.deepStrictEqual(heard, []);
    assert.strictEqual(readable.onCancel, null);
  });
});

test('server: a handler that lets go of an upload stops a sender that asked to hear', async (t) => {
  const { client, escalated, held, loud } = await uploader(t);
  const upload = client.createStream('big', 200 * 64 * KIB);
  upload.write(bytes(64 * KIB));
  assert.strictEqual(await client.api.files.refuse({ id: upload.id }), 'refused');
  await closedSoon(upload, 'the sender of a refused upload was never told');
  assert.ok(upload.error instanceof WrpcError);
  assert.strictEqual(upload.error.code, CANCELLED);
  assert.strictEqual(upload.write(bytes(64 * KIB)), false, 'the rest of a refused upload was sent');
  assert.strictEqual(await upload.writeAll(bytes(64 * KIB)), false);
  // The writable's terminate is what lets the server forget the stream.
  await waitFor(async () => (await held()) === 0, 'the server kept a stream both ends had given up');
  assert.deepStrictEqual(escalated, [], 'a reader letting go is not an error');
  assert.deepStrictEqual(loud(), [], 'nor a line an operator is paged for');
});

test('server: a handler that refuses an upload with a reason is heard by its sender', async (t) => {
  const { client, escalated, held } = await uploader(t);
  const upload = client.createStream('big', 200 * 64 * KIB);
  const args = { id: upload.id, message: 'Files over 1 MiB need a plan', code: 402 };
  assert.strictEqual(await client.api.files.reject(args), 'rejected');
  await closedSoon(upload, 'the sender of a rejected upload was never told');
  assert.strictEqual(upload.error.code, 402);
  assert.strictEqual(upload.error.message, 'Files over 1 MiB need a plan');
  await waitFor(async () => (await held()) === 0, 'the server kept the rejected stream');
  // A reason is written for the sender, whatever its code: a 5xx is not
  // replaced by its status line, as a handler's own exception would be.
  const full = client.createStream('big', 200 * 64 * KIB);
  await client.api.files.reject({ id: full.id, message: 'The disk for uploads is full', code: 507 });
  await closedSoon(full, 'the sender of a rejected upload was never told');
  assert.strictEqual(full.error.code, 507);
  assert.strictEqual(full.error.message, 'The disk for uploads is full');
  assert.deepStrictEqual(escalated, []);
});

test('server: the word goes out once, only to a sender that asked, only while bytes are outstanding', async (t) => {
  const { router } = fixture();
  const { writer, loud } = recorder();
  const { url } = await bootServer(t, { router, logger: writer });
  const peer = await rawPeer(t, url);
  const asked = (id, size) => peer.send({ type: 'stream', id, name: 'blob', size, cancel: true });
  const refuse = async (id) => {
    peer.call(`refuse-${id}`, 'files/refuse', { id });
    await waitFor(() => peer.answer(`refuse-${id}`), `the handler refused ${id}`);
    await peer.settle();
  };

  await t.test('asked: one callback on the stream id, code 499', async () => {
    asked('a', 100);
    await refuse('a');
    for (let i = 0; i < 3; i++) peer.chunk('a', bytes(8));
    await peer.settle();
    const answers = peer.answers.filter((packet) => packet.id === 'a');
    assert.strictEqual(answers.length, 1, `${answers.length} answers for one cancelled stream`);
    assert.strictEqual(answers[0].type, 'callback');
    assert.strictEqual(answers[0].error.code, CANCELLED);
    assert.match(answers[0].error.message, /no longer read/);
    // The sender's terminate is the end of it, and is not answered.
    peer.end('a', 'terminate');
    await peer.settle();
    assert.strictEqual(peer.answers.filter((packet) => packet.id === 'a').length, 1);
  });

  await t.test('every byte already here: there is nothing left to stop', async () => {
    asked('b', 4);
    peer.chunk('b', bytes(4));
    await refuse('b');
    assert.strictEqual(peer.answer('b'), undefined, 'a cancel for a stream that had fully arrived');
    peer.end('b');
    // Nor when its reader refuses it with a reason: that is the answer of
    // the call that named the stream.
    asked('b2', 4);
    peer.chunk('b2', bytes(4));
    peer.call('reject-b2', 'files/reject', { id: 'b2', message: 'Not this one', code: 422 });
    await waitFor(() => peer.answer('reject-b2'), 'the handler rejected b2');
    await peer.settle();
    assert.strictEqual(peer.answer('b2'), undefined, 'the refusal of a stream that had fully arrived');
    peer.end('b2');
  });

  await t.test("a reader's reason is still a reader letting go: not said to a sender that did not ask", async () => {
    peer.announce('c', 100);
    peer.call('reject-c', 'files/reject', { id: 'c', message: 'Not this one', code: 422 });
    await waitFor(() => peer.answer('reject-c'), 'the handler rejected c');
    await peer.settle();
    assert.strictEqual(peer.answer('c'), undefined, 'a sender that did not ask was answered');
    peer.end('c', 'terminate');
  });

  await t.test('the terminate of a stream this end does not hold is not answered', async () => {
    peer.end('never-announced', 'terminate');
    await peer.settle();
    assert.strictEqual(peer.answer('never-announced'), undefined);
    // An end is: it promises bytes this end never saw.
    peer.end('never-announced');
    await waitFor(() => peer.answer('never-announced'), 'the end of an unknown stream');
    assert.strictEqual(peer.answer('never-announced').error.code, 400);
    // Named by its id alone: an `end` carries no name, and the message used
    // to read "Stream never-announced/undefined is not initialized".
    assert.strictEqual(peer.answer('never-announced').error.message, 'Stream never-announced is not initialized');
  });

  await t.test('nothing of it is a warning', async () => {
    peer.call('h', 'files/held');
    await waitFor(() => peer.answer('h'), 'held');
    assert.strictEqual(peer.answer('h').result.streams, 0);
    assert.deepStrictEqual(
      loud().filter((entry) => entry.code !== 400),
      [],
    );
  });
});

test('client: a reader that lets go of a download stops the server that sent it', async (t) => {
  const { client, escalated, seen, loud } = await uploader(t);
  const size = 64 * 1024 * KIB;
  const id = await client.api.files.pour({ size });
  const readable = client.getStream(id);
  assert.ok((await readable.read()).length > 0);
  await readable.terminate();
  const record = seen.get(id);
  await waitFor(() => record.stream.closed, {
    message: 'the server went on with a download nobody reads',
    timeout: 1500,
  });
  assert.strictEqual(record.stream.error.code, CANCELLED);
  assert.match(record.stream.error.message, /no longer read/);
  await waitFor(() => record.done, 'the writing loop stopped');
  assert.ok(record.written < size, `all ${size} bytes of a dropped download were written`);
  // The server's terminate answers it: the client forgets the stream.
  await waitFor(() => {
    try {
      client.getStream(id);
      return false;
    } catch {
      return true;
    }
  }, 'the client kept a stream both ends had given up');
  await client.api.files.ping({});
  assert.deepStrictEqual(escalated, []);
  assert.deepStrictEqual(loud(), []);
});

// What a client says to the sender of a download, packet by packet.
class RecordingTransport extends BareTransport {
  sent = [];
  send(packet) {
    this.sent.push(packet);
  }
}

test('client: the word goes out once, only to a server that asked, only while bytes are outstanding', async (t) => {
  const bare = () => {
    const transport = new RecordingTransport();
    const client = new WrpcClient('fake://x', transport);
    client.on('error', () => {});
    const message = (packet) => transport.emit('message', JSON.stringify(packet));
    const announce = (id, size, cancel) => message({ type: 'stream', id, name: 'down', size, cancel });
    const chunk = (id, length) => transport.emit('message', chunkEncode(id, bytes(length)).buffer);
    return { transport, client, message, announce, chunk };
  };

  await t.test('asked: one callback on the stream id, code 499', async () => {
    const { transport, client, announce, chunk } = bare();
    announce('s', 100, true);
    chunk('s', 8);
    await client.getStream('s').terminate();
    await client.getStream('s').terminate();
    assert.deepStrictEqual(transport.sent, [
      { type: 'callback', id: 's', error: { message: 'Stream down is no longer read', code: CANCELLED } },
    ]);
  });

  await t.test('not asked (a 1.0 server, 2.0.1): nothing', async () => {
    const { transport, client, announce } = bare();
    announce('s', 100);
    await client.getStream('s').terminate();
    assert.deepStrictEqual(transport.sent, []);
  });

  await t.test("the server's own terminate, and its end: nothing", async () => {
    const { transport, client, message, announce, chunk } = bare();
    announce('s', 100, true);
    message({ type: 'stream', id: 's', status: 'terminate' });
    announce('short', 100, true);
    chunk('short', 8);
    message({ type: 'stream', id: 'short', status: 'end' });
    await tick();
    assert.throws(() => client.getStream('s'), /not initialized/);
    assert.deepStrictEqual(transport.sent, []);
  });

  await t.test('every byte already here: nothing', async () => {
    const { transport, client, announce, chunk } = bare();
    announce('s', 8, true);
    chunk('s', 8);
    await client.getStream('s').terminate();
    announce('t', 8, true);
    chunk('t', 8);
    await client.getStream('t').terminate('Not this one', 422);
    assert.deepStrictEqual(transport.sent, []);
  });

  await t.test("a reader's own reason, while bytes are still to come: the server is told it", async () => {
    const { transport, client, announce, chunk } = bare();
    announce('s', 8, true);
    chunk('s', 4);
    await client.getStream('s').terminate('Not this one', 422);
    assert.deepStrictEqual(transport.sent, [
      { type: 'callback', id: 's', error: { message: 'Not this one', code: 422 } },
    ]);
  });

  await t.test('a chunk past the announced size: the server is told why', async () => {
    const { transport, announce, chunk } = bare();
    announce('s', 4, true);
    chunk('s', 8);
    chunk('s', 8);
    await tick();
    assert.strictEqual(transport.sent.length, 1);
    assert.strictEqual(transport.sent[0].error.code, 400);
    assert.match(transport.sent[0].error.message, /announced size/);
  });

  await t.test('a connection that closed: nobody is left to tell', async () => {
    const { transport, client, announce } = bare();
    announce('s', 100, true);
    const readable = client.getStream('s');
    void transport.emit('close');
    await tick();
    assert.strictEqual(readable.status, 'terminated');
    assert.deepStrictEqual(transport.sent, []);
  });

  await t.test('a cancel that crossed the end of its stream is nobody’s error', async () => {
    const { transport, message } = bare();
    const escalated = [];
    const client = new WrpcClient('fake://y', transport);
    client.on('error', (error) => escalated.push(error));
    void client;
    message({ type: 'callback', id: 'ended-long-ago', error: { message: 'Stream up is no longer read', code: 499 } });
    await tick();
    assert.deepStrictEqual(escalated, []);
  });
});

test('server: a client that lets go of a download it was sent closes the writable; one that crossed its end is quiet', async (t) => {
  const { router, seen } = fixture();
  const { writer, loud, entries } = recorder();
  const { url } = await bootServer(t, { router, logger: writer });
  const peer = await rawPeer(t, url);
  peer.call('p', 'files/pour', { size: 64 * 1024 * KIB });
  await waitFor(() => peer.answer('p'), 'the download was opened');
  const id = peer.answer('p').result;
  const announced = peer.answers.find((packet) => packet.type === 'stream' && packet.id === id);
  assert.strictEqual(announced.cancel, true, 'the server did not ask to hear of a cancel');
  // The peer is the sender of what it answers: only the message and code
  // of its error are kept, and the message is bounded.
  const error = { message: 'x'.repeat(4096), code: 499, details: { nested: true }, stack: 'forged' };
  peer.send({ type: 'callback', id, error });
  const record = seen.get(id);
  await waitFor(() => record.stream.closed, { message: 'the writable went on', timeout: 1500 });
  assert.strictEqual(record.stream.error.code, 499);
  assert.ok(record.stream.error.message.length <= 256, 'a peer chose the length of a server-side message');
  assert.strictEqual(record.stream.error.details, undefined);
  await peer.settle();
  const ended = peer.answers.filter((packet) => packet.type === 'stream' && packet.id === id && packet.status);
  assert.deepStrictEqual(
    ended.map((packet) => packet.status),
    ['terminate'],
  );
  // Again, for a stream that is over by now: the cancel that crossed its
  // end. Not an ask's answer gone astray, and not a line to be paged for.
  peer.send({ type: 'callback', id, error: { message: 'late', code: 499 } });
  await peer.settle();
  assert.deepStrictEqual(loud(), []);
  // Anything else nothing waits for still is one.
  peer.send({ type: 'callback', id: 'never-asked', error: { message: 'what', code: 400 } });
  await peer.settle();
  assert.strictEqual(entries.filter((entry) => entry.level === 'warn').length, 1);
});
