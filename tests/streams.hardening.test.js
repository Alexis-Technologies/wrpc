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
const { WrpcReadable } = require('../src/streams.js');
const { chunkEncode } = require('../src/chunks.js');
const { Client } = require('../src/rpc/client.js');
const { WrpcClient } = require('../src/client.js');
const { defineRouter, procedure } = require('../index.js');
const { bootServer, connectClient, waitFor } = require('./helpers/server.js');
const { recorder } = require('./helpers/recorder.js');

const KIB = 1024;
const bytes = (length, fill = 1) => new Uint8Array(length).fill(fill);
const tick = () => new Promise((resolve) => setImmediate(resolve));

// What a promise did within `ms`. A hang is an answer here — 'pending' —
// instead of a test that times out with nothing to read.
const outcome = async (promise, ms = 500) => {
  const controller = new AbortController();
  const result = await Promise.race([
    Promise.resolve(promise).then(
      (value) => ({ status: 'fulfilled', value }),
      (error) => ({ status: 'rejected', error }),
    ),
    timers.setTimeout(ms, { status: 'pending' }, { signal: controller.signal }).catch(() => null),
  ]);
  controller.abort();
  return result;
};

const drain = async (readable) => {
  let size = 0;
  for await (const chunk of readable) size += chunk.length;
  return size;
};

// A raw protocol peer: what a sender that does not keep its word looks like.
const rawPeer = async (t, url) => {
  const socket = new WebSocket(url, ['wrpc.v2']);
  socket.binaryType = 'arraybuffer';
  t.after(() => socket.close());
  const answers = [];
  socket.addEventListener('message', ({ data }) => {
    if (typeof data === 'string') answers.push(JSON.parse(data));
  });
  await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));
  const send = (packet) => socket.send(JSON.stringify(packet));
  return {
    answers,
    send,
    announce: (id, size, name = 'blob') => send({ type: 'stream', id, name, size }),
    chunk: (id, payload) => socket.send(chunkEncode(id, payload)),
    end: (id, status = 'end') => send({ type: 'stream', id, status }),
    call: (id, method, args = {}) => send({ type: 'call', id, method, args }),
    answer: (id) => answers.find((packet) => packet.id === id),
    // A round trip: everything sent before it was dispatched by the server.
    async settle() {
      const id = `ping-${answers.length}-${Math.random()}`;
      send({ type: 'call', id, method: 'files/ping', args: {} });
      await waitFor(() => answers.some((packet) => packet.id === id), 'the round trip');
    },
  };
};

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
  const client = await connectClient(t, url);
  await client.load('files');
  const upload = client.createStream('big', 200 * 64 * KIB);
  assert.strictEqual(await client.api.files.refuse({ id: upload.id }), 'refused');
  // The sender has not heard: it goes on with the body.
  for (let i = 0; i < 200; i++) upload.write(bytes(64 * KIB));
  assert.strictEqual(await client.api.files.ping({}), 'pong');
  assert.strictEqual(seen.get(upload.id).queue.length, 0, 'the refused upload was still held');
  assert.deepStrictEqual(await client.api.files.held({}), { streams: 1, bytes: 0 });
  upload.end();
  await waitFor(async () => (await client.api.files.held({})).streams === 0, 'the end forgets the stream');
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
  const { url } = await bootServer(t, { router, logger: writer, maxStreamBuffer: 64 * KIB });
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
  assert.strictEqual(find('stream.buffer').max, 64 * KIB);
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

  await t.test("the server's terminate is the quiet end", async () => {
    const { transport, client, escalated, announce, end } = bare();
    announce('s', 8);
    const readable = client.getStream('s');
    const reading = drain(readable);
    transport.emit('message', chunkEncode('s', bytes(4)).buffer);
    await tick();
    end('s', 'terminate');
    assert.strictEqual(await reading, 4);
    assert.strictEqual(readable.status, 'terminated');
    assert.strictEqual(readable.error, null);
    assert.deepStrictEqual(escalated, []);
    await tick();
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
    { type: 'stream', id: stream.id, name: 'empty', size: 0 },
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
