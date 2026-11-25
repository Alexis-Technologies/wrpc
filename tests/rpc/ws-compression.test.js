'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const zlib = require('node:zlib');

const { RpcServer, defineRouter, procedure, WrpcClient } = require('../../index.js');
const { FRAME_MARK, FRAME_PACKET_COMPRESSED, FRAME_CHUNK_COMPRESSED } = require('../../src/wire.js');
const { ProtocolClient } = require('../websocket/protocolClient.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');
const { recorder } = require('../helpers/recorder.js');
const { createMetrics, point } = require('../helpers/metrics.js');
const { suiteScope } = require('../helpers/scope.js');

const DEFLATE = 'deflate-raw';

const router = defineRouter({
  data: {
    echo: procedure({ access: 'public', handler: async (_ctx, args) => args }),
    big: procedure({
      access: 'public',
      handler: async (_ctx, { rows }) => Array.from({ length: rows }, (_, i) => ({ i, name: `row-${i}` })),
    }),
    upload: procedure({
      access: 'public',
      handler: async (ctx, { stream }) => {
        let bytes = 0;
        for await (const chunk of ctx.client.getStream(stream)) bytes += chunk.length;
        return bytes;
      },
    }),
  },
});

// Every frame the server's socket receives from a client, and the server
// Client it was attached as — through the one public seam every upgrade
// goes through.
const spyInbound = (server) => {
  const frames = [];
  const clients = [];
  const attachSocket = server.rpc.attachSocket.bind(server.rpc);
  server.rpc.attachSocket = (socket, meta) => {
    socket.on('message', (data, isBinary) => frames.push({ isBinary, bytes: Buffer.from(data) }));
    const client = attachSocket(socket, meta);
    clients.push(client);
    return client;
  };
  return { frames, clients };
};

const binaryFrames = (frames) => frames.filter((f) => f.isBinary);
const markedFrames = (frames) => binaryFrames(frames).filter((f) => f.bytes[0] === FRAME_MARK);

describe('ws compression', () => {
  it('should be off by default — a Node client sends text frames, nothing negotiated', async (t) => {
    const { server, url } = await bootServer(t, { router });
    const { frames, clients } = spyInbound(server);
    const client = await connectClient(t, url);
    await client.load('data');
    const echoed = await client.api.data.echo({ text: 'y'.repeat(20_000) });
    assert.strictEqual(echoed.text.length, 20_000);
    assert.strictEqual(binaryFrames(frames).length, 0);
    assert.strictEqual(clients[0].compression, null);
  });

  describe('both ends on — negotiated over ping/pong, packets and chunks past the threshold go compressed', () => {
    const scope = suiteScope();
    let frames = null;
    let clients = null;
    let client = null;
    after(() => scope.close());
    before(async () => {
      const { server, url } = await bootServer(scope, { router, compression: true });
      ({ frames, clients } = spyInbound(server));
      client = await connectClient(scope, url, { compression: true });
      await waitFor(() => clients.length === 1 && clients[0].compression !== null, 'never negotiated');
      assert.strictEqual(clients[0].compression.id, DEFLATE);
    });

    it('should send a large argument up as a marked binary frame that inflates to the packet', async () => {
      await client.load('data');
      frames.length = 0;
      const echoed = await client.api.data.echo({ text: 'y'.repeat(20_000) });
      assert.strictEqual(echoed.text.length, 20_000);
      const marked = markedFrames(frames);
      assert.strictEqual(marked.length, 1);
      const frame = marked[0].bytes;
      assert.strictEqual(frame[1], FRAME_PACKET_COMPRESSED);
      assert.ok(frame.length < 500, `${frame.length} B on the wire for a 20 KB packet`);
      const packet = JSON.parse(zlib.inflateRawSync(frame.subarray(2)).toString());
      assert.strictEqual(packet.method, 'data/echo');
    });

    it('should keep a small packet a text frame', async () => {
      frames.length = 0;
      const rows = await client.api.data.big({ rows: 5 });
      assert.strictEqual(rows.length, 5);
      assert.strictEqual(binaryFrames(frames).length, 0);
    });

    it('should compress an upload chunk past the threshold too, and reassemble it', async () => {
      frames.length = 0;
      const up = client.createStream('blob', 30_000);
      const call = client.api.data.upload({ stream: up.id });
      up.write(new Uint8Array(30_000).fill(7));
      up.end();
      assert.strictEqual(await call, 30_000);
      const chunks = markedFrames(frames).filter((f) => f.bytes[1] === FRAME_CHUNK_COMPRESSED);
      assert.ok(chunks.length >= 1);
      assert.ok(
        chunks.every((f) => f.bytes.length < 1000),
        'each 30 KB chunk is a few hundred bytes',
      );
    });
  });

  it('should stay plain with one side on and the other off — nothing breaks', async (t) => {
    const plainServer = await bootServer(t, { router });
    const spyA = spyInbound(plainServer.server);
    const eager = await connectClient(t, plainServer.url, { compression: true });
    await eager.load('data');
    assert.strictEqual((await eager.api.data.echo({ text: 'y'.repeat(20_000) })).text.length, 20_000);
    assert.strictEqual(binaryFrames(spyA.frames).length, 0, 'the server answered a plain pong');
    assert.strictEqual(spyA.clients[0].compression, null);

    const onServer = await bootServer(t, { router, compression: true });
    const spyB = spyInbound(onServer.server);
    const plain = await connectClient(t, onServer.url);
    await plain.load('data');
    assert.strictEqual((await plain.api.data.echo({ text: 'y'.repeat(20_000) })).text.length, 20_000);
    assert.strictEqual(binaryFrames(spyB.frames).length, 0);
    assert.strictEqual(spyB.clients[0].compression, null);
  });

  // ---- the wire, by hand --------------------------------------------------

  const rawClient = (t, url) =>
    new Promise((resolve, reject) => {
      const socket = new ProtocolClient(url.replace(/^ws/, 'ws'));
      const messages = [];
      socket.on('message', (payload) => messages.push(payload.toString()));
      socket.once('open', () => resolve({ socket, messages }));
      socket.once('error', reject);
      t.after(() => socket.close());
    });

  const nextMessage = async (messages, index) => {
    await waitFor(() => messages.length > index, 'no answer');
    return JSON.parse(messages[index]);
  };

  it('should carry enc in the pong only when the server agreed and refuse frames before that, on the wire', async (t) => {
    const log = recorder();
    const metrics = createMetrics();
    const { url } = await bootServer(t, {
      router,
      compression: true,
      maxMessage: 4096,
      logger: log.writer,
      telemetry: { meter: metrics.meter },
    });
    const { socket, messages } = await rawClient(t, url);
    const call = Buffer.from(JSON.stringify({ type: 'call', id: 'c1', method: 'data/big', args: { rows: 3 } }));
    const frame = (kind, body) => Buffer.concat([Buffer.from([FRAME_MARK, kind]), body]);

    // Before any negotiation a marked frame is a 400, not a hang-up.
    socket.sendBinary(frame(FRAME_PACKET_COMPRESSED, zlib.deflateRawSync(call)));
    let answer = await nextMessage(messages, 0);
    assert.strictEqual(answer.error.code, 400);

    // Another codec: the pong names nothing, and nothing is enabled.
    socket.sendText(JSON.stringify({ type: 'ping', enc: 'brotli' }));
    answer = await nextMessage(messages, 1);
    assert.deepStrictEqual(answer, { type: 'pong' });

    // The platform codec: agreed.
    socket.sendText(JSON.stringify({ type: 'ping', enc: DEFLATE }));
    answer = await nextMessage(messages, 2);
    assert.deepStrictEqual(answer, { type: 'pong', enc: DEFLATE });

    // A compressed call packet now dispatches.
    socket.sendBinary(frame(FRAME_PACKET_COMPRESSED, zlib.deflateRawSync(call)));
    answer = await nextMessage(messages, 3);
    assert.strictEqual(answer.id, 'c1');
    assert.strictEqual(answer.result.length, 3);

    // An unknown kind, and a body that inflates past maxMessage: both 400.
    socket.sendBinary(frame(9, zlib.deflateRawSync(call)));
    answer = await nextMessage(messages, 4);
    assert.strictEqual(answer.error.code, 400);
    socket.sendBinary(frame(FRAME_PACKET_COMPRESSED, zlib.deflateRawSync(Buffer.alloc(100_000, 0x20))));
    answer = await nextMessage(messages, 5);
    assert.strictEqual(answer.error.code, 400);
    // And bytes that are not a deflate stream at all: 400 as well — but the
    // two do not read the same in the log. One is the cap; the other is the
    // bytes (garbage, or a dictionary the two ends do not share).
    socket.sendBinary(frame(FRAME_PACKET_COMPRESSED, Buffer.from('this was never deflated')));
    answer = await nextMessage(messages, 6);
    assert.strictEqual(answer.error.code, 400);
    const inflates = log.all('frame.refused').filter((line) => line.reason === 'inflate');
    assert.deepStrictEqual(
      inflates.map((line) => [line.codec, line.code]),
      [
        [DEFLATE, 'ERR_BUFFER_TOO_LARGE'],
        [DEFLATE, 'Z_DATA_ERROR'],
      ],
    );
    const counted = point(await metrics.collect(), 'wrpc.compression.failures');
    assert.strictEqual(counted.value, 2);
    assert.deepStrictEqual(counted.attributes, {
      'wrpc.compression.carrier': 'ws',
      'wrpc.compression.direction': 'decode',
    });

    // A plain text packet still works on the same connection.
    socket.sendText(JSON.stringify({ type: 'call', id: 'c2', method: 'data/big', args: { rows: 2 } }));
    answer = await nextMessage(messages, 7);
    assert.strictEqual(answer.result.length, 2);
  });

  describe('with a codec list: the pong names the first codec of the CLIENT’s list the server holds', () => {
    const scope = suiteScope();
    let url = '';
    let frames = null;
    let clients = null;
    after(() => scope.close());
    before(async () => {
      const booted = await bootServer(scope, { router, compression: { codec: ['deflate-raw', 'brotli'] } });
      url = booted.url;
      ({ frames, clients } = spyInbound(booted.server));
    });

    it('should pick from a list on the wire, junk in it skipped, the client’s order deciding', async (t) => {
      const { socket, messages } = await rawClient(t, url);
      socket.sendText(JSON.stringify({ type: 'ping', enc: ['lz4', 7, 'brotli', DEFLATE] }));
      assert.deepStrictEqual(await nextMessage(messages, 0), { type: 'pong', enc: 'brotli' });
      const call = Buffer.from(JSON.stringify({ type: 'call', id: 'c1', method: 'data/big', args: { rows: 3 } }));
      socket.sendBinary(
        Buffer.concat([Buffer.from([FRAME_MARK, FRAME_PACKET_COMPRESSED]), zlib.brotliCompressSync(call)]),
      );
      assert.strictEqual((await nextMessage(messages, 1)).result.length, 3);
      // Nothing in common: a plain pong, and the earlier agreement is gone.
      socket.sendText(JSON.stringify({ type: 'ping', enc: ['lz4'] }));
      assert.deepStrictEqual(await nextMessage(messages, 2), { type: 'pong' });
      socket.sendText(JSON.stringify({ type: 'ping', enc: {} }));
      assert.deepStrictEqual(await nextMessage(messages, 3), { type: 'pong' });
    });

    it('should fall back to the next codec of a client that prefers one the server lacks, not to plain', async (t) => {
      const mine = { id: 'mine', encode: (b) => zlib.deflateRawSync(b), decode: (b) => zlib.inflateRawSync(b) };
      const client = await connectClient(t, url, { compression: { codec: [mine, 'brotli', 'deflate-raw'] } });
      await waitFor(() => clients.at(-1).compression !== null, 'never negotiated');
      assert.strictEqual(clients.at(-1).compression.id, 'brotli');
      await client.load('data');
      frames.length = 0;
      const echoed = await client.api.data.echo({ text: 'y'.repeat(20_000) });
      assert.strictEqual(echoed.text.length, 20_000);
      const [marked] = markedFrames(frames);
      const packet = JSON.parse(zlib.brotliDecompressSync(marked.bytes.subarray(2)).toString());
      assert.strictEqual(packet.args.text.length, 20_000, 'the frame is Brotli, as the pong said');
    });
  });

  it('should answer every ping plainly from a server without the option, on the wire', async (t) => {
    const { url } = await bootServer(t, { router });
    const { socket, messages } = await rawClient(t, url);
    socket.sendText(JSON.stringify({ type: 'ping', enc: DEFLATE }));
    assert.deepStrictEqual(await nextMessage(messages, 0), { type: 'pong' });
  });

  it('should stub the browser half — a page compresses under permessage-deflate instead', () => {
    const { createWsCompression } = require('../../src/client/wsCompression.browser.js');
    assert.strictEqual(createWsCompression(true), null);
    assert.strictEqual(createWsCompression({ threshold: 1 }), null);
  });

  it('should validate the options', () => {
    assert.throws(() => new RpcServer({ router, compression: 'lz4' }), /compression must be true, false/);
    assert.throws(() => new RpcServer({ router, maxMessage: 0 }), /maxMessage/);
    const asyncCodec = { id: 'x', encode: async (b) => b, decode: async (b) => b };
    assert.throws(() => new RpcServer({ router, compression: { codec: asyncCodec } }), /synchronously/);
  });

  it('should fail a client with a bad option at open', async (t) => {
    const { url } = await bootServer(t, { router, compression: true });
    await assert.rejects(
      WrpcClient.connect(url, { compression: 'lz4', reconnect: false, heartbeat: false, logger: false }),
      /compression must be true, false/,
    );
  });

  it('should have the Node half say what was agreed — nothing before the pong, the codec after', () => {
    const { createWsCompression } = require('../../src/client/wsCompression.js');
    assert.strictEqual(createWsCompression(false), null);
    const compression = createWsCompression({ threshold: 1 });
    assert.deepStrictEqual(JSON.parse(compression.offer), { type: 'ping', enc: ['deflate-raw'] });
    // Before the server answered: nothing is active and nothing is framed.
    assert.strictEqual(compression.active, false);
    assert.strictEqual(compression.id, null);
    assert.strictEqual(compression.encode('x'.repeat(200)), null);
    // What is not a pong is not an answer; a pong without `enc` is a no.
    assert.strictEqual(compression.accept('{"type":"event","name":"a/b"}'), null);
    assert.strictEqual(compression.accept(new Uint8Array(4)), null);
    assert.strictEqual(compression.accept('{"type":"pong"}'), false);
    assert.strictEqual(compression.accept('{"type":"pong","enc":"brotli"}'), false, 'a codec this side never offered');
    assert.strictEqual(compression.active, false);
    // The pong names the one codec to send with.
    assert.strictEqual(compression.accept('{"type":"pong","enc":"deflate-raw"}'), true);
    assert.strictEqual(compression.active, true);
    assert.strictEqual(compression.id, 'deflate-raw');
    const text = 'x'.repeat(200);
    const packet = compression.encode(text);
    assert.deepStrictEqual([packet[0], packet[1]], [0, 3], 'a packet under the 0x00 marker, kind 3');
    assert.strictEqual(zlib.inflateRawSync(packet.subarray(2)).toString(), text);
    const chunk = compression.encode(Buffer.alloc(200, 7));
    assert.deepStrictEqual([chunk[0], chunk[1]], [0, 4], 'a chunk is kind 4');
    assert.strictEqual(compression.encode('{}'), null, 'what does not get smaller leaves as it is');
  });
});
