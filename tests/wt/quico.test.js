'use strict';

// fromQuico over a fake of quico's request/response shape — the real quico
// is a devDependency exercised only by the guarded integration test.

const { test } = require('node:test');
const assert = require('node:assert');
const { Duplex, Writable } = require('node:stream');

const { fromQuico, isWtSession } = require('../../wt.js');

const fakeQuico = () => {
  const listeners = {};
  const sent = { datagrams: [], head: null, ended: false, streams: [] };
  const req = {
    headers: {
      ':method': 'CONNECT',
      ':protocol': 'webtransport',
      ':path': '/api?x=1',
      origin: 'https://a',
      'User-Agent': 'q',
    },
    url: '/api?x=1',
    on(event, fn) {
      (listeners[event] ??= []).push(fn);
    },
  };
  const emit = (event, value) => {
    for (const fn of listeners[event] ?? []) fn(value);
  };
  const duplex = () => {
    const chunks = [];
    const d = new Duplex({
      read() {},
      write(chunk, _enc, cb) {
        chunks.push(Buffer.from(chunk));
        cb();
      },
    });
    d.written = chunks;
    return d;
  };
  const res = {
    writeHead(status) {
      sent.head = status;
    },
    end() {
      sent.ended = true;
    },
    sendDatagram(data) {
      sent.datagrams.push(Buffer.from(data));
    },
    createBidirectionalStream() {
      const d = duplex();
      sent.streams.push(d);
      return d;
    },
    createUnidirectionalStream() {
      const w = new Writable({
        write(_c, _e, cb) {
          cb();
        },
      });
      return w;
    },
  };
  return { req, res, emit, sent, duplex };
};

test('wt quico: the request becomes a W3C-shaped session plus the CONNECT request', async () => {
  const { req, res, emit, sent, duplex } = fakeQuico();
  const { session, headers, url, remoteAddress } = fromQuico(req, res);
  assert.strictEqual(sent.head, 200);
  assert.strictEqual(isWtSession(session), true);
  assert.deepStrictEqual(headers, { origin: 'https://a', 'user-agent': 'q' });
  assert.strictEqual(url, '/api?x=1');
  assert.strictEqual(remoteAddress, '');

  // An incoming stream: the node Duplex bridged to a WHATWG pair.
  const incoming = duplex();
  emit('stream', incoming);
  const reader = session.incomingBidirectionalStreams.getReader();
  const { value: stream } = await reader.read();
  incoming.push(Buffer.from('hello'));
  const r = stream.readable.getReader();
  const { value } = await r.read();
  assert.strictEqual(Buffer.from(value).toString(), 'hello');
  const w = stream.writable.getWriter();
  await w.write(new Uint8Array([1, 2]));
  assert.deepStrictEqual(Array.from(incoming.written[0]), [1, 2]);

  // Outgoing streams and datagrams go through res.
  const opened = await session.createBidirectionalStream();
  const ow = opened.writable.getWriter();
  await ow.write(new Uint8Array([3]));
  assert.deepStrictEqual(Array.from(sent.streams[0].written[0]), [3]);
  const dw = session.datagrams.writable.getWriter();
  await dw.write(new Uint8Array([4, 5]));
  await dw.write(new Uint8Array(2000)); // above the size: dropped
  assert.deepStrictEqual(
    sent.datagrams.map((d) => Array.from(d)),
    [[4, 5]],
  );
  emit('datagram', Buffer.from([6]));
  const dr = session.datagrams.readable.getReader();
  assert.deepStrictEqual(Array.from((await dr.read()).value), [6]);

  // close() ends the CONNECT response and settles closed.
  session.close({ closeCode: 1001, reason: 'bye' });
  assert.strictEqual(sent.ended, true);
  assert.deepStrictEqual(await session.closed, { closeCode: 1001, reason: 'bye' });
  assert.deepStrictEqual(await reader.read(), { value: undefined, done: true });
});

test('wt quico: a peer end settles closed; the inputs are validated', async () => {
  const { req, res, emit } = fakeQuico();
  const { session } = fromQuico(req, res, { accept: false });
  emit('end');
  assert.deepStrictEqual(await session.closed, { closeCode: 0, reason: '' });
  assert.throws(() => fromQuico({}, res), TypeError);
  assert.throws(() => fromQuico({ headers: {}, on() {} }, res), /not a WebTransport request/);
});
