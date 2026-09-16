'use strict';

// The quico adapter: quico (a pure-JS HTTP/3 stack) surfaces a WebTransport
// session as an http-shaped request — `req.headers[':protocol'] ===
// 'webtransport'`, accepted with `res.writeHead(200)`, streams as node
// Duplexes on `req.on('stream')` and `res.createBidirectionalStream()`,
// datagrams as Buffers on `req.on('datagram')` and `res.sendDatagram()`.
// fromQuico() turns that into the W3C-shaped session the port names, node
// streams bridged to WHATWG ones with the built-in toWeb() adapters, and
// hands back the CONNECT request alongside. Nothing is required from quico:
// the request and response are whatever its server passed the handler.

const { Readable, Writable } = require('node:stream');

const { isWtStream } = require('./port.js');

// A readable whose controller stays reachable — what a node event source
// is pushed into, and what a close ends.
const source = () => {
  let controller = null;
  let ended = false;
  const readable = new ReadableStream({
    start(c) {
      controller = c;
    },
  });
  return {
    readable,
    push(value) {
      if (!ended) controller.enqueue(value);
    },
    end() {
      if (ended) return;
      ended = true;
      try {
        controller.close();
      } catch {
        // Cancelled by its reader.
      }
    },
  };
};

const toWtStream = (duplex) => {
  if (isWtStream(duplex)) return duplex;
  return { readable: Readable.toWeb(duplex), writable: Writable.toWeb(duplex) };
};

/**
 * `{ session, headers, url, remoteAddress }` for one accepted WebTransport
 * request — spread the last three into attachSession's options. Accepts the
 * request (`writeHead(200)`) unless `accept: false`, in which case the
 * caller has already done so.
 */
const fromQuico = (req, res, { accept = true, maxDatagramSize = 1200 } = {}) => {
  if (typeof req?.on !== 'function' || typeof res?.writeHead !== 'function') {
    throw new TypeError('fromQuico: a quico request and response are required');
  }
  if (req.headers?.[':protocol'] !== 'webtransport') {
    throw new TypeError("fromQuico: not a WebTransport request (':protocol' is not 'webtransport')");
  }
  if (accept) res.writeHead(200);
  const bidi = source();
  const uni = source();
  const datagramsIn = source();
  let settleClosed = null;
  const closed = new Promise((resolve) => {
    settleClosed = resolve;
  });
  let done = false;
  const finish = (info) => {
    if (done) return;
    done = true;
    bidi.end();
    uni.end();
    datagramsIn.end();
    settleClosed(info);
  };
  req.on('stream', (duplex) => bidi.push(toWtStream(duplex)));
  req.on('unidirectionalStream', (readable) => uni.push(Readable.toWeb(readable)));
  req.on('datagram', (data) => datagramsIn.push(new Uint8Array(data.buffer, data.byteOffset, data.byteLength)));
  // quico 0.4 reports none of these for a session that vanished — pair the
  // adapter with attachSession's idleTimeout for liveness.
  for (const event of ['close', 'end', 'error', 'aborted']) {
    req.on(event, () => finish({ closeCode: 0, reason: '' }));
  }
  const session = {
    ready: Promise.resolve(),
    closed,
    incomingBidirectionalStreams: bidi.readable,
    incomingUnidirectionalStreams: uni.readable,
    datagrams: {
      readable: datagramsIn.readable,
      writable: new WritableStream({
        write(chunk) {
          if (chunk.byteLength > maxDatagramSize) return;
          res.sendDatagram(Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength));
        },
      }),
      maxDatagramSize,
    },
    async createBidirectionalStream() {
      const duplex = res.createBidirectionalStream();
      if (!duplex) throw new Error('quico: the session is not open');
      return toWtStream(duplex);
    },
    async createUnidirectionalStream() {
      const writable = res.createUnidirectionalStream();
      if (!writable) throw new Error('quico: the session is not open');
      return Writable.toWeb(writable);
    },
    // quico has no session close of its own: ending the CONNECT response
    // ends the session (the capsule protocol's FIN), which is what a peer
    // observes as the close.
    close(info = {}) {
      finish({ closeCode: info.closeCode ?? 0, reason: info.reason ?? '' });
      try {
        res.end();
      } catch {
        // Already ended.
      }
    },
  };
  const headers = {};
  for (const name in req.headers) {
    if (name.charCodeAt(0) === 58) continue;
    const value = req.headers[name];
    if (value !== undefined && value !== null) headers[name.toLowerCase()] = String(value);
  }
  const url = typeof req.url === 'string' ? req.url : (req.headers[':path'] ?? '/');
  const remoteAddress = req.socket?.remoteAddress ?? req.remoteAddress ?? '';
  return { session, headers, url, remoteAddress };
};

module.exports = { fromQuico };
