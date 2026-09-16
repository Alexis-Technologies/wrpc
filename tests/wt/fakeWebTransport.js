'use strict';

// An in-repo fake of the W3C WebTransport subset wrpc speaks — the client
// object a page constructs (`new WebTransport(url, init)`) and the server
// session the port names (src/webtransport/port.js) — connected in memory.
// A world hands out the constructor and the sessions it accepts, the latter
// as a ReadableStream in the shape @fails-components/webtransport's
// sessionStream() has, so acceptSessions() is exercised for real.
//
// Deliberately faithful in the places a transport gets wrong:
//   - a write() COPIES the chunk and delivers it asynchronously (a reused
//     buffer is safe, a retained view never aliases the sender's memory);
//   - close() before `ready` rejects `ready`; close() settles `closed` on
//     BOTH ends with the close info, ends every readable and fails every
//     writable — a read in progress sees `done`, a queued write rejects;
//   - a datagram above maxDatagramSize is dropped silently (Chrome), and
//     world.lossy(rate) drops a share of the rest, invisibly, like a network;
//   - the server session carries the CONNECT request as fails does: a
//     `header` object with the pseudo-headers (`:path` keeps the query) and
//     `origin`, and a `peerAddress`.
//
// Not a *.test.js — a helper for tests/wt/*.test.js.

const copy = (chunk) => {
  if (chunk instanceof Uint8Array) return chunk.slice();
  if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk.slice(0));
  if (ArrayBuffer.isView(chunk)) {
    return new Uint8Array(chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength));
  }
  throw new TypeError('fake: a WebTransport stream carries bytes');
};

// A readable whose controller stays reachable, so a session close can end it.
const readableSource = () => {
  let controller = null;
  let ended = false;
  const readable = new ReadableStream({
    start(c) {
      controller = c;
    },
  });
  return {
    readable,
    push(chunk) {
      if (ended) return false;
      controller.enqueue(chunk);
      return true;
    },
    end() {
      if (ended) return;
      ended = true;
      try {
        controller.close();
      } catch {
        // A cancelled readable has no controller to close.
      }
    },
    fail(error) {
      if (ended) return;
      ended = true;
      try {
        controller.error(error);
      } catch {
        // Already errored.
      }
    },
  };
};

// One direction of a stream: the writable end here, the readable end at the
// peer. Delivery is asynchronous (a microtask), and gated while the world
// holds writes — how a test provokes backpressure.
const writableSink = (world, deliver, { end = null, fail = null } = {}) => {
  let closed = false;
  const writable = new WritableStream({
    async write(chunk) {
      if (closed) throw new Error('fake: the stream is closed');
      const bytes = copy(chunk);
      await world.gate();
      if (closed) throw new Error('fake: the stream is closed');
      deliver(bytes);
    },
    close() {
      closed = true;
      if (end) end();
    },
    abort(reason) {
      closed = true;
      // A RESET_STREAM: the peer's read errors, it does not end.
      if (fail) fail(reason instanceof Error ? reason : new Error('fake: the stream was reset'));
      else if (end) end();
    },
  });
  return {
    writable,
    fail() {
      closed = true;
    },
  };
};

class FakeSession {
  ready;
  closed;
  incomingBidirectionalStreams;
  incomingUnidirectionalStreams;
  datagrams;
  // Server side only, the CONNECT request; the fails shape.
  header = null;
  peerAddress = null;
  url = null;
  #world;
  #peer = null;
  #settleReady;
  #settleClosed;
  #closed = false;
  #bidi = readableSource();
  #uni = readableSource();
  #datagramsIn = readableSource();
  #streams = [];
  // Test-visible: unidirectional streams opened by this end.
  uniOpened = 0;

  constructor(world) {
    this.#world = world;
    this.ready = new Promise((resolve, reject) => {
      this.#settleReady = { resolve, reject };
    });
    this.closed = new Promise((resolve, reject) => {
      this.#settleClosed = { resolve, reject };
    });
    // Never unhandled: a session nobody awaits `closed` on must not crash the run.
    this.closed.catch(() => {});
    this.ready.catch(() => {});
    this.incomingBidirectionalStreams = this.#bidi.readable;
    this.incomingUnidirectionalStreams = this.#uni.readable;
    const out = writableSink(world, (bytes) => {
      if (bytes.length > world.maxDatagramSize) return;
      if (world.random() < world.lossRate) return;
      this.#peer?.receiveDatagram(bytes);
    });
    this.datagrams = {
      readable: this.#datagramsIn.readable,
      writable: out.writable,
      maxDatagramSize: world.maxDatagramSize,
    };
    this.#streams.push({ fail: out.fail, end: () => this.#datagramsIn.end() });
  }

  get open() {
    return !this.#closed;
  }

  /** @internal */
  link(peer) {
    this.#peer = peer;
  }

  /** @internal */
  accept() {
    this.#settleReady.resolve();
  }

  /** @internal */
  refuse(error) {
    this.#closed = true;
    this.#settleReady.reject(error);
    this.#settleClosed.reject(error);
  }

  /** @internal */
  receiveDatagram(bytes) {
    this.#datagramsIn.push(bytes);
  }

  /** @internal — the peer opened a stream towards us. */
  incoming(kind, stream) {
    if (kind === 'bidi') this.#bidi.push(stream);
    else this.#uni.push(stream);
  }

  // A pair of directions; `toPeer` is what the peer reads, `fromPeer` what
  // we read. The peer receives the mirror image.
  #pair() {
    const here = readableSource();
    const there = readableSource();
    const toPeer = writableSink(this.#world, (bytes) => there.push(bytes), {
      end: () => there.end(),
      fail: (error) => there.fail(error),
    });
    const fromPeer = writableSink(this.#world, (bytes) => here.push(bytes), {
      end: () => here.end(),
      fail: (error) => here.fail(error),
    });
    const local = { readable: here.readable, writable: toPeer.writable };
    const remote = { readable: there.readable, writable: fromPeer.writable };
    this.#streams.push({ fail: toPeer.fail, end: () => here.end() });
    this.#peer.registerStream({ fail: fromPeer.fail, end: () => there.end() });
    return { local, remote };
  }

  /** @internal */
  registerStream(handle) {
    this.#streams.push(handle);
  }

  async createBidirectionalStream() {
    await this.ready;
    if (this.#closed) throw new Error('fake: the session is closed');
    const { local, remote } = this.#pair();
    this.#peer.incoming('bidi', remote);
    return local;
  }

  async createUnidirectionalStream() {
    await this.ready;
    if (this.#closed) throw new Error('fake: the session is closed');
    // A host that never grants unidirectional streams (quico 0.4): Chrome's
    // QuotaExceededError.
    if (this.#world.uniQuota === 0) throw new DOMException('No streams available', 'QuotaExceededError');
    this.uniOpened++;
    const { local, remote } = this.#pair();
    this.#peer.incoming('uni', remote.readable);
    return local.writable;
  }

  close(info = {}) {
    if (this.#closed) return;
    const closeInfo = { closeCode: info.closeCode ?? 0, reason: info.reason ?? '' };
    this.#teardown(closeInfo);
    this.#peer?.#teardown(closeInfo);
  }

  #teardown(closeInfo) {
    if (this.#closed) return;
    this.#closed = true;
    this.#settleReady.reject(new Error('fake: the session closed before it was ready'));
    for (let i = 0; i < this.#streams.length; i++) {
      this.#streams[i].fail();
      this.#streams[i].end();
    }
    this.#bidi.end();
    this.#uni.end();
    this.#datagramsIn.end();
    this.#settleClosed.resolve(closeInfo);
  }
}

/**
 * A world: `WebTransport` to construct client sessions with, `sessions` (a
 * ReadableStream of the server halves, in fails' sessionStream() shape) or
 * `next()` to take them one at a time, and the knobs.
 */
const createFakeWt = ({ maxDatagramSize = 1200, random = Math.random, origin = 'https://app.example' } = {}) => {
  const accepted = readableSource();
  let reader = null;
  let refusal = null;
  let held = null;
  const world = {
    maxDatagramSize,
    random,
    lossRate: 0,
    // -1 = unlimited; 0 = createUnidirectionalStream() rejects (no credit).
    uniQuota: -1,
    origin,
    sessions: accepted.readable,
    /** The next accepted server session. */
    async next() {
      reader ??= accepted.readable.getReader();
      const { value, done } = await reader.read();
      if (done) throw new Error('fake: the world is closed');
      return value;
    },
    /** Drops this share of datagrams, silently. */
    lossy(rate) {
      world.lossRate = rate;
    },
    /** The next connection's handshake fails: `ready` and `closed` reject with `error`. */
    refuse(error = new Error('fake: connection refused')) {
      refusal = error;
    },
    /** Holds every write until the returned release() — backpressure on demand. */
    hold() {
      let release;
      held = new Promise((resolve) => {
        release = resolve;
      });
      return () => {
        held = null;
        release();
      };
    },
    /** @internal */
    gate() {
      return held ?? Promise.resolve();
    },
    close() {
      accepted.end();
    },
  };

  class WebTransport extends FakeSession {
    constructor(url, init = {}) {
      super(world);
      this.url = url;
      this.init = init;
      // The handshake is asynchronous like the real one: a close() in the
      // same turn abandons it.
      queueMicrotask(() => {
        if (!this.open) return;
        if (refusal) {
          const error = refusal;
          refusal = null;
          this.refuse(error);
          return;
        }
        const server = new FakeSession(world);
        const parsed = new URL(url);
        server.header = {
          ':method': 'CONNECT',
          ':protocol': 'webtransport',
          ':scheme': parsed.protocol.slice(0, -1),
          ':authority': parsed.host,
          ':path': `${parsed.pathname}${parsed.search}`,
          origin,
          'user-agent': 'fake-wt/1',
        };
        server.peerAddress = '127.0.0.1:4433';
        server.url = url;
        this.link(server);
        server.link(this);
        server.accept();
        this.accept();
        accepted.push(server);
      });
    }
  }

  world.WebTransport = WebTransport;
  return world;
};

module.exports = { createFakeWt, FakeSession };
