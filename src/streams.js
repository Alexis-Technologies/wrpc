'use strict';

const { Emitter } = require('./utils.js');
const { chunkEncode } = require('./chunks.js');

const PUSH_EVENT = Symbol('push');
const PULL_EVENT = Symbol('pull');
const DEFAULT_HIGH_WATER_MARK = 32;
const MAX_LISTENERS = 10;
const MAX_HIGH_WATER_MARK = 1000;

class WrpcReadable extends Emitter {
  #consuming = false;
  queue = [];
  streaming = true;
  status = 'active';
  bytesRead = 0;
  highWaterMark = DEFAULT_HIGH_WATER_MARK;

  constructor(id, name, size, options = {}) {
    super();
    this.id = id;
    this.name = name;
    this.size = size;
    const { highWaterMark } = options;
    if (highWaterMark) this.highWaterMark = highWaterMark;
  }

  // The high-water mark applies only once a consumer attached: before that
  // nobody emits PULL_EVENT, so blocking here would deadlock producers whose
  // consumer starts from a later packet on the same paused socket (the
  // upload-then-call wire pattern).
  async push(data) {
    while (this.#consuming && this.queue.length > this.highWaterMark) {
      this.checkStreamLimits();
      await this.waitEvent(PULL_EVENT);
    }
    this.queue.push(data);
    if (this.queue.length === 1) this.emit(PUSH_EVENT);
    return data;
  }

  async finalize(writable) {
    const onError = () => this.terminate();
    writable.once('error', onError);
    for await (const chunk of this) {
      // A closed sink accepts nothing and will never emit 'drain'
      if (writable.closed) {
        await this.terminate();
        writable.removeListener('error', onError);
        return;
      }
      const needDrain = !writable.write(chunk);
      if (needDrain && !writable.closed) await writable.waitEvent('drain');
    }
    this.emit('end');
    writable.end();
    await writable.waitEvent('close');
    await this.close();
    writable.removeListener('error', onError);
  }

  pipe(writable) {
    this.finalize(writable).catch((error) => this.emit('error', error));
    return writable;
  }

  async toBlob(type = '') {
    const chunks = [];
    for await (const chunk of this) {
      chunks.push(chunk);
    }
    return new Blob(chunks, { type });
  }

  async close() {
    await this.stop(false);
    this.status = 'closed';
  }

  async terminate() {
    await this.stop(true);
    this.status = 'terminated';
  }

  async stop(force = false) {
    if (!force) {
      while (this.streaming && this.bytesRead !== this.size) {
        await this.waitEvent(PULL_EVENT);
      }
    }
    this.streaming = false;
    this.queue.length = 0;
    this.emit(PUSH_EVENT, null);
    this.emit(PULL_EVENT);
  }

  async read() {
    this.#consuming = true;
    if (this.queue.length > 0) return this.pull();
    const finisher = await this.waitEvent(PUSH_EVENT);
    if (finisher === null) return null;
    return this.pull();
  }

  pull() {
    this.#consuming = true;
    const data = this.queue.shift();
    /* c8 ignore next -- read()/waitEvent(PUSH_EVENT) callers already guard against an empty queue */
    if (!data) return data;
    this.bytesRead += data.length;
    this.emit(PULL_EVENT);
    return data;
  }

  checkStreamLimits() {
    if (this.listenerCount(PULL_EVENT) >= MAX_LISTENERS) {
      ++this.highWaterMark;
    }
    /* c8 ignore start -- defensive guard; needs 1000+ concurrent stalled backpressure waiters to reach */
    if (this.highWaterMark > MAX_HIGH_WATER_MARK) {
      throw new Error('Stream overflow occurred');
    }
    /* c8 ignore stop */
  }

  waitEvent(event) {
    return new Promise((resolve) => this.once(event, resolve));
  }

  async *[Symbol.asyncIterator]() {
    while (this.streaming) {
      const chunk = await this.read();
      if (!chunk) return;
      yield chunk;
    }
  }
}

class WrpcWritable extends Emitter {
  #waitingDrain = false;
  #closeArmed = false;
  #closed = false;

  constructor(id, name, size, transport) {
    super();
    this.id = id;
    this.name = name;
    this.size = size;
    this.transport = transport;
    this.init();
  }

  get closed() {
    return this.#closed;
  }

  init() {
    const { id, name, size } = this;
    const packet = { type: 'stream', id, name, size };
    this.transport.send(packet);
  }

  // Reports the transport's real acceptance: false means the socket buffer
  // is above its high-water mark — wait for this stream's 'drain' event.
  // A false return can also mean the transport closed: check `closed` (or
  // listen for 'close') — after it, no 'drain' will ever follow.
  // Transports without flow-control reporting (browser WebSocket, HTTP)
  // always count as accepted.
  write(data) {
    if (this.#closed) return false;
    const chunk = chunkEncode(this.id, data);
    const accepted = this.transport.write(chunk) !== false;
    if (!accepted && !this.#waitingDrain && typeof this.transport.once === 'function') {
      this.#waitingDrain = true;
      const release = () => {
        if (!this.#waitingDrain) return;
        this.#waitingDrain = false;
        this.emit('drain');
      };
      this.transport.once('drain', release);
      if (!this.#closeArmed) {
        this.#closeArmed = true;
        this.transport.once('close', () => {
          this.#closed = true;
          release();
          this.emit('close');
        });
      }
    }
    return accepted;
  }

  end() {
    const packet = { type: 'stream', id: this.id, status: 'end' };
    this.transport.send(packet);
  }

  terminate() {
    const packet = { type: 'stream', id: this.id, status: 'terminate' };
    this.transport.send(packet);
  }
}

module.exports = { WrpcReadable, WrpcWritable };
