'use strict';

const { EventEmitter } = require('events');

class MockSocket extends EventEmitter {
  #isCorked = false;
  #pendingWrites = [];

  constructor() {
    super();
    this.writtenData = [];
    this.ended = false;
    this.destroyed = false;
    this.paused = false;
    // Backpressure modelling: write() reports this value, writableLength
    // mimics the kernel/stream buffer size, drain() releases the pressure.
    this.writeResult = true;
    this.writableLength = 0;
  }

  cork() {
    this.#isCorked = true;
  }

  uncork() {
    this.#isCorked = false;
    if (this.#pendingWrites.length) {
      const combined = Buffer.isBuffer(this.#pendingWrites[0])
        ? Buffer.concat(this.#pendingWrites)
        : this.#pendingWrites.join('');
      this.write(combined);
      this.#pendingWrites.length = 0;
    }
  }

  write(data) {
    if (this.#isCorked) {
      this.#pendingWrites.push(data);
    } else {
      this.writtenData.push(data);
    }
    return this.writeResult;
  }

  pause() {
    this.paused = true;
  }

  resume() {
    this.paused = false;
  }

  drain() {
    this.writeResult = true;
    this.writableLength = 0;
    this.emit('drain');
  }

  end(data) {
    if (data !== undefined) this.write(data);
    this.ended = true;
    process.nextTick(() => this.emit('close'));
  }

  destroy() {
    this.destroyed = true;
    process.nextTick(() => this.emit('close'));
  }
}

module.exports = { MockSocket };
