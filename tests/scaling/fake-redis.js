'use strict';

// Not a *.test.js: the ioredis-shaped fake shared by the Redis adapter
// suite and the backplane contract run over it.

const { EventEmitter } = require('node:events');

// An in-repo fake shaped like ioredis: publish/subscribe/unsubscribe return
// promises, every channel of a connection arrives through one 'message'
// event, and duplicate() hands out a second connection. Running the adapter
// against a real Redis is a separate, optional CI job — the contract this
// fake encodes is what the adapter actually depends on.
class FakeRedis extends EventEmitter {
  constructor(bus = new EventEmitter()) {
    super();
    bus.setMaxListeners(0);
    bus.duplicates = bus.duplicates ?? [];
    this.bus = bus;
    this.channels = new Set();
    this.published = [];
    this.failPublish = false;
    this.#listen();
  }

  #listen() {
    this.bus.on('publish', (channel, message) => {
      if (!this.channels.has(channel)) return;
      this.emit('message', channel, message);
    });
  }

  duplicate() {
    const sub = new FakeRedis(this.bus);
    this.bus.duplicates.push(sub);
    return sub;
  }

  async publish(channel, message) {
    if (this.failPublish) throw new Error('redis is down');
    this.published.push([channel, message]);
    // Never inside publish(): a real Redis delivers over the network, and
    // the backplane contract checks that no handler runs synchronously.
    queueMicrotask(() => this.bus.emit('publish', channel, message));
    return 1;
  }

  async subscribe(channel) {
    this.channels.add(channel);
    return this.channels.size;
  }

  async unsubscribe(channel) {
    this.channels.delete(channel);
    return this.channels.size;
  }

  async quit() {
    this.quitCalls = (this.quitCalls ?? 0) + 1;
    return 'OK';
  }
}

module.exports = { FakeRedis };
