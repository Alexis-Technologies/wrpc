'use strict';

const DEFAULT_PREFIX = 'wrpc';

// In-process backplane: the reference implementation of the contract, and
// what makes multi-instance fan-out testable (and useful) without a broker —
// two RpcServer instances sharing one MemoryBackplane behave exactly like two
// processes sharing a Redis.
//
// Delivery is deferred to a microtask on purpose: a real broker never calls
// a handler synchronously inside publish(), and code that accidentally
// depends on synchronous delivery would break the moment Redis is swapped in.
class MemoryBackplane {
  #channels = new Map(); // channel -> Set<handler>
  #prefix;
  #console;
  #closed = false;

  constructor({ prefix = DEFAULT_PREFIX, console = globalThis.console } = {}) {
    this.#prefix = prefix;
    this.#console = console;
  }

  get name() {
    return 'memory';
  }

  get size() {
    return this.#channels.size;
  }

  #key(channel) {
    return this.#prefix ? `${this.#prefix}:${channel}` : channel;
  }

  publish(channel, message) {
    if (this.#closed) return;
    const handlers = this.#channels.get(this.#key(channel));
    if (!handlers || handlers.size === 0) return;
    for (const handler of Array.from(handlers)) {
      queueMicrotask(() => {
        // Unsubscribed between publish and delivery: a broker would not
        // deliver either.
        if (this.#closed || !handlers.has(handler)) return;
        try {
          handler(message);
        } catch (error) {
          this.#console.error(error);
        }
      });
    }
  }

  subscribe(channel, handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('MemoryBackplane.subscribe: handler must be a function');
    }
    const key = this.#key(channel);
    let handlers = this.#channels.get(key);
    if (!handlers) {
      handlers = new Set();
      this.#channels.set(key, handlers);
    }
    handlers.add(handler);
    return () => {
      const current = this.#channels.get(key);
      if (!current || !current.delete(handler)) return;
      if (current.size === 0) this.#channels.delete(key);
    };
  }

  close() {
    this.#closed = true;
    this.#channels.clear();
  }
}

const createMemoryBackplane = (options) => new MemoryBackplane(options);

module.exports = { MemoryBackplane, createMemoryBackplane, DEFAULT_PREFIX };
