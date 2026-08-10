'use strict';

class Emitter {
  #events = new Map();
  #maxListeners = 10;

  constructor(options = {}) {
    this.#maxListeners = options.maxListeners ?? 10;
  }

  emit(eventName, value) {
    const event = this.#events.get(eventName);
    if (!event) {
      if (eventName !== 'error') return Promise.resolve();
      throw new Error('Unhandled error');
    }
    const listeners = event.on.slice();
    const promises = listeners.map(async (fn) => fn(value));
    if (event.once.size > 0) {
      const len = event.on.length;
      const remaining = new Array(len);
      let index = 0;
      for (let i = 0; i < len; i++) {
        const listener = event.on[i];
        if (!event.once.has(listener)) remaining[index++] = listener;
      }
      if (index === 0) {
        this.#events.delete(eventName);
      } else {
        remaining.length = index;
        this.#events.set(eventName, { on: remaining, once: new Set() });
      }
    }
    return Promise.all(promises).then(() => undefined);
  }

  #addListener(eventName, listener, once) {
    let event = this.#events.get(eventName);
    if (!event) {
      const on = [listener];
      event = { on, once: once ? new Set(on) : new Set() };
      this.#events.set(eventName, event);
    } else {
      if (event.on.includes(listener)) {
        throw new Error('Duplicate listeners detected');
      }
      event.on.push(listener);
      if (once) event.once.add(listener);
    }
    // A warning, not a throw: exceeding the cap usually signals a leak,
    // but killing legitimate fan-out (many streams awaiting one 'drain')
    // is worse than a noisy console (cross-platform, so no process API).
    if (event.on.length > this.#maxListeners) {
      globalThis.console.warn(
        `MaxListenersExceededWarning: Possible ${String(eventName)} memory leak. ` +
          `${event.on.length} listeners added, current maxListeners is ${this.#maxListeners}.`,
      );
    }
  }

  on(eventName, listener) {
    this.#addListener(eventName, listener, false);
  }

  once(eventName, listener) {
    this.#addListener(eventName, listener, true);
  }

  off(eventName, listener) {
    if (!listener) return void this.#events.delete(eventName);
    const event = this.#events.get(eventName);
    if (!event) return;
    const index = event.on.indexOf(listener);
    if (index > -1) event.on.splice(index, 1);
    event.once.delete(listener);
  }

  clear(eventName) {
    if (!eventName) return void this.#events.clear();
    this.#events.delete(eventName);
  }

  listeners(eventName) {
    if (!eventName) throw new Error('Expected eventName');
    const event = this.#events.get(eventName);
    return event ? event.on : [];
  }

  listenerCount(eventName) {
    if (!eventName) throw new Error('Expected eventName');
    const event = this.#events.get(eventName);
    return event ? event.on.length : 0;
  }

  eventNames() {
    return Array.from(this.#events.keys());
  }
}

const jsonParse = (data = null) => {
  if (data === null) return null;
  try {
    return JSON.parse(data);
  } catch {
    return null;
  }
};

// Counting semaphore with a bounded wait queue (ported from metautil):
// enter() resolves when a slot frees up, rejects on queue overflow or
// after `timeout` ms in the queue.
class Semaphore {
  #concurrency;
  #counter;
  #size;
  #timeout;
  #queue = [];

  constructor({ concurrency, size = 0, timeout = 0 } = {}) {
    this.#concurrency = concurrency;
    this.#counter = concurrency;
    this.#size = size;
    this.#timeout = timeout;
  }

  get empty() {
    return this.#counter === this.#concurrency;
  }

  enter() {
    return new Promise((resolve, reject) => {
      if (this.#counter > 0) {
        this.#counter--;
        return void resolve();
      }
      if (this.#queue.length >= this.#size) {
        return void reject(new Error('Semaphore queue is full'));
      }
      const waiter = { resolve, reject, timer: null };
      if (this.#timeout > 0) {
        waiter.timer = setTimeout(() => {
          const index = this.#queue.indexOf(waiter);
          if (index > -1) this.#queue.splice(index, 1);
          reject(new Error('Semaphore timeout'));
        }, this.#timeout);
      }
      this.#queue.push(waiter);
    });
  }

  leave() {
    const waiter = this.#queue.shift();
    if (!waiter) {
      if (this.#counter < this.#concurrency) this.#counter++;
      return;
    }
    if (waiter.timer) clearTimeout(waiter.timer);
    waiter.resolve();
  }
}

module.exports = { Emitter, jsonParse, Semaphore };
