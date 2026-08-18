'use strict';

// Every emit() answers with a promise, and a listener's SYNCHRONOUS throw has
// to become a rejection: `void emitter.emit('close')` is the house style, and a
// throw escaping it would land in a socket handler. The async-arrow wrapper on
// the slow path buys exactly that, at one closure and one promise per listener
// per emit. The overwhelmingly common shape is a single sync listener — a
// transport's 'message', a stream's chunk push — so it is spelled out by hand:
// 4.0 -> 14.3 M emit/s awaited, 4.8 -> 63.9 M fire-and-forget; bench/emitter.js.
const RESOLVED = Promise.resolve();

class Emitter {
  #events = new Map();
  #maxListeners = 10;

  constructor(options = {}) {
    this.#maxListeners = options.maxListeners ?? 10;
  }

  emit(eventName, value) {
    const event = this.#events.get(eventName);
    if (!event) {
      if (eventName !== 'error') return RESOLVED;
      throw new Error('Unhandled error');
    }
    const on = event.on;
    // Gated on once.size because the sweep below may delete the whole record,
    // and off() splices without deleting, so an emptied `on` is reachable too.
    if (event.once.size === 0 && on.length < 2) {
      if (on.length === 0) return RESOLVED;
      let result;
      try {
        result = on[0](value);
      } catch (error) {
        return Promise.reject(error);
      }
      if (result === null || result === undefined) return RESOLVED;
      return typeof result.then === 'function' ? Promise.resolve(result).then(() => undefined) : RESOLVED;
    }
    // More than one listener, or a once to sweep: snapshot first, because a
    // listener may call off() while the eager invocation below is still going.
    const listeners = on.slice();
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

const DEFAULT_HIGH_WATER_MARK = 1024;

// Push -> pull adapter: the missing primitive between "something calls me
// with a value" (an emitter, a backplane message, a websocket frame) and
// "someone is `for await`-ing values". Used on the server to feed a
// subscription handler and on the client to back `subscription.iterate()`.
//
// The queue is bounded: a producer that outruns the consumer drops the
// OLDEST pending value rather than growing without limit, and says so
// through `dropped`. Silent unbounded buffering is how a slow consumer
// takes a process down.
class EventStream {
  #queue = [];
  #waiting = null;
  #done = false;
  #error = null;
  #highWaterMark;
  #onAbort = null;
  #signal = null;

  constructor({ signal = null, highWaterMark = DEFAULT_HIGH_WATER_MARK } = {}) {
    this.#highWaterMark = highWaterMark;
    this.dropped = 0;
    if (!signal) return;
    this.#signal = signal;
    if (signal.aborted) {
      this.#done = true;
      return;
    }
    this.#onAbort = () => this.end();
    signal.addEventListener('abort', this.#onAbort, { once: true });
  }

  get length() {
    return this.#queue.length;
  }

  get closed() {
    return this.#done;
  }

  push(value) {
    if (this.#done) return false;
    if (this.#waiting) {
      const { resolve } = this.#waiting;
      this.#waiting = null;
      resolve({ value, done: false });
      return true;
    }
    this.#queue.push(value);
    if (this.#queue.length > this.#highWaterMark) {
      this.#queue.shift();
      this.dropped++;
    }
    return true;
  }

  /** Ends the stream; a pending next() resolves as done. */
  end() {
    if (this.#done) return;
    this.#done = true;
    this.#detach();
    if (!this.#waiting) return;
    const { resolve } = this.#waiting;
    this.#waiting = null;
    resolve({ value: undefined, done: true });
  }

  /** Ends the stream by throwing into the consumer. */
  fail(error) {
    if (this.#done) return;
    this.#error = error;
    this.#done = true;
    this.#detach();
    if (!this.#waiting) return;
    const { reject } = this.#waiting;
    this.#waiting = null;
    reject(error);
  }

  #detach() {
    if (!this.#onAbort || !this.#signal) return;
    this.#signal.removeEventListener('abort', this.#onAbort);
    this.#onAbort = null;
  }

  next() {
    if (this.#queue.length > 0) {
      return Promise.resolve({ value: this.#queue.shift(), done: false });
    }
    if (this.#error) {
      const error = this.#error;
      this.#error = null;
      return Promise.reject(error);
    }
    if (this.#done) return Promise.resolve({ value: undefined, done: true });
    if (this.#waiting) {
      return Promise.reject(new Error('EventStream: concurrent next() is not supported'));
    }
    return new Promise((resolve, reject) => {
      this.#waiting = { resolve, reject };
    });
  }

  // Consumers that break out of `for await` land here: the stream has to
  // release its abort listener, or a long-lived signal pins it forever.
  return() {
    this.end();
    return Promise.resolve({ value: undefined, done: true });
  }

  [Symbol.asyncIterator]() {
    return this;
  }
}

const createEventStream = (options) => new EventStream(options);

// Reconnect pacing: truncated exponential backoff with AWS "full jitter"
// (https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/).
//
//   window = min(maxDelay, minDelay * factor ** attempt)
//   delay  = jitter ? random_between(0, window) : window
//
// Jittering the WHOLE window rather than adding a small offset is what
// actually breaks up the thundering herd: after a server restart, a thousand
// clients that all disconnected in the same millisecond would otherwise all
// come back in the same millisecond. `attempt` is 0-based, so the first
// retry waits inside the minDelay window.
const backoffDelay = ({ attempt = 0, minDelay, maxDelay, factor = 2, jitter = true, random = Math.random }) => {
  const growth = factor > 0 ? factor ** attempt : 1;
  // Infinity * 0 is NaN, and an overflowing exponential must still cap.
  const window = Math.min(maxDelay, minDelay * growth);
  const capped = Number.isFinite(window) ? window : maxDelay;
  if (!jitter) return Math.round(capped);
  return Math.round(random() * capped);
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

  // `signal` (optional) makes a queued waiter abortable: a caller that gave
  // up — cancelled, disconnected, timed out — leaves the queue immediately
  // instead of taking a slot later and doing work nobody will read.
  enter(signal = null) {
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        return void reject(new Error('Semaphore entry aborted'));
      }
      if (this.#counter > 0) {
        this.#counter--;
        return void resolve();
      }
      if (this.#queue.length >= this.#size) {
        return void reject(new Error('Semaphore queue is full'));
      }
      const waiter = { resolve, reject, timer: null, release: null };
      const evict = (error) => {
        const index = this.#queue.indexOf(waiter);
        if (index > -1) this.#queue.splice(index, 1);
        if (waiter.timer) clearTimeout(waiter.timer);
        reject(error);
      };
      if (this.#timeout > 0) {
        waiter.timer = setTimeout(() => evict(new Error('Semaphore timeout')), this.#timeout);
      }
      if (signal) {
        const onAbort = () => evict(new Error('Semaphore entry aborted'));
        signal.addEventListener('abort', onAbort, { once: true });
        waiter.release = () => signal.removeEventListener('abort', onAbort);
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
    waiter.release?.();
    waiter.resolve();
  }
}

module.exports = { Emitter, jsonParse, Semaphore, backoffDelay, EventStream, createEventStream };
