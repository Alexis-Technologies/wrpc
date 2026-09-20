'use strict';

// A leaf on purpose: the per-message codecs (src/compression/) and the
// encryption of @alexify/wrpc/encryption both answer promises in a browser
// — CompressionStream and crypto.subtle can only — and both need their
// messages kept in order around that. An entry that wants the ordering
// should not pay for the other's negotiation, so the class lives here and
// compression/index.js re-exports it.

const { isPromise } = require('./compression/ids.js');

/**
 * Keeps messages in order around a codec that may answer asynchronously.
 * `push(value, deliver, recover)`: `value` is the message, or a promise of
 * it; `deliver` runs with the settled value in push order; `recover` runs
 * with the error when the promise rejected (send it plain, hang up). A
 * push with nothing in flight and a plain value delivers synchronously —
 * the common path costs no promise — and once something is in flight
 * every later push waits behind it, plain or not, which is what keeps the
 * wire ordered. Errors thrown by deliver/recover go to `onError`.
 */
class Sequencer {
  #tail = null;
  #pending = 0;
  #onError;

  constructor(onError = null) {
    this.#onError = onError;
  }

  /** Pushes waiting on an earlier one; 0 when nothing is in flight. */
  get pending() {
    return this.#pending;
  }

  push(value, deliver, recover) {
    const async = isPromise(value);
    if (this.#tail === null && !async) return void deliver(value);
    this.#pending++;
    const run = () => (async ? value.then(deliver, recover) : deliver(value));
    const previous = this.#tail;
    const step = previous === null ? new Promise((resolve) => resolve(run())) : previous.then(run, run);
    const settled = () => {
      this.#pending--;
      if (this.#tail === done) this.#tail = null;
    };
    const done = step.then(settled, (error) => {
      settled();
      if (this.#onError) this.#onError(error);
    });
    this.#tail = done;
  }
}

module.exports = { Sequencer };
