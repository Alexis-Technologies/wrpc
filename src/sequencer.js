'use strict';

// A leaf on purpose: the per-message codecs (src/compression/) and the
// encryption of @alexify/wrpc/encryption both answer promises in a browser
// — CompressionStream and crypto.subtle can only — and both need their
// messages kept in order around that. An entry that wants the ordering
// should not pay for the other's negotiation, so the class lives here and
// compression/index.js re-exports it.

const { isPromise } = require('./compression/ids.js');

const RESOLVED = Promise.resolve();
const noop = () => {};

// How many asynchronous decodes a transport starts before it holds the
// rest back — a thunk in its slot, a paused reader (WebTransport): a burst
// of compressed frames used to start an inflate per frame at once.
const INFLIGHT_LIMIT = 4;

/**
 * Keeps messages in order around a codec that may answer asynchronously.
 * `push(value, deliver, recover)`: `value` is the message, a promise of
 * it, or a THUNK producing either — called at once with nothing in
 * flight, in its slot otherwise, which is how a consumer starts no more
 * work than it means to have in flight; `deliver` runs with the settled
 * value in push order; `recover` runs with the error when the promise
 * rejected or the thunk threw (send it plain, hang up). A push with
 * nothing in flight and a plain value delivers synchronously — the common
 * path costs no promise — and once something is in flight every later
 * push waits behind it, plain or not, which is what keeps the wire
 * ordered. Errors thrown by deliver/recover go to `onError`.
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

  /** Settles once everything pushed so far has been delivered. */
  get idle() {
    return this.#tail ?? RESOLVED;
  }

  push(value, deliver, recover) {
    let later = typeof value === 'function';
    if (later && this.#tail === null) {
      // Nothing in flight: called now, and its answer takes the plain path.
      try {
        value = value();
      } catch (error) {
        return void this.#recover(recover, error);
      }
      later = false;
    }
    const async = later || isPromise(value);
    if (this.#tail === null && !async) return void deliver(value);
    // A promise queued behind another: a rejection before its turn is not
    // an unhandled one — it is handled in its slot, by recover. (With
    // nothing in flight the slot runs now, and the handler is on in time.)
    if (!later && async && this.#tail !== null) value.then(undefined, noop);
    this.#pending++;
    const run = () => {
      if (!later) return async ? value.then(deliver, recover) : deliver(value);
      let result;
      try {
        result = value();
      } catch (error) {
        if (recover) return void recover(error);
        throw error;
      }
      return isPromise(result) ? result.then(deliver, recover) : deliver(result);
    };
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

  #recover(recover, error) {
    if (recover) recover(error);
    else if (this.#onError) this.#onError(error);
  }
}

module.exports = { Sequencer, INFLIGHT_LIMIT };
