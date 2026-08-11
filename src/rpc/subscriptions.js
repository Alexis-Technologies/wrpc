'use strict';

// Subscriptions: a procedure that answers with a STREAM of values instead of
// one, expressed as an async generator.
//
//   onMessage: procedure.subscription({
//     handler: async function* (context, args, { lastEventId, signal }) {
//       yield tracked('7', { text: 'hi' });   // resumable
//       yield { text: 'hi' };                 // not resumable
//     },
//   })
//
// On the wire that is `{type:'subscribe', id, method, args, lastEventId?}` in,
// then `{type:'data', id, eventId?, data}` per yield, then `{type:'end', id}`
// (with an `error` when the generator threw). `{type:'unsubscribe', id}` ends
// it early. See docs/reference/protocol.md.
//
// Three things make resume work, and they are deliberately separate:
//   tracked()          labels ONE value with an id the client remembers,
//   createEventLog()   remembers recent values so a handler can replay them,
//   lastEventId        is what the client sends back to say where it stopped.
// A handler that yields untracked values simply has no resume — which is the
// right default for a live-only feed.

// The push -> pull adapter a handler yields from lives in utils: the client
// needs the same primitive to back `subscription.iterate()`, and it is no
// more subscription-specific than Emitter is.
const { EventStream, createEventStream } = require('../utils.js');

const TRACKED = Symbol.for('wrpc.tracked');

/** Labels a yielded value with the id a resuming client will send back. */
const tracked = (eventId, data) => ({ [TRACKED]: true, id: String(eventId), data });

const isTracked = (value) => typeof value === 'object' && value !== null && value[TRACKED] === true;

const DEFAULT_LOG_SIZE = 100;

// A bounded replay buffer. Ids are monotonic decimal strings, so "later than"
// is a numeric comparison and a client that reconnects with an id the buffer
// has already dropped gets an honest "cannot resume" rather than a silently
// truncated history.
class EventLog {
  #entries = [];
  #size;
  #next;

  constructor({ size = DEFAULT_LOG_SIZE, start = 0 } = {}) {
    if (!(Number.isInteger(size) && size > 0)) {
      throw new TypeError('createEventLog: size must be a positive integer');
    }
    this.#size = size;
    this.#next = start;
  }

  get size() {
    return this.#size;
  }

  get length() {
    return this.#entries.length;
  }

  /** The id of the newest entry, or null while the log is empty. */
  get lastEventId() {
    return this.#entries.length > 0 ? this.#entries[this.#entries.length - 1].id : null;
  }

  push(data) {
    const id = String(this.#next++);
    this.#entries.push({ id, data });
    if (this.#entries.length > this.#size) this.#entries.shift();
    return id;
  }

  /**
   * Everything after `lastEventId`, as tracked values ready to yield.
   * `null` (not an empty array) means the id is no longer in the buffer, so
   * the caller has to decide between a snapshot and an error — silently
   * skipping the gap is the one thing that must not happen.
   * With no `lastEventId` there is nothing to replay: `[]`.
   */
  since(lastEventId) {
    if (lastEventId === undefined || lastEventId === null || lastEventId === '') return [];
    const cursor = Number(lastEventId);
    if (!Number.isFinite(cursor)) return null;
    // Everything asked for is newer than everything we hold: nothing missed.
    if (cursor >= this.#next - 1) return [];
    const oldest = this.#entries.length > 0 ? Number(this.#entries[0].id) : this.#next;
    if (cursor < oldest - 1) return null; // the gap is older than the buffer
    return this.#entries.filter((entry) => Number(entry.id) > cursor).map((entry) => tracked(entry.id, entry.data));
  }

  clear() {
    this.#entries.length = 0;
  }
}

const createEventLog = (options) => new EventLog(options);

// The pump. Drives one subscription's generator, writes a `data` packet per
// yield, and answers with `end` exactly once — whether the generator
// finished, threw, or was cut short by an unsubscribe or a disconnect.
//
// Backpressure is real: `client.send` reports the transport's high-water
// mark, and the pump waits for 'drain' before pulling the next value. A
// generator that ignores that would turn a slow consumer into unbounded
// server-side memory.
const runSubscription = async (client, { id, procedure, context, args, lastEventId, signal }) => {
  const iterator = procedure.subscribe(context, args, { lastEventId, signal });
  let terminal = { type: 'end', id };
  try {
    while (true) {
      const { value, done } = await iterator.next();
      if (done || signal.aborted) break;
      const packet = { type: 'data', id };
      if (isTracked(value)) {
        packet.eventId = value.id;
        packet.data = value.data;
      } else {
        packet.data = value;
      }
      const flushed = client.send(packet);
      if (flushed === false) await client.drain();
      if (signal.aborted) break;
    }
  } catch (error) {
    if (!signal.aborted) {
      const code = typeof error.code === 'number' ? error.code : 500;
      terminal = { type: 'end', id, error: { message: error.message, code } };
    }
  }
  // Close the generator so its `finally` runs — that is where a handler
  // releases the listener or connection it opened. NOT awaited: a handler
  // that ignores its abort signal would otherwise hold the terminal packet
  // hostage, and the peer has already been told to stop.
  void Promise.resolve()
    .then(() => iterator.return?.())
    .catch((error) => client.warn(`SUBSCRIPTION\t${id}\t${error?.stack ?? error}`));
  return terminal;
};

module.exports = {
  TRACKED,
  tracked,
  isTracked,
  EventLog,
  createEventLog,
  EventStream,
  createEventStream,
  runSubscription,
};
