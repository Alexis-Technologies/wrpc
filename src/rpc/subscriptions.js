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
const { publicErrorMessage } = require('../transport.js');

const TRACKED = Symbol.for('wrpc.tracked');

/** Labels a yielded value with the id a resuming client will send back. */
const tracked = (eventId, data) => ({ [TRACKED]: true, id: String(eventId), data });

const isTracked = (value) => typeof value === 'object' && value !== null && value[TRACKED] === true;

const DEFAULT_LOG_SIZE = 100;

// A bounded replay buffer. Ids are `<epoch>.<n>` — a monotonic counter
// stamped with WHICH log minted it. The epoch is random per instance by
// default, so after a restart (or on another node holding its own log) a
// client's lastEventId belongs to a foreign epoch and since() answers
// `null` — an honest "cannot resume, take a snapshot" — instead of a
// numeric coincidence silently pretending nothing was missed. A persisted
// or shared log passes its own stable `epoch`.
class EventLog {
  #entries = [];
  #size;
  #next;
  #epoch;

  constructor({ size = DEFAULT_LOG_SIZE, start = 0, epoch = null } = {}) {
    if (!(Number.isInteger(size) && size > 0)) {
      throw new TypeError('createEventLog: size must be a positive integer');
    }
    this.#size = size;
    this.#next = start;
    this.#epoch = epoch === null || epoch === undefined ? Math.random().toString(36).slice(2, 10) : String(epoch);
    if (this.#epoch.includes('.')) {
      throw new TypeError('createEventLog: epoch must not contain a dot');
    }
  }

  get size() {
    return this.#size;
  }

  get length() {
    return this.#entries.length;
  }

  /** Which log incarnation mints this log's ids. */
  get epoch() {
    return this.#epoch;
  }

  /** The id of the newest entry, or null while the log is empty. */
  get lastEventId() {
    return this.#entries.length > 0 ? this.#entries[this.#entries.length - 1].id : null;
  }

  push(data) {
    const n = this.#next++;
    const id = `${this.#epoch}.${n}`;
    this.#entries.push({ id, n, data });
    if (this.#entries.length > this.#size) this.#entries.shift();
    return id;
  }

  /**
   * Everything after `lastEventId`, as tracked values ready to yield.
   * `null` (not an empty array) means the log cannot bridge the gap — the
   * id fell out of the buffer, or it was minted by another epoch (another
   * process, a restart) — so the caller has to decide between a snapshot
   * and an error. Silently skipping the gap is the one thing that must not
   * happen. With no `lastEventId` there is nothing to replay: `[]`.
   */
  since(lastEventId) {
    if (lastEventId === undefined || lastEventId === null || lastEventId === '') return [];
    const text = String(lastEventId);
    const dot = text.lastIndexOf('.');
    if (dot < 0 || text.slice(0, dot) !== this.#epoch) return null; // foreign epoch
    const cursor = Number(text.slice(dot + 1));
    if (!Number.isFinite(cursor)) return null;
    // Everything asked for is newer than everything we hold: nothing missed.
    if (cursor >= this.#next - 1) return [];
    const oldest = this.#entries.length > 0 ? this.#entries[0].n : this.#next;
    if (cursor < oldest - 1) return null; // the gap is older than the buffer
    // Entries are appended in order, so the first one past the cursor marks
    // the start of the tail — one pass, one array, instead of filter().map()
    // building two.
    const missed = [];
    for (let i = 0; i < this.#entries.length; i++) {
      const entry = this.#entries[i];
      if (entry.n > cursor) missed.push(tracked(entry.id, entry.data));
    }
    return missed;
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
const runSubscription = async (client, { id, procedure, context, args, lastEventId, signal, stats = null, hooks }) => {
  const iterator = procedure.subscribe(context, args, { lastEventId, signal }, hooks);
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
      if (stats) stats.values++;
      if (flushed === false) {
        client.otel.recordBackpressure(client.transportKind);
        await client.drain();
      }
      if (signal.aborted) break;
    }
  } catch (error) {
    if (!signal.aborted) {
      const code = typeof error.code === 'number' ? error.code : 500;
      terminal = { type: 'end', id, error: { message: publicErrorMessage(code, error), code } };
    } else {
      // Aborted: the terminal packet stays clean because the peer asked to
      // stop, so this error reaches nobody unless it is logged here.
      client.log.debug({ err: error, event: 'subscription.aborted', id });
    }
  }
  // Close the generator so its `finally` runs — that is where a handler
  // releases the listener or connection it opened. NOT awaited: a handler
  // that ignores its abort signal would otherwise hold the terminal packet
  // hostage, and the peer has already been told to stop.
  void Promise.resolve()
    .then(() => iterator.return?.())
    .catch((error) =>
      client.warn(`SUBSCRIPTION\t${id}\t${error?.stack ?? error}`, { event: 'subscription.return', id, err: error }),
    );
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
