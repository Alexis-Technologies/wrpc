'use strict';

// One live reader per topic per instance, shared by every local read of it.
//
// A durable feed is read by many subscriptions at once — one per connected
// browser — and a broker consumer per subscription does not survive
// contact with Kafka (a group join each) or Redis (a blocked connection
// each). So an adapter provides two primitives and this module does the
// rest:
//
//   live(topic, { signal, onEntry })  -> Promise<cursor>
//     Starts ONE tail. Resolves once positioned, with the cursor of the
//     tip at that moment; afterwards calls onEntry(entry) for every entry
//     appended past it, in order, until the signal aborts.
//   range(topic, { after, limit })    -> Promise<entry[]>
//     Entries strictly after the cursor `after` (null: the oldest
//     retained), at most `limit`, in order. Throws coded 410/400 for an
//     unusable cursor.
//   covered(cursor, entry)            -> boolean  (already delivered?)
//   advance(cursor, entry)            -> cursor   (the resume token after it)
//
// A reader resuming from a cursor catches up through range() and then
// joins the shared tail; the live tail was positioned BEFORE the range
// read, so the two overlap instead of leaving a hole, and `covered` drops
// the overlap (verified against real Redis Streams in the phase-0 spike).
// A reader that falls behind the tail by more than `highWaterMark` entries
// stops buffering and catches up through range() again — memory per slow
// subscriber stays bounded.
//
// For single-sequence logs a cursor is simply the last entry id; for
// Kafka it is a vector of partition offsets, which is why the resume token
// yielded to the feed is the ADVANCED CURSOR, not the entry's own position.

const { codedError } = require('./ids.js');

const DEFAULT_HIGH_WATER_MARK = 1024;
const DEFAULT_PAGE = 256;

class TopicTails {
  #live;
  #range;
  #covered;
  #advance;
  #highWaterMark;
  #page;
  #tails = new Map(); // topic -> { readers, ready, current, controller, failure }

  constructor({ live, range, covered, advance, highWaterMark = DEFAULT_HIGH_WATER_MARK, page = DEFAULT_PAGE }) {
    for (const [name, fn] of Object.entries({ live, range, covered, advance })) {
      if (typeof fn !== 'function') throw new TypeError(`TopicTails: ${name} must be a function`);
    }
    this.#live = live;
    this.#range = range;
    this.#covered = covered;
    this.#advance = advance;
    this.#highWaterMark = highWaterMark;
    this.#page = page;
  }

  /** Number of topics with a running tail — for tests and diagnostics. */
  get size() {
    return this.#tails.size;
  }

  #join(topic, reader) {
    let tail = this.#tails.get(topic);
    if (!tail) {
      const controller = new AbortController();
      tail = { readers: new Set(), ready: null, current: null, controller, failure: null };
      const onEntry = (entry) => {
        tail.current = this.#advance(tail.current, entry);
        for (const member of tail.readers) member.push(entry);
      };
      tail.ready = Promise.resolve()
        .then(() => this.#live(topic, { signal: controller.signal, onEntry }))
        .then(
          (cursor) => {
            tail.current = cursor ?? null;
          },
          (error) => {
            tail.failure = error;
            // A failed tail is forgotten: the next read starts a fresh one
            // instead of joining a corpse.
            if (this.#tails.get(topic) === tail) this.#tails.delete(topic);
            for (const member of tail.readers) member.fail(error);
          },
        );
      this.#tails.set(topic, tail);
    }
    tail.readers.add(reader);
    return tail;
  }

  #leave(topic, tail, reader) {
    tail.readers.delete(reader);
    if (tail.readers.size > 0 || this.#tails.get(topic) !== tail) return;
    this.#tails.delete(topic);
    tail.controller.abort();
  }

  read(topic, { after = null, from = 'latest', signal = null } = {}) {
    const tails = this;
    let wake = null;
    const reader = {
      queue: [],
      lagging: false,
      failure: null,
      push(entry) {
        if (reader.lagging) return;
        if (reader.queue.length >= tails.#highWaterMark) {
          reader.lagging = true;
          reader.queue.length = 0;
        } else reader.queue.push(entry);
        wake?.();
      },
      fail(error) {
        reader.failure = error;
        wake?.();
      },
    };
    const tail = this.#join(topic, reader);
    const catchUp = (after !== null && after !== undefined) || from === 'earliest';

    const iterate = async function* () {
      const onAbort = () => wake?.();
      signal?.addEventListener('abort', onAbort, { once: true });
      try {
        await tail.ready;
        if (tail.failure) throw tail.failure;
        let cursor = catchUp ? (after ?? null) : tail.current;
        let pending = catchUp;
        for (;;) {
          if (signal?.aborted) return;
          if (reader.failure) throw reader.failure;
          if (pending || reader.lagging) {
            reader.lagging = false;
            pending = false;
            // Page until a short page: that is "caught up to the tip as of
            // now"; whatever arrived meanwhile is in the live queue.
            for (;;) {
              const page = await tails.#range(topic, { after: cursor, limit: tails.#page });
              for (const entry of page) {
                if (cursor !== null && tails.#covered(cursor, entry)) continue;
                cursor = tails.#advance(cursor, entry);
                yield { id: cursor, value: entry.value, headers: entry.headers };
                if (signal?.aborted) return;
              }
              if (page.length < tails.#page) break;
            }
            continue;
          }
          if (reader.queue.length > 0) {
            const entry = reader.queue.shift();
            if (cursor !== null && tails.#covered(cursor, entry)) continue;
            cursor = tails.#advance(cursor, entry);
            yield { id: cursor, value: entry.value, headers: entry.headers };
            continue;
          }
          await new Promise((resolve) => {
            wake = () => {
              wake = null;
              resolve();
            };
            if (signal?.aborted || reader.queue.length > 0 || reader.lagging || reader.failure) wake();
          });
        }
      } finally {
        signal?.removeEventListener('abort', onAbort);
        tails.#leave(topic, tail, reader);
      }
    };

    let started = false;
    // A read that is never iterated still holds a place on the tail; its
    // signal is how it lets go.
    signal?.addEventListener('abort', () => started || tails.#leave(topic, tail, reader), { once: true });
    const ready = tail.ready.then(() => {
      if (tail.failure) throw tail.failure;
    });
    ready.catch(() => {}); // surfaced through the iteration too; never unhandled
    return {
      ready,
      [Symbol.asyncIterator]: () => {
        // One iteration per read(): a second loop over the same reader
        // would split its queue between two consumers.
        if (started) throw codedError('A log read can be iterated once', 500);
        started = true;
        return iterate();
      },
    };
  }

  close() {
    for (const tail of this.#tails.values()) tail.controller.abort();
    this.#tails.clear();
  }
}

module.exports = { TopicTails, DEFAULT_HIGH_WATER_MARK };
