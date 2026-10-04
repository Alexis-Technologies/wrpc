'use strict';

// One live reader per topic per instance, shared by every local read of it.
//
// A durable feed is read by many subscriptions at once — one per connected
// browser — and a broker consumer per subscription does not survive
// contact with Kafka (a group join each) or Redis (a blocked connection
// each). So an adapter provides two primitives and this module does the
// rest:
//
//   live(topic, { signal, onEntry, onEnd })  -> Promise<cursor>
//     Starts ONE tail. Resolves once positioned, with the cursor of the
//     tip at that moment; afterwards calls onEntry(entry) for every entry
//     appended past it, in order, until the signal aborts — or calls
//     onEnd(error) once, when the tail died on its own (the broker
//     cancelled the consumer, the channel closed, the consumer crashed):
//     the readers then move to a fresh tail and catch up from their own
//     cursors, so a dead tail loses nothing. A tail that ends without
//     saying so used to freeze every subscription of the topic for good.
//   range(topic, { after, limit })    -> Promise<entry[] | { entries, done }>
//     Entries strictly after the cursor `after` (null: the oldest
//     retained), at most `limit`, in order. Throws coded 410/400 for an
//     unusable cursor. A log read straight off its store (memory, Redis)
//     answers an array, and a SHORT page is the end. A broker that pages
//     by TIME — a stream reader with no "end" (AMQP), a consumer group's
//     fetch (Kafka), a pull with an expiry (NATS) — answers `{ entries,
//     done }`, where `done` says the page reached the tip as of the call:
//     a page cut short by a pause is not the end, and it used to be taken
//     for one, leaving whatever came after the pause unread by a reader
//     that then joined the live tail past it.
//   covered(cursor, entry)            -> boolean  (already delivered?)
//   advance(cursor, entry)            -> cursor   (the resume token after it)
//   contiguous?(cursor, entry)        -> boolean  (optional, dense ids only)
//     Whether `entry` is the one right after `cursor`. A page whose head
//     is not — a stream reader that skipped — is not yielded; the range is
//     asked again from the cursor, a bounded number of times, then 503.
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

const { backoffDelay } = require('../utils.js');
const { codedError } = require('./ids.js');

const DEFAULT_HIGH_WATER_MARK = 1024;
const DEFAULT_PAGE = 256;
// A tail that ENDED under its readers — the broker cancelled the consumer,
// its channel closed, the consumer crashed — is replaced by a fresh one,
// and each reader catches up from its own cursor through range() so the
// gap is closed. A broker that cannot start the fresh tail is asked again
// on this backoff, this many times, before the readers fail 503.
const MAX_REJOINS = 5;
const REJOIN_BACKOFF = Object.freeze({ minDelay: 200, maxDelay: 5000, factor: 2, jitter: true });
// A catch-up that makes no progress — a page answered `done: false` with
// nothing in it, or one whose head does not follow the cursor — is asked
// again after this pause, this many times, before the read fails 503.
const STALL_DELAY = 50;
const MAX_STALLS = 10;

const sleep = (ms) =>
  new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    if (typeof timer.unref === 'function') timer.unref();
  });

class TopicTails {
  #live;
  #range;
  #covered;
  #advance;
  #contiguous;
  #highWaterMark;
  #page;
  #onLag;
  #tails = new Map(); // topic -> { readers, ready, current, controller, failure }

  constructor({
    live,
    range,
    covered,
    advance,
    contiguous = null,
    highWaterMark = DEFAULT_HIGH_WATER_MARK,
    page = DEFAULT_PAGE,
    // `onLag(topic, readers)`: readers of a topic that fell `highWaterMark`
    // entries behind its tail just had their buffers dropped and will
    // catch up through range() instead — the one sign of subscribers that
    // cannot keep up, and of range load on the broker, which used to pass
    // without a trace.
    onLag = null,
  }) {
    for (const [name, fn] of Object.entries({ live, range, covered, advance })) {
      if (typeof fn !== 'function') throw new TypeError(`TopicTails: ${name} must be a function`);
    }
    if (contiguous !== null && typeof contiguous !== 'function') {
      throw new TypeError('TopicTails: contiguous must be a function or null');
    }
    if (onLag !== null && typeof onLag !== 'function') {
      throw new TypeError('TopicTails: onLag must be a function or null');
    }
    this.#onLag = onLag;
    this.#live = live;
    this.#range = range;
    this.#covered = covered;
    this.#advance = advance;
    this.#contiguous = contiguous;
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
      tail = { readers: new Set(), ready: null, current: null, positioned: false, controller, failure: null };
      const onEntry = (entry) => {
        tail.current = this.#advance(tail.current, entry);
        // push() answers true for a reader that JUST fell behind: nothing
        // is counted, and nothing called, while everybody keeps up.
        let lagged = 0;
        for (const member of tail.readers) if (member.push(entry)) lagged++;
        if (lagged > 0 && this.#onLag !== null) {
          try {
            this.#onLag(topic, lagged);
          } catch {
            // An observer's throw is not the tail's failure.
          }
        }
      };
      // The tail died under its readers: forgotten, stopped, and every
      // reader told to re-join (see read()). Before it was positioned, a
      // failure is live()'s rejection, not an end.
      const onEnd = () => {
        if (!tail.positioned || this.#tails.get(topic) !== tail) return;
        this.#tails.delete(topic);
        tail.controller.abort();
        const members = Array.from(tail.readers);
        tail.readers.clear();
        for (const member of members) member.rejoin();
      };
      tail.ready = Promise.resolve()
        .then(() => this.#live(topic, { signal: controller.signal, onEntry, onEnd }))
        .then(
          (cursor) => {
            tail.current = cursor ?? null;
            tail.positioned = true;
            // A 'latest' reader starts where the tail stood the moment it
            // was READY — not where it stands at the reader's first next():
            // what is appended in between is in the reader's queue, and a
            // start taken later would call it covered and drop it.
            for (const member of tail.readers) member.start = tail.current;
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
    if (tail.positioned) reader.start = tail.current;
    return tail;
  }

  #leave(topic, tail, reader) {
    tail.readers.delete(reader);
    if (tail.readers.size > 0 || this.#tails.get(topic) !== tail) return;
    this.#tails.delete(topic);
    this.#stop(tail);
  }

  // A tail still positioning is stopped once it is: an adapter's live()
  // aborted in the middle of its own setup is a consumer half opened.
  #stop(tail) {
    if (tail.positioned || tail.failure !== null) return void tail.controller.abort();
    void tail.ready.then(() => tail.controller.abort());
  }

  read(topic, { after = null, from = 'latest', signal = null } = {}) {
    const tails = this;
    let wake = null;
    const reader = {
      queue: [],
      lagging: false,
      failure: null,
      // Where a 'latest' read starts: the tail's cursor as of ready (#join).
      start: null,
      push(entry) {
        if (reader.lagging) return false;
        const lagged = reader.queue.length >= tails.#highWaterMark;
        if (lagged) {
          reader.lagging = true;
          reader.queue.length = 0;
        } else reader.queue.push(entry);
        wake?.();
        return lagged;
      },
      fail(error) {
        reader.failure = error;
        wake?.();
      },
      // The tail ended: joined afresh at the next turn of the loop, and
      // caught up from the cursor — `lagging` is exactly that path.
      moved: false,
      rejoin() {
        reader.moved = true;
        reader.lagging = true;
        wake?.();
      },
    };
    let tail = this.#join(topic, reader);
    const catchUp = (after !== null && after !== undefined) || from === 'earliest';

    // `entered`: the generator's body ran — and with it the `finally` that
    // leaves the tail. A generator returned before its first next() never
    // runs its body, so its place is released from the iterator instead.
    let entered = false;
    const iterate = async function* () {
      entered = true;
      const onAbort = () => wake?.();
      signal?.addEventListener('abort', onAbort, { once: true });
      try {
        await tail.ready;
        if (tail.failure) throw tail.failure;
        let cursor = catchUp ? (after ?? null) : reader.start;
        let pending = catchUp;
        for (;;) {
          if (signal?.aborted) return;
          if (reader.failure) throw reader.failure;
          if (pending || reader.lagging) {
            reader.lagging = false;
            pending = false;
            if (reader.moved) {
              reader.moved = false;
              // A fresh tail, joined BEFORE the catch-up below so the two
              // overlap the way a resume does. A broker that cannot start
              // one is asked again on a backoff; then the read fails.
              for (let attempt = 0; ; attempt++) {
                tail = tails.#join(topic, reader);
                await tail.ready.catch(() => {});
                if (tail.failure === null) break;
                if (reader.failure === tail.failure) reader.failure = null;
                if (signal?.aborted) return;
                if (attempt >= MAX_REJOINS - 1) throw codedError('Log tail ended', 503);
                await sleep(backoffDelay({ ...REJOIN_BACKOFF, attempt }));
              }
            }
            // Page until the tip: that is "caught up as of now"; whatever
            // arrived meanwhile is in the live queue. The tip is a short
            // page for a store read directly, and what `done` says for a
            // broker that pages by time (see the header).
            let stalls = 0;
            for (;;) {
              const result = await tails.#range(topic, { after: cursor, limit: tails.#page });
              const page = Array.isArray(result) ? result : result.entries;
              const done = Array.isArray(result) ? page.length < tails.#page : result.done === true;
              let advanced = false;
              let hole = false;
              for (const entry of page) {
                if (cursor !== null && tails.#covered(cursor, entry)) continue;
                if (cursor !== null && tails.#contiguous !== null && !tails.#contiguous(cursor, entry)) {
                  hole = true;
                  break;
                }
                cursor = tails.#advance(cursor, entry);
                advanced = true;
                yield { id: cursor, value: entry.value, headers: entry.headers };
                if (signal?.aborted) return;
              }
              if (!hole && done) break;
              if (advanced && !hole) {
                stalls = 0;
                continue;
              }
              // No progress: a hole at the head of the page, or a page the
              // adapter called incomplete with nothing in it. Asked again
              // after a pause, a bounded number of times.
              if (++stalls > MAX_STALLS) {
                throw codedError(
                  hole ? 'Log read fell behind a gap in the topic' : 'Log read could not reach the tip',
                  503,
                );
              }
              await sleep(STALL_DELAY);
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
    // signal is how it lets go — until the body runs, whose `finally` does.
    signal?.addEventListener('abort', () => entered || tails.#leave(topic, tail, reader), { once: true });
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
        const iterator = iterate();
        // return()/throw() before the first next() end a generator without
        // running its body: the reader's place on the tail is released here
        // instead — a feed that closed its read at once used to hold a live
        // reader for the life of the broker. `next` is not wrapped: hot path.
        const release = () => {
          if (!entered) tails.#leave(topic, tail, reader);
        };
        const { return: end, throw: raise } = iterator;
        iterator.return = (value) => {
          release();
          return end.call(iterator, value);
        };
        iterator.throw = (error) => {
          release();
          return raise.call(iterator, error);
        };
        return iterator;
      },
    };
  }

  close() {
    for (const tail of this.#tails.values()) {
      this.#stop(tail);
      // A reader parked on a closed broker used to wait forever.
      for (const member of tail.readers) member.fail(codedError('Log closed', 503));
    }
    this.#tails.clear();
  }
}

module.exports = { TopicTails, DEFAULT_HIGH_WATER_MARK };
