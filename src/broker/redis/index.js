'use strict';

// The Redis broker: all four capabilities over an INJECTED ioredis-shaped
// client (per the zero-dependency rule — `ioredis` is a devDependency of
// this repository and never a runtime one). Anything with the same command
// surface plugs in: ioredis, a node-redis wrapper, Valkey, KeyDB, Dragonfly.
//
//   const Redis = require('ioredis');
//   const broker = createRedisBroker({ client: new Redis(url) });
//
// Capability map:
//   backplane  PUBLISH / SUBSCRIBE                     (the scaling adapter)
//   log        Streams: XADD, XRANGE, XREAD BLOCK      (one shared tail per
//                                                       instance, TopicTails)
//   queue      Streams + consumer groups: XREADGROUP,
//              XACK + XDEL, XAUTOCLAIM for what a dead
//              consumer held, a ZSET for delayed retries
//   direct     PUBLISH for inboxes, a list (BLMPOP) for
//              competing groups
//
// Connections: the injected one runs every command; blocking reads
// (XREAD/XREADGROUP/BLMPOP) and subscriptions each need one of their own, so
// the adapter duplicates lazily — one for the log tail, one per queue
// consumer, one per direct group, one for pub/sub. `duplicate()` (or an
// injected `connect` factory) is what it duplicates with, and it quits only
// the connections it opened itself.

const { createRedisAdapter } = require('../../scaling/redis.js');
const { createLoggerWriter } = require('../../logging.js');
const { generateUUID } = require('../../runtime/node.js');
const { resolveGenerateId } = require('../../utils.js');

// An injected `generateId` is used VERBATIM for every id this adapter mints
// — never truncated. Trimming a user's id would quietly weaken the
// uniqueness they chose it for, and all wrpc knows about their generator is
// that it answers a string. The cost is that a generator answering
// characters a broker refuses in a consumer name, subject or queue name
// fails at the driver, not here.
const { TopicTails } = require('../tail.js');
const { codedError, toText, toBytes, toHeaders, encodeToken } = require('../ids.js');

const DEFAULT_PREFIX = 'wrpc';
const DEFAULT_BLOCK_MS = 1000;
const DEFAULT_CLAIM_IDLE_MS = 60_000;
const DEFAULT_PREFETCH = 16;
const DEFAULT_INBOX_TTL_MS = 60_000;
const STREAM_ID = /^\d{1,20}-\d{1,20}$/;

const ATTEMPT_HEADER = 'x-wrpc-attempt';
const REDELIVERED_HEADER = 'x-wrpc-redelivered';
const DEAD_REASON_HEADER = 'x-wrpc-dead-reason';

const isFunction = (value) => typeof value === 'function';

// A stream id is `<ms>-<seq>`: compared numerically, part by part, because
// both halves outgrow Number's safe range in neither case but the STRING
// order is wrong ('10-0' < '9-0').
const compareIds = (a, b) => {
  const [ams, aseq] = a.split('-');
  const [bms, bseq] = b.split('-');
  if (ams.length !== bms.length) return ams.length < bms.length ? -1 : 1;
  if (ams !== bms) return ams < bms ? -1 : 1;
  if (aseq.length !== bseq.length) return aseq.length < bseq.length ? -1 : 1;
  return aseq === bseq ? 0 : aseq < bseq ? -1 : 1;
};

// XADD field pairs: the value, and the headers as one JSON field. Two
// fields rather than one per header — a header name is application data and
// must never become a Redis field name pattern.
const encodeFields = (value, headers) => {
  const fields = ['v', toText(value)];
  const bag = headers ? toHeaders(headers) : null;
  if (bag && Object.keys(bag).length > 0) fields.push('h', JSON.stringify(bag));
  return fields;
};

const decodeEntry = (id, fields) => {
  let value = '';
  let headers = null;
  for (let i = 0; i < fields.length; i += 2) {
    if (fields[i] === 'v') value = fields[i + 1];
    else if (fields[i] === 'h') headers = fields[i + 1];
  }
  let parsed = null;
  if (headers) {
    try {
      parsed = JSON.parse(headers);
    } catch {
      parsed = null;
    }
  }
  return { id, value, headers: toHeaders(parsed) };
};

const checkClient = (client, label) => {
  if (!client || !isFunction(client.xadd) || !isFunction(client.publish) || !isFunction(client.xreadgroup)) {
    throw new TypeError(`${label}: options.client must be an ioredis-shaped client (xadd/xreadgroup/publish/...)`);
  }
  return client;
};

const createRedisBroker = (options = {}) => {
  const {
    client,
    subscriber = null,
    connect = null,
    prefix = DEFAULT_PREFIX,
    logger = globalThis.console,
    blockMs = DEFAULT_BLOCK_MS,
    claimIdleMs = DEFAULT_CLAIM_IDLE_MS,
    maxLen = 0,
    inboxTtl = DEFAULT_INBOX_TTL_MS,
    generateId = null,
  } = options;
  // Strict: a new option, so a bad generator is refused at construction
  // rather than producing a name the broker rejects at connect time.
  const nextId = generateId === null ? generateUUID : resolveGenerateId(generateId, 'createRedisBroker').generate;
  // Two names the broker itself repeats in every log line and metric label
  // it emits, so the DEFAULT stays short; an injected generator is used
  // whole, per nextId above.
  const shortName = generateId === null ? () => generateUUID().slice(0, 8) : nextId;
  checkClient(client, 'createRedisBroker');
  if (connect !== null && !isFunction(connect)) {
    throw new TypeError('createRedisBroker: options.connect must be a function returning a new client');
  }
  if (connect === null && !isFunction(client.duplicate)) {
    throw new TypeError('createRedisBroker: options.client must support duplicate(), or pass options.connect');
  }
  const log = createLoggerWriter(logger).child({ component: 'broker', broker: 'redis' });
  const owned = new Set();
  let closed = false;

  // Every blocking read and every subscription needs a connection of its
  // own; they are opened on demand and quit by close() — an INJECTED client
  // never is, its lifetime is the caller's.
  const spawn = () => {
    const created = connect ? connect() : client.duplicate();
    owned.add(created);
    return created;
  };

  const key = (kind, name) => `${prefix}:${kind}:${encodeToken(name, { maxLength: 200 })}`;
  const report = (event, error, extra = {}) => log.error({ err: error, event, ...extra });

  // ---------------------------------------------------------------------
  // log

  const streamKey = (topic) => key('log', topic);

  const tip = async (topic) => {
    const rows = await client.xrevrange(streamKey(topic), '+', '-', 'COUNT', 1);
    return rows.length > 0 ? rows[0][0] : null;
  };

  const firstId = async (topic) => {
    try {
      const info = await client.xinfo('STREAM', streamKey(topic));
      const at = info.indexOf('first-entry');
      const entry = at >= 0 ? info[at + 1] : null;
      return entry ? entry[0] : null;
    } catch {
      return null; // no such key: an empty stream
    }
  };

  const range = async (topic, { after, limit }) => {
    const start = after === null || after === undefined ? '-' : `(${after}`;
    const rows = await client.xrange(streamKey(topic), start, '+', 'COUNT', limit);
    const entries = new Array(rows.length);
    for (let i = 0; i < rows.length; i++) entries[i] = decodeEntry(rows[i][0], rows[i][1]);
    return entries;
  };

  // One blocking XREAD loop per instance, multiplexed over every topic with
  // a live reader. A blocked read cannot be interrupted, so a topic added or
  // dropped takes effect within `blockMs` — which is why the default is a
  // second rather than the minutes a single-topic reader could afford.
  const tail = {
    connection: null,
    cursors: new Map(), // topic -> last id read
    handlers: new Map(), // topic -> onEntry
    running: false,
  };

  const pump = async () => {
    if (tail.running) return;
    tail.running = true;
    try {
      for (;;) {
        if (closed || tail.handlers.size === 0) return;
        const topics = Array.from(tail.handlers.keys());
        const keys = topics.map(streamKey);
        const ids = topics.map((topic) => tail.cursors.get(topic) ?? '$');
        let rows;
        try {
          rows = await tail.connection.xread('COUNT', 512, 'BLOCK', blockMs, 'STREAMS', ...keys, ...ids);
        } catch (error) {
          if (closed) return;
          report('broker.redis.tail', error);
          await new Promise((resolve) => setTimeout(resolve, Math.min(blockMs, 1000)));
          continue;
        }
        if (!rows) continue;
        for (const [name, entries] of rows) {
          const topic = topics[keys.indexOf(name)];
          const onEntry = tail.handlers.get(topic);
          for (const [id, fields] of entries) {
            tail.cursors.set(topic, id);
            if (onEntry) onEntry(decodeEntry(id, fields));
          }
        }
      }
    } finally {
      tail.running = false;
    }
  };

  const tails = new TopicTails({
    live: async (topic, { signal, onEntry }) => {
      if (!tail.connection) tail.connection = spawn();
      const from = (await tip(topic)) ?? '0-0';
      tail.cursors.set(topic, from);
      tail.handlers.set(topic, onEntry);
      signal.addEventListener(
        'abort',
        () => {
          tail.handlers.delete(topic);
          tail.cursors.delete(topic);
        },
        { once: true },
      );
      void pump();
      return from === '0-0' ? null : from;
    },
    range,
    covered: (cursor, entry) => compareIds(entry.id, cursor) <= 0,
    advance: (_cursor, entry) => entry.id,
  });

  const parseId = (text) => (typeof text === 'string' && text.length <= 64 && STREAM_ID.test(text) ? text : null);

  const read = (topic, options = {}) => {
    const { after = null, from = 'latest', signal = null } = options;
    if (from !== 'latest' && from !== 'earliest') {
      throw new TypeError("redis log.read: from must be 'latest' or 'earliest'");
    }
    if (after === null || after === undefined) return tails.read(topic, { from, signal });
    if (parseId(after) === null) {
      return failedRead(codedError('Malformed event id', 400));
    }
    // The position is checked before the tail joins: an id past the tip is
    // a client asking for a future, an id below the first retained one is a
    // gap the retention created.
    // Awaited by the iteration below AND by `ready`; a read nobody awaits
    // must not raise an unhandled rejection.
    const checked = (async () => {
      const [last, first] = await Promise.all([tip(topic), firstId(topic)]);
      if (last === null || compareIds(after, last) > 0) throw codedError('Event id is beyond the end of the log', 400);
      if (first !== null && compareIds(after, first) < 0) {
        throw codedError('Event history was trimmed past this id', 410);
      }
    })();
    checked.catch(() => {});
    const inner = tails.read(topic, { after, signal });
    const ready = Promise.all([checked, inner.ready]).then(() => undefined);
    ready.catch(() => {}); // surfaced through the iteration; never unhandled
    return {
      ready,
      [Symbol.asyncIterator]: () => {
        const iterator = inner[Symbol.asyncIterator]();
        let verified = false;
        return {
          next: async () => {
            if (!verified) {
              try {
                await checked;
              } catch (error) {
                // A refused position must not leave this reader on the
                // shared tail: an iterator whose next() rejects is never
                // closed by a for-await loop.
                await iterator.return?.();
                throw error;
              }
              verified = true;
            }
            return iterator.next();
          },
          return: (value) => iterator.return?.(value) ?? Promise.resolve({ value, done: true }),
        };
      },
    };
  };

  const failedRead = (error) => {
    const rejected = Promise.reject(error);
    rejected.catch(() => {});
    return {
      ready: rejected,
      [Symbol.asyncIterator]: () => ({
        next: () => Promise.reject(error),
        return: () => Promise.resolve({ value: undefined, done: true }),
      }),
    };
  };

  const append = async (topic, value, { headers = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    const fields = encodeFields(value, headers);
    if (maxLen > 0) return client.xadd(streamKey(topic), 'MAXLEN', '~', String(maxLen), '*', ...fields);
    return client.xadd(streamKey(topic), '*', ...fields);
  };

  // ---------------------------------------------------------------------
  // queue

  const queueKey = (name) => key('q', name);
  const delayedKey = (name) => `${queueKey(name)}:delayed`;

  const ensureGroup = async (name, group) => {
    try {
      await client.xgroup('CREATE', queueKey(name), group, '0', 'MKSTREAM');
    } catch (error) {
      // BUSYGROUP: the group is already there, with its own position.
      if (!/BUSYGROUP/.test(String(error?.message))) throw error;
    }
  };

  const produce = async (name, body, { headers = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    await client.xadd(queueKey(name), '*', ...encodeFields(body, headers));
  };

  const consume = async (name, onDelivery, options = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    if (!isFunction(onDelivery)) throw new TypeError('redis queue.consume: onDelivery must be a function');
    const { group = name, prefetch = DEFAULT_PREFETCH, deadLetter = null, signal = null } = options;
    if (!Number.isInteger(prefetch) || prefetch <= 0) {
      throw new TypeError('redis queue.consume: prefetch must be a positive integer');
    }
    const consumerName = `wrpc-${shortName()}`;
    const stream = queueKey(name);
    await ensureGroup(name, group);
    const connection = spawn();
    const state = { running: true, paused: false, inflight: 0, healthy: true, timer: null };
    // What this consumer is holding right now: XAUTOCLAIM would otherwise
    // claim its own in-flight entries back and deliver them twice.
    const held = new Set();
    // Entries XREADGROUP already moved into the pending list when a pause
    // landed: holding them here (rather than dropping them for XAUTOCLAIM)
    // is what makes resume() immediate.
    const buffered = [];

    const settleDone = (id) => {
      held.delete(id);
      state.inflight--;
      if (state.running && !state.paused) void loop();
    };

    const requeue = async (message, headers) => {
      // Streams have no "put it back": the copy carries the attempt count,
      // and the original leaves the pending list for good.
      await client.xadd(stream, '*', ...encodeFields(message.body, headers));
    };

    const dispatch = (id, fields, { claimed = false } = {}) => {
      const entry = decodeEntry(id, fields);
      const attempt = Number(entry.headers[ATTEMPT_HEADER] ?? '1') || 1;
      const message = { id, body: entry.value, headers: entry.headers };
      let settled = false;
      const finish = async (work) => {
        if (settled) return;
        settled = true;
        try {
          await work();
        } catch (error) {
          report('broker.redis.settle', error, { queue: name });
        } finally {
          settleDone(id);
        }
      };
      const drop = async () => {
        await client.xack(stream, group, id);
        await client.xdel(stream, id);
      };
      const copyHeaders = (extra) => {
        const headers = { ...entry.headers, ...extra };
        delete headers[REDELIVERED_HEADER];
        return { ...headers, [REDELIVERED_HEADER]: '1' };
      };
      const delivery = Object.freeze({
        id,
        body: entry.value,
        headers: entry.headers,
        attempt,
        // Either a copy this adapter re-added, or an entry reclaimed from a
        // consumer that stopped holding it.
        redelivered: claimed || entry.headers[REDELIVERED_HEADER] === '1',
        ack: () => finish(drop),
        retry: ({ delay = 0 } = {}) =>
          finish(async () => {
            const headers = copyHeaders({ [ATTEMPT_HEADER]: String(attempt + 1) });
            if (delay > 0) {
              await client.zadd(
                delayedKey(name),
                String(Date.now() + delay),
                JSON.stringify({ body: message.body, headers }),
              );
            } else await requeue(message, headers);
            await drop();
          }),
        release: () =>
          finish(async () => {
            await requeue(message, copyHeaders({ [ATTEMPT_HEADER]: String(attempt) }));
            await drop();
          }),
        deadLetter: (reason = '') =>
          finish(async () => {
            if (deadLetter) {
              const headers = {
                ...entry.headers,
                [DEAD_REASON_HEADER]: String(reason),
                [ATTEMPT_HEADER]: String(attempt),
              };
              await client.xadd(queueKey(deadLetter), '*', ...encodeFields(message.body, headers));
            }
            await drop();
          }),
      });
      state.inflight++;
      held.add(id);
      Promise.resolve()
        .then(() => onDelivery(delivery))
        .catch((error) => {
          report('broker.redis.delivery', error, { queue: name });
          void delivery.release();
        });
    };

    // Delayed retries and whatever a dead consumer still holds.
    const sweep = async () => {
      if (!state.running || state.paused || closed) return;
      try {
        const due = await client.zrangebyscore(delayedKey(name), '-inf', String(Date.now()), 'LIMIT', 0, 64);
        for (const item of due) {
          const removed = await client.zrem(delayedKey(name), item);
          if (removed === 0) continue; // another instance took it
          const { body, headers } = JSON.parse(item);
          await client.xadd(stream, '*', ...encodeFields(body, headers));
        }
        const [, claimed] = await client.xautoclaim(stream, group, consumerName, String(claimIdleMs), '0', 'COUNT', 16);
        for (const row of claimed ?? []) {
          if (!row || !row[1] || held.has(row[0])) continue;
          if (state.paused || !state.running || state.inflight >= prefetch) break;
          dispatch(row[0], row[1], { claimed: true });
        }
      } catch (error) {
        if (!closed) report('broker.redis.sweep', error, { queue: name });
      }
    };

    let reading = false;
    const loop = async () => {
      if (reading || !state.running || state.paused) return;
      reading = true;
      try {
        for (;;) {
          if (!state.running || state.paused || closed) return;
          const capacity = prefetch - state.inflight;
          if (capacity <= 0) return;
          let rows;
          try {
            rows = await connection.xreadgroup(
              'GROUP',
              group,
              consumerName,
              'COUNT',
              capacity,
              'BLOCK',
              blockMs,
              'STREAMS',
              stream,
              '>',
            );
            state.healthy = true;
          } catch (error) {
            if (closed || !state.running) return;
            state.healthy = false;
            report('broker.redis.read', error, { queue: name });
            await new Promise((resolve) => setTimeout(resolve, Math.min(blockMs, 1000)));
            continue;
          }
          if (!rows) continue;
          for (const [, entries] of rows) {
            for (const [id, fields] of entries) {
              if (state.paused || !state.running) buffered.push([id, fields]);
              else dispatch(id, fields);
            }
          }
        }
      } finally {
        reading = false;
      }
    };

    const interval = Math.max(50, Math.min(claimIdleMs, blockMs));
    state.timer = setInterval(() => void sweep(), interval);
    if (isFunction(state.timer.unref)) state.timer.unref();
    void loop();

    const stop = async () => {
      if (!state.running) return;
      state.running = false;
      clearInterval(state.timer);
      if (owned.delete(connection)) await quit(connection, { force: true });
      // What this consumer still holds stays in the group's pending list;
      // another consumer claims it through XAUTOCLAIM.
    };
    if (signal) signal.addEventListener('abort', () => void stop(), { once: true });
    return {
      stop,
      pause: async () => {
        state.paused = true;
      },
      resume: async () => {
        state.paused = false;
        while (buffered.length > 0 && state.inflight < prefetch) {
          const [id, fields] = buffered.shift();
          dispatch(id, fields);
        }
        void loop();
      },
      get healthy() {
        return state.running && state.healthy && !closed;
      },
    };
  };

  // ---------------------------------------------------------------------
  // direct

  const inboxKey = (address) => key('inbox', address);
  const listKey = (address) => `${inboxKey(address)}:list`;
  const groupKey = (address) => `${inboxKey(address)}:group`;

  const encodeMessage = (body, { headers, correlationId, replyTo }) => {
    const binary = typeof body !== 'string';
    return JSON.stringify({
      b: binary ? Buffer.from(toBytes(body)).toString('base64') : toText(body),
      n: binary ? 1 : 0,
      h: headers ? toHeaders(headers) : null,
      c: correlationId ?? null,
      r: replyTo ?? null,
    });
  };

  const decodeMessage = (text) => {
    const frame = JSON.parse(text);
    return {
      body: frame.n ? new Uint8Array(Buffer.from(frame.b, 'base64')) : frame.b,
      headers: toHeaders(frame.h),
      correlationId: frame.c ?? null,
      replyTo: frame.r ?? null,
    };
  };

  let pubsub = null; // the one subscribed connection for plain listeners
  const plain = new Map(); // channel -> Set<handler>

  const ensurePubsub = () => {
    if (pubsub) return pubsub;
    pubsub = subscriber ?? spawn();
    pubsub.on('message', (channel, message) => {
      const handlers = plain.get(channel);
      if (!handlers) return;
      let decoded;
      try {
        decoded = decodeMessage(message);
      } catch (error) {
        return void report('broker.redis.decode', error, { channel });
      }
      for (const handler of Array.from(handlers)) {
        try {
          const result = handler(decoded);
          if (result && isFunction(result.catch)) result.catch((error) => report('broker.redis.listener', error));
        } catch (error) {
          report('broker.redis.listener', error);
        }
      }
    });
    pubsub.on('error', (error) => report('broker.redis.pubsub', error));
    return pubsub;
  };

  const listenPlain = async (address, onMessage) => {
    const channel = inboxKey(address);
    const connection = ensurePubsub();
    let handlers = plain.get(channel);
    if (!handlers) {
      handlers = new Set();
      plain.set(channel, handlers);
      await connection.subscribe(channel);
    }
    handlers.add(onMessage);
    return async () => {
      const current = plain.get(channel);
      if (!current || !current.delete(onMessage) || current.size > 0) return;
      plain.delete(channel);
      if (isFunction(connection.unsubscribe)) await connection.unsubscribe(channel).catch(() => {});
    };
  };

  // Competing listeners take from a list, which is what makes delivery
  // exactly-one-of-them. A presence key (refreshed while the listener runs)
  // is how a sender knows a group is there at all — without one, the list
  // would grow for nobody.
  const listenGroup = async (address, onMessage) => {
    const list = listKey(address);
    const presence = groupKey(address);
    const connection = spawn();
    let running = true;
    await client.set(presence, '1', 'PX', String(inboxTtl));
    const beat = setInterval(
      () => {
        client.set(presence, '1', 'PX', String(inboxTtl)).catch((error) => report('broker.redis.presence', error));
      },
      Math.max(1000, inboxTtl / 3),
    );
    if (isFunction(beat.unref)) beat.unref();
    const loop = async () => {
      for (;;) {
        if (!running || closed) return;
        let popped;
        try {
          popped = await connection.blpop(list, Math.max(1, Math.round(blockMs / 1000)));
        } catch (error) {
          if (!running || closed) return;
          report('broker.redis.blpop', error, { address });
          await new Promise((resolve) => setTimeout(resolve, Math.min(blockMs, 1000)));
          continue;
        }
        if (!popped) continue;
        try {
          const message = decodeMessage(popped[1]);
          const result = onMessage(message);
          if (result && isFunction(result.catch)) result.catch((error) => report('broker.redis.listener', error));
        } catch (error) {
          report('broker.redis.listener', error, { address });
        }
      }
    };
    void loop();
    return async () => {
      running = false;
      clearInterval(beat);
      await client.del(presence).catch(() => {});
      if (owned.delete(connection)) await quit(connection, { force: true });
    };
  };

  const listen = async (address, onMessage, { group = null } = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    if (!isFunction(onMessage)) throw new TypeError('redis direct.listen: onMessage must be a function');
    if (typeof address !== 'string' || address.length === 0) {
      throw new TypeError('redis direct.listen: address must be a non-empty string');
    }
    return group === null || group === undefined ? listenPlain(address, onMessage) : listenGroup(address, onMessage);
  };

  const send = async (address, body, options = {}) => {
    if (closed) throw codedError('Broker is closed', 503);
    const payload = encodeMessage(body, options);
    const channel = inboxKey(address);
    const [subscribers, grouped] = await Promise.all([
      client.publish(channel, payload),
      client.exists(groupKey(address)),
    ]);
    if (grouped > 0) {
      // A TTL so a group that died does not leave work piling up forever;
      // the timeout hint (an RPC deadline) shortens it further.
      const ttl = Math.max(1, Math.min(options.timeout ?? inboxTtl, inboxTtl));
      await client.rpush(listKey(address), payload);
      await client.pexpire(listKey(address), String(ttl));
      return;
    }
    // Nobody at all: the fast 503 an RPC caller wants instead of a timeout.
    if (subscribers === 0) throw codedError(`No listener at ${address}`, 503);
  };

  // ---------------------------------------------------------------------

  // A connection parked in a blocking read cannot answer QUIT until the
  // block expires, so the ones this adapter blocks on are dropped instead.
  const quit = async (connection, { force = false } = {}) => {
    try {
      if (force && isFunction(connection.disconnect)) return void connection.disconnect();
      if (isFunction(connection.quit)) await connection.quit();
      else if (isFunction(connection.disconnect)) connection.disconnect();
    } catch (error) {
      report('broker.redis.quit', error);
    }
  };

  const backplane = createRedisAdapter({ pub: client, sub: subscriber ?? undefined, prefix, logger });

  const close = async () => {
    if (closed) return;
    closed = true;
    tails.close();
    tail.handlers.clear();
    backplane.close();
    plain.clear();
    for (const connection of Array.from(owned)) {
      owned.delete(connection);
      await quit(connection, { force: connection === tail.connection });
    }
  };

  return {
    name: 'redis',
    backplane,
    log: Object.freeze({ name: 'redis', append, read, parseId }),
    queue: Object.freeze({ name: 'redis', produce, consume }),
    direct: Object.freeze({ name: 'redis', inbox: () => `${prefix}.inbox.${nextId()}`, listen, send }),
    close,
  };
};

module.exports = { createRedisBroker, compareIds };
