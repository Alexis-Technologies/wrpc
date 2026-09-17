'use strict';

// A small in-process Redis for the broker adapter's unit tests: the exact
// command surface src/broker/redis/index.js uses, with real blocking
// semantics (XREAD/XREADGROUP/BLPOP park until something arrives or the
// block expires). The same suites run against a real server in
// tests/broker/redis.integration.test.js — this one is what `pnpm test`
// runs, and what the coverage thresholds see.
//
// Not a *.test.js: node --test must not run it.

const { EventEmitter } = require('node:events');

const now = () => Date.now();

class FakeRedisServer {
  streams = new Map(); // key -> { entries: [{ id, fields, delivered }], groups: Map }
  lists = new Map();
  zsets = new Map();
  strings = new Map(); // key -> { value, expires }
  channels = new Map(); // channel -> Set<client>
  #waiters = new Set();
  #seq = 0;

  notify() {
    const waiters = Array.from(this.#waiters);
    this.#waiters.clear();
    for (const wake of waiters) wake();
  }

  wait(ms) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.#waiters.delete(wake);
        resolve();
      }, ms);
      const wake = () => {
        clearTimeout(timer);
        resolve();
      };
      this.#waiters.add(wake);
    });
  }

  nextId() {
    return `${now()}-${this.#seq++}`;
  }

  stream(key, create = true) {
    let entry = this.streams.get(key);
    if (!entry && create) {
      entry = { entries: [], groups: new Map() };
      this.streams.set(key, entry);
    }
    return entry;
  }

  live(key) {
    const record = this.strings.get(key);
    if (!record) return null;
    if (record.expires !== null && record.expires <= now()) {
      this.strings.delete(key);
      return null;
    }
    return record;
  }
}

const compare = (a, b) => {
  const [am, as] = a.split('-').map(Number);
  const [bm, bs] = b.split('-').map(Number);
  return am === bm ? as - bs : am - bm;
};

const after = (id) => (id.startsWith('(') ? { id: id.slice(1), exclusive: true } : { id, exclusive: false });

// One connection. `duplicate()` hands out another against the same server.
class FakeRedis extends EventEmitter {
  constructor(server = new FakeRedisServer(), options = {}) {
    super();
    this.server = server;
    this.options = options;
    this.subscribed = new Set();
    this.ended = false;
    this.quitCalls = 0;
    this.fail = options.fail ?? null; // (command, args) => Error | null
  }

  duplicate() {
    const copy = new FakeRedis(this.server, this.options);
    (this.server.clients ??= []).push(copy);
    return copy;
  }

  // `fail` may be set on the server (every connection, the usual case for a
  // test) or on one connection.
  #check(command, args) {
    if (this.ended) throw new Error('Connection is closed');
    const failure = (this.server.fail ?? this.fail)?.(command, args);
    if (failure) throw failure;
  }

  async quit() {
    this.quitCalls++;
    this.ended = true;
    for (const channel of this.subscribed) this.server.channels.get(channel)?.delete(this);
    this.subscribed.clear();
    this.server.notify();
    return 'OK';
  }

  disconnect() {
    void this.quit();
  }

  // ---- strings / keys

  async set(key, value, mode, ttl) {
    this.#check('set', [key]);
    const expires = mode === 'PX' ? now() + Number(ttl) : null;
    this.server.strings.set(key, { value, expires });
    return 'OK';
  }

  async exists(key) {
    this.#check('exists', [key]);
    return this.server.live(key) ? 1 : 0;
  }

  async del(key) {
    this.#check('del', [key]);
    return this.server.strings.delete(key) ? 1 : 0;
  }

  async pexpire(key, ms) {
    const list = this.server.lists.get(key);
    if (list) list.expires = now() + Number(ms);
    return 1;
  }

  // ---- lists

  async rpush(key, value) {
    this.#check('rpush', [key]);
    let list = this.server.lists.get(key);
    if (!list || (list.expires && list.expires <= now())) {
      list = { items: [], expires: null };
      this.server.lists.set(key, list);
    }
    list.items.push(value);
    this.server.notify();
    return list.items.length;
  }

  async blpop(key, seconds) {
    this.#check('blpop', [key]);
    const deadline = now() + Number(seconds) * 1000;
    for (;;) {
      const list = this.server.lists.get(key);
      if (list && list.items.length > 0) return [key, list.items.shift()];
      if (now() >= deadline || this.ended) return null;
      await this.server.wait(Math.min(25, Math.max(1, deadline - now())));
    }
  }

  // ---- sorted sets

  async zadd(key, score, member) {
    this.#check('zadd', [key]);
    let set = this.server.zsets.get(key);
    if (!set) {
      set = new Map();
      this.server.zsets.set(key, set);
    }
    set.set(member, Number(score));
    return 1;
  }

  async zrangebyscore(key, min, max, ...rest) {
    this.#check('zrangebyscore', [key]);
    const set = this.server.zsets.get(key);
    if (!set) return [];
    const low = min === '-inf' ? -Infinity : Number(min);
    const high = max === '+inf' ? Infinity : Number(max);
    const limit = rest[0] === 'LIMIT' ? Number(rest[2]) : Infinity;
    const out = [];
    for (const [member, score] of set) {
      if (score < low || score > high) continue;
      out.push(member);
      if (out.length >= limit) break;
    }
    return out;
  }

  async zrem(key, member) {
    this.#check('zrem', [key]);
    const set = this.server.zsets.get(key);
    return set && set.delete(member) ? 1 : 0;
  }

  // ---- pub/sub

  async publish(channel, message) {
    this.#check('publish', [channel]);
    const subscribers = this.server.channels.get(channel);
    if (!subscribers || subscribers.size === 0) return 0;
    for (const client of Array.from(subscribers)) {
      setImmediate(() => {
        if (!client.ended) client.emit('message', channel, message);
      });
    }
    return subscribers.size;
  }

  async subscribe(channel) {
    this.#check('subscribe', [channel]);
    let subscribers = this.server.channels.get(channel);
    if (!subscribers) {
      subscribers = new Set();
      this.server.channels.set(channel, subscribers);
    }
    subscribers.add(this);
    this.subscribed.add(channel);
    return this.subscribed.size;
  }

  async unsubscribe(channel) {
    this.server.channels.get(channel)?.delete(this);
    this.subscribed.delete(channel);
    return this.subscribed.size;
  }

  // ---- streams

  async xadd(key, ...args) {
    this.#check('xadd', [key]);
    let index = 0;
    let maxLen = 0;
    if (args[0] === 'MAXLEN') {
      maxLen = Number(args[2]);
      index = 3;
    }
    index++; // the '*' id
    const stream = this.server.stream(key);
    const id = this.server.nextId();
    stream.entries.push({ id, fields: args.slice(index) });
    if (maxLen > 0 && stream.entries.length > maxLen) stream.entries.splice(0, stream.entries.length - maxLen);
    this.server.notify();
    return id;
  }

  async xrange(key, start, end, countKeyword, count) {
    this.#check('xrange', [key]);
    const stream = this.server.stream(key, false);
    if (!stream) return [];
    const from = start === '-' ? null : after(start);
    const rows = [];
    for (const entry of stream.entries) {
      if (from) {
        const order = compare(entry.id, from.id);
        if (order < 0 || (from.exclusive && order === 0)) continue;
      }
      if (end !== '+' && compare(entry.id, end) > 0) break;
      rows.push([entry.id, entry.fields]);
      if (countKeyword === 'COUNT' && rows.length >= Number(count)) break;
    }
    return rows;
  }

  async xrevrange(key, _end, _start, _countKeyword, count) {
    this.#check('xrevrange', [key]);
    const stream = this.server.stream(key, false);
    if (!stream) return [];
    const rows = [];
    for (let i = stream.entries.length - 1; i >= 0 && rows.length < Number(count ?? 1); i--) {
      rows.push([stream.entries[i].id, stream.entries[i].fields]);
    }
    return rows;
  }

  async xinfo(_sub, key) {
    this.#check('xinfo', [key]);
    const stream = this.server.stream(key, false);
    if (!stream || stream.entries.length === 0) throw new Error('ERR no such key');
    const first = stream.entries[0];
    const last = stream.entries[stream.entries.length - 1];
    return [
      'length',
      stream.entries.length,
      'first-entry',
      [first.id, first.fields],
      'last-entry',
      [last.id, last.fields],
    ];
  }

  async xread(...args) {
    this.#check('xread', args);
    let index = 0;
    let count = 10;
    let block = null;
    while (args[index] !== 'STREAMS') {
      if (args[index] === 'COUNT') count = Number(args[index + 1]);
      if (args[index] === 'BLOCK') block = Number(args[index + 1]);
      index += 2;
    }
    const rest = args.slice(index + 1);
    const keys = rest.slice(0, rest.length / 2);
    const ids = rest.slice(rest.length / 2).map((id, i) => {
      if (id !== '$') return id;
      const stream = this.server.stream(keys[i], false);
      return stream && stream.entries.length > 0 ? stream.entries[stream.entries.length - 1].id : '0-0';
    });
    const deadline = block === null ? 0 : now() + block;
    for (;;) {
      const result = [];
      for (let i = 0; i < keys.length; i++) {
        const stream = this.server.stream(keys[i], false);
        if (!stream) continue;
        const rows = [];
        for (const entry of stream.entries) {
          if (compare(entry.id, ids[i]) <= 0) continue;
          rows.push([entry.id, entry.fields]);
          if (rows.length >= count) break;
        }
        if (rows.length > 0) result.push([keys[i], rows]);
      }
      if (result.length > 0) return result;
      if (block === null || now() >= deadline || this.ended) return null;
      await this.server.wait(Math.min(25, Math.max(1, deadline - now())));
    }
  }

  async xgroup(action, key, group, id, mkstream) {
    this.#check('xgroup', [key]);
    if (action !== 'CREATE') return 'OK';
    const stream = this.server.stream(key, mkstream === 'MKSTREAM');
    if (!stream) throw new Error('ERR The XGROUP subcommand requires the key to exist');
    if (stream.groups.has(group)) {
      const error = new Error('BUSYGROUP Consumer Group name already exists');
      throw error;
    }
    const last = id === '$' && stream.entries.length > 0 ? stream.entries[stream.entries.length - 1].id : '0-0';
    stream.groups.set(group, { lastId: id === '0' ? '0-0' : last, pending: new Map() });
    return 'OK';
  }

  async xreadgroup(...args) {
    this.#check('xreadgroup', args);
    const group = args[1];
    const consumer = args[2];
    let index = 3;
    let count = 10;
    let block = null;
    while (args[index] !== 'STREAMS') {
      if (args[index] === 'COUNT') count = Number(args[index + 1]);
      if (args[index] === 'BLOCK') block = Number(args[index + 1]);
      index += 2;
    }
    const key = args[index + 1];
    const deadline = block === null ? 0 : now() + block;
    for (;;) {
      const stream = this.server.stream(key, false);
      const state = stream?.groups.get(group);
      if (state) {
        const rows = [];
        for (const entry of stream.entries) {
          if (compare(entry.id, state.lastId) <= 0) continue;
          state.lastId = entry.id;
          state.pending.set(entry.id, { consumer, since: now() });
          rows.push([entry.id, entry.fields]);
          if (rows.length >= count) break;
        }
        if (rows.length > 0) return [[key, rows]];
      }
      if (block === null || now() >= deadline || this.ended) return null;
      await this.server.wait(Math.min(25, Math.max(1, deadline - now())));
    }
  }

  async xack(key, group, id) {
    this.#check('xack', [key]);
    const state = this.server.stream(key, false)?.groups.get(group);
    return state && state.pending.delete(id) ? 1 : 0;
  }

  async xdel(key, id) {
    this.#check('xdel', [key]);
    const stream = this.server.stream(key, false);
    if (!stream) return 0;
    const at = stream.entries.findIndex((entry) => entry.id === id);
    if (at < 0) return 0;
    stream.entries.splice(at, 1);
    return 1;
  }

  async xautoclaim(key, group, consumer, minIdle, _start, _countKeyword, count) {
    this.#check('xautoclaim', [key]);
    const stream = this.server.stream(key, false);
    const state = stream?.groups.get(group);
    if (!state) return ['0-0', [], []];
    const claimed = [];
    for (const [id, holder] of state.pending) {
      if (now() - holder.since < Number(minIdle)) continue;
      const entry = stream.entries.find((item) => item.id === id);
      if (!entry) {
        state.pending.delete(id);
        continue;
      }
      holder.consumer = consumer;
      holder.since = now();
      claimed.push([entry.id, entry.fields]);
      if (claimed.length >= Number(count ?? 10)) break;
    }
    return ['0-0', claimed, []];
  }
}

const createFakeRedis = (options = {}) => new FakeRedis(new FakeRedisServer(), options);

module.exports = { FakeRedis, FakeRedisServer, createFakeRedis };
