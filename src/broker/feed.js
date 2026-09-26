'use strict';

// A durable subscription feed: `procedure.subscription({ handler: brokerFeed(...) })`.
//
// The "broker-backed feed" recipe of docs/guide/rooms.md#delivery, made
// first-class. A broker log guarantees delivery to the SERVER; the last hop
// to a browser still loses whatever was in flight during a reconnect unless
// the server replays from where the client left off — and the broker
// already keeps that position. So every value is `tracked()` with the log's
// resume token, and a re-subscribe carrying `lastEventId` resumes there, on
// whichever instance the client reconnected to.
//
// `lastEventId` is peer-controlled. It is length-capped, syntax-checked by
// the log's own parseId before it reaches the broker, and — with `secret` —
// HMAC-signed, so a client cannot replay a topic from an offset it made up.
// It is a POSITION, never a permission: who may read the topic is the
// procedure's `access` (and the topic resolver's) business.

const { tracked } = require('../rpc/subscriptions.js');
const { capabilityOf, brokerName } = require('./port.js');
const { codedError, signId, openId, toText } = require('./ids.js');
const { createBrokerSealing } = require('./sealing.js');

const DEFAULT_MAX_ID_LENGTH = 512;
// A sealed entry the feed cannot open is logged at warn once per reason per
// this interval, with the running count, and at debug in between: a topic
// holding a thousand entries under a key this service dropped is one line
// per subscriber, not a thousand.
const REFUSAL_INTERVAL = 10_000;

const DECODERS = Object.freeze({
  json: (text) => JSON.parse(text),
  text: (text) => text,
});

const isIterable = (value) =>
  value !== null &&
  typeof value === 'object' &&
  (typeof value[Symbol.asyncIterator] === 'function' || typeof value[Symbol.iterator] === 'function');

const brokerFeed = (broker, topic, options = {}) => {
  const log = capabilityOf(broker, 'log', 'brokerFeed');
  const system = brokerName(log) === 'custom' ? brokerName(broker) : brokerName(log);
  if (typeof topic !== 'function' && (typeof topic !== 'string' || topic.length === 0)) {
    throw new TypeError('brokerFeed: topic must be a non-empty string or a (context, args) => topic function');
  }
  const {
    from = 'latest',
    decode = 'json',
    map = null,
    onGap = null,
    secret = null,
    maxIdLength = DEFAULT_MAX_ID_LENGTH,
    encryption = null,
  } = options;
  // The reading half of a publisher's `encryption`: entries are opened
  // before they are decoded, bound to the topic they are read from. No
  // replay window — a log is read again, by design.
  const sealing = createBrokerSealing(encryption, 'brokerFeed: options', {
    layer: 'broker-log',
    replay: false,
    text: true,
  });
  if (from !== 'latest' && from !== 'earliest') {
    throw new TypeError("brokerFeed: from must be 'latest' or 'earliest'");
  }
  const decodeValue = typeof decode === 'function' ? decode : DECODERS[decode];
  if (typeof decodeValue !== 'function') {
    throw new TypeError("brokerFeed: decode must be 'json', 'text' or a function");
  }
  if (map !== null && typeof map !== 'function') throw new TypeError('brokerFeed: map must be a function');
  if (onGap !== null && typeof onGap !== 'function') throw new TypeError('brokerFeed: onGap must be a function');
  if (secret !== null && (typeof secret !== 'string' || secret.length === 0)) {
    throw new TypeError('brokerFeed: secret must be a non-empty string');
  }
  if (!Number.isInteger(maxIdLength) || maxIdLength <= 0) {
    throw new TypeError('brokerFeed: maxIdLength must be a positive integer');
  }

  // The resume position a peer-supplied id stands for, or the refusal code.
  // `reason` separates the three ways it can be refused, because they mean
  // very different things to an operator: `length` and `syntax` are a stale
  // or garbled client, while `signature` is a token that was TAMPERED with
  // — someone trying to read a topic from an offset they made up. That last
  // one was indistinguishable from the other two, and silent.
  // A signed id is bound to the topic it was issued for: a token from one
  // feed does not position a reader on another that shares the secret.
  const position = (lastEventId, name) => {
    if (lastEventId === undefined || lastEventId === null) return { after: null, code: null, reason: null };
    if (typeof lastEventId !== 'string' || lastEventId.length > maxIdLength) {
      return { after: null, code: 400, reason: 'length' };
    }
    const id = secret === null ? lastEventId : openId(secret, lastEventId, name);
    if (id === null) return { after: null, code: 400, reason: 'signature' };
    if (log.parseId(id) === null) return { after: null, code: 400, reason: 'syntax' };
    return { after: id, code: null, reason: null };
  };

  return async function* feed(context, args, { lastEventId = null, signal: outer = null } = {}) {
    const name = typeof topic === 'function' ? await topic(context, args) : topic;
    if (typeof name !== 'string' || name.length === 0) {
      throw codedError('brokerFeed: the topic resolver must answer a non-empty string', 500);
    }
    let { after, code, reason } = position(lastEventId, name);
    let resumedFrom = lastEventId;
    if (reason !== null) {
      // The id itself is peer-controlled text and is NOT logged; its length
      // and the reason are what an operator can act on.
      const level = reason === 'signature' ? 'warn' : 'debug';
      context?.log?.[level]({ event: 'broker.feed.resume', topic: name, reason, length: lastEventId.length });
    }
    // Every read this feed opens is scoped to the feed: a read left behind
    // — the gap read a snapshot ran under, a read a refused position ended
    // — is aborted when the feed ends, not when the subscriber's own signal
    // (which a pump may never fire) lets it go.
    const scope = new AbortController();
    const signal = scope.signal;
    const onAbort = () => scope.abort();
    outer?.addEventListener('abort', onAbort, { once: true });
    if (outer?.aborted) scope.abort();
    // Refusals of this subscriber's read, counted per reason (see
    // REFUSAL_INTERVAL); the metric counts every one.
    const refusals = new Map();
    const refuse = (id, reason) => {
      context?.otel?.recordBrokerRefusal(system, reason);
      const now = Date.now();
      let tally = refusals.get(reason);
      if (tally === undefined) {
        tally = { count: 0, since: now, logged: 0 };
        refusals.set(reason, tally);
      }
      tally.count++;
      const level = tally.count === 1 || now - tally.since >= REFUSAL_INTERVAL ? 'warn' : 'debug';
      if (level === 'warn') {
        tally.since = now;
        tally.logged = tally.count;
      }
      context?.log?.[level]({ event: 'feed.refused', topic: name, id, reason, count: tally.count });
    };
    try {
      for (;;) {
        if (signal.aborted) return;
        let read;
        if (code !== null) {
          if (!onGap) {
            throw codedError(code === 400 ? 'Invalid event id' : 'Event history is no longer available', code);
          }
          // A subscriber that fell behind retention, or one resuming from a
          // position this log no longer has: real event loss, answered with a
          // snapshot. Worth a line — it is the signal retention is tuned from.
          context?.log?.info({ event: 'broker.feed.gap', topic: name, code });
          // Positioned BEFORE the snapshot is built: whatever is appended
          // while the application assembles it is read afterwards, not lost.
          read = log.read(name, { from: 'latest', signal });
          await read.ready;
          const snapshot = await onGap(context, args, { lastEventId: resumedFrom, code });
          if (isIterable(snapshot)) yield* snapshot;
          else if (snapshot !== undefined) yield snapshot;
          code = null;
        } else {
          read = log.read(name, after !== null ? { after, signal } : { from, signal });
        }
        let delivered = false;
        try {
          for await (const raw of read) {
            delivered = true;
            after = raw.id;
            let entry = raw;
            if (sealing !== null) {
              const opened = sealing.open(name, { headers: raw.headers, body: raw.value });
              if (opened.refused !== undefined) {
                // Skipped like an undecodable entry — and never yielded as it is.
                refuse(raw.id, opened.refused);
                continue;
              }
              if (opened.sealed) entry = { ...raw, headers: opened.headers, value: toText(opened.body) };
            }
            let value;
            try {
              value = decodeValue(entry.value);
            } catch (error) {
              // One undecodable entry must not end every subscriber's feed.
              context?.log?.warn({ event: 'feed.decode', topic: name, id: entry.id, err: error });
              continue;
            }
            if (map !== null) {
              value = await map(value, entry, context);
              if (value === undefined) continue;
            }
            yield tracked(secret === null ? entry.id : signId(secret, entry.id, name), value);
          }
          return;
        } catch (error) {
          // A position the log refuses — at the start (a stale or forged id)
          // or mid-stream (a reader the retention overtook) — becomes a gap
          // the application can answer with a snapshot.
          const refused = error?.code === 410 || (error?.code === 400 && !delivered);
          if (!refused || signal.aborted) throw error;
          code = error.code;
          // What the client itself holds: its own id at the start, the last
          // token this feed handed it mid-stream.
          resumedFrom = after === null ? lastEventId : secret === null ? after : signId(secret, after, name);
          after = null;
        }
      }
    } finally {
      outer?.removeEventListener('abort', onAbort);
      scope.abort();
      // What the debug lines held since the last warn, so a feed that ended
      // between two intervals still accounts for every refusal.
      for (const [reason, tally] of refusals) {
        if (tally.count > tally.logged) {
          context?.log?.info({ event: 'feed.refused', topic: name, reason, count: tally.count, summary: true });
        }
      }
    }
  };
};

module.exports = { brokerFeed };
