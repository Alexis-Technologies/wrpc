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
const { capabilityOf } = require('./port.js');
const { codedError, signId, openId } = require('./ids.js');

const DEFAULT_MAX_ID_LENGTH = 512;

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
  } = options;
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
  const position = (lastEventId) => {
    if (lastEventId === undefined || lastEventId === null) return { after: null, code: null, reason: null };
    if (typeof lastEventId !== 'string' || lastEventId.length > maxIdLength) {
      return { after: null, code: 400, reason: 'length' };
    }
    const id = secret === null ? lastEventId : openId(secret, lastEventId);
    if (id === null) return { after: null, code: 400, reason: 'signature' };
    if (log.parseId(id) === null) return { after: null, code: 400, reason: 'syntax' };
    return { after: id, code: null, reason: null };
  };

  return async function* feed(context, args, { lastEventId = null, signal = null } = {}) {
    const name = typeof topic === 'function' ? await topic(context, args) : topic;
    if (typeof name !== 'string' || name.length === 0) {
      throw codedError('brokerFeed: the topic resolver must answer a non-empty string', 500);
    }
    let { after, code, reason } = position(lastEventId);
    let resumedFrom = lastEventId;
    if (reason !== null) {
      // The id itself is peer-controlled text and is NOT logged; its length
      // and the reason are what an operator can act on.
      const level = reason === 'signature' ? 'warn' : 'debug';
      context?.log?.[level]({ event: 'broker.feed.resume', topic: name, reason, length: lastEventId.length });
    }
    for (;;) {
      if (signal?.aborted) return;
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
        for await (const entry of read) {
          delivered = true;
          after = entry.id;
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
          yield tracked(secret === null ? entry.id : signId(secret, entry.id), value);
        }
        return;
      } catch (error) {
        // A position the log refuses — at the start (a stale or forged id)
        // or mid-stream (a reader the retention overtook) — becomes a gap
        // the application can answer with a snapshot.
        const refused = error?.code === 410 || (error?.code === 400 && !delivered);
        if (!refused || signal?.aborted) throw error;
        code = error.code;
        // What the client itself holds: its own id at the start, the last
        // token this feed handed it mid-stream.
        resumedFrom = after === null ? lastEventId : secret === null ? after : signId(secret, after);
        after = null;
      }
    }
  };
};

module.exports = { brokerFeed };
