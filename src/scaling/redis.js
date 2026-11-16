'use strict';

const { DEFAULT_PREFIX } = require('./memory.js');
const { createLoggerWriter } = require('../logging.js');

// Redis backplane, modelled on the ioredis API — `publish(channel, message)`,
// `subscribe(channel)` plus an `'message'(channel, message)` event, and
// `duplicate()` for the second connection a subscriber needs.
//
// Per the zero-dependency rule nothing is required here: the caller injects
// its own clients and they are validated structurally, so any client with
// that shape (ioredis, node-redis wrapped, a fake in tests) plugs in.
//
//   const { createRedisAdapter } = require('@alexify/wrpc/scaling');
//   const Redis = require('ioredis');
//   const pub = new Redis(url);
//   const backplane = createRedisAdapter({ pub });   // sub = pub.duplicate()
//
// A Redis connection in subscriber mode cannot issue other commands, which
// is why publishing and subscribing need two clients.

const isFunction = (value) => typeof value === 'function';

const checkPub = (pub) => {
  if (!pub || !isFunction(pub.publish)) {
    throw new TypeError('createRedisAdapter: options.pub must be a client with publish(channel, message)');
  }
  return pub;
};

const resolveSub = (pub, sub) => {
  const client = sub ?? (isFunction(pub.duplicate) ? pub.duplicate() : null);
  if (!client || !isFunction(client.subscribe) || !isFunction(client.on)) {
    throw new TypeError(
      'createRedisAdapter: options.sub must be a client with subscribe(channel) and on(event, listener), ' +
        'or options.pub must support duplicate()',
    );
  }
  return client;
};

const detach = (client, event, listener) => {
  if (isFunction(client.off)) return void client.off(event, listener);
  if (isFunction(client.removeListener)) client.removeListener(event, listener);
};

const createRedisAdapter = (options = {}) => {
  const { pub, sub, prefix = DEFAULT_PREFIX, logger = globalThis.console } = options;
  checkPub(pub);
  // A subscriber this adapter duplicated for itself is a connection nobody
  // else holds a reference to, so close() has to release it. An INJECTED one
  // belongs to the caller and is only unsubscribed.
  const owned = sub === undefined || sub === null;
  const subscriber = resolveSub(pub, sub);

  const handlers = new Map(); // full channel name -> Set<handler>
  let closed = false;

  const log = createLoggerWriter(logger);
  // `event` is on every other entry in the package and is the field the
  // logging guide tells operators to filter on; without it these lines
  // could not be alerted on alongside the rest.
  // Channels whose SUBSCRIBE has been sent and not answered yet.
  const subscribing = new Map();
  const report = (error) => void log.error({ err: error, event: 'backplane.redis', component: 'redis' });
  const settle = (result) => {
    if (result && isFunction(result.catch)) result.catch(report);
  };
  const key = (channel) => (prefix ? `${prefix}:${channel}` : channel);

  // ioredis delivers every channel of a connection through one 'message'
  // event, so a single listener demultiplexes for all subscriptions.
  const onMessage = (channel, message) => {
    const set = handlers.get(channel);
    if (!set) return;
    // Snapshot is load-bearing, unlike the one dropped in scaling/memory.js:
    // these handlers run SYNCHRONOUSLY, so one that subscribes or unsubscribes
    // during dispatch would otherwise perturb a live Set iterator.
    for (const handler of Array.from(set)) {
      try {
        handler(message);
      } catch (error) {
        report(error);
      }
    }
  };
  subscriber.on('message', onMessage);

  // The subscriber this adapter duplicated has no other listeners: without
  // this, its first network error is an 'error' event nobody handles — an
  // uncaught exception that takes the process down for a Redis blip.
  const onError = (error) => report(error);
  subscriber.on('error', onError);

  // ioredis re-emits 'ready' after every reconnect. Its own auto-resubscribe
  // covers the common case; re-issuing the SUBSCRIBEs here also covers
  // clients (or configurations) that do not, and a duplicate SUBSCRIBE is
  // idempotent on the server side.
  const onReady = () => {
    for (const name of handlers.keys()) {
      try {
        settle(subscriber.subscribe(name));
      } catch (error) {
        report(error);
      }
    }
  };
  subscriber.on('ready', onReady);

  const unsubscribe = (name) => {
    if (!isFunction(subscriber.unsubscribe)) return;
    try {
      settle(subscriber.unsubscribe(name));
    } catch (error) {
      report(error);
    }
  };

  return {
    name: 'redis',

    publish(channel, message) {
      if (closed) return;
      try {
        settle(pub.publish(key(channel), message));
      } catch (error) {
        report(error);
      }
    },

    subscribe(channel, handler) {
      if (!isFunction(handler)) {
        throw new TypeError('createRedisAdapter: subscribe handler must be a function');
      }
      if (closed) return () => {};
      const name = key(channel);
      const off = () => {
        const current = handlers.get(name);
        if (!current || !current.delete(handler)) return;
        if (current.size > 0) return;
        handlers.delete(name);
        unsubscribe(name);
      };
      const existing = handlers.get(name);
      if (existing) {
        existing.add(handler);
        // The channel's SUBSCRIBE is still on its way: this handler stands
        // or falls with it, like the one that issued it.
        const issued = subscribing.get(name);
        if (issued === undefined) return off;
        const joined = issued.then(() => off);
        joined.catch(() => {});
        return joined;
      }
      const set = new Set([handler]);
      handlers.set(name, set);
      // The SUBSCRIBE's own answer is what this answers with. It used to be
      // logged and dropped — the unsubscribe function came back at once,
      // whatever Redis said — so a refused SUBSCRIBE (an ACL without the
      // channel, a connection in the wrong state) looked like a subscription
      // to everything above: the retry of RoomsBackplane and of the cluster
      // never ran, and the server reported itself healthy while deaf.
      let issued = null;
      try {
        issued = Promise.resolve(subscriber.subscribe(name));
      } catch (error) {
        issued = Promise.reject(error);
      }
      const subscribed = issued.then(
        () => {
          subscribing.delete(name);
          return off;
        },
        (error) => {
          subscribing.delete(name);
          // Nothing is subscribed: the handlers of this attempt go with it,
          // or a retry would deliver every message twice once it succeeds.
          if (handlers.get(name) === set) handlers.delete(name);
          report(error);
          throw error;
        },
      );
      // Handled from its birth: a caller that takes the answer and never
      // looks at it must not get an unhandled rejection for a Redis refusal.
      subscribed.catch(() => {});
      subscribing.set(name, subscribed);
      return subscribed;
    },

    // Releases what this adapter owns — its subscriptions, its listener, and
    // the subscriber connection it opened itself. An INJECTED client is never
    // quit: its lifetime belongs to the caller.
    close() {
      if (closed) return;
      closed = true;
      for (const name of Array.from(handlers.keys())) unsubscribe(name);
      handlers.clear();
      subscribing.clear();
      detach(subscriber, 'message', onMessage);
      detach(subscriber, 'error', onError);
      detach(subscriber, 'ready', onReady);
      if (!owned) return;
      try {
        if (isFunction(subscriber.quit)) settle(subscriber.quit());
        else if (isFunction(subscriber.disconnect)) subscriber.disconnect();
      } catch (error) {
        report(error);
      }
    },
  };
};

module.exports = { createRedisAdapter };
