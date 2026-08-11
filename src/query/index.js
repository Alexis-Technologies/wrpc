'use strict';

// TanStack Query bindings, in the shape tRPC v11 settled on: not hooks, but
// OPTION FACTORIES. `wq.queryOptions(['chat', 'list'], args)` hands back the
// `{ queryKey, queryFn }` a user spreads into their own `useQuery`, so this
// file works with React Query, Solid Query, Svelte Query and query-core alike
// — and never imports any of them.
//
// Nothing is required here, by design. The wrpc client and the queryClient are
// INJECTED and checked structurally, which is the same rule the engine, the
// backplane and the framework adapters follow: zero runtime dependencies, and
// duck typing instead of instanceof.
//
//   const { QueryClient, useQuery } = ...              // the user's own
//   const wq = createQueryUtils(client, { queryClient });
//   useQuery(wq.queryOptions(['chat', 'list'], { room: 'a' }));
//
// Paths are resolved LAZILY, inside queryFn/mutationFn rather than when the
// options are built. That is not an implementation detail: `client.api` only
// exists after `load()`, and it is rebuilt from scratch on every reconnect —
// options captured before either would otherwise point at a dead method.

const isFunction = (value) => typeof value === 'function';

const requireClient = (client) => {
  if (!client || typeof client.api !== 'object' || client.api === null) {
    throw new TypeError('createQueryUtils(client): client must be a WrpcClient (an object with an `api`)');
  }
};

const requireQueryClient = (queryClient, where) => {
  if (!queryClient || !isFunction(queryClient.setQueryData)) {
    throw new TypeError(`${where}: pass a queryClient with a setQueryData() method (TanStack QueryClient)`);
  }
};

// Default cache update: the newest value REPLACES what was there. Appending by
// default would look friendlier and quietly grow without bound for the life of
// the subscription, which is the wrong default for a feed that never ends.
const replace = (_previous, data) => data;

const createQueryUtils = (client, options = {}) => {
  requireClient(client);
  const { prefix = [], queryClient: defaultQueryClient = null } = options;
  if (!Array.isArray(prefix)) throw new TypeError('createQueryUtils: prefix must be an array');
  if (defaultQueryClient !== null) requireQueryClient(defaultQueryClient, 'createQueryUtils');

  const checkPath = (path) => {
    if (!Array.isArray(path) || path.length !== 2) {
      throw new TypeError(`wrpc/query: a path is [unit, method], got ${JSON.stringify(path)}`);
    }
    const [unit, method] = path;
    if (typeof unit !== 'string' || typeof method !== 'string') {
      throw new TypeError(`wrpc/query: a path is [unit, method] of strings, got ${JSON.stringify(path)}`);
    }
    return [unit, method];
  };

  // Own properties only, both hops. A unit object is an Emitter, so
  // ['chat', 'on'] would otherwise resolve to Emitter.prototype.on and be
  // called as if it were a procedure; and `api` is a plain object, so
  // ['constructor', 'x'] would reach Object.prototype.
  const member = (unit, method) => {
    if (!Object.hasOwn(client.api, unit)) {
      throw new Error(`wrpc/query: unit '${unit}' is not loaded — await client.load('${unit}') first`);
    }
    const loaded = client.api[unit];
    if (!Object.hasOwn(loaded, method)) {
      throw new Error(`wrpc/query: unit '${unit}' has no method '${method}'`);
    }
    return loaded[method];
  };

  const resolveCall = (path) => {
    const [unit, method] = checkPath(path);
    const found = member(unit, method);
    if (found?.kind === 'subscription') {
      throw new Error(`wrpc/query: ${unit}/${method} is a subscription — use subscriptionHandler()`);
    }
    if (!isFunction(found)) throw new Error(`wrpc/query: ${unit}/${method} is not callable`);
    return found;
  };

  const resolveSubscription = (path) => {
    const [unit, method] = checkPath(path);
    const found = member(unit, method);
    if (isFunction(found)) {
      throw new Error(`wrpc/query: ${unit}/${method} is a call — use queryOptions() or mutationOptions()`);
    }
    if (found?.kind !== 'subscription') throw new Error(`wrpc/query: ${unit}/${method} is not a subscription`);
    return found;
  };

  /**
   * The cache key for a path. Prefix-shaped on purpose: TanStack matches keys
   * by prefix, so `invalidateQueries({ queryKey: wq.queryKey(['chat']) })`
   * — or just `['chat']` — reaches every query of the unit, and adding `args`
   * last keeps two different arguments in two different cache entries.
   */
  const queryKey = (path, args) => {
    const base = Array.isArray(path) ? path : [path];
    return args === undefined ? [...prefix, ...base] : [...prefix, ...base, args];
  };

  return Object.freeze({
    queryKey,

    /**
     * `{ queryKey, queryFn }` for useQuery; `extra` is passed through.
     *
     * The path is checked here — a typo is a programmer error and should
     * surface where it was written — but RESOLVED inside queryFn, which is
     * what lets options outlive a `load()` or a reconnect.
     */
    queryOptions(path, args, extra) {
      checkPath(path);
      return {
        ...extra,
        queryKey: queryKey(path, args),
        // `async` so that a resolution failure (unit not loaded, method gone)
        // REJECTS rather than throwing out of queryFn: a query's error channel
        // is where a caller is looking, and not every consumer wraps the call.
        // TanStack hands queryFn an AbortSignal and aborts it when the query is
        // cancelled or the component unmounts; forwarding it is what makes that
        // reach the server as `{type:'cancel'}`.
        queryFn: async (context) => resolveCall(path)(args ?? {}, { signal: context?.signal }),
      };
    },

    /** `{ mutationKey, mutationFn }` for useMutation; the variables ARE the args. */
    mutationOptions(path, extra) {
      checkPath(path);
      return {
        ...extra,
        mutationKey: queryKey(path),
        mutationFn: async (args) => resolveCall(path)(args ?? {}),
      };
    },

    /**
     * Bridges a subscription into the cache: every value is written through
     * `queryClient.setQueryData`, so the same component that reads a query
     * reads live data with no extra wiring. Returns the wrpc subscription
     * handle — call `unsubscribe()` on teardown.
     *
     * A reconnect needs nothing from the caller for DELIVERY: the client
     * re-opens its subscriptions from the last eventId it saw, on the SAME
     * record, so this keeps receiving without re-subscribing and without
     * leaking a second listener. It does not make resume free of duplicates
     * though — an untracked feed (one that never yields `tracked()`) has no
     * resume point, so it replays from the start, and an accumulating `update`
     * appends the replay to what it already had. Label values with `tracked()`
     * server-side, or make `update` idempotent.
     */
    subscriptionHandler(path, args, handlerOptions = {}) {
      // The path first: a path that is not a path is a more fundamental
      // mistake than a missing queryClient, and it names the one in the message.
      const [unit, method] = checkPath(path);
      const {
        queryClient = defaultQueryClient,
        queryKey: key = queryKey(path, args),
        update = replace,
        onData,
        onError,
        onEnd,
        lastEventId,
      } = handlerOptions;
      requireQueryClient(queryClient, `wrpc/query: subscriptionHandler(${unit}/${method})`);
      if (!isFunction(update)) throw new TypeError('wrpc/query: subscriptionHandler update must be a function');
      const subscription = resolveSubscription(path);
      return subscription.subscribe(args ?? {}, {
        lastEventId,
        onData: (data) => {
          // Contained on purpose. This runs synchronously inside the client's
          // packet dispatch, so a throw here (a buggy `update`, or a key
          // TanStack cannot hash) would escape as a CLIENT-level 'error' and
          // abandon the rest of the packet — a cache-write failure belongs to
          // this subscription. With no onError it still propagates: a feed
          // that stopped working must not do so quietly.
          try {
            queryClient.setQueryData(key, (previous) => update(previous, data));
          } catch (error) {
            if (!onError) throw error;
            onError(error);
            return;
          }
          onData?.(data);
        },
        onError,
        onEnd,
      });
    },
  });
};

module.exports = { createQueryUtils };
