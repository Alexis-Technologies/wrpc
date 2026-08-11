/**
 * `@alexify/wrpc/query` — TanStack Query bindings as option factories.
 *
 * Framework-agnostic and dependency-free: this subpath produces the plain
 * `{ queryKey, queryFn }` / `{ mutationKey, mutationFn }` objects that
 * `useQuery`/`useMutation` (React, Solid, Svelte, Vue) or query-core's
 * observers accept, and imports neither React nor `@tanstack/*`. The wrpc
 * client and the `QueryClient` are injected by the caller.
 *
 * With a contract type the paths, arguments and results are all checked:
 *
 * ```ts
 * const wq = createQueryUtils<Api>(client, { queryClient });
 * useQuery(wq.queryOptions(['chat', 'list'], { room: 'a' }));
 * ```
 */

import type {
  FirstArg,
  IsAny,
  Subscription,
  SubscriptionContract,
  UntypedApi,
  WrpcClient,
  WrpcError,
} from './index.js';

/** Units of a contract, as string keys. */
export type ApiUnits<Api> = Extract<keyof Api, string>;

/** Methods of one unit, as string keys. */
export type ApiMethods<Api, Unit extends ApiUnits<Api>> = Extract<keyof Api[Unit], string>;

/** One member of a contract, looked up by path. */
export type ApiMember<Api, Path extends [ApiUnits<Api>, string]> = Path[1] extends keyof Api[Path[0]]
  ? Api[Path[0]][Path[1]]
  : never;

type PathsWhere<Api, Subscriptions extends boolean> = {
  [Unit in ApiUnits<Api>]: {
    [Method in ApiMethods<Api, Unit>]: IsAny<Api[Unit][Method]> extends true ? [Unit, Method]
      : Api[Unit][Method] extends SubscriptionContract<any, any> ? (Subscriptions extends true ? [Unit, Method] : never)
      : Subscriptions extends true ? never
      : [Unit, Method];
  }[ApiMethods<Api, Unit>];
}[ApiUnits<Api>];

/** Every `[unit, method]` of the contract that is a call. */
export type CallPath<Api> = PathsWhere<Api, false>;

/** Every `[unit, method]` of the contract that is a subscription. */
export type SubscriptionPath<Api> = PathsWhere<Api, true>;

/**
 * What the call at `Path` takes. Projected from the parameter tuple through
 * {@link FirstArg}, so a zero-argument procedure is `void` rather than the
 * `unknown` naive inference produces.
 */
export type CallArgs<Api, Path extends [ApiUnits<Api>, string]> = IsAny<ApiMember<Api, Path>> extends true ? any
  : ApiMember<Api, Path> extends (...args: infer Params) => any ? FirstArg<Params>
  : unknown;

/** What the call at `Path` answers with. */
export type CallResult<Api, Path extends [ApiUnits<Api>, string]> = IsAny<ApiMember<Api, Path>> extends true ? any
  : ApiMember<Api, Path> extends (...args: Array<any>) => infer Result ? Awaited<Result>
  : unknown;

/**
 * `[args]` when the procedure needs them, `[args?]` when it does not — so a
 * forgotten argument object is a compile error rather than a 400 from the
 * server's input validator.
 */
export type CallParams<Api, Path extends [ApiUnits<Api>, string], Rest extends Array<unknown>> = [
  CallArgs<Api, Path>,
] extends [void] ? [args?: CallArgs<Api, Path>, ...rest: Rest]
  : [args: CallArgs<Api, Path>, ...rest: Rest];

/** What the subscription at `Path` takes. */
export type SubscriptionArgs<Api, Path extends [ApiUnits<Api>, string]> = IsAny<ApiMember<Api, Path>> extends true ? any
  : ApiMember<Api, Path> extends SubscriptionContract<infer Args, any> ? Args
  : unknown;

/** What the subscription at `Path` yields. */
export type SubscriptionData<Api, Path extends [ApiUnits<Api>, string]> = IsAny<ApiMember<Api, Path>> extends true ? any
  : ApiMember<Api, Path> extends SubscriptionContract<any, infer Data> ? Data
  : unknown;

/** A cache key: the optional prefix, the path, then the arguments. */
export type WrpcQueryKey = Array<unknown>;

/** The subset of `QueryFunctionContext` this uses; TanStack's own is a superset. */
export interface QueryFnContext {
  signal?: AbortSignal;
}

/**
 * Assignable to TanStack's `UseQueryOptions`/`QueryObserverOptions`: the two
 * fields TanStack requires, plus whatever was passed through as `extra`.
 */
export interface WrpcQueryOptions<Result> {
  queryKey: WrpcQueryKey;
  queryFn(context?: QueryFnContext): Promise<Result>;
}

export interface WrpcMutationOptions<Args, Result> {
  mutationKey: WrpcQueryKey;
  mutationFn(args?: Args): Promise<Result>;
}

/** The one method of `QueryClient` this needs. Structural: no import. */
export interface QueryCache {
  setQueryData(queryKey: any, updater: (previous: any) => any): unknown;
}

export interface SubscriptionHandlerOptions<Data, Cached = unknown> {
  /** Required unless one was given to {@link createQueryUtils}. */
  queryClient?: QueryCache;
  /** Defaults to `queryKey(path, args)`. */
  queryKey?: WrpcQueryKey;
  /**
   * How a value enters the cache. The default REPLACES the entry; append with
   * `(previous, data) => [...(previous ?? []), data]` when the feed is a list.
   */
  update?(previous: Cached | undefined, data: Data): Cached;
  /** Where to resume from; the server decides what that means. */
  lastEventId?: string;
  onData?(data: Data): void;
  onError?(error: WrpcError): void;
  onEnd?(): void;
}

export interface CreateQueryUtilsOptions {
  /** Prepended to every key, so several clients can share one cache. */
  prefix?: Array<unknown>;
  /** The default for {@link QueryUtils.subscriptionHandler}. */
  queryClient?: QueryCache;
}

export interface QueryUtils<Api = UntypedApi> {
  /** The cache key for a path; prefix-matchable, which is how invalidation works. */
  queryKey<Path extends [ApiUnits<Api>, string]>(path: Path, args?: unknown): WrpcQueryKey;
  /**
   * `{ queryKey, queryFn }` for useQuery. `extra` is merged in first and is
   * deliberately NOT checked against TanStack's option types — that would
   * require depending on them. Assign the result to your framework's option
   * type (as `tests/query.test-d.ts` does) if you want a typo in `staleTime`
   * caught.
   */
  queryOptions<Path extends CallPath<Api>>(
    path: Path,
    ...params: CallParams<Api, Path, [extra?: Record<string, unknown>]>
  ): WrpcQueryOptions<CallResult<Api, Path>>;
  mutationOptions<Path extends CallPath<Api>>(
    path: Path,
    extra?: Record<string, unknown>,
  ): WrpcMutationOptions<CallArgs<Api, Path>, CallResult<Api, Path>>;
  /**
   * Opens the subscription and writes every value into the cache. Returns the
   * wrpc handle: `unsubscribe()` on teardown — one call opens one
   * subscription, and nothing here de-duplicates two callers of the same feed.
   *
   * Two things to know about the cache entry, both TanStack's behaviour rather
   * than this bridge's:
   * - The **default `queryKey` is the subscription's own path**, where no
   *   `queryFn` can exist (`queryOptions` refuses subscription paths). Read it
   *   with `enabled: false`/`skipToken`, or pass `queryKey` to write into the
   *   query that already holds the data.
   * - An entry written only by `setQueryData` has no observers, so it is
   *   **garbage-collected after `gcTime`** (5 minutes in a browser; never on a
   *   server). An accumulating `update` therefore loses its history unless
   *   something is observing the key.
   */
  subscriptionHandler<Path extends SubscriptionPath<Api>, Cached = unknown>(
    path: Path,
    args?: SubscriptionArgs<Api, Path>,
    options?: SubscriptionHandlerOptions<SubscriptionData<Api, Path>, Cached>,
  ): Subscription;
}

/**
 * Binds a wrpc client to TanStack Query. Paths are resolved lazily, when a
 * query actually runs — `client.api` only exists after `load()`, and it is
 * rebuilt on every reconnect.
 */
export declare function createQueryUtils<Api = UntypedApi>(
  client: WrpcClient<Api>,
  options?: CreateQueryUtilsOptions,
): QueryUtils<Api>;
