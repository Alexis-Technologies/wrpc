import { expectAssignable, expectError, expectType } from 'tsd';
import * as wrpcQuery from '../query.js';
import type { QueryUtils, WrpcMutationOptions, WrpcQueryOptions } from '../query.js';
import type { Subscription, SubscriptionContract, WrpcClient } from '../index.js';
// The real thing, a devDependency used only here: an option object that is not
// assignable to TanStack's own types is not a binding, it is a lookalike.
import { QueryClient } from '@tanstack/query-core';
import type { MutationObserverOptions, QueryObserverOptions } from '@tanstack/query-core';

expectType<typeof wrpcQuery.createQueryUtils>(wrpcQuery.createQueryUtils);

interface Api {
  chat: {
    send(args: { text: string }): Promise<{ id: string }>;
    list(args: { room: string }): Promise<Array<{ id: string }>>;
    stats(): Promise<{ total: number }>;
    onMessage: SubscriptionContract<{ room: string }, { text: string }>;
  };
  'auth.1': {
    signIn(args: { login: string }): Promise<{ token: string }>;
  };
}

declare const client: WrpcClient<Api>;
const wq = wrpcQuery.createQueryUtils<Api>(client);
expectType<QueryUtils<Api>>(wq);
// The client's own contract is enough — the type argument is inferred from it
expectType<QueryUtils<Api>>(wrpcQuery.createQueryUtils(client));
expectAssignable<wrpcQuery.CreateQueryUtilsOptions>({ prefix: ['wrpc'], queryClient: new QueryClient() });

// A real QueryClient satisfies the structural cache contract, which is the
// whole reason this subpath can stay dependency-free.
expectAssignable<wrpcQuery.QueryCache>(new QueryClient());

// --- queries ---------------------------------------------------------------
const list = wq.queryOptions(['chat', 'list'], { room: 'a' });
expectType<WrpcQueryOptions<Array<{ id: string }>>>(list);
expectType<Array<unknown>>(list.queryKey);
expectType<Promise<Array<{ id: string }>>>(list.queryFn());
expectType<Promise<Array<{ id: string }>>>(list.queryFn({ signal: AbortSignal.timeout(1) }));
// Passing through TanStack's own options is how staleTime and friends get set
expectAssignable<WrpcQueryOptions<{ id: string }>>(wq.queryOptions(['chat', 'send'], { text: 'x' }, { staleTime: 1 }));
expectError(wq.queryOptions(['chat', 'list'], { room: 1 }));
expectError(wq.queryOptions(['chat', 'nope'], {}));
expectError(wq.queryOptions(['nope', 'list'], {}));
// Args the procedure needs cannot be forgotten; a procedure with none is happy
// without them, and refuses ones it never declared.
expectError(wq.queryOptions(['chat', 'list']));
expectType<WrpcQueryOptions<{ total: number }>>(wq.queryOptions(['chat', 'stats']));
expectError(wq.queryOptions(['chat', 'stats'], { junk: 1 }));
expectType<void>(null as unknown as wrpcQuery.CallArgs<Api, ['chat', 'stats']>);
// A subscription answers with a stream: it is not a query
expectError(wq.queryOptions(['chat', 'onMessage'], { room: 'a' }));
// ...and a versioned unit key works like any other
expectType<WrpcQueryOptions<{ token: string }>>(wq.queryOptions(['auth.1', 'signIn'], { login: 'me' }));

// --- mutations -------------------------------------------------------------
const send = wq.mutationOptions(['chat', 'send']);
expectType<WrpcMutationOptions<{ text: string }, { id: string }>>(send);
expectType<Promise<{ id: string }>>(send.mutationFn({ text: 'hi' }));
expectError(send.mutationFn({ text: 1 }));
expectError(wq.mutationOptions(['chat', 'onMessage']));

// --- subscriptions ---------------------------------------------------------
declare const queryClient: QueryClient;
expectType<Subscription>(
  wq.subscriptionHandler(
    ['chat', 'onMessage'],
    { room: 'a' },
    {
      queryClient,
      onData: (data) => expectType<{ text: string }>(data),
    },
  ),
);
// The updater sees the cached value and the new one, both typed
expectType<Subscription>(
  wq.subscriptionHandler<['chat', 'onMessage'], Array<string>>(
    ['chat', 'onMessage'],
    { room: 'a' },
    {
      queryClient,
      update: (previous, data) => [...(previous ?? []), data.text],
    },
  ),
);
expectError(wq.subscriptionHandler(['chat', 'onMessage'], { room: 1 }, { queryClient }));
// A call answers once: it is not a subscription
expectError(wq.subscriptionHandler(['chat', 'send'], { text: 'x' }, { queryClient }));

// --- assignable to TanStack's own option types -----------------------------
expectAssignable<QueryObserverOptions<Array<{ id: string }>, Error, Array<{ id: string }>, Array<{ id: string }>>>(
  wq.queryOptions(['chat', 'list'], { room: 'a' }),
);
expectAssignable<MutationObserverOptions<{ id: string }, Error, { text: string }>>(
  wq.mutationOptions(['chat', 'send']),
);

// --- the untyped default ---------------------------------------------------
declare const looseClient: WrpcClient;
const loose = wrpcQuery.createQueryUtils(looseClient);
expectAssignable<Array<unknown>>(loose.queryKey(['anything', 'goes']));
expectAssignable<WrpcQueryOptions<unknown>>(loose.queryOptions(['anything', 'goes'], { x: 1 }));
expectAssignable<WrpcMutationOptions<unknown, unknown>>(loose.mutationOptions(['anything', 'goes']));
expectType<Subscription>(loose.subscriptionHandler(['anything', 'goes'], {}, { queryClient }));
// A path is always [unit, method] — never a string, never three deep
expectError(loose.queryOptions('anything'));
expectError(loose.queryOptions(['anything', 'goes', 'deeper']));

// --- the derived path types -------------------------------------------------
expectAssignable<wrpcQuery.CallPath<Api>>(['chat', 'send']);
expectAssignable<wrpcQuery.CallPath<Api>>(['auth.1', 'signIn']);
expectError<wrpcQuery.CallPath<Api>>(['chat', 'onMessage']);
expectAssignable<wrpcQuery.SubscriptionPath<Api>>(['chat', 'onMessage']);
expectError<wrpcQuery.SubscriptionPath<Api>>(['chat', 'send']);
expectType<{ text: string }>(null as unknown as wrpcQuery.CallArgs<Api, ['chat', 'send']>);
expectType<{ id: string }>(null as unknown as wrpcQuery.CallResult<Api, ['chat', 'send']>);
expectType<{ room: string }>(null as unknown as wrpcQuery.SubscriptionArgs<Api, ['chat', 'onMessage']>);
expectType<{ text: string }>(null as unknown as wrpcQuery.SubscriptionData<Api, ['chat', 'onMessage']>);
